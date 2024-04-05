use std::sync::Arc;

use trans::rdma::control::RdmaControl;
use trans::rdma::rcconn::RdmaRcConn;

use trans::memstore::memdb::{MemDB, TableSchema};
use trans::memstore::{MemStoreValue, RobinhoodMemStore};

use trans::framework::scheduler::AsyncScheduler;
use trans::framework::worker::AsyncWorker;

use trans::occ::occ_remote::OccRemote;
use trans::occ::RwType;

#[repr(C)]
#[derive(Clone)]
struct Account {
    balance: u64,
}

impl Default for Account {
    fn default() -> Self {
        Self {
            balance: 10000,
        }
    }
}

impl MemStoreValue for Account {}

struct OccCtrlWorker<'worker> {
    memdb: Arc<MemDB<'worker>>,
    scheduler: Arc<AsyncScheduler<'worker>>,
}

impl<'worker> OccCtrlWorker<'worker> {
    pub fn new(memdb: &Arc<MemDB<'worker>>, scheduler: &Arc<AsyncScheduler<'worker>>) -> Self {
        Self {
            memdb: memdb.clone(),
            scheduler: scheduler.clone(),
        }
    }
}

impl<'worker> OccCtrlWorker<'worker> {
    async fn prepare_data(&self) {
        let mut occ1 = OccRemote::<8>::new(
            0, 
            1, 
            &self.memdb,
            &self.scheduler,
        );

        occ1.start();
        let idx = occ1.write::<Account>(0, 1, 10037, RwType::INSERT);
        occ1.set_value(false, idx, &Account{
            balance: 34567
        });

        let idx = occ1.write::<Account>(0, 1, 13356, RwType::INSERT);
        occ1.set_value(false, idx, &Account{
            balance: 67890
        });

        occ1.commit().await;

        assert_eq!(occ1.is_commited(), true);

        let mut occ2 = OccRemote::<8>::new(
            0, 
            1, 
            &self.memdb,
            &self.scheduler,
        );
        occ2.start();

        let idx = occ2.read::<Account>(0, 1, 10037);
        assert_eq!(occ2.get_value::<Account>(false, idx).await.balance, 34567);

        let idx = occ2.read::<Account>(0, 1, 13356);
        assert_eq!(occ2.get_value::<Account>(false, idx).await.balance, 67890);

        occ2.commit().await;

        assert_eq!(occ2.is_commited(), true);
    }

    async fn test_conflicts(&self) {
        let mut occ1 = OccRemote::<8>::new(
            0, 
            1, 
            &self.memdb,
            &self.scheduler,
        );
        occ1.start();

        let mut occ2 = OccRemote::<8>::new(
            0, 
            2, 
            &self.memdb,
            &self.scheduler,
        );
        occ2.start();

        let idx2 = occ2.read::<Account>(0, 1, 10037);
        let balance2 = occ2.get_value::<Account>(false, idx2).await.balance;
        let idx2 = occ2.write::<Account>(0, 1, 13356, RwType::UPDATE);
        occ2.set_value::<Account>(false, idx2, &Account{
            balance: balance2,
        });

        let idx1 = occ1.fetch_write::<Account>(0, 1, 13356);
        let balance1 = occ1.get_value::<Account>(true, idx1).await.balance + 1;

        occ1.set_value(true, idx1, &Account{
            balance: balance1,
        });

        occ2.commit().await;
        assert_eq!(occ2.is_aborted(), true);

        occ1.commit().await;
        assert_eq!(occ1.is_commited(), true);

        let mut occ3 = OccRemote::<8>::new(
            0, 
            1, 
            &self.memdb,
            &self.scheduler,
        );
        occ3.start();
        let idx3 = occ3.read::<Account>(0, 1, 13356);
        let balance3 = occ3.get_value::<Account>(false, idx3).await.balance;

        assert_eq!(balance3, balance1);

        occ3.commit().await;
        assert_eq!(occ3.is_commited(), true);
    }

    async fn work_routine(&self, cid: u32) {
        self.prepare_data().await;
        self.test_conflicts().await;
    }
}

impl<'worker> AsyncWorker<'worker> for OccCtrlWorker<'worker> {
    fn get_scheduler(&self) -> &AsyncScheduler<'worker> {
        return &self.scheduler;
    }

    fn has_stopped(&self) -> bool {
        false
    }
}

#[tokio::main]
async fn main() {
    // memdb
    let mut memdb = Arc::new(MemDB::new());
    let memstore = RobinhoodMemStore::<Account>::new();
    
    Arc::get_mut(&mut memdb).unwrap().add_schema(0, TableSchema::default(), memstore);

    // scheduler
    let mut rdma = RdmaControl::new(0);
    rdma.connect(1, "10.10.10.6\0", "7472\0").unwrap();

    let allocator = rdma.get_allocator();
    let scheduler = Arc::new(AsyncScheduler::new(3, &allocator));

    let conn = rdma.get_connection(1);
    conn.lock().unwrap().init_and_start_recvs().unwrap();

    scheduler.append_conn(1, &conn);
    conn.lock()
        .unwrap()
        .register_recv_callback(&scheduler)
        .unwrap();

    let worker = Arc::new(OccCtrlWorker::new(&memdb, &scheduler));
    // scheduler.register_callback(&worker);

    {
        let worker1 = worker.clone();
        tokio::task::spawn(async move {
            worker1.work_routine(1).await;
        });

        let worker0 = worker.clone();
        tokio::task::spawn(async move {
            worker0.main_routine().await;
        })
        .await
        .unwrap();
    }
}