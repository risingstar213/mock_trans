#![feature(get_mut_unchecked)]

#[cfg(feature = "doca_deps")]
mod test_dpu {

use std::sync::Arc;

use trans::occ::occ_rpc_id;
use trans::rdma::control::RdmaControl;

use trans::occ::doca_comm_info_id;
use trans::framework::rpc::RpcHandler;
use trans::doca_comm_chan::connection::DocaCommChannel;
use trans::doca_comm_chan::connection::DocaCommHandler;
use trans::memstore::memdb::{ValueDB, TableSchema};
use trans::memstore::{MemStoreValue, RobinhoodMemStore};
use trans::memstore::RobinhoodValueStore;

use trans::framework::scheduler::AsyncScheduler;

use trans::occ::occ_host::OccHost;
use trans::occ::RwType;
use trans::occ::HostRpcProc;

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

struct OccCtrlWorker {
    valuedb: Arc<ValueDB>,
    scheduler: Arc<AsyncScheduler>,
    proc: HostRpcProc,
}

impl OccCtrlWorker {
    pub fn new(valuedb: &Arc<ValueDB>, scheduler: &Arc<AsyncScheduler>) -> Self {
        Self {
            valuedb: valuedb.clone(),
            scheduler: scheduler.clone(),
            proc: HostRpcProc::new(0, valuedb, scheduler),
        }
    }
}

impl OccCtrlWorker {
    async fn prepare_data(&self) {
        let mut occ1 = OccHost::<8>::new(
            0, 
            0,
            1, 
            &self.valuedb,
            &self.scheduler,
        );

        occ1.start();
        let idx = occ1.write::<Account>(0, 0, 10037, RwType::INSERT);
        occ1.set_value(false, idx, &Account{
            balance: 34567
        });

        let idx = occ1.write::<Account>(0, 0, 13356, RwType::INSERT);
        occ1.set_value(false, idx, &Account{
            balance: 67890
        });

        occ1.commit().await;

        assert_eq!(occ1.is_commited(), true);

        let mut occ2 = OccHost::<8>::new(
            0, 
            0,
            1, 
            &self.valuedb,
            &self.scheduler,
        );
        occ2.start();

        let idx = occ2.read::<Account>(0, 0, 10037);
        assert_eq!(occ2.get_value::<Account>(false, idx).await.balance, 34567);

        let idx = occ2.read::<Account>(0, 0, 13356);
        assert_eq!(occ2.get_value::<Account>(false, idx).await.balance, 67890);

        occ2.commit().await;

        assert_eq!(occ2.is_commited(), true);

        println!("insert !");
    }

    async fn test_conflicts(&self) {
        let mut occ1 = OccHost::<8>::new(
            0,
            0, 
            1, 
            &self.valuedb,
            &self.scheduler,
        );
        occ1.start();

        let mut occ2 = OccHost::<8>::new(
            0, 
            0,
            2, 
            &self.valuedb,
            &self.scheduler,
        );
        occ2.start();

        let idx2 = occ2.read::<Account>(0, 0, 10037);
        let balance2 = occ2.get_value::<Account>(false, idx2).await.balance;
        let idx2 = occ2.write::<Account>(0, 0, 13356, RwType::UPDATE);
        occ2.set_value::<Account>(false, idx2, &Account{
            balance: balance2,
        });

        let idx1 = occ1.fetch_write::<Account>(0, 0, 13356);
        let balance1 = occ1.get_value::<Account>(true, idx1).await.balance + 1;

        occ1.set_value(true, idx1, &Account{
            balance: balance1,
        });

        occ2.commit().await;
        assert_eq!(occ2.is_aborted(), true);

        occ1.commit().await;
        assert_eq!(occ1.is_commited(), true);

        let mut occ3 = OccHost::<8>::new(
            0, 
            0,
            1, 
            &self.valuedb,
            &self.scheduler,
        );
        occ3.start();
        let idx3 = occ3.read::<Account>(0, 0, 13356);
        let balance3 = occ3.get_value::<Account>(false, idx3).await.balance;

        assert_eq!(balance3, balance1);

        occ3.commit().await;
        assert_eq!(occ3.is_commited(), true);

        println!("pass conflicts!");
    }

    async fn work_routine(&self, cid: u32) {
        self.prepare_data().await;
        self.test_conflicts().await;
    }

    async fn main_routine(&self) {
        loop {
            self.scheduler.poll_recvs();
            self.scheduler.poll_sends();
            self.scheduler.poll_comm_chan();

            self.scheduler.yield_now(0).await;
        }
    }
}

impl DocaCommHandler for OccCtrlWorker {
    fn comm_handler(
            &self,
            buf: &trans::doca_comm_chan::comm_buf::DocaCommBuf,
            info_id: u32,
            info_payload: u32,
            info_pid: u32,
            info_cid: u32,
        ) {
        match info_id {
            doca_comm_info_id::REMOTE_READ_INFO => {
                self.proc.remote_read_info_handler(buf, info_payload, info_pid, info_cid);
            }
            doca_comm_info_id::REMOTE_FETCHWRITE_INFO => {
                self.proc.remote_fetch_write_info_handler(buf, info_payload, info_pid, info_cid);
            }
            _ => { panic!("unsupported!"); }
        }
    }
}

impl RpcHandler for OccCtrlWorker {
    fn rpc_handler(
        &self,
            src_conn: &mut trans::rdma::rcconn::RdmaRcConn,
            rpc_id: u32,
            msg: *mut u8,
            size: u32,
            meta: trans::framework::rpc::RpcProcessMeta,
    ) {
        match rpc_id {
            occ_rpc_id::COMMIT_RPC => {
                self.proc.commit_cache_rpc_handler(src_conn, msg, size, meta);
            }
            _ => {
                unimplemented!();
            }
        }
    }
}

pub async fn test() {
    // memdb
    let mut valuedb = Arc::new(ValueDB::new());
    let valuestore = RobinhoodValueStore::<Account>::new();
    
    Arc::get_mut(&mut valuedb).unwrap().add_schema(0, TableSchema::default(), valuestore);

    // comm chan
    let comm_chan = DocaCommChannel::new_client("cc_server\0", "af:00.0");

    // rdma conn
    let mut rdma = RdmaControl::new(0);
    rdma.init("0.0.0.0\0", "7472\0");
    rdma.listen_task(1);

    // scheduler
    let allocator = rdma.get_allocator();
    let mut scheduler = Arc::new(AsyncScheduler::new(0, 1, &allocator));

    // scheduler add comm chan
    unsafe {
        Arc::get_mut_unchecked(&mut scheduler).set_comm_chan(comm_chan);
    }

    // scheduler add rdma conn
    let conn = rdma.get_connection(1);
    conn.lock().unwrap().init_and_start_recvs().unwrap();
    conn.lock()
        .unwrap()
        .register_recv_callback(&scheduler)
        .unwrap();

    unsafe {
        Arc::get_mut_unchecked(&mut scheduler).append_conn(1, &conn);
    }
    
    // worker
    let worker = Arc::new(OccCtrlWorker::new(&valuedb, &scheduler));
    // worker comm chan handler
    unsafe {
        Arc::get_mut_unchecked(&mut scheduler).register_comm_handler(&worker);
    }

    // worker rdma conn handler
    unsafe {
        Arc::get_mut_unchecked(&mut scheduler).register_callback(&worker);
    }

    {
        // let worker1 = worker.clone();
        // tokio::task::spawn(async move {
        //     worker1.work_routine(1).await;
        // });

        let worker0 = worker.clone();
        tokio::task::spawn(async move {
            worker0.main_routine().await;
        })
        .await
        .unwrap();
    }
}

}

fn main() {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async move {
            #[cfg(feature = "doca_deps")]
            test_dpu::test().await;
    });
}