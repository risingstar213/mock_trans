#![feature(get_mut_unchecked)]
use std::marker::PhantomData;
use std::sync::Arc;

use trans::occ::doca_comm_info_id;
use trans::rdma::control::RdmaControl;

use trans::doca_comm_chan::connection::DocaCommChannel;
use trans::doca_comm_chan::connection::DocaCommHandler;
use trans::memstore::memdb::{MemDB, TableSchema};
use trans::memstore::{MemStoreValue, RobinhoodMemStore};
use trans::memstore::RobinhoodValueStore;

use trans::framework::scheduler::AsyncScheduler;

use trans::occ::occ_host::OccHost;
use trans::occ::RwType;
use trans::occ::DpuRpcProc;

struct OccProcWorker {
    scheduler: Arc<AsyncScheduler>,
    proc: DpuRpcProc,
}

impl OccProcWorker {
    pub fn new(memdb: &Arc<MemDB>, scheduler: &Arc<AsyncScheduler>) -> Self {
        Self {
            scheduler: scheduler.clone(),
            proc: DpuRpcProc::new(0, memdb, scheduler),
        }
    }
}

impl DocaCommHandler for OccProcWorker {
    fn comm_handler(
            &self,
            buf: &trans::doca_comm_chan::comm_buf::DocaCommBuf,
            info_id: u32,
            info_payload: u32,
            info_pid: u32,
            info_cid: u32,
        ) {
        match info_id {
            doca_comm_info_id::LOCAL_READ_INFO => {
                self.proc.local_read_info_handler(buf, info_payload, info_pid, info_cid);
            }
            doca_comm_info_id::LOCAL_LOCK_INFO => {
                self.proc.local_lock_info_handler(buf, info_payload, info_pid, info_cid);
            }
            doca_comm_info_id::LOCAL_VALIDATE_INFO => {
                self.proc.local_validate_info_handler(buf, info_payload, info_pid, info_cid);
            }
            doca_comm_info_id::LOCAL_RELEASE_INFO => {
                self.proc.local_release_info_handler(buf, info_payload, info_pid, info_cid);
            }
            doca_comm_info_id::LOCAL_ABORT_INFO => {
                self.proc.local_abort_info_handler(buf, info_payload, info_pid, info_cid);
            }
            _ => { panic!("unsupported!"); }
        }
    }
}

impl OccProcWorker {
    pub async fn main_routine(&self) {
        loop {
            self.scheduler.poll_comm_chan();
            self.scheduler.yield_now(0).await;
        }
    }
}

async fn test() {
    // memdb
    let mut memdb = Arc::new(MemDB::new());
    let memstore = RobinhoodMemStore::<PhantomData<usize>>::new();

    // comm conn
    // scheduler
    let mut comm_chan = DocaCommChannel::new_server("cc_server", "03:00.0", "af:00.0");

    let rdma = RdmaControl::new(100);

    let allocator = rdma.get_allocator();
    let mut scheduler = Arc::new(AsyncScheduler::new(0, 1, &allocator));

    unsafe {
        Arc::get_mut_unchecked(&mut scheduler).set_comm_chan(comm_chan);
    }

    let worker = Arc::new(OccProcWorker::new(&memdb, &scheduler));

    unsafe {
        Arc::get_mut_unchecked(&mut scheduler).register_comm_handler(&worker);
    }

    {
        let worker0 = worker.clone();
        tokio::task::spawn(async move {
            worker0.main_routine().await;
        })
        .await
        .unwrap();
    }
}


fn main() {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async move {
            test().await;
    });
}