#![feature(get_mut_unchecked)]
use std::sync::Arc;
use std::env;

use trans::common::logs::init_log;
use trans::occ::{BatchRpcProc, occ_rpc_id};

use trans::memstore::memdb::{MemDB, TableSchema};
use trans::memstore::{MemStoreValue, RobinhoodMemStore};

use trans::rdma::control::RdmaControl;
use trans::rdma::rcconn::RdmaRcConn;

use trans::framework::rpc::{RpcHandler, RpcProcessMeta};
use trans::framework::scheduler::AsyncScheduler;
use trans::framework::worker::AsyncWorker;

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

struct OccProcWorker {
    scheduler: Arc<AsyncScheduler>,
    proc: BatchRpcProc,
}

impl OccProcWorker {
    pub fn new(memdb: &Arc<MemDB>, scheduler: &Arc<AsyncScheduler>) -> Self {
        Self {
            scheduler: scheduler.clone(),
            proc: BatchRpcProc::new(0, memdb, scheduler),
        }
    }
}

impl RpcHandler for OccProcWorker {
    fn rpc_handler(
        &self,
        src_conn: &mut RdmaRcConn,
        rpc_id: u32,
        msg: *mut u8,
        size: u32,
        meta: RpcProcessMeta,
    ) {
        match rpc_id {
            occ_rpc_id::READ_RPC => {
                self.proc.read_rpc_handler(src_conn, msg, size, meta);
            }
            occ_rpc_id::FETCHWRITE_RPC => {
                self.proc.fetch_write_rpc_handler(src_conn, msg, size, meta);
            }
            occ_rpc_id::LOCK_RPC => {
                self.proc.lock_rpc_handler(src_conn, msg, size, meta);
            }
            occ_rpc_id::VALIDATE_RPC => {
                self.proc.validate_rpc_handler(src_conn, msg, size, meta);
            }
            occ_rpc_id::COMMIT_RPC => {
                self.proc.commit_rpc_handler(src_conn, msg, size, meta);
            }
            occ_rpc_id::RELEASE_RPC => {
                self.proc.release_rpc_handler(src_conn, msg, size, meta);
            }
            occ_rpc_id::ABORT_RPC => {
                self.proc.abort_rpc_handler(src_conn, msg, size, meta);
            }
            _ => {
                unimplemented!();
            }
        }
    }
}

impl AsyncWorker for OccProcWorker {
    fn get_scheduler(&self) -> &AsyncScheduler {
        return &self.scheduler;
    }

    fn has_stopped(&self) -> bool {
        false
    }
}

#[tokio::main]
async fn main() {
    let log_path = env::current_dir().unwrap().join("test.log");
    init_log(log_path.as_path());
    
    // memdb
    let mut memdb = Arc::new(MemDB::new());
    let memstore = RobinhoodMemStore::<Account>::new();
    
    Arc::get_mut(&mut memdb).unwrap().add_schema(0, TableSchema::default(), memstore);
    // scheduler
    let mut rdma = RdmaControl::new(1);
    rdma.init("0.0.0.0\0", "7472\0");
    rdma.listen_task(1);

    let allocator = rdma.get_allocator();
    let mut scheduler = Arc::new(AsyncScheduler::new(0, 1, &allocator));

    let conn = rdma.get_connection(0);
    conn.lock().unwrap().init_and_start_recvs().unwrap();

    unsafe {
        Arc::get_mut_unchecked(&mut scheduler).append_conn(0, &conn);
    }
    conn.lock()
        .unwrap()
        .register_recv_callback(&scheduler)
        .unwrap();

    let worker = Arc::new(OccProcWorker::new(&memdb, &scheduler));
    unsafe {
        Arc::get_mut_unchecked(&mut scheduler).register_callback(&worker);
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