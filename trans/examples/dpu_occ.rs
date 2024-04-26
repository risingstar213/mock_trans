#![feature(get_mut_unchecked)]

#[cfg(feature = "doca_deps")]
mod test_dpu {

use std::marker::PhantomData;
use std::sync::Arc;

use trans::occ::{BatchRpcProc, occ_rpc_id};
use trans::framework::rpc::RpcHandler;
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

impl RpcHandler for OccProcWorker {
    fn rpc_handler(
            &self,
            src_conn: &mut trans::rdma::rcconn::RdmaRcConn,
            rpc_id: u32,
            msg: *mut u8,
            size: u32,
            meta: trans::framework::rpc::RpcProcessMeta,
        ) {
            match rpc_id {
                occ_rpc_id::READ_RPC => {
                    self.proc.read_cache_rpc_handler(src_conn, msg, size, meta);
                }
                occ_rpc_id::FETCHWRITE_RPC => {
                    self.proc.fetch_write_cache_rpc_handler(src_conn, msg, size, meta);
                }
                occ_rpc_id::LOCK_RPC => {
                    self.proc.lock_cache_rpc_handler(src_conn, msg, size, meta);
                }
                occ_rpc_id::VALIDATE_RPC => {
                    self.proc.validate_cache_rpc_handler(src_conn, msg, size, meta);
                }
                occ_rpc_id::RELEASE_RPC => {
                    self.proc.release_cache_rpc_handler(src_conn, msg, size, meta);
                }
                occ_rpc_id::ABORT_RPC => {
                    self.proc.abort_cache_rpc_handler(src_conn, msg, size, meta);
                }
                _ => {
                    unimplemented!();
                }
            }
    }
}

impl OccProcWorker {
    pub async fn main_routine(&self) {
        loop {
            self.scheduler.poll_recvs();
            self.scheduler.poll_sends();
            self.scheduler.poll_comm_chan();

            self.scheduler.yield_now(0).await;
        }
    }
}

pub async fn test() {
    // memdb
    let mut memdb = Arc::new(MemDB::new());
    let memstore = RobinhoodMemStore::<PhantomData<usize>>::new();

    Arc::get_mut(&mut memdb).unwrap().add_schema(0, TableSchema::default(), memstore);


    // comm chan
    let comm_chan = DocaCommChannel::new_server("cc_server\0", "03:00.0", "af:00.0");

    // rdma conn
    let mut rdma = RdmaControl::new(100);
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
    let worker = Arc::new(OccProcWorker::new(&memdb, &scheduler));
    // worker comm chan handler
    unsafe {
        Arc::get_mut_unchecked(&mut scheduler).register_comm_handler(&worker);
    }

    // worker rdma conn handler
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