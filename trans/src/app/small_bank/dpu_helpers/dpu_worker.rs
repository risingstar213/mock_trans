use std::sync::Arc;
use std::time::Duration;
use std::thread::sleep;

use crate::common::random::FastRandom;
use crate::memstore::memdb::MemDB;
use crate::occ::occ_rpc_id;
use crate::occ::DpuRpcProc;
use crate::occ::doca_comm_info_id;
use crate::framework::scheduler::AsyncScheduler;
use crate::framework::rpc::*;
use crate::doca_comm_chan::connection::DocaCommHandler;
use crate::doca_comm_chan::comm_buf::DocaCommBuf;
use crate::rdma::rcconn::RdmaRcConn;
use crate::SMALL_BANK_NROUTINES;

pub struct SmallBankDpuWorker {
    scheduler: Arc<AsyncScheduler>,
    proc: DpuRpcProc,
}

impl SmallBankDpuWorker {
    pub fn new(tid: u32, memdb: &Arc<MemDB>, scheduler: &Arc<AsyncScheduler>) -> Self {
        Self {
            scheduler: scheduler.clone(),
            proc: DpuRpcProc::new(tid, memdb, scheduler),
        }
    }
}

impl DocaCommHandler for SmallBankDpuWorker {
    fn comm_handler(
            &self,
            buf: &DocaCommBuf,
            info_id: u32,
            info_payload: u32,
            info_pid: u32,
            info_tid: u32,
            info_cid: u32,
        ) {
        match info_id {
            doca_comm_info_id::LOCAL_READ_INFO => {
                self.proc.local_read_info_handler(buf, info_payload, info_pid, info_tid, info_cid);
            }
            doca_comm_info_id::LOCAL_LOCK_INFO => {
                self.proc.local_lock_info_handler(buf, info_payload, info_pid, info_tid, info_cid);
            }
            doca_comm_info_id::LOCAL_VALIDATE_INFO => {
                self.proc.local_validate_info_handler(buf, info_payload, info_pid, info_tid, info_cid);
            }
            doca_comm_info_id::LOCAL_RELEASE_INFO => {
                self.proc.local_release_info_handler(buf, info_payload, info_pid, info_tid, info_cid);
            }
            doca_comm_info_id::LOCAL_ABORT_INFO => {
                self.proc.local_abort_info_handler(buf, info_payload, info_pid, info_tid, info_cid);
            }
            _ => { panic!("unsupported!"); }
        }
    }
}

impl RpcHandler for SmallBankDpuWorker {
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

impl SmallBankDpuWorker {
    async fn main_routine(&self, tid: u32) {
        loop {
            self.scheduler.poll_recvs();
            self.scheduler.poll_sends();
            self.scheduler.poll_comm_chan();

            // sleep(Duration::from_millis(1));

            // self.scheduler.yield_now(0).await;
        }
    }

    pub async fn run(self: &Arc<Self>, tid: u32) {
        self.main_routine(tid).await;
    }
}