use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::Mutex as AsyncMutex;

use crate::common::random::FastRandom;
use crate::memstore::memdb::ValueDB;
use crate::occ::occ_rpc_id;
use crate::occ::HostRpcProc;
use crate::occ::doca_comm_info_id;
use crate::framework::scheduler::AsyncScheduler;
use crate::framework::rpc::*;
use crate::doca_comm_chan::connection::DocaCommHandler;
use crate::doca_comm_chan::comm_buf::DocaCommBuf;
use crate::rdma::rcconn::RdmaRcConn;
use crate::TPCC_NROUTINES;

use super::super::*;

pub struct TpccHostWorker {
    pub part_id: u64,
    pub tid: u32,
    pub valuedb: Arc<ValueDB>,
    pub scheduler: Arc<AsyncScheduler>,
    pub proc: HostRpcProc,
}

impl TpccHostWorker {
    pub fn new(part_id: u64, tid: u32, valuedb: &Arc<ValueDB>, scheduler: &Arc<AsyncScheduler>) -> Self {
        Self {
            part_id: part_id,
            tid: tid, 
            scheduler: scheduler.clone(),
            valuedb: valuedb.clone(),
            proc: HostRpcProc::new(tid, valuedb, scheduler),
        }
    }
}

impl DocaCommHandler for TpccHostWorker {
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
            doca_comm_info_id::REMOTE_READ_INFO => {
                self.proc.remote_read_info_handler(buf, info_payload, info_pid, info_tid, info_cid);
            }
            doca_comm_info_id::REMOTE_FETCHWRITE_INFO => {
                self.proc.remote_fetch_write_info_handler(buf, info_payload, info_pid, info_tid, info_cid);
            }
            _ => { panic!("unsupported!"); }
        }
    }
}

impl RpcHandler for TpccHostWorker {
    fn rpc_handler(
        &self,
            src_conn: &mut RdmaRcConn,
            rpc_id: u32,
            msg: *mut u8,
            size: u32,
            meta: RpcProcessMeta,
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

impl TpccHostWorker {
    async fn work_routine(&self, cid: u32, rand_seed: usize, conn: Arc<AsyncMutex<mpsc::Receiver<TpccClientReq>>>) {
        let mut rand_gen = FastRandom::new(rand_seed);
        
        loop {
            let mut receiver = conn.lock().await;
            let req = receiver.recv().await.unwrap();
            drop(receiver);

            match req.workload {
                TpccWorkLoadId::TxnNewOrder => {
                    self.txn_new_order(&mut rand_gen, cid).await;
                }
            }
        }
    }

    async fn main_routine(&self) {
        loop {
            self.scheduler.poll_comm_chan();
            self.scheduler.poll_recvs();
            self.scheduler.poll_sends();

            self.scheduler.yield_now(0).await;
        }
    }

    pub async fn run(self: &Arc<Self>, rand_seed: usize, client: &Arc<AsyncMutex<mpsc::Receiver<TpccClientReq>>>) {
        let mut futures = Vec::new();
        let mut rand_gen = FastRandom::new(rand_seed);
        
        for i in 1..TPCC_NROUTINES {
            let self_clone = self.clone();
            let seed = rand_gen.next();
            let client_clone = client.clone();
    
            futures.push(tokio::spawn(async move {
                self_clone.work_routine(i as _, seed, client_clone).await;
            }));
        }
    
        let self_clone = self.clone();
        tokio::spawn(async move {
            self_clone.main_routine().await
        }).await.unwrap();
    
        for f in futures {
            tokio::join!(f).0.unwrap();
        }
    }
}