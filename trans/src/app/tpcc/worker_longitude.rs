use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::Mutex as AsyncMutex;

use crate::rdma::control::RdmaControl;
use crate::rdma::rcconn::RdmaRcConn;
use crate::memstore::memdb::MemDB;
use crate::TPCC_NROUTINES;
use crate::common::random::FastRandom;
use crate::framework::worker::AsyncWorker;
use crate::framework::scheduler::AsyncScheduler;
use crate::framework::rpc::*;
use crate::occ::occ_rpc_id;

use super::TpccHybridLongitudeWorker;
use super::TpccHostLongitudeWorker;
use super::TpccClientReq;
use super::TpccWorkLoadId;

impl AsyncWorker for TpccHybridLongitudeWorker {
    fn get_scheduler(&self) -> &crate::framework::scheduler::AsyncScheduler {
        &self.scheduler
    }

    fn has_stopped(&self) -> bool {
        false
    }
}

impl RpcHandler for TpccHybridLongitudeWorker {
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
            occ_rpc_id::COMMIT_RPC => {
                self.proc.commit_cache_rpc_handler(src_conn, msg, size, meta);
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


impl TpccHybridLongitudeWorker {
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

impl AsyncWorker for TpccHostLongitudeWorker {
    fn get_scheduler(&self) -> &crate::framework::scheduler::AsyncScheduler {
        &self.scheduler
    }

    fn has_stopped(&self) -> bool {
        false
    }
}

impl RpcHandler for TpccHostLongitudeWorker {
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
            occ_rpc_id::COMMIT_RPC => {
                self.proc.commit_cache_rpc_handler(src_conn, msg, size, meta);
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


impl TpccHostLongitudeWorker {
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

    pub async fn run_main(self: &Arc<Self>, rand_seed: usize) {
    
        let self_clone = self.clone();
        tokio::spawn(async move {
            self_clone.main_routine().await
        }).await.unwrap();
    }
}