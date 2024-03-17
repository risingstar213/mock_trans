use std::sync::Arc;
use std::sync::Mutex;
use std::collections::HashMap;

use crate::rdma::RdmaRecvCallback;
use crate::rdma::rcconn::RdmaRcConn;

use super::rpc::AsyncRpc;

// process send / read / write saparately 
// because send does not require polling but read / write need

pub struct AsyncScheduler<'a> {
    conns:    Mutex<HashMap<u64, Arc<RdmaRcConn<'a>>>>,
    // pendings: Mutex<HashMap<u64, u64>>,
}

impl<'a> AsyncScheduler<'a> {
    pub fn new() -> Self {
        Self {
            conns:   Mutex::new(HashMap::new())
        }
    }

    fn append_conn(&self, id: u64, conn: &Arc<RdmaRcConn<'a>>) {
        self.conns.lock().unwrap().insert(id, conn.clone());
    }

    fn poll_comps(&self) {
        
    }
}

impl<'a> RdmaRecvCallback for AsyncScheduler<'a> {
    fn rdma_recv_handler(&self, msg: *mut u8) {
        todo!();
    }
}