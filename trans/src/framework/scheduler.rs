use std::sync::Arc;

use crate::rdma::RdmaRecvCallback;

struct AsyncScheduler {
    
}

impl AsyncScheduler {
    pub fn new() -> Self {
        Self {

        }
    }
}

impl RdmaRecvCallback for AsyncScheduler {
    fn rdma_recv_handler(&self, msg: *mut u8) {
        
    }
}