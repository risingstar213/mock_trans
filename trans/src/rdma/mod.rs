pub mod control;
pub mod connection;
use lazy_static::lazy_static;
use std::sync::Arc;

pub trait RdmaRecvCallback {
    fn rdma_recv_handler(&self, msg: *mut u8);
}

#[derive(Default)]
struct DefaultRdmaRecvCallback;

impl RdmaRecvCallback for DefaultRdmaRecvCallback {
    fn rdma_recv_handler(&self, msg: *mut u8) {
        unimplemented!("rdma recv callback");
    }
}

lazy_static! {
    static ref DEFAULT_RDMA_RECV_HANDLER: Arc<DefaultRdmaRecvCallback> = Arc::new(DefaultRdmaRecvCallback);
}

pub trait RdmaSendCallback {
    fn rdma_send_handler(&self, wr_id: u64);
}


struct DefaultRdmaSendCallback;

impl RdmaSendCallback for DefaultRdmaSendCallback {
    fn rdma_send_handler(&self, wr_id: u64) {
        unimplemented!("rdma send callback");
    }
}

lazy_static! {
    static ref DEFAULT_RDMA_SEND_HANDLER: Arc<DefaultRdmaSendCallback> = Arc::new(DefaultRdmaSendCallback);
}