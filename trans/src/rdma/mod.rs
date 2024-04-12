pub mod control;
pub mod one_side;
pub mod rcconn;
pub mod two_sides;

pub use control::RdmaBaseAllocator;

use lazy_static::lazy_static;
use rcconn::RdmaRcConn;
use std::sync::Arc;

pub trait RdmaRecvCallback {
    fn rdma_recv_handler(&self, src_conn: &mut RdmaRcConn, msg: *mut u8);
}

#[derive(Default)]
struct DefaultRdmaRecvCallback;

impl RdmaRecvCallback for DefaultRdmaRecvCallback {
    #![allow(unused_variables)]
    fn rdma_recv_handler(&self, src_conn: &mut RdmaRcConn, msg: *mut u8) {
        unimplemented!("rdma recv callback");
    }
}

lazy_static! {
    static ref DEFAULT_RDMA_RECV_HANDLER: Arc<DefaultRdmaRecvCallback> =
        Arc::new(DefaultRdmaRecvCallback);
}

pub trait RdmaSendCallback {
    fn rdma_send_handler(&self, wr_id: u64);
}

struct DefaultRdmaSendCallback;

impl RdmaSendCallback for DefaultRdmaSendCallback {
    #![allow(unused_variables)]
    fn rdma_send_handler(&self, wr_id: u64) {
        unimplemented!("rdma send callback");
    }
}

lazy_static! {
    static ref DEFAULT_RDMA_SEND_HANDLER: Arc<DefaultRdmaSendCallback> =
        Arc::new(DefaultRdmaSendCallback);
}
