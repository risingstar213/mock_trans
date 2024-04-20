use std::sync::Arc;

use byte_struct::*;
use lazy_static::lazy_static;

use crate::rdma::rcconn::RdmaRcConn;
// two-side information

pub mod rpc_msg_type {
    pub type Type = u32;
    pub const REQ: Type = 0;
    pub const Y_REQ: Type = 1;
    pub const RESP: Type = 2;
}

bitfields!(
    #[derive(PartialEq, Debug)]
    pub RpcHeaderMeta: u32 {
        pub rpc_type:    2,
        pub rpc_id:      5,
        pub rpc_payload: 18,
        pub rpc_cid:     7
    }
);

impl RpcHeaderMeta {
    pub fn new(rpc_type: u32, rpc_id: u32, rpc_payload: u32, rpc_cid: u32) -> Self {
        Self {
            rpc_type: rpc_type,
            rpc_id: rpc_id,
            rpc_payload: rpc_payload,
            rpc_cid: rpc_cid,
        }
    }

    pub fn to_header(&self) -> u32 {
        self.to_raw()
    }

    pub fn from_header(raw: u32) -> Self {
        RpcHeaderMeta::from_raw(raw)
    }
}

pub struct RpcProcessMeta {
    pub rpc_cid: u32,
    pub peer_id: u64,
    pub peer_tid: u64,
}

impl RpcProcessMeta {
    pub fn new(rpc_cid: u32, peer_id: u64, peer_tid: u64) -> Self {
        Self {
            rpc_cid: rpc_cid,
            peer_id: peer_id,
            peer_tid: peer_tid,
        }
    }
}

// pub trait RpcMsg {
//     fn get_header() -> RpcHeaderMeta;
// }

pub trait AsyncRpc {
    fn get_reply_buf(&self) -> *mut u8;
    fn get_req_buf(&self, cid: u32) -> *mut u8;

    fn send_reply(
        &self,
        src_conn: &mut RdmaRcConn,
        msg: *mut u8,
        rpc_id: u32,
        rpc_size: u32,
        rpc_cid: u32,
        peer_id: u64,
        peer_tid: u64,
    );
    fn append_pending_req(
        &self,
        msg: *mut u8,
        rpc_id: u32,
        rpc_size: u32,
        rpc_cid: u32,
        rpc_type: u32,
        peer_id: u64,
        peer_tid: u64,
    );
    fn send_req(
        &self,
        msg: *mut u8,
        rpc_id: u32,
        rpc_size: u32,
        rpc_cid: u32,
        rpc_type: u32,
        peer_id: u64,
        peer_tid: u64,
    );
}

pub trait RpcHandler {
    fn rpc_handler(
        &self,
        src_conn: &mut RdmaRcConn,
        rpc_id: u32,
        msg: *mut u8,
        size: u32,
        meta: RpcProcessMeta,
    );
}

pub struct DefaultRpcHandler;

impl RpcHandler for DefaultRpcHandler {
    #[allow(unused)]
    fn rpc_handler(
        &self,
        src_conn: &mut RdmaRcConn,
        rpc_id: u32,
        msg: *mut u8,
        size: u32,
        meta: RpcProcessMeta,
    ) {
        unimplemented!("rpc handler");
    }
}

lazy_static! {
    pub static ref DEFAULT_RPC_HANDLER: Arc<DefaultRpcHandler> = Arc::new(DefaultRpcHandler);
}
#[test]
fn test_bitfield() {
    let header = RpcHeaderMeta {
        rpc_type: 1,
        rpc_id: 2,
        rpc_payload: 345,
        rpc_cid: 4,
    };

    let meta = header.to_raw();

    dbg!(header, meta);
}
