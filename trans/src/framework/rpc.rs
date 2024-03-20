use byte_struct::*;

use crate::rdma::rcconn::RdmaRcConn;
use super::scheduler::AsyncScheduler;
// two-side information

pub mod RpcMsgType {
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
            rpc_type:    rpc_type,
            rpc_id:      rpc_id,
            rpc_payload: rpc_payload,
            rpc_cid:     rpc_cid
        }
    }

    pub fn to_header(&self) -> u32 {
        self.to_raw()
    }

    pub fn from_header(raw: u32) -> Self {
        RpcHeaderMeta::from_raw(raw)
    }
}

// pub trait RpcMsg {
//     fn get_header() -> RpcHeaderMeta;
// }

pub trait AsyncRpc {
    fn get_reply_buf(&self) -> *mut u8;
    fn get_req_buf(&self) -> *mut u8;

    fn send_reply(
        src_conn: &mut RdmaRcConn, 
        msg: *mut u8,
        rpc_id: u32, 
        rpc_size: u32,
        rpc_cid: u32,
        peer_id: u64,
        peer_tid: u64
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
    fn append_req(
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
    fn rpc_handler(&self, src_conn: &mut RdmaRcConn);
}

#[test]
fn test_bitfield() {
    let header = RpcHeaderMeta {
        rpc_type: 1,
        rpc_id:   2,
        rpc_payload: 345,
        rpc_cid:  4
    };

    let meta = header.to_raw();

    dbg!(header);
}