mod batch_rpc_msg_wrapper;

pub mod batch_rpc_ctrl;
pub mod batch_rpc_proc;

// peer_id and cid are used by server to store info when encounter YReq
#[repr(C)]
#[derive(Clone)]
struct BatchRpcReqHeader {
    peer_id: u64,
    cid:     u32,
    num:     u32,
}

#[repr(C)]
#[derive(Clone)]
struct BatchRpcRespHeader {
    num: u32
}