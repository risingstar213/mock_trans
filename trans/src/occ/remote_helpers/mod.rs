pub mod batch_rpc_msg_wrapper;

pub mod batch_rpc_ctrl;
pub mod batch_rpc_proc;

// peer_id and cid are used by server to store info when encounter YReq
#[repr(C)]
#[derive(Clone)]
pub struct BatchRpcReqHeader {
    pub(crate) peer_id: u64,
    pub(crate) cid:     u32,
    pub(crate) num:     u32,
}

#[repr(C)]
#[derive(Clone)]
pub struct BatchRpcRespHeader {
    pub(crate) write: bool,
    pub(crate) num:   u32,
}

#[repr(C)]
#[derive(Clone)]
pub struct BatchRpcReduceResp {
    pub(crate) success: bool,
}