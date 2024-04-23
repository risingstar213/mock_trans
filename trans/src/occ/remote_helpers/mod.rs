pub mod batch_rpc_msg_wrapper;

pub mod batch_rpc_ctrl;
pub mod batch_rpc_proc;
pub mod batch_rpc_proc_yield;

use crate::framework::rpc::*;

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


pub mod occ_rpc_id {
    pub type Type = u32;
    #[allow(unused)]
    pub const IGNORE_RPC:      Type = 0;
    pub const READ_RPC:        Type = 1;
    pub const FETCHWRITE_RPC:  Type = 2;
    pub const LOCK_RPC:        Type = 3;
    pub const VALIDATE_RPC:    Type = 4;
    pub const COMMIT_RPC:      Type = 5;
    pub const RELEASE_RPC:     Type = 6;
    pub const ABORT_RPC:       Type = 7;
}

#[repr(C)]
#[derive(Clone)]
pub struct ReadReqItem {
    pub(crate) table_id: usize,
    pub(crate) key:      u64,
    pub(crate) read_idx: usize,
}

#[repr(C)]
#[derive(Clone)]
pub struct ReadRespItem {
    pub(crate) read_idx: usize,
    pub(crate) seq:      u64,
    pub(crate) length:   usize,
}

#[repr(C)]
#[derive(Clone)]
pub struct ReadCacheRespItem {
    pub(crate) read_idx: usize,
    pub(crate) length:   usize,
}

#[repr(C)]
#[derive(Clone)]
pub struct FetchWriteReqItem {
    pub(crate) table_id:   usize,
    pub(crate) key:        u64,
    pub(crate) update_idx: usize,
}

#[repr(C)]
#[derive(Clone)]
pub struct FetchWriteRespItem {
    pub(crate) update_idx: usize,
    pub(crate) seq:        u64,
    pub(crate) length:     usize,
}

#[repr(C)]
#[derive(Clone)]
pub struct FetchWriteCacheRespItem {
    pub(crate) update_idx: usize,
    pub(crate) length:     usize,
}

#[repr(C)]
#[derive(Clone)]
pub struct LockReqItem {
    pub(crate) table_id: usize,
    pub(crate) key:      u64,
}

#[repr(C)]
#[derive(Clone)]
pub struct ValidateReqItem {
    pub(crate) table_id: usize,
    pub(crate) key:      u64,
    pub(crate) old_seq:  u64,
}

#[repr(C)]
#[derive(Clone)]
pub struct CommitReqItem {
    pub(crate) table_id: usize,
    pub(crate) key:      u64,
    pub(crate) length:   u32, // flexible length, zero means erase
}

#[repr(C)]
#[derive(Clone)]
pub struct CommitCacheReqItem {
    pub(crate) length:   u32, // flexible length, zero means erase
}

#[repr(C)]
#[derive(Clone)]
pub struct ReleaseReqItem {
    pub(crate) table_id: usize,
    pub(crate) key:      u64,
}

#[repr(C)]
#[derive(Clone)]
pub struct AbortReqItem {
    pub(crate) table_id: usize,
    pub(crate) key:      u64,
    pub(crate) insert:   bool,
}

#[repr(C)]
#[derive(Clone)]
pub struct DummyReqItem {}

// pub struct YieldReq {
//     yield_rpc_id: occ_rpc_id::Type,
//     yield_meta:   RpcProcessMeta,
//     addi_success: bool,
// }