pub mod comm_chan_ctrl;

pub mod dpu_rpc_proc;
pub mod host_rpc_proc;

#[repr(C)]
#[derive(Clone)]
pub struct DocaCommReply {
    success: u32,
}

pub mod doca_comm_info_id {
    pub type Type = u32;
    #[allow(unused)]
    pub const LOCAL_READ_INFO:        Type = 0;
    pub const LOCAL_LOCK_INFO:        Type = 1;
    pub const LOCAL_VALIDATE_INFO:    Type = 2;
    pub const LOCAL_RELEASE_INFO:     Type = 3;
    pub const LOCAL_ABORT_INFO:       Type = 4;
    pub const REMOTE_READ_INFO:       Type = 5;
    pub const REMOTE_FETCHWRITE_INFO: Type = 6;
}

#[repr(C)]
#[derive(Clone)]
pub struct LocalReadInfoItem {
    pub(crate) table_id: usize,
    pub(crate) key:      u64,
}

#[repr(C)]
#[derive(Clone)]
pub struct LocalLockInfoItem {
    pub(crate) table_id: usize,
    pub(crate) key:      u64,
}


