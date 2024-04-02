pub mod occ_rpc_id {
    pub type Type = u32;
    pub const IGNORE_RPC:     Type = 0;
    pub const READ_RPC:       Type = 1;
    pub const FETCHWRITE_RPC: Type = 2;
    pub const WRITE_RPC:      Type = 3;
    pub const VALIDATE_RPC:   Type = 4;
    pub const COMMIT_RPC:     Type = 5;
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
pub struct FetchWriteReqItem {
    pub(crate) table_id:   usize,
    pub(crate) key:        u64,
    pub(crate) update_idx: usize,
    pub(crate) lock_sig:   u64,
}

#[repr(C)]
#[derive(Clone)]
pub struct WriteReqItem {
    pub(crate) table_id: usize,
    pub(crate) key:      u64,
    pub(crate) lock_sig: u64
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
    pub(crate) lock_sig: u64,
}