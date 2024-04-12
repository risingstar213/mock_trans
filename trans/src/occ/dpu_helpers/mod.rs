#[repr(C)]
#[derive(Clone)]
pub struct DmaReadSetItem {
    pub(crate) table_id: usize,
    pub(crate) key:      u64,
    pub(crate) old_seq:  u64,
}

// For write
#[repr(C)]
#[derive(Clone)]
pub struct DmaWriteSetItem {
    pub(crate) table_id: usize,
    pub(crate) key:      u64,
}