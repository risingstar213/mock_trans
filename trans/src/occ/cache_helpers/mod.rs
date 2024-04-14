pub mod trans_cache_view;

#[repr(C)]
#[derive(Clone)]
pub struct CacheReadSetItem {
    pub(crate) table_id: usize,
    pub(crate) key:      u64,
    pub(crate) old_seq:  u64,
}

// For write
#[repr(C)]
#[derive(Clone)]
pub struct CacheWriteSetItem {
    pub(crate) table_id: usize,
    pub(crate) key:      u64,
}