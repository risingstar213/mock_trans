use super::memstore::MemNode;
use super::memstore::MemStore;
use super::memstore::MemStoreValue;

struct TableSchema {
    k_len: u32,
    v_len: u32,
    meta_len: u32,
}

pub struct MemDB {
    metas: Vec<TableSchema>,
    tables: Vec<Box<dyn MemStore>>,
}

impl MemDB {}
