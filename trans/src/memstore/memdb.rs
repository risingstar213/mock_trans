use core::panic;
use std::hash::Hash;

use super::memstore::{MemNode, MemNodeMeta, MemStore};
use super::hashtable::HashTableMemStore;

pub struct TableSchema {
    k_len: u32,
    v_len: u32,
    meta_len: u32,
}

impl TableSchema {
    pub fn new(k_len: u32, v_len: u32, meta_len: u32) -> Self {
        Self {
            k_len,
            v_len,
            meta_len,
        }
    }
}

pub enum MemStoreType {
    TAB_NONE,
    TAB_ROBINHOOD,
    TAB_BPLUSTREE,
}

pub struct MemDB<'memdb> {
    metas:  Vec<TableSchema>,
    tables: Vec<Box<dyn MemStore + 'memdb>>,
}

impl<'memdb> MemDB<'memdb>
{
    pub fn new() -> Self {
        Self {
            metas: Vec::new(),
            tables: Vec::new()
        }
    }

    pub fn add_schema(&mut self, table_id: usize, schema: TableSchema, table: impl MemStore + 'memdb) {
        let table_count = self.metas.len();
        if table_count != table_id {
            panic!("not rational add schema");
        }

        self.metas.push(schema);
        self.tables.push(Box::new(table) as _);
    }

    // local
    pub fn local_get_readonly(&self, table_id: usize, key: u64, ptr: *mut u8, len: u32) -> Option<MemNodeMeta>
    {
        if table_id >= self.metas.len() {
            println!("the table does not exists!");
            return None;
        }

        self.tables[table_id].local_get_readonly(key, ptr, len)
    }

    pub fn local_get_for_upd(&self, table_id: usize, key: u64, ptr: *mut u8, len: u32, lock_content: u64) -> Option<MemNodeMeta>
    {
        if table_id >= self.metas.len() {
            println!("the table does not exists!");
            return None;
        }

        self.tables[table_id].local_get_for_upd(key, ptr, len, lock_content)
    }

    pub fn local_lock_for_ins(&self, table_id: usize, key: u64, lock_content: u64) -> Option<MemNodeMeta>
    {
        if table_id >= self.metas.len() {
            println!("the table does not exists!");
            return None;
        }

        self.tables[table_id].local_lock_for_ins(key, lock_content)
    }

    pub fn local_unlock(&self, table_id: usize, key: u64, lock_content: u64) -> Option<MemNodeMeta>
    {
        if table_id >= self.metas.len() {
            println!("the table does not exists!");
            return None;
        }

        self.tables[table_id].local_unlock(key, lock_content)
    }

    pub fn local_advance_seq(&self, table_id: usize, key: u64) -> Option<MemNodeMeta>
    {
        if table_id >= self.metas.len() {
            println!("the table does not exists!");
            return None;
        }

        self.tables[table_id].local_advance_seq(key)
    }

    pub fn local_upd_val(&self, table_id: usize, key: u64, ptr: *mut u8, len: u32) -> Option<MemNodeMeta>
    {
        if table_id >= self.metas.len() {
            println!("the table does not exists!");
            return None;
        }

        self.tables[table_id].local_upd_val(key, ptr, len)
    }

    pub fn local_erase(&self, table_id: usize, key: u64) -> Option<MemNodeMeta>
    {
        if table_id >= self.metas.len() {
            println!("the table does not exists!");
            return None;
        }

        self.tables[table_id].local_erase(key)
    }
}
