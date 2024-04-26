use core::panic;

use super::memstore::{MemNodeMeta, MemStore};
use super::valuestore::ValueStore;

#[allow(unused)]
pub struct TableSchema {
    k_len: u32,
    v_len: u32,
    meta_len: u32,
}

impl Default for TableSchema {
    fn default() -> Self {
        Self {
            k_len: 0,
            v_len: 0,
            meta_len: 0
        }
    }
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
    TabNone,
    TableRobinhood,
    TabBplustree,
}

pub struct MemDB {
    metas:  Vec<TableSchema>,
    tables: Vec<Box<dyn MemStore + Send + Sync + 'static>>,
}

impl MemDB
{
    pub fn new() -> Self {
        Self {
            metas: Vec::new(),
            tables: Vec::new()
        }
    }

    pub fn add_schema(&mut self, table_id: usize, schema: TableSchema, table: impl MemStore + Send + Sync + 'static) {
        let table_count = self.metas.len();
        if table_count != table_id {
            panic!("not rational add schema");
        }

        self.metas.push(schema);
        self.tables.push(Box::new(table) as _);
    }

    // local
    pub fn get_item_length(&self, table_id: usize) -> usize {
        if table_id >= self.metas.len() {
            println!("the table does not exists!");
            return 0;
        }

        self.tables[table_id].get_item_length()
    }

    pub fn local_get_meta(&self, table_id: usize, key: u64) -> Option<MemNodeMeta>
    {
        if table_id >= self.metas.len() {
            println!("the table does not exists!");
            return None;
        }

        self.tables[table_id].local_get_meta(key)
    }


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

    pub fn local_lock(&self, table_id: usize, key: u64, lock_content: u64) -> Option<MemNodeMeta>
    {
        if table_id >= self.metas.len() {
            println!("the table does not exists!");
            return None;
        }

        self.tables[table_id].local_lock(key, lock_content)
    }

    pub fn local_unlock(&self, table_id: usize, key: u64, lock_content: u64) -> Option<MemNodeMeta>
    {
        if table_id >= self.metas.len() {
            println!("the table does not exists!");
            return None;
        }

        self.tables[table_id].local_unlock(key, lock_content)
    }

    pub fn local_upd_val_seq(&self, table_id: usize, key: u64, ptr: *const u8, len: u32) -> Option<MemNodeMeta>
    {
        if table_id >= self.metas.len() {
            println!("the table does not exists!");
            return None;
        }

        self.tables[table_id].local_upd_val_seq(key, ptr, len)
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


pub struct ValueDB {
    metas:  Vec<TableSchema>,
    tables: Vec<Box<dyn ValueStore + Send + Sync + 'static>>,
}

impl ValueDB
{
    pub fn new() -> Self {
        Self {
            metas: Vec::new(),
            tables: Vec::new()
        }
    }

    pub fn add_schema(&mut self, table_id: usize, schema: TableSchema, table: impl ValueStore + Send + Sync + 'static) {
        let table_count = self.metas.len();
        if table_count != table_id {
            panic!("not rational add schema");
        }

        self.metas.push(schema);
        self.tables.push(Box::new(table) as _);
    }

    // local
    pub fn get_item_length(&self, table_id: usize) -> usize {
        if table_id >= self.metas.len() {
            println!("the table does not exists!");
            return 0;
        }

        self.tables[table_id].get_item_length()
    }

    pub fn local_get_value(&self, table_id: usize, key: u64, ptr: *mut u8, len: u32) -> bool
    {
        if table_id >= self.metas.len() {
            println!("the table does not exists!");
            return false;
        }

        self.tables[table_id].local_get_value(key, ptr, len)
    }

    pub fn local_set_value(&self, table_id: usize, key: u64, ptr: *const u8, len: u32) -> bool
    {
        if table_id >= self.metas.len() {
            println!("the table does not exists!");
            return false;
        }

        self.tables[table_id].local_set_value(key, ptr, len)
    }

    pub fn local_put_value(&self, table_id: usize, key: u64, ptr: *const u8, len: u32)
    {
        if table_id >= self.metas.len() {
            println!("the table does not exists!");
            return;
        }

        self.tables[table_id].local_put_value(key, ptr, len);
    }

    pub fn local_erase_value(&self, table_id: usize, key: u64)
    {
        if table_id >= self.metas.len() {
            println!("the table does not exists!");
            return;
        }

        self.tables[table_id].local_erase_value(key);
    }
}
