use std::sync::RwLock;

use super::robinhood::robinhoodcell::RobinHoodTableCell;
use super::memstore::{MemNode, MemNodeMeta, MemStore, MemStoreValue};

pub struct RobinhoodMemStore<T>
where
    T: MemStoreValue,
{
    table: RwLock<RobinHoodTableCell<T>>,
}

impl<T> RobinhoodMemStore<T> 
where 
    T: MemStoreValue 
{
    pub fn new() -> Self {
        Self {
            table: RwLock::new(RobinHoodTableCell::<T>::new())
        }
    }
}

impl<T> MemStore for RobinhoodMemStore<T>
where
    T: MemStoreValue,
{
    #[inline]
    fn get_item_length(&self) -> usize {
        std::mem::size_of::<T>()
    }

    fn local_get_meta(&self, key: u64) -> Option<MemNodeMeta> {
        let mut ret: Option<MemNodeMeta> = Some(MemNodeMeta::new(0, 0));

        let table = self.table.read().unwrap();

        match table.get(key) {
            Some(node) => {
                ret = Some(MemNodeMeta::new(node.get_lock(), node.get_seq()));
            }
            None => {}
        }

        ret
    }

    fn local_get_readonly(&self, key: u64, ptr: *mut u8, len: u32) -> Option<MemNodeMeta> {
        if std::mem::size_of::<T>() > len as usize {
            panic!("get length is not rational!");
        }

        let value = unsafe { (ptr as *mut T).as_mut().unwrap() };
        let mut ret: Option<MemNodeMeta> = Some(MemNodeMeta::new(0, 0));

        let table = self.table.read().unwrap();

        match table.get(key) {
            Some(node) => {
                *value = node.get_value().clone();
                ret = Some(MemNodeMeta::new(node.get_lock(), node.get_seq()));
            }
            None => {}
        }

        ret
    }

    fn local_get_for_upd(
        &self,
        key: u64,
        ptr: *mut u8,
        len: u32,
        lock_content: u64,
    ) -> Option<MemNodeMeta> {
        if std::mem::size_of::<T>() > len as usize {
            panic!("get length is not rational!");
        }

        let value = unsafe { (ptr as *mut T).as_mut().unwrap() };
        let mut ret: Option<MemNodeMeta> = Some(MemNodeMeta::new(0, 0));

        let table = self.table.read().unwrap();

        match table.get(key) {
            Some(node) => {
                if node.try_lock(lock_content) {
                    *value = node.get_value().clone();
                }
                ret = Some(MemNodeMeta::new(node.get_lock(), node.get_seq()));
            }
            None => {}
        }

        ret
    }

    fn local_lock(&self, key: u64, lock_content: u64) -> Option<MemNodeMeta> {
        let mut ret: Option<MemNodeMeta> = Some(MemNodeMeta::new(0, 0));

        let mut need_insert = false;
        
        let table = self.table.read().unwrap();

        match table.get(key) {
            Some(node) => {
                node.try_lock(lock_content);
                ret = Some(MemNodeMeta::new(node.get_lock(), node.get_seq()));
            }
            None => {
                need_insert = true;
            }
        }

        drop(table);

        if !need_insert {
            return ret;
        }

        let table = self.table.write().unwrap();

        match table.get(key) {
            Some(node) => {
                node.try_lock(lock_content);
                ret = Some(MemNodeMeta::new(node.get_lock(), node.get_seq()));
            }
            None => {
                let node = MemNode::new_zero(lock_content, 2);
                table.put(key, &node);
                ret = Some(MemNodeMeta::new(lock_content, 2))
            }
        }

        ret
    }

    fn local_unlock(&self, key: u64, lock_content: u64) -> Option<MemNodeMeta> {
        let mut ret: Option<MemNodeMeta> = Some(MemNodeMeta::new(0, 0));

        let table = self.table.read().unwrap();
        match table.get(key) {
            Some(node) => {
                node.try_unlock(lock_content);
                ret = Some(MemNodeMeta::new(node.get_lock(), node.get_seq()));
            }
            None => {}
        }

        ret
    }

    fn local_upd_val_seq(&self, key: u64, ptr: *const u8, len: u32) -> Option<MemNodeMeta> {
        let mut ret: Option<MemNodeMeta> = Some(MemNodeMeta::new(0, 0));

        if std::mem::size_of::<T>() > len as usize {
            panic!("upd length is not rational!");
        }
        let table = self.table.read().unwrap();

        let value = unsafe { (ptr as *const T).as_ref().unwrap() };

        match table.get(key) {
            Some(node) => {
                node.set_value(value);
                node.advance_seq();
                ret = Some(MemNodeMeta::new(node.get_lock(), node.get_seq()));
            }
            None => {}
        }

        ret
    }

    fn local_erase(&self, key: u64) -> Option<MemNodeMeta> {
        let mut ret: Option<MemNodeMeta> = Some(MemNodeMeta::new(0, 0));
        let table = self.table.write().unwrap();

        match table.erase(key) {
            Some(node) => {
                ret = Some(MemNodeMeta::new(node.get_lock(), node.get_seq()));
            }
            None => {}
        }

        ret
    }
}
