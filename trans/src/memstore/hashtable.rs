use super::hashtablecell::HashTableCell;
use super::memstore::{MemNode, MemNodeMeta, MemStore, MemStoreValue};

pub struct HashTableMemStore<T>
where
    T: MemStoreValue,
{
    table: HashTableCell<T>,
}

impl<T> HashTableMemStore<T> 
where 
    T: MemStoreValue 
{
    pub fn new() -> Self {
        Self {
            table: HashTableCell::<T>::new()
        }
    }
}

impl<T> MemStore for HashTableMemStore<T>
where
    T: MemStoreValue,
{
    fn lock_shared(&self) {
        self.table.rlock();
    }
    fn unlock_shared(&self) {
        self.table.runlock();
    }
    fn lock_exclusive(&self) {
        self.table.wlock();
    }
    fn unlock_exclusive(&self) {
        self.table.wunlock();
    }

    fn local_get_meta(&self, key: u64) -> Option<MemNodeMeta> {
        let mut ret: Option<MemNodeMeta> = None;

        self.table.rlock();

        match self.table.get(key) {
            Some(node) => {
                ret = Some(MemNodeMeta::new(node.get_lock(), node.get_seq()));
            }
            None => {}
        }

        self.table.runlock();
        ret
    }

    fn local_get_readonly(&self, key: u64, ptr: *mut u8, len: u32) -> Option<MemNodeMeta> {
        if std::mem::size_of::<T>() > len as usize {
            panic!("get length is not rational!");
        }

        let value = unsafe { (ptr as *mut T).as_mut().unwrap() };
        let mut ret: Option<MemNodeMeta> = None;

        self.table.rlock();

        match self.table.get(key) {
            Some(node) => {
                *value = node.get_value().clone();
                ret = Some(MemNodeMeta::new(node.get_lock(), node.get_seq()));
            }
            None => {}
        }

        self.table.runlock();
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
        let mut ret: Option<MemNodeMeta> = None;

        self.table.rlock();

        match self.table.get(key) {
            Some(node) => {
                node.try_lock(lock_content);
                *value = node.get_value().clone();
                ret = Some(MemNodeMeta::new(node.get_lock(), node.get_seq()));
            }
            None => {}
        }

        self.table.runlock();
        ret
    }

    fn local_lock(&self, key: u64, lock_content: u64) -> Option<MemNodeMeta> {
        let ret: Option<MemNodeMeta>;

        self.table.wlock();

        match self.table.get(key) {
            Some(node) => {
                node.try_lock(lock_content);
                ret = Some(MemNodeMeta::new(node.get_lock(), node.get_seq()));
            }
            None => {
                let node = MemNode::new_zero(lock_content, 2);
                self.table.put(key, &node);
                ret = Some(MemNodeMeta::new(lock_content, 2))
            }
        }

        self.table.wunlock();
        ret
    }

    fn local_unlock(&self, key: u64, lock_content: u64) -> Option<MemNodeMeta> {
        let mut ret: Option<MemNodeMeta> = None;

        self.table.rlock();
        match self.table.get(key) {
            Some(node) => {
                node.try_unlock(lock_content);
                ret = Some(MemNodeMeta::new(node.get_lock(), node.get_seq()));
            }
            None => {}
        }

        self.table.runlock();
        ret
    }

    fn local_upd_val_seq(&self, key: u64, ptr: *const u8, len: u32) -> Option<MemNodeMeta> {
        let mut ret: Option<MemNodeMeta> = None;

        if std::mem::size_of::<T>() > len as usize {
            panic!("upd length is not rational!");
        }
        self.table.rlock();

        let value = unsafe { (ptr as *const T).as_ref().unwrap() };

        match self.table.get(key) {
            Some(node) => {
                node.set_value(value);
                node.advance_seq();
                ret = Some(MemNodeMeta::new(node.get_lock(), node.get_seq()));
            }
            None => {}
        }

        self.table.runlock();
        ret
    }

    fn local_erase(&self, key: u64) -> Option<MemNodeMeta> {
        let mut ret: Option<MemNodeMeta> = None;
        self.table.wlock();

        match self.table.erase(key) {
            Some(node) => {
                ret = Some(MemNodeMeta::new(node.get_lock(), node.get_seq()));
            }
            None => {}
        }

        self.table.wunlock();
        ret
    }
}
