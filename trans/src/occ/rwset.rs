use crate::memstore::MemStoreValue;
use super::occ::MemStoreItemEnum;
pub enum RwType {
    READ,
    INSERT,
    UPDATE,
    ERASE,
}

pub struct RwItem<E>
where
    E: MemStoreItemEnum
{
    table_id: usize,
    rwtype:   RwType,
    key:      u64,
    value:    E,
    lock:     u64,
    seq:      u64,
}

impl<E> RwItem<E>
where
    E: MemStoreItemEnum
{
    pub fn new_zero(table_id: usize, rwtype: RwType, key: u64) -> Self {
        Self {
            table_id: table_id,
            rwtype:   rwtype,
            key:      key,
            value:    E::default(),
            lock:     0,
            seq:      0,
        }
    }
    
    pub fn new(table_id: usize, rwtype: RwType, key: u64, value: E, lock: u64, seq: u64) -> Self {
        Self {
            table_id: table_id,
            rwtype:   rwtype,
            key:      key,
            value:    value,
            lock:     lock,
            seq:      seq
        }
    }

    pub fn get_value_inner<T: MemStoreValue>(&mut self) -> T {
        self.value.get_inner::<T>()
    }
}
pub struct RwSet<E> 
where
    E: MemStoreItemEnum
{
    items: Vec<RwItem<E>>,
}

impl<E> RwSet<E>
where
    E: MemStoreItemEnum
{
    pub fn new() -> Self {
        Self {
            items: Vec::new(),
        }
    }

    pub fn push(&mut self, item: RwItem<E>) {
        self.items.push(item);
    }

    #[inline]
    pub fn get_len(&self) -> usize {
        self.items.len()
    }
}