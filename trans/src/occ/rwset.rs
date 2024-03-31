use crate::memstore::MemStoreValue;
use super::occ::MemStoreItemEnum;
pub enum RwType {
    READ,
    INSERT,
    UPDATE,
    ERASE,
}

pub struct RwItem<const ITEM_MAX_SIZE: usize>
{
    pub(crate) table_id: usize,
    pub(crate) rwtype:   RwType,
    pub(crate) key:      u64,
    pub(crate) value:    MemStoreItemEnum<ITEM_MAX_SIZE>,
    pub(crate) lock:     u64,
    pub(crate) seq:      u64,
}

impl<const ITEM_MAX_SIZE: usize> RwItem<ITEM_MAX_SIZE>
{
    pub fn new_zero(table_id: usize, rwtype: RwType, key: u64) -> Self {
        Self {
            table_id: table_id,
            rwtype:   rwtype,
            key:      key,
            value:    MemStoreItemEnum::default(),
            lock:     0,
            seq:      0,
        }
    }
    
    pub fn new(table_id: usize, rwtype: RwType, key: u64, value: MemStoreItemEnum<ITEM_MAX_SIZE>, lock: u64, seq: u64) -> Self {
        Self {
            table_id: table_id,
            rwtype:   rwtype,
            key:      key,
            value:    value,
            lock:     lock,
            seq:      seq
        }
    }

    // pub fn get_value_inner<T: MemStoreValue>(&mut self) -> T {
    //     self.value.get_inner::<T>()
    // }
}
pub struct RwSet<const ITEM_MAX_SIZE: usize> 
{
    items: Vec<RwItem<ITEM_MAX_SIZE>>,
}

impl<const ITEM_MAX_SIZE: usize> RwSet<ITEM_MAX_SIZE>
{
    pub fn new() -> Self {
        Self {
            items: Vec::new(),
        }
    }

    pub fn push(&mut self, item: RwItem<ITEM_MAX_SIZE>) {
        self.items.push(item);
    }

    pub fn bucket(&mut self, idx: usize) -> &mut RwItem<ITEM_MAX_SIZE> {
        return &mut self.items[idx];
    }

    #[inline]
    pub fn get_len(&self) -> usize {
        self.items.len()
    }
}