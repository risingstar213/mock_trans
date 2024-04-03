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
    pub(crate) part_id:  u64,
    pub(crate) rwtype:   RwType,
    pub(crate) key:      u64,
    pub(crate) value:    MemStoreItemEnum<ITEM_MAX_SIZE>,
    // pub(crate) lock:     u64,
    pub(crate) seq:      u64,
}

impl<const ITEM_MAX_SIZE: usize> RwItem<ITEM_MAX_SIZE>
{
    pub fn new_zero(table_id: usize, part_id: u64, rwtype: RwType, key: u64) -> Self {
        Self {
            table_id: table_id,
            part_id:  part_id,
            rwtype:   rwtype,
            key:      key,
            value:    MemStoreItemEnum::default(),
            seq:      0,
        }
    }
    
    pub fn new(table_id: usize, part_id: u64, rwtype: RwType, key: u64, value: MemStoreItemEnum<ITEM_MAX_SIZE>, seq: u64) -> Self {
        Self {
            table_id: table_id,
            part_id:  part_id,
            rwtype:   rwtype,
            key:      key,
            value:    value,
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