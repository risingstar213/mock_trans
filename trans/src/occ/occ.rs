use crate::memstore::MemStoreValue;

use super::rwset::{RwSet, RwType};

pub trait MemStoreItemEnum: Default {
    fn from_raw<T: MemStoreValue>(value: T) -> Self;
    fn get_inner<T: MemStoreValue>(&self) -> T;
}

#[derive(PartialEq, Eq)]
pub enum OCCStatus {
    OCC_UNINIT,
    OCC_INPROGRESS,
    OCC_COMMITED,
    OCC_ABORTED
}

pub trait OCC {
    fn read<T: MemStoreValue>(&mut self, table_id: usize, key: u64) -> usize;

    // fetch for write
    fn fetch_write<T: MemStoreValue>(&mut self, table_id: usize, key: u64, lock_content: u64) -> usize;

    fn write<T: MemStoreValue>(&mut self, table_id: usize, key: u64, lock_content: u64, rwtype: RwType) -> usize;

    fn is_aborted(&self) -> bool;
}

