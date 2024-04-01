use crate::memstore::MemStoreValue;

use super::rwset::{RwSet, RwType};

// pub trait MemStoreItemEnum: Default {
//     fn from_raw<T: MemStoreValue>(value: T) -> Self;
//     fn get_inner<T: MemStoreValue>(&self) -> &T;
//     fn set_inner<T: MemStoreValue>(&mut self, value: &T);
//     fn get_raw_ptr(&mut self) -> *mut u8;
//     fn get_len(&mut self) -> u32;
// }

// How to fix this type of dynamic dispatch elegently?
pub struct MemStoreItemEnum<const ITEM_MAX_SIZE: usize> {
    inner: [u8; ITEM_MAX_SIZE],
}

impl<const SIZE: usize> Default for MemStoreItemEnum<SIZE> {
    fn default() -> Self {
        Self {
            inner: unsafe { std::mem::zeroed() }
        }
    }
}

impl<const ITEM_MAX_SIZE: usize> MemStoreItemEnum<ITEM_MAX_SIZE> {
    pub fn from_raw<T: MemStoreValue>(value: T) -> Self {
        if ITEM_MAX_SIZE < std::mem::size_of::<T>() {
            panic!("not rational!");
        }
        let mut item = Self::default();
        let ref_mut = unsafe { (&mut item.inner as *mut u8 as *mut T).as_mut().unwrap() };
        *ref_mut = value;
        item
    }

    pub fn get_inner<T: MemStoreValue>(&self) -> &T {
        if ITEM_MAX_SIZE < std::mem::size_of::<T>() {
            panic!("not rational!");
        }
        let ref_mut = unsafe{ (&self.inner as *const u8 as *const T).as_ref().unwrap()};
        return ref_mut;
    }

    pub fn set_inner<T: MemStoreValue>(&mut self, value: &T) {
        if ITEM_MAX_SIZE < std::mem::size_of::<T>() {
            panic!("not rational!");
        }

        let ref_mut = unsafe { (&mut self.inner as *mut u8 as *mut T).as_mut().unwrap() };
        *ref_mut = value.clone();
    }

    pub fn get_raw_ptr(&mut self) -> *mut u8 {
        return &mut self.inner as *mut u8;
    }
}

#[derive(PartialEq, Eq)]
pub enum OccStatus {
    OccUnint,
    OccInprogress,
    OccMustabort,
    OccCommited,
    OccAborted
}

pub trait OccExecute {
    fn lock_writes(&mut self);

    fn validate(&mut self);

    fn log_write(&mut self);

    fn commit_write(&mut self);

    fn unlock(&mut self);

    fn recover_on_aborted(&mut self);
}

pub trait Occ {
    fn start(&mut self);

    fn read<T: MemStoreValue>(&mut self, table_id: usize, key: u64) -> usize;

    // fetch for write
    fn fetch_write<T: MemStoreValue>(&mut self, table_id: usize, key: u64, lock_content: u64) -> usize;

    fn write<T: MemStoreValue>(&mut self, table_id: usize, key: u64, lock_content: u64, rwtype: RwType) -> usize;

    fn get_value<T: MemStoreValue>(&mut self, update: bool, idx: usize) -> &T;

    fn set_value<T: MemStoreValue>(&mut self, update: bool, idx: usize, value: &T);

    fn commit(&mut self);

    fn abort(&mut self);

    fn is_aborted(&self) -> bool;

    fn is_commited(&self) -> bool;
}

