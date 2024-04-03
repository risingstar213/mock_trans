use byte_struct::*;

use crate::memstore::MemStoreValue;

use super::rwset::{RwSet, RwType};

bitfields!(
    pub LockContent: u64 {
        pub peer_id: 54,
        pub cid:     10,
    }
);

impl LockContent {
    pub fn new(peer_id: u64, cid: u32) -> Self {
        Self {
            peer_id: peer_id,
            cid:     cid as _,
        }
    }

    pub fn to_content(&self) -> u64 {
        self.to_raw()
    }

    pub fn from_content(content: u64) -> Self {
        Self::from_raw(content)
    }
}

// How to fix this type of dynamic dispatch elegently?
pub struct MemStoreItemEnum<const ITEM_MAX_SIZE: usize> {
    length: u32,
    inner: [u8; ITEM_MAX_SIZE],
}

impl<const SIZE: usize> Default for MemStoreItemEnum<SIZE> {
    fn default() -> Self {
        Self {
            length: 0,
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
        item.length = std::mem::size_of::<T>() as u32;

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
        self.length = std::mem::size_of::<T>() as u32;

        let ref_mut = unsafe { (&mut self.inner as *mut u8 as *mut T).as_mut().unwrap() };
        *ref_mut = value.clone();
    }

    pub fn get_raw_ptr(&mut self) -> *mut u8 {
        return &mut self.inner as *mut u8;
    }

    pub fn get_length(&self) -> u32 {
        return self.length;
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

    fn read<T: MemStoreValue>(&mut self, table_id: usize, part_id: u64, key: u64) -> usize;

    // fetch for write
    fn fetch_write<T: MemStoreValue>(&mut self, table_id: usize, part_id: u64, key: u64) -> usize;

    fn write<T: MemStoreValue>(&mut self, table_id: usize, part_id: u64, key: u64, rwtype: RwType) -> usize;

    fn get_value<T: MemStoreValue>(&mut self, update: bool, idx: usize) -> &T;

    fn set_value<T: MemStoreValue>(&mut self, update: bool, idx: usize, value: &T);

    fn commit(&mut self);

    fn abort(&mut self);

    fn is_aborted(&self) -> bool;

    fn is_commited(&self) -> bool;
}

