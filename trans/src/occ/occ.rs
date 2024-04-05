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

    pub fn get_raw_ptr(&self) -> *const u8 {
        return &self.inner as *const u8;
    }

    pub fn get_length(&self) -> u32 {
        return self.length;
    }

    pub fn set_raw_data(&mut self, ptr: *const u8, len: u32) {
        unsafe {
            let dst = &mut self.inner as *mut _;
            std::ptr::copy_nonoverlapping(ptr, dst, len as _);
        }

        self.length = len;
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


