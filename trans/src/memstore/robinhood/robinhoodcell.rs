use byte_struct::*;
use std::cell::{Cell, UnsafeCell};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::RwLock;
use std::time::SystemTime;
use std::time::Duration;

use super::super::memstore::{MemNode, MemStoreValue};
use super::robinhood::RobinHood;

use crate::{ROBINHOOD_SIZE, ROBINHOOD_DIB_MAX};

bitfields!(
    pub VersionRwLock: u32 {
        pub version:    22,
        pub write_flag: 1,
        pub read_flag:  9,
    }
);

/// | 31 ... 10 | 9 | 8 ... 0 |
/// TODO: using htm to ensure consistence
/// UnsafeCell is in need
pub struct RobinHoodTableCell<T>
where
    T: MemStoreValue,
{
    table: UnsafeCell<RobinHood<u64, MemNode<T>, ROBINHOOD_SIZE>>,
}

unsafe impl<T> Send for RobinHoodTableCell<T> where T: MemStoreValue {}
unsafe impl<T> Sync for RobinHoodTableCell<T> where T: MemStoreValue {}

impl<T> RobinHoodTableCell<T>
where
    T: MemStoreValue,
{
    pub fn new() -> Self {
        Self {
            table:  UnsafeCell::new(RobinHood::new(ROBINHOOD_DIB_MAX))
        }
    }

    pub fn get(&self, key: u64) -> Option<&MemNode<T>> {
        let ref_table = unsafe { self.table.get().as_ref().unwrap() };

        ref_table.get(&key)
    }

    pub fn put(&self, key: u64, value: &MemNode<T>) {
        let refmut_table = unsafe { self.table.get().as_mut().unwrap() };

        refmut_table.put(&key, &value);
    }

    pub fn erase(&self, key: u64) -> Option<MemNode<T>> {
        let refmut_table = unsafe { self.table.get().as_mut().unwrap() };

        refmut_table.erase(&key)
    }
}
