use byte_struct::*;
use std::borrow::Borrow;
use std::borrow::BorrowMut;
use std::cell::{Cell, UnsafeCell};
use std::sync::atomic::{AtomicU32, Ordering};

use super::memstore::{MemNode, MemStoreValue};
use super::robinhood::RobinHood;

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
pub struct HashTableCell<T>
where
    T: MemStoreValue,
{
    rwlock: AtomicU32,
    table: UnsafeCell<RobinHood<u64, MemNode<T>>>,
}

unsafe impl<T> Send for HashTableCell<T> where T: MemStoreValue {}
unsafe impl<T> Sync for HashTableCell<T> where T: MemStoreValue {}

impl<T> HashTableCell<T>
where
    T: MemStoreValue,
{
    pub fn rlock(&self) {
        loop {
            let old = self.rwlock.load(Ordering::Acquire);
            let mut meta = VersionRwLock::from_raw(old);

            if meta.write_flag == 0 {
                meta.read_flag += 1;
                meta.version += 1;

                match self.rwlock.compare_exchange(
                    old,
                    meta.to_raw(),
                    Ordering::Release,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        return;
                    }
                    Err(_) => {
                        continue;
                    }
                }
            }
        }
    }

    pub fn runlock(&self) {
        loop {
            let old = self.rwlock.load(Ordering::Acquire);
            let mut meta = VersionRwLock::from_raw(old);

            if meta.write_flag != 0 {
                panic!("not rational")
            }

            meta.read_flag -= 1;
            meta.version += 1;

            match self.rwlock.compare_exchange(
                old,
                meta.to_raw(),
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    break;
                }
                Err(_) => {
                    continue;
                }
            }
        }
    }

    pub fn wlock(&self) {
        loop {
            let old = self.rwlock.load(Ordering::Acquire);
            let mut meta = VersionRwLock::from_raw(old);

            if meta.read_flag == 0 {
                meta.read_flag = 1;
                meta.version += 1;

                match self.rwlock.compare_exchange(
                    old,
                    meta.to_raw(),
                    Ordering::Release,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        return;
                    }
                    Err(_) => {
                        continue;
                    }
                }
            }
        }
    }

    pub fn wunlock(&self) {
        loop {
            let old = self.rwlock.load(Ordering::Acquire);
            let mut meta = VersionRwLock::from_raw(old);

            if meta.read_flag != 0 {
                panic!("not rational")
            }

            meta.write_flag = 0;
            meta.version += 1;

            match self.rwlock.compare_exchange(
                old,
                meta.to_raw(),
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    break;
                }
                Err(_) => {
                    continue;
                }
            }
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
