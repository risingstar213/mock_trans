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
    rwlock: AtomicU32,
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
            rwlock: AtomicU32::new(0),
            table:  UnsafeCell::new(RobinHood::new(ROBINHOOD_DIB_MAX))
        }
    }
    
    pub fn rlock(&self) {
        let start_time = SystemTime::now();
        loop {

            let old = self.rwlock.load(Ordering::SeqCst);
            let mut meta = VersionRwLock::from_raw(old);

            let now_time = SystemTime::now();
            let duration = now_time.duration_since(start_time).unwrap();
            if duration.as_millis() > 1000 && duration.as_millis() % 1000 == 0 {
                println!("read may be dead lock! read:{}, write:{}", meta.read_flag, meta.write_flag);
            }

            if meta.write_flag == 0 {
                meta.read_flag += 1;
                meta.version += 1;
                if meta.version >= 1 << 20 {
                    meta.version = 0;
                }

                match self.rwlock.compare_exchange(
                    old,
                    meta.to_raw(),
                    Ordering::SeqCst,
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
    }

    pub fn runlock(&self) {
        loop {
            let old = self.rwlock.load(Ordering::SeqCst);
            let mut meta = VersionRwLock::from_raw(old);

            if meta.write_flag != 0 {
                panic!("not rational")
            }

            meta.read_flag -= 1;
            meta.version += 1;
            if meta.version >= 1 << 20 {
                meta.version = 0;
            }

            match self.rwlock.compare_exchange(
                old,
                meta.to_raw(),
                Ordering::SeqCst,
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
        let start_time = SystemTime::now();
        loop {
            let old = self.rwlock.load(Ordering::SeqCst);
            let mut meta = VersionRwLock::from_raw(old);

            let now_time = SystemTime::now();
            let duration = now_time.duration_since(start_time).unwrap();
            if duration.as_millis() > 1000 && duration.as_millis() % 1000 == 0 {
                println!("write may be dead lock!");
            }

            if meta.read_flag == 0 {
                meta.write_flag = 1;
                meta.version += 1;
                if meta.version >= 1 << 20 {
                    meta.version = 0;
                }

                match self.rwlock.compare_exchange(
                    old,
                    meta.to_raw(),
                    Ordering::SeqCst,
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

        println!("wlock !!!");
    }

    pub fn wunlock(&self) {
        loop {
            let old = self.rwlock.load(Ordering::SeqCst);
            let mut meta = VersionRwLock::from_raw(old);

            if meta.read_flag != 0 {
                panic!("not rational")
            }

            meta.write_flag = 0;
            meta.version += 1;
            if meta.version >= 1 << 20 {
                meta.version = 0;
            }

            match self.rwlock.compare_exchange(
                old,
                meta.to_raw(),
                Ordering::SeqCst,
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
