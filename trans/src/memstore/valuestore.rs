use std::sync::RwLock;
use std::cell::UnsafeCell;

use super::robinhood::robinhood::RobinHood;
use super::memstore::MemStoreValue;

use crate::{ROBINHOOD_SIZE, ROBINHOOD_DIB_MAX};

pub trait ValueStore {
    fn get_item_length(&self) -> usize;
    fn local_get_value(&self, key: u64, ptr: *mut u8, len: u32) -> bool;
    fn local_set_value(&self, key: u64, ptr: *const u8, len: u32);
    fn local_put_value(&self, key: u64, ptr: *const u8, len: u32);
    fn local_erase_value(&self, key: u64);
}

struct UnsafeValue<T: MemStoreValue>(UnsafeCell<T>);

impl<T> Clone for UnsafeValue<T>
where
    T: MemStoreValue,
{
    fn clone(&self) -> Self {
        Self {
            0: UnsafeCell::new(unsafe { (*self.0.get()).clone() }),
        }
    }
}

impl<T> Default for UnsafeValue<T>
where
    T: MemStoreValue,
{
    fn default() -> Self {
        Self {
            0: UnsafeCell::new(T::default()),
        }
    }
}

impl<T> UnsafeValue<T>
where
    T: MemStoreValue
{
    pub fn set_value(&self, value: &T) {
        unsafe {
            *self.0.get() = value.clone();
        }
    }
    
    pub fn get_value(&self) -> &T {
        unsafe { self.0.get().as_mut().unwrap() }
    }
}


unsafe impl<T> Send for UnsafeValue<T> where T: MemStoreValue {}
unsafe impl<T> Sync for UnsafeValue<T> where T: MemStoreValue {}


pub struct RobinhoodValueStore<T>
where
    T: MemStoreValue
{
    table: RwLock<RobinHood<u64, UnsafeValue<T>, ROBINHOOD_SIZE>>,
}

impl<T> RobinhoodValueStore<T> 
where
    T: MemStoreValue
{
    pub fn new() -> Self {
        Self {
            table: RwLock::new(RobinHood::new(ROBINHOOD_DIB_MAX)),
        }
    }
}

impl<T> ValueStore for RobinhoodValueStore<T> 
where
    T: MemStoreValue
{
    #[inline]
    fn get_item_length(&self) -> usize {
        std::mem::size_of::<T>()
    }

    fn local_get_value(&self, key: u64, ptr: *mut u8, len: u32) -> bool {
        if std::mem::size_of::<T>() > len as usize {
            panic!("get length is not rational!");
        }

        let value = unsafe { (ptr as *mut T).as_mut().unwrap() };

        let table = self.table.read().unwrap();

        match table.get(&key) {
            Some(v) => {
                *value = v.get_value().clone();
                return true;
            }
            None => {
                return false;
            }
        }
    }

    fn local_set_value(&self, key: u64, ptr: *const u8, len: u32) {
        if std::mem::size_of::<T>() > len as usize {
            panic!("get length is not rational!");
        }

        let table = self.table.read().unwrap();

        let value = unsafe { (ptr as *const T).as_ref().unwrap() };

        match table.get(&key) {
            Some(node) => {
                node.set_value(value);
            }
            None => {}
        }
    }

    fn local_put_value(&self, key: u64, ptr: *const u8, len: u32) {
        if std::mem::size_of::<T>() > len as usize {
            panic!("get length is not rational!");
        }
        let mut table = self.table.write().unwrap();
    
        let value = unsafe { (ptr as *const T).as_ref().unwrap() };

        match table.get(&key) {
            Some(_) => {}
            None => {
                let cell = UnsafeValue::default();
                cell.set_value(value);
                table.put(&key, &cell);
            }
        }
    }

    fn local_erase_value(&self, key: u64) {
        let mut table = self.table.write().unwrap();
        match table.erase(&key) {
            Some(_) => {}
            None => {}
        }
    }
}