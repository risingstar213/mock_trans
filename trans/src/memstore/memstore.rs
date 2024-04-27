use std::cell::UnsafeCell;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

// just marker trait
pub trait MemStoreValue: Clone + Send + Sync + Default {}

impl<T> MemStoreValue for T
where
    T: Clone + Send + Sync + Default 
{}

#[repr(C)]
pub struct MemNode<T>
where
    T: MemStoreValue,
{
    lock: AtomicU64,
    seq: AtomicU64,
    // temporary
    value: UnsafeCell<T>,
}

unsafe impl<T> Send for MemNode<T> where T: MemStoreValue {}
unsafe impl<T> Sync for MemNode<T> where T: MemStoreValue {}

impl<T> Clone for MemNode<T>
where
    T: MemStoreValue,
{
    fn clone(&self) -> Self {
        Self {
            lock: AtomicU64::from(self.lock.load(Ordering::Acquire)),
            seq: AtomicU64::from(self.seq.load(Ordering::Acquire)),
            value: UnsafeCell::new(unsafe { (*self.value.get()).clone() }),
        }
    }
}

impl<T> Default for MemNode<T>
where
    T: MemStoreValue,
{
    fn default() -> Self {
        Self {
            lock: AtomicU64::from(0),
            seq: AtomicU64::from(0),
            value: UnsafeCell::new(T::default()),
        }
    }
}

impl<T> MemNode<T>
where
    T: MemStoreValue,
{
    pub fn new(lock: u64, seq: u64, value: &T) -> Self {
        Self {
            lock: AtomicU64::from(lock),
            seq: AtomicU64::from(seq),
            value: UnsafeCell::new(value.clone()),
        }
    }

    pub fn new_zero(lock: u64, seq: u64) -> Self {
        Self {
            lock: AtomicU64::from(lock),
            seq: AtomicU64::from(seq),
            value: UnsafeCell::new(T::default()),
        }
    }

    pub fn try_lock(&self, lock_sig: u64) -> bool {
        match self
            .lock
            .compare_exchange(0, lock_sig, Ordering::Release, Ordering::Relaxed)
        {
            Ok(_) => {
                return true;
            }
            Err(_) => {
                return false;
            }
        }
    }

    pub fn try_unlock(&self, lock_sig: u64) -> bool {
        match self
            .lock
            .compare_exchange(lock_sig, 0, Ordering::Release, Ordering::Relaxed)
        {
            Ok(_) => {
                return true;
            }
            Err(_) => {
                return false;
            }
        }
    }

    pub fn advance_seq(&self) {
        self.seq.fetch_add(2, Ordering::AcqRel);
    }

    pub fn set_value(&self, value: &T) {
        unsafe {
            *self.value.get() = value.clone();
        }
    }

    pub fn get_lock(&self) -> u64 {
        self.lock.load(Ordering::Acquire)
    }

    pub fn get_seq(&self) -> u64 {
        self.seq.load(Ordering::Acquire)
    }

    pub fn get_value(&self) -> &T {
        unsafe { self.value.get().as_mut().unwrap() }
    }
}

pub struct MemNodeMeta {
    pub lock: u64,
    pub seq: u64,
}

impl MemNodeMeta {
    pub fn new(lock: u64, seq: u64) -> Self {
        Self {
            lock: lock,
            seq: seq,
        }
    }
}

pub trait MemStore {
    // for remote operation
    fn get_item_length(&self) -> usize;

    fn local_get_meta(&self, key: u64) -> Option<MemNodeMeta>;
    fn local_get_readonly(&self, key: u64, ptr: *mut u8, len: u32) -> Option<MemNodeMeta>;
    fn local_get_for_upd(
        &self,
        key: u64,
        ptr: *mut u8,
        len: u32,
        lock_content: u64,
    ) -> Option<MemNodeMeta>;
    fn local_lock(&self, key: u64, lock_content: u64) -> Option<MemNodeMeta>;
    fn local_unlock(&self, key: u64, lock_content: u64) -> Option<MemNodeMeta>;
    // update or insert
    fn local_upd_val_seq(&self, key: u64, ptr: *const u8, len: u32) -> Option<MemNodeMeta>;
    fn local_erase(&self, key: u64) -> Option<MemNodeMeta>;
}
