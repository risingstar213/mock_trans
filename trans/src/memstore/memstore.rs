use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

#[repr(C)]
pub struct MemNode<T>
where 
    T: Clone + Send + Sync
{
    lock:  AtomicU64,
    seq:   u64,
    value: T,
}

impl<T> Clone for MemNode<T>
where
    T: Clone + Send + Sync
{
    fn clone(&self) -> Self {
        Self {
            lock:  AtomicU64::from(self.lock.load(Ordering::Acquire)),
            seq:   self.seq,
            value: self.value.clone(),
        }
    }
}

impl<T> MemNode<T>
where
    T: Clone + Send + Sync
{
    pub fn try_lock_up(&self, lock_sig: u64) -> bool {
        match self.lock.compare_exchange(
            0, 
            lock_sig, 
            Ordering::Release,
            Ordering::Relaxed
        ) {
            Ok(_) => {
                return true;
            },
            Err(_) => {
                return false;
            }
        }
    }

    pub fn try_release_op(&self, lock_sig: u64) -> bool {
        match self.lock.compare_exchange(
            lock_sig, 
            0, 
            Ordering::Release, 
            Ordering::Relaxed
        ) {
            Ok(_) => {
                return true;
            },
            Err(_) => {
                return false;
            }
        }
    }
}

pub trait MemStore {
    fn get(&self);
    fn put(&mut self);
}