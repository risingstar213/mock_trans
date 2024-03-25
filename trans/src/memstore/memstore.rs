use std::sync::atomic::AtomicU64;

#[repr(C)]
pub struct MemNode {
    lock: AtomicU64,
    seq:  u64,
}

pub trait MemStore {
    fn get(&self);
    fn put(&mut self);
}