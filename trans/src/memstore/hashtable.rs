use std::collections::HashMap;
use std::sync::atomic::AtomicPtr;

use super::memstore::{MemStore, MemNode};
use super::robinhood::RobinHood;

pub struct HashTableMemStore<T>
where 
    T: Clone + Send + Sync
{
    table: RobinHood<u64, MemNode<T>>,
}

impl<T> MemStore for HashTableMemStore<T>
where
    T: Clone + Send + Sync
{
    fn get(&self) {
        
    }

    fn put(&mut self) {

    }
}