use std::collections::HashMap;

use super::memstore::{MemStore, MemNode};
use super::robinhood::RobinHood;

pub struct HashTableMemStore
{
    // table: RobinHood<u64, MemNode>,
}

impl MemStore for HashTableMemStore {
    fn get(&self) {
        
    }

    fn put(&mut self) {

    }
}