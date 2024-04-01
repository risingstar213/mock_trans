use std::sync::Arc;

use crate::memstore::memdb::MemDB;
use crate::memstore::MemStoreValue;

use super::occ::{OccStatus, OccExecute, Occ};
use super::rwset::{RwSet, RwType};

pub struct OccRdma<'trans, const MAX_ITEM_SIZE: usize>
{
    status:    OccStatus,
    memdb:     Arc<MemDB<'trans>>,
    readset:   RwSet<MAX_ITEM_SIZE>,
    updateset: RwSet<MAX_ITEM_SIZE>,
    writeset:  RwSet<MAX_ITEM_SIZE>
}

impl<'trans, const MAX_ITEM_SIZE: usize> OccRdma<'trans, MAX_ITEM_SIZE>
{
    pub fn new(memdb: &Arc<MemDB<'trans>>) -> Self {
        Self {
            status:    OccStatus::OccUnint,
            memdb:     memdb.clone(),
            readset:   RwSet::new(),
            updateset: RwSet::new(),
            writeset:  RwSet::new(),
        }
    }

}

impl<'trans, const MAX_ITEM_SIZE: usize> OccExecute for OccRdma<'trans, MAX_ITEM_SIZE>
{
    fn lock_writes(&mut self) {
        
    }

    fn validate(&mut self) {
        
    }

    fn log_write(&mut self) {
        
    }
    
    fn commit_write(&mut self) {
        
    }

    fn unlock(&mut self) {
        
    }

    fn recover_on_aborted(&mut self) {
        
    }
}

impl<'trans, const MAX_ITEM_SIZE: usize> Occ for OccRdma<'trans, MAX_ITEM_SIZE>
{
    fn start(&mut self) {
        unimplemented!()
    }

    fn read<T: MemStoreValue>(&mut self, table_id: usize, key: u64) -> usize {
        unimplemented!()
    }

    // fetch for write
    fn fetch_write<T: MemStoreValue>(&mut self, table_id: usize, key: u64, lock_content: u64) -> usize {
        unimplemented!()
    }

    fn write<T: MemStoreValue>(&mut self, table_id: usize, key: u64, lock_content: u64, rwtype: RwType) -> usize {
        unimplemented!()
    }

    fn get_value<T: MemStoreValue>(&mut self, update: bool, idx: usize) -> &T {
        unimplemented!()
    }

    fn set_value<T: MemStoreValue>(&mut self, update: bool, idx: usize, value: &T) {
        unimplemented!()
    }

    fn commit(&mut self) {
        unimplemented!()
    }

    fn abort(&mut self) {
        unimplemented!()
    }

    fn is_aborted(&self) -> bool {
        unimplemented!()
    }

    fn is_commited(&self) -> bool {
        unimplemented!()
    }
}