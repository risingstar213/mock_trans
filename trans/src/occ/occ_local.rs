use std::sync::Arc;

use crate::memstore::memdb::MemDB;
use crate::memstore::MemStoreValue;
use crate::memstore::MemNodeMeta;

use super::occ::OCCStatus;
use super::occ::{OCC, MemStoreItemEnum};
use super::rwset::{RwType, RwItem, RwSet};

pub struct OccLocal<'trans, E> 
where
    E: MemStoreItemEnum
{
    status:    OCCStatus,
    memdb:     Arc<MemDB<'trans>>,
    readset:   RwSet<E>,
    updateset: RwSet<E>,
    writeset:  RwSet<E>
}

impl<'trans, E> OccLocal<'trans, E> 
where
    E: MemStoreItemEnum
{
    pub fn new(memdb: &Arc<MemDB<'trans>>) -> Self {
        Self {
            status:    OCCStatus::OCC_UNINIT,
            memdb:     memdb.clone(),
            readset:   RwSet::<E>::new(),
            updateset: RwSet::<E>::new(),
            writeset:  RwSet::<E>::new(),
        }
    }
}

impl<'trans, E> OCC for OccLocal<'trans, E>
where
    E: MemStoreItemEnum
{
    fn read<T: MemStoreValue>(&mut self, table_id: usize, key: u64) -> usize {
        // local
        let mut value = T::default();
        let ptr = &mut value as *mut T as *mut u8;
        let len = std::mem::size_of::<T>();
        let meta = self.memdb.local_get_readonly(table_id, key, ptr, len as _).unwrap();

        let item = RwItem::<E>::new(
            table_id, 
            RwType::READ, 
            key, 
            MemStoreItemEnum::from_raw(value),
            meta.lock, 
            meta.seq
        );

        if meta.lock != 0 {
            self.status = OCCStatus::OCC_ABORTED;
        }

        self.readset.push(item);
        return self.readset.get_len() - 1;
    }

    fn fetch_write<T: MemStoreValue>(&mut self, table_id: usize, key: u64, lock_content: u64) -> usize {
        // local
        let mut value = T::default();
        let ptr = &mut value as *mut T as *mut u8;
        let len = std::mem::size_of::<T>();
        let meta = self.memdb.local_get_for_upd(table_id, key, ptr, len as _, lock_content).unwrap();

        let item = RwItem::<E>::new(
            table_id,
            RwType::UPDATE,
            key,
            MemStoreItemEnum::from_raw(value),
            meta.lock,
            meta.seq
        );

        if meta.lock != lock_content {
            self.status = OCCStatus::OCC_ABORTED;
        }

        self.updateset.push(item);
        return self.updateset.get_len() - 1;
    }

    fn write<T: MemStoreValue>(&mut self, table_id: usize, key: u64, lock_content: u64, rwtype: RwType) -> usize {
        // local
        let value = T::default();
        let meta = MemNodeMeta::new(0, 0);

        // lock later
        let item = RwItem::<E>::new(
            table_id,
            RwType::UPDATE,
            key,
            MemStoreItemEnum::from_raw(value),
            meta.lock,
            meta.seq
        );

        self.writeset.push(item);
        return self.writeset.get_len() - 1;
        // let meta = match rwtype {
        //     RwType::UPDATE => {

        //     },
        //     RwType::INSERT => {

        //     },
        //     RwType::ERASE => {

        //     },
        //     _ => {
        //         MemNodeMeta::new(0, 0);
        //     }
        // }
    }

    #[inline]
    fn is_aborted(&self) -> bool {
        self.status.eq(&OCCStatus::OCC_ABORTED)
    }

    // fn get_readset(&'trans self) -> &'trans RwSet<'trans> {
    //     &self.readset
    // }

    // fn get_writeset(&'trans self ) -> &'trans RwSet<'trans> {
    //     &self.writeset
    // }
}
