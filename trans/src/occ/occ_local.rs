use std::sync::Arc;

use crate::memstore::memdb::MemDB;
use crate::memstore::MemStoreValue;
use crate::memstore::MemNodeMeta;

use super::occ::{Occ, OccExecute, OccStatus, MemStoreItemEnum};
use super::rwset::{RwType, RwItem, RwSet};

pub struct OccLocal<'trans, const MAX_ITEM_SIZE: usize> 
{
    status:    OccStatus,
    memdb:     Arc<MemDB<'trans>>,
    readset:   RwSet<MAX_ITEM_SIZE>,
    updateset: RwSet<MAX_ITEM_SIZE>,
    writeset:  RwSet<MAX_ITEM_SIZE>
}

impl<'trans, const MAX_ITEM_SIZE: usize> OccLocal<'trans, MAX_ITEM_SIZE> 
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

impl<'trans, const MAX_ITEM_SIZE: usize> OccExecute for OccLocal<'trans, MAX_ITEM_SIZE>
{
    fn lock_writes(&mut self) {
        if self.status != OccStatus::OccInprogress {
            return;
        }

        for i in 0..self.writeset.get_len() {
            let item = self.writeset.bucket(i);

            let meta = self.memdb.local_lock(item.table_id, item.key, item.lock).unwrap();

            if meta.lock != item.lock {
                self.status = OccStatus::OccMustabort;
                break;
            }
        }
    }
    
    fn validate(&mut self) {
        if self.status != OccStatus::OccInprogress {
            return;
        }

        for i in 0..self.readset.get_len() {
            let item = self.readset.bucket(i);

            let meta = self.memdb.local_get_meta(item.table_id, item.key).unwrap();

            if meta.lock != 0 || (meta.seq != item.seq) {
                self.status = OccStatus::OccMustabort;
            }
        }
    }

    fn log_write(&mut self) {
        // unimplemented temporarily
    }

    fn commit_write(&mut self) {
        if self.status != OccStatus::OccInprogress {
            return;
        }

        for i in 0..self.updateset.get_len() {
            let item = self.updateset.bucket(i);

            match item.rwtype {
                RwType::ERASE => {
                    self.memdb.local_erase(item.table_id, item.key);
                },
                RwType::INSERT | RwType::UPDATE => {
                    let raw = item.value.get_raw_ptr();
                    self.memdb.local_upd_val_seq(item.table_id, item.key, raw, MAX_ITEM_SIZE as u32);
                },
                _ => {}
            }

        }

        for i in 0..self.writeset.get_len() {
            let item = self.writeset.bucket(i);

            match item.rwtype {
                RwType::ERASE => {
                    self.memdb.local_erase(item.table_id, item.key);
                },
                RwType::INSERT | RwType::UPDATE => {
                    let raw = item.value.get_raw_ptr();
                    self.memdb.local_upd_val_seq(item.table_id, item.key, raw, MAX_ITEM_SIZE as u32);
                },
                _ => {}
            }
        }
    }

    fn unlock(&mut self) {
        if self.status != OccStatus::OccInprogress {
            return;
        }

        for i in 0..self.updateset.get_len() {
            let item = self.updateset.bucket(i);
            self.memdb.local_unlock(item.table_id, item.key, item.lock);
        }

        for i in 0..self.writeset.get_len() {
            let item = self.writeset.bucket(i);
            self.memdb.local_unlock(item.table_id, item.key, item.lock);
        }
    }

    fn recover_on_aborted(&mut self) {
        if self.status != OccStatus::OccMustabort {
            return;
        }

        for i in 0..self.updateset.get_len() {
            let item = self.updateset.bucket(i);
            self.memdb.local_unlock(item.table_id, item.key, item.lock);
        }

        for i in 0..self.writeset.get_len() {
            let item = self.writeset.bucket(i);

            match item.rwtype {
                RwType::ERASE | RwType::UPDATE => {
                    self.memdb.local_unlock(item.table_id, item.key, item.lock);
                },
                RwType::INSERT => {
                    self.memdb.local_erase(item.table_id, item.key);
                },
                _ => {}
            }
        }
    }
}

impl<'trans, const MAX_ITEM_SIZE: usize> Occ for OccLocal<'trans, MAX_ITEM_SIZE>
{
    fn start(&mut self) {
        self.status = OccStatus::OccInprogress;
    }
    
    fn read<T: MemStoreValue>(&mut self, table_id: usize, part_id: u64, key: u64) -> usize {
        // local
        assert_eq!(part_id, 0);

        let mut value = T::default();
        let ptr = &mut value as *mut T as *mut u8;
        let len = std::mem::size_of::<T>();
        let meta = self.memdb.local_get_readonly(table_id, key, ptr, len as _).unwrap();

        let item = RwItem::new(
            table_id,
            0,
            RwType::READ, 
            key, 
            MemStoreItemEnum::from_raw(value),
            meta.lock, 
            meta.seq
        );

        if meta.lock != 0 {
            self.status = OccStatus::OccMustabort;
        }

        self.readset.push(item);
        return self.readset.get_len() - 1;
    }

    fn fetch_write<T: MemStoreValue>(&mut self, table_id: usize, part_id: u64, key: u64, lock_content: u64) -> usize {
        // local
        assert_eq!(part_id, 0);

        let mut value = T::default();
        let ptr = &mut value as *mut T as *mut u8;
        let len = std::mem::size_of::<T>();
        let meta = self.memdb.local_get_for_upd(table_id, key, ptr, len as _, lock_content).unwrap();

        let item = RwItem::new(
            table_id,
            0,
            RwType::UPDATE,
            key,
            MemStoreItemEnum::from_raw(value),
            lock_content,
            meta.seq
        );

        if meta.lock != lock_content {
            self.status = OccStatus::OccMustabort;
        }

        self.updateset.push(item);
        return self.updateset.get_len() - 1;
    }

    fn write<T: MemStoreValue>(&mut self, table_id: usize,  part_id: u64, key: u64, lock_content: u64, rwtype: RwType) -> usize {
        // local
        assert_eq!(part_id, 0);

        let value = T::default();
        let meta = MemNodeMeta::new(lock_content, 0);

        // lock later
        let item = RwItem::new(
            table_id,
            0,
            rwtype,
            key,
            MemStoreItemEnum::from_raw(value),
            meta.lock,
            meta.seq
        );

        self.writeset.push(item);
        return self.writeset.get_len() - 1;
    }

    fn get_value<T: MemStoreValue>(&mut self, update: bool, idx: usize) -> &T {
        if update {
            return self.updateset.bucket(idx).value.get_inner();
        } else {
            return self.readset.bucket(idx).value.get_inner();
        }
    }

    fn set_value<T: MemStoreValue>(&mut self, update: bool, idx: usize, value: &T) {
        if update {
            self.updateset.bucket(idx).value.set_inner(value);
        } else {
            self.writeset.bucket(idx).value.set_inner(value);
        }
    }

    fn commit(&mut self) {
        self.lock_writes();
        if self.status.eq(&OccStatus::OccMustabort) {
            return self.abort();
        }

        self.validate();
        if self.status.eq(&OccStatus::OccMustabort) {
            return self.abort();
        }

        self.log_write();

        self.commit_write();

        self.unlock();

        self.status = OccStatus::OccCommited;
    }

    fn abort(&mut self) {
        self.recover_on_aborted();

        self.status = OccStatus::OccAborted;
    }

    #[inline]
    fn is_aborted(&self) -> bool {
        self.status.eq(&OccStatus::OccAborted)
    }

    #[inline]
    fn is_commited(&self) -> bool {
        self.status.eq(&OccStatus::OccCommited)
    }
}