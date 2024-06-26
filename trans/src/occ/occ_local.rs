use std::sync::Arc;

use crate::memstore::memdb::MemDB;
use crate::memstore::MemStoreValue;
use crate::memstore::MemNodeMeta;

use super::occ::{OccStatus, MemStoreItemEnum, LockContent};
use super::rwset::{RwType, RwItem, RwSet};

pub struct OccLocal<const MAX_ITEM_SIZE: usize> 
{
    status:    OccStatus,
    cid:       u32,
    memdb:     Arc<MemDB>,
    readset:   RwSet<MAX_ITEM_SIZE>,
    updateset: RwSet<MAX_ITEM_SIZE>,
    writeset:  RwSet<MAX_ITEM_SIZE>
}

impl<const MAX_ITEM_SIZE: usize> OccLocal<MAX_ITEM_SIZE> 
{
    pub fn new(cid: u32, memdb: &Arc<MemDB>) -> Self {
        Self {
            status:    OccStatus::OccUnint,
            cid:       cid,
            memdb:     memdb.clone(),
            readset:   RwSet::new(),
            updateset: RwSet::new(),
            writeset:  RwSet::new(),
        }
    }
}

impl<'trans, const MAX_ITEM_SIZE: usize> OccLocal<MAX_ITEM_SIZE>
{
    fn lock_writes(&mut self) {
        if self.status != OccStatus::OccInprogress {
            return;
        }

        let lock_content = LockContent::new(0, 0, self.cid);

        for i in 0..self.writeset.get_len() {
            let item = self.writeset.bucket(i);

            let meta = self.memdb.local_lock(item.table_id, item.key, lock_content.to_content()).unwrap();

            if meta.lock != lock_content.to_content() {
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

    fn log_writes(&mut self) {
        // unimplemented temporarily
    }

    fn commit_writes(&mut self) {
        if self.status != OccStatus::OccInprogress {
            return;
        }

        for i in 0..self.updateset.get_len() {
            let item = self.updateset.bucket(i);

            match item.rwtype {
                RwType::ERASE => {
                    self.memdb.local_erase(item.table_id, item.key);
                }
                RwType::INSERT | RwType::UPDATE => {
                    let raw = item.value.get_raw_ptr();
                    self.memdb.local_upd_val_seq(item.table_id, item.key, raw, MAX_ITEM_SIZE as u32);
                }
                _ => {}
            }

        }

        for i in 0..self.writeset.get_len() {
            let item = self.writeset.bucket(i);

            match item.rwtype {
                RwType::ERASE => {
                    self.memdb.local_erase(item.table_id, item.key);
                }
                RwType::INSERT | RwType::UPDATE => {
                    let raw = item.value.get_raw_ptr();
                    self.memdb.local_upd_val_seq(item.table_id, item.key, raw, MAX_ITEM_SIZE as u32);
                }
                _ => {}
            }
        }
    }

    fn release(&mut self) {
        if self.status != OccStatus::OccInprogress {
            return;
        }

        let lock_content = LockContent::new(0, 0, self.cid);

        for i in 0..self.updateset.get_len() {
            let item = self.updateset.bucket(i);
            self.memdb.local_unlock(item.table_id, item.key, lock_content.to_content());
        }

        for i in 0..self.writeset.get_len() {
            let item = self.writeset.bucket(i);
            self.memdb.local_unlock(item.table_id, item.key, lock_content.to_content());
        }
    }

    fn recover_on_aborted(&mut self) {
        if self.status != OccStatus::OccMustabort {
            return;
        }

        let lock_content = LockContent::new(0, 0, self.cid);

        for i in 0..self.updateset.get_len() {
            let item = self.updateset.bucket(i);
            self.memdb.local_unlock(item.table_id, item.key, lock_content.to_content());
        }

        for i in 0..self.writeset.get_len() {
            let item = self.writeset.bucket(i);

            match item.rwtype {
                RwType::ERASE | RwType::UPDATE => {
                    self.memdb.local_unlock(item.table_id, item.key, lock_content.to_content());
                }
                RwType::INSERT => {
                    self.memdb.local_erase(item.table_id, item.key);
                }
                _ => {}
            }
        }
    }
}

impl< const MAX_ITEM_SIZE: usize> OccLocal< MAX_ITEM_SIZE>
{
    pub fn start(&mut self) {
        self.status = OccStatus::OccInprogress;
    }
    
    pub fn read<T: MemStoreValue>(&mut self, table_id: usize, part_id: u64, key: u64) -> usize {
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
            meta.seq
        );

        self.readset.push(item);
        return self.readset.get_len() - 1;
    }

    pub fn fetch_write<T: MemStoreValue>(&mut self, table_id: usize, part_id: u64, key: u64) -> usize {
        // local
        assert_eq!(part_id, 0);

        let lock_content = LockContent::new(0, 0, self.cid);

        let mut value = T::default();
        let ptr = &mut value as *mut T as *mut u8;
        let len = std::mem::size_of::<T>();
        let meta = self.memdb.local_get_for_upd(table_id, key, ptr, len as _, lock_content.to_content()).unwrap();

        let item = RwItem::new(
            table_id,
            0,
            RwType::UPDATE,
            key,
            MemStoreItemEnum::from_raw(value),
            meta.seq
        );

        if meta.lock != lock_content.to_content() {
            self.status = OccStatus::OccMustabort;
        }

        self.updateset.push(item);
        return self.updateset.get_len() - 1;
    }

    pub fn write<T: MemStoreValue>(&mut self, table_id: usize,  part_id: u64, key: u64, rwtype: RwType) -> usize {
        // local
        assert_eq!(part_id, 0);

        let lock_content = LockContent::new(0, 0, self.cid);

        let value = T::default();

        // lock later
        let item = RwItem::new(
            table_id,
            0,
            rwtype,
            key,
            MemStoreItemEnum::from_raw(value),
            0
        );

        self.writeset.push(item);
        return self.writeset.get_len() - 1;
    }

    pub fn get_value<T: MemStoreValue>(&mut self, update: bool, idx: usize) -> &T {
        if update {
            return self.updateset.bucket(idx).value.get_inner();
        } else {
            return self.readset.bucket(idx).value.get_inner();
        }
    }

    pub fn set_value<T: MemStoreValue>(&mut self, update: bool, idx: usize, value: &T) {
        if update {
            self.updateset.bucket(idx).value.set_inner(value);
        } else {
            self.writeset.bucket(idx).value.set_inner(value);
        }
    }

    pub fn commit(&mut self) {
        self.lock_writes();
        if self.status.eq(&OccStatus::OccMustabort) {
            return self.abort();
        }

        self.validate();
        if self.status.eq(&OccStatus::OccMustabort) {
            return self.abort();
        }

        self.log_writes();

        self.commit_writes();

        self.release();

        self.status = OccStatus::OccCommited;
    }

    pub fn abort(&mut self) {
        self.recover_on_aborted();

        self.status = OccStatus::OccAborted;
    }

    #[inline]
    pub fn is_aborted(&self) -> bool {
        self.status.eq(&OccStatus::OccAborted)
    }

    #[inline]
    pub fn is_commited(&self) -> bool {
        self.status.eq(&OccStatus::OccCommited)
    }
}