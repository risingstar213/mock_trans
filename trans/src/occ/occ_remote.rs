use std::sync::Arc;

use crate::framework::scheduler;
use crate::memstore::memdb::MemDB;
use crate::memstore::MemStoreValue;
use crate::framework::scheduler::AsyncScheduler;

use super::occ::{LockContent, MemStoreItemEnum, Occ, OccExecute, OccStatus};
use super::rwset::{RwSet, RwItem, RwType};
use super::remote_helpers::batch_rpc_ctrl::BatchRpcCtrl;
use super::remote_helpers::batch_rpc_proc::*;

pub struct OccRemote<'trans, const MAX_ITEM_SIZE: usize>
{
    status:    OccStatus,
    part_id:   u64,
    cid:       u32,
    memdb:     Arc<MemDB<'trans>>,
    batch_rpc: BatchRpcCtrl<'trans>,
    readset:   RwSet<MAX_ITEM_SIZE>,
    updateset: RwSet<MAX_ITEM_SIZE>,
    writeset:  RwSet<MAX_ITEM_SIZE>
}

// local operations
impl<'trans, const MAX_ITEM_SIZE: usize> OccRemote<'trans, MAX_ITEM_SIZE>
{
    pub fn new(part_id: u64, cid: u32, memdb: &Arc<MemDB<'trans>>, scheduler: &Arc<AsyncScheduler<'trans>>) -> Self {
        Self {
            status:    OccStatus::OccUnint,
            part_id:   part_id,
            cid:       cid,
            memdb:     memdb.clone(),
            batch_rpc: BatchRpcCtrl::new(scheduler),
            readset:   RwSet::new(),
            updateset: RwSet::new(),
            writeset:  RwSet::new(),
        }
    }

    #[inline]
    fn local_read<T: MemStoreValue>(&mut self, table_id: usize, key: u64) -> usize {
        let read_idx = self.readset.get_len();
        let mut value = T::default();
        let ptr = &mut value as *mut T as *mut u8;
        let len = std::mem::size_of::<T>();
        let meta = self.memdb.local_get_readonly(table_id, key, ptr, len as _).unwrap();

        let item = RwItem::new(
            table_id, 
            self.part_id,
            RwType::READ, 
            key, 
            MemStoreItemEnum::from_raw(value),
            meta.seq
        );


        self.readset.push(item);
        return read_idx;
    }

    #[inline]
    fn local_fetch_write<T: MemStoreValue>(&mut self, table_id: usize, key: u64) -> usize {
        let update_idx = self.updateset.get_len();

        let lock_content = LockContent::new(self.part_id, self.cid);

        let mut value = T::default();
        let ptr = &mut value as *mut T as *mut u8;
        let len = std::mem::size_of::<T>();
        let meta = self.memdb.local_get_for_upd(table_id, key, ptr, len as _, lock_content.to_content()).unwrap();

        let item = RwItem::new(
            table_id,
            self.part_id,
            RwType::UPDATE,
            key,
            MemStoreItemEnum::from_raw(value),
            meta.seq
        );

        if meta.lock != lock_content.to_content() {
            self.status = OccStatus::OccMustabort;
        }

        self.updateset.push(item);

        update_idx
    }

    #[inline]
    fn local_lock_write(&mut self, idx: usize, lock_content: u64) {

        let item = self.writeset.bucket(idx);

        let meta = self.memdb.local_lock(item.table_id, item.key, lock_content).unwrap();

        if meta.lock != lock_content {
            self.status = OccStatus::OccMustabort;
        }
    }
}

impl<'trans, const MAX_ITEM_SIZE: usize> OccRemote<'trans, MAX_ITEM_SIZE>
{
    // read
    #[inline]
    fn remote_read_rpc<T: MemStoreValue>(&mut self, table_id: usize,  part_id: u64, key: u64) -> usize {
        let read_idx = self.readset.get_len();
        let remote_req = ReadReqItem{
            table_id: table_id,
            key:      key,
            read_idx: read_idx,
        };
        self.batch_rpc.append_req::<ReadReqItem>(
            &remote_req, 
            self.cid, 
            part_id, 
            0, 
            occ_rpc_id::READ_RPC
        );
        
        // pending
        let item = RwItem::new(
            table_id, 
            part_id,
            RwType::READ, 
            key, 
            MemStoreItemEnum::default(),
            0
        );
        self.readset.push(item);

        read_idx
    }
    // fetch write
    #[inline]
    fn remote_fetch_write_rpc<T: MemStoreValue>(&mut self, table_id: usize, part_id: u64, key: u64) -> usize {
        let update_idx = self.updateset.get_len();
        let remote_req = FetchWriteReqItem{
            table_id:   table_id,
            key:        key,
            update_idx: update_idx,
        };
        self.batch_rpc.append_req::<FetchWriteReqItem>(
            &remote_req, 
            self.cid, 
            part_id, 
            0, 
            occ_rpc_id::FETCHWRITE_RPC
        );
        // pending
        let item = RwItem::new(
            table_id,
            part_id,
            RwType::UPDATE,
            key,
            MemStoreItemEnum::default(),
            0
        );
        self.updateset.push(item);

        update_idx
    }

    #[inline]
    fn remote_lock_writes_rpc(&mut self, idx: usize) {
        let item = self.writeset.bucket(idx);

        let remote_req = WriteReqItem{
            table_id: item.table_id,
            key:      item.key,
        };

        self.batch_rpc.append_req::<WriteReqItem>(
            &remote_req, 
            self.cid, 
            item.part_id, 
            0, 
            occ_rpc_id::WRITE_RPC
        );
    }
}

impl<'trans, const MAX_ITEM_SIZE: usize> OccExecute for OccRemote<'trans, MAX_ITEM_SIZE>
{
    fn lock_writes(&mut self) {
        let lock_content = LockContent::new(self.part_id, self.cid);
        for i in 0..self.writeset.get_len() {
            let item = self.writeset.bucket(i);
            if item.part_id == self.part_id {
                self.local_lock_write(i, lock_content.to_content());
            } else {
                self.remote_lock_writes_rpc(i);
            }
        }
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

impl<'trans, const MAX_ITEM_SIZE: usize> Occ for OccRemote<'trans, MAX_ITEM_SIZE>
{
    fn start(&mut self) {
        unimplemented!()
    }

    fn read<T: MemStoreValue>(&mut self, table_id: usize, part_id: u64, key: u64) -> usize {
        if part_id == self.part_id {
            // local
            self.local_read::<T>(table_id, key)
        } else {
            // remote
            self.remote_read_rpc::<T>(table_id, part_id, key)
        }
    }

    // fetch for write
    fn fetch_write<T: MemStoreValue>(&mut self, table_id: usize, part_id: u64, key: u64) -> usize {
        if part_id == self.part_id {
            // local
            self.local_fetch_write::<T>(table_id, key)
        } else {
            // remote
            self.remote_fetch_write_rpc::<T>(table_id, part_id, key)
        }
    }

    fn write<T: MemStoreValue>(&mut self, table_id: usize, part_id: u64, key: u64, rwtype: RwType) -> usize {
        let write_idx = self.writeset.get_len();

        // lock later
        let item = RwItem::new(
            table_id,
            0,
            rwtype,
            key,
            MemStoreItemEnum::default(),
            0
        );

        self.writeset.push(item);

        write_idx
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

    #[inline]
    fn is_aborted(&self) -> bool {
        self.status.eq(&OccStatus::OccAborted)
    }

    #[inline]
    fn is_commited(&self) -> bool {
        self.status.eq(&OccStatus::OccCommited)
    }
}