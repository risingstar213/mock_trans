use std::sync::Arc;
use log::{ error, debug };

use crate::doca_comm_chan::comm_buf::DocaCommBuf;
use crate::memstore::memdb::ValueDB;
use crate::memstore::MemStoreValue;
use crate::framework::scheduler::AsyncScheduler;
use crate::MAX_RESP_SIZE;

use super::occ::{LockContent, MemStoreItemEnum, OccStatus};
use super::rwset::{RwSet, RwItem, RwType};
use super::remote_helpers::batch_rpc_msg_wrapper::BatchRpcRespWrapper;
use super::remote_helpers::batch_rpc_ctrl::BatchRpcCtrl;
use super::remote_helpers::*;
use super::dpu_helpers::comm_chan_ctrl::CommChanCtrl;
use super::dpu_helpers::*;

pub struct OccHost<const MAX_ITEM_SIZE: usize>
{
    status:    OccStatus,
    part_id:   u64,
    tid:       u32,
    cid:       u32,
    valuedb:   Arc<ValueDB>,
    batch_rpc: BatchRpcCtrl,
    comm_chan: CommChanCtrl,
    readset:   RwSet<MAX_ITEM_SIZE>,
    updateset: RwSet<MAX_ITEM_SIZE>,
    writeset:  RwSet<MAX_ITEM_SIZE>
}

// local operations
impl<const MAX_ITEM_SIZE: usize> OccHost<MAX_ITEM_SIZE>
{
    pub fn new(part_id: u64, tid: u32, cid: u32, valuedb: &Arc<ValueDB>, scheduler: &Arc<AsyncScheduler>) -> Self {
        Self {
            status:    OccStatus::OccUnint,
            part_id:   part_id,
            tid:       tid,
            cid:       cid,
            valuedb:   valuedb.clone(),
            batch_rpc: BatchRpcCtrl::new(scheduler, cid),
            comm_chan: CommChanCtrl::new(scheduler, part_id as _, cid),
            readset:   RwSet::new(),
            updateset: RwSet::new(),
            writeset:  RwSet::new(),
        }
    }

    #[inline]
    fn local_read<T: MemStoreValue>(&mut self, table_id: usize, key: u64) -> usize {
        let read_idx = self.readset.get_len();

        self.comm_chan.append_read(table_id, key, read_idx);

        let item = RwItem::new(
            table_id, 
            self.part_id,
            RwType::READ, 
            key, 
            MemStoreItemEnum::default(),
            0
        );


        self.readset.push(item);
        return read_idx;
    }

    #[inline]
    fn local_fetch_write<T: MemStoreValue>(&mut self, table_id: usize, key: u64) -> usize {
        let update_idx = self.updateset.get_len();

        self.comm_chan.append_update(table_id, key, update_idx);

        let item = RwItem::new(
            table_id,
            self.part_id,
            RwType::UPDATE,
            key,
            MemStoreItemEnum::default(),
            0
        );

        self.updateset.push(item);

        update_idx
    }

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
}

impl<const MAX_ITEM_SIZE: usize> OccHost<MAX_ITEM_SIZE>
{
    #[inline]
    fn commit_writes_on(&mut self, update: bool) {
        let ref_set = if update {
            &mut self.updateset
        } else {
            &mut self.writeset
        };

        for i in 0..ref_set.get_len() {
            let item = ref_set.bucket(i);

            if item.part_id == self.part_id {
                // local
                match item.rwtype {
                    RwType::ERASE => {
                        self.valuedb.local_erase_value(item.table_id, item.key);
                    }
                    RwType::INSERT => {
                        let raw = item.value.get_raw_ptr();
                        self.valuedb.local_put_value(item.table_id, item.key, raw, MAX_ITEM_SIZE as u32);
                    }
                    RwType::UPDATE => {
                        let raw = item.value.get_raw_ptr();
                        self.valuedb.local_set_value(item.table_id, item.key, raw, MAX_ITEM_SIZE as u32);
                    }
                    _ => {}
                }
            } else {
                // remote
                match item.rwtype {
                    RwType::ERASE => {
                        let remote_req = CommitCacheReqItem{
                            length:   0,
                        };

                        self.batch_rpc.append_req(
                            &remote_req, 
                            item.part_id, 
                            0, 
                            occ_rpc_id::COMMIT_RPC,
                        );
                    }
                    RwType::INSERT | RwType::UPDATE => {
                        let length = item.value.get_length();
                        let remote_req = CommitCacheReqItem{
                            length:   item.value.get_length(),
                        };

                        self.batch_rpc.append_req_with_data(
                            &remote_req, 
                            item.value.get_raw_ptr(), 
                            length as usize, 
                            item.part_id, 
                            0, 
                            occ_rpc_id::COMMIT_RPC,
                        );
                    }
                    _ => {}
                }
            }
        }
    }

    #[inline]
    fn release_on(&mut self, update: bool) {
        let ref_set = if update {
            &mut self.updateset
        } else {
            &mut self.writeset
        };

        let lock_content =  LockContent::new(self.part_id, self.tid, self.cid);

        for i in 0..ref_set.get_len() {
            let item = ref_set.bucket(i);

            if item.part_id == self.part_id {
                self.comm_chan.append_info(doca_comm_info_id::LOCAL_RELEASE_INFO);
            } else {
                let remote_req = DummyReqItem{};
    
                self.batch_rpc.append_req(
                    &remote_req, 
                    item.part_id, 
                    0, 
                    occ_rpc_id::RELEASE_RPC,
                );
            }
        }
    }

    #[inline]
    fn abort_on(&mut self, update: bool) {
        let ref_set = if update {
            &mut self.updateset
        } else {
            &mut self.writeset
        };

        let lock_content =  LockContent::new(self.part_id, self.tid, self.cid);

        for i in 0..ref_set.get_len() {
            let item = ref_set.bucket(i);

            if item.part_id == self.part_id {
                self.comm_chan.append_info(doca_comm_info_id::LOCAL_ABORT_INFO);
            } else {
                let remote_req = DummyReqItem{};
    
                self.batch_rpc.append_req(
                    &remote_req, 
                    item.part_id, 
                    0, 
                    occ_rpc_id::ABORT_RPC,
                );
            }
        }
    }

    #[inline]
    fn process_read_resp(&mut self, wrapper: &mut BatchRpcRespWrapper, num: u32) {
        for _ in 0..num {
            let item = wrapper.get_item::<ReadCacheRespItem>();
            let raw_data = wrapper.get_extra_data_const_ptr::<ReadCacheRespItem>();

            if item.read_idx >= self.readset.get_len() {
                println!("read length overflow???, cid:{}, num:{}", self.cid, num);
            }

            let bucket = self.readset.bucket(item.read_idx);

            bucket.value.set_raw_data(raw_data, item.length as _);

            wrapper.shift_to_next_item::<ReadCacheRespItem>(item.length);
        }
    }

    #[inline]
    fn process_fetch_write_resp(&mut self, wrapper: &mut BatchRpcRespWrapper, num: u32) {
        for _ in 0..num {
            let item = wrapper.get_item::<FetchWriteCacheRespItem>();
            let raw_data = wrapper.get_extra_data_const_ptr::<FetchWriteCacheRespItem>();

            if item.length == 0 {
                self.status = OccStatus::OccMustabort;
                break;
            }

            if item.update_idx >= self.updateset.get_len() {
                error!("update length overflow???, cid:{}, num:{}", self.cid, num);
                panic!();
            }

            let bucket = self.updateset.bucket(item.update_idx);
            bucket.value.set_raw_data(raw_data, item.length as _);

            wrapper.shift_to_next_item::<FetchWriteCacheRespItem>(item.length);
        }
    }

    fn process_batch_rpc_resp(&mut self) {
        let (mut resp_buf, resp_num) = self.batch_rpc.get_resp_buf_num().unwrap();

        for i in 0..resp_num {
            let mut wrapper = BatchRpcRespWrapper::new(resp_buf, MAX_RESP_SIZE);
            let header = wrapper.get_header();
            
            if header.cid != self.cid {
                error!("holy shit! {}th got strange resp ! me:{}, get:{}, write: {}, num: {}", i, self.cid, header.cid, header.write, header.num);
                panic!();
            }
            
            if header.write {
                self.process_fetch_write_resp(&mut wrapper, header.num);
            } else {
                self.process_read_resp(&mut wrapper, header.num);
            }

            resp_buf = unsafe { resp_buf.byte_add(crate::MAX_PACKET_SIZE) };
        }
    }
    
    fn process_batch_rpc_reduce_resp(&mut self) {
        let (mut resp_buf, resp_num) = self.batch_rpc.get_resp_buf_num().unwrap();
        for _ in 0..resp_num {
            let reduce_resp = unsafe { (resp_buf as *const BatchRpcReduceResp).as_ref().unwrap() };
            if !reduce_resp.success {
                self.status = OccStatus::OccMustabort;
                break;
            }

            resp_buf = unsafe { resp_buf.byte_add(crate::MAX_PACKET_SIZE) };
        }
    }

    fn process_comm_chan_resp(&mut self) {
        if !self.comm_chan.get_success() {
            self.status = OccStatus::OccMustabort;
            return;
        }

        for read_item in self.comm_chan.get_read_seqs() {
            let mut value = [0u8; MAX_ITEM_SIZE];
            let ptr = &mut value as *mut u8;
            let len = self.valuedb.get_item_length(read_item.table_id);
            let meta = self.valuedb.local_get_value(read_item.table_id, read_item.key, ptr, len as _);

            let bucket = self.readset.bucket(read_item.read_idx);

            bucket.value.set_raw_data(ptr, len as _);
        }

        for update_item in self.comm_chan.get_update_seqs() {
            let mut value = [0u8; MAX_ITEM_SIZE];
            let ptr = &mut value as *mut u8;
            let len = self.valuedb.get_item_length(update_item.table_id);
            let meta = self.valuedb.local_get_value(update_item.table_id, update_item.key, ptr, len as _);

            let bucket = self.updateset.bucket(update_item.update_idx);

            bucket.value.set_raw_data(ptr, len as _);
        }
    }
    
    fn process_comm_chan_reduce_resp(&mut self) {
        if !self.comm_chan.get_success() {
            self.status = OccStatus::OccMustabort;
            return;
        }
    }
}

impl<const MAX_ITEM_SIZE: usize> OccHost<MAX_ITEM_SIZE>
{
    async fn lock_writes(&mut self) {
        self.batch_rpc.restart_batch();
        self.comm_chan.restart_batch();

        let lock_content = LockContent::new(self.part_id, self.tid,  self.cid);
        for i in 0..self.writeset.get_len() {
            let item = self.writeset.bucket(i);
            if item.part_id == self.part_id {
                // local
                self.comm_chan.append_lock(item.table_id, item.key);
            } else {
                // remote
                let remote_req = LockReqItem{
                    table_id: item.table_id,
                    key:      item.key,
                };
        
                self.batch_rpc.append_req::<LockReqItem>(
                    &remote_req, 
                    item.part_id, 
                    0, 
                    occ_rpc_id::LOCK_RPC
                );
            }
        }

        // send remote
        self.batch_rpc.send_batch_reqs();
        // send dpu
        self.comm_chan.send_comm_info();

        // wait dpu
        self.comm_chan.wait_until_done().await;
        // process dpu
        self.process_comm_chan_reduce_resp();

        // wait remote
        self.batch_rpc.wait_until_done().await;
        // process remote
        self.process_batch_rpc_reduce_resp();
    }

    async fn validate(&mut self) {
        self.batch_rpc.restart_batch();
        self.comm_chan.restart_batch();

        for i in 0..self.readset.get_len() {
            let item = self.readset.bucket(i);
            if item.part_id == self.part_id {
                // local
                self.comm_chan.append_info(doca_comm_info_id::LOCAL_VALIDATE_INFO);
            } else {
                // remote
                let remote_req = DummyReqItem{};
        
                self.batch_rpc.append_req::<DummyReqItem>(
                    &remote_req, 
                    item.part_id, 
                    0, 
                    occ_rpc_id::VALIDATE_RPC,
                );
            }
        }

        // send remote
        self.batch_rpc.send_batch_reqs();
        // send dpu
        self.comm_chan.send_comm_info();

        // wait dpu
        self.comm_chan.wait_until_done().await;
        // process dpu
        self.process_comm_chan_reduce_resp();

        // wait remote
        self.batch_rpc.wait_until_done().await;
        // process remote
        self.process_batch_rpc_reduce_resp();
    }

    async fn log_writes(&mut self) {
        // unimplemented temporarily
    }
    
    async fn commit_writes(&mut self) {
        self.batch_rpc.restart_batch();
        self.commit_writes_on(true);
        self.commit_writes_on(false);

        self.batch_rpc.send_batch_reqs();
        self.batch_rpc.wait_until_done().await;
    }

    async fn release(&mut self) {
        self.batch_rpc.restart_batch();
        self.comm_chan.restart_batch();
        self.release_on(true);
        self.release_on(false);

        self.batch_rpc.send_batch_reqs();
        self.comm_chan.send_comm_info();
        self.comm_chan.wait_until_done().await;
        self.batch_rpc.wait_until_done().await;
    }

    async fn recover_on_aborted(&mut self) {
        self.batch_rpc.restart_batch();
        self.comm_chan.restart_batch();
        self.abort_on(true);
        self.abort_on(false);

        self.batch_rpc.send_batch_reqs();
        self.comm_chan.send_comm_info();
        self.comm_chan.wait_until_done().await;
        self.batch_rpc.wait_until_done().await;
    }
}

impl<const MAX_ITEM_SIZE: usize> OccHost< MAX_ITEM_SIZE>
{
    pub fn start(&mut self) {
        self.batch_rpc.restart_batch();
        self.comm_chan.restart_batch();
        self.status = OccStatus::OccInprogress;
    }

    pub fn read<T: MemStoreValue>(&mut self, table_id: usize, part_id: u64, key: u64) -> usize {
        if part_id == self.part_id {
            // local
            self.local_read::<T>(table_id, key)
        } else {
            // remote
            self.remote_read_rpc::<T>(table_id, part_id, key)
        }
    }

    // fetch for write
    pub fn fetch_write<T: MemStoreValue>(&mut self, table_id: usize, part_id: u64, key: u64) -> usize {
        if part_id == self.part_id {
            // local
            self.local_fetch_write::<T>(table_id, key)
        } else {
            // remote
            self.remote_fetch_write_rpc::<T>(table_id, part_id, key)
        }
    }

    pub fn write<T: MemStoreValue>(&mut self, table_id: usize, part_id: u64, key: u64, rwtype: RwType) -> usize {
        let write_idx = self.writeset.get_len();

        // lock later
        let item = RwItem::new(
            table_id,
            part_id,
            rwtype,
            key,
            MemStoreItemEnum::default(),
            0
        );

        self.writeset.push(item);

        write_idx
    }

    pub async fn get_value<'trans, T: MemStoreValue + 'trans>(&mut self, update: bool, idx: usize) -> &'trans T {
        // TODO: more careful check
        self.batch_rpc.send_batch_reqs();
        self.comm_chan.send_comm_info();

        self.comm_chan.wait_until_done().await;
        self.process_comm_chan_resp();

        self.batch_rpc.wait_until_done().await;
        self.process_batch_rpc_resp();

        self.batch_rpc.restart_batch();
        self.comm_chan.restart_batch();

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

    pub async fn commit(&mut self) {
        self.lock_writes().await;
        if self.status.eq(&OccStatus::OccMustabort) {
            return self.abort().await;
        }

        self.validate().await;
        if self.status.eq(&OccStatus::OccMustabort) {
            return self.abort().await;
        }

        self.log_writes().await;

        self.commit_writes().await;

        self.release().await;

        self.status = OccStatus::OccCommited;
    }

    pub async fn abort(&mut self) {
        self.recover_on_aborted().await;
        
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