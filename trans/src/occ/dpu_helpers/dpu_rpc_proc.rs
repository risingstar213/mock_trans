use std::sync::Arc;

use crate::doca_comm_chan::comm_buf::DocaCommBuf;
use crate::doca_comm_chan::doca_comm_info_type;
use crate::doca_comm_chan::DocaCommHeaderMeta;
use crate::framework::scheduler;
use crate::framework::scheduler::AsyncScheduler;
use crate::framework::rpc::*;
use crate::memstore::memdb::MemDB;
use crate::occ::cache_helpers::trans_cache_view::TransCacheView;
use crate::occ::cache_helpers::trans_cache_view::TransKey;
use crate::occ::cache_helpers::{ CacheReadSetItem, CacheWriteSetItem };
use crate::occ::occ::LockContent;
use crate::rdma::rcconn::RdmaRcConn;
use crate::MAIN_ROUTINE_ID;
use crate::MAX_RESP_SIZE;

use super::*;


pub struct DpuRpcProc {
    pub tid:        u32,
    pub memdb:      Arc<MemDB>,
    pub scheduler:  Arc<AsyncScheduler>,
    pub trans_view: TransCacheView
}

impl DpuRpcProc {
    pub fn new(tid: u32, memdb: &Arc<MemDB>, scheduler: &Arc<AsyncScheduler>) -> Self {
        Self {
            tid:  tid,
            memdb: memdb.clone(),
            scheduler: scheduler.clone(),
            trans_view: TransCacheView::new(scheduler),
        }
    }
}

// dpu comm conn handler
impl DpuRpcProc {
    pub fn local_read_info_handler(
        &self,
        buf: &DocaCommBuf,
        info_payload: u32,
        info_pid: u32,
        info_cid: u32,
    ) {
        let count = info_payload as usize / std::mem::size_of::<LocalReadInfoItem>();

        let trans_key = TransKey::new_raw(info_pid, self.tid, info_cid);
        self.trans_view.start_read_trans(&trans_key);
        let mut read_cache_writer = self.trans_view.new_read_cache_writer(&trans_key, MAIN_ROUTINE_ID);

        let items = unsafe { buf.get_const_slice::<LocalReadInfoItem>(count) };
        for item in items {
            let meta = self.memdb.local_get_meta(
                item.table_id, 
                item.key
            ).unwrap();

            read_cache_writer.block_append_item(&self.trans_view, CacheReadSetItem{
                table_id: item.table_id, 
                key:      item.key,
                old_seq:  meta.seq as u64,
            });
        }

        read_cache_writer.block_sync_buf(&self.trans_view);

        let mut comm_reply = self.scheduler.comm_chan_alloc_buf();
        comm_reply.set_payload(0);
        unsafe { comm_reply.append_item(DocaCommReply { success: true }); }

        comm_reply.set_header(DocaCommHeaderMeta{
            info_type: doca_comm_info_type::REPLY,
            info_id:   doca_comm_info_id::LOCAL_READ_INFO,
            info_payload: std::mem::size_of::<DocaCommReply>() as u32,
            info_pid: info_pid,
            info_cid: info_cid,
        });

        self.scheduler.block_send_info(&mut comm_reply);
        self.scheduler.comm_chan_dealloc_buf(comm_reply);
    }

    pub fn local_lock_info_handler(
        &self,
        buf: &DocaCommBuf,
        info_payload: u32,
        info_pid: u32,
        info_cid: u32,
    ) {
        let count = info_payload as usize / std::mem::size_of::<LocalLockInfoItem>();
    
        let trans_key = TransKey::new_raw(info_pid, self.tid, info_cid);
        self.trans_view.start_write_trans(&trans_key);
        let mut write_cache_writer = self.trans_view.new_write_cache_writer(&trans_key, MAIN_ROUTINE_ID);

        let mut success = true;
        let lock_content = LockContent::new(info_pid as _, self.tid as _, info_cid);

        let items = unsafe { buf.get_const_slice::<LocalLockInfoItem>(count) };
        for item in items {
            let meta = self.memdb.local_lock(
                item.table_id, 
                item.key, 
                lock_content.to_content(),
            ).unwrap();

            if meta.lock != lock_content.to_content() {
                success = false;
                break;
            }

            // remain bugs !!! seq num !!!
            write_cache_writer.block_append_item(&self.trans_view, CacheWriteSetItem{
                table_id: item.table_id, 
                key:      item.key,
                insert:   (meta.seq == 2),
            })
        }

        write_cache_writer.block_sync_buf(&self.trans_view);

        let mut comm_reply = self.scheduler.comm_chan_alloc_buf();
        comm_reply.set_payload(0);
        unsafe { comm_reply.append_item(DocaCommReply { success: success }); }

        comm_reply.set_header(DocaCommHeaderMeta{
            info_type: doca_comm_info_type::REPLY,
            info_id:   doca_comm_info_id::LOCAL_LOCK_INFO,
            info_payload: std::mem::size_of::<DocaCommReply>() as u32,
            info_pid: info_pid,
            info_cid: info_cid,
        });

        self.scheduler.block_send_info(&mut comm_reply);
        self.scheduler.comm_chan_dealloc_buf(comm_reply);
    }

    pub fn local_validate_info_handler(
        &self,
        buf: &DocaCommBuf,
        info_payload: u32,
        info_pid: u32,
        info_cid: u32,
    ) {

        let trans_key = TransKey::new_raw(info_pid, self.tid, info_cid);
        let buf_count = self.trans_view.get_read_range_num(&trans_key);

        let mut success = true;

        for i in 0..buf_count {
            let read_buf = self.trans_view.block_get_read_buf(&trans_key, i, 0);

            for item in read_buf.iter() {
                let meta = self.memdb.local_get_meta(
                    item.table_id, 
                    item.key
                ).unwrap();
    
                if meta.lock != 0 || (meta.seq != item.old_seq) {
                    success = false;
                    break;
                }
            }
        }

        self.trans_view.end_read_trans(&trans_key);

        let mut comm_reply = self.scheduler.comm_chan_alloc_buf();
        comm_reply.set_payload(0);
        unsafe { comm_reply.append_item(DocaCommReply { success: success }); }

        comm_reply.set_header(DocaCommHeaderMeta{
            info_type: doca_comm_info_type::REPLY,
            info_id:   doca_comm_info_id::LOCAL_VALIDATE_INFO,
            info_payload: std::mem::size_of::<DocaCommReply>() as u32,
            info_pid: info_pid,
            info_cid: info_cid,
        });

        self.scheduler.block_send_info(&mut comm_reply);
        self.scheduler.comm_chan_dealloc_buf(comm_reply);
    }

    pub fn local_release_info_handler(
        &self,
        buf: &DocaCommBuf,
        info_payload: u32,
        info_pid: u32,
        info_cid: u32,
    ) {
        let trans_key = TransKey::new_raw(info_pid, self.tid, info_cid);
        let buf_count = self.trans_view.get_write_range_num(&trans_key);

        let lock_content = LockContent::new(info_pid as _, self.tid as _, info_cid);

        // unlock
        for i in 0..buf_count {
            let write_buf = self.trans_view.block_get_write_buf(&trans_key, i, 0);

            for item in write_buf.iter() {
                self.memdb.local_unlock(item.table_id, item.key, lock_content.to_content());
            }
        }

        self.trans_view.end_write_trans(&trans_key);

        let mut comm_reply = self.scheduler.comm_chan_alloc_buf();
        comm_reply.set_payload(0);

        comm_reply.set_header(DocaCommHeaderMeta{
            info_type: doca_comm_info_type::REPLY,
            info_id:   doca_comm_info_id::LOCAL_RELEASE_INFO,
            info_payload: 0,
            info_pid: info_pid,
            info_cid: info_cid,
        });

        self.scheduler.block_send_info(&mut comm_reply);
        self.scheduler.comm_chan_dealloc_buf(comm_reply);
    }

    pub fn local_abort_info_handler(
        &self,
        buf: &DocaCommBuf,
        info_payload: u32,
        info_pid: u32,
        info_cid: u32,
    ) {
        let trans_key = TransKey::new_raw(info_pid, self.tid, info_cid);
        let buf_count = self.trans_view.get_write_range_num(&trans_key);
    
        let lock_content = LockContent::new(info_pid as _, self.tid as _, info_cid);

        for i in 0..buf_count {
            let write_buf = self.trans_view.block_get_write_buf(&trans_key, i, 0);

            for item in write_buf.iter() {
                if item.insert {
                    self.memdb.local_erase(item.table_id, item.key);
                } else {
                    self.memdb.local_unlock(
                        item.table_id, 
                        item.key, 
                        lock_content.to_content(),
                    );
                }
                
                self.memdb.local_unlock(item.table_id, item.key, lock_content.to_content());
            }
        }

        self.trans_view.end_write_trans(&trans_key);

        let mut comm_reply = self.scheduler.comm_chan_alloc_buf();
        comm_reply.set_payload(0);

        comm_reply.set_header(DocaCommHeaderMeta{
            info_type: doca_comm_info_type::REPLY,
            info_id:   doca_comm_info_id::LOCAL_ABORT_INFO,
            info_payload: 0,
            info_pid: info_pid,
            info_cid: info_cid,
        });

        self.scheduler.block_send_info(&mut comm_reply);
        self.scheduler.comm_chan_dealloc_buf(comm_reply);
    }
}