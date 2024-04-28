use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;
use std::cell::UnsafeCell;
use log::debug;

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
use super::super::remote_helpers::*;
use super::super::remote_helpers::batch_rpc_msg_wrapper::{ BatchRpcReqWrapper, BatchRpcRespWrapper };

pub struct DpuRpcProc {
    pub tid:        u32,
    pub memdb:      Arc<MemDB>,
    pub scheduler:  Arc<AsyncScheduler>,
    pub trans_view: UnsafeCell<TransCacheView>,
    pub reply_bufs: UnsafeCell<HashMap<u32, DocaCommBuf>>
}

unsafe impl Send for DpuRpcProc {}
unsafe impl Sync for DpuRpcProc {}

impl DpuRpcProc {
    pub fn new(tid: u32, memdb: &Arc<MemDB>, scheduler: &Arc<AsyncScheduler>) -> Self {
        Self {
            tid:  tid,
            memdb: memdb.clone(),
            scheduler: scheduler.clone(),
            trans_view: UnsafeCell::new(TransCacheView::new(scheduler)),
            reply_bufs: UnsafeCell::new(HashMap::new())
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
        info_tid: u32,
        info_cid: u32,
    ) {
        if info_tid != self.tid {
            panic!("what the fuck {} , {} ", info_tid, self.tid);
        }
        
        let count = info_payload as usize / std::mem::size_of::<LocalReadInfoItem>();

        let trans_key = TransKey::new_raw(info_pid, self.tid, info_cid);
        let trans_view = unsafe { self.trans_view.get().as_mut().unwrap() };
        trans_view.start_read_trans(&trans_key);
        let mut read_cache_writer = trans_view.new_read_cache_writer(&trans_key, MAIN_ROUTINE_ID);

        for i in 0..count {
            let item = unsafe { buf.get_item::<LocalReadInfoItem>(i) };
            let meta = self.memdb.local_get_meta(
                item.table_id, 
                item.key
            ).unwrap();

            read_cache_writer.block_append_item(trans_view, CacheReadSetItem{
                table_id: item.table_id, 
                key:      item.key,
                old_seq:  meta.seq as u64,
            });
        }

        read_cache_writer.block_sync_buf(trans_view);

        let reply_bufs = unsafe { self.reply_bufs.get().as_mut().unwrap() };
        if let None = reply_bufs.get_mut(&info_cid) {
            reply_bufs.insert(info_cid, self.scheduler.comm_chan_alloc_buf(0));
        }

        let comm_reply = reply_bufs.get_mut(&info_cid).unwrap();
        comm_reply.set_payload(0);
        unsafe { comm_reply.append_item(DocaCommReply { success: true }); }

        comm_reply.set_header(DocaCommHeaderMeta{
            info_type: doca_comm_info_type::REPLY as _,
            info_id:   doca_comm_info_id::LOCAL_READ_INFO as _,
            info_payload: std::mem::size_of::<DocaCommReply>() as _,
            info_pid: info_pid as _,
            info_tid: self.tid as _,
            info_cid: info_cid as _,
        });

        self.scheduler.block_send_info(comm_reply);
    }

    pub fn local_lock_info_handler(
        &self,
        buf: &DocaCommBuf,
        info_payload: u32,
        info_pid: u32,
        info_tid: u32,
        info_cid: u32,
    ) {
        if info_tid != self.tid {
            panic!("what the fuck {} , {} ", info_tid, self.tid);
        }
        
        let count = info_payload as usize / std::mem::size_of::<LocalLockInfoItem>();
    
        let trans_key = TransKey::new_raw(info_pid, self.tid, info_cid);
        let trans_view = unsafe { self.trans_view.get().as_mut().unwrap() };
        trans_view.start_write_trans(&trans_key);
        let mut write_cache_writer = trans_view.new_write_cache_writer(&trans_key, MAIN_ROUTINE_ID);

        let mut success = true;
        let lock_content = LockContent::new(info_pid as _, self.tid as _, info_cid);

        for i in 0..count {
            let item = unsafe { buf.get_item::<LocalLockInfoItem>(i) };
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
            write_cache_writer.block_append_item(trans_view, CacheWriteSetItem{
                table_id: item.table_id, 
                key:      item.key,
                insert:   (meta.seq == 2),
            })
        }

        write_cache_writer.block_sync_buf(trans_view);

        let reply_bufs = unsafe { self.reply_bufs.get().as_mut().unwrap() };
        if let None = reply_bufs.get_mut(&info_cid) {
            reply_bufs.insert(info_cid, self.scheduler.comm_chan_alloc_buf(0));
        }

        let comm_reply = reply_bufs.get_mut(&info_cid).unwrap();
        comm_reply.set_payload(0);
        unsafe { comm_reply.append_item(DocaCommReply { success: success }); }

        comm_reply.set_header(DocaCommHeaderMeta{
            info_type: doca_comm_info_type::REPLY as _,
            info_id:   doca_comm_info_id::LOCAL_LOCK_INFO as _,
            info_payload: std::mem::size_of::<DocaCommReply>() as _,
            info_pid: info_pid as _,
            info_tid: self.tid as _,
            info_cid: info_cid as _,
        });

        self.scheduler.block_send_info(comm_reply);
    }

    pub fn local_validate_info_handler(
        &self,
        buf: &DocaCommBuf,
        info_payload: u32,
        info_pid: u32,
        info_tid: u32,
        info_cid: u32,
    ) {

        if info_tid != self.tid {
            panic!("what the fuck {} , {} ", info_tid, self.tid);
        }

        let trans_key = TransKey::new_raw(info_pid, self.tid, info_cid);
        let trans_view = unsafe { self.trans_view.get().as_mut().unwrap() };
        let buf_count = trans_view.get_read_range_num(&trans_key);

        let mut success = true;

        for i in 0..buf_count {
            let read_buf = trans_view.block_get_read_buf(&trans_key, i, 0);

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

        trans_view.end_read_trans(&trans_key);

        let reply_bufs = unsafe { self.reply_bufs.get().as_mut().unwrap() };
        if let None = reply_bufs.get_mut(&info_cid) {
            reply_bufs.insert(info_cid, self.scheduler.comm_chan_alloc_buf(0));
        }

        let comm_reply = reply_bufs.get_mut(&info_cid).unwrap();
        comm_reply.set_payload(0);
        unsafe { comm_reply.append_item(DocaCommReply { success: success }); }

        comm_reply.set_header(DocaCommHeaderMeta{
            info_type: doca_comm_info_type::REPLY as _,
            info_id:   doca_comm_info_id::LOCAL_VALIDATE_INFO as _,
            info_payload: std::mem::size_of::<DocaCommReply>() as _,
            info_pid: info_pid as _,
            info_tid: self.tid as _,
            info_cid: info_cid as _,
        });

        self.scheduler.block_send_info(comm_reply);
    }

    pub fn local_release_info_handler(
        &self,
        buf: &DocaCommBuf,
        info_payload: u32,
        info_pid: u32,
        info_tid: u32,
        info_cid: u32,
    ) {
        if info_tid != self.tid {
            panic!("what the fuck {} , {} ", info_tid, self.tid);
        }
        
        let trans_key = TransKey::new_raw(info_pid, self.tid, info_cid);
        let trans_view = unsafe { self.trans_view.get().as_mut().unwrap() };
        let buf_count = trans_view.get_write_range_num(&trans_key);

        let lock_content = LockContent::new(info_pid as _, self.tid as _, info_cid);

        // unlock
        for i in 0..buf_count {
            let write_buf = trans_view.block_get_write_buf(&trans_key, i, 0);

            for item in write_buf.iter() {
                self.memdb.local_unlock(item.table_id, item.key, lock_content.to_content());
            }
        }

        trans_view.end_write_trans(&trans_key);

        let reply_bufs = unsafe { self.reply_bufs.get().as_mut().unwrap() };
        if let None = reply_bufs.get_mut(&info_cid) {
            reply_bufs.insert(info_cid, self.scheduler.comm_chan_alloc_buf(0));
        }

        let comm_reply = reply_bufs.get_mut(&info_cid).unwrap();
        comm_reply.set_payload(0);

        comm_reply.set_header(DocaCommHeaderMeta{
            info_type: doca_comm_info_type::REPLY as _,
            info_id:   doca_comm_info_id::LOCAL_RELEASE_INFO as _,
            info_payload: 0,
            info_pid: info_pid as _,
            info_tid: self.tid as _,
            info_cid: info_cid as _,
        });

        self.scheduler.block_send_info(comm_reply);
    }

    pub fn local_abort_info_handler(
        &self,
        buf: &DocaCommBuf,
        info_payload: u32,
        info_pid: u32,
        info_tid: u32,
        info_cid: u32,
    ) {
        if info_tid != self.tid {
            panic!("what the fuck {} , {} ", info_tid, self.tid);
        }
        
        let trans_key = TransKey::new_raw(info_pid, self.tid, info_cid);
        let trans_view = unsafe { self.trans_view.get().as_mut().unwrap() };
        let buf_count = trans_view.get_write_range_num(&trans_key);
    
        let lock_content = LockContent::new(info_pid as _, self.tid as _, info_cid);

        for i in 0..buf_count {
            let write_buf = trans_view.block_get_write_buf(&trans_key, i, 0);

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

        trans_view.end_write_trans(&trans_key);

        let reply_bufs = unsafe { self.reply_bufs.get().as_mut().unwrap() };
        if let None = reply_bufs.get_mut(&info_cid) {
            reply_bufs.insert(info_cid, self.scheduler.comm_chan_alloc_buf(0));
        }

        let comm_reply = reply_bufs.get_mut(&info_cid).unwrap();
        comm_reply.set_payload(0);

        comm_reply.set_header(DocaCommHeaderMeta{
            info_type: doca_comm_info_type::REPLY as _,
            info_id:   doca_comm_info_id::LOCAL_ABORT_INFO as _,
            info_payload: 0,
            info_pid: info_pid as _,
            info_tid: self.tid as _,
            info_cid: info_cid as _,
        });

        self.scheduler.block_send_info(comm_reply);
    }
}

impl DpuRpcProc {
    pub fn read_cache_rpc_handler(
        &self,
        src_conn: &mut RdmaRcConn,
        msg: *mut u8,
        size: u32,
        meta: RpcProcessMeta
    ) {
        let mut req_wrapper = BatchRpcReqWrapper::new(msg, size as _);
        
        let mut comm_req = self.scheduler.comm_chan_alloc_buf(0);
        comm_req.set_payload(0);

        let trans_key = TransKey::new(self.tid, &meta);
        let trans_view = unsafe { self.trans_view.get().as_mut().unwrap() };
        trans_view.start_read_trans(&trans_key);
        let mut read_cache_writer = trans_view.new_read_cache_writer(&trans_key, MAIN_ROUTINE_ID);

        let req_header = req_wrapper.get_header();

        for i in 0..req_header.num {
            let req_item = req_wrapper.get_item::<ReadReqItem>();

            let meta = self.memdb.local_get_meta(
                req_item.table_id, 
                req_item.key, 
            ).unwrap();

            unsafe {
                comm_req.append_item(req_item.clone());
            }

            read_cache_writer.block_append_item(trans_view, CacheReadSetItem{
                table_id: req_item.table_id, 
                key:      req_item.key,
                old_seq:  meta.seq as u64,
            });

            req_wrapper.shift_to_next_item::<ReadReqItem>(0);
        }

        read_cache_writer.block_sync_buf(trans_view);

        let comm_payload = comm_req.get_payload();

        comm_req.set_header(DocaCommHeaderMeta{
            info_type: doca_comm_info_type::REQ as _,
            info_id:   doca_comm_info_id::REMOTE_READ_INFO as _,
            info_payload: comm_payload as _,
            info_pid: meta.peer_id as _,
            info_tid: self.tid as _,
            info_cid: meta.rpc_cid as _,
        });

        self.scheduler.block_send_info(&mut comm_req);
        self.scheduler.comm_chan_dealloc_buf(comm_req, 0);

    }

    pub fn fetch_write_cache_rpc_handler(
        &self,
        src_conn: &mut RdmaRcConn,
        msg: *mut u8,
        size: u32,
        meta: RpcProcessMeta
    ) {
        let mut req_wrapper = BatchRpcReqWrapper::new(msg, size as _);

        let mut comm_req = self.scheduler.comm_chan_alloc_buf(0);
        comm_req.set_payload(0);

        let trans_key = TransKey::new(self.tid, &meta);
        let trans_view = unsafe { self.trans_view.get().as_mut().unwrap() };
        trans_view.start_write_trans(&trans_key);
        let mut write_cache_writer = trans_view.new_write_cache_writer(&trans_key, MAIN_ROUTINE_ID);

        let req_header = req_wrapper.get_header();

        let mut lock_success = true;
        let lock_content = LockContent::new(meta.peer_id, self.tid as _, meta.rpc_cid);

        for i in 0..req_header.num {
            let req_item = req_wrapper.get_item::<FetchWriteReqItem>();

            let meta = self.memdb.local_lock(
                req_item.table_id, 
                req_item.key, 
                lock_content.to_content(),
            ).unwrap();

            if meta.lock != lock_content.to_content() {
                lock_success = false;
                break;
            } else {
                unsafe {
                    comm_req.append_item(req_item.clone());
                }

                write_cache_writer.block_append_item(trans_view, CacheWriteSetItem{
                    table_id: req_item.table_id, 
                    key:      req_item.key,
                    insert:   false,
                });
            }

            req_wrapper.shift_to_next_item::<FetchWriteReqItem>(0);
        }

        write_cache_writer.block_sync_buf(trans_view);

        // 加锁成功，则向上递交 read 请求
        if lock_success {
            let comm_payload = comm_req.get_payload();

            comm_req.set_header(DocaCommHeaderMeta{
                info_type: doca_comm_info_type::REQ as _,
                info_id:   doca_comm_info_id::REMOTE_FETCHWRITE_INFO as _,
                info_payload: comm_payload as _,
                info_pid: meta.peer_id as _,
                info_tid: self.tid as _,
                info_cid: meta.rpc_cid as _,
            });

            self.scheduler.block_send_info(&mut comm_req);
            self.scheduler.comm_chan_dealloc_buf(comm_req, 0);
            return;
        }
        // 否则，直接返回错误信息
        self.scheduler.comm_chan_dealloc_buf(comm_req, 0);

        let resp_buf = self.scheduler.get_reply_buf(0);
        let mut resp_wrapper = BatchRpcRespWrapper::new(resp_buf, MAX_RESP_SIZE - 4);

        for _ in 0..req_header.num {
            let req_item = req_wrapper.get_item::<FetchWriteReqItem>();
            resp_wrapper.set_item(FetchWriteCacheRespItem{
                update_idx: req_item.update_idx,
                length: 0,
            });

            req_wrapper.shift_to_next_item::<FetchWriteReqItem>(0);
            resp_wrapper.shift_to_next_item::<FetchWriteCacheRespItem>(0);
        }

        resp_wrapper.set_header(BatchRpcRespHeader {
            write: true,
            cid: meta.rpc_cid,
            num: req_header.num,
        });

        self.scheduler.send_reply(
            src_conn, 
            resp_buf, 
            occ_rpc_id::FETCHWRITE_RPC, 
            resp_wrapper.get_off() as _, 
            meta.rpc_cid, 
            meta.peer_id, 
            meta.peer_tid
        );
    }


    pub fn lock_cache_rpc_handler(
        &self,
        src_conn: &mut RdmaRcConn,
        msg: *mut u8,
        size: u32,
        meta: RpcProcessMeta
    ) {
        let mut req_wrapper = BatchRpcReqWrapper::new(msg, size as _);
        let resp_buf = self.scheduler.get_reply_buf(0);

        let trans_key = TransKey::new(self.tid, &meta);
        let trans_view = unsafe { self.trans_view.get().as_mut().unwrap() };
        trans_view.start_write_trans(&trans_key);
        let mut write_cache_writer = trans_view.new_write_cache_writer(&trans_key, MAIN_ROUTINE_ID);

        let req_header = req_wrapper.get_header();

        let mut success = true;
        let lock_content = LockContent::new(meta.peer_id, self.tid as _, meta.rpc_cid);

        for _ in 0..req_header.num {
            let req_item = req_wrapper.get_item::<LockReqItem>();

            let meta = self.memdb.local_lock(
                req_item.table_id, 
                req_item.key, 
                lock_content.to_content(),
            ).unwrap();

            if meta.lock != lock_content.to_content() {
                success = false;
                break;
            }

            // remain bugs !!! seq num !!!
            write_cache_writer.block_append_item(trans_view, CacheWriteSetItem{
                table_id: req_item.table_id, 
                key:      req_item.key,
                insert:   (meta.seq == 2),
            });

            req_wrapper.shift_to_next_item::<LockReqItem>(0);
        }

        write_cache_writer.block_sync_buf(trans_view);

        let reduce_resp = unsafe { (resp_buf as *mut BatchRpcReduceResp).as_mut().unwrap() };
        *reduce_resp = BatchRpcReduceResp{
            success: success
        };

        self.scheduler.send_reply(
            src_conn, 
            resp_buf, 
            occ_rpc_id::LOCK_RPC, 
            std::mem::size_of::<BatchRpcReduceResp>() as _, 
            meta.rpc_cid, 
            meta.peer_id, 
            meta.peer_tid
        );

    }

    pub fn validate_cache_rpc_handler(
        &self,
        src_conn: &mut RdmaRcConn,
        msg: *mut u8,
        size: u32,
        meta: RpcProcessMeta
    ) {
        let resp_buf = self.scheduler.get_reply_buf(0);

        let trans_key = TransKey::new(self.tid, &meta);
        let trans_view = unsafe { self.trans_view.get().as_mut().unwrap() };
        let buf_count = trans_view.get_read_range_num(&trans_key);

        let mut success = true;
        
        for i in 0..buf_count {
            let read_buf = trans_view.block_get_read_buf(&trans_key, i, meta.rpc_cid);

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

        trans_view.end_read_trans(&trans_key);

        let reduce_resp = unsafe { (resp_buf as *mut BatchRpcReduceResp).as_mut().unwrap() };
        *reduce_resp = BatchRpcReduceResp{
            success: success
        };

        self.scheduler.send_reply(
            src_conn, 
            resp_buf, 
            occ_rpc_id::VALIDATE_RPC, 
            std::mem::size_of::<BatchRpcReduceResp>() as _, 
            meta.rpc_cid, 
            meta.peer_id, 
            meta.peer_tid
        );
    }

    pub fn release_cache_rpc_handler(
        &self,
        src_conn: &mut RdmaRcConn,
        msg: *mut u8,
        size: u32,
        meta: RpcProcessMeta
    ) {
        // let mut req_wrapper = BatchRpcReqWrapper::new(msg, size as _);
        let resp_buf = self.scheduler.get_reply_buf(0);

        let trans_key = TransKey::new(self.tid, &meta);
        let trans_view = unsafe { self.trans_view.get().as_mut().unwrap() };
        let buf_count = trans_view.get_write_range_num(&trans_key);

        let lock_content = LockContent::new(meta.peer_id, self.tid as _, meta.rpc_cid);

        // unlock
        for i in 0..buf_count {
            let write_buf = trans_view.block_get_write_buf(&trans_key, i, 0);

            for item in write_buf.iter() {
                self.memdb.local_unlock(item.table_id, item.key, lock_content.to_content());
            }
        }

        trans_view.end_write_trans(&trans_key);

        self.scheduler.send_reply(
            src_conn, 
            resp_buf, 
            occ_rpc_id::RELEASE_RPC, 
            0, 
            meta.rpc_cid, 
            meta.peer_id, 
            meta.peer_tid
        );
    }

    pub fn abort_cache_rpc_handler(
        &self,
        src_conn: &mut RdmaRcConn,
        msg: *mut u8,
        size: u32,
        meta: RpcProcessMeta
    ) {
        // let mut req_wrapper = BatchRpcReqWrapper::new(msg, size as _);
        let resp_buf = self.scheduler.get_reply_buf(0);

        let trans_key = TransKey::new(self.tid, &meta);
        let trans_view = unsafe { self.trans_view.get().as_mut().unwrap() };
        let buf_count = trans_view.get_write_range_num(&trans_key);

        let lock_content = LockContent::new(meta.peer_id, self.tid as _, meta.rpc_cid);

        // unlock
        for i in 0..buf_count {
            let write_buf = trans_view.block_get_write_buf(&trans_key, i, 0);

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

        trans_view.end_write_trans(&trans_key);

        self.scheduler.send_reply(
            src_conn, 
            resp_buf, 
            occ_rpc_id::ABORT_RPC, 
            0, 
            meta.rpc_cid, 
            meta.peer_id, 
            meta.peer_tid
        );
    }
}