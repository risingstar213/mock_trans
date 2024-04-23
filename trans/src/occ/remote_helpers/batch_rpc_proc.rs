use std::sync::Arc;

use tokio::sync::mpsc;

use crate::framework::YieldReq;
use crate::framework::scheduler::AsyncScheduler;
use crate::framework::rpc::*;
use crate::memstore::memdb::MemDB;
use crate::occ::cache_helpers::trans_cache_view::TransCacheView;
use crate::occ::cache_helpers::trans_cache_view::TransKey;
use crate::rdma::rcconn::RdmaRcConn;
use crate::MAIN_ROUTINE_ID;
use crate::MAX_RESP_SIZE;

use super::*;

use super::batch_rpc_msg_wrapper::BatchRpcReqWrapper;
use super::batch_rpc_msg_wrapper::BatchRpcRespWrapper;
use super::super::occ::LockContent;
use super::super::cache_helpers::CacheReadSetItem;

pub struct BatchRpcProc {
    pub tid:        u32,
    pub memdb:      Arc<MemDB>,
    pub scheduler:  Arc<AsyncScheduler>,
    pub trans_view: TransCacheView
}

impl BatchRpcProc {
    pub fn new(tid: u32, memdb: &Arc<MemDB>, scheduler: &Arc<AsyncScheduler>) -> Self {
        Self {
            tid: tid,
            memdb: memdb.clone(),
            scheduler: scheduler.clone(),
            trans_view: TransCacheView::new(scheduler),
        }
    }
}

impl BatchRpcProc {
    pub fn read_rpc_handler(
        &self,
        src_conn: &mut RdmaRcConn,
        msg: *mut u8,
        size: u32,
        meta: RpcProcessMeta
    ) {
        let mut req_wrapper = BatchRpcReqWrapper::new(msg, size as _);
        let resp_buf = self.scheduler.get_reply_buf(0);
        let mut resp_wrapper = BatchRpcRespWrapper::new(resp_buf, MAX_RESP_SIZE - 4);

        let req_header = req_wrapper.get_header();

        for _ in 0..req_header.num {
            let req_item = req_wrapper.get_item::<ReadReqItem>();
            let data_len = self.memdb.get_item_length(req_item.table_id);

            let meta = self.memdb.local_get_readonly(
                req_item.table_id, 
                req_item.key, 
                resp_wrapper.get_extra_data_raw_ptr::<ReadRespItem>(), 
                data_len as u32,
            ).unwrap();

            resp_wrapper.set_item(ReadRespItem{
                read_idx: req_item.read_idx,
                seq:      meta.seq as u64,
                length:   data_len,
            });

            req_wrapper.shift_to_next_item::<ReadReqItem>(0);
            resp_wrapper.shift_to_next_item::<ReadRespItem>(data_len);
        }

        resp_wrapper.set_header(BatchRpcRespHeader {
            write: false,
            num: req_header.num,
        });

        self.scheduler.send_reply(
            src_conn, 
            resp_buf, 
            occ_rpc_id::READ_RPC, 
            resp_wrapper.get_off() as _, 
            meta.rpc_cid, 
            meta.peer_id, 
            meta.peer_tid
        );
        
    }

    pub fn fetch_write_rpc_handler(
        &self,
        src_conn: &mut RdmaRcConn,
        msg: *mut u8,
        size: u32,
        meta: RpcProcessMeta
    ) {
        let mut req_wrapper = BatchRpcReqWrapper::new(msg, size as _);
        let resp_buf = self.scheduler.get_reply_buf(0);
        let mut resp_wrapper = BatchRpcRespWrapper::new(resp_buf, MAX_RESP_SIZE - 4);

        let req_header = req_wrapper.get_header();

        let lock_content = LockContent::new(meta.peer_id, self.tid as _, meta.rpc_cid);

        for _ in 0..req_header.num {
            let req_item = req_wrapper.get_item::<FetchWriteReqItem>();
            let mut data_len = self.memdb.get_item_length(req_item.table_id);

            let meta = self.memdb.local_get_for_upd(
                req_item.table_id, 
                req_item.key, 
                resp_wrapper.get_extra_data_raw_ptr::<FetchWriteRespItem>(), 
                data_len as u32, 
                lock_content.to_content()
            ).unwrap();

            if meta.lock != lock_content.to_content() {
                data_len = 0;
                resp_wrapper.set_item(FetchWriteRespItem{
                    update_idx: req_item.update_idx,
                    seq: 0,
                    length: 0,
                });
            } else {
                resp_wrapper.set_item(FetchWriteRespItem{
                    update_idx: req_item.update_idx,
                    seq: meta.seq,
                    length: data_len,
                })
            }

            req_wrapper.shift_to_next_item::<FetchWriteReqItem>(0);
            resp_wrapper.shift_to_next_item::<FetchWriteRespItem>(data_len);
        }

        resp_wrapper.set_header(BatchRpcRespHeader {
            write: true,
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

    pub fn lock_rpc_handler(
        &self,
        src_conn: &mut RdmaRcConn,
        msg: *mut u8,
        size: u32,
        meta: RpcProcessMeta
    ) {
        let mut req_wrapper = BatchRpcReqWrapper::new(msg, size as _);
        let resp_buf = self.scheduler.get_reply_buf(0);

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

            req_wrapper.shift_to_next_item::<LockReqItem>(0);
        }

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

    pub fn validate_rpc_handler(
        &self,
        src_conn: &mut RdmaRcConn,
        msg: *mut u8,
        size: u32,
        meta: RpcProcessMeta
    ) {
        let mut req_wrapper = BatchRpcReqWrapper::new(msg, size as _);
        let resp_buf = self.scheduler.get_reply_buf(0);

        let req_header = req_wrapper.get_header();

        let mut success = true;      
        for _ in 0..req_header.num {
            let req_item = req_wrapper.get_item::<ValidateReqItem>();

            let meta = self.memdb.local_get_meta(
                req_item.table_id, 
                req_item.key
            ).unwrap();

            if meta.lock != 0 || (meta.seq != req_item.old_seq) {
                success = false;
                break;
            }

            req_wrapper.shift_to_next_item::<ValidateReqItem>(0);
        }

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

    pub fn commit_rpc_handler(
        &self,
        src_conn: &mut RdmaRcConn,
        msg: *mut u8,
        size: u32,
        meta: RpcProcessMeta
    ) {
        let mut req_wrapper = BatchRpcReqWrapper::new(msg, size as _);
        let resp_buf = self.scheduler.get_reply_buf(0);

        let req_header = req_wrapper.get_header();

        // modify
        for _ in 0..req_header.num {
            let req_item = req_wrapper.get_item::<CommitReqItem>();
            let data_len = req_item.length;

            if data_len == 0 {
                self.memdb.local_erase(req_item.table_id, req_item.key);
            } else {
                self.memdb.local_upd_val_seq(
                    req_item.table_id, 
                    req_item.key, 
                    req_wrapper.get_extra_data_const_ptr::<CommitReqItem>(), 
                    data_len,
                );
            }

            req_wrapper.shift_to_next_item::<CommitReqItem>(data_len as _);
        }

        // TODO: mark unlock and no neef release rpc

        self.scheduler.send_reply(
            src_conn, 
            resp_buf, 
            occ_rpc_id::COMMIT_RPC, 
            0, 
            meta.rpc_cid, 
            meta.peer_id, 
            meta.peer_tid
        );

    }

    pub fn release_rpc_handler(
        &self,
        src_conn: &mut RdmaRcConn,
        msg: *mut u8,
        size: u32,
        meta: RpcProcessMeta
    ) {
        let mut req_wrapper = BatchRpcReqWrapper::new(msg, size as _);
        let resp_buf = self.scheduler.get_reply_buf(0);

        let req_header = req_wrapper.get_header();

        let lock_content = LockContent::new(meta.peer_id, self.tid as _, meta.rpc_cid);

        // unlock
        for _ in 0..req_header.num {
            let req_item = req_wrapper.get_item::<ReleaseReqItem>();
            self.memdb.local_unlock(req_item.table_id, req_item.key, lock_content.to_content());
        
            req_wrapper.shift_to_next_item::<ReleaseReqItem>(0);
        }

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

    pub fn abort_rpc_handler(
        &self,
        src_conn: &mut RdmaRcConn,
        msg: *mut u8,
        size: u32,
        meta: RpcProcessMeta
    ) {
        let mut req_wrapper = BatchRpcReqWrapper::new(msg, size as _);
        let resp_buf = self.scheduler.get_reply_buf(0);

        let req_header = req_wrapper.get_header();

        let lock_content = LockContent::new(meta.peer_id, self.tid as _, meta.rpc_cid);

        for _ in 0..req_header.num {
            let req_item = req_wrapper.get_item::<AbortReqItem>();

            if req_item.insert {
                self.memdb.local_erase(req_item.table_id, req_item.key);
            } else {
                self.memdb.local_unlock(
                    req_item.table_id, 
                    req_item.key, 
                    lock_content.to_content(),
                );
            }

            req_wrapper.shift_to_next_item::<AbortReqItem>(0);
        }

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

impl BatchRpcProc {
    pub fn read_cache_rpc_handler(
        &self,
        src_conn: &mut RdmaRcConn,
        msg: *mut u8,
        size: u32,
        meta: RpcProcessMeta
    ) {
        let mut req_wrapper = BatchRpcReqWrapper::new(msg, size as _);
        let resp_buf = self.scheduler.get_reply_buf(0);
        let mut resp_wrapper = BatchRpcRespWrapper::new(resp_buf, MAX_RESP_SIZE - 4);
        
        let trans_key = TransKey::new(self.tid, &meta);
        self.trans_view.start_read_trans(&trans_key);
        let mut read_cache_writer = self.trans_view.new_read_cache_writer(&trans_key, MAIN_ROUTINE_ID);

        let req_header = req_wrapper.get_header();

        for _ in 0..req_header.num {
            let req_item = req_wrapper.get_item::<ReadReqItem>();
            let data_len = self.memdb.get_item_length(req_item.table_id);

            let meta = self.memdb.local_get_readonly(
                req_item.table_id, 
                req_item.key, 
                resp_wrapper.get_extra_data_raw_ptr::<ReadCacheRespItem>(), 
                data_len as u32,
            ).unwrap();

            resp_wrapper.set_item(ReadCacheRespItem{
                read_idx: req_item.read_idx,
                length:   data_len,
            });

            read_cache_writer.block_append_item(&self.trans_view, CacheReadSetItem{
                table_id: req_item.table_id, 
                key:      req_item.key,
                old_seq:  meta.seq as u64,
            });

            req_wrapper.shift_to_next_item::<ReadReqItem>(0);
            resp_wrapper.shift_to_next_item::<ReadCacheRespItem>(data_len);
        }

        read_cache_writer.block_sync_buf(&self.trans_view);

        resp_wrapper.set_header(BatchRpcRespHeader {
            write: false,
            num: req_header.num,
        });

        self.scheduler.send_reply(
            src_conn, 
            resp_buf, 
            occ_rpc_id::READ_RPC, 
            resp_wrapper.get_off() as _, 
            meta.rpc_cid, 
            meta.peer_id, 
            meta.peer_tid
        );
    }

    pub fn fetch_write_cache_rpc_handler(
        &self,
        src_conn: &mut RdmaRcConn,
        msg: *mut u8,
        size: u32,
        meta: RpcProcessMeta
    ) {
        let mut req_wrapper = BatchRpcReqWrapper::new(msg, size as _);
        let resp_buf = self.scheduler.get_reply_buf(0);
        let mut resp_wrapper = BatchRpcRespWrapper::new(resp_buf, MAX_RESP_SIZE - 4);

        let req_header = req_wrapper.get_header();

        let lock_content = LockContent::new(meta.peer_id, self.tid as _, meta.rpc_cid);

        for _ in 0..req_header.num {
            let req_item = req_wrapper.get_item::<FetchWriteReqItem>();
            let mut data_len = self.memdb.get_item_length(req_item.table_id);

            let meta = self.memdb.local_get_for_upd(
                req_item.table_id, 
                req_item.key, 
                resp_wrapper.get_extra_data_raw_ptr::<FetchWriteReqItem>(), 
                data_len as u32, 
                lock_content.to_content()
            ).unwrap();

            if meta.lock != lock_content.to_content() {
                data_len = 0;
                resp_wrapper.set_item(FetchWriteRespItem{
                    update_idx: req_item.update_idx,
                    seq: 0,
                    length: 0,
                });
            } else {
                resp_wrapper.set_item(FetchWriteRespItem{
                    update_idx: req_item.update_idx,
                    seq: meta.seq,
                    length: data_len,
                })
            }

            req_wrapper.shift_to_next_item::<FetchWriteReqItem>(0);
            resp_wrapper.shift_to_next_item::<FetchWriteRespItem>(data_len);
        }

        resp_wrapper.set_header(BatchRpcRespHeader {
            write: true,
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

    pub fn validate_cache_rpc_handler(
        &self,
        src_conn: &mut RdmaRcConn,
        msg: *mut u8,
        size: u32,
        meta: RpcProcessMeta
    ) {
        let resp_buf = self.scheduler.get_reply_buf(0);

        let trans_key = TransKey::new(self.tid, &meta);
        let buf_count = self.trans_view.get_read_range_num(&trans_key);

        let mut success = true;
        
        for i in 0..buf_count {
            let read_buf = self.trans_view.block_get_read_buf(&trans_key, i, meta.rpc_cid);

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


}