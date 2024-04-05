use std::sync::Arc;

use crate::framework::scheduler::AsyncScheduler;
use crate::framework::rpc::*;
use crate::memstore::memdb::MemDB;
use crate::rdma::rcconn::RdmaRcConn;
use crate::MAX_RESP_SIZE;

use super::BatchRpcReqHeader;
use super::BatchRpcRespHeader;
use super::BatchRpcReduceResp;

use super::batch_rpc_msg_wrapper::BatchRpcReqWrapper;
use super::batch_rpc_msg_wrapper::BatchRpcRespWrapper;
use super::super::occ::LockContent;

pub mod occ_rpc_id {
    pub type Type = u32;
    #[allow(unused)]
    pub const IGNORE_RPC:     Type = 0;
    pub const READ_RPC:       Type = 1;
    pub const FETCHWRITE_RPC: Type = 2;
    pub const LOCK_RPC:       Type = 3;
    pub const VALIDATE_RPC:   Type = 4;
    pub const COMMIT_RPC:     Type = 5;
    pub const RELEASE_RPC:    Type = 6;
    pub const ABORT_RPC:      Type = 7;
}

#[repr(C)]
#[derive(Clone)]
pub struct ReadReqItem {
    pub(crate) table_id: usize,
    pub(crate) key:      u64,
    pub(crate) read_idx: usize,
}

#[repr(C)]
#[derive(Clone)]
pub struct ReadRespItem {
    pub(crate) read_idx: usize,
    pub(crate) seq:      u64,
    pub(crate) length:   usize,
}

#[repr(C)]
#[derive(Clone)]
pub struct FetchWriteReqItem {
    pub(crate) table_id:   usize,
    pub(crate) key:        u64,
    pub(crate) update_idx: usize,
}

#[repr(C)]
#[derive(Clone)]
pub struct FetchWriteRespItem {
    pub(crate) update_idx: usize,
    pub(crate) seq:        u64,
    pub(crate) length:     usize,
}

#[repr(C)]
#[derive(Clone)]
pub struct LockReqItem {
    pub(crate) table_id: usize,
    pub(crate) key:      u64,
}

#[repr(C)]
#[derive(Clone)]
pub struct ValidateReqItem {
    pub(crate) table_id: usize,
    pub(crate) key:      u64,
    pub(crate) old_seq:  u64,
}

#[repr(C)]
#[derive(Clone)]
pub struct CommitReqItem {
    pub(crate) table_id: usize,
    pub(crate) key:      u64,
    pub(crate) length:   u32, // flexible length, zero means erase
}

#[repr(C)]
#[derive(Clone)]
pub struct ReleaseReqItem {
    pub(crate) table_id: usize,
    pub(crate) key:      u64,
}

#[repr(C)]
#[derive(Clone)]
pub struct AbortReqItem {
    pub(crate) table_id: usize,
    pub(crate) key:      u64,
    pub(crate) insert:   bool,
}

pub struct BatchRpcProc<'worker> {
    memdb:     Arc<MemDB<'worker>>,
    scheduler: Arc<AsyncScheduler<'worker>>,
}

impl<'worker> BatchRpcProc<'worker> {
    pub fn new(memdb: &Arc<MemDB<'worker>>, scheduler: &Arc<AsyncScheduler<'worker>>) -> Self {
        Self {
            memdb: memdb.clone(),
            scheduler: scheduler.clone(),
        }
    }
}

impl<'worker> BatchRpcProc<'worker> {
    #[allow(unused)]
    pub fn read_rpc_handler(
        &self,
        src_conn: &mut RdmaRcConn,
        msg: *mut u8,
        size: u32,
        meta: RpcProcessMeta
    ) {
        let mut req_wrapper = BatchRpcReqWrapper::new(msg, size as _);
        let resp_buf = self.scheduler.get_reply_buf();
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
        let resp_buf = self.scheduler.get_reply_buf();
        let mut resp_wrapper = BatchRpcRespWrapper::new(resp_buf, MAX_RESP_SIZE - 4);

        let req_header = req_wrapper.get_header();

        let lock_content = LockContent::new(meta.peer_id, meta.rpc_cid);

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

    pub fn lock_rpc_handler(
        &self,
        src_conn: &mut RdmaRcConn,
        msg: *mut u8,
        size: u32,
        meta: RpcProcessMeta
    ) {
        let mut req_wrapper = BatchRpcReqWrapper::new(msg, size as _);
        let resp_buf = self.scheduler.get_reply_buf();

        let req_header = req_wrapper.get_header();

        let mut success = true;
        let lock_content = LockContent::new(meta.peer_id, meta.rpc_cid);

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
        let resp_buf = self.scheduler.get_reply_buf();

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
        let resp_buf = self.scheduler.get_reply_buf();

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
        let resp_buf = self.scheduler.get_reply_buf();

        let req_header = req_wrapper.get_header();

        let lock_content = LockContent::new(meta.peer_id, meta.rpc_cid);

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
        let resp_buf = self.scheduler.get_reply_buf();

        let req_header = req_wrapper.get_header();

        let lock_content = LockContent::new(meta.peer_id, meta.rpc_cid);

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