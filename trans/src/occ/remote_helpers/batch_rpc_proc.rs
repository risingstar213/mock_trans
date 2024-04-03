use std::sync::Arc;

use crate::framework::scheduler::AsyncScheduler;
use crate::framework::rpc::*;
use crate::memstore::memdb::MemDB;
use crate::rdma::rcconn::RdmaRcConn;
use crate::MAX_RESP_SIZE;

use super::BatchRpcReqHeader;
use super::BatchRpcRespHeader;

use super::batch_rpc_msg_wrapper::BatchRpcReqWrapper;
use super::batch_rpc_msg_wrapper::BatchRpcRespWrapper;
use super::super::occ::LockContent;

pub mod occ_rpc_id {
    pub type Type = u32;
    pub const IGNORE_RPC:     Type = 0;
    pub const READ_RPC:       Type = 1;
    pub const FETCHWRITE_RPC: Type = 2;
    pub const WRITE_RPC:      Type = 3;
    pub const VALIDATE_RPC:   Type = 4;
    pub const COMMIT_RPC:     Type = 5;
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
    pub(crate) lock_res:   bool,
    pub(crate) length:     usize,
}

#[repr(C)]
#[derive(Clone)]
pub struct WriteReqItem {
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
    pub(crate) length:   u32, // flexible length
}

pub struct BatchRpcProc<'worker> {
    scheduler: Arc<AsyncScheduler<'worker>>,
    memdb:     Arc<MemDB<'worker>>
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

        resp_wrapper.set_header(BatchRpcRespHeader { num: req_header.num });

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
            let data_len = self.memdb.get_item_length(req_item.table_id);

            let meta = self.memdb.local_get_for_upd(
                req_item.table_id, 
                req_item.key, 
                resp_wrapper.get_extra_data_raw_ptr::<FetchWriteReqItem>(), 
                data_len as u32, 
                lock_content.to_content()
            ).unwrap();
        }
    }
}