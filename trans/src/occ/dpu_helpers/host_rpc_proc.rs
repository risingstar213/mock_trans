use std::sync::Arc;

use crate::doca_comm_chan::comm_buf::DocaCommBuf;
use crate::doca_comm_chan::doca_comm_info_type;
use crate::doca_comm_chan::DocaCommHeaderMeta;
use crate::framework::scheduler;
use crate::framework::scheduler::AsyncScheduler;
use crate::framework::rpc::*;
use crate::memstore::memdb::ValueDB;
use crate::occ::occ::LockContent;
use crate::rdma::rcconn::RdmaRcConn;
use crate::MAIN_ROUTINE_ID;
use crate::MAX_RESP_SIZE;

use super::super::remote_helpers::*;
use super::super::remote_helpers::batch_rpc_msg_wrapper::{ BatchRpcReqWrapper, BatchRpcRespWrapper };

pub struct HostRpcProc {
    pub tid:        u32,
    pub valuedb:      Arc<ValueDB>,
    pub scheduler:  Arc<AsyncScheduler>,
}

impl HostRpcProc {
    pub fn new(tid: u32, valuedb: &Arc<ValueDB>, scheduler: &Arc<AsyncScheduler>) -> Self {
        Self {
            tid:  tid,
            valuedb: valuedb.clone(),
            scheduler: scheduler.clone(),
        }
    }
}

impl HostRpcProc {
    pub fn remote_read_info_handler(
        &self,
        buf: &DocaCommBuf,
        info_payload: u32,
        info_pid: u32,
        info_cid: u32,
    ) {
        let count = info_payload as usize / std::mem::size_of::<ReadReqItem>();
        let resp_buf = self.scheduler.get_reply_buf(0);
        let mut resp_wrapper = BatchRpcRespWrapper::new(resp_buf, MAX_RESP_SIZE - 4);
    
        for i in 0..count {
            let req_item = unsafe { buf.get_item::<ReadReqItem>(i) };
            let data_len = self.valuedb.get_item_length(req_item.table_id);

            self.valuedb.local_get_value(
                req_item.table_id,
                req_item.key,
                resp_wrapper.get_extra_data_raw_ptr::<ReadCacheRespItem>(),
                data_len as u32,
            );

            resp_wrapper.set_item(ReadCacheRespItem{
                read_idx: req_item.read_idx,
                length:   data_len,
            });

            resp_wrapper.shift_to_next_item::<ReadCacheRespItem>(data_len);
        }

        resp_wrapper.set_header(BatchRpcRespHeader {
            write: false,
            num: count as _,
        });

        self.scheduler.send_req(
            resp_buf, 
            occ_rpc_id::READ_RPC, 
            resp_wrapper.get_off() as _, 
            info_cid, 
            rpc_msg_type::RESP, 
            info_pid as _, 
            self.tid as _,
        );
    }

    pub fn remote_fetch_write_info_handler(
        &self,
        buf: &DocaCommBuf,
        info_payload: u32,
        info_pid: u32,
        info_cid: u32,
    ) {
        let count = info_payload as usize / std::mem::size_of::<FetchWriteReqItem>();
        let resp_buf = self.scheduler.get_reply_buf(0);
        let mut resp_wrapper = BatchRpcRespWrapper::new(resp_buf, MAX_RESP_SIZE - 4);
    
        for i in 0..count {
            let req_item = unsafe { buf.get_item::<FetchWriteReqItem>(i) };
            let data_len = self.valuedb.get_item_length(req_item.table_id);

            self.valuedb.local_get_value(
                req_item.table_id, 
                req_item.key, 
                resp_wrapper.get_extra_data_raw_ptr::<FetchWriteCacheRespItem>(), 
                data_len as u32,
            );

            resp_wrapper.set_item(FetchWriteCacheRespItem{
                update_idx: req_item.update_idx,
                length: data_len,
            });

            resp_wrapper.shift_to_next_item::<FetchWriteCacheRespItem>(data_len);
        }


        resp_wrapper.set_header(BatchRpcRespHeader {
            write: true,
            num: count as _,
        });

        self.scheduler.send_req(
            resp_buf, 
            occ_rpc_id::FETCHWRITE_RPC, 
            resp_wrapper.get_off() as _, 
            info_cid, 
            rpc_msg_type::RESP, 
            info_pid as _, 
            self.tid as _,
        );
    }
}

impl HostRpcProc {
    pub fn commit_cache_rpc_handler(
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
                self.valuedb.local_erase_value(req_item.table_id, req_item.key);
            } else {
                let success = self.valuedb.local_set_value(
                    req_item.table_id, 
                    req_item.key, 
                    req_wrapper.get_extra_data_const_ptr::<CommitReqItem>(), 
                    data_len,
                );

                if !success {
                    self.valuedb.local_put_value(
                        req_item.table_id, 
                        req_item.key, 
                        req_wrapper.get_extra_data_const_ptr::<CommitReqItem>(), 
                        data_len,
                    );
                }
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
}