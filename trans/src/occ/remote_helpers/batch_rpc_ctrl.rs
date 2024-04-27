use std::sync::Arc;
use std::collections::HashMap;

use crate::{MAX_REQ_SIZE, MAX_RESP_SIZE};
use crate::framework::scheduler::AsyncScheduler;
use crate::framework::rpc::*;

use super::BatchRpcReqHeader;
use super::batch_rpc_msg_wrapper::BatchRpcReqWrapper;

struct BatchRpcReq {
    pub peer_id:  u64,
    pub peer_tid: u64,
    pub rpc_id:   u32,
    
    pub wrapper:  BatchRpcReqWrapper,
    pub req_num:  usize
}

impl BatchRpcReq {
    pub fn new(peer_id: u64, peer_tid: u64, rpc_id: u32, req_buf: *mut u8) -> Self {
        Self {
            peer_id:  peer_id,
            peer_tid: peer_tid,
            rpc_id:   rpc_id,
            wrapper:  BatchRpcReqWrapper::new(req_buf, MAX_REQ_SIZE - 4),
            req_num:  0,
        }
    }
}

#[derive(Eq, PartialEq)]
enum BatchRpcStatus {
    BatchUninit,
    BatchPendingReq,
    BatchWaitingResp,
}

#[derive(Hash, Eq, PartialEq)]
struct PeerReqKey {
    peer_id:  u64,
    peer_tid: u64,
    rpc_id:   u32,
}

impl PeerReqKey {
    pub fn new(peer_id: u64, peer_tid: u64, rpc_id: u32) -> Self {
        Self {
            peer_id:  peer_id,
            peer_tid: peer_tid,
            rpc_id:   rpc_id
        }
    }
}

// req bufs are shared between coroutines, so it doesn't matter.
pub struct BatchRpcCtrl {
    status:    BatchRpcStatus,
    cid:       u32,
    scheduler: Arc<AsyncScheduler>,
    req_msgs:  Vec<BatchRpcReq>,
    peer_map:  HashMap<PeerReqKey, usize>,
    resp_buf:  Option<usize>,
}

impl BatchRpcCtrl 
{   
    pub fn new(scheduler: &Arc<AsyncScheduler>, cid: u32) -> Self {
        Self {
            status:    BatchRpcStatus::BatchUninit,
            cid:       cid,
            scheduler: scheduler.clone(),
            req_msgs:  Vec::new(),
            peer_map:  HashMap::new(),
            resp_buf:  None
        }
    }
    
    pub fn restart_batch(&mut self) {
        self.req_msgs.clear();
        self.peer_map.clear();
        self.resp_buf = None;

        self.status = BatchRpcStatus::BatchPendingReq;
    }
    
    pub fn send_batch_reqs(&mut self) {
        if self.status != BatchRpcStatus::BatchPendingReq {
            return;
        }

        let resp_buf = self.scheduler.get_reply_buf(self.cid);
        self.resp_buf = Some(resp_buf as usize);

        self.scheduler.prepare_multi_replys(self.cid, resp_buf, self.req_msgs.len() as u32);

        for i in 0..self.req_msgs.len() {
            let peer_id = self.req_msgs[i].peer_id;
            let req_num = self.req_msgs[i].req_num as u32;

            self.req_msgs[i].wrapper.set_header(BatchRpcReqHeader{
                peer_id: peer_id,
                num:     req_num,
            });

            self.scheduler.append_pending_req(
                self.req_msgs[i].wrapper.get_buf(), 
                self.req_msgs[i].rpc_id, 
                self.req_msgs[i].wrapper.get_off() as u32, 
                self.cid, 
                rpc_msg_type::REQ, 
                self.req_msgs[i].peer_id, 
                self.req_msgs[i].peer_tid,
            )
        }

        self.scheduler.flush_pending();

        self.status = BatchRpcStatus::BatchWaitingResp;
    }

    #[inline]
    fn get_proper_buffer(&mut self, key: PeerReqKey, msg_len: usize) -> usize {
        let mut msg_idx = -1;
        let search = self.peer_map.get(&key);
        
        if let Some(idx) = search {
            let req_msg = &self.req_msgs[*idx];
            if req_msg.wrapper.get_off() + msg_len + 4 < MAX_REQ_SIZE {
                msg_idx = *idx as i32;
            }
        }

        if msg_idx < 0 {
            let new_buf = self.scheduler.get_req_buf(self.cid);
            self.req_msgs.push(BatchRpcReq::new(
                key.peer_id,
                key.peer_tid,
                key.rpc_id,
                new_buf,
            ));
            msg_idx = (self.req_msgs.len()-1) as i32;
            self.peer_map.insert(key, msg_idx as usize);
        }

        return msg_idx as usize;
    }

    pub fn append_req<T:  Clone + Send + Sync>(&mut self, msg: &T, peer_id: u64, peer_tid: u64, rpc_id: u32) {
        if self.status != BatchRpcStatus::BatchPendingReq {
            return;
        }
        
        let key = PeerReqKey::new(peer_id, peer_tid, rpc_id);
        let msg_len = std::mem::size_of::<T>();

        let msg_idx = self.get_proper_buffer(key, msg_len);

        self.req_msgs[msg_idx].wrapper.set_item::<T>(msg.clone());
        self.req_msgs[msg_idx].req_num += 1;

        self.req_msgs[msg_idx].wrapper.shift_to_next_item::<T>(0);
    }

    pub fn append_req_with_data<T:  Clone + Send + Sync>(&mut self, msg: &T, data_ptr: *const u8, extra_len: usize, peer_id: u64, peer_tid: u64, rpc_id: u32) {
        if self.status != BatchRpcStatus::BatchPendingReq {
            return;
        }
        
        let key = PeerReqKey::new(peer_id, peer_tid, rpc_id);
        let msg_len = std::mem::size_of::<T>() + extra_len;

        let msg_idx = self.get_proper_buffer(key, msg_len);

        self.req_msgs[msg_idx].wrapper.set_item::<T>(msg.clone());
        let dst_ptr = self.req_msgs[msg_idx].wrapper.get_extra_data_raw_ptr::<T>();
        unsafe {
            std::ptr::copy_nonoverlapping(data_ptr, dst_ptr, extra_len);
        }

        self.req_msgs[msg_idx as usize].req_num += 1;
        self.req_msgs[msg_idx].wrapper.shift_to_next_item::<T>(extra_len);
    }

    pub async fn wait_until_done(&mut self) {
        self.scheduler.yield_until_ready(self.cid).await;
    }

    pub fn get_resp_buf_num(&mut self) -> Option<(*mut u8, usize)> {
        if let Some(buf) = self.resp_buf {
            return Some((buf as *mut u8, self.req_msgs.len()));
        }
        return None;
    }
}