use std::sync::Arc;
use std::collections::HashMap;

use crate::{MAX_REQ_SIZE, MAX_RESP_SIZE};
use crate::framework::scheduler::AsyncScheduler;
use crate::framework::rpc::*;

use super::BatchRpcReqHeader;

struct BatchRpcReq {
    pub peer_id:  u64,
    pub peer_tid: u64,
    pub rpc_id:   u32,
    pub req_buf:  usize,
    pub req_len:  usize,
    pub req_num:  usize,
}

impl BatchRpcReq {
    pub fn new(peer_id: u64, peer_tid: u64, rpc_id: u32, req_buf: usize, req_len: usize) -> Self {
        Self {
            peer_id:  peer_id,
            peer_tid: peer_tid,
            rpc_id:   rpc_id,
            req_buf:  req_buf,
            req_len:  req_len,
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
pub struct BatchRpcCtrl<'trans> {
    status:    BatchRpcStatus,
    scheduler: Arc<AsyncScheduler<'trans>>,
    req_msgs:  Vec<BatchRpcReq>,
    peer_map:  HashMap<PeerReqKey, usize>,
    resp_buf:  Option<usize>,
}

impl<'trans> BatchRpcCtrl<'trans> 
{   
    pub fn new(scheduler: &Arc<AsyncScheduler<'trans>>) -> Self {
        Self {
            status:    BatchRpcStatus::BatchUninit,
            scheduler: scheduler.clone(),
            req_msgs:  Vec::new(),
            peer_map:  HashMap::new(),
            resp_buf:  None
        }
    }
    
    pub fn reset_batch(&mut self) {
        self.req_msgs.clear();
        self.peer_map.clear();
        self.resp_buf = None;

        self.status = BatchRpcStatus::BatchUninit;
    }

    pub fn start_batch(&mut self) {
        self.status = BatchRpcStatus::BatchPendingReq;
    }
    
    pub fn send_batch_reqs(&mut self, cid: u32) {
        if self.status != BatchRpcStatus::BatchPendingReq {
            return;
        }

        let resp_buf = self.scheduler.get_reply_buf();
        self.resp_buf = Some(resp_buf as usize);

        self.scheduler.prepare_multi_replys(cid, resp_buf, self.req_msgs.len() as u32);

        for i in 0..self.req_msgs.len() {
            let msg = self.req_msgs[i].req_buf as *mut u8;

            let header = unsafe { (msg as *mut BatchRpcReqHeader).as_mut().unwrap() };
            header.num = self.req_msgs[i].req_num as _;

            self.scheduler.append_pending_req(
                msg, 
                self.req_msgs[i].rpc_id, 
                self.req_msgs[i].req_len as _, 
                cid, 
                rpc_msg_type::REQ, 
                self.req_msgs[i].peer_id, 
                self.req_msgs[i].peer_tid,
            )
        }

        self.scheduler.flush_pending();

        self.status = BatchRpcStatus::BatchWaitingResp;
    }

    pub fn append_req<T:  Clone + Send + Sync>(&mut self, msg: &T, cid: u32, peer_id: u64, peer_tid: u64, rpc_id: u32) {
        if self.status != BatchRpcStatus::BatchPendingReq {
            return;
        }
        
        let key = PeerReqKey::new(peer_id, peer_tid, rpc_id);
        let msg_len = std::mem::size_of::<T>();

        let mut msg_idx = -1;
        let search = self.peer_map.get(&key);
        
        if let Some(idx) = search {
            let req_msg = &self.req_msgs[*idx];
            if req_msg.req_len + msg_len + 4 < MAX_REQ_SIZE {
                msg_idx = *idx as i32;
            }
        }

        if msg_idx < 0 {
            let new_buf = self.scheduler.get_req_buf(cid) as usize;
            self.req_msgs.push(BatchRpcReq::new(
                peer_id,
                peer_tid,
                rpc_id,
                new_buf,
                std::mem::size_of::<BatchRpcReqHeader>())
            );
            msg_idx = (self.req_msgs.len()-1) as i32;
            self.peer_map.insert(key, msg_idx as usize);
        }

        let req_head = unsafe { (self.req_msgs[msg_idx as usize].req_buf as *mut T).as_mut().unwrap() };
        *req_head = msg.clone();
        self.req_msgs[msg_idx as usize].req_len += msg_len;
        self.req_msgs[msg_idx as usize].req_num += 1;

    }

    pub fn get_resp_buf_num(&mut self) -> Option<(*mut u8, usize)> {
        if let Some(buf) = self.resp_buf {
            return Some((buf as *mut u8, self.req_msgs.len()));
        }
        return None;
    }
}