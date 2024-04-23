use std::alloc::Layout;
use std::sync::Arc;

use crate::rdma::RdmaBaseAllocator;

use crate::{MAX_INFLIGHT_REPLY, MAX_INFLIGHT_REQS_PER_ROUTINE, MAX_REQ_SIZE, MAX_RESP_SIZE};

// reused buffer for rpc
pub struct RpcBufAllocator {
    coroutine_num: u32,
    req_buf_pool: Vec<Vec<*mut u8>>,
    req_heads: Vec<u32>,
    reply_buf_pool: Vec<*mut u8>,
    reply_heads: u32,
}

impl RpcBufAllocator {
    pub fn new(coroutine_num: u32, allocator: &Arc<RdmaBaseAllocator>) -> Self {
        let mut req_bufs = Vec::new();
        let mut req_heads = Vec::new();
        let mut reply_bufs = Vec::new();

        let req_layout =
            Layout::from_size_align(MAX_REQ_SIZE, std::mem::size_of::<usize>()).unwrap();
        let resp_layout =
            Layout::from_size_align(MAX_RESP_SIZE, std::mem::size_of::<usize>()).unwrap();

        for _ in 0..coroutine_num {
            let mut reqs = Vec::new();
            for _ in 0..MAX_INFLIGHT_REQS_PER_ROUTINE {
                let req_addr = unsafe { allocator.alloc(req_layout) };
                reqs.push(req_addr);
            }

            req_bufs.push(reqs);
            req_heads.push(0);
        }

        for _ in 0..MAX_INFLIGHT_REPLY {
            let reply_addr = unsafe { allocator.alloc(resp_layout) };
            reply_bufs.push(reply_addr);
        }

        Self {
            coroutine_num: coroutine_num,
            req_buf_pool: req_bufs,
            req_heads: req_heads,
            reply_buf_pool: reply_bufs,
            reply_heads: coroutine_num,
        }
    }

    pub fn get_reply_buf(&mut self, cid: u32) -> *mut u8 {
        // 目前工作协程只会用到一个 reply buf, 因此不参与循环以减少冲突的可能性
        if cid != 0 {
            return self.reply_buf_pool[cid as usize];
        }
        let buf = *self
            .reply_buf_pool
            .get::<usize>(self.reply_heads as _)
            .unwrap();
        self.reply_heads += 1;
        if self.reply_heads >= MAX_INFLIGHT_REPLY as u32 {
            self.reply_heads = self.coroutine_num;
        }
        buf
    }

    pub fn get_req_buf(&mut self, cid: u32) -> *mut u8 {
        let buf = self.req_buf_pool[cid as usize][self.req_heads[cid as usize] as usize];
        self.req_heads[cid as usize] += 1;
        if self.req_heads[cid as usize] >= MAX_INFLIGHT_REQS_PER_ROUTINE as u32 {
            self.req_heads[cid as usize] = 0;
        }

        buf
    }
}
