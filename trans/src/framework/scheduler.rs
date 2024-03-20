 use std::alloc::Layout;
use std::sync::{Arc, Weak};
use std::sync::{Mutex, RwLock};
use std::collections::HashMap;

use tokio;

use ll_alloc::LockedHeap;

use crate::rdma::RdmaRecvCallback;
use crate::rdma::rcconn::RdmaRcConn;
use crate::rdma::one_side::OneSideComm;
use crate::rdma::two_sides::TwoSidesComm;
use crate::rdma::RdmaSendCallback;
use crate::{MAX_RESP_SIZE, MAX_SEND_SIZE};

use super::rpc::AsyncRpc;
use super::rpc::RpcHeaderMeta;
use super::rpc::RpcHandler;
use super::rpc::RpcMsgType;

// process send / read / write saparately
// because send does not require polling but read / write need

pub struct AsyncScheduler<'a> {
    allocator:    Arc<LockedHeap>,
    conns:        HashMap<u64, Arc<Mutex<RdmaRcConn<'a>>>>,
    //  read / write (one-side primitives)
    // pending for coroutines
    pendings:     Mutex<Vec<u32>>,

    // rpcs (two-sides primitives)
    reply_bufs:   Mutex<Vec<*mut u8>>,
    reply_counts: Mutex<Vec<u32>>,

    // callbacks
    callbacks:    Vec<Weak<dyn RpcHandler + 'a>>
}

impl<'a> AsyncScheduler<'a> {
    pub fn new(routine_num: u64, allocator: &Arc<LockedHeap>) -> Self {
        Self {
            allocator:    allocator.clone(),
            conns:        HashMap::new(),
            pendings:     Mutex::new(Vec::new()),
            reply_bufs:   Mutex::new(Vec::new()),
            reply_counts: Mutex::new(Vec::new()),

            // callbacks
            callbacks:    Vec::new(),
        }
    }

    pub fn append_conn(&mut self, id: u64, conn: &Arc<Mutex<RdmaRcConn<'a>>>) {
        self.conns.insert(id, conn.clone());
    }

    pub fn register_callback(&mut self, callback: &Arc<impl RpcHandler + 'a>) {
        self.callbacks.push(Arc::downgrade(callback) as _);
    }

    pub fn poll_sends(&self) {
        let iter = self.conns.iter();
        for (peer_id, conn) in iter {
            conn.lock().unwrap().poll_send();
        }
    }
}

impl<'a> RdmaSendCallback for AsyncScheduler<'a> {
    fn rdma_send_handler(&self, wr_id: u64) {
        todo!();
        // update pending and signal
    }
}

// RPCs
impl<'a> AsyncScheduler<'a> {
    fn poll_recvs(&self) {
        // let mut pendings = self.pendings.lock().unwrap();
        let iter = self.conns.iter();
        for (peer_id, conn) in iter {
            conn.lock().unwrap().poll_recvs();
        }
    }

    fn prepare_msg_header(
        msg: *mut u8,
        rpc_id: u32,
        rpc_size: u32,
        rpc_cid: u32,
        rpc_type: u32,
    ) {
        let meta = RpcHeaderMeta::new(rpc_type, rpc_id, rpc_size, rpc_cid);
        unsafe {
            *(msg.byte_sub(4) as *mut u32) = meta.to_header();
        }
    }

    pub fn prepare_multi_replys(
        &self,
        cid:         u32,
        reply_buf:   *mut u8,
        reply_count: u32,
    ) {

    }

    pub fn free_reply_buf(&self, addr: *mut u8, size: usize) {
        let layout = Layout::from_size_align(MAX_RESP_SIZE, std::mem::align_of::<usize>()).unwrap();
        // unsafe { self.allocator.alloc(layout) }
        unsafe { self.allocator.dealloc(addr, layout) };
    }

    pub fn free_req_buf(&self, addr: *mut u8, size: usize) {
        let layout = Layout::from_size_align(MAX_SEND_SIZE, std::mem::align_of::<usize>()).unwrap();
        unsafe { self.allocator.dealloc(addr.wrapping_byte_sub(1), layout) };
    }
}

impl<'a> RdmaRecvCallback for AsyncScheduler<'a> {
    fn rdma_recv_handler(&self, src_conn :&mut RdmaRcConn, msg: *mut u8) {
        // todo!();
        let meta = RpcHeaderMeta::from_header(unsafe { *(msg as *mut u32) });

        match meta.rpc_type {
            RpcMsgType::REQ  => {
                let callback = self.callbacks.get::<usize>(meta.rpc_id as usize).unwrap();
                callback.upgrade().unwrap().rpc_handler(src_conn);
            },
            RpcMsgType::Y_REQ => {
                unimplemented!("yreq type is not allowed for time being");
            },
            RpcMsgType::RESP => {
                let index = meta.rpc_cid as usize;

                let buf = *self.reply_bufs.lock().unwrap().get::<usize>(index).unwrap();
                unsafe { std::ptr::copy_nonoverlapping(msg.byte_add(1), buf, meta.rpc_payload as _); }

                if let Some(mut_buf) = self.reply_bufs.lock().unwrap().get_mut::<usize>(index) {
                    *mut_buf = mut_buf.wrapping_add((meta.rpc_payload + 1) as usize);
                }

                if let Some(mut_count) = self.reply_counts.lock().unwrap().get_mut::<usize>(index) {
                    *mut_count -= 1;
                }
            },
            _ => {
                unimplemented!("rpc type");
            }
        }
    }
}

impl<'a> AsyncRpc for AsyncScheduler<'a> {
    fn get_reply_buf(&self) -> *mut u8 {
        // std::ptr::null_mut()
        let layout = Layout::from_size_align(MAX_RESP_SIZE, std::mem::align_of::<usize>()).unwrap();
        unsafe { self.allocator.alloc(layout) }
    }

    fn get_req_buf(&self) -> *mut u8 {
        let layout = Layout::from_size_align(MAX_SEND_SIZE, std::mem::align_of::<usize>()).unwrap();
        let buf = unsafe { self.allocator.alloc(layout) };
        buf.wrapping_add(1)
    }

    fn send_reply(
            src_conn: &mut RdmaRcConn,
            msg: *mut u8,
            rpc_id: u32,
            rpc_size: u32,
            rpc_cid: u32,
            peer_id: u64,
            peer_tid: u64,
        ) {
        Self::prepare_msg_header(msg, rpc_id, rpc_size, rpc_cid, RpcMsgType::RESP);
        src_conn.send_one(unsafe { msg.byte_sub(4) as _ }, rpc_size + 4);
    }

    fn append_pending_req(
            &self,
            msg: *mut u8,
            rpc_id: u32,
            rpc_size: u32,
            rpc_cid: u32,
            rpc_type: u32,
            peer_id: u64,
            peer_tid: u64,
        ) {
        // todo!();
        Self::prepare_msg_header(msg, rpc_id, rpc_size, rpc_cid, rpc_type);

        let conn = self.conns.get(&peer_id).unwrap();
        conn.lock().unwrap().send_pending(unsafe { msg.byte_sub(4) as _ }, rpc_size + 4).unwrap();
    }

    fn append_req(
            &self,
            msg: *mut u8,
            rpc_id: u32,
            rpc_size: u32,
            rpc_cid: u32,
            rpc_type: u32,
            peer_id: u64,
            peer_tid: u64,
        ) {
        // todo!();
        Self::prepare_msg_header(msg, rpc_id, rpc_size, rpc_cid, rpc_type);

        let conn = self.conns.get(&peer_id).unwrap();
        conn.lock().unwrap().send_one(unsafe { msg.byte_sub(4) as _ }, rpc_size + 4);
    }
}

// Async waiter
impl<'a> AsyncScheduler<'a> {
    pub async fn yield_now(&self) {
        tokio::task::yield_now().await;
    }

    pub async fn yield_until_ready(&self, cid: u32) {
        loop {
            let pendings = *self.pendings.lock().unwrap().get::<usize>(cid as _).unwrap();
            let reply_count = *self.reply_counts.lock().unwrap().get::<usize>(cid as _).unwrap();
            if pendings > 0 || reply_count > 0 {
                self.yield_now().await;
            }
        }
    }
}
