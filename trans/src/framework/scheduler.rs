// use std::alloc::Layout;
use std::collections::HashMap;
use std::sync::{Arc, Weak};
use std::sync::{Mutex, RwLock};

use ll_alloc::LockedHeap;

use crate::rdma::rcconn::RdmaRcConn;
use crate::rdma::RdmaRecvCallback;
// use crate::rdma::one_side::OneSideComm;
use crate::rdma::two_sides::TwoSidesComm;
use crate::rdma::RdmaSendCallback;
// use crate::{MAX_REQ_SIZE, MAX_RESP_SIZE};

use super::rpc_shared_buffer::RpcBufAllocator;

use super::rpc::rpc_msg_type;
use super::rpc::RpcHandler;
use super::rpc::RpcHeaderMeta;
use super::rpc::DEFAULT_RPC_HANDLER;
use super::rpc::{AsyncRpc, RpcProcessMeta};

// process send / read / write saparately
// because send does not require polling but read / write need

pub struct ReplyMeta {
    reply_bufs: Vec<*mut u8>,
    reply_counts: Vec<u32>,
}

impl ReplyMeta {
    fn new(routine_num: u32) -> Self {
        let mut bufs = Vec::new();
        let mut counts = Vec::new();

        for _ in 0..routine_num {
            bufs.push(std::ptr::null_mut());
            counts.push(0);
        }
        Self {
            reply_bufs: bufs,
            reply_counts: counts,
        }
    }
}

pub struct AsyncScheduler<'sched> {
    allocator: Mutex<RpcBufAllocator>,
    conns: RwLock<HashMap<u64, Arc<Mutex<RdmaRcConn<'sched>>>>>,
    //  read / write (one-side primitives)
    // pending for coroutines
    pendings: Mutex<Vec<u32>>,

    // rpcs (two-sides primitives)
    reply_metas: Mutex<ReplyMeta>,

    // callbacks
    callback: RwLock<Weak<dyn RpcHandler + Send + Sync + 'sched>>,
}

// 手动标记 Send + Sync
unsafe impl<'sched> Send for AsyncScheduler<'sched> {}
unsafe impl<'sched> Sync for AsyncScheduler<'sched> {}

impl<'sched> AsyncScheduler<'sched> {
    pub fn new(routine_num: u32, allocator: &Arc<LockedHeap>) -> Self {
        let mut pendings = Vec::new();
        for _ in 0..routine_num {
            pendings.push(0);
        }
        Self {
            allocator: Mutex::new(RpcBufAllocator::new(routine_num, allocator)),
            conns: RwLock::new(HashMap::new()),
            pendings: Mutex::new(pendings),
            reply_metas: Mutex::new(ReplyMeta::new(routine_num)),

            // callbacks
            callback: RwLock::new(Arc::downgrade(&DEFAULT_RPC_HANDLER) as _),
        }
    }

    pub fn append_conn(&self, id: u64, conn: &Arc<Mutex<RdmaRcConn<'sched>>>) {
        self.conns.write().unwrap().insert(id, conn.clone());
    }

    pub fn register_callback(&self, callback: &Arc<impl RpcHandler + Send + Sync + 'sched>) {
        *self.callback.write().unwrap() = Arc::downgrade(callback) as _;
    }

    pub fn poll_sends(&self) {
        let guard = self.conns.read().unwrap();
        for (_, conn) in guard.iter() {
            conn.lock().unwrap().poll_send();
        }
    }
}

impl<'sched> RdmaSendCallback for AsyncScheduler<'sched> {
    #[allow(unused)]
    fn rdma_send_handler(&self, wr_id: u64) {
        todo!();
        // update pending and signal
    }
}

// RPCs
impl<'sched> AsyncScheduler<'sched> {
    pub fn poll_recvs(&self) {
        // let mut pendings = self.pendings.lock().unwrap();
        let guard = self.conns.read().unwrap();
        for (_, conn) in guard.iter() {
            conn.lock().unwrap().poll_recvs();
        }

        for (_, conn) in guard.iter() {
            conn.lock().unwrap().flush_pending().unwrap();
        }
    }

    fn prepare_msg_header(msg: *mut u8, rpc_id: u32, rpc_size: u32, rpc_cid: u32, rpc_type: u32) {
        let meta = RpcHeaderMeta::new(rpc_type, rpc_id, rpc_size, rpc_cid);
        unsafe {
            *(msg.sub(4) as *mut u32) = meta.to_header();
        }
    }

    pub fn prepare_multi_replys(&self, cid: u32, reply_buf: *mut u8, reply_count: u32) {
        let mut reply_metas = self.reply_metas.lock().unwrap();
        if let Some(mut_count) = reply_metas.reply_counts.get_mut::<usize>(cid as _) {
            *mut_count = reply_count;
        }
        if let Some(mut_buf) = reply_metas.reply_bufs.get_mut::<usize>(cid as _) {
            *mut_buf = reply_buf;
        }
    }

    // pub fn free_reply_buf(&self, addr: *mut u8, size: usize) {
    //     let layout = Layout::from_size_align(MAX_RESP_SIZE, std::mem::align_of::<usize>()).unwrap();
    //     // unsafe { self.allocator.alloc(layout) }
    //     unsafe { self.allocator.dealloc(addr.sub(4), layout) };
    // }

    // pub fn free_req_buf(&self, addr: *mut u8, size: usize) {
    //     let layout = Layout::from_size_align(MAX_REQ_SIZE, std::mem::align_of::<usize>()).unwrap();
    //     unsafe { self.allocator.dealloc(addr.sub(4), layout) };
    // }

    pub fn flush_pending(&self) {
        let guard = self.conns.read().unwrap();
        for (_, conn) in guard.iter() {
            conn.lock().unwrap().flush_pending().unwrap();
        }
    }
}

impl<'sched> RdmaRecvCallback for AsyncScheduler<'sched> {
    fn rdma_recv_handler(&self, src_conn: &mut RdmaRcConn, msg: *mut u8) {
        // todo!();
        let meta = RpcHeaderMeta::from_header(unsafe { *(msg as *mut u32) });

        match meta.rpc_type {
            rpc_msg_type::REQ => {
                let callback = &self.callback;
                let process_meta = RpcProcessMeta::new(meta.rpc_cid, src_conn.get_conn_id(), 0);
                callback.read().unwrap().upgrade().unwrap().rpc_handler(
                    src_conn,
                    meta.rpc_id,
                    unsafe { msg.add(4) },
                    meta.rpc_payload,
                    process_meta,
                );
            }
            rpc_msg_type::Y_REQ => {
                unimplemented!("yreq type is not allowed for time being");
            }
            rpc_msg_type::RESP => {
                let index = meta.rpc_cid as usize;
                let mut reply_metas = self.reply_metas.lock().unwrap();

                let buf = *reply_metas.reply_bufs.get::<usize>(index).unwrap();
                unsafe {
                    std::ptr::copy_nonoverlapping(msg.add(4), buf, meta.rpc_payload as _);
                }

                if let Some(mut_buf) = reply_metas.reply_bufs.get_mut::<usize>(index) {
                    *mut_buf = unsafe { mut_buf.add((meta.rpc_payload) as usize) };
                }

                if let Some(mut_count) = reply_metas.reply_counts.get_mut::<usize>(index) {
                    *mut_count -= 1;
                }
            }
            _ => {
                unimplemented!("rpc type");
            }
        }
    }
}

impl<'sched> AsyncRpc for AsyncScheduler<'sched> {
    fn get_reply_buf(&self) -> *mut u8 {
        // std::ptr::null_mut()
        // let layout = Layout::from_size_align(MAX_RESP_SIZE, std::mem::align_of::<usize>()).unwrap();
        // let buf = unsafe { self.allocator.alloc(layout) };
        let buf = self.allocator.lock().unwrap().get_reply_buf();
        unsafe { buf.add(4) }
    }

    fn get_req_buf(&self, cid: u32) -> *mut u8 {
        // let layout = Layout::from_size_align(MAX_REQ_SIZE, std::mem::align_of::<usize>()).unwrap();
        // let buf = unsafe { self.allocator.alloc(layout) };
        let buf = self.allocator.lock().unwrap().get_req_buf(cid);
        unsafe { buf.add(4) }
    }

    #[allow(unused_variables)]
    fn send_reply(
        &self,
        src_conn: &mut RdmaRcConn,
        msg: *mut u8,
        rpc_id: u32,
        rpc_size: u32,
        rpc_cid: u32,
        peer_id: u64,
        peer_tid: u64,
    ) {
        Self::prepare_msg_header(msg, rpc_id, rpc_size, rpc_cid, rpc_msg_type::RESP);
        src_conn
            .send_pending(unsafe { msg.sub(4) as _ }, rpc_size + 4)
            .unwrap();
    }

    #[allow(unused_variables)]
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

        let guard = self.conns.read().unwrap();
        let conn = guard.get(&peer_id).unwrap();
        conn.lock()
            .unwrap()
            .send_pending(unsafe { msg.sub(4) as _ }, rpc_size + 4)
            .unwrap();
    }

    #[allow(unused_variables)]
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

        let guard = self.conns.read().unwrap();
        let conn = guard.get(&peer_id).unwrap();
        conn.lock()
            .unwrap()
            .send_one(unsafe { msg.sub(4) as _ }, rpc_size + 4);
    }
}

// Async waiter
impl<'sched> AsyncScheduler<'sched> {
    pub async fn yield_now(&self, cid: u32) {
        // println!("yield: {}", _cid);
        tokio::task::yield_now().await;
    }

    pub async fn yield_until_ready(&self, cid: u32) {
        loop {
            let pendings = *self
                .pendings
                .lock()
                .unwrap()
                .get::<usize>(cid as _)
                .unwrap();
            let reply_counts = *self
                .reply_metas
                .lock()
                .unwrap()
                .reply_counts
                .get::<usize>(cid as _)
                .unwrap();
            if pendings == 0 && reply_counts == 0 {
                break;
            }
            self.yield_now(cid).await;
        }
    }
}
