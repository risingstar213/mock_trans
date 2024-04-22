use std::collections::HashMap;
use std::sync::{Arc, Weak};
use std::sync::Mutex;

use crate::doca_dma::{DmaLocalBuf, DmaRemoteBuf};
use crate::rdma::RdmaBaseAllocator;
use crate::rdma::rcconn::RdmaRcConn;
use crate::rdma::RdmaRecvCallback;
// use crate::rdma::one_side::OneSideComm;
use crate::rdma::two_sides::TwoSidesComm;
use crate::rdma::RdmaSendCallback;

#[cfg(feature = "doca_deps")]
use doca::DOCAError;

#[cfg(feature = "doca_deps")]
use crate::doca_dma::connection::DocaDmaConn;

use super::rpc_shared_buffer::RpcBufAllocator;

use super::rpc::rpc_msg_type;
use super::rpc::RpcHandler;
use super::rpc::RpcHeaderMeta;
use super::rpc::DEFAULT_RPC_HANDLER;
use super::rpc::{AsyncRpc, RpcProcessMeta};

// process send / read / write saparately
// because send does not require polling but read / write need

struct ReplyMeta {
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

#[cfg(feature = "doca_deps")]
#[derive(Clone, Copy)]
enum DmaStatus {
    DmaIdle,
    DmaWaiting,
    DmaError,
}

#[cfg(feature = "doca_deps")]
struct DmaMeta(DmaStatus);

pub struct AsyncScheduler {
    tid: usize,
    allocator: Mutex<RpcBufAllocator>,
    conns: HashMap<u64, Arc<Mutex<RdmaRcConn>>>,
    //  read / write (one-side primitives)
    // pending for coroutines
    pendings: Mutex<Vec<u32>>,

    // rpcs (two-sides primitives)
    reply_metas: Mutex<ReplyMeta>,

    // callbacks
    callback: Weak<dyn RpcHandler + Send + Sync + 'static>,

    #[cfg(feature = "doca_deps")]
    dma_conn: Option<Arc<Mutex<DocaDmaConn>>>,
    #[cfg(feature = "doca_deps")]
    dma_meta: Mutex<Vec<DmaMeta>>,
}

// 手动标记 Send + Sync
unsafe impl Send for AsyncScheduler {}
unsafe impl Sync for AsyncScheduler {}

impl AsyncScheduler {
    pub fn new(tid: usize, routine_num: u32, allocator: &Arc<RdmaBaseAllocator>) -> Self {
        let mut pendings = Vec::new();
        #[cfg(feature = "doca_deps")]
        let mut dma_meta = Vec::new();
        for _ in 0..routine_num {
            pendings.push(0);
            #[cfg(feature = "doca_deps")]
            dma_meta.push(DmaMeta(DmaStatus::DmaIdle));
        }
        Self {
            tid: tid,
            allocator: Mutex::new(RpcBufAllocator::new(routine_num, allocator)),
            conns: HashMap::new(),
            pendings: Mutex::new(pendings),
            reply_metas: Mutex::new(ReplyMeta::new(routine_num)),

            // callbacks
            callback: Arc::downgrade(&DEFAULT_RPC_HANDLER) as _,

            #[cfg(feature = "doca_deps")]
            dma_conn: None,
            #[cfg(feature = "doca_deps")]
            dma_meta: Mutex::new(dma_meta),
        }
    }

    pub fn append_conn(&mut self, id: u64, conn: &Arc<Mutex<RdmaRcConn>>) {
        self.conns.insert(id, conn.clone());
    }

    pub fn register_callback(&mut self, callback: &Arc<impl RpcHandler + Send + Sync + 'static>) {
        self.callback = Arc::downgrade(callback) as _;
    }

    pub fn poll_sends(&self) {
        for (_, conn) in self.conns.iter() {
            conn.lock().unwrap().poll_send();
        }
    }
}

impl RdmaSendCallback for AsyncScheduler {
    #[allow(unused)]
    fn rdma_send_handler(&self, wr_id: u64) {
        todo!();
        // update pending and signal
    }
}

// RPCs
impl AsyncScheduler {
    pub fn poll_recvs(&self) {
        // let mut pendings = self.pendings.lock().unwrap();
        for (_, conn) in self.conns.iter() {
            conn.lock().unwrap().poll_recvs();
        }

        for (_, conn) in self.conns.iter() {
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

    pub fn flush_pending(&self) {
        for (_, conn) in self.conns.iter() {
            conn.lock().unwrap().flush_pending().unwrap();
        }
    }
}

impl RdmaRecvCallback for AsyncScheduler {
    fn rdma_recv_handler(&self, src_conn: &mut RdmaRcConn, msg: *mut u8) {
        // todo!();
        let meta = RpcHeaderMeta::from_header(unsafe { *(msg as *mut u32) });

        match meta.rpc_type {
            rpc_msg_type::REQ => {
                let callback = &self.callback;
                let process_meta = RpcProcessMeta::new(meta.rpc_cid, src_conn.get_conn_id(), 0);
                callback.upgrade().unwrap().rpc_handler(
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

impl AsyncRpc for AsyncScheduler {
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

        let conn = self.conns.get(&peer_id).unwrap();
        conn.lock()
            .unwrap()
            .send_pending(unsafe { msg.sub(4) as _ }, rpc_size + 4)
            .unwrap();
    }

    #[allow(unused_variables)]
    fn send_req(
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

        let conn =  self.conns.get(&peer_id).unwrap();
        conn.lock()
            .unwrap()
            .send_one(unsafe { msg.sub(4) as _ }, rpc_size + 4);
    }
}

// for doca dma
#[cfg(feature = "doca_deps")]
impl AsyncScheduler {
    pub fn set_dma_conn(&mut self, dma_conn: &Arc<Mutex<DocaDmaConn>>) {
        self.dma_conn = Some(dma_conn.clone());
    }

    #[inline]
    pub fn post_read_dma_req(&self, local_offset: usize, remote_offset: usize, payload: usize, cid: u32) {
        self.dma_conn
            .as_ref()
            .unwrap()
            .lock()
            .unwrap()
            .post_read_dma_reqs(local_offset, remote_offset, payload, cid as _);

        if let Some(status) = self.dma_meta.lock().unwrap().get_mut::<usize>(cid as _) {
            status.0 = DmaStatus::DmaWaiting;
        }
    }

    #[inline]
    pub fn post_write_dma_req(&self, local_offset: usize, remote_offset: usize, payload: usize, cid: u32) {
        self.dma_conn
            .as_ref()
            .unwrap()
            .lock()
            .unwrap()
            .post_write_dma_reqs(local_offset, remote_offset, payload, cid as _);

            if let Some(status) = self.dma_meta.lock().unwrap().get_mut::<usize>(cid as _) {
                status.0 = DmaStatus::DmaWaiting;
            }
    }

    #[inline]
    pub fn poll_dma_comps(&self) {
        let mut dma_conn = self.dma_conn
            .as_ref()
            .unwrap()
            .lock()
            .unwrap();

        loop {
            let (event, error) = dma_conn.poll_completion();
            match error {
                DOCAError::DOCA_SUCCESS => {
                    let cid: usize = event.user_mark() as _;
                    if let Some(status) = self.dma_meta.lock().unwrap().get_mut::<usize>(cid) {
                        status.0 = DmaStatus::DmaIdle;
                    }
                }
                DOCAError::DOCA_ERROR_AGAIN => {
                    break;
                }
                _ => {
                    let cid: usize = event.user_mark() as _;
                    if let Some(status) = self.dma_meta.lock().unwrap().get_mut::<usize>(cid) {
                        status.0 = DmaStatus::DmaError;
                    }
                }
            }
        }
    }

    pub fn busy_until_dma_ready(&self, cid: u32) -> Result<(), ()> {
        loop {
            let status = self.dma_meta.lock().unwrap().get::<usize>(cid as _).unwrap().0;
            match status {
                DmaStatus::DmaIdle => {
                    return Ok(());
                }
                DmaStatus::DmaError => {
                    return Err(());
                }
                DmaStatus::DmaWaiting => {
                    continue;
                }
            }
        }
    }

    pub async fn yield_until_dma_ready(&self, cid: u32) -> Result<(), ()> {
        loop {
            let status = self.dma_meta.lock().unwrap().get::<usize>(cid as _).unwrap().0;
            match status {
                DmaStatus::DmaIdle => {
                    return Ok(());
                }
                DmaStatus::DmaError => {
                    return Err(());
                }
                DmaStatus::DmaWaiting => {
                    self.yield_now(cid).await;
                }
            }
        }
    }

    #[inline]
    pub fn dma_get_local_buf(&self, cid: u32) -> DmaLocalBuf {
        self.dma_conn.as_ref().unwrap().lock().unwrap().get_local_buf(cid)
    }

    #[inline]
    pub fn dma_alloc_remote_buf(&self) -> DmaRemoteBuf {
        self.dma_conn.as_ref().unwrap().lock().unwrap().alloc_remote_buf()
    }

    #[inline]
    pub fn dma_dealloc_remote_buf(&self, buf: DmaRemoteBuf) {
        self.dma_conn.as_ref().unwrap().lock().unwrap().dealloc_remote_buf(buf);
    }
}

// Async waiter
impl AsyncScheduler {
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
