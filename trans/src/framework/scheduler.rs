use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Weak};
use std::sync::Mutex;

use crate::doca_dma::{DmaLocalBuf, DmaRemoteBuf};
use crate::rdma::RdmaBaseAllocator;
use crate::rdma::rcconn::RdmaRcConn;
use crate::rdma::RdmaRecvCallback;
// use crate::rdma::one_side::OneSideComm;
use crate::rdma::two_sides::TwoSidesComm;
use crate::rdma::RdmaSendCallback;
use crate::MAX_CONN_MSG_SIZE;

#[cfg(feature = "doca_deps")]
use doca::DOCAError;

#[cfg(feature = "doca_deps")]
use crate::doca_dma::connection::DocaDmaConn;

#[cfg(feature = "doca_deps")]
use crate::doca_comm_chan::connection::DocaCommChannel;

#[cfg(feature = "doca_deps")]
use crate::doca_comm_chan::connection::{ DocaCommHandler, DEFAULT_DOCA_CONN_HANDLER };

#[cfg(feature = "doca_deps")]
use crate::doca_comm_chan::comm_buf::{ DocaCommBuf, DocaCommReply };

#[cfg(feature = "doca_deps")]
use crate::doca_comm_chan::{ doca_comm_info_type, DocaCommHeaderMeta };

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
    vers: UnsafeCell<Vec<u32>>,
    //  read / write (one-side primitives)
    // pending for coroutines
    pendings: UnsafeCell<Vec<u32>>,

    // rpcs (two-sides primitives)
    reply_metas: UnsafeCell<ReplyMeta>,

    // callbacks
    callback: Weak<dyn RpcHandler + Send + Sync + 'static>,

    #[cfg(feature = "doca_deps")]
    dma_conn: Option<Arc<Mutex<DocaDmaConn>>>,
    #[cfg(feature = "doca_deps")]
    dma_meta: UnsafeCell<Vec<DmaMeta>>,
    #[cfg(feature = "doca_deps")]
    comm_chan: Option<DocaCommChannel>,
    #[cfg(feature = "doca_deps")]
    comm_handler: Weak<dyn DocaCommHandler + Send + Sync + 'static>,
    #[cfg(feature = "doca_deps")]
    comm_replys: UnsafeCell<Vec<DocaCommReply>>,
}

// 手动标记 Send + Sync
unsafe impl Send for AsyncScheduler {}
unsafe impl Sync for AsyncScheduler {}

impl AsyncScheduler {
    pub fn new(tid: usize, routine_num: u32, allocator: &Arc<RdmaBaseAllocator>) -> Self {
        let mut pendings = Vec::new();
        #[cfg(feature = "doca_deps")]
        let dma_meta = Vec::new();

        let mut vers = Vec::new();
        for _ in 0..routine_num {
            pendings.push(0);
            vers.push(0);
        }
        Self {
            tid: tid,
            allocator: Mutex::new(RpcBufAllocator::new(routine_num, allocator)),
            conns: HashMap::new(),
            vers: UnsafeCell::new(vers),
            pendings: UnsafeCell::new(pendings),
            reply_metas: UnsafeCell::new(ReplyMeta::new(routine_num)),

            // callbacks
            callback: Arc::downgrade(&DEFAULT_RPC_HANDLER) as _,

            #[cfg(feature = "doca_deps")]
            dma_conn: None,
            #[cfg(feature = "doca_deps")]
            dma_meta: UnsafeCell::new(dma_meta),
            #[cfg(feature = "doca_deps")]
            comm_chan: None,
            #[cfg(feature = "doca_deps")]
            comm_handler: Arc::downgrade(&DEFAULT_DOCA_CONN_HANDLER) as _,
            #[cfg(feature = "doca_deps")]
            comm_replys: UnsafeCell::new(Vec::new()),
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
        let reply_metas = unsafe { self.reply_metas.get().as_mut().unwrap() };
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
                let reply_metas = unsafe { self.reply_metas.get().as_mut().unwrap() };

                let buf = *reply_metas.reply_bufs.get::<usize>(index).unwrap();
                unsafe {
                    std::ptr::copy_nonoverlapping(msg.add(4), buf, meta.rpc_payload as _);
                }

                if let Some(mut_buf) = reply_metas.reply_bufs.get_mut::<usize>(index) {
                    *mut_buf = unsafe { mut_buf.add(crate::MAX_PACKET_SIZE) };
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
    fn get_reply_buf(&self, cid: u32) -> *mut u8 {
        // std::ptr::null_mut()
        // let layout = Layout::from_size_align(MAX_RESP_SIZE, std::mem::align_of::<usize>()).unwrap();
        // let buf = unsafe { self.allocator.alloc(layout) };
        let buf = self.allocator.lock().unwrap().get_reply_buf(cid);
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

        let conn = self.conns.get(&peer_id);
        if conn.is_none() {
            panic!("{}", peer_id);
        }
        conn.as_ref()
            .unwrap()
            .lock()
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
        let routine_num = self.pendings.get_mut().len();
        for _ in 0..routine_num {
            self.dma_meta.get_mut().push(DmaMeta(DmaStatus::DmaIdle));
        }
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

        let dma_meta = unsafe { self.dma_meta.get().as_mut().unwrap() };
        dma_meta[cid as usize].0 = DmaStatus::DmaWaiting;
    }

    #[inline]
    pub fn post_write_dma_req(&self, local_offset: usize, remote_offset: usize, payload: usize, cid: u32) {
        self.dma_conn
            .as_ref()
            .unwrap()
            .lock()
            .unwrap()
            .post_write_dma_reqs(local_offset, remote_offset, payload, cid as _);

        let dma_meta = unsafe { self.dma_meta.get().as_mut().unwrap() };
        dma_meta[cid as usize].0 = DmaStatus::DmaWaiting;
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
                    let dma_meta = unsafe { self.dma_meta.get().as_mut().unwrap() };
                    dma_meta[cid as usize].0 = DmaStatus::DmaIdle;
                }
                DOCAError::DOCA_ERROR_AGAIN => {
                    break;
                }
                _ => {
                    let cid: usize = event.user_mark() as _;
                    let dma_meta = unsafe { self.dma_meta.get().as_mut().unwrap() };
                    dma_meta[cid as usize].0 = DmaStatus::DmaError;
                }
            }
        }
    }

    pub fn busy_until_dma_ready(&self, cid: u32) -> Result<(), ()> {
        loop {
            let status = unsafe{ self.dma_meta.get().as_mut().unwrap().get::<usize>(cid as _).unwrap().0 };
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
            let status = unsafe{ self.dma_meta.get().as_mut().unwrap().get::<usize>(cid as _).unwrap().0 };
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

#[cfg(feature = "doca_deps")]
impl AsyncScheduler {
    pub fn set_comm_chan(&mut self, chan: DocaCommChannel) {
        let routine_num = self.pendings.get_mut().len();
        for _ in 0..routine_num {
            self.comm_replys.get_mut().push(DocaCommReply::new());
        }
        self.comm_chan = Some(chan)
    }

    pub fn register_comm_handler(&mut self, handler: &Arc<impl DocaCommHandler + Send + Sync + 'static>) {
        self.comm_handler = Arc::downgrade(handler) as _;
    }

    pub fn alloc_new_ver(&self, cid: u32) -> u32 {
        let vers = unsafe { self.vers.get().as_mut().unwrap() };
        vers[cid as usize] += 1;
        if vers[cid as usize] > 10000000 {
            vers[cid as usize] = 0;
        }
        let ver = vers[cid as usize];
        ver
    }

    #[inline]
    pub fn comm_chan_alloc_buf(&self, cid: u32) -> DocaCommBuf {
        // println!("({}){} alloc buf", self.tid,cid);
        self.comm_chan.as_ref().unwrap().alloc_buf(cid)
    }

    #[inline]
    pub fn comm_chan_dealloc_buf(&self, buf: DocaCommBuf, cid: u32) {
        // println!("({}){} dealloc buf", self.tid,cid);
        self.comm_chan.as_ref().unwrap().dealloc_buf(buf, cid);
    }

    pub fn comm_chan_append_empty_info(&self, header: DocaCommHeaderMeta) {
        self.comm_chan.as_ref().unwrap().append_empty_msg(header);
    }

    pub fn comm_chan_append_item_info<ITEM: Clone>(&self, header: DocaCommHeaderMeta, item: ITEM) {
        self.comm_chan.as_ref().unwrap().append_item_msg(header, item);
    }

    pub fn comm_chan_append_slice_info<ITEM: Clone>(&self, header: DocaCommHeaderMeta, items: &[ITEM]) {
        self.comm_chan.as_ref().unwrap().append_slice_msg(header, items);
    }

    pub fn prepare_comm_replys(&self, cid: u32, count: usize) {
        let comm_replys = unsafe { self.comm_replys.get().as_mut().unwrap() };
        comm_replys[cid as usize].reset(count);
    }

    pub fn poll_comm_chan(&self) {
        let mut recv_bufs: Vec<DocaCommBuf> = Vec::with_capacity(10);
        loop {
            let mut recv_buf = self.comm_chan_alloc_buf(0);
            recv_buf.set_payload(MAX_CONN_MSG_SIZE);
            let res = self.comm_chan.as_ref().unwrap().recv_info(&mut recv_buf);
            if res == DOCAError::DOCA_ERROR_AGAIN {
                self.comm_chan_dealloc_buf(recv_buf, 0);
                break;
            }
            if res != DOCAError::DOCA_SUCCESS {
                panic!("the connection is lost {}", self.tid);
            }
            recv_bufs.push(recv_buf);
        }
        for i in 0..recv_bufs.len() {
            let recv_buf: &mut _ = recv_bufs.get_mut(i).unwrap();

            recv_buf.start_read();

            // println!("recv {}", recv_buf.get_payload());

            while let Some(header) = recv_buf.get_header() {
                match header.info_type as u32 {
                    doca_comm_info_type::REQ => {
                        // println!("REQ({})", header.info_payload);
                        self.comm_handler.upgrade().unwrap().comm_handler(
                            recv_buf,
                            header.info_id as _,
                            header.info_payload as _,
                            header.info_pid as _,
                            header.info_tid as _,
                            header.info_cid as _,
                        );
                    }
                    doca_comm_info_type::REPLY => {
                        // println!("REPLY({})", header.info_payload);
                        let replys = unsafe { self.comm_replys.get().as_mut().unwrap() };
                        
                        let reply_success = if header.info_payload > 0 {
                            let success = recv_buf.get_item::<u32>(0);
                            *success
                        } else {
                            1
                        };
    
                        let vers = unsafe { self.vers.get().as_mut().unwrap() };
                        let local_ver = vers[header.info_cid as usize];
    
                        if replys[header.info_cid as usize].get_pending_count() == 0 {
                            println!("what the fuck? self.tid:{}, ver: {} , header: {:?}", self.tid, local_ver, header);  
                            for i in 0..replys.len() {
                                println!("{} pendingnum {}", i, replys[i].get_pending_count());
                            }                      
                        } else {
                            replys[header.info_cid as usize].append_reply(reply_success);
                        }
                    }
                    _ => { panic!("unsupported!, read_idx: {}", recv_buf.get_read_idx()); }
                }

                recv_buf.shift_to_next_msg(header.info_payload);
            }
        }

        while let Some(recv_buf) = recv_bufs.pop() {
            self.comm_chan_dealloc_buf(recv_buf, 0);
        }

        self.comm_chan.as_ref().unwrap().flush_pending_msgs();
    }

    pub async fn yield_until_comm_ready(&self, cid: u32) -> bool {
        let mut count = 0;
        // let mut start = std::time::SystemTime::now();
        loop {

            let comm_replys = unsafe { self.comm_replys.get().as_mut().unwrap() };
            if comm_replys[cid as usize].get_pending_count() > 0 {
                // let now = std::time::SystemTime::now();
                // let duration = now.duration_since(start).unwrap();
                // if duration.as_millis() > 5000 {
                //     println!("({})wait dpu comm too long {}, count:{} ", self.tid, cid, count);
                //     start = now;
                //     count += 1;
                // }
                self.yield_now(cid).await;
            } else {
                return comm_replys[cid as usize].get_success();
            }
        }
    }

}

// Async waiter
impl AsyncScheduler {
    pub async fn yield_now(&self, cid: u32) {
        // println!("yield: {}", _cid);
        tokio::task::yield_now().await;
    }

    pub async fn yield_until_ready(&self, cid: u32) {
        // let mut start = std::time::SystemTime::now();
        loop {
            
            let pendings = unsafe { self.pendings.get().as_ref().unwrap() };
            let reply_counts = unsafe { &self.reply_metas.get().as_ref().unwrap().reply_counts };
            if pendings[cid as usize] == 0 && reply_counts[cid as usize] == 0 {
                break;
            }
            // let now = std::time::SystemTime::now();
            // let duration = now.duration_since(start).unwrap();
            // if duration.as_millis() > 10000 {
            //     println!("wait too long");
            //     start = now;
            // }
            self.yield_now(cid).await;
        }
    }
}
