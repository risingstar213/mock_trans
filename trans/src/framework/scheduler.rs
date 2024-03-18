use std::sync::Arc;
use std::sync::{Mutex, RwLock};
use std::collections::HashMap;

use crate::rdma::RdmaRecvCallback;
use crate::rdma::rcconn::RdmaRcConn;
use crate::rdma::one_side::OneSideComm;
use crate::rdma::two_sides::TwoSidesComm;
use crate::rdma::two_sides;
use crate::rdma::RdmaSendCallback;

use super::rpc::AsyncRpc;
use super::rpc::RpcHeaderMeta;

// process send / read / write saparately 
// because send does not require polling but read / write need

pub struct AsyncScheduler<'a> {
    conns:      RwLock<HashMap<u64, Arc<RdmaRcConn<'a>>>>,
    pendings:   Mutex<HashMap<u64, u64>>,
    //  read / write (one-side primitives)

    // rpcs (two-sides primitives)
    reply_bufs: Mutex<Vec<*mut u8>>,

    // callbacks
    // callbacks:  RwLock<Weak<dyn >>
}

impl<'a> AsyncScheduler<'a> {
    pub fn new(routine_num: u64) -> Self {
        Self {
            conns:      RwLock::new(HashMap::new()),
            pendings:   Mutex::new(HashMap::new()),
            reply_bufs: Mutex::new(Vec::new())

            // callbacks: RwLock::NEW
        }
    }

    fn append_conn(&self, id: u64, conn: &Arc<RdmaRcConn<'a>>) {
        self.conns.write().unwrap().insert(id, conn.clone());
    }

    fn poll_comps(&self) {
        
    }
}

impl<'a> RdmaRecvCallback for AsyncScheduler<'a> {
    fn rdma_recv_handler(&self, msg: *mut u8) {
        // todo!();
        
    }
}

impl<'a> RdmaSendCallback for AsyncScheduler<'a> {
    fn rdma_send_handler(&self, wr_id: u64) {
        todo!();
    }
}

impl<'a> AsyncScheduler<'a> {
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
}

impl<'a> AsyncRpc for AsyncScheduler<'a> {
    fn get_reply_buf() -> *mut u8 {
        std::ptr::null_mut()
    }

    fn get_req_buf() -> *mut u8 {
        std::ptr::null_mut()
    }

    fn send_reply(peer_id: u64, msg: *mut u8) {
        todo!();
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
        
        let conn_pool = self.conns.read().unwrap();
        let conn = conn_pool.get(&peer_id).unwrap();
        conn.send_pending(unsafe { msg.byte_sub(4) as _ }, rpc_size + 4).unwrap();
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
        
        let conn_pool = self.conns.read().unwrap();
        let conn = conn_pool.get(&peer_id).unwrap();
        conn.send_one(unsafe { msg.byte_sub(4) as _ }, rpc_size + 4);
    }
}