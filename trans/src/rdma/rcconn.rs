use std::alloc::Layout;
// use std::sync::{Mutex, MutexGuard};
use std::sync::{Arc, Weak};

use ll_alloc::LockedHeap;
use rdma_sys::ibv_wr_opcode::IBV_WR_SEND;
use rdma_sys::*;

use super::{one_side::OneSideComm, two_sides::TwoSidesComm};
use super::{RdmaRecvCallback, RdmaSendCallback};
use super::{DEFAULT_RDMA_RECV_HANDLER, DEFAULT_RDMA_SEND_HANDLER};
use super::control::RdmaBaseAllocator;
use crate::*;

#[allow(unused)]
struct RdmaRcMeta {
    conn_id: *mut rdma_cm_id,
    lm: *mut u8,
    lmr: *mut ibv_mr,
    raddr: u64,
    rid: u32,
}


// TODO: Pin the structure
struct RdmaElement {
    recv_head: u64,
    rsges: [ibv_sge; MAX_RECV_SIZE],
    rwrs: [ibv_recv_wr; MAX_RECV_SIZE],
    low_watermark: u64, // number of msgs need to poll
    high_watermark: u64,
    // for send primitives, (TODO: using ud qpairs for two-side primitives)
    current_idx: u64,
    pending_sends: u64,
    ssges: [ibv_sge; MAX_DOORBELL_SEND_SIZE],
    swrs: [ibv_send_wr; MAX_DOORBELL_SEND_SIZE],
}

impl Default for RdmaElement {
    fn default() -> Self {
        Self {
            recv_head: 0,
            rsges: unsafe { std::mem::zeroed() },
            rwrs: unsafe { std::mem::zeroed() },
            low_watermark: 0,
            high_watermark: 0,
            current_idx: 0,
            pending_sends: 0,
            ssges: unsafe { std::mem::zeroed() },
            swrs: unsafe { std::mem::zeroed() },
        }
    }
}

// TODO: Recv elements can be shared between connections ?
// How to treat buffers to store reqs? They cannot be shared.
// Using SRQs to solve this problem
pub struct RdmaRcConn<'conn> {
    conn_id: u64,
    meta: RdmaRcMeta,
    allocator: Arc<RdmaBaseAllocator>,
    elements: RdmaElement,
    rwcs: [ibv_wc; MAX_RECV_SIZE],
    rhandler: Weak<dyn RdmaRecvCallback + Send + Sync + 'conn>,
    whandler: Weak<dyn RdmaSendCallback + Send + Sync + 'conn>,
}

unsafe impl<'conn> Send for RdmaRcConn<'conn> {}
// unsafe impl<'conn> Sync for RdmaRcConn<'conn> {}

// RC Connection
impl<'conn> RdmaRcConn<'conn> {
    pub fn new(
        conn_id: u64,
        id: *mut rdma_cm_id,
        lm: *mut u8,
        lmr: *mut ibv_mr,
        raddr: u64,
        rid: u32,
        allocator: &Arc<RdmaBaseAllocator>,
    ) -> Self {
        let meta = RdmaRcMeta {
            conn_id: id,
            lm: lm,
            lmr: lmr,
            raddr: raddr,
            rid: rid,
        };

        // let allocator = unsafe { LockedHeap::new(lm, (NPAGES * 4096) as usize) };

        Self {
            conn_id: conn_id,
            meta: meta,
            allocator: allocator.clone(),
            elements: RdmaElement::default(),
            rwcs: unsafe { std::mem::zeroed() },
            rhandler: Arc::downgrade(&DEFAULT_RDMA_RECV_HANDLER) as _,
            whandler: Arc::downgrade(&DEFAULT_RDMA_SEND_HANDLER) as _,
        }
    }

    pub fn get_conn_id(&self) -> u64 {
        return self.conn_id;
    }

    pub fn init_and_start_recvs(&mut self) -> TransResult<()> {
        for i in 0..MAX_RECV_SIZE {
            let addr = self.alloc_mr(MAX_PACKET_SIZE).unwrap() as u64;
            self.elements.rsges[i] = ibv_sge {
                addr: addr,
                length: MAX_PACKET_SIZE as _,
                lkey: unsafe { (*self.meta.lmr).lkey },
            };

            let next = if i + 1 == MAX_RECV_SIZE {
                &mut self.elements.rwrs[0] as *mut _
            } else {
                &mut self.elements.rwrs[i + 1] as *mut _
            };

            self.elements.rwrs[i] = ibv_recv_wr {
                wr_id: addr,
                sg_list: &mut self.elements.rsges[i] as *mut _,
                next: next,
                num_sge: 1,
            };
        }

        for i in 0..MAX_DOORBELL_SEND_SIZE {
            self.elements.ssges[i].lkey = unsafe { (*self.meta.lmr).lkey };

            self.elements.swrs[i].wr_id = 0;
            self.elements.swrs[i].opcode = IBV_WR_SEND;
            self.elements.swrs[i].num_sge = 1;
            let next = if i + 1 == MAX_DOORBELL_SEND_SIZE {
                std::ptr::null_mut()
            } else {
                &mut self.elements.swrs[i + 1] as *mut _
            };
            self.elements.swrs[i].next = next;
            self.elements.swrs[i].sg_list = &mut self.elements.ssges[i] as *mut _;
        }

        self.post_recvs(MAX_RECV_SIZE as u64).unwrap();
        Ok(())
    }

    pub fn register_recv_callback(
        &mut self,
        handler: &Arc<impl RdmaRecvCallback + Send + Sync + 'conn>,
    ) -> TransResult<()> {
        self.rhandler = Arc::downgrade(handler) as _;
        Ok(())
    }

    // (deprecated) raw send request for test
    #[deprecated]
    pub fn post_send(
        &self,
        wr_op: std::os::raw::c_uint,
        local_buf: *mut u8,
        len: u32,
        off: u64,
        flags: u32,
        wr_id: u64,
        imm: u32,
    ) -> TransResult<()> {
        let mut bad_sr: *mut ibv_send_wr = std::ptr::null_mut();

        let mut sge = ibv_sge {
            addr: local_buf as _,
            length: len,
            lkey: unsafe { (*self.meta.lmr).lkey },
        };

        let mut sr = unsafe { std::mem::zeroed::<ibv_send_wr>() };

        sr.wr_id = wr_id;
        sr.opcode = wr_op;
        sr.num_sge = 1;
        sr.next = std::ptr::null_mut();
        sr.sg_list = &mut sge as *mut _;
        sr.send_flags = flags;
        sr.imm_data_invalidated_rkey_union.imm_data = imm;

        sr.wr.rdma.remote_addr = self.meta.raddr + off;
        sr.wr.rdma.rkey = self.meta.rid;

        let ret =
            unsafe { rdma_seterrno(ibv_post_send((*self.meta.conn_id).qp, &mut sr, &mut bad_sr)) };

        if ret != 0 {
            return Err(TransError::TransRdmaError);
        }

        Ok(())
    }

    // for send primitives
    pub fn flush_pending_with_signal(&mut self, force_signal: bool) -> TransResult<()> {
        let mut bad_wr: *mut ibv_send_wr = std::ptr::null_mut();
        // let mut elements = self.elements.lock().unwrap();
        let current_idx = self.elements.current_idx;
        let need_signal = self.need_signals_for_pending() || force_signal;
        if current_idx > 0 {
            // update metas
            self.elements.current_idx = 0;
            self.elements.high_watermark += current_idx;
            if need_signal {
                self.elements.pending_sends = 0;
            } else {
                self.elements.pending_sends += current_idx;
            }

            self.elements.swrs[(current_idx - 1) as usize].next = std::ptr::null_mut();
            if need_signal {
                self.elements.swrs[(current_idx - 1) as usize].send_flags |=
                    ibv_send_flags::IBV_SEND_SIGNALED.0;
                self.elements.swrs[(current_idx - 1) as usize].wr_id =
                    self.elements.high_watermark << WRID_RESERVE_BITS; // for polling
            }
            let ret = unsafe {
                ibv_post_send(
                    (*self.meta.conn_id).qp,
                    &mut self.elements.swrs[0] as *mut _,
                    &mut bad_wr as *mut _,
                )
            };

            if current_idx < MAX_DOORBELL_SEND_SIZE as _ {
                self.elements.swrs[(current_idx - 1) as usize].next =
                    &mut self.elements.swrs[current_idx as usize] as *mut _;
            }

            if ret != 0 {
                return Err(TransError::TransRdmaError);
            }
        }

        if self.need_poll() {
            self.poll_until_complete();
        }
        Ok(())
    }

    // signals for sending pending
    #[inline]
    fn need_signals_for_pending(&self) -> bool {
        (self.elements.pending_sends + self.elements.current_idx) >= MAX_SIGNAL_PENDINGS as _
    }

    // #[inline]
    // pub fn set_low_watermark(&self, low_watermark: u64) {
    //     let mut elements = self.elements.lock().unwrap();
    //     elements.low_watermark = low_watermark;
    // }

    #[inline]
    pub fn get_high_watermark(&self) -> u64 {
        return self.elements.high_watermark;
    }

    #[inline]
    pub fn need_poll(&self) -> bool {
        let elements = &self.elements;
        (elements.high_watermark - elements.low_watermark) >= (MAX_SEND_SIZE / 2) as u64
    }

    // batch recv
    pub fn post_recvs(&mut self, recv_num: u64) -> TransResult<()> {
        if recv_num <= 0 {
            return Ok(());
        }

        let recv_head = self.elements.recv_head;
        let mut recv_tail = recv_head + recv_num - 1;
        if recv_tail > MAX_RECV_SIZE as u64 {
            recv_tail -= MAX_RECV_SIZE as u64;
        }

        let temp = self.elements.rwrs[recv_tail as usize].next;
        self.elements.rwrs[recv_tail as usize].next = std::ptr::null_mut();

        let mut bad_wr = std::ptr::null_mut();
        let ret = unsafe {
            ibv_post_recv(
                (*self.meta.conn_id).qp,
                &mut self.elements.rwrs[recv_head as usize],
                &mut bad_wr as *mut _,
            )
        };

        if ret != 0 {
            return Err(TransError::TransRdmaError);
        }

        self.elements.recv_head = (recv_tail + 1) % (MAX_RECV_SIZE as u64);
        self.elements.rwrs[recv_tail as usize].next = temp;

        Ok(())
    }

    // This func can only be called in master coroutine, so it is impossible to be recusively locked.
    pub fn poll_recvs(&mut self) -> i32 {
        let poll_result = unsafe {
            ibv_poll_cq(
                (*self.meta.conn_id).recv_cq,
                MAX_RECV_SIZE as _,
                &mut self.rwcs[0] as *mut _,
            )
        };
        for i in 0..poll_result as usize {
            let addr = self.rwcs[i].wr_id as *mut u8;
            self.rhandler
                .upgrade()
                .unwrap()
                .rdma_recv_handler(self, addr);
        }
        // self.flush_pending().unwrap();

        if poll_result > 0 {
            self.post_recvs(poll_result as _).unwrap();
        }

        // elements
        poll_result
    }

    pub fn poll_send(&mut self) -> i32 {
        let mut wc: ibv_wc = unsafe { std::mem::zeroed() };
        let poll_result =
            unsafe { ibv_poll_cq((*self.meta.conn_id).send_cq, 1, &mut wc as *mut _) };

        if poll_result > 0 {
            self.elements.low_watermark = wc.wr_id >> WRID_RESERVE_BITS;

            match wc.opcode {
                ibv_wc_opcode::IBV_WC_SEND => {
                    let element = &mut self.elements;
                    println!(
                        "poll send high: {}, low: {}",
                        element.high_watermark, element.low_watermark
                    );
                }
                _ => {
                    self.whandler.upgrade().unwrap().rdma_send_handler(wc.wr_id);
                }
            }
        }
        poll_result
    }

    #[inline]
    pub fn poll_until_complete(&mut self) {
        loop {
            self.poll_send();

            let elements = &self.elements;
            if (elements.high_watermark - elements.low_watermark) <= elements.pending_sends {
                break;
            }
        }
    }

    #[inline]
    pub fn poll_in_need(&mut self) {
        while self.need_poll() {
            self.poll_send();
        }
    }

    #[deprecated]
    pub fn poll_comps(&mut self) -> i32 {
        let mut wc: ibv_wc = unsafe { std::mem::zeroed() };
        let mut poll_result =
            unsafe { ibv_poll_cq((*self.meta.conn_id).recv_cq, 1, &mut wc as *mut _) };
        if poll_result == 0 {
            poll_result =
                unsafe { ibv_poll_cq((*self.meta.conn_id).send_cq, 1, &mut wc as *mut _) };
        }

        if poll_result > 0 {
            // println!("poll one result  {}:{}", wc.opcode, wc.status);
            match wc.opcode {
                ibv_wc_opcode::IBV_WC_SEND => {
                    // println!("into send");
                }
                ibv_wc_opcode::IBV_WC_RECV => {
                    // println!("into recv");
                    let addr = wc.wr_id as *mut u8;
                    self.rhandler
                        .upgrade()
                        .unwrap()
                        .rdma_recv_handler(self, addr);
                }
                _ => {
                    unimplemented!();
                }
            }
        }
        return poll_result;
    }

    #[inline]
    pub fn alloc_mr(&self, size: usize) -> TransResult<*mut u8> {
        let layout = Layout::from_size_align(size, std::mem::align_of::<usize>()).unwrap();
        let addr = unsafe { self.allocator.alloc(layout) };
        if addr.is_null() {
            return Err(TransError::TransRdmaError);
        } else {
            return Ok(addr);
        }
    }

    #[inline]
    pub fn deallocate_mr(&self, addr: *mut u8, size: usize) {
        let layout = Layout::from_size_align(size, std::mem::align_of::<usize>()).unwrap();
        unsafe {
            self.allocator.dealloc(addr, layout);
        }
    }
}

impl<'conn> OneSideComm for RdmaRcConn<'conn> {
    // for read / write primitives
    // read / write has no response, so the batch sending must be controlled by apps
    // the last must be send signaled
    fn post_batch(&mut self, send_wr: *mut ibv_send_wr, num: u64) -> TransResult<()> {
        // update meta
        self.elements.high_watermark += num;
        self.elements.pending_sends = 0;

        let mut bad_wr: *mut ibv_send_wr = std::ptr::null_mut();
        let ret = unsafe { ibv_post_send((*self.meta.conn_id).qp, send_wr, &mut bad_wr as *mut _) };

        if ret != 0 {
            return Err(TransError::TransRdmaError);
        }
        self.poll_in_need();
        Ok(())
    }
}

impl<'conn> TwoSidesComm for RdmaRcConn<'conn> {
    // for send primitives
    #[inline]
    fn flush_pending(&mut self) -> TransResult<()> {
        return self.flush_pending_with_signal(false);
    }

    // for send primitives
    fn send_pending(&mut self, msg: *mut u8, length: u32) -> TransResult<()> {
        let current_idx = self.elements.current_idx as usize;
        // update metas
        self.elements.current_idx += 1;

        self.elements.ssges[current_idx].addr = msg as _;
        self.elements.ssges[current_idx].length = length;

        // TODO: IBV_SEND_INLINE
        self.elements.swrs[current_idx].send_flags = 0;

        if current_idx + 1 == MAX_DOORBELL_SEND_SIZE {
            return self.flush_pending();
        }
        Ok(())
    }
}

impl<'conn> Drop for RdmaRcConn<'conn> {
    fn drop(&mut self) {
        unsafe {
            rdma_dereg_mr(self.meta.lmr);
            rdma_disconnect(self.meta.conn_id);
            // free(self.meta.lm as *mut _);
        }
    }
}
