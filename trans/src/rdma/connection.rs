use std::alloc::Layout;
use std::sync::{Mutex, MutexGuard};
use std::sync::{Arc, Weak};

use libc::free;
use rdma_sys::ibv_wr_opcode::IBV_WR_SEND;
use rdma_sys::*;
use ll_alloc::LockedHeap;

use crate::*;
use super::RdmaRecvCallback;
use super::DEFAULT_RDMA_RECV_HANDLER;

struct RdmaRcMeta {
    conn_id: *mut rdma_cm_id,
    lm: *mut u8,
    lmr: *mut ibv_mr,
    raddr: u64,
    rid: u32,
}

struct RdmaElement {
    recv_head:     u64, 
    idle_recv_num: u64,
    rsges:         [ibv_sge; MAX_RECV_SIZE],
    rwrs:          [ibv_recv_wr; MAX_RECV_SIZE],
    rwcs:          [ibv_wc; MAX_RECV_SIZE],
    low_watermark: u64, // number of msgs need to poll
    high_watermark: u64,
    // for send primitives, (TODO: using ud qpairs for two-side primitives)
    current_idx:   u64,
    pending_sends: u64,
    ssges:         [ibv_sge; MAX_DOORBELL_SEND_SIZE],
    swrs:          [ibv_send_wr; MAX_DOORBELL_SEND_SIZE],
}

impl Default for RdmaElement {
    fn default() -> Self {
        Self {
            recv_head :     0,
            idle_recv_num:  0,
            rsges:          unsafe { std::mem::zeroed() },
            rwrs:           unsafe { std::mem::zeroed() },
            rwcs:           unsafe { std::mem::zeroed() },
            low_watermark:  0,
            high_watermark: 0,
            current_idx:    0,
            pending_sends:  0,
            ssges:         unsafe { std::mem::zeroed() },
            swrs:          unsafe { std::mem::zeroed() },
        }
    }
}

pub struct RdmaRcConn<'a> {
    meta:      RdmaRcMeta,
    allocator: LockedHeap,
    elements:  Mutex<RdmaElement>,
    handler:   Mutex<Weak<dyn RdmaRecvCallback + Send + Sync + 'a>>
}

unsafe impl<'a> Send for RdmaRcConn<'a> {}
unsafe impl<'a> Sync for RdmaRcConn<'a> {}

// RC Connection 
impl<'a> RdmaRcConn<'a> {
    pub fn new(id: *mut rdma_cm_id, lm: *mut u8, lmr: *mut ibv_mr, raddr: u64, rid: u32) -> Self {
        let meta = RdmaRcMeta {
            conn_id: id,
            lm:      lm,
            lmr:     lmr,
            raddr:   raddr,
            rid:     rid,
        };

        let allocator = unsafe { LockedHeap::new(lm, (NPAGES * 4096) as usize) };

        Self {
            meta:      meta,
            allocator: allocator,
            elements:  Mutex::new(RdmaElement::default()),
            handler:   Mutex::new(Arc::downgrade(&DEFAULT_RDMA_RECV_HANDLER) as _),
        }
    }

    pub fn init_and_start_recvs(&self) -> TransResult<()>  {
        let mut elements = self.elements.lock().unwrap();
        for i in 0..MAX_RECV_SIZE {
            let addr = self.alloc_mr(MAX_PACKET_SIZE).unwrap() as u64;
            elements.rsges[i] = ibv_sge {
                addr: addr,
                length: MAX_PACKET_SIZE as _,
                lkey: unsafe { (*self.meta.lmr).lkey }
            };

            let next = if i+1 == MAX_RECV_SIZE {
                &mut elements.rwrs[0] as *mut _
            } else {
                &mut elements.rwrs[i+1] as *mut _
            };

            elements.rwrs[i] = ibv_recv_wr {
                wr_id: addr,
                sg_list: &mut elements.rsges[i] as *mut _,
                next: next,
                num_sge: 1,
            };
        }

        for i in 0..MAX_DOORBELL_SEND_SIZE {
            elements.ssges[i].lkey   = unsafe { (*self.meta.lmr).lkey };

            elements.swrs[i].wr_id   = 0;
            elements.swrs[i].opcode  = IBV_WR_SEND;
            elements.swrs[i].num_sge = 1;
            let next = if i+1 == MAX_DOORBELL_SEND_SIZE {
                std::ptr::null_mut()
            } else {
                &mut elements.swrs[i+1] as *mut _
            };
            elements.swrs[i].next = next;
            elements.swrs[i].sg_list = &mut elements.ssges[i] as *mut _;
        }
        // not recursive loc, need drop
        drop(elements);

        self.post_recvs(MAX_RECV_SIZE as u64).unwrap();
        Ok(())
    }

    pub fn register_recv_callback(&self, handler: &Arc<impl RdmaRecvCallback + Send + Sync + 'a>) -> TransResult<()> {
        *self.handler.lock().unwrap() = Arc::downgrade( handler) as _;
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
        imm: u32
    ) -> TransResult<()> {
        let mut bad_sr: *mut ibv_send_wr = std::ptr::null_mut();

        let mut sge = ibv_sge {
            addr:   local_buf as _,
            length: len,
            lkey:   unsafe { (*self.meta.lmr).lkey }
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
        sr.wr.rdma.rkey        = self.meta.rid;

        let ret = unsafe {
            rdma_seterrno(ibv_post_send((*self.meta.conn_id).qp, &mut sr, &mut bad_sr))
        };

        if ret != 0 {
            return Err(TransError::TransRdmaError);
        }

        Ok(())
    }

    // for read / write primitives
    // read / write has no response, so the batch sending must be controlled by apps
    // the last must be send signaled
    pub fn post_batch(
        &self,
        send_wr: *mut ibv_send_wr,
        num: u64,
    ) -> TransResult<()> {
        // dangerous!!!
        let mut element = self.elements.lock().unwrap();
        // update meta
        element.high_watermark += num;
        element.pending_sends = 0;

        let mut bad_wr: *mut ibv_send_wr = std::ptr::null_mut();
        let ret = unsafe {
            ibv_post_send((*self.meta.conn_id).qp, send_wr,  &mut bad_wr as *mut _)
        };

        if ret != 0 {
            return Err(TransError::TransRdmaError);
        }
        
        Ok(())
    }

    // for send primitives
    pub fn flush_pending(&self) -> TransResult<()> {
        let mut bad_wr: *mut ibv_send_wr = std::ptr::null_mut();
        let mut elements = self.elements.lock().unwrap();
        let current_idx = elements.current_idx;
        let need_signal = Self::need_signals_for_pending(&elements);
        if current_idx > 0 {
            // update metas
            elements.current_idx     = 0;
            elements.high_watermark += current_idx;
            if need_signal {
                elements.pending_sends = 0;
            } else {
                elements.pending_sends += current_idx;
            }

            elements.swrs[(current_idx-1) as usize].next = std::ptr::null_mut();
            if need_signal {
                elements.swrs[(current_idx-1) as usize].send_flags |= ibv_send_flags::IBV_SEND_SIGNALED.0;
                elements.swrs[(current_idx-1) as usize].wr_id = elements.high_watermark << WRID_RESERVE_BITS; // for polling
            }
            let ret = unsafe {
                ibv_post_send((*self.meta.conn_id).qp, &mut elements.swrs[0] as *mut _, &mut bad_wr as *mut _)
            };

            elements.swrs[(current_idx-1) as usize].next = 
                &mut elements.swrs[current_idx as usize] as *mut _;

            if ret != 0 {
                return Err(TransError::TransRdmaError);
            }
        }
        Ok(())
    }

    // for send primitives
    pub fn send_pending(&self, msg: *mut u8, length: u32) -> TransResult<()> {
        let mut elements = self.elements.lock().unwrap();
        let current_idx = elements.current_idx as usize;
        // update metas
        elements.current_idx += 1;

        elements.ssges[current_idx].addr   = msg as _;
        elements.ssges[current_idx].length = length;

        // TODO: IBV_SEND_INLINE
        elements.swrs[current_idx].send_flags = 0;

        drop(elements);
        if current_idx+1 == MAX_DOORBELL_SEND_SIZE {
            return self.flush_pending();
        }
        Ok(())
    }

    // signals for sending pending
    #[inline]
    fn need_signals_for_pending(elements: &MutexGuard<RdmaElement>) -> bool {
        (elements.pending_sends + elements.current_idx) >= MAX_SIGNAL_PENDINGS as _
    }

    #[inline]
    pub fn set_low_watermark(&self, low_watermark: u64) {
        let mut elements = self.elements.lock().unwrap();
        elements.low_watermark = low_watermark;
    }

    #[inline]
    pub fn get_high_watermark(&self) -> u64 {
        return self.elements.lock().unwrap().high_watermark;
    }

    #[inline]
    pub fn need_poll(&self) -> bool {
        let elements = self.elements.lock().unwrap();
        (elements.high_watermark - elements.low_watermark) >= (MAX_SEND_SIZE / 2) as u64
    }

    // batch recv
    pub fn post_recvs(&self, recv_num: u64) -> TransResult<()> 
    {
        if recv_num <= 0 {
            return Ok(());
        }
        let mut elements = self.elements.lock().unwrap();

        let recv_head = elements.recv_head;
        let mut recv_tail = recv_head + recv_num - 1;
        if recv_tail > MAX_RECV_SIZE as u64 {
            recv_tail -= MAX_RECV_SIZE as u64;
        }

        let temp = elements.rwrs[recv_tail as usize].next;
        elements.rwrs[recv_tail as usize].next = std::ptr::null_mut();

        let mut bad_wr = std::ptr::null_mut();
        let ret = unsafe {
            ibv_post_recv(
                (*self.meta.conn_id).qp, 
                &mut elements.rwrs[recv_head as usize], 
                &mut bad_wr as *mut _
            )
        };

        if ret != 0 {
            return Err(TransError::TransRdmaError);
        }

        elements.recv_head = (recv_tail + 1) % (MAX_RECV_SIZE as u64);
        elements.rwrs[recv_tail as usize].next = temp;
        
        Ok(())
    }

    pub fn poll_comps(&self) -> i32 {
        let mut wc: ibv_wc = unsafe { std::mem::zeroed() };
        let mut poll_result = unsafe { 
            ibv_poll_cq(
            (*self.meta.conn_id).recv_cq,
            1,
            &mut wc as *mut _,
            )
        };
        if poll_result == 0 {
            poll_result = unsafe { 
                ibv_poll_cq(
                (*self.meta.conn_id).send_cq,
                1,
                &mut wc as *mut _,
                )
            };
        }

        if poll_result > 0 {
            // println!("poll one result  {}:{}", wc.opcode, wc.status);
            match wc.opcode {
                ibv_wc_opcode::IBV_WC_SEND => {
                    // println!("into send");
                },
                ibv_wc_opcode::IBV_WC_RECV => {
                    // println!("into recv");
                    let addr = wc.wr_id as *mut u8;
                    self.handler.lock().unwrap().upgrade().unwrap().rdma_recv_handler(addr);
                },
                _ => {
                    unimplemented!();
                }
            }
        }
        return poll_result;
    }

    #[inline]
    pub fn alloc_mr(&self, size: usize) -> TransResult<*mut u8> {
        let layout = Layout::from_size_align(size, std::mem::size_of::<usize>()).unwrap();
        let addr = unsafe { self.allocator.alloc(layout) };
        if addr.is_null() {
            return Err(TransError::TransRdmaError);
        } else {
            return Ok(addr);
        }
    }

    #[inline]
    pub fn deallocate_mr(&self, addr: *mut u8, size: usize) {
        let layout = Layout::from_size_align(size, std::mem::size_of::<usize>()).unwrap();
        unsafe { self.allocator.dealloc(addr, layout); }
    }

}

impl<'a> Drop for RdmaRcConn<'a> {
    fn drop(&mut self) {
        unsafe {
            rdma_dereg_mr(self.meta.lmr);
            rdma_disconnect(self.meta.conn_id);
            free(self.meta.lm as *mut _);
        }

    }
}