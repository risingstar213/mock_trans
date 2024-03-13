use libc::free;
use rdma_sys::*;

use crate::{TransError, TransResult, NPAGES};
use ll_alloc::LockedHeap;

pub struct RdmaConnection {
    conn_id: *mut rdma_cm_id,
    lm: *mut u8,
    lmr: *mut ibv_mr,
    raddr: u64,
    rid: u32,
    allocator: LockedHeap
}

// RC Connection 
impl RdmaConnection {
    pub fn new(id: *mut rdma_cm_id, lm: *mut u8, lmr: *mut ibv_mr, raddr: u64, rid: u32) -> Self {
        Self {
            conn_id: id,
            lm: lm,
            lmr: lmr,
            raddr: raddr,
            rid: rid,
            allocator: unsafe { LockedHeap::new(lm, (NPAGES * 4096) as usize) },
        }
    }

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
            lkey:   unsafe { (*self.lmr).lkey }
        };

        let mut sr = unsafe { std::mem::zeroed::<ibv_send_wr>() };

        sr.wr_id = wr_id;
        sr.opcode = wr_op;
        sr.num_sge = 1;
        sr.next = std::ptr::null_mut();
        sr.sg_list = &mut sge as *mut _;
        sr.send_flags = flags;
        sr.imm_data_invalidated_rkey_union.imm_data = imm;

        sr.wr.rdma.remote_addr = self.raddr;
        sr.wr.rdma.rkey        = self.rid;

        let ret = unsafe {
            rdma_seterrno(ibv_post_send((*self.conn_id).qp, &mut sr, &mut bad_sr))
        };

        if ret != 0 {
            return Err(TransError::TransRdmaError);
        }

        Ok(())
    }

    // batch recv
    pub fn post_recvs(recv_num: u64) -> TransResult<()> 
    {
        if recv_num <= 0 {
            return Ok(());
        }


        Ok(())
    }

}

impl Drop for RdmaConnection {
    fn drop(&mut self) {
        unsafe {
            rdma_dereg_mr(self.lmr);
            rdma_disconnect(self.conn_id);
            free(self.lm as *mut _);
        }

    }
}