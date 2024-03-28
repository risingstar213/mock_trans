#![allow(unused)]

use libc::*;
use rdma_sys::*;
use std::ptr;

enum MsgType {
    MsgSync, // used for sync for start/end/write/read
    MsgData,
    MsgCmd,
    MsgAddr,
}

#[inline]
pub unsafe fn rdma_post_send_with_imm(
    id: *mut rdma_cm_id,
    context: *mut c_void,
    addr: *mut c_void,
    length: usize,
    mr: *mut ibv_mr,
    flags: c_int,
    imm: c_int,
) -> c_int {
    let mut sge = ibv_sge {
        addr: addr as u64,
        length: length as u32,
        lkey: if !mr.is_null() { (*mr).lkey } else { 0 },
    };
    let nsge = 1;

    let mut wr = std::mem::zeroed::<ibv_send_wr>();
    wr.wr_id = context as u64;
    wr.next = ptr::null::<ibv_send_wr>() as *mut _;
    wr.sg_list = &mut sge as *mut ibv_sge;
    wr.num_sge = nsge;
    wr.opcode = ibv_wr_opcode::IBV_WR_SEND;
    wr.send_flags = flags as c_uint;
    wr.imm_data_invalidated_rkey_union.imm_data = imm as u32;
    let mut bad = ptr::null::<ibv_send_wr>() as *mut _;

    rdma_seterrno(ibv_post_send((*id).qp, &mut wr, &mut bad))
}

pub struct AddRequest {
    a: u8,
    b: u8,
}

pub struct AddResponse {
    sum: u8,
}
