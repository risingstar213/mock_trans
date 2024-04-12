/*
 *  This code refer to smartnic-bench from smartnickit-project
 * 
 *  https://github.com/smartnickit-project/smartnic-bench.git
 * 
 */

use libc::free;

use std::io::Read;
use std::sync::Arc;
use std::ptr::{ NonNull, null_mut };
use std::net::SocketAddr;

use doca::context::work_queue;
use doca::{open_device_with_pci, DOCAEvent};
use doca::dma::{ DOCAContext, DOCADMAJob };
use doca::{ DOCAError, RawPointer, RawPointerMsg, DOCAResult, LoadedInfo, DOCABuffer, DOCARegisteredMemory, DOCAMmap, BufferInventory, DOCAWorkQueue, DMAEngine };

use crate::common::connection::recv_config;

use super::config::{ DocaConnInfo, DocaConnInfoMsg };

pub fn recv_doca_config(addr: SocketAddr) -> DocaConnInfo {
    recv_config::<DocaConnInfoMsg>(addr).into()
}

pub fn load_doca_config(thread_id: usize, doca_conn: &DocaConnInfo) -> DOCAResult<LoadedInfo> {
    /* parse the received messages */
    let dev_id = thread_id % doca_conn.exports.len();
    let buf_id = thread_id % doca_conn.buffers.len();
    let export_desc_buffer = doca_conn.exports[dev_id].to_vec().into_boxed_slice();
    let export_payload = export_desc_buffer.len();
    Ok(LoadedInfo {
        export_desc: RawPointer {
            inner: NonNull::new(Box::into_raw(export_desc_buffer) as *mut _).unwrap(),
            payload: export_payload,
        },
        remote_addr: doca_conn.buffers[buf_id],
    })
}