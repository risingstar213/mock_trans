/*
 *  This code refer to smartnic-bench from smartnickit-project
 * 
 *  https://github.com/smartnickit-project/smartnic-bench.git
 * 
 */

use std::sync::atomic::{ compiler_fence, Ordering };
use std::slice;
use std::net::{ SocketAddr, TcpStream };
use std::io::Write;
use std::time::Duration;
use std::ptr::{ NonNull, null_mut };
use std::sync::Arc;

use doca::dma::DOCAContext;
use doca::{ DOCAMmap, DOCARegisteredMemory, BufferInventory, DOCAWorkQueue, DMAEngine, RawPointer, RawPointerMsg };

use crate::common::connection::send_config;
use super::config::{ DocaConnInfo, DocaConnInfoMsg };

pub fn send_doca_config(addr: SocketAddr, num_dev: usize, doca_mmap: &mut Arc<DOCAMmap>, src_buf: RawPointer) {
    let mut doca_conn: DocaConnInfo = Default::default();

    for i in 0..num_dev {
        let export_desc = 
            Arc::get_mut(doca_mmap)
                .expect("doca map is owned by more than once!")
                .export_dpu(i)
                .unwrap();

        doca_conn.exports.push(unsafe {
            slice::from_raw_parts_mut(export_desc.inner.as_ptr() as *mut _, export_desc.payload).to_vec()
        });
    }
    doca_conn.buffers.push(src_buf);
    send_config::<DocaConnInfoMsg>(addr, doca_conn.into())
}
