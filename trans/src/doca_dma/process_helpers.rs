/*
 *  This code refer to smartnic-bench from smartnickit-project
 * 
 *  https://github.com/smartnickit-project/smartnic-bench.git
 * 
 */

use std::io::Read;
use std::sync::Arc;
use std::ptr::{ NonNull, null_mut };
use std::time::Duration;
use std::net::SocketAddr;

use doca::context::work_queue;
use doca::{open_device_with_pci, DOCAEvent};
use doca::dma::{ DOCAContext, DOCADMAJob };
use doca::{ DOCAError, RawPointer, RawPointerMsg, DOCAResult, LoadedInfo, DOCABuffer, DOCARegisteredMemory, DOCAMmap, BufferInventory, DOCAWorkQueue, DMAEngine };
use doca_sys::doca_error;

use std::net::{ TcpListener };

use crate::common::connection::recv_config;

use super::config::DOCA_MAX_CONN_LENGTH;
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

pub struct DocaDmaConn {
    workq:         Arc<DOCAWorkQueue<DMAEngine>>,
    read_dma_job:  DOCADMAJob,
    write_dma_job: DOCADMAJob,
}

impl DocaDmaConn {
    pub fn new(workq: &Arc<DOCAWorkQueue<DMAEngine>>, local_buf: &Arc<DOCABuffer>, remote_buf: &Arc<DOCABuffer>) -> Self {
        let read_dma_job = workq.create_dma_job(remote_buf, local_buf);
        let write_dma_job = workq.create_dma_job(local_buf, remote_buf);
        Self {
            workq: workq.clone(),
            read_dma_job: read_dma_job,
            write_dma_job: write_dma_job,
        }
    }

    #[inline]
    pub fn post_read_dma_reqs(&mut self, src_offset: usize, dst_offset: usize, payload: usize, user_mark: u64) {
        self.read_dma_job.set_src_data(src_offset, payload);
        self.read_dma_job.set_dst_data(dst_offset, payload);
        self.read_dma_job.set_user_data(user_mark);

        Arc::get_mut(&mut self.workq)
            .unwrap()
            .submit(&self.read_dma_job)
            .unwrap();
    }

    #[inline]
    pub fn post_write_dma_reqs(&mut self, src_offset: usize, dst_offset: usize, payload: usize, user_mark: u64) {
        self.write_dma_job.set_src_data(src_offset, payload);
        self.write_dma_job.set_dst_data(dst_offset, payload);
        self.write_dma_job.set_user_data(user_mark);

        Arc::get_mut(&mut self.workq)
            .unwrap()
            .submit(&self.write_dma_job)
            .unwrap();
    }

    #[inline]
    pub fn poll_completion(&mut self) -> (DOCAEvent, doca_error) {
        Arc::get_mut(&mut self.workq)
            .unwrap()
            .progress_retrieve()
    }

}