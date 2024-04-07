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

use doca::open_device_with_pci;
use doca::dma::{ DOCAContext, DOCADMAJob };
use doca::{ DOCAError, RawPointer, RawPointerMsg, DOCAResult, LoadedInfo, DOCABuffer, DOCARegisteredMemory, DOCAMmap, BufferInventory, DOCAWorkQueue, DMAEngine };

use std::net::{ TcpListener };

use super::connections::DOCA_MAX_CONN_LENGTH;
use super::connections::DocaConnInfo;

pub fn recv_doca_config(addr: SocketAddr) -> Vec<u8> {
    let mut conn_info = [0u8; DOCA_MAX_CONN_LENGTH];
    let mut conn_info_len = 0;
    /* receive the DOCA buffer message from the host */
    let listener = TcpListener::bind(addr).unwrap();
    loop {
        if let Ok(res) = listener.accept() {
            let (mut stream, _) = res;
            conn_info_len = stream.read(&mut conn_info).unwrap();
            break;
        }
    }

    conn_info[0..conn_info_len].to_vec()
}

pub fn load_doca_config(thread_id: usize, doca_conn: &DocaConnInfo) -> DOCAResult<LoadedInfo> {
    /* parse the received messages */
    let dev_id = thread_id % doca_conn.exports.len();
    let buf_id = thread_id % doca_conn.buffers.len();
    let mut export_desc_buffer = doca_conn.exports[dev_id].to_vec().into_boxed_slice();
    let export_payload = export_desc_buffer.len();
    Ok(LoadedInfo {
        export_desc: RawPointer {
            inner: NonNull::new(Box::into_raw(export_desc_buffer) as *mut _).unwrap(),
            payload: export_payload,
        },
        remote_addr: doca_conn.buffers[buf_id],
    })
}