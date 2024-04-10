use clap::{arg, App, AppSettings};
use doca::{dma::DOCAContext, *};

use std::sync::Arc;

use trans::doca_dma::process_helpers::{ recv_doca_config, load_doca_config };
use trans::doca_dma::config::{ DocaConnInfo };

fn main() {

    let pci_addr = "af:00.0";

    let doca_conn = recv_doca_config("0.0.0.0:7473".parse().unwrap());
    let remote_config = load_doca_config(0, &doca_conn).unwrap();

    println!(
        "Check export len {}, remote len {}, remote addr {:?}",
        remote_config.export_desc.payload,
        remote_config.remote_addr.payload,
        remote_config.remote_addr.inner.as_ptr()
    );

    // Allocate the local buffer to store the transferred data
    #[allow(unused_mut)]
    let mut dpu_buffer = vec![0u8; remote_config.remote_addr.payload].into_boxed_slice();

    /* ********** The main test body ********** */

    // Create a DMA_ENGINE;
    let device = crate::open_device_with_pci(pci_addr).unwrap();

    let dma = DMAEngine::new().unwrap();

    let ctx = DOCAContext::new(&dma, vec![device.clone()]).unwrap();

    let mut workq = DOCAWorkQueue::new(1, &ctx).unwrap();

    let mut doca_mmap = Arc::new(DOCAMmap::new().unwrap());
    Arc::get_mut(&mut doca_mmap)
        .unwrap()
        .add_device(&device)
        .unwrap();

    // Create the remote mmap
    #[allow(unused_mut)]
    let mut remote_mmap =
        Arc::new(DOCAMmap::new_from_export(remote_config.export_desc, &device).unwrap());

    let inv = BufferInventory::new(1024).unwrap();
    let mut dma_src_buf =
        Arc::new(DOCARegisteredMemory::new_from_remote(&remote_mmap, remote_config.remote_addr)
            .unwrap()
            .to_buffer(&inv)
            .unwrap());
    unsafe {
        Arc::get_mut(&mut dma_src_buf)
            .unwrap()
            .set_data(0, remote_config.remote_addr.payload)
            .unwrap()
    };

    let dma_dst_buf =
        Arc::new(DOCARegisteredMemory::new(&doca_mmap, unsafe { RawPointer::from_box(&dpu_buffer) })
            .unwrap()
            .to_buffer(&inv)
            .unwrap());

    doca_mmap.start().unwrap();

    /* Start to submit the DMA job!  */
    let job = workq.create_dma_job(&dma_src_buf, &dma_dst_buf);
    workq.submit(&job).expect("failed to submit the job");

    loop {
        let event = workq.poll_completion();
        match event {
            Ok(_e) => {
                println!("Job finished!");
                break;
            }
            Err(e) => {
                if e == DOCAError::DOCA_ERROR_AGAIN {
                    continue;
                } else {
                    panic!("Job failed! {:?}", e);
                }
            }
        }
    }

    /* ------- Finalize check ---------- */
    println!(
        "[After] dst_buffer check: {}",
        String::from_utf8(dpu_buffer.to_vec()).unwrap()
    );
}
