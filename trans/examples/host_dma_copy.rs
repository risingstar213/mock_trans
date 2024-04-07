use std::{ptr::NonNull, sync::Arc};

use doca::*;
use doca_sys::*;

use trans::doca_dma::host_helpers::send_doca_config;

fn main() {

    let pci_addr = "af.00.0";
    let cpy_txt = "This is a sample copy text";

    let length = cpy_txt.as_bytes().len();

    println!(
        "[Init] params check, pci: {}, cpy_txt {}, length {}",
        pci_addr, cpy_txt, length
    );

    let running = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));

    // first malloc the source buffer
    let mut src_buffer = vec![0u8; length].into_boxed_slice();

    // copy the text into src_buffer
    src_buffer.copy_from_slice(cpy_txt.as_bytes());
    let str = String::from_utf8(src_buffer.to_vec()).unwrap();
    println!("src_buffer check: {}", str);

    // Open device
    let device = doca::device::open_device_with_pci(pci_addr).unwrap();

    let mut local_mmap = Arc::new(DOCAMmap::new().unwrap());
    let local_mmap_ref = Arc::get_mut(&mut local_mmap).unwrap();

    let dev_idx = local_mmap_ref.add_device(&device).unwrap();

    let src_raw = RawPointer {
        inner: NonNull::new(src_buffer.as_mut_ptr() as *mut _).unwrap(),
        payload: length,
    };

    // populate the buffer into the mmap
    local_mmap_ref.set_memrange(src_raw).unwrap();

    local_mmap_ref.set_permission(doca_access_flags::DOCA_ACCESS_DPU_READ_ONLY).unwrap();

    local_mmap_ref.start().unwrap();

    // and export it into memory so later we can store it into a file
    // let export = local_mmap_ref.export_dpu(dev_idx).unwrap();
    // doca::save_config(export, src_raw, export_file, buffer_file).unwrap();
    // println!(
    //     "Please copy {} and {} to the DPU and run DMA Copy DPU sample before closing",
    //     export_file, buffer_file
    // );

    send_doca_config("192.168.100.2:7473".parse().unwrap(), 1, &mut local_mmap, src_raw);

    let r = running.clone();
    ctrlc::set_handler(move || {
        r.store(false, std::sync::atomic::Ordering::SeqCst);
    })
    .expect("Error setting Ctrl-C handler");

    while running.load(std::sync::atomic::Ordering::SeqCst) {
        // Your program's code goes here
        std::thread::sleep(std::time::Duration::from_millis(1000));
    }

    println!("Server is down!");
}
