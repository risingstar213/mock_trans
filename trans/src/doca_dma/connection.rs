use doca::dma::DOCADMAReusableJob;
use libc::memalign;
use libc::free;
use std::net::SocketAddr;
use std::sync::{ Arc, Mutex };
use std::ptr::NonNull;

use doca::{open_device_with_pci, DOCAEvent};
use doca::dma::{ DOCAContext, DOCADMAJob };
use doca::{ DOCAError, RawPointer, RawPointerMsg, DOCAResult, LoadedInfo, DOCABuffer, DOCARegisteredMemory, DOCAMmap, BufferInventory, DOCAWorkQueue, DMAEngine };
use doca_sys::doca_error;
use doca_sys::doca_access_flags;

use crate::{DOCA_WORKQ_DEPTH, MAX_DMA_BUF_REMOTE, MAX_DMA_BUF_SIZE, MAX_DMA_BUF_PER_ROUTINE};
use super::process_helpers::{ recv_doca_config, load_doca_config };
use super::export_helpers::send_doca_config;

use super::dma_shared_buffer::{ DmaLocalBuf, DmaLocalBufAllocator };
use super::dma_shared_buffer::{ DmaRemoteBuf, DmaRemoteBufAllocator };

pub struct DocaDmaControl {
    conn: Option<Arc<Mutex<DocaDmaConn>>>,
}

impl DocaDmaControl {
    pub fn new() -> Self {
        Self {
            conn: None
        }
    }
    pub fn listen_on(&mut self, pci_addr: &str, listen_addr: SocketAddr, coroutine_num: usize) {
        let conn_info = recv_doca_config(listen_addr);
        let config = load_doca_config(0, &conn_info).unwrap();

        assert!(config.remote_addr.payload >= (MAX_DMA_BUF_REMOTE * MAX_DMA_BUF_SIZE));

        let device = open_device_with_pci(pci_addr).unwrap();
        let dma = DMAEngine::new().unwrap();
        let ctx = DOCAContext::new(&dma, vec![device.clone()]).unwrap();

        let workq = Arc::new(DOCAWorkQueue::new(DOCA_WORKQ_DEPTH as _, &ctx).unwrap());
        // get local mmap
        let mut doca_mmap = Arc::new(DOCAMmap::new().unwrap());
        Arc::get_mut(&mut doca_mmap)
            .unwrap()
            .add_device(&device)
            .unwrap();
        // get remote mmap
        let remote_mmap =
            Arc::new(DOCAMmap::new_from_export(config.export_desc, &device).unwrap());

        let inv = BufferInventory::new(64).unwrap();
        // remote buf
        let mut remote_buf = 
            DOCARegisteredMemory::new_from_remote(&remote_mmap, config.remote_addr)
                .unwrap()
                .to_buffer(&inv)
                .unwrap();
        unsafe {
            remote_buf.set_data(0, config.remote_addr.payload).unwrap();
        }
        // local buf
        let lm_length = coroutine_num * MAX_DMA_BUF_SIZE * MAX_DMA_BUF_PER_ROUTINE;
        let lm = unsafe { memalign(4096, lm_length) } as *mut u8;
        let local_buf = 
            DOCARegisteredMemory::new(&doca_mmap, unsafe { RawPointer::from_raw_ptr(lm, lm_length) })
                .unwrap()
                .to_buffer(&inv)
                .unwrap();
        
        doca_mmap.start().unwrap();

        let conn = Arc::new(Mutex::new(DocaDmaConn::new(&workq, local_buf, remote_buf, lm, coroutine_num)));

        self.conn = Some(conn);

    }

    pub fn get_conn(&self) -> Option<Arc<Mutex<DocaDmaConn>>> {
        self.conn.clone()
    }

    // temporarily, the dpu side just loop until down, becasuse it doesn't need anything
    pub fn connect_and_waiting_loop(&mut self, pci_addr: &str, connect_addr: SocketAddr) {
        let running = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
        let buffer_length = MAX_DMA_BUF_REMOTE * MAX_DMA_BUF_SIZE;

        // first malloc the source buffer
        let mut src_buffer = vec![0u8; buffer_length].into_boxed_slice();

        // Open device
        let device = doca::device::open_device_with_pci(pci_addr).unwrap();

        let mut local_mmap = Arc::new(DOCAMmap::new().unwrap());
        let local_mmap_ref = Arc::get_mut(&mut local_mmap).unwrap();

        let dev_idx = local_mmap_ref.add_device(&device).unwrap();

        let src_raw = RawPointer {
            inner: NonNull::new(src_buffer.as_mut_ptr() as *mut _).unwrap(),
            payload: buffer_length,
        };

        // populate the buffer into the mmap
        local_mmap_ref.set_memrange(src_raw).unwrap();

        local_mmap_ref.set_permission(doca_access_flags::DOCA_ACCESS_DPU_READ_WRITE).unwrap();

        local_mmap_ref.start().unwrap();

        send_doca_config(connect_addr, 1, &mut local_mmap, src_raw);

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
}

pub struct DocaDmaConn {
    lm:            *mut u8,
    workq:         Arc<DOCAWorkQueue<DMAEngine>>,
    dma_job:       DOCADMAReusableJob,
    local_alloc:   DmaLocalBufAllocator,
    remote_alloc:  DmaRemoteBufAllocator,
}

impl DocaDmaConn {
    pub fn new(workq: &Arc<DOCAWorkQueue<DMAEngine>>, local_buf: DOCABuffer, remote_buf: DOCABuffer, lm: *mut u8, coroutine_num: usize) -> Self {
        let dma_job = workq.create_dma_reusable_job(local_buf, remote_buf);
        Self {
            lm: lm,
            workq: workq.clone(),
            dma_job: dma_job,
            local_alloc:  DmaLocalBufAllocator::new(coroutine_num as _, lm as _, coroutine_num as usize * MAX_DMA_BUF_PER_ROUTINE * MAX_DMA_BUF_SIZE),
            remote_alloc: DmaRemoteBufAllocator::new(),
        }
    }

    #[inline]
    pub fn post_read_dma_reqs(&mut self, local_offset: usize, remote_offset: usize, payload: usize, user_mark: u64) {
        self.dma_job.set_local_data(local_offset, payload);
        self.dma_job.set_remote_data(remote_offset, payload);
        self.dma_job.set_user_data_write(user_mark, false);

        Arc::get_mut(&mut self.workq)
            .unwrap()
            .submit(&self.dma_job)
            .unwrap();
    }

    #[inline]
    pub fn post_write_dma_reqs(&mut self, local_offset: usize, remote_offset: usize, payload: usize, user_mark: u64) {
        self.dma_job.set_local_data(local_offset, payload);
        self.dma_job.set_remote_data(remote_offset, payload);
        self.dma_job.set_user_data_write(user_mark, true);

        Arc::get_mut(&mut self.workq)
            .unwrap()
            .submit(&self.dma_job)
            .unwrap();
    }

    #[inline]
    pub fn poll_completion(&mut self) -> (DOCAEvent, doca_error) {
        Arc::get_mut(&mut self.workq)
            .unwrap()
            .progress_retrieve()
    }

    #[inline]
    pub fn get_local_buf(&mut self, cid: u32) -> DmaLocalBuf {
        self.local_alloc.get_local_buf(cid)
    }

    #[inline]
    pub fn alloc_remote_buf(&mut self) -> DmaRemoteBuf {
        self.remote_alloc.alloc_remote_buf()
    }

    #[inline]
    pub fn dealloc_remote_buf(&mut self, buf: DmaRemoteBuf) {
        self.remote_alloc.dealloc_remote_buf(buf);
    }

}

impl Drop for DocaDmaConn {
    fn drop(&mut self) {
        unsafe { free(self.lm as _); }
    }
}