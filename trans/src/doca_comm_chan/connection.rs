use std::sync::{ Arc, Mutex };
use lazy_static::lazy_static;

use doca::comm_chan::CommChannel;
use doca::device::{ open_device_with_pci, open_device_rep_with_pci };
use doca_sys::doca_error;


use super::conn_buf::{ DocaConnBufAllocator, DocaConnBuf };
pub struct DocaCommChannel {
    allocator: Mutex<DocaConnBufAllocator>,
    chan: Arc<CommChannel>,
}

impl DocaCommChannel {
    pub fn new_server(server_name: &str, pci_addr: &str, pci_addr_rep: &str) -> Self {
        let device = open_device_with_pci(pci_addr).unwrap();
        let device_rep = open_device_rep_with_pci(&device, pci_addr_rep).unwrap();
    
        Self {
            allocator: Mutex::new(DocaConnBufAllocator::new()),
            chan: CommChannel::create_server(server_name, &device, &device_rep)
        }
    }

    pub fn new_client(server_name: &str, pci_addr: &str) -> Self {
        let device = open_device_with_pci(pci_addr).unwrap();

        Self {
            allocator: Mutex::new(DocaConnBufAllocator::new()),
            chan: CommChannel::create_client(server_name, &device)
        }
    }

    pub fn block_send_info(&self, buf: &mut DocaConnBuf) {
        self.chan.block_send_req(buf.as_raw_pointer());
    }

    pub fn recv_info(&self, buf: &mut DocaConnBuf) -> doca_error {
        self.chan.recv_req(buf.as_raw_pointer())
    }

    pub fn alloc_buf(&self) -> DocaConnBuf {
        self.allocator.lock().unwrap().alloc_buf()
    }

    pub fn dealloc_buf(&self, buf: DocaConnBuf) {
        self.allocator.lock().unwrap().dealloc_buf(buf);
    }
}

pub trait DocaCommHandler {
    fn comm_handler(
        &self,
        buf: &DocaConnBuf
    );
}

pub struct DefaultDocaCommHandler;

impl DocaCommHandler for DefaultDocaCommHandler {
    #[allow(unused)]
    fn comm_handler(
        &self,
        buf: &DocaConnBuf
    ) {
        unimplemented!("comm handler");
    }
}

lazy_static! {
    pub static ref DEFAULT_DOCA_CONN_HANDLER: Arc<DefaultDocaCommHandler> = Arc::new(DefaultDocaCommHandler);
}