use std::sync::{ Arc, Mutex };
use std::cell::UnsafeCell;
use lazy_static::lazy_static;

use doca::comm_chan::CommChannel;
use doca::device::{ open_device_with_pci, open_device_rep_with_pci };
use doca_sys::doca_error;

use super::DocaCommHeaderMeta;
use crate::MAX_CONN_MSG_SIZE;

use super::comm_buf::{ DocaCommBufAllocator, DocaCommBuf };
pub struct DocaCommChannel {
    allocator: UnsafeCell<DocaCommBufAllocator>,
    chan: Arc<CommChannel>,
    buf:  UnsafeCell<Option<DocaCommBuf>>,
}

impl DocaCommChannel {
    pub fn new_server(server_name: &str, pci_addr: &str, pci_addr_rep: &str) -> Self {
        let device = open_device_with_pci(pci_addr).unwrap();
        let device_rep = open_device_rep_with_pci(&device, pci_addr_rep).unwrap();

        let allocator = DocaCommBufAllocator::new();
    
        Self {
            allocator: UnsafeCell::new(allocator),
            chan: CommChannel::create_server(server_name, &device, &device_rep),
            buf: UnsafeCell::new(None),
        }
    }

    pub fn new_client(server_name: &str, pci_addr: &str) -> Self {
        let device = open_device_with_pci(pci_addr).unwrap();

        let allocator = DocaCommBufAllocator::new();

        Self {
            allocator: UnsafeCell::new(allocator),
            chan: CommChannel::create_client(server_name, &device),
            buf: UnsafeCell::new(None),
        }
    }

    #[inline]
    fn get_write_buf(&self, total_size: usize) -> &mut DocaCommBuf {
        let buf_opt = unsafe { self.buf.get().as_mut().unwrap() };
        let allocator = unsafe { self.allocator.get().as_mut().unwrap() };
        if buf_opt.is_none() {
            let mut alloc_buf = allocator.alloc_buf(0);
            alloc_buf.set_payload(0);
            *buf_opt = Some(alloc_buf);
        } else {
            let buf = buf_opt.as_mut().unwrap();
            if buf.get_payload() + total_size > MAX_CONN_MSG_SIZE {
                self.chan.block_send_req(buf.as_raw_pointer());
                allocator.dealloc_buf(buf_opt.take().unwrap(), 0);
                let mut alloc_buf = allocator.alloc_buf(0);
                alloc_buf.set_payload(0);
                *buf_opt = Some(alloc_buf);
            }
        }

        buf_opt.as_mut().unwrap()
    }

    pub fn append_empty_msg(&self, header: DocaCommHeaderMeta) {
        let total_size = 4;
        assert!(0 == header.info_payload as usize);

        let buf = self.get_write_buf(total_size);

        unsafe {
            buf.append_empty(header);
        }
    }

    pub fn append_item_msg<ITEM: Clone>(&self, header: DocaCommHeaderMeta, item: ITEM) {
        assert!(std::mem::size_of::<ITEM>() % 4 == 0);
        assert!(std::mem::size_of::<ITEM>() == header.info_payload as usize);
        
        let total_size = std::mem::size_of::<ITEM>() + 4;
        let buf = self.get_write_buf(total_size);

        unsafe {
            buf.append_item(header, item);
        }
    }

    pub fn append_slice_msg<ITEM: Clone>(&self, header: DocaCommHeaderMeta, items: &[ITEM]) {
        let items_size = std::mem::size_of::<ITEM>() * items.len();
        
        assert!(std::mem::size_of::<ITEM>() % 4 == 0);
        assert!(items_size == header.info_payload as usize);

        let total_size = items_size + 4;
        let buf = self.get_write_buf(total_size);

        unsafe {
            buf.append_seqs(header,items.as_ptr() as _, items_size);
        }
    }

    pub fn flush_pending_msgs(&self) {
        let buf_opt = unsafe { self.buf.get().as_mut().unwrap() };
        let allocator = unsafe { self.allocator.get().as_mut().unwrap() };
        if let Some(mut buf) = buf_opt.take() {
            self.chan.block_send_req(buf.as_raw_pointer());
            allocator.dealloc_buf(buf, 0);
        }
    }

    pub fn recv_info(&self, buf: &mut DocaCommBuf) -> doca_error {
        self.chan.recv_req(buf.as_raw_pointer())
    }

    pub fn alloc_buf(&self, cid: u32) -> DocaCommBuf {
        let allocator = unsafe { self.allocator.get().as_mut().unwrap() };
        allocator.alloc_buf(cid)
    }

    pub fn dealloc_buf(&self, buf: DocaCommBuf, cid: u32) {
        let allocator = unsafe { self.allocator.get().as_mut().unwrap() };
        allocator.dealloc_buf(buf, cid);
    }
}

pub trait DocaCommHandler {
    fn comm_handler(
        &self,
        buf: &DocaCommBuf,
        info_id: u32,
        info_payload: u32,
        info_pid: u32,
        info_tid: u32,
        info_cid: u32,
    );
}

pub struct DefaultDocaCommHandler;

impl DocaCommHandler for DefaultDocaCommHandler {
    #[allow(unused)]
    fn comm_handler(
        &self,
        buf: &DocaCommBuf,
        info_id: u32,
        info_payload: u32,
        info_pid: u32,
        info_tid: u32,
        info_cid: u32,
    ) {
        unimplemented!("comm handler");
    }
}

lazy_static! {
    pub static ref DEFAULT_DOCA_CONN_HANDLER: Arc<DefaultDocaCommHandler> = Arc::new(DefaultDocaCommHandler);
}