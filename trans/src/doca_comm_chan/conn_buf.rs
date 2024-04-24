use std::pin::Pin;
use doca::RawPointer;

use crate::{ MAX_CONN_INFO_BUFS, MAX_CONN_MSG_SIZE };
use super::DocaConnHeaderMeta;

pub struct DocaConnBuf {
    buf: Pin<Box<[u8; MAX_CONN_MSG_SIZE + 4]>>,
    pointer: RawPointer,
}

impl DocaConnBuf {
    pub fn new() -> Self {
        let mut buf = Box::pin([0u8; MAX_CONN_MSG_SIZE + 4]);
        let pointer = unsafe {
            RawPointer::from_raw_ptr(
                buf.as_mut_ptr(), 
                0
            )
        };

        Self {
            buf: buf,
            pointer: pointer,
        }
    }

    pub fn as_raw_pointer(&mut self) -> &mut RawPointer {
       &mut self.pointer
    }

    pub fn get_header(&self) -> DocaConnHeaderMeta {
        let header = unsafe { *(self.buf.as_ptr() as *const u32).as_ref().unwrap() };
        DocaConnHeaderMeta::from_header(header)
    }

    pub unsafe fn get_const_slice<ITEM:Clone>(&self, count: usize) -> &'static [ITEM] {
        unsafe {
            std::slice::from_raw_parts(self.buf.as_ptr().byte_add(4) as *const ITEM, count)
        }
    }

    pub fn set_header(&mut self, meta: DocaConnHeaderMeta) {
        let header = unsafe { (self.buf.as_mut_ptr() as *mut u32).as_mut().unwrap() };
        *header = meta.to_header();
    }

    pub fn set_payload(&mut self, payload: usize) {
        self.pointer.payload = payload + 4;
    }

    pub unsafe fn set_item<ITEM: Clone>(&mut self, idx: usize, item: ITEM) {
        let slice_mut = std::slice::from_raw_parts_mut(self.buf.as_ptr().byte_add(4) as *mut ITEM, idx + 1);
        slice_mut[idx] = item;
    }
}

pub struct DocaConnBufAllocator {
    buf_pool: Vec<DocaConnBuf>,
}

impl DocaConnBufAllocator {
    pub fn new() -> Self {
        let mut pool = Vec::new();
        for _ in 0..MAX_CONN_INFO_BUFS {
            pool.push(DocaConnBuf::new())
        }

        Self {
            buf_pool: pool,
        }
    }

    pub fn alloc_buf(&mut self) -> DocaConnBuf {
        self.buf_pool.pop().unwrap()
    }

    pub fn dealloc_buf(&mut self, buf: DocaConnBuf) {
        self.buf_pool.push(buf);
    }
}