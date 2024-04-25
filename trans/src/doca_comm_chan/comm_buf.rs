use std::pin::Pin;
use doca::RawPointer;

use crate::{ MAX_CONN_INFO_BUFS, MAX_CONN_MSG_SIZE };
use super::DocaCommHeaderMeta;

pub struct DocaCommBuf {
    buf: Pin<Box<[u8; MAX_CONN_MSG_SIZE + 4]>>,
    pointer: RawPointer,
}

unsafe impl Send for DocaCommBuf {}
unsafe impl Sync for DocaCommBuf {}

impl DocaCommBuf {
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

    pub fn get_header(&self) -> DocaCommHeaderMeta {
        let header = unsafe { *(self.buf.as_ptr() as *const u32).as_ref().unwrap() };
        DocaCommHeaderMeta::from_header(header)
    }

    pub unsafe fn get_const_slice<ITEM:Clone>(&self, count: usize) -> &'static [ITEM] {
        unsafe {
            std::slice::from_raw_parts(self.buf.as_ptr().byte_add(4) as *const ITEM, count)
        }
    }

    pub unsafe fn get_const_ptr(&self) -> *const u8 {
        self.buf.as_ptr().byte_add(4)
    }

    #[inline]
    pub fn set_header(&mut self, meta: DocaCommHeaderMeta) {
        let header = unsafe { (self.buf.as_mut_ptr() as *mut u32).as_mut().unwrap() };
        *header = meta.to_header();
    }

    #[inline]
    pub fn set_payload(&mut self, payload: usize) {
        self.pointer.payload = payload + 4;
    }

    #[inline]
    pub fn get_payload(&mut self) -> usize {
        self.pointer.payload - 4
    }

    #[inline]
    pub unsafe fn append_item<ITEM: Clone>(&mut self, item: ITEM) {
        let slice_mut = std::slice::from_raw_parts_mut(self.buf.as_ptr().byte_add(self.pointer.payload) as *mut ITEM, 1);
        slice_mut[0] = item;
        self.pointer.payload += std::mem::size_of::<ITEM>();
    }
}

pub struct DocaCommBufAllocator {
    buf_pool: Vec<DocaCommBuf>,
}

impl DocaCommBufAllocator {
    pub fn new() -> Self {
        let mut pool = Vec::new();
        for _ in 0..MAX_CONN_INFO_BUFS {
            pool.push(DocaCommBuf::new())
        }

        Self {
            buf_pool: pool,
        }
    }

    pub fn alloc_buf(&mut self) -> DocaCommBuf {
        self.buf_pool.pop().unwrap()
    }

    pub fn dealloc_buf(&mut self, buf: DocaCommBuf) {
        self.buf_pool.push(buf);
    }
}

pub struct DocaCommReply {
    buf: Pin<Box<[u8; MAX_CONN_MSG_SIZE]>>,
    payload: usize,
    pending_count: usize,
}

impl DocaCommReply {
    pub fn new() -> Self {
        Self {
            buf: Box::pin([0u8; MAX_CONN_MSG_SIZE]),
            payload: 0,
            pending_count: 0,
        }
    }

    pub fn reset(&mut self, pending_count: usize) {
        self.payload = 0;
        self.pending_count += pending_count;
    }

    pub fn append_reply(&mut self, payload: usize) {
        self.payload += payload;
        self.pending_count -= 1;
    }

    pub unsafe fn get_mut_ptr(&mut self) -> *mut u8 {
        self.buf.as_mut_ptr().byte_add(self.payload)
    }

    pub unsafe fn get_const_head_ptr(&self) -> *const u8 {
        self.buf.as_ptr()
    }

    pub fn get_pending_count(&self) -> usize {
        self.pending_count
    }
}

pub struct DocaCommReplyWrapper {
    ptr: *const u8,
}

impl DocaCommReplyWrapper {
    pub fn new(ptr: *const u8) -> Self {
        Self {
            ptr: ptr
        }
    }
    pub fn get_item<ITEM: Clone>(&self, offset: usize) -> &ITEM {
        unsafe{ (self.ptr.byte_add(offset) as *const ITEM).as_ref().unwrap() }
    }
}