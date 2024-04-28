use std::{os::raw::c_void, pin::Pin, ptr::NonNull};
use libc::{ memalign, free };
use doca::RawPointer;

use std::collections::vec_deque::VecDeque;

use crate::{ MAX_CONN_INFO_BUFS, MAX_CONN_MSG_SIZE };
use super::DocaCommHeaderMeta;

pub struct DocaCommBuf {
    buf: NonNull<c_void>,
    pointer: RawPointer,
    read_idx: usize,
}

impl Drop for DocaCommBuf {
    fn drop(&mut self) {
        unsafe {
            println!("free buf !");
            free(self.buf.as_ptr() as _);
        }
    }
}

unsafe impl Send for DocaCommBuf {}
unsafe impl Sync for DocaCommBuf {}

impl DocaCommBuf {
    pub fn new() -> Self {
        let buf = unsafe { memalign(8, MAX_CONN_MSG_SIZE) };
        let pointer = unsafe {
            RawPointer::from_raw_ptr(
                buf as _, 
                0
            )
        };

        Self {
            buf: NonNull::new(buf).unwrap(),
            pointer: pointer,
            read_idx: 0,
        }
    }

    pub fn as_raw_pointer(&mut self) -> &mut RawPointer {
       &mut self.pointer
    }

    #[inline]
    pub fn set_payload(&mut self, payload: usize) {
        self.pointer.payload = payload;
    }

    #[inline]
    pub fn get_payload(&mut self) -> usize {
        self.pointer.payload
    }

    #[inline]
    pub unsafe fn append_empty(&mut self, header: DocaCommHeaderMeta) {
        // println!("append empty!");
        let header_size = 4;
        let header_mut = unsafe{ (self.buf.as_ptr().byte_add(self.pointer.payload) as *mut u32).as_mut().unwrap() };
        *header_mut = header.to_raw();
        self.pointer.payload += header_size;
    }

    #[inline]
    pub unsafe fn append_item<ITEM: Clone>(&mut self, header: DocaCommHeaderMeta, item: ITEM) {
        let header_size = 4;
        let item_size = std::mem::size_of::<ITEM>();
        // println!("append item {}!",item_size);
        let header_mut = unsafe{ (self.buf.as_ptr().byte_add(self.pointer.payload) as *mut u32).as_mut().unwrap() };
        *header_mut = header.to_raw();
        let item_mut = unsafe{ (self.buf.as_ptr().byte_add(self.pointer.payload + header_size) as *mut ITEM).as_mut().unwrap() };
        *item_mut = item;
        self.pointer.payload += header_size + item_size;
    }

    #[inline]
    pub unsafe fn append_seqs(&mut self, header: DocaCommHeaderMeta, ptr: *const u8, len: usize) {
        // println!("append seqs {}!", len);
        let header_size = 4;
        let header_mut = unsafe{ (self.buf.as_ptr().byte_add(self.pointer.payload) as *mut u32).as_mut().unwrap() };
        *header_mut = header.to_raw();
        std::ptr::copy_nonoverlapping(ptr, self.buf.as_ptr().byte_add(self.pointer.payload + header_size) as _, len);
        self.pointer.payload += header_size + len;
    }

    #[inline]
    pub fn start_read(&mut self) {
        self.read_idx = 0;
    }

    #[inline]
    pub fn get_header(&self) -> Option<DocaCommHeaderMeta> {
        if self.read_idx >= self.pointer.payload {
            return None;
        }

        let raw = unsafe{ (self.buf.as_ptr().byte_add(self.read_idx) as *const u32).as_ref().unwrap() };
        Some(DocaCommHeaderMeta::from_header(*raw))
    }

    #[inline]
    pub fn get_item<ITEM: Clone>(&self, idx: usize) -> &ITEM {
        let offset = idx * std::mem::size_of::<ITEM>();
        unsafe{ (self.buf.as_ptr().byte_add(self.read_idx + 4 + offset) as *const ITEM).as_ref().unwrap() }
    }

    pub fn get_read_idx(&self) -> usize {
        self.read_idx
    }

    pub fn shift_to_next_msg(&mut self, payload: u32) {
        self.read_idx += 4 + payload as usize;
    }
}

pub struct DocaCommBufAllocator {
    buf_pool: VecDeque<DocaCommBuf>,
}

impl DocaCommBufAllocator {
    pub fn new() -> Self {
        let mut pool = VecDeque::new();
        for _ in 0..MAX_CONN_INFO_BUFS {
            pool.push_back(DocaCommBuf::new())
        }

        Self {
            buf_pool: pool,
        }
    }

    pub fn alloc_buf(&mut self, cid: u32) -> DocaCommBuf {
        self.buf_pool.pop_front().unwrap()
    }

    pub fn dealloc_buf(&mut self, buf: DocaCommBuf, cid: u32) {
        self.buf_pool.push_back(buf);
    }
}

pub struct DocaCommReply {
    success: bool,
    pending_count: usize,
}

impl DocaCommReply {
    pub fn new() -> Self {
        Self {
            success: true,
            pending_count: 0,
        }
    }

    pub fn reset(&mut self, pending_count: usize) {
        self.success = true;
        self.pending_count = pending_count;
    }

    pub fn append_reply(&mut self, success: u32) {
        self.success = self.success && (success > 0);
        self.pending_count -= 1;
    }

    pub fn get_pending_count(&self) -> usize {
        self.pending_count
    }

    pub fn get_success(&self) -> bool {
        self.success
    }
}