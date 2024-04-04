use super::BatchRpcReqHeader;
use super::BatchRpcRespHeader;

pub struct BatchRpcReqWrapper {
    buf: usize,
    off: usize,
    cap: usize,
}

impl BatchRpcReqWrapper {
    pub fn new(buf: *mut u8, cap: usize) -> Self {
        Self {
            buf: buf as usize,
            off: std::mem::size_of::<BatchRpcReqHeader>(),
            cap: cap,
        }
    }

    pub fn get_header(&self) -> BatchRpcReqHeader {
        let req_header = unsafe { (self.buf as *const BatchRpcReqHeader).as_ref().unwrap() };

        req_header.clone()
    }

    pub fn set_header(&mut self, header: BatchRpcReqHeader) {
        let req_header = unsafe { (self.buf as *mut BatchRpcReqHeader).as_mut().unwrap() };
        *req_header = header;
    }

    pub fn get_item<ITEM: Clone>(&self) -> &ITEM {
        let ptr = unsafe { (self.buf as *mut u8).byte_add(self.off) };
        unsafe { (ptr as *const ITEM).as_ref().unwrap() }
    }

    pub fn set_item<ITEM: Clone>(&mut self, item: ITEM) {
        let ptr = unsafe { (self.buf as *mut u8).byte_add(self.off) };
        let mut_item = unsafe { (ptr as *mut ITEM).as_mut().unwrap() };
        *mut_item = item;
    }

    // optional
    pub fn get_extra_data_raw_ptr<ITEM: Clone>(&mut self) -> *mut u8 {
        let ptr = unsafe { (self.buf as *mut u8).byte_add(self.off + std::mem::size_of::<ITEM>()) };
        ptr
    }

    pub fn get_extra_data_const_ptr<ITEM: Clone>(&self) -> *const u8 {
        let ptr = unsafe { (self.buf as *mut u8).byte_add(self.off + std::mem::size_of::<ITEM>()) };
        ptr
    }

    pub fn shift_to_next_item<ITEM: Clone>(&mut self, extra_data_len: usize) {
        self.off += std::mem::size_of::<ITEM>() + extra_data_len;
    }

    pub fn get_cap(&self) -> usize {
        self.cap
    }

    pub fn get_off(&self) -> usize {
        self.off
    }

    pub fn get_buf(&mut self) -> *mut u8 {
        self.buf as _
    }
}

pub struct BatchRpcRespWrapper {
    buf: usize,
    off: usize,
    cap: usize,
}

impl BatchRpcRespWrapper {
    pub fn new(buf: *mut u8, cap: usize) -> Self {
        Self {
            buf: buf as usize,
            off: std::mem::size_of::<BatchRpcRespHeader>(),
            cap: cap,
        }
    }

    pub fn get_header(&self) -> BatchRpcRespHeader {
        let resp_header = unsafe { (self.buf as *const BatchRpcRespHeader).as_ref().unwrap() };
        resp_header.clone()
    }

    pub fn set_header(&mut self, header: BatchRpcRespHeader) {
        let resp_header = unsafe { (self.buf as *mut BatchRpcRespHeader).as_mut().unwrap() };
        *resp_header = header;
    }

    pub fn get_item<ITEM: Clone>(&self) -> &ITEM {
        let ptr = unsafe { (self.buf as *mut u8).byte_add(self.off) };
        unsafe { (ptr as *const ITEM).as_ref().unwrap() }
    }

    pub fn set_item<ITEM: Clone>(&mut self, item: ITEM) {
        let ptr = unsafe { (self.buf as *mut u8).byte_add(self.off) };
        let mut_item = unsafe { (ptr as *mut ITEM).as_mut().unwrap() };
        *mut_item = item;
    }
    // optional
    pub fn get_extra_data_raw_ptr<ITEM: Clone>(&mut self) -> *mut u8 {
        let ptr = unsafe { (self.buf as *mut u8).byte_add(self.off + std::mem::size_of::<ITEM>()) };
        ptr
    }

    pub fn get_extra_data_const_ptr<ITEM: Clone>(&self) -> *const u8 {
        let ptr = unsafe { (self.buf as *mut u8).byte_add(self.off + std::mem::size_of::<ITEM>()) };
        ptr
    }

    pub fn shift_to_next_item<ITEM: Clone>(&mut self, extra_data_len: usize) {
        self.off += std::mem::size_of::<ITEM>() + extra_data_len;
    }

    pub fn get_cap(&self) -> usize {
        self.cap
    }

    pub fn get_off(&self) -> usize {
        self.off
    }

}