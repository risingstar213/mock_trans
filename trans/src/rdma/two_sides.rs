use crate::TransResult;

pub trait TwoSidesComm {
    fn send_pending(&mut self, msg: *mut u8, length: u32) -> TransResult<()>;
    fn flush_pending(&mut self) -> TransResult<()>;
    fn send_one(&mut self, msg: *mut u8, length: u32) {
        self.send_pending(msg, length).unwrap();
        self.flush_pending().unwrap();
    }
}

