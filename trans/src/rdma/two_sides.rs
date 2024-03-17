use crate::TransResult;

pub trait TwoSidesComm {
    fn send_pending(&self, msg: *mut u8, length: u32) -> TransResult<()>;
    fn flush_pending(&self) -> TransResult<()>;
    fn send_one(&self, msg: *mut u8) {
        self.send_pending(msg, 1).unwrap();
        self.flush_pending().unwrap();
    }
}

