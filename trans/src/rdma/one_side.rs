use rdma_sys::ibv_send_wr;

use crate::TransResult;

pub trait OneSideComm {
    fn post_batch(&mut self, send_wr: *mut ibv_send_wr, num: u64) -> TransResult<()>;
}
