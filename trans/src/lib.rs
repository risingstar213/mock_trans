pub mod common;
pub mod rdma;
pub mod doca_dma;
pub mod framework;
pub mod memstore;
pub mod occ;

#[allow(unused)]
#[derive(Debug)]
pub enum TransError {
    TransRdmaError,
    TransDocaError,
    TransSyncError
}

type TransResult<T> = Result<T, TransError>;


// connection info
const PEERNUMS:u64 = 2;
const SERVERs: [&str; 2] = ["10.10.10.6\0", "10.10.10.9\0"];
const PORTs: [&str; 2] = ["7471\0", "7472\0"];

// mem info
const NPAGES: u64 = 8;
const MAX_PACKET_SIZE: usize = 128;

// send recv info
const MAX_SEND_SIZE: usize = 32;
const MAX_RECV_SIZE: usize = 64;
const MAX_DOORBELL_SEND_SIZE: usize = 8;
// const MAX_IDLE_RECV_NUM: usize = 1;

const MAX_SIGNAL_PENDINGS: usize = MAX_SEND_SIZE - MAX_DOORBELL_SEND_SIZE;
const WRID_RESERVE_BITS: usize = 8;
