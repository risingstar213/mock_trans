pub mod common;

#[cfg(feature = "doca_deps")]
pub mod doca_dma;

pub mod framework;
pub mod memstore;
pub mod occ;
pub mod rdma;

#[derive(Debug)]
pub enum TransError {
    TransRdmaError,
    TransDocaError,
    TransSyncError,
}

type TransResult<T> = Result<T, TransError>;

// connection info
const PEERNUMS: u64 = 2;
const SERVERS: [&str; 2] = ["10.10.10.6\0", "10.10.10.9\0"];
const PORTS: [&str; 2] = ["7471\0", "7472\0"];

// mem info
const NPAGES: u64 = 64;
const MAX_PACKET_SIZE: usize = 128;

// send recv info
const MAX_SEND_SIZE: usize = 32;
const MAX_RECV_SIZE: usize = 64;
const MAX_DOORBELL_SEND_SIZE: usize = 8;
// const MAX_IDLE_RECV_NUM: usize = 1;

const MAX_SIGNAL_PENDINGS: usize = MAX_SEND_SIZE - MAX_DOORBELL_SEND_SIZE;
const WRID_RESERVE_BITS: usize = 8;

// RPCs
const MAX_INFLIGHT_REPLY: usize = 32;
const MAX_INFLIGHT_REQS_PER_ROUTINE: usize = 32;
const MAX_REQ_SIZE: usize = 128;
const MAX_RESP_SIZE: usize = 1024;

/////////////////// MemStore //////////////////////////
const ROBINHOOD_SIZE:    usize = 2048;
const ROBINHOOD_DIB_MAX: usize = 8;


/////////////////// DOCA DMA //////////////////////////
const DOCA_WORKQ_DEPTH: usize = 8;
const MAX_DMA_BUF_SIZE: usize = 1024;
const MAX_DMA_BUF_REMOTE: usize = 64;
const MAX_DMA_BUF_PER_ROUTINE: usize = 64;

/////////////////// CACHE /////////////////////////////
const MAX_LOCAL_CACHE_BUF_COUNT: usize = 16;