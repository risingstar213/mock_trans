#![feature(get_mut_unchecked)]
pub mod common;

pub mod rdma;
pub mod doca_dma;
pub mod doca_comm_chan;

pub mod framework;
pub mod memstore;
pub mod occ;

pub mod app;

#[derive(Debug)]
pub enum TransError {
    TransRdmaError,
    TransDocaError,
    TransSyncError,
}

type TransResult<T> = Result<T, TransError>;

// connection info
const PEERNUMS: u64 = 2;

// mem info
const NPAGES: u64 = 256;
const MAX_PACKET_SIZE: usize = 1024;

// send recv info
const MAX_SEND_SIZE: usize = 128;
const MAX_RECV_SIZE: usize = 64;
const MAX_DOORBELL_SEND_SIZE: usize = 8;
// const MAX_IDLE_RECV_NUM: usize = 1;

const MAX_SIGNAL_PENDINGS: usize = MAX_SEND_SIZE - MAX_DOORBELL_SEND_SIZE;
const WRID_RESERVE_BITS: usize = 8;

// RPCs
const MAX_INFLIGHT_REPLY: usize = 128;
const MAX_INFLIGHT_REQS_PER_ROUTINE: usize = 8;
const MAX_REQ_SIZE: usize = 1024;
const MAX_RESP_SIZE: usize = 2048;

/////////////////// MemStore //////////////////////////
const ROBINHOOD_SIZE:    usize = 24576;
const ROBINHOOD_DIB_MAX: usize = 8;


/////////////////// DOCA DMA //////////////////////////
const DOCA_WORKQ_DEPTH: usize = 8;
const MAX_DMA_BUF_SIZE: usize = 1024;
const MAX_DMA_BUF_REMOTE: usize = 128;
const MAX_DMA_BUF_PER_ROUTINE: usize = 16;

/////////////////// DOCA CONN /////////////////////////
const MAX_CONN_INFO_BUFS: usize = 10;
const MAX_CONN_MSG_SIZE: usize = 2048;

/////////////////// CACHE /////////////////////////////
const MAX_LOCAL_CACHE_BUF_COUNT: usize = 32;

/////////////////// WORKER ////////////////////////////
const MAIN_ROUTINE_ID: u32 = 0;

/////////////////// Small Bank Wokeloads //////////////
pub const SMALL_BANK_NROUTINES: usize = 8;
pub const SMALL_BANK_NTHREADS: usize = 4;
const SMALL_BANK_NPARTITIONS:  usize = 2;
const SMALL_BANK_DEFAULT_NACCOUNTS: usize = 1000;
const SMALL_BANK_DEFAULT_NHOTACCOUTS: usize = 40;
const SMALL_BANK_SCALE: usize = 1;
const SMALL_BANK_TX_HOT: usize = 90;

const SMALL_BANK_MIN_BALANCE: f64 = 10000.0;
const SMALL_BANK_MAX_BALANCE: f64 = 50000.0;

/////////////////// SMALL TPCC ////////////////////////
pub const TPCC_NROUTINES: usize = 16;
pub const TPCC_NTHREADS: usize = 8;
const TPCC_NPARTITIONS:  usize = 2;
const TPCC_REMOTE_THRESHHOOD: usize = 50;
const TPCC_SCALE: usize = 1;