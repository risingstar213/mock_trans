pub mod common;
pub mod context;
pub mod rdma;

#[allow(unused)]
#[derive(Debug)]
pub enum TransError {
    TransRdmaError,
    TransDocaError,
    TransSyncError
}

type TransResult<T> = Result<T, TransError>;


static PEERNUMS:u64 = 2;
static SERVERs: [&str; 2] = ["10.10.10.6\0", "10.10.10.9\0"];
static PORTs: [&str; 2] = ["7471\0", "7472\0"];

static NPAGES: u64 = 4;