pub mod rpc;
mod rpc_shared_buffer;
pub mod scheduler;
pub mod worker;

use rpc::*;
pub struct YieldReq {
    yield_meta:   RpcProcessMeta,
    addi_success: bool,
}