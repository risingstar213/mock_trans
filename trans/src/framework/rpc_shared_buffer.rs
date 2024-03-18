
// reused buffer for rpc
pub struct RpcBufAllocator {
    req_buf_pool: Vec<*mut u8>,
    
}