use std::sync::Arc;
use std::collections::HashMap;

struct BatchRpcMeta {

}

struct BatchReqSend {
    peer_id: u64,
    rpc_id:  u64,

}

// req bufs are shared between coroutines, so it doesn't matter.
struct BatchRpcOp {
    
    resp_buf: *mut u8,
}