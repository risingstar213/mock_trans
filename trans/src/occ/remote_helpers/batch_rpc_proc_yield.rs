use tokio::sync::mpsc;
use tokio::sync::Mutex as AsyncMutex;

use super::*;

use super::batch_rpc_proc::BatchRpcProc;
use super::super::occ::LockContent;

#[cfg(feature = "doca_deps")]
impl<'worker> BatchRpcProc<'worker> {
    // work for validate cache
    pub async fn validate_cache_work(&self, yield_req: YieldReq) {
        
    }
}

// lazy release
//  atomic visibility
// async lock

// global trans id = { local trans id, node id }
// object version = { local version, node id }