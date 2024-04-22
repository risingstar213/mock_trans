use tokio::sync::mpsc;
use tokio::sync::Mutex as AsyncMutex;

use super::*;

use super::batch_rpc_proc::BatchRpcProc;
use super::super::occ::LockContent;
use super::super::cache_helpers::CacheReadSetItem;
use super::super::cache_helpers::trans_cache_view::TransKey;

impl<'worker> BatchRpcProc<'worker> {
    // work for validate cache
    // pub async fn validate_cache_work(&self, yield_req: YieldReq, cid: u32) {
    //     let trans_key = TransKey::new(&yield_req.yield_meta);

    //     let mut success = yield_req.addi_success;

    //     let buf_count = self.trans_view.get_read_range_num(&trans_key);

    //     for i in 0..buf_count {
    //         let read_buf = self.trans_view.get_read_buf(&trans_key, i, cid).await;

    //         for item in read_buf.iter() {
    //             let meta = self.memdb.local_get_meta(
    //                 item.table_id, 
    //                 item.key
    //             ).unwrap();
    
    //             if meta.lock != 0 || (meta.seq != item.old_seq) {
    //                 success = false;
    //                 break;
    //             }
    //         }
    //     }

    //     self.trans_view.end_read_trans(&trans_key);

    //     let resp_buf = self.scheduler.get_reply_buf();
    //     let reduce_resp = unsafe { (resp_buf as *mut BatchRpcReduceResp).as_mut().unwrap() };
    //     *reduce_resp = BatchRpcReduceResp{
    //         success: success
    //     };

    //     self.scheduler.send_req(
    //         resp_buf, 
    //         occ_rpc_id::VAL_CACHE_RPC, 
    //         std::mem::size_of::<BatchRpcReduceResp>() as _,
    //         yield_req.yield_meta.rpc_cid,
    //         rpc_msg_type::RESP,
    //         yield_req.yield_meta.peer_id, 
    //         yield_req.yield_meta.peer_tid
    //     )
    // }
}

// lazy release
//  atomic visibility
// async lock

// global trans id = { local trans id, node id }
// object version = { local version, node id }