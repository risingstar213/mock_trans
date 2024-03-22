use std::sync::Arc;

use tokio;

use super::scheduler::AsyncScheduler;
use super::rpc::AsyncRpc;

pub trait AsyncWorker<'a>
    where Self : Send + Sync
{
    fn get_scheduler(&self) -> &AsyncScheduler<'a>;

    fn has_stopped(&self) -> bool;

    // routine 0
    fn main_routine(self: &Arc<Self>) -> impl std::future::Future<Output = ()> + Send {
        let self_clone = self.clone();
        async move {
            let scheduler = self_clone.get_scheduler();
            loop {
                if self_clone.has_stopped() {
                    break;
                }

                scheduler.poll_recvs();
                scheduler.poll_sends();

                scheduler.yield_now(0).await;    
            }
        }
    }
}