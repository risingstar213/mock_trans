use std::sync::Arc;

use super::scheduler::AsyncScheduler;

pub trait AsyncWorker
where
    Self: Send + Sync,
{
    fn get_scheduler(&self) -> &AsyncScheduler;

    fn has_stopped(&self) -> bool;

    // routine 0
    fn main_routine(self: Arc<Self>) -> impl std::future::Future<Output = ()> + Send {
        async move {
            let scheduler = self.get_scheduler();
            loop {
                if self.has_stopped() {
                    break;
                }

                scheduler.poll_recvs();
                scheduler.poll_sends();

                scheduler.yield_now(0).await;
            }
        }
    }
}
