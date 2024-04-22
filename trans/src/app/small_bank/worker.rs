use std::future;
use std::sync::Arc;
use libc::rand;
use tokio::sync::mpsc;
use tokio::sync::Mutex as AsyncMutex;

use crate::SMALL_BANK_NROUTINES;
use crate::common::random::FastRandom;
use crate::framework::worker::AsyncWorker;

use super::SmallBankWorker;
use super::SmallBankClientReq;
use super::SmallBankWordLoadId;

impl<'worker> AsyncWorker for SmallBankWorker<'worker> {
    fn get_scheduler(&self) -> &crate::framework::scheduler::AsyncScheduler {
        &self.scheduler
    }

    fn has_stopped(&self) -> bool {
        false
    }
}

impl<'worker> SmallBankWorker<'worker> {
    async fn work_routine(&self, cid: u32, rand_seed: usize, conn: Arc<AsyncMutex<mpsc::Receiver<SmallBankClientReq>>>) {
        let mut rand_gen = FastRandom::new(rand_seed);
        
        loop {
            let mut receiver = conn.lock().await;
            let req = receiver.recv().await.unwrap();
            drop(receiver);

            match req.workload {
                SmallBankWordLoadId::TxnSendPayment => {
                    self.txn_send_payment(&mut rand_gen, cid).await;
                }
                SmallBankWordLoadId::TxnDepositChecking => {
                    self.txn_deposit_checking(&mut rand_gen, cid).await;
                }
                SmallBankWordLoadId::TxnBalance => {
                    self.txn_balance(&mut rand_gen, cid).await;
                }
                SmallBankWordLoadId::TxnTransactSavings => {
                    self.txn_transact_savings(&mut rand_gen, cid).await;
                }
                SmallBankWordLoadId::TxnWriteCheck => {
                    self.txn_write_check(&mut rand_gen, cid).await;
                }
                SmallBankWordLoadId::TxnAmalgamate => {
                    self.txn_amalgamate(&mut rand_gen, cid).await;
                }
            }
        }
    }
}

async fn run_workers<'worker>(worker: &Arc<SmallBankWorker<'worker>>, rand_seed: usize, conn: &Arc<AsyncMutex<mpsc::Receiver<SmallBankClientReq>>>) {
    // let mut futures = Vec::new();
    let mut rand_gen = FastRandom::new(rand_seed);
    
    for i in 1..SMALL_BANK_NROUTINES {
        // let worker_clone = worker.clone();
        // let seed = rand_gen.next();
        // let conn_clone = conn.clone();

        // futures.push(tokio::spawn(async move {
        //     worker_clone.work_routine(i as _, seed, conn_clone).await;
        // }));
    }

    // let worker_clone = worker.clone();
    // tokio::spawn(async move {
    //     worker_clone.main_routine().await
    // }).await.unwrap();

    // for f in futures {
    //     tokio::join!(f).0.unwrap();
    // }
}