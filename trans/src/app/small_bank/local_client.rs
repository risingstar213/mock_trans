use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::mpsc;

use crate::common::random::FastRandom;

use super::SmallBankClientReq;
use super::SmallBankWordLoadId;

// different 
pub struct SmallBankClient {
    senders: Vec<Arc<Mutex<mpsc::Sender<SmallBankClientReq>>>>,
}

const SMALL_BANK_WORKLOAD_MIX: [usize; 6] = [25, 15, 15, 15, 15, 15];

const fn get_workload_mix_sum() -> usize {
    let mut sum = 0;
    sum += SMALL_BANK_WORKLOAD_MIX[0];
    sum += SMALL_BANK_WORKLOAD_MIX[1];
    sum += SMALL_BANK_WORKLOAD_MIX[2];
    sum += SMALL_BANK_WORKLOAD_MIX[3];
    sum += SMALL_BANK_WORKLOAD_MIX[4];
    sum += SMALL_BANK_WORKLOAD_MIX[5];
    sum
}

impl SmallBankClient {
    fn new() -> Self {
        Self {
            senders: Vec::new(),
        }
    }

    fn add_sender(&mut self, sender: &Arc<Mutex<mpsc::Sender<SmallBankClientReq>>>) {
        self.senders.push(sender.clone());
    }

    async fn send_workload(&self, rand_gen: &mut FastRandom) {
        let num_thread = self.senders.len();
        let tid = rand_gen.next() % num_thread;

        let mut d = rand_gen.next() % get_workload_mix_sum();
        let mut tx_idx = 0;

        for i in 0..SMALL_BANK_WORKLOAD_MIX.len() {
            if i == SMALL_BANK_WORKLOAD_MIX.len() - 1 || d < SMALL_BANK_WORKLOAD_MIX[i] {
                tx_idx = i;
                break;
            }
            d -= SMALL_BANK_WORKLOAD_MIX[i];
        }

        self.senders[tx_idx].lock().unwrap().send(SmallBankClientReq{
            workload: SmallBankWordLoadId::from(tx_idx),
        }).await.unwrap();
    }

    async fn work_loop(&self, rand_seed: usize) {

    }
}