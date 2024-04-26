use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::mpsc;
use tokio::time::sleep;
use std::time::{ Duration, SystemTime };

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
    pub fn new() -> Self {
        Self {
            senders: Vec::new(),
        }
    }

    pub fn add_sender(&mut self, sender: &Arc<Mutex<mpsc::Sender<SmallBankClientReq>>>) {
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

        self.senders[tid].lock().unwrap().send(SmallBankClientReq{
            workload: SmallBankWordLoadId::from(tx_idx),
        }).await.unwrap();
    }

    async fn send_workload_test(&self, rand_gen: &mut FastRandom) {
        let num_thread = self.senders.len();
        let tid = rand_gen.next() % num_thread;

        let d = rand_gen.next() % 5000;

        if d > 0 {
            self.senders[tid].lock().unwrap().send(SmallBankClientReq{
                workload: SmallBankWordLoadId::from(6),
            }).await.unwrap();
        } else {
            self.senders[tid].lock().unwrap().send(SmallBankClientReq{
                workload: SmallBankWordLoadId::from(7),
            }).await.unwrap();
        }
    }

    pub async fn work_loop(&self, rand_seed: usize) {
        let mut count = 0;
        let start_time = SystemTime::now();
        let mut rand_gen = FastRandom::new(rand_seed);
        loop {
            self.send_workload_test(&mut rand_gen).await;
            count += 1;

            if count % 10000 == 0 {
                let now_time = SystemTime::now();
                let duration = now_time.duration_since(start_time).unwrap();
                // println!("{}, {}", count, duration.as_millis());
            }
            // sleep(Duration::from_millis(1)).await;
        }

    }
}