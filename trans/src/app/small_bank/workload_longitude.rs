use std::sync::Arc;

use crate::common::random::FastRandom;
use crate::framework::scheduler::AsyncScheduler;
use crate::memstore::memdb::MemDB;
use crate::occ::occ_trans_cache::OccTransCache;

use super::utils::accout_to_part_host_longitude;
use super::utils::accout_to_part_hybrid_longitude;
use super::SmallBankHybridLongitudeWorker;
use super::SmallBankHostLongitudeWorker;
use super::small_bank_table_id;
use super::SMALL_BANK_MAX_ITEM_SIZE;
use super::SmallBankAccounts;
use super::SmallBankChecking;
use super::SmallBankSavings;
use super::utils::{ random_get_accounts, account_to_part };

// polymorphic manually

// workload
impl SmallBankHybridLongitudeWorker {
    // update checking * 2
    pub async fn txn_send_payment(&self, rand_gen: &mut FastRandom, cid: u32) {
        // println!("txn_send_payment");
        let mut txn = OccTransCache::<SMALL_BANK_MAX_ITEM_SIZE>::new(
            self.part_id, 
            self.tid,
            cid, 
            &self.memdb, 
            &self.scheduler,
        );

        txn.start();

        let mut accounts = Vec::new();
        random_get_accounts(2, rand_gen, &mut accounts);

        txn.fetch_write::<SmallBankChecking>(
            small_bank_table_id::CHECKING_TABLE_ID,
            accout_to_part_hybrid_longitude(accounts[0], self.part_id as _) as _,
            accounts[0] as _,
        );

        txn.fetch_write::<SmallBankChecking>(
            small_bank_table_id::CHECKING_TABLE_ID,
            accout_to_part_hybrid_longitude(accounts[1], self.part_id as _) as _,
            accounts[1] as _,
        );

        let c0 = txn.get_value::<SmallBankChecking>(true, 0).await.c_balance;
        let c1 = txn.get_value::<SmallBankChecking>(true, 1).await.c_balance;

        let amount = 5.0;
        if c0 < amount {
            txn.set_value(true, 0, &SmallBankChecking{ c_balance: c0 });
            txn.set_value(true, 1, &SmallBankChecking{ c_balance: c1 });
        } else {
            txn.set_value(true, 0, &SmallBankChecking{ c_balance: c0 + amount });
            txn.set_value(true, 1, &SmallBankChecking{ c_balance: c1 - amount });
        }

        txn.commit().await;

        let _ = txn.is_commited();

    }

    // update checking
    pub async fn txn_deposit_checking(&self, rand_gen: &mut FastRandom, cid: u32) {
        // println!("txn_deposit_checking");
        let mut txn = OccTransCache::<SMALL_BANK_MAX_ITEM_SIZE>::new(
            self.part_id, 
            self.tid,
            cid, 
            &self.memdb, 
            &self.scheduler,
        );

        txn.start();

        let mut accounts = Vec::new();
        random_get_accounts(1, rand_gen, &mut accounts);

        txn.fetch_write::<SmallBankChecking>(
            small_bank_table_id::CHECKING_TABLE_ID,
            accout_to_part_hybrid_longitude(accounts[0], self.part_id as _) as _,
            accounts[0] as _,
        );

        let cv = txn.get_value::<SmallBankChecking>(true, 0).await.c_balance;

        let amount = 1.3;

        txn.set_value(true, 0, &SmallBankChecking{ c_balance: cv + amount });
    
        txn.commit().await;

        let _ = txn.is_commited();
    }

    // read checking && saving
    pub async fn txn_balance(&self, rand_gen: &mut FastRandom, cid: u32) {
        // println!("txn_balance");
        let mut txn = OccTransCache::<SMALL_BANK_MAX_ITEM_SIZE>::new(
            self.part_id, 
            self.tid,
            cid, 
            &self.memdb, 
            &self.scheduler,
        );

        txn.start();

        let mut accounts = Vec::new();
        random_get_accounts(1, rand_gen, &mut accounts);

        txn.read::<SmallBankChecking>(
            small_bank_table_id::CHECKING_TABLE_ID,
            accout_to_part_hybrid_longitude(accounts[0], self.part_id as _) as _,
            accounts[0] as _
        );
        txn.read::<SmallBankSavings>(
            small_bank_table_id::SAVINGS_TABLE_ID,
            accout_to_part_hybrid_longitude(accounts[0], self.part_id as _) as _,
            accounts[0] as _
        );

        let cv = txn.get_value::<SmallBankChecking>(false, 0).await.c_balance;
        let sv = txn.get_value::<SmallBankSavings>(false, 1).await.s_balance;

        let res = cv + sv;

        txn.commit().await;

        let _ = txn.is_commited();
    }

    // update saving
    pub async fn txn_transact_savings(&self, rand_gen: &mut FastRandom, cid: u32) {
        // println!("txn_transact_savings");
        let mut txn = OccTransCache::<SMALL_BANK_MAX_ITEM_SIZE>::new(
            self.part_id, 
            self.tid,
            cid, 
            &self.memdb, 
            &self.scheduler,
        );

        txn.start();

        let mut accounts = Vec::new();
        random_get_accounts(1, rand_gen, &mut accounts);

        txn.fetch_write::<SmallBankSavings>(
            small_bank_table_id::SAVINGS_TABLE_ID,
            accout_to_part_hybrid_longitude(accounts[0], self.part_id as _) as _,
            accounts[0] as _,
        );

        let sv = txn.get_value::<SmallBankSavings>(true, 0).await.s_balance;

        let amount = 20.20;

        txn.set_value(true, 0, &SmallBankSavings{ s_balance: sv + amount });
    
        txn.commit().await;

        let _ = txn.is_commited();
    }

    // read checing && saving -> write checking
    pub async fn txn_write_check(&self, rand_gen: &mut FastRandom, cid: u32) {
        // println!("txn_write_check");
        let mut txn = OccTransCache::<SMALL_BANK_MAX_ITEM_SIZE>::new(
            self.part_id, 
            self.tid,
            cid, 
            &self.memdb, 
            &self.scheduler,
        );

        txn.start();

        let mut accounts = Vec::new();
        random_get_accounts(1, rand_gen, &mut accounts);

        txn.fetch_write::<SmallBankChecking>(
            small_bank_table_id::CHECKING_TABLE_ID,
            accout_to_part_hybrid_longitude(accounts[0], self.part_id as _) as _,
            accounts[0] as _
        );
        txn.read::<SmallBankSavings>(
            small_bank_table_id::SAVINGS_TABLE_ID,
            accout_to_part_hybrid_longitude(accounts[0], self.part_id as _) as _,
            accounts[0] as _
        );

        let cv = txn.get_value::<SmallBankChecking>(true, 0).await.c_balance;
        let sv = txn.get_value::<SmallBankSavings>(false, 0).await.s_balance;

        let total = cv + sv;

        let amount = 5.0;

        if total < amount {
            txn.set_value(true, 0, &SmallBankChecking{ c_balance: cv - amount + 1.0 });
        } else {
            txn.set_value(true, 0, &SmallBankChecking{ c_balance: cv - amount });
        }

        txn.commit().await;

        let _ = txn.is_commited();

    }

    // read checing && saving -> write checking
    pub async fn txn_amalgamate(&self, rand_gen: &mut FastRandom, cid: u32) {
        // println!("txn_amalgamate");
        let mut txn = OccTransCache::<SMALL_BANK_MAX_ITEM_SIZE>::new(
            self.part_id, 
            self.tid,
            cid, 
            &self.memdb, 
            &self.scheduler,
        );

        txn.start();

        let mut accounts = Vec::new();
        random_get_accounts(2, rand_gen, &mut accounts);

        txn.fetch_write::<SmallBankSavings>(
            small_bank_table_id::SAVINGS_TABLE_ID,
            accout_to_part_hybrid_longitude(accounts[0], self.part_id as _) as _,
            accounts[0] as _,
        );

        txn.fetch_write::<SmallBankChecking>(
            small_bank_table_id::CHECKING_TABLE_ID,
            accout_to_part_hybrid_longitude(accounts[0], self.part_id as _) as _,
            accounts[0] as _,
        );

        txn.fetch_write::<SmallBankChecking>(
            small_bank_table_id::CHECKING_TABLE_ID,
            accout_to_part_hybrid_longitude(accounts[1], self.part_id as _) as _,
            accounts[1] as _,
        );

        let s0 = txn.get_value::<SmallBankSavings>(true, 0).await.s_balance;
        let c0 = txn.get_value::<SmallBankChecking>(true, 1).await.c_balance;
        let c1 = txn.get_value::<SmallBankChecking>(true, 2).await.c_balance;


        txn.set_value(true, 0, &SmallBankSavings{ s_balance: 0.0 });
        txn.set_value(true, 1, &SmallBankChecking{ c_balance: 0.0 });
        txn.set_value(true, 2, &SmallBankChecking{ c_balance: s0 + c0 + c1 });
    
        txn.commit().await;

        let _ = txn.is_commited();
    }

    pub async fn txn_exchange(&self, rand_gen: &mut FastRandom, cid: u32) {
        let lid = rand_gen.next() % 4;
        let mut rid = lid;

        while lid == rid {
            rid = rand_gen.next() % 4;
        }

        let mut txn = OccTransCache::<SMALL_BANK_MAX_ITEM_SIZE>::new(
            self.part_id, 
            self.tid,
            cid, 
            &self.memdb, 
            &self.scheduler,
        );

        txn.start();

        txn.fetch_write::<SmallBankChecking>(
            small_bank_table_id::CHECKING_TABLE_ID,
            accout_to_part_hybrid_longitude(lid, self.part_id as _) as _,
            lid as _,
        );

        txn.fetch_write::<SmallBankChecking>(
            small_bank_table_id::CHECKING_TABLE_ID,
            accout_to_part_hybrid_longitude(rid, self.part_id as _) as _,
            rid as _,
        );

        let lvalue = txn.get_value::<SmallBankChecking>(true, 0).await.c_balance;
        let rvalue = txn.get_value::<SmallBankChecking>(true, 1).await.c_balance;
    
        txn.set_value(true, 0, &SmallBankChecking{ c_balance: rvalue });
        txn.set_value(true, 1, &SmallBankChecking{ c_balance: lvalue });

        txn.commit().await;

        let _ = txn.is_commited();
    }

    pub async fn txn_exchange_check(&self, rand_gen: &mut FastRandom, cid: u32) {
        let mut txn = OccTransCache::<SMALL_BANK_MAX_ITEM_SIZE>::new(
            self.part_id, 
            self.tid,
            cid, 
            &self.memdb, 
            &self.scheduler,
        );

        txn.start();

        let start_time = std::time::SystemTime::now();

        for i in 0..4 {
            txn.read::<SmallBankChecking>(small_bank_table_id::CHECKING_TABLE_ID,
                accout_to_part_hybrid_longitude(i, self.part_id as _) as _,
                i as _,
            );
        }
        let mut values = Vec::new();

        for i in 0..4 {
            let value = txn.get_value::<SmallBankChecking>(false, i).await.c_balance;
            values.push(value);
        }

        txn.commit().await;

        let end_time = std::time::SystemTime::now();


        if txn.is_commited() {
            println!("check: commit {}", end_time.duration_since(start_time).unwrap().as_micros());
        } else {
            println!("check: abort {}", end_time.duration_since(start_time).unwrap().as_micros());
        }
    }
}


// workload
impl SmallBankHostLongitudeWorker {
    // update checking * 2
    pub async fn txn_send_payment(&self, rand_gen: &mut FastRandom, cid: u32) {
        // println!("txn_send_payment");
        let mut txn = OccTransCache::<SMALL_BANK_MAX_ITEM_SIZE>::new(
            self.part_id, 
            self.tid,
            cid, 
            &self.memdb, 
            &self.scheduler,
        );

        txn.start();

        let mut accounts = Vec::new();
        random_get_accounts(2, rand_gen, &mut accounts);

        txn.fetch_write::<SmallBankChecking>(
            small_bank_table_id::CHECKING_TABLE_ID,
            accout_to_part_host_longitude(accounts[0], self.part_id as _) as _,
            accounts[0] as _,
        );

        txn.fetch_write::<SmallBankChecking>(
            small_bank_table_id::CHECKING_TABLE_ID,
            accout_to_part_host_longitude(accounts[1], self.part_id as _) as _,
            accounts[1] as _,
        );

        let c0 = txn.get_value::<SmallBankChecking>(true, 0).await.c_balance;
        let c1 = txn.get_value::<SmallBankChecking>(true, 1).await.c_balance;

        let amount = 5.0;
        if c0 < amount {
            txn.set_value(true, 0, &SmallBankChecking{ c_balance: c0 });
            txn.set_value(true, 1, &SmallBankChecking{ c_balance: c1 });
        } else {
            txn.set_value(true, 0, &SmallBankChecking{ c_balance: c0 + amount });
            txn.set_value(true, 1, &SmallBankChecking{ c_balance: c1 - amount });
        }

        txn.commit().await;

        let _ = txn.is_commited();

    }

    // update checking
    pub async fn txn_deposit_checking(&self, rand_gen: &mut FastRandom, cid: u32) {
        // println!("txn_deposit_checking");
        let mut txn = OccTransCache::<SMALL_BANK_MAX_ITEM_SIZE>::new(
            self.part_id, 
            self.tid,
            cid, 
            &self.memdb, 
            &self.scheduler,
        );

        txn.start();

        let mut accounts = Vec::new();
        random_get_accounts(1, rand_gen, &mut accounts);

        txn.fetch_write::<SmallBankChecking>(
            small_bank_table_id::CHECKING_TABLE_ID,
            accout_to_part_host_longitude(accounts[0], self.part_id as _) as _,
            accounts[0] as _,
        );

        let cv = txn.get_value::<SmallBankChecking>(true, 0).await.c_balance;

        let amount = 1.3;

        txn.set_value(true, 0, &SmallBankChecking{ c_balance: cv + amount });
    
        txn.commit().await;

        let _ = txn.is_commited();
    }

    // read checking && saving
    pub async fn txn_balance(&self, rand_gen: &mut FastRandom, cid: u32) {
        // println!("txn_balance");
        let mut txn = OccTransCache::<SMALL_BANK_MAX_ITEM_SIZE>::new(
            self.part_id, 
            self.tid,
            cid, 
            &self.memdb, 
            &self.scheduler,
        );

        txn.start();

        let mut accounts = Vec::new();
        random_get_accounts(1, rand_gen, &mut accounts);

        txn.read::<SmallBankChecking>(
            small_bank_table_id::CHECKING_TABLE_ID,
            accout_to_part_host_longitude(accounts[0], self.part_id as _) as _,
            accounts[0] as _
        );
        txn.read::<SmallBankSavings>(
            small_bank_table_id::SAVINGS_TABLE_ID,
            accout_to_part_host_longitude(accounts[0], self.part_id as _) as _,
            accounts[0] as _
        );

        let cv = txn.get_value::<SmallBankChecking>(false, 0).await.c_balance;
        let sv = txn.get_value::<SmallBankSavings>(false, 1).await.s_balance;

        let res = cv + sv;

        txn.commit().await;

        let _ = txn.is_commited();
    }

    // update saving
    pub async fn txn_transact_savings(&self, rand_gen: &mut FastRandom, cid: u32) {
        // println!("txn_transact_savings");
        let mut txn = OccTransCache::<SMALL_BANK_MAX_ITEM_SIZE>::new(
            self.part_id, 
            self.tid,
            cid, 
            &self.memdb, 
            &self.scheduler,
        );

        txn.start();

        let mut accounts = Vec::new();
        random_get_accounts(1, rand_gen, &mut accounts);

        txn.fetch_write::<SmallBankSavings>(
            small_bank_table_id::SAVINGS_TABLE_ID,
            accout_to_part_host_longitude(accounts[0], self.part_id as _) as _,
            accounts[0] as _,
        );

        let sv = txn.get_value::<SmallBankSavings>(true, 0).await.s_balance;

        let amount = 20.20;

        txn.set_value(true, 0, &SmallBankSavings{ s_balance: sv + amount });
    
        txn.commit().await;

        let _ = txn.is_commited();
    }

    // read checing && saving -> write checking
    pub async fn txn_write_check(&self, rand_gen: &mut FastRandom, cid: u32) {
        // println!("txn_write_check");
        let mut txn = OccTransCache::<SMALL_BANK_MAX_ITEM_SIZE>::new(
            self.part_id, 
            self.tid,
            cid, 
            &self.memdb, 
            &self.scheduler,
        );

        txn.start();

        let mut accounts = Vec::new();
        random_get_accounts(1, rand_gen, &mut accounts);

        txn.fetch_write::<SmallBankChecking>(
            small_bank_table_id::CHECKING_TABLE_ID,
            accout_to_part_host_longitude(accounts[0], self.part_id as _) as _,
            accounts[0] as _
        );
        txn.read::<SmallBankSavings>(
            small_bank_table_id::SAVINGS_TABLE_ID,
            accout_to_part_host_longitude(accounts[0], self.part_id as _) as _,
            accounts[0] as _
        );

        let cv = txn.get_value::<SmallBankChecking>(true, 0).await.c_balance;
        let sv = txn.get_value::<SmallBankSavings>(false, 0).await.s_balance;

        let total = cv + sv;

        let amount = 5.0;

        if total < amount {
            txn.set_value(true, 0, &SmallBankChecking{ c_balance: cv - amount + 1.0 });
        } else {
            txn.set_value(true, 0, &SmallBankChecking{ c_balance: cv - amount });
        }

        txn.commit().await;

        let _ = txn.is_commited();

    }

    // read checing && saving -> write checking
    pub async fn txn_amalgamate(&self, rand_gen: &mut FastRandom, cid: u32) {
        // println!("txn_amalgamate");
        let mut txn = OccTransCache::<SMALL_BANK_MAX_ITEM_SIZE>::new(
            self.part_id, 
            self.tid,
            cid, 
            &self.memdb, 
            &self.scheduler,
        );

        txn.start();

        let mut accounts = Vec::new();
        random_get_accounts(2, rand_gen, &mut accounts);

        txn.fetch_write::<SmallBankSavings>(
            small_bank_table_id::SAVINGS_TABLE_ID,
            accout_to_part_host_longitude(accounts[0], self.part_id as _) as _,
            accounts[0] as _,
        );

        txn.fetch_write::<SmallBankChecking>(
            small_bank_table_id::CHECKING_TABLE_ID,
            accout_to_part_host_longitude(accounts[0], self.part_id as _) as _,
            accounts[0] as _,
        );

        txn.fetch_write::<SmallBankChecking>(
            small_bank_table_id::CHECKING_TABLE_ID,
            accout_to_part_host_longitude(accounts[1], self.part_id as _) as _,
            accounts[1] as _,
        );

        let s0 = txn.get_value::<SmallBankSavings>(true, 0).await.s_balance;
        let c0 = txn.get_value::<SmallBankChecking>(true, 1).await.c_balance;
        let c1 = txn.get_value::<SmallBankChecking>(true, 2).await.c_balance;


        txn.set_value(true, 0, &SmallBankSavings{ s_balance: 0.0 });
        txn.set_value(true, 1, &SmallBankChecking{ c_balance: 0.0 });
        txn.set_value(true, 2, &SmallBankChecking{ c_balance: s0 + c0 + c1 });
    
        txn.commit().await;

        let _ = txn.is_commited();
    }

    pub async fn txn_exchange(&self, rand_gen: &mut FastRandom, cid: u32) {
        let lid = rand_gen.next() % 4;
        let mut rid = lid;

        while lid == rid {
            rid = rand_gen.next() % 4;
        }

        let mut txn = OccTransCache::<SMALL_BANK_MAX_ITEM_SIZE>::new(
            self.part_id, 
            self.tid,
            cid, 
            &self.memdb, 
            &self.scheduler,
        );

        txn.start();

        txn.fetch_write::<SmallBankChecking>(
            small_bank_table_id::CHECKING_TABLE_ID,
            accout_to_part_host_longitude(lid, self.part_id as _) as _,
            lid as _,
        );

        txn.fetch_write::<SmallBankChecking>(
            small_bank_table_id::CHECKING_TABLE_ID,
            accout_to_part_host_longitude(rid, self.part_id as _) as _,
            rid as _,
        );

        let lvalue = txn.get_value::<SmallBankChecking>(true, 0).await.c_balance;
        let rvalue = txn.get_value::<SmallBankChecking>(true, 1).await.c_balance;
    
        txn.set_value(true, 0, &SmallBankChecking{ c_balance: rvalue });
        txn.set_value(true, 1, &SmallBankChecking{ c_balance: lvalue });

        txn.commit().await;

        let _ = txn.is_commited();
    }

    pub async fn txn_exchange_check(&self, rand_gen: &mut FastRandom, cid: u32) {
        let mut txn = OccTransCache::<SMALL_BANK_MAX_ITEM_SIZE>::new(
            self.part_id, 
            self.tid,
            cid, 
            &self.memdb, 
            &self.scheduler,
        );

        txn.start();

        let start_time = std::time::SystemTime::now();

        for i in 0..4 {
            txn.read::<SmallBankChecking>(small_bank_table_id::CHECKING_TABLE_ID,
                accout_to_part_host_longitude(i, self.part_id as _) as _,
                i as _,
            );
        }
        let mut values = Vec::new();

        for i in 0..4 {
            let value = txn.get_value::<SmallBankChecking>(false, i).await.c_balance;
            values.push(value);
        }

        txn.commit().await;

        let end_time = std::time::SystemTime::now();


        if txn.is_commited() {
            println!("check: commit {}", end_time.duration_since(start_time).unwrap().as_micros());
        } else {
            println!("check: abort {}", end_time.duration_since(start_time).unwrap().as_micros());
        }
    }
}