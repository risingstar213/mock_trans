use std::sync::Arc;

use crate::common::random::FastRandom;
use crate::framework::scheduler::AsyncScheduler;
use crate::memstore::memdb::MemDB;
use crate::occ::occ_host::OccHost;

use super::host_worker::SmallBankHostWorker;
use super::super::*;
use super::super::utils::{ random_get_accounts, account_to_part };

impl SmallBankHostWorker {
    // update checking * 2
    pub async fn txn_send_payment(&self, rand_gen: &mut FastRandom, cid: u32) {
        // println!("txn_send_payment");
        let mut txn = OccHost::<SMALL_BANK_MAX_ITEM_SIZE>::new(
            self.part_id, 
            self.tid,
            cid, 
            &self.valuedb, 
            &self.scheduler,
        );

        txn.start();

        let mut accounts = Vec::new();
        random_get_accounts(2, rand_gen, &mut accounts);

        txn.fetch_write::<SmallBankChecking>(
            small_bank_table_id::CHECKING_TABLE_ID,
            account_to_part(accounts[0]) as _,
            accounts[0] as _,
        );

        txn.fetch_write::<SmallBankChecking>(
            small_bank_table_id::CHECKING_TABLE_ID,
            account_to_part(accounts[1]) as _,
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
        let mut txn = OccHost::<SMALL_BANK_MAX_ITEM_SIZE>::new(
            self.part_id, 
            self.tid,
            cid, 
            &self.valuedb, 
            &self.scheduler,
        );

        txn.start();

        let mut accounts = Vec::new();
        random_get_accounts(1, rand_gen, &mut accounts);

        txn.fetch_write::<SmallBankChecking>(
            small_bank_table_id::CHECKING_TABLE_ID,
            account_to_part(accounts[0]) as _,
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
        let mut txn = OccHost::<SMALL_BANK_MAX_ITEM_SIZE>::new(
            self.part_id, 
            self.tid,
            cid, 
            &self.valuedb, 
            &self.scheduler,
        );

        txn.start();

        let mut accounts = Vec::new();
        random_get_accounts(1, rand_gen, &mut accounts);

        txn.read::<SmallBankChecking>(
            small_bank_table_id::CHECKING_TABLE_ID,
            account_to_part(accounts[0]) as _,
            accounts[0] as _
        );
        txn.read::<SmallBankSavings>(
            small_bank_table_id::SAVINGS_TABLE_ID,
            account_to_part(accounts[0]) as _,
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
        let mut txn = OccHost::<SMALL_BANK_MAX_ITEM_SIZE>::new(
            self.part_id, 
            self.tid,
            cid, 
            &self.valuedb, 
            &self.scheduler,
        );

        txn.start();

        let mut accounts = Vec::new();
        random_get_accounts(1, rand_gen, &mut accounts);

        txn.fetch_write::<SmallBankSavings>(
            small_bank_table_id::SAVINGS_TABLE_ID,
            account_to_part(accounts[0]) as _,
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
        let mut txn = OccHost::<SMALL_BANK_MAX_ITEM_SIZE>::new(
            self.part_id, 
            self.tid,
            cid, 
            &self.valuedb, 
            &self.scheduler,
        );

        txn.start();

        let mut accounts = Vec::new();
        random_get_accounts(1, rand_gen, &mut accounts);

        txn.fetch_write::<SmallBankChecking>(
            small_bank_table_id::CHECKING_TABLE_ID,
            account_to_part(accounts[0]) as _,
            accounts[0] as _
        );
        txn.read::<SmallBankSavings>(
            small_bank_table_id::SAVINGS_TABLE_ID,
            account_to_part(accounts[0]) as _,
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
        let mut txn = OccHost::<SMALL_BANK_MAX_ITEM_SIZE>::new(
            self.part_id, 
            self.tid,
            cid, 
            &self.valuedb, 
            &self.scheduler,
        );

        txn.start();

        let mut accounts = Vec::new();
        random_get_accounts(2, rand_gen, &mut accounts);

        txn.fetch_write::<SmallBankSavings>(
            small_bank_table_id::SAVINGS_TABLE_ID,
            account_to_part(accounts[0]) as _,
            accounts[0] as _,
        );

        txn.fetch_write::<SmallBankChecking>(
            small_bank_table_id::CHECKING_TABLE_ID,
            account_to_part(accounts[0]) as _,
            accounts[0] as _,
        );

        txn.fetch_write::<SmallBankChecking>(
            small_bank_table_id::CHECKING_TABLE_ID,
            account_to_part(accounts[1]) as _,
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
}