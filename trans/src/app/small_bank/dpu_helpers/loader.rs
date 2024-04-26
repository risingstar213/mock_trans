use std::marker::PhantomData;
use std::sync::Arc;

use crate::memstore::memdb::TableSchema;
use crate::memstore::{ RobinhoodMemStore, RobinhoodValueStore };
use crate::SMALL_BANK_MIN_BALANCE;
use crate::SMALL_BANK_MAX_BALANCE;
use crate::common::random::FastRandom;
use crate::memstore::memdb::{ MemDB, ValueDB };

use super::super::utils::{ accounts_num, account_to_part };
use super::super::*;

#[inline]
fn random_account_number(rand_gen: &mut FastRandom) -> f64 {
    rand_gen.next_uniform() * (SMALL_BANK_MAX_BALANCE - SMALL_BANK_MIN_BALANCE) + SMALL_BANK_MAX_BALANCE
}

pub struct SmallBankDpuLoader {}

impl SmallBankDpuLoader {
    pub fn new_hostdb(part_id: u64) -> Arc<ValueDB> {
        let mut valuedb = Arc::new(ValueDB::new());
        let valuestore0 = RobinhoodValueStore::<SmallBankAccounts>::new();
        let valuestore1 = RobinhoodValueStore::<SmallBankSavings>::new();
        let valuestore2 = RobinhoodValueStore::<SmallBankChecking>::new();


        Arc::get_mut(&mut valuedb).unwrap().add_schema(0, TableSchema::default(), valuestore0);
        Arc::get_mut(&mut valuedb).unwrap().add_schema(1, TableSchema::default(), valuestore1);
        Arc::get_mut(&mut valuedb).unwrap().add_schema(2, TableSchema::default(), valuestore2);

        Self::hostdb_do_load((23984543 + part_id * 73) as usize, part_id, &valuedb);

        valuedb
    }

    pub fn hostdb_do_load(rand_seed: usize, part_id: u64, valuedb: &Arc<ValueDB>) {
        let mut rand_gen = FastRandom::new(rand_seed);

        for account in 0..accounts_num() {
            if account_to_part(account) != part_id as usize {
                continue;
            }

            let a_new = SmallBankAccounts::default();
            let s_new = SmallBankSavings { s_balance: random_account_number(&mut rand_gen) };
            let c_new = SmallBankChecking{ c_balance: random_account_number(&mut rand_gen) };

            valuedb.local_put_value(
                small_bank_table_id::ACCOUNTS_TABLE_ID,
                account as _, 
                &a_new as *const _ as _, 
                std::mem::size_of::<SmallBankAccounts>() as _,
            );

            valuedb.local_put_value(
                small_bank_table_id::SAVINGS_TABLE_ID, 
                account as _, 
                &s_new as *const _ as _,
                std::mem::size_of::<SmallBankSavings>() as _,
            );

            valuedb.local_put_value(
                small_bank_table_id::CHECKING_TABLE_ID, 
                account as _, 
                &c_new as *const _ as _,
                std::mem::size_of::<SmallBankChecking>() as _,
            );
        }
    }

    pub fn new_dpudb(part_id: u64) -> Arc<MemDB> {
        let mut memdb = Arc::new(MemDB::new());
        let memstore0 = RobinhoodMemStore::<PhantomData<usize>>::new();
        let memstore1 = RobinhoodMemStore::<PhantomData<usize>>::new();
        let memstore2 = RobinhoodMemStore::<PhantomData<usize>>::new();

        Arc::get_mut(&mut memdb).unwrap().add_schema(0, TableSchema::default(), memstore0);
        Arc::get_mut(&mut memdb).unwrap().add_schema(1, TableSchema::default(), memstore1);
        Arc::get_mut(&mut memdb).unwrap().add_schema(2, TableSchema::default(), memstore2);

        Self::dpudb_do_load((23984543 + part_id * 73) as usize, part_id, &memdb);

        memdb
    }

    pub fn dpudb_do_load(rand_seed: usize, part_id: u64, memdb: &Arc<MemDB>) {
        let mut rand_gen = FastRandom::new(rand_seed);

        for account in 0..accounts_num() {
            if account_to_part(account) != part_id as usize {
                continue;
            }

            let a_new = SmallBankAccounts::default();
            let s_new = SmallBankSavings { s_balance: random_account_number(&mut rand_gen) };
            let c_new = SmallBankChecking{ c_balance: random_account_number(&mut rand_gen) };

            memdb.local_lock(
                small_bank_table_id::ACCOUNTS_TABLE_ID, 
                account as _, 
                0
            );

            memdb.local_upd_val_seq(
                small_bank_table_id::ACCOUNTS_TABLE_ID,
                account as _, 
                &a_new as *const _ as _, 
                std::mem::size_of::<SmallBankAccounts>() as _,
            );

            memdb.local_lock(
                small_bank_table_id::SAVINGS_TABLE_ID, 
                account as _, 
                0
            );

            memdb.local_upd_val_seq(
                small_bank_table_id::SAVINGS_TABLE_ID, 
                account as _, 
                &s_new as *const _ as _,
                std::mem::size_of::<SmallBankSavings>() as _,
            );

            memdb.local_lock(
                small_bank_table_id::CHECKING_TABLE_ID, 
                account as _, 
                0
            );

            memdb.local_upd_val_seq(
                small_bank_table_id::CHECKING_TABLE_ID, 
                account as _, 
                &c_new as *const _ as _,
                std::mem::size_of::<SmallBankChecking>() as _,
            );
        }
    }
}