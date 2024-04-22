use std::sync::Arc;

use crate::memstore::memdb::TableSchema;
use crate::memstore::RobinhoodMemStore;
use crate::SMALL_BANK_MIN_BALANCE;
use crate::SMALL_BANK_MAX_BALANCE;
use crate::common::random::FastRandom;
use crate::memstore::memdb::MemDB;

use super::utils::{ accounts_num, account_to_part };
use super::SmallBankAccounts;
use super::SmallBankSavings;
use super::SmallBankChecking;
use super::small_bank_table_id;

#[inline]
fn random_account_number(rand_gen: &mut FastRandom) -> f64 {
    rand_gen.next_uniform() * (SMALL_BANK_MAX_BALANCE - SMALL_BANK_MIN_BALANCE) + SMALL_BANK_MAX_BALANCE
}

pub struct SmallBankLoader {}

impl SmallBankLoader {
    pub fn new_memdb(part_id: u64) -> Arc<MemDB> {
        let mut memdb = Arc::new(MemDB::new());
        let memstore0 = RobinhoodMemStore::<SmallBankAccounts>::new();
        let memstore1 = RobinhoodMemStore::<SmallBankSavings>::new();
        let memstore2 = RobinhoodMemStore::<SmallBankChecking>::new();

        Arc::get_mut(&mut memdb).unwrap().add_schema(0, TableSchema::default(), memstore0);
        Arc::get_mut(&mut memdb).unwrap().add_schema(1, TableSchema::default(), memstore1);
        Arc::get_mut(&mut memdb).unwrap().add_schema(2, TableSchema::default(), memstore2);

        Self::do_load((23984543 + part_id * 73) as usize, part_id, &memdb);

        memdb
    }

    pub fn do_load(rand_seed: usize, part_id: u64, memdb: &Arc<MemDB>) {
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