use std::collections::HashSet;
use std::hash::Hash;
use std::hash::RandomState;
use std::hash::{BuildHasher, Hasher};
use lazy_static::lazy_static;

use crate::common::random::FastRandom;
use crate::SMALL_BANK_NROUTINES;
use crate::SMALL_BANK_NTHREADS;
use crate::SMALL_BANK_NPARTITIONS;
use crate::SMALL_BANK_DEFAULT_NACCOUNTS;
use crate::SMALL_BANK_DEFAULT_NHOTACCOUTS;
use crate::SMALL_BANK_SCALE;
use crate::SMALL_BANK_TX_HOT;

const fn scale_factor() -> usize {
    return SMALL_BANK_SCALE * SMALL_BANK_NTHREADS;
}

const fn accounts_num() -> usize {
    return SMALL_BANK_DEFAULT_NACCOUNTS * SMALL_BANK_NPARTITIONS * scale_factor();
}

const fn hot_accounts_num() -> usize {
    return SMALL_BANK_DEFAULT_NHOTACCOUTS * SMALL_BANK_NPARTITIONS * scale_factor();
}

#[inline]
fn get_account(random_gen: &mut FastRandom) -> usize {
    if random_gen.next() % 100 < SMALL_BANK_TX_HOT {
        return random_gen.next() % hot_accounts_num();
    } else {
        return random_gen.next() % accounts_num();
    }
}

pub fn random_get_accounts(num: usize, rand_gen: &mut FastRandom, accounts: &mut Vec<usize>) {
    let mut temp_set = HashSet::new();

    for _ in 0..num {
        loop {
            let account = get_account(rand_gen);
            if !temp_set.contains(&account) {
                temp_set.insert(account);
                accounts.push(account);
                break;
            }
        }
    }
}

lazy_static! {
    static ref HASH_BUILDER: RandomState = RandomState::new();
}

pub fn account_to_part(account: usize) -> usize {
    let mut hasher = HASH_BUILDER.build_hasher();
    account.hash(&mut hasher);

    hasher.finish() as usize % SMALL_BANK_NPARTITIONS
}