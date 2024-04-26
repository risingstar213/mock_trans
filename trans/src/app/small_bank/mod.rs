mod utils;
pub mod workload;
pub mod worker;
pub mod local_client;
pub mod loader;

#[cfg(feature = "doca_deps")]
pub mod dpu_helpers;

use std::sync::Arc;

use crate::framework::scheduler::AsyncScheduler;
use crate::memstore::memdb::MemDB;
use crate::occ::BatchRpcProc;

pub mod small_bank_table_id {
    pub const ACCOUNTS_TABLE_ID: usize = 0;
    pub const SAVINGS_TABLE_ID:  usize = 1;
    pub const CHECKING_TABLE_ID: usize = 2;
}

const SMALL_BANK_MAX_ITEM_SIZE: usize = 64;

#[derive(Clone)]
#[repr(C)]
pub struct SmallBankAccounts {
    a_name: [u8; 64],
}

impl Default for SmallBankAccounts {
    fn default() -> Self {
        Self {
            a_name: [0u8; 64],
        }
    }
}

#[derive(Clone, Default)]
#[repr(C)]
pub struct SmallBankSavings {
    s_balance: f64,
}

#[derive(Clone, Default)]
#[repr(C)]
pub struct SmallBankChecking {
    c_balance: f64,
}

pub enum SmallBankWordLoadId {
    TxnSendPayment,
    TxnDepositChecking,
    TxnBalance,
    TxnTransactSavings,
    TxnWriteCheck,
    TxnAmalgamate,
    TxnExchange,
    TxnExchangeCheck,
}

impl From<usize> for SmallBankWordLoadId {
    fn from(value: usize) -> Self {
        match value {
            0 => SmallBankWordLoadId::TxnSendPayment,
            1 => SmallBankWordLoadId::TxnDepositChecking,
            2 => SmallBankWordLoadId::TxnBalance,
            3 => SmallBankWordLoadId::TxnTransactSavings,
            4 => SmallBankWordLoadId::TxnWriteCheck,
            5 => SmallBankWordLoadId::TxnAmalgamate,
            6 => SmallBankWordLoadId::TxnExchange,
            7 => SmallBankWordLoadId::TxnExchangeCheck,
            _ => panic!("unsupported"),
        }
    }
}

pub struct SmallBankClientReq {
    workload: SmallBankWordLoadId,
}

pub struct SmallBankWorker {
    part_id: u64,
    tid: u32,
    memdb: Arc<MemDB>,
    scheduler: Arc<AsyncScheduler>,
    proc: BatchRpcProc,
}

impl SmallBankWorker {
    pub fn new(part_id: u64, tid: u32, memdb: &Arc<MemDB>, scheduler: &Arc<AsyncScheduler>) -> Self {
        Self {
            part_id: part_id,
            tid: tid, 
            scheduler: scheduler.clone(),
            memdb: memdb.clone(),
            proc: BatchRpcProc::new(tid, memdb, scheduler),
        }
    }
}