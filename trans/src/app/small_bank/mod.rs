mod utils;
pub mod workload;
pub mod local_client;

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