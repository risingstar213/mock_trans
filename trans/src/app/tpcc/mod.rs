pub mod utils;
pub mod workload;
pub mod loader;
pub mod worker;
pub mod local_client;

#[cfg(feature = "doca_deps")]
pub mod dpu_helpers;

use std::sync::Arc;

use crate::{framework::scheduler::AsyncScheduler, memstore::memdb::MemDB, occ::BatchRpcProc};

pub mod tpcc_table_id {
    pub const DISTRICTS_TABLE_ID:  usize = 0;
    pub const STOCKS_TABLE_ID:     usize = 1;
    pub const ORDERS_TABLE_ID:     usize = 2;
    pub const WAREHOUSES_TABLE_ID: usize = 3;
    pub const CUSTORMERS_TABLE_ID: usize = 4;
    pub const ITEMS_TABLE_ID:      usize = 5;
}

const TPCC_ITEM_SIZE: usize = 128;

// shared
#[derive(Clone)]
#[repr(C)]
pub struct TpccWarehouses {
    // need
    w_tax:  f64,
    //
    w_paddings: [u8; 90],
}

impl Default for TpccWarehouses {
    fn default() -> Self {
        unsafe{ std::mem::zeroed() }
    }
}

// shared
#[derive(Clone)]
#[repr(C)]
pub struct TpccDistricts {
    // need
    d_next_o_id: u64,
    d_tax: f64,
    // 
    d_paddings: [u8; 90],
}

impl Default for TpccDistricts {
    fn default() -> Self {
        unsafe{ std::mem::zeroed() }
    }
}

// local
#[derive(Clone)]
#[repr(C)]
pub struct TpccCustormers {
    // need
    c_discount: f64,
    c_balance:  f64,
    //
    c_padings: [u8; 100],
}

impl Default for TpccCustormers {
    fn default() -> Self {
        unsafe{ std::mem::zeroed() }
    }
}


#[derive(Clone)]
#[repr(C)]
pub struct TpccStocks {
    // need
    s_quantity: u64,
    s_ytd: u64,
    s_order_cnt: u64,
    s_remote_cnt: u64,
}

impl Default for TpccStocks {
    fn default() -> Self {
        unsafe{ std::mem::zeroed() }
    }
}

// only read
#[derive(Clone)]
#[repr(C)]
pub struct TpccItems {
    // need
    i_price: f64,
    i_paddings:  [u8; 74],
}

impl Default for TpccItems {
    fn default() -> Self {
        unsafe{ std::mem::zeroed() }
    }
}

#[derive(Clone)]
#[repr(C)]
pub struct TpccOrders {
    o_c_id:u64,
    o_carrier_id: u64,
    o_ol_cnt: u32,
    o_all_local: u32,
    o_entry_d: u64,
}

impl Default for TpccOrders {
    fn default() -> Self {
        unsafe{ std::mem::zeroed() }
    }
}

pub enum TpccWorkLoadId {
    TxnNewOrder,
}

impl From<usize> for TpccWorkLoadId {
    fn from(value: usize) -> Self {
        match value {
            0 => TpccWorkLoadId::TxnNewOrder,
            _ => panic!("unsupported"),
        }
    }
}

pub struct TpccClientReq {
    workload: TpccWorkLoadId,
}

pub struct TpccWorker {
    part_id: u64,
    tid: u32,
    memdb: Arc<MemDB>,
    scheduler: Arc<AsyncScheduler>,
    proc: BatchRpcProc
}

impl TpccWorker {
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

pub struct TpccHybridWorker {
    part_id: u64,
    tid: u32,
    memdb: Arc<MemDB>,
    scheduler: Arc<AsyncScheduler>,
    proc: BatchRpcProc
}

impl TpccHybridWorker {
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