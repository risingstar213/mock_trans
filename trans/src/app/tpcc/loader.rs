use std::sync::Arc;

use crate::memstore::memdb::TableSchema;
use crate::memstore::RobinhoodMemStore;
use crate::common::random::FastRandom;
use crate::memstore::memdb::MemDB;

use super::*;
use super::utils::*;

pub struct TpccLoader {}

impl TpccLoader {
    pub fn new_memdb(part_id: u64) -> Arc<MemDB> {
        let mut memdb = Arc::new(MemDB::new());
        let memstore0 = RobinhoodMemStore::<TpccDistricts>::new();
        let memstore1 = RobinhoodMemStore::<TpccStocks>::new();
        let memstore2 = RobinhoodMemStore::<TpccOrders>::new();

        Arc::get_mut(&mut memdb).unwrap().add_schema(0, TableSchema::default(), memstore0);
        Arc::get_mut(&mut memdb).unwrap().add_schema(1, TableSchema::default(), memstore1);
        Arc::get_mut(&mut memdb).unwrap().add_schema(2, TableSchema::default(), memstore2);

        Self::do_load((23984543 + part_id * 73) as usize, part_id, &memdb);

        memdb
    }

    pub fn do_load(rand_seed: usize, part_id: u64, memdb: &Arc<MemDB>) {
        let w_start = warehouses_start(part_id as _);
        let w_end = warehouses_end(part_id as _);
        for i in w_start..w_end {
            for j in 0..10 {
                let d_id = j + i * 10;
                memdb.local_lock(
                    tpcc_table_id::DISTRICTS_TABLE_ID, 
                    d_id as _, 
                    0
                );

                let dist = TpccDistricts::default();
    
                memdb.local_upd_val_seq(
                    tpcc_table_id::DISTRICTS_TABLE_ID, 
                    d_id as _, 
                    &dist as *const _ as _, 
                    std::mem::size_of::<TpccDistricts>() as _,
                );
            }

            for j in 0..10000 {
                let s_id = make_stock_key(i, j);
                memdb.local_lock(
                    tpcc_table_id::STOCKS_TABLE_ID, 
                    s_id as _, 
                    0
                );

                let stock = TpccStocks::default();

                memdb.local_upd_val_seq(
                    tpcc_table_id::STOCKS_TABLE_ID, 
                    s_id as _, 
                    &stock as *const _ as _, 
                    std::mem::size_of::<TpccStocks>() as _,
                );
            }
        }

        for i in 0..num_orders() {
            if order_id_to_part_id(i) as u64 != part_id {
                continue;
            }

            memdb.local_lock(
                tpcc_table_id::ORDERS_TABLE_ID, 
                i as _, 
                0
            );

            let order = TpccOrders::default();

            memdb.local_upd_val_seq(
                tpcc_table_id::ORDERS_TABLE_ID, 
                i as _, 
                &order as *const _ as _, 
                std::mem::size_of::<TpccOrders>() as _,
            );
        }
    }
}
       