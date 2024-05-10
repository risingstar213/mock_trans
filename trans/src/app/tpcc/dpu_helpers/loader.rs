use std::marker::PhantomData;
use std::sync::Arc;

use crate::memstore::memdb::TableSchema;
use crate::memstore::{ RobinhoodMemStore, RobinhoodValueStore };
use crate::common::random::FastRandom;
use crate::memstore::memdb::{ MemDB, ValueDB };

use super::super::*;
use super::super::utils::*;

pub struct TpccDpuLoader {}

impl TpccDpuLoader {
    pub fn new_hostdb(part_id: u64) -> Arc<ValueDB> {
        let mut valuedb = Arc::new(ValueDB::new());
        let valuestore0 = RobinhoodValueStore::<TpccDistricts>::new();
        let valuestore1 = RobinhoodValueStore::<TpccStocks>::new();
        let valuestore2 = RobinhoodValueStore::<TpccOrders>::new();


        Arc::get_mut(&mut valuedb).unwrap().add_schema(0, TableSchema::default(), valuestore0);
        Arc::get_mut(&mut valuedb).unwrap().add_schema(1, TableSchema::default(), valuestore1);
        Arc::get_mut(&mut valuedb).unwrap().add_schema(2, TableSchema::default(), valuestore2);

        Self::hostdb_do_load((23984543 + part_id * 73) as usize, part_id, &valuedb);

        valuedb
    }

    pub fn hostdb_do_load(rand_seed: usize, part_id: u64, valuedb: &Arc<ValueDB>) {
        let w_start = warehouses_start(part_id as _);
        let w_end = warehouses_end(part_id as _);

        for i in w_start..w_end {
            for j in 0..10 {
                let d_id = j + i * 10;
                let dist = TpccDistricts::default();

                valuedb.local_put_value(
                    tpcc_table_id::DISTRICTS_TABLE_ID, 
                    d_id as _, 
                    &dist as *const _ as _, 
                    std::mem::size_of::<TpccDistricts>() as _,
                );
            }

            for j in 0..num_items() {
                let s_id = make_stock_key(i, j);

                let stock = TpccStocks::default();

                valuedb.local_put_value(
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

            let order = TpccOrders::default();

            valuedb.local_put_value(
                tpcc_table_id::ORDERS_TABLE_ID, 
                i as _, 
                &order as *const _ as _, 
                std::mem::size_of::<TpccOrders>() as _,
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

            for j in 0..num_items() {
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