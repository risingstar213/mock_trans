use std::sync::Arc;

use crate::common::random::FastRandom;
use crate::framework::scheduler::AsyncScheduler;
use crate::memstore::memdb::MemDB;
use crate::occ::occ_host::OccHost;

use super::host_worker::TpccHostWorker;
use super::super::*;
use super::super::utils::*;

impl TpccHostWorker {
    pub async fn txn_new_order(&self, rand_gen: &mut FastRandom, cid: u32) {
        let mut txn = OccHost::<TPCC_ITEM_SIZE>::new(
            self.part_id, 
            self.tid,
            cid, 
            &self.valuedb, 
            &self.scheduler,
        );

        txn.start();

        let w_id = rand_gen.next() % num_warehouses();
        let d_id = (rand_gen.next() % 10) + 10 * w_id;
        let c_id = rand_gen.next() % num_customers();
        
        let stock_count = rand_gen.next() % 6 + 6;

        let mut stocks = Vec::new();
        random_get_stocks(stock_count, rand_gen, &mut stocks);

        for i in 0..stock_count {
            txn.fetch_write::<TpccStocks>(
                tpcc_table_id::STOCKS_TABLE_ID,
                warehouse_id_to_part_id(stock_id_to_warehouse_id(stocks[i])) as _,
                stocks[i] as _,
            );
        }

        let idx = txn.fetch_write::<TpccDistricts>(
            tpcc_table_id::DISTRICTS_TABLE_ID,
            warehouse_id_to_part_id(w_id) as _,
            d_id as _,
        );

        let mut dist = txn.get_value::<TpccDistricts>(true, idx).await.clone();
        let o_id = make_order_key(d_id, dist.d_next_o_id as _);

        dist.d_next_o_id = next_o_id(dist.d_next_o_id as _) as _;
        txn.set_value(true, idx, &dist);

        for i in 0..stock_count {
            let mut stock = txn.get_value::<TpccStocks>(true, i).await.clone();
            let ol_quantity = rand_gen.next() % 10 + 1;
            let i_price = rand_gen.next_uniform() * 10000.0;

            if stock.s_quantity >= 10 + ol_quantity as u64 {
                stock.s_quantity -= ol_quantity as u64;
            } else {
                stock.s_quantity += 91 - ol_quantity as u64;
            }

            txn.set_value(true, i, &stock)
        }

        let order = TpccOrders {
            o_c_id:       c_id as _,
            o_carrier_id: 0,
            o_all_local:  true as _,
            o_ol_cnt:     stock_count as _,
            o_entry_d:    0,
        };

        let idx = txn.write::<TpccOrders>(
            tpcc_table_id::ORDERS_TABLE_ID, 
            order_id_to_part_id(o_id) as _, 
            o_id as _, 
            crate::occ::RwType::UPDATE,
        );

        txn.set_value(false, idx, &order);

        txn.commit().await;

    }
}