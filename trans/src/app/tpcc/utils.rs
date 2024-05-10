use std::collections::HashSet;
use std::hash::Hash;
use std::hash::RandomState;
use std::hash::{BuildHasher, Hasher};
use lazy_static::lazy_static;

use crate::TPCC_NPARTITIONS;
use crate::TPCC_SCALE;
use crate::TPCC_PART_OFFLOAD_RATIO;

use crate::common::random::FastRandom;

pub fn random_get_stocks(num: usize, rand_gen: &mut FastRandom, stocks: &mut Vec<usize>) {
    let mut temp_set = HashSet::new();

    for _ in 0..num {
        loop {
            let supplied_w_id = rand_gen.next() % num_warehouses();
            let i_id = rand_gen.next() % num_items();

            let stock_id = make_stock_key(supplied_w_id, i_id);
            if !temp_set.contains(&stock_id) {
                stocks.push(stock_id);
                temp_set.insert(stock_id);
                break;
            }
        }
    }
}

pub fn make_stock_key(w_id: usize, i_id: usize) -> usize {
    w_id * 100000 + i_id
}

pub fn make_order_key(d_id: usize, o_id: usize) -> usize {
    return d_id * 1000 + o_id;
}

pub fn next_o_id(o_id: usize) -> usize {
    return (o_id + 1) % 1000;
}

pub fn num_warehouses() -> usize {
    return TPCC_NPARTITIONS * TPCC_SCALE;
}

pub fn num_customers() -> usize {
    return 10000 * TPCC_SCALE;
}

pub fn num_items() -> usize {
    return 100000 * TPCC_SCALE;
}

pub fn num_orders() -> usize {
    return 10 * num_warehouses() * 1000;
}

pub fn stock_id_to_item_id(s_id: usize) -> usize {
    return s_id % 100000;
}

pub fn stock_id_to_warehouse_id(s_id: usize) -> usize {
    return s_id / 100000;
}

pub fn warehouse_id_to_part_id(w_id: usize) -> usize {
    return w_id / TPCC_SCALE;
}

pub fn order_id_to_part_id(o_id: usize) -> usize {
    return o_id % TPCC_NPARTITIONS;
}

pub fn warehouses_start(part_id: usize) -> usize {
    part_id * TPCC_SCALE
}

pub fn warehouses_end(part_id: usize) -> usize {
    (part_id + 1) * TPCC_SCALE
}

/////////////////////////////////////////////////////////
#[inline]
pub fn stock_id_to_part_id_hybrid_longitude(stock_id: usize, part_id: usize) -> usize {
    let w_id = stock_id_to_warehouse_id(stock_id);
    let i_id = stock_id_to_item_id(stock_id);
    let p_id = warehouse_id_to_part_id(w_id);

    if p_id == part_id {
        return p_id;
    } else if i_id % 100 >= TPCC_PART_OFFLOAD_RATIO {
        return p_id;
    } else {
        return p_id + 100;
    }
}

#[inline]
pub fn stock_id_to_part_id_host_longitude(stock_id: usize, part_id: usize) -> usize {
    let w_id = stock_id_to_warehouse_id(stock_id);
    let i_id = stock_id_to_item_id(stock_id);
    let p_id = warehouse_id_to_part_id(w_id);

    if p_id != part_id {
        return p_id;
    } else if i_id % 100 >= TPCC_PART_OFFLOAD_RATIO {
        return p_id;
    } else {
        return p_id + 100;
    }
}

#[inline]
pub fn dist_id_to_part_id_hybrid_longitude(d_id: usize, part_id: usize) -> usize {
    let w_id = d_id / 10;
    let d_local = d_id % 10;
    let p_id = warehouse_id_to_part_id(w_id);

    if p_id == part_id {
        return p_id;
    } else if d_local * 10 >= TPCC_PART_OFFLOAD_RATIO {
        return p_id;
    } else {
        return p_id + 100;
    }
}

#[inline]
pub fn dist_id_to_part_id_host_longitude(d_id: usize, part_id: usize) -> usize {
    let w_id = d_id / 10;
    let d_local = d_id % 10;
    let p_id = warehouse_id_to_part_id(w_id);

    if p_id != part_id {
        return p_id;
    } else if d_local * 10 >= TPCC_PART_OFFLOAD_RATIO {
        return p_id;
    } else {
        return p_id + 100;
    }
}

#[inline]
pub fn order_id_to_part_id_hybrid_longitude(o_id: usize, part_id: usize) -> usize {
    let p_id = order_id_to_part_id(o_id);

    if p_id == part_id {
        return p_id;
    } else if o_id % 100 >= TPCC_PART_OFFLOAD_RATIO {
        return p_id;
    } else {
        return p_id + 100;
    }
}

#[inline]
pub fn order_id_to_part_id_host_longitude(o_id: usize, part_id: usize) -> usize {
    let p_id = order_id_to_part_id(o_id);

    if p_id != part_id {
        return p_id;
    } else if o_id % 100 >= TPCC_PART_OFFLOAD_RATIO {
        return p_id;
    } else {
        return p_id + 100;
    }
}