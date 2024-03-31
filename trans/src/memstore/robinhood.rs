use std::collections::HashMap;
use std::hash::Hash;
use std::hash::RandomState;
use std::hash::{BuildHasher, Hasher};

use rand::prelude::*;

#[repr(C)]
#[derive(Clone)]
struct RobinHoodUnit<K, V>
where
    K: Default + Eq + PartialEq + Hash + Copy + Clone + Send + Sync,
    V: Default + Send + Sync,
{
    valid: bool,
    key: K,
    dib: usize,
    value: V,
}

impl<K, V> Default for RobinHoodUnit<K, V>
where
    K: Default + Eq + PartialEq + Hash + Copy + Clone + Send + Sync,
    V: Default + Send + Sync,
{
    fn default() -> Self {
        Self {
            valid: false,
            key: K::default(),
            dib: 0,
            value: V::default(),
        }
    }
}

impl<K, V> RobinHoodUnit<K, V>
where
    K: Default + Eq + PartialEq + Hash + Copy + Clone + Send + Sync,
    V: Default + Send + Sync,
{
    fn new(valid: bool, key: K, dib: usize, value: V) -> Self {
        Self {
            valid: valid,
            key: key,
            dib: dib,
            value: value,
        }
    }
}

struct UpdateList<K, V>
where
    K: Default + Eq + PartialEq + Hash + Copy + Clone + Send + Sync,
    V: Default + Send + Sync,
{
    units: Vec<RobinHoodUnit<K, V>>,
    idx: Vec<usize>,
}

impl<K, V> UpdateList<K, V>
where
    K: Default + Eq + PartialEq + Hash + Copy + Clone + Send + Sync,
    V: Default + Send + Sync,
{
    fn new() -> Self {
        Self {
            units: Vec::new(),
            idx: Vec::new(),
        }
    }
}

// #[derive(Copy, Clone)]
// struct RobinHoodMeta<K>
// where
//     K: Eq + PartialEq + Hash + Copy + Clone + Send + Sync,
// {
//     valid:  bool,
//     key:    K,
//     dib:    usize,
// }

// impl<K> Default for RobinHoodMeta<K>
// where
//     K: Eq + PartialEq + Hash + Copy + Clone + Send + Sync,
// {
//     fn default() -> Self {
//         Self {
//             valid: false,
//             key:   unsafe { std::mem::zeroed() },
//             dib:   0,
//         }
//     }
// }

// impl<K> RobinHoodMeta<K>
// where
//     K: Eq + PartialEq + Hash + Copy + Clone + Send + Sync,
// {
//     pub fn new(valid: bool, key: K, dib: usize) -> Self {
//         Self {
//             valid: valid,
//             key:   key,
//             dib:   dib,
//         }
//     }
// }

// TODO: link lists
#[allow(unused)]
struct OverflowBuckets<K, V>
where
    K: Default + Eq + PartialEq + Hash + Copy + Clone + Send + Sync,
    V: Default + Clone + Send + Sync,
{
    keys: Vec<K>,
    values: Vec<V>,
}

#[allow(unused)]
impl<K, V> OverflowBuckets<K, V>
where
    K: Default + Eq + PartialEq + Hash + Copy + Clone + Send + Sync,
    V: Default + Clone + Send + Sync,
{
    pub fn new() -> Self {
        Self {
            keys: Vec::new(),
            values: Vec::new(),
        }
    }

    pub fn push(&mut self, key: &K, value: V) {
        self.keys.push(*key);
        self.values.push(value);
    }
}

/// TODO: overflow chains
/// Need uniform marks
/// But the size is restricted.
/// How to implement?
pub struct RobinHood<K, V>
where
    K: Default + Eq + PartialEq + Hash + Copy + Clone + Send + Sync,
    V: Default + Clone + Send + Sync,
{
    units: Vec<RobinHoodUnit<K, V>>,
    inbuf_cap: usize,
    dib_max: usize,
    inbuf_size: usize,
    // TODO: simple linked buckets
    of_buckets: HashMap<K, V>,
    hash_builder: RandomState,
}

// (TODO:) expose memory to support one-side rdma primitives and dma functions
impl<K, V> RobinHood<K, V>
where
    K: Default + Eq + PartialEq + Hash + Copy + Clone + Send + Sync,
    V: Default + Clone + Send + Sync,
{
    pub fn new(size: usize, dib_max: usize) -> Self {
        let mut units = Vec::with_capacity(size);

        for _ in 0..size {
            units.push(RobinHoodUnit::default());
        }

        Self {
            units: units,
            inbuf_cap: size,
            dib_max: dib_max,
            inbuf_size: 0,
            of_buckets: HashMap::<K, V>::new(),
            hash_builder: RandomState::new(),
        }
    }

    #[allow(unused)]
    pub fn from_raw(size: usize, dib_max: usize) -> Self {
        Self::new(size, dib_max)
    }

    #[allow(unused)]
    pub fn into_raw(&self) -> *mut u8 {
        std::ptr::null_mut()
    }

    fn hash(&self, key: &K) -> usize {
        let mut hasher = self.hash_builder.build_hasher();
        key.hash(&mut hasher);

        hasher.finish() as usize % self.inbuf_cap
    }

    // lookup for read or update
    pub fn get(&self, key: &K) -> Option<&V> {
        if let Some(value) = self.of_buckets.get(key) {
            return Some(value);
        }

        let capacity = self.inbuf_cap;
        let inds = self.hash(key);
        let mut ind = inds;

        loop {
            if !self.units[ind].valid {
                return None;
            }

            if self.units[ind].key.eq(key) {
                return Some(&self.units[ind].value);
            }

            ind += 1;
            if ind >= capacity {
                ind = 0;
            }

            if ind == inds {
                return None;
            }
        }
    }

    #[inline]
    fn update_with_list(&mut self, update_list: &mut UpdateList<K, V>) {
        let update_length = update_list.units.len();
        if update_length > 0 {
            for i in (0..update_length).rev() {
                let idx = update_list.idx[i];
                std::mem::swap(&mut self.units[idx], &mut update_list.units[i]);
            }
        }
        return;
    }

    // insert
    pub fn put(&mut self, key: &K, value: &V) {
        let capacity = self.inbuf_cap;

        if self.inbuf_size >= capacity {
            self.of_buckets.insert(*key, value.clone());
            return;
        }

        let inds = self.hash(key);
        let mut ind = inds;

        let mut now_key = *key;
        let mut now_dib = 0;
        let mut now_data = value.clone();
        let mut update_list = UpdateList::<K, V>::new();

        loop {
            if now_dib >= self.dib_max {
                self.of_buckets.insert(now_key, now_data);

                self.update_with_list(&mut update_list);
                return;
            } else if !self.units[ind].valid {
                self.units[ind] = RobinHoodUnit::new(true, now_key, now_dib, now_data);
                self.inbuf_size += 1;

                self.update_with_list(&mut update_list);
                return;
            }

            if self.units[ind].dib <= now_dib {
                update_list.units.push(RobinHoodUnit::new(
                    true,
                    now_key,
                    now_dib,
                    now_data.clone(),
                ));
                update_list.idx.push(ind);

                now_key = self.units[ind].key;
                now_dib = self.units[ind].dib;
                now_data = self.units[ind].value.clone();
            }

            ind += 1;
            now_dib += 1;

            if ind >= capacity {
                ind = 0;
            }
        }
    }

    #[inline]
    fn get_index_inbuf(&self, key: &K) -> Option<usize> {
        let capacity = self.inbuf_cap;
        let inds = self.hash(key);
        let mut ind = inds;

        loop {
            if !self.units[ind].valid {
                return None;
            }
            if self.units[ind].key.eq(key) {
                return Some(ind);
            }

            ind += 1;
            if ind >= capacity {
                ind = 0;
            }

            if ind == inds {
                return None;
            }
        }
    }

    // delete
    pub fn erase(&mut self, key: &K) -> Option<V> {
        if let Some(value) = self.of_buckets.remove(key) {
            return Some(value);
        }

        let capacity = self.inbuf_cap;
        if let Some(mut ind) = self.get_index_inbuf(key) {
            let old_value = self.units[ind].value.clone();
            self.units[ind] = RobinHoodUnit::default();
            // back shift
            loop {
                let mut next_ind = ind + 1;
                if next_ind >= capacity {
                    next_ind = 0;
                }

                if !self.units[next_ind].valid || self.units[next_ind].dib == 0 {
                    self.inbuf_size -= 1;
                    return Some(old_value);
                }

                self.units.swap(ind, next_ind);
                self.units[ind].dib -= 1;

                ind = next_ind;
            }
        }
        return None;
    }
}

impl<V> RobinHood<usize, V>
where
    V: Default + Clone + Send + Sync,
{
    #[allow(unused)]
    fn print_store(&self) {
        let capacity = self.inbuf_cap;
        for i in 0..capacity {
            print!(
                "({}, {}, {})",
                self.units[i].valid, self.units[i].key, self.units[i].dib
            );
        }
        println!();
    }
}

trait RandGen {
    /// Randomly generates a value.
    fn rand_gen(rng: &mut ThreadRng) -> Self;
}

impl RandGen for usize {
    /// pick only 16 bits, MSB=0
    fn rand_gen(rng: &mut ThreadRng) -> Self {
        const MASK: usize = 0x4004004004007777usize;
        rng.gen::<usize>() & MASK
    }
}

#[allow(unused)]
fn stress_sequential(steps: usize) {
    #[derive(Debug, Eq, PartialEq)]
    enum Ops {
        LookupSome,
        LookupNone,
        Insert,
        DeleteSome,
        DeleteNone,
    }

    let ops = [
        Ops::LookupSome,
        Ops::LookupNone,
        Ops::Insert,
        Ops::DeleteSome,
        Ops::DeleteNone,
    ];

    let mut rng = thread_rng();
    let mut map = RobinHood::<usize, usize>::new(1000, 4);
    let mut hashmap = HashMap::<usize, usize>::new();

    for i in 0..steps {
        let op = ops.choose(&mut rng).unwrap();

        let count = hashmap.len();
        // if count >= 95 && *op == Ops::Insert {
        //     // println!("interation {}: skip the insert!", i);
        //     continue;
        // }

        match op {
            Ops::LookupSome => {
                if let Some(key) = hashmap.keys().choose(&mut rng) {
                    println!("iteration {}: lookup({:?}) (existing)", i, key);
                    assert_eq!(map.get(key), hashmap.get(key));
                }
            }
            Ops::LookupNone => {
                let key = usize::rand_gen(&mut rng);
                println!("iteration {}: lookup({:?}) (non-existing)", i, key);
                assert_eq!(map.get(&key), hashmap.get(&key));
            }
            Ops::Insert => {
                let key = usize::rand_gen(&mut rng);

                if hashmap.contains_key(&key) {
                    continue;
                }
                let value = rng.gen::<usize>();
                println!("iteration {}: insert({:?}, {})", i, key, value);
                let _ = map.put(&key, &value);
                hashmap.entry(key).or_insert(value);

                map.print_store();
            }
            Ops::DeleteSome => {
                let key = hashmap.keys().choose(&mut rng).map(|k| k.clone());
                if let Some(key) = key {
                    println!("iteration {}: delete({:?}) (existing)", i, key);
                    assert_eq!(map.erase(&key).ok_or(()), hashmap.remove(&key).ok_or(()));

                    map.print_store();
                }
            }
            Ops::DeleteNone => {
                let key = usize::rand_gen(&mut rng);
                println!("iteration {}: delete({:?}) (non-existing)", i, key);
                assert_eq!(map.erase(&key).ok_or(()), hashmap.remove(&key).ok_or(()));
            }
        }
    }
}

#[test]
fn test_robinhood() {
    stress_sequential(50000);
}
