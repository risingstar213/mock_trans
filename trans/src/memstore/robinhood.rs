use std::collections::btree_map::Values;
use std::collections::HashMap;
use std::hash::{BuildHasher, Hasher};
use std::hash::RandomState;
use std::hash::Hash;

use rand::prelude::*;

struct UpdateList<K, V> 
where
    K: Eq + PartialEq + Hash + Copy + Clone + Send + Sync,
    V: Clone + Send + Sync,
{
    metas: Vec<RobinHoodMeta<K>>,
    data:  Vec<V>,
    idx:   Vec<usize>,
}

impl<K, V> UpdateList<K, V>
where
    K: Eq + PartialEq + Hash + Copy + Clone + Send + Sync,
    V: Clone + Send + Sync, 
{
    fn new() -> Self {
        Self {
            metas: Vec::new(),
            data:  Vec::new(),
            idx:   Vec::new()
        }
    }
}

#[derive(Copy, Clone)]
struct RobinHoodMeta<K>
where
    K: Eq + PartialEq + Hash + Copy + Clone + Send + Sync,
{
    valid:  bool,
    key:    K,
    dib:    usize,
}

impl<K> Default for RobinHoodMeta<K>
where
    K: Eq + PartialEq + Hash + Copy + Clone + Send + Sync,
{
    fn default() -> Self {
        Self {
            valid: false,
            key:   unsafe { std::mem::zeroed() },
            dib:   0,
        }
    }
}

impl<K> RobinHoodMeta<K>
where
    K: Eq + PartialEq + Hash + Copy + Clone + Send + Sync,
{
    pub fn new(valid: bool, key: K, dib: usize) -> Self {
        Self {
            valid: valid,
            key:   key,
            dib:   dib,
        }
    }
}


// TODO: link lists
#[allow(unused)]
struct OverflowBuckets<K, V>
where 
    K: Eq + PartialEq + Hash + Copy + Clone + Send + Sync,
    V: Clone + Send + Sync,
{
    keys:   Vec<K>,
    values: Vec<V>
}

#[allow(unused)]
impl<K, V> OverflowBuckets<K, V> 
where 
    K: Eq + PartialEq + Hash + Copy + Clone + Send + Sync,
    V: Clone + Send + Sync,
{
    pub fn new() -> Self {
        Self {
            keys:   Vec::new(),
            values: Vec::new()
        }
    }

    pub fn push(&mut self, key: &K, value: &V) {
        self.keys.push(*key);
        self.values.push(value.clone());
    }

}

// TODO: overflow chains
pub struct RobinHood<K, V> 
where
    K: Eq + PartialEq + Hash + Copy + Clone + Send + Sync,
    V: Clone + Send + Sync,
{
    metas:        Vec<RobinHoodMeta<K>>,
    data:         Vec<V>,
    dib_max:      usize, 
    inbuf_size:   usize,
    // TODO: simple linked buckets
    of_buckets:   HashMap<K, V>,
    hash_builder: RandomState,
}

impl<K, V> RobinHood<K, V> 
where
    K: Eq + PartialEq + Hash + Copy + Clone + Send + Sync,
    V: Clone + Send + Sync,
{
    pub fn new(size: usize, dib_max: usize) -> Self {
        let mut data = Vec::new();
        let mut metas = Vec::new();

        for _ in 0..size {
            data.push(unsafe { std::mem::zeroed() });
            metas.push(RobinHoodMeta::default());
        }
        
        Self {
            metas:        metas,
            data:         data,
            dib_max:      dib_max,
            inbuf_size:   0,
            of_buckets:   HashMap::<K, V>::new(),
            hash_builder: RandomState::new()
        }
    }
    
    fn hash(&self, key: &K) -> usize {
        let mut hasher = self.hash_builder.build_hasher();
        key.hash(&mut hasher);

        let capacity = self.metas.len();
        hasher.finish() as usize % capacity
    }
    
    pub fn get(&self, key: &K) -> Option<&V> {
        if let Some(value) = self.of_buckets.get(key) {
            return Some(value);
        }

        let capacity = self.metas.len();
        let inds = self.hash(key);
        let mut ind = inds;

        loop {
            if !self.metas[ind].valid {
                return None;
            }

            if self.metas[ind].key.eq(key) {
                return Some(&self.data[ind]);
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

    pub fn put(&mut self, key: &K, value: &V) {
        let capacity = self.metas.len();

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

        let mut insert_last = false;

        loop {
            if now_dib >= self.dib_max {
                self.of_buckets.insert(now_key, now_data.clone());

                insert_last = true;
            } else if !self.metas[ind].valid {
                self.metas[ind] = RobinHoodMeta::new(
                    true,
                    now_key,
                    now_dib
                );
                self.data[ind] = now_data.clone();
                self.inbuf_size += 1;

                insert_last = true;
            }

            if insert_last {
                if update_list.metas.len() > 0 {
                    for i in (0..update_list.metas.len()).rev() {

                        if update_list.metas[i].key == now_key {
                            println!("not rational !")
                        }
                        let idx = update_list.idx[i];

                        self.metas[idx] = update_list.metas[i];
                        self.data[idx] = update_list.data[i].clone();
                    }
                }
                return;
            }

            if self.metas[ind].dib <= now_dib {
                update_list.metas.push(RobinHoodMeta::new(
                    true,
                    now_key,
                    now_dib,
                ));
                update_list.data.push(now_data.clone());
                update_list.idx.push(ind);

                now_key = self.metas[ind].key;
                now_dib = self.metas[ind].dib;
                now_data = self.data[ind].clone();
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
        let capacity = self.metas.len();
        let inds = self.hash(key);
        let mut ind = inds;

        loop {
            if !self.metas[ind].valid {
                return None;
            }
            if self.metas[ind].key.eq(key) {
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

    pub fn erase(&mut self, key: &K) -> Option<V> {

        if let Some(value) = self.of_buckets.remove(key) {
            return Some(value);
        }

        let capacity = self.metas.len();
        if let Some(mut ind) = self.get_index_inbuf(key) {
            let old_value = self.data[ind].clone();
            // back shift
            loop {
                let mut next_ind = ind + 1;
                if next_ind >= capacity {
                    next_ind = 0;
                }

                if !self.metas[next_ind].valid || self.metas[next_ind].dib == 0 {
                    self.metas[ind] = RobinHoodMeta::default();
                    self.data[ind] = unsafe { std::mem::zeroed() };
                    self.inbuf_size -= 1;
                    return Some(old_value);
                }

                self.metas[ind] = RobinHoodMeta::new(
                    true,
                    self.metas[next_ind].key,
                    self.metas[next_ind].dib - 1,
                );
                self.data[ind] = self.data[next_ind].clone();

                ind = next_ind;
            }
        }
        return None;
    }
}

impl<V> RobinHood<usize, V>
where
    V: Clone + Send + Sync,
{
    pub fn print_store(&self) {
        let capacity = self.metas.len();
        for i in 0..capacity {
            print!("({}, {}, {})", self.metas[i].valid, self.metas[i].key, self.metas[i].dib);
        }
        println!();
    }
}

pub trait RandGen {
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

pub fn stress_sequential(steps: usize) {
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

                // map.print_store();
            }
            Ops::DeleteSome => {
                let key = hashmap.keys().choose(&mut rng).map(|k| k.clone());
                if let Some(key) = key {
                    println!("iteration {}: delete({:?}) (existing)", i, key);
                    assert_eq!(map.erase(&key).ok_or(()), hashmap.remove(&key).ok_or(()));

                    // map.print_store();
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