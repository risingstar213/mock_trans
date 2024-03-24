use std::hash::{BuildHasher, Hasher};
use std::hash::RandomState;
use std::hash::Hash;


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

pub struct RobinHood<K, V, S = RandomState> 
where
    K: Eq + PartialEq + Hash + Copy + Clone + Send + Sync,
    V: Clone + Send + Sync,
    S: BuildHasher
{
    metas:        Vec<RobinHoodMeta<K>>,
    data:         Vec<V>,
    size:         usize,
    hash_builder: S,
}

impl<K, V, S> RobinHood<K, V, S> 
where
    K: Eq + PartialEq + Hash + Copy + Clone + Send + Sync,
    V: Clone + Send + Sync,
    S: BuildHasher
{
    pub fn new(size: usize, hasher: S) -> Self {
        let mut data = Vec::new();
        let mut metas = Vec::new();

        for _ in 0..size {
            data.push(unsafe { std::mem::zeroed() });
            metas.push(RobinHoodMeta::default());
        }
        
        Self {
            data:         data,
            metas:        metas,
            size:         size,
            hash_builder: hasher
        }
    }
    
    fn hash(&self, key: &K) -> usize {
        let mut hasher = self.hash_builder.build_hasher();
        key.hash(&mut hasher);
        hasher.finish() as usize % self.size
    }
    
    pub fn get(&self, key: &K) -> Option<V> {
        let inds = self.hash(key);
        let mut ind = inds;

        loop {
            if self.metas[ind].key.eq(key) {
                return Some(self.data[ind].clone());
            }

            ind += 1;
            if ind >= self.size {
                ind = 0;
            }

            if ind == inds {
                return None;
            }
        }
    }

    pub fn put(&mut self, key: &K, value: &V) {
        let inds = self.hash(key);
        let mut ind = inds;

        let mut now_key = *key;
        let mut now_dib = 1;
        let mut now_data = value.clone();
        let mut update_list = UpdateList::<K, V>::new();
        loop {
            let slot = &self.metas[ind];
            if !slot.valid {
                return;
            }

            if slot.dib <= now_dib {
                update_list.metas.push(RobinHoodMeta::new(
                    true,
                    now_key,
                    now_dib,
                ));
                update_list.data.push(now_data.clone());
                update_list.idx.push(ind);

                now_key = slot.key;
                now_dib = slot.dib;
                now_data = self.data[ind].clone();
            }

            ind += 1;
            now_dib += 1;
        }
    }

    pub fn erase(&mut self, key: &K, value: &V) {
        todo!()
    }
}