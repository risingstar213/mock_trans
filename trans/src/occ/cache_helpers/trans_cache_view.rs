use std::sync::Arc;
use std::hash::Hash;
use std::collections::HashMap;

use crate::MAX_DMA_BUF_SIZE;
use crate::MAX_LOCAL_CACHE_COUNT;
use crate::framework::rpc::RpcProcessMeta;
use crate::framework::scheduler::AsyncScheduler;
use crate::occ::occ::LockContent;
use crate::doca_dma::{ DmaLocalBuf, DmaRemoteBuf };

use super::{ CacheReadSetItem, CacheWriteSetItem };

// simplified trans id
#[derive(PartialEq, Eq, Hash, Clone)]
pub struct TransKey {
    key: u64,
}

impl TransKey {
    pub fn new(meta: RpcProcessMeta) -> Self {
        let lockcontent = LockContent::new(meta.peer_id, meta.rpc_cid);
        Self {
            key: lockcontent.to_content(),
        }
    }
}

// local cache allocator
type LocalCacheBuf = Box<[u8; MAX_DMA_BUF_SIZE]>;

struct LocalCacheBufAllocator {
    free_bufs: Vec<LocalCacheBuf>,
    alloc_num: usize,
}

impl LocalCacheBufAllocator {
    pub fn new() -> Self {
        Self {
            free_bufs: Vec::new(),
            alloc_num: 0,
        }
    }

    pub fn alloc_buf(&mut self) -> Option<LocalCacheBuf> {
        if !self.free_bufs.is_empty() {
            return self.free_bufs.pop();
        } else if self.alloc_num < MAX_LOCAL_CACHE_COUNT {
            return Some(Box::new([0u8; MAX_DMA_BUF_SIZE]));
        } else {
            return None;
        }
    }

    pub fn dealloc_buf(&mut self, buf: LocalCacheBuf) {
        self.free_bufs.push(buf);
    }
}

enum CacheBuf {
    local_buf(LocalCacheBuf),
    remote_buf(DmaRemoteBuf),
}

struct CacheMeta {
    read_range: Vec<CacheBuf>,
    read_count:   Vec<usize>,
    write_range: Vec<CacheBuf>,
    write_count: Vec<usize>,
}

impl CacheMeta {
    pub fn new() -> Self {
        Self {
            read_range: Vec::new(),
            read_count: Vec::new(),
            write_range: Vec::new(),
            write_count: Vec::new(),
        }
    }
}

pub struct TransCacheView<'worker> {
    local_alloc: LocalCacheBufAllocator,
    trans_map: HashMap<TransKey, CacheMeta>,
    scheduler: Arc<AsyncScheduler<'worker>>,
}

impl<'worker> TransCacheView<'worker> {
    pub fn new(scheduler: &Arc<AsyncScheduler<'worker>>) -> Self {
        Self {
            local_alloc: LocalCacheBufAllocator::new(),
            trans_map: HashMap::new(),
            scheduler: scheduler.clone(),
        }
    }

    #[inline]
    pub fn start_trans(&mut self, key: TransKey) {
        self.trans_map.insert(key, CacheMeta::new());
    }

    #[inline]
    pub fn end_trans(&mut self, key: TransKey) {
        let meta = self.trans_map.get_mut(&key).unwrap();
        while let Some(range) = meta.read_range.pop() {
            match range {
                CacheBuf::local_buf(buf) => {
                    self.local_alloc.dealloc_buf(buf);
                }
                CacheBuf::remote_buf(buf) => {
                    #[cfg(feature = "doca_deps")]
                    self.scheduler.dma_dealloc_remote_buf(buf);
                }
            }
        }

        while let Some(range) = meta.write_range.pop() {
            match range {
                CacheBuf::local_buf(buf) => {
                    self.local_alloc.dealloc_buf(buf);
                }
                CacheBuf::remote_buf(buf) => {
                    self.scheduler.dma_dealloc_remote_buf(buf);
                }
            }
        }

        self.trans_map.remove(&key);
    }

    #[inline]
    pub fn get_read_range_num(&self, key: TransKey) -> usize {
        let meta = self.trans_map.get(&key).unwrap();
        meta.read_range.len()
    }

    // TODO: prefetch
    #[inline]
    pub async fn get_read_buf(&mut self, key: TransKey, idx: usize, cid: u32) -> &[CacheReadSetItem] {
        let meta = self.trans_map.get(&key).unwrap();
        let count = meta.read_count[idx];
        let buf = &meta.read_range[idx];
        match buf {
            CacheBuf::local_buf(buf) => {
                unsafe {
                    std::slice::from_raw_parts(buf.as_ptr() as *const CacheReadSetItem, count)
                }
            }
            CacheBuf::remote_buf(buf) => {
                let remote_off = buf.get_off();
                let payload = std::mem::size_of::<CacheReadSetItem>() * count;
                let mut dma_local_buf = self.scheduler.dma_get_local_buf(cid);

                loop {
                    self.scheduler.post_read_dma_req(dma_local_buf.get_off(), remote_off, payload, cid);

                    match self.scheduler.yield_until_dma_ready(cid).await {
                        Ok(_) => {
                            break;
                        }
                        Err(_) => {}
                    }
                }

                unsafe {
                    dma_local_buf.get_mut_slice(count)
                }
            }
        }
    }

    #[inline]
    pub fn get_write_range_num(&self, key: TransKey) -> usize {
        let meta = self.trans_map.get(&key).unwrap();
        meta.write_range.len()
    }

    // TODO: prefetch
    #[inline]
    pub async fn get_write_buf(&mut self, key: TransKey, idx: usize, cid: u32) -> &[CacheWriteSetItem] {
        let meta = self.trans_map.get(&key).unwrap();
        let count = meta.write_count[idx];
        let buf = &meta.write_range[idx];
        match buf {
            CacheBuf::local_buf(buf) => {
                unsafe {
                    std::slice::from_raw_parts(buf.as_ptr() as *const CacheWriteSetItem, count)
                }
            }
            CacheBuf::remote_buf(buf) => {
                let remote_off = buf.get_off();
                let payload = std::mem::size_of::<CacheWriteSetItem>() * count;
                let mut dma_local_buf = self.scheduler.dma_get_local_buf(cid);

                loop {
                    self.scheduler.post_read_dma_req(dma_local_buf.get_off(), remote_off, payload, cid);

                    match self.scheduler.yield_until_dma_ready(cid).await {
                        Ok(_) => {
                            break;
                        }
                        Err(_) => {}
                    }
                }

                unsafe {
                    dma_local_buf.get_mut_slice(count)
                }
            }
        }
    }
}

