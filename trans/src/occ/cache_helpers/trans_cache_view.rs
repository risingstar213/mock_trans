use std::pin::Pin;
use std::sync::{ Arc, Mutex };
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

#[derive(Clone)]
struct LocalCacheBuf(Pin<Arc<[u8; MAX_DMA_BUF_SIZE]>>);

impl LocalCacheBuf {
    #[inline]
    pub unsafe fn get_const_slice<ITEM: Clone>(&self, count: usize) -> &'static [ITEM] {
        unsafe {
            std::slice::from_raw_parts(self.0.as_ptr() as *const ITEM, count)
        }
    }

    #[inline]
    pub unsafe fn set_item<ITEM: Clone>(&mut self, idx: usize, item: ITEM) {
        let slice_mut = std::slice::from_raw_parts_mut(self.0.as_ptr() as *mut ITEM, idx + 1);
        slice_mut[idx] = item;
    }
}

// local cache allocator
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
            return Some(LocalCacheBuf(Arc::pin([0u8; MAX_DMA_BUF_SIZE])));
        } else {
            return None;
        }
    }

    pub fn dealloc_buf(&mut self, buf: LocalCacheBuf) {
        self.free_bufs.push(buf);
    }
}

#[derive(Clone)]
enum CacheBuf {
    LocalBuf(LocalCacheBuf),
    RemoteBuf(DmaRemoteBuf),
}

pub const fn max_read_item_count() -> usize {
    MAX_DMA_BUF_SIZE / std::mem::size_of::<CacheReadSetItem>()
}

pub const fn max_write_item_count() -> usize {
    MAX_DMA_BUF_SIZE / std::mem::size_of::<CacheWriteSetItem>()
}

#[derive(Clone)]
struct CacheMeta {
    buf:   CacheBuf,
    count: usize,
}

impl CacheMeta {
    pub fn new(buf: CacheBuf, count: usize) -> Self {
        Self {
            buf: buf,
            count: count,
        }
    }
}

pub struct TransCacheView<'worker> {
    local_alloc: Mutex<LocalCacheBufAllocator>,
    trans_read_map: Mutex<HashMap<TransKey, Vec<CacheMeta>>>,
    trans_write_map: Mutex<HashMap<TransKey, Vec<CacheMeta>>>,
    scheduler: Arc<AsyncScheduler<'worker>>,
}

impl<'worker> TransCacheView<'worker> {
    pub fn new(scheduler: &Arc<AsyncScheduler<'worker>>) -> Self {
        Self {
            local_alloc: Mutex::new(LocalCacheBufAllocator::new()),
            trans_read_map: Mutex::new(HashMap::new()),
            trans_write_map: Mutex::new(HashMap::new()),
            scheduler: scheduler.clone(),
        }
    }

    #[inline]
    pub fn start_trans(&self, key: &TransKey) {
        self.trans_read_map.lock().unwrap().insert(key.clone(), Vec::new());
        self.trans_read_map.lock().unwrap().insert(key.clone(), Vec::new());
    }

    #[inline]
    pub fn end_trans(&self, key: &TransKey) {
        let mut read_map = self.trans_read_map.lock().unwrap();
        let read_metas = read_map.get_mut(key).unwrap();
        while let Some(meta) = read_metas.pop() {
            match meta.buf {
                CacheBuf::LocalBuf(buf) => {
                    self.local_alloc.lock().unwrap().dealloc_buf(buf);
                }
                CacheBuf::RemoteBuf(buf) => {
                    #[cfg(feature = "doca_deps")]
                    self.scheduler.dma_dealloc_remote_buf(buf);
                }
            }
        }
        read_map.remove(key);

        let mut write_map = self.trans_write_map.lock().unwrap();

        let write_metas =write_map.get_mut(key).unwrap();
        while let Some(meta) = write_metas.pop() {
            match meta.buf {
                CacheBuf::LocalBuf(buf) => {
                    self.local_alloc.lock().unwrap().dealloc_buf(buf);
                }
                CacheBuf::RemoteBuf(buf) => {
                    #[cfg(feature = "doca_deps")]
                    self.scheduler.dma_dealloc_remote_buf(buf);
                }
            }
        }

        write_map.remove(key);
    }

    // read buf
    #[inline]
    pub fn get_read_range_num(&self, key: &TransKey) -> usize {
        self.trans_read_map.lock().unwrap().get(key).unwrap().len()
    }

    // TODO: prefetch
    #[inline]
    pub async fn get_read_buf(&self, key: &TransKey, idx: usize, cid: u32) -> &[CacheReadSetItem] {
        let read_map = self.trans_read_map.lock().unwrap();
        let meta = read_map.get(key).unwrap().get(idx).unwrap().clone();
        drop(read_map);
        match &meta.buf {
            CacheBuf::LocalBuf(buf) => {
                unsafe {
                    buf.get_const_slice(meta.count)
                }
            }
            CacheBuf::RemoteBuf(buf) => {
                let remote_off = buf.get_off();
                let payload = std::mem::size_of::<CacheReadSetItem>() * meta.count;
                let dma_local_buf = self.scheduler.dma_get_local_buf(cid);

                loop {
                    // sync functions can be more useful ???
                    self.scheduler.post_read_dma_req(dma_local_buf.get_off(), remote_off, payload, cid);

                    match self.scheduler.yield_until_dma_ready(cid).await {
                        Ok(_) => {
                            break;
                        }
                        Err(_) => {}
                    }
                }

                unsafe {
                    dma_local_buf.get_const_slice(meta.count)
                }
            }
        }
    }

    #[inline]
    pub fn alloc_read_buf(&mut self, key: &TransKey) {
        let new_buf = self.scheduler.dma_alloc_remote_buf();

        let mut read_map = self.trans_read_map.lock().unwrap();
        let metas = read_map.get_mut(key).unwrap();
        metas.push(CacheMeta::new(CacheBuf::RemoteBuf(new_buf), 0));
    }

    // write buf
    #[inline]
    pub fn get_write_range_num(&self, key: &TransKey) -> usize {
        self.trans_write_map.lock().unwrap().get(key).unwrap().len()
    }

    // TODO: prefetch
    #[inline]
    pub async fn get_write_buf(&self, key: &TransKey, idx: usize, cid: u32) -> &[CacheWriteSetItem] {
        let write_map = self.trans_write_map.lock().unwrap();
        let meta = write_map.get(key).unwrap().get(idx).unwrap().clone();
        drop(write_map);
        match &meta.buf {
            CacheBuf::LocalBuf(buf) => {
                unsafe {
                    buf.get_const_slice(meta.count)
                }
            }
            CacheBuf::RemoteBuf(buf) => {
                let remote_off = buf.get_off();
                let payload = std::mem::size_of::<CacheWriteSetItem>() * meta.count;
                let dma_local_buf = self.scheduler.dma_get_local_buf(cid);

                loop {
                    // sync functions can be more useful ???
                    self.scheduler.post_read_dma_req(dma_local_buf.get_off(), remote_off, payload, cid);

                    match self.scheduler.yield_until_dma_ready(cid).await {
                        Ok(_) => {
                            break;
                        }
                        Err(_) => {}
                    }
                }

                unsafe {
                    dma_local_buf.get_const_slice(meta.count)
                }
            }
        }
    }

    pub fn alloc_write_buf(&self, key: &TransKey) {
        let new_buf = self.scheduler.dma_alloc_remote_buf();
        let mut write_map = self.trans_write_map.lock().unwrap();
        let metas = write_map.get_mut(key).unwrap();
        metas.push(CacheMeta::new(CacheBuf::RemoteBuf(new_buf), 0));
    }
}

pub struct ReadCacheMetaWriter {
    meta:          CacheMeta,
    dma_local_buf: Option<DmaLocalBuf>,
    dirty_count:   usize,
}

impl ReadCacheMetaWriter {
    fn new(meta: &CacheMeta) -> Self {
        Self {
            meta: meta.clone(),
            dma_local_buf: None,
            dirty_count: 0,
        }
    }

    pub async fn append_item(&mut self, item: CacheReadSetItem) {
        match &mut self.meta.buf {
            CacheBuf::LocalBuf(buf) => {
                let idx = self.dirty_count + self.meta.count;
                unsafe {
                    buf.set_item::<CacheReadSetItem>(idx, item);
                }
            }
            CacheBuf::RemoteBuf(buf) => {
                let idx = self.dirty_count;
                if let Some(dma_buf) = &mut self.dma_local_buf {
                    unsafe {
                        dma_buf.set_item::<CacheReadSetItem>(idx, item);
                    }
                }
            }
        }
    }
}