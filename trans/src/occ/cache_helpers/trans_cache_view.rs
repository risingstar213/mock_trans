use core::panic;
use std::pin::Pin;
use std::sync::{ Arc, Mutex };
use std::hash::Hash;
use std::collections::HashMap;

use crate::MAX_DMA_BUF_SIZE;
use crate::MAX_LOCAL_CACHE_BUF_COUNT;
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
    pub fn new(meta: &RpcProcessMeta) -> Self {
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
        } else if self.alloc_num < MAX_LOCAL_CACHE_BUF_COUNT {
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

impl CacheBuf {
    fn is_remote_buf(&self) -> bool {
        matches!(*self, CacheBuf::RemoteBuf(_))
    }

    fn is_local_buf(&self) -> bool {
        matches!(*self, CacheBuf::LocalBuf(_))
    }
}

pub const fn max_read_item_count_in_buf() -> usize {
    MAX_DMA_BUF_SIZE / std::mem::size_of::<CacheReadSetItem>()
}

pub const fn max_write_item_count_in_buf() -> usize {
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
    pub fn start_read_trans(&self, key: &TransKey) {
        if self.trans_read_map.lock().unwrap().contains_key(key) {
            return;
        }
        self.trans_read_map.lock().unwrap().insert(key.clone(), Vec::new());
    }

    #[inline]
    pub fn start_write_trans(&self, key: &TransKey) {
        if self.trans_write_map.lock().unwrap().contains_key(key) {
            return;
        }
        self.trans_write_map.lock().unwrap().insert(key.clone(), Vec::new());
    }

    #[inline]
    pub fn end_read_trans(&self, key: &TransKey) {
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
    }

    #[inline]
    pub fn end_write_trans(&self, key: &TransKey) {
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

    #[cfg(feature = "doca_deps")]
    fn get_local_dma_buf(&self, cid: u32) -> DmaLocalBuf {
        self.scheduler.dma_get_local_buf(cid)
    }

    #[inline]
    async fn get_buf_slice<ITEM: Clone + 'static>(&self, meta: CacheMeta, cid: u32) -> &[ITEM] {
        match &meta.buf {
            CacheBuf::LocalBuf(buf) => {
                unsafe {
                    return buf.get_const_slice(meta.count);
                }
            }
            CacheBuf::RemoteBuf(buf) => {
                #[cfg(feature = "doca_deps")]
                if true {
                    let remote_off = buf.get_off();
                    let payload = std::mem::size_of::<ITEM>() * meta.count;
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

                    return unsafe { dma_local_buf.get_const_slice(meta.count) };
                }

                panic!("unsupported!");

            }
        }
    }

    #[inline]
    fn block_get_buf_slice<ITEM: Clone + 'static>(&self, meta: CacheMeta, cid: u32) -> &[ITEM] {
        match &meta.buf {
            CacheBuf::LocalBuf(buf) => {
                unsafe {
                    return buf.get_const_slice(meta.count);
                }
            }
            CacheBuf::RemoteBuf(buf) => {
                #[cfg(feature = "doca_deps")]
                if true {
                    let remote_off = buf.get_off();
                    let payload = std::mem::size_of::<ITEM>() * meta.count;
                    let dma_local_buf = self.scheduler.dma_get_local_buf(cid);

                    loop {
                        // sync functions can be more useful ???
                        self.scheduler.post_read_dma_req(dma_local_buf.get_off(), remote_off, payload, cid);

                        match self.scheduler.busy_until_dma_ready(cid) {
                            Ok(_) => {
                                break;
                            }
                            Err(_) => {}
                        }
                    }

                    return unsafe { dma_local_buf.get_const_slice(meta.count) };
                }

                panic!("unsupported!");

            }
        }
    }

    #[inline]
    async fn sync_buf<ITEM: Clone + 'static>(&self, meta: CacheMeta, cid: u32, dma_local_buf: &Option<DmaLocalBuf>, dirty_count: usize) {
        match &meta.buf {
            CacheBuf::LocalBuf(_) => {}
            CacheBuf::RemoteBuf(buf) => {
                #[cfg(feature = "doca_deps")]
                if true {
                    let remote_off = buf.get_off() + meta.count * std::mem::size_of::<ITEM>();
                    let payload = dirty_count * std::mem::size_of::<ITEM>();

                    loop {
                        // sync functions can be more useful ???
                        self.scheduler.post_write_dma_req(dma_local_buf.unwrap().get_off(), remote_off, payload, cid);

                        match self.scheduler.yield_until_dma_ready(cid).await {
                            Ok(_) => {
                                break;
                            }
                            Err(_) => {}
                        }
                    }

                    return;
                }
                panic!("unsupported!");
            }
        }
    }

    fn block_sync_buf<ITEM: Clone + 'static>(&self, meta: CacheMeta, cid: u32, dma_local_buf: &Option<DmaLocalBuf>, dirty_count: usize) {
        match &meta.buf {
            CacheBuf::LocalBuf(_) => {}
            CacheBuf::RemoteBuf(buf) => {
                #[cfg(feature = "doca_deps")]
                if true {
                    let remote_off = buf.get_off() + meta.count * std::mem::size_of::<ITEM>();
                    let payload = dirty_count * std::mem::size_of::<ITEM>();

                    loop {
                        // sync functions can be more useful ???
                        self.scheduler.post_write_dma_req(dma_local_buf.unwrap().get_off(), remote_off, payload, cid);

                        match self.scheduler.busy_until_dma_ready(cid) {
                            Ok(_) => {
                                break;
                            }
                            Err(_) => {}
                        }
                    }

                    return;
                }

                panic!("unsupported");
            }
        }
    }
}

// read write
impl<'worker> TransCacheView<'worker> {
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
        self.get_buf_slice(meta, cid).await
    }

    #[inline]
    pub fn block_get_read_buf(&self, key: &TransKey, idx: usize, cid: u32) -> &[CacheReadSetItem] {
        let read_map = self.trans_read_map.lock().unwrap();
        let meta = read_map.get(key).unwrap().get(idx).unwrap().clone();
        drop(read_map);
        self.block_get_buf_slice(meta, cid)
    }

    #[inline]
    pub fn alloc_read_buf(&self, key: &TransKey) {
        let new_buf = self.local_alloc.lock().unwrap().alloc_buf().unwrap();

        let mut read_map = self.trans_read_map.lock().unwrap();
        let read_vec = read_map.get_mut(key).unwrap();
        read_vec.push(CacheMeta::new(CacheBuf::LocalBuf(new_buf), 0));
    }

    #[inline]
    pub fn new_read_cache_writer(&self, key: &TransKey, cid: u32) -> ReadCacheMetaWriter {
        let mut read_map = self.trans_read_map.lock().unwrap();
        let read_vec = read_map.get_mut(key).unwrap();

        if read_vec.len() == 0 {
            self.alloc_read_buf(key);
        }

        let idx = read_vec.len() - 1;
        
        ReadCacheMetaWriter::new(key, cid, read_vec.get(idx).unwrap())
    }

    #[inline]
    fn get_last_read_meta(&self, key: &TransKey) -> CacheMeta {
        let read_map = self.trans_read_map.lock().unwrap();
        let read_vec = read_map.get(key).unwrap();
        let idx = read_vec.len() - 1;
        read_vec.get(idx).unwrap().clone()
    }

    #[inline]
    async fn sync_read_buf(&self, key: &TransKey, cid: u32, dma_local_buf: &Option<DmaLocalBuf>, dirty_count: usize) {
        let mut read_map = self.trans_read_map.lock().unwrap();
        let read_vec = read_map.get_mut(key).unwrap();
        let idx = read_vec.len() - 1;
        // update count here is proper here ???
        read_vec.get_mut(idx).unwrap().count += dirty_count;
        let meta = read_vec.get(idx).unwrap().clone();
        drop(read_map);

        self.sync_buf::<CacheReadSetItem>(meta, cid, dma_local_buf, dirty_count).await;
    }

    #[inline]
    fn block_sync_read_buf(&self, key: &TransKey, cid: u32, dma_local_buf: &Option<DmaLocalBuf>, dirty_count: usize) {
        let mut read_map = self.trans_read_map.lock().unwrap();
        let read_vec = read_map.get_mut(key).unwrap();
        let idx = read_vec.len() - 1;
        // update count here is proper here ???
        read_vec.get_mut(idx).unwrap().count += dirty_count;
        let meta = read_vec.get(idx).unwrap().clone();
        drop(read_map);

        self.block_sync_buf::<CacheReadSetItem>(meta, cid, dma_local_buf, dirty_count);
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
        self.get_buf_slice(meta, cid).await
    }

    #[inline]
    pub fn block_get_write_buf(&self, key: &TransKey, idx: usize, cid: u32) -> &[CacheWriteSetItem] {
        let write_map = self.trans_write_map.lock().unwrap();
        let meta = write_map.get(key).unwrap().get(idx).unwrap().clone();
        drop(write_map);
        self.block_get_buf_slice(meta, cid)
    }

    pub fn alloc_write_buf(&self, key: &TransKey) {
        let new_buf = self.local_alloc.lock().unwrap().alloc_buf().unwrap();
        let mut write_map = self.trans_write_map.lock().unwrap();
        let metas = write_map.get_mut(key).unwrap();
        metas.push(CacheMeta::new(CacheBuf::LocalBuf(new_buf), 0));
    }

    #[inline]
    pub fn new_write_cache_writer(&self, key: &TransKey, cid: u32) -> WriteCacheMetaWriter {
        let mut write_map = self.trans_write_map.lock().unwrap();
        let write_vec = write_map.get_mut(key).unwrap();

        if write_vec.len() == 0 {
            self.alloc_write_buf(key);
        }

        let idx = write_vec.len() - 1;
        
        WriteCacheMetaWriter::new(key, cid, write_vec.get(idx).unwrap())
    }

    #[inline]
    fn get_last_write_meta(&self, key: &TransKey) -> CacheMeta {
        let write_map = self.trans_write_map.lock().unwrap();
        let write_vec = write_map.get(key).unwrap();
        let idx = write_vec.len() - 1;
        write_vec.get(idx).unwrap().clone()
    }

    #[inline]
    async fn sync_write_buf(&self, key: &TransKey, cid: u32, dma_local_buf: &Option<DmaLocalBuf>, dirty_count: usize) {
        let mut write_map = self.trans_write_map.lock().unwrap();
        let write_vec = write_map.get_mut(key).unwrap();
        let idx = write_vec.len() - 1;
        // update count here is proper here ???
        write_vec.get_mut(idx).unwrap().count += dirty_count;
        let meta = write_vec.get(idx).unwrap().clone();
        drop(write_map);

        self.sync_buf::<CacheWriteSetItem>(meta, cid, dma_local_buf, dirty_count).await;
    }

    #[inline]
    fn block_sync_write_buf(&self, key: &TransKey, cid: u32, dma_local_buf: &Option<DmaLocalBuf>, dirty_count: usize) {
        let mut write_map = self.trans_write_map.lock().unwrap();
        let write_vec = write_map.get_mut(key).unwrap();
        let idx = write_vec.len() - 1;
        // update count here is proper here ???
        write_vec.get_mut(idx).unwrap().count += dirty_count;
        let meta = write_vec.get(idx).unwrap().clone();
        drop(write_map);

        self.block_sync_buf::<CacheWriteSetItem>(meta, cid, dma_local_buf, dirty_count);
    }
}

pub struct ReadCacheMetaWriter {
    trans_key:     TransKey,
    cid:           u32,
    meta:          CacheMeta,
    dma_local_buf: Option<DmaLocalBuf>,
    dirty_count:   usize,
}

impl<'worker> ReadCacheMetaWriter {
    fn new(trans_key: &TransKey, cid: u32, meta: &CacheMeta) -> Self {
        Self {
            trans_key: trans_key.clone(),
            cid: cid,
            meta: meta.clone(),
            dma_local_buf: None,
            dirty_count: 0,
        }
    }

    fn set_item(&mut self, item: CacheReadSetItem) {
        match &mut self.meta.buf {
            CacheBuf::LocalBuf(buf) => {
                let idx = self.dirty_count + self.meta.count;
                unsafe {
                    buf.set_item(idx, item);
                }
            }
            CacheBuf::RemoteBuf(_) => {
                #[cfg(feature = "doca_deps")]
                if true {
                    let idx = self.dirty_count;
                    if let Some(dma_buf) = &mut self.dma_local_buf {
                        unsafe {
                            dma_buf.set_item(idx, item);
                        }
                    }
                    return;
                }

                panic!("unsupported");
            }
        }

        self.dirty_count += 1;
    }

    pub async fn append_item(&mut self, view: &TransCacheView<'worker>, item: CacheReadSetItem) {
        if self.meta.count + self.dirty_count >= max_read_item_count_in_buf() {
            view.sync_read_buf(&self.trans_key, self.cid, &self.dma_local_buf, self.dirty_count).await;
            view.alloc_read_buf(&self.trans_key);

            self.meta = view.get_last_read_meta(&self.trans_key);

            #[cfg(feature = "doca_deps")]
            if self.meta.buf.is_remote_buf() && self.dma_local_buf.is_none() {
                self.dma_local_buf = Some(view.get_local_dma_buf(self.cid));
            }

            self.dirty_count = 0;
        }
        self.set_item(item);
    }

    pub async fn sync_buf(&self, view: &TransCacheView<'worker>) {
        if self.dma_local_buf.is_some() {
            view.sync_read_buf(&self.trans_key, self.cid, &self.dma_local_buf, self.dirty_count).await;
        }
    }

    pub fn block_append_item(&mut self, view: &TransCacheView<'worker>, item: CacheReadSetItem) {
        if self.meta.count + self.dirty_count >= max_read_item_count_in_buf() {
            view.block_sync_read_buf(&self.trans_key, self.cid, &self.dma_local_buf, self.dirty_count);
            view.alloc_read_buf(&self.trans_key);

            self.meta = view.get_last_read_meta(&self.trans_key);

            #[cfg(feature = "doca_deps")]
            if self.meta.buf.is_remote_buf() && self.dma_local_buf.is_none() {
                self.dma_local_buf = Some(view.get_local_dma_buf(self.cid));
            }

            self.dirty_count = 0;
        }
    }

    pub fn block_sync_buf(&self, view: &TransCacheView<'worker>) {
        if self.dma_local_buf.is_some() {
            view.block_sync_read_buf(&self.trans_key, self.cid, &self.dma_local_buf, self.dirty_count);
        }
    }
}

pub struct WriteCacheMetaWriter {
    trans_key:     TransKey,
    cid:           u32,
    meta:          CacheMeta,
    dma_local_buf: Option<DmaLocalBuf>,
    dirty_count:   usize,
}

impl<'worker> WriteCacheMetaWriter {
    fn new(trans_key: &TransKey, cid: u32, meta: &CacheMeta) -> Self {
        Self {
            trans_key: trans_key.clone(),
            cid: cid,
            meta: meta.clone(),
            dma_local_buf: None,
            dirty_count: 0,
        }
    }

    fn set_item(&mut self, item: CacheWriteSetItem) {
        match &mut self.meta.buf {
            CacheBuf::LocalBuf(buf) => {
                let idx = self.dirty_count + self.meta.count;
                unsafe {
                    buf.set_item(idx, item);
                }
            }
            CacheBuf::RemoteBuf(_) => {
                #[cfg(feature = "doca_deps")]
                if true {
                    let idx = self.dirty_count;
                    if let Some(dma_buf) = &mut self.dma_local_buf {
                        unsafe {
                            dma_buf.set_item(idx, item);
                        }
                    }
                    return;
                }

                panic!("unsupported!");
            }
        }

        self.dirty_count += 1;
    }

    pub async fn append_item(&mut self, view: &TransCacheView<'worker>, item: CacheWriteSetItem) {
        if self.meta.count + self.dirty_count >= max_write_item_count_in_buf() {
            view.sync_write_buf(&self.trans_key, self.cid, &self.dma_local_buf, self.dirty_count).await;
            view.alloc_write_buf(&self.trans_key);

            self.meta = view.get_last_write_meta(&self.trans_key);

            #[cfg(feature = "doca_deps")]
            if self.meta.buf.is_remote_buf() && self.dma_local_buf.is_none() {
                self.dma_local_buf = Some(view.get_local_dma_buf(self.cid));
            }

            self.dirty_count = 0;
        }

        self.set_item(item);
    }

    pub async fn sync_buf(&self, view: &TransCacheView<'worker>) {
        if self.dma_local_buf.is_some() {
            view.sync_write_buf(&self.trans_key, self.cid, &self.dma_local_buf, self.dirty_count).await;
        }
    }

    pub fn block_append_item(&mut self, view: &TransCacheView<'worker>, item: CacheWriteSetItem) {
        if self.meta.count + self.dirty_count >= max_write_item_count_in_buf() {
            view.block_sync_write_buf(&self.trans_key, self.cid, &self.dma_local_buf, self.dirty_count);
            view.alloc_write_buf(&self.trans_key);

            self.meta = view.get_last_write_meta(&self.trans_key);

            #[cfg(feature = "doca_deps")]
            if self.meta.buf.is_remote_buf() && self.dma_local_buf.is_none() {
                self.dma_local_buf = Some(view.get_local_dma_buf(self.cid));
            }

            self.dirty_count = 0;
        }

        self.set_item(item);
    }

    pub fn block_sync_buf(&self, view: &TransCacheView<'worker>) {
        if self.dma_local_buf.is_some() {
            view.block_sync_write_buf(&self.trans_key, self.cid, &self.dma_local_buf, self.dirty_count);
        }
    }
}