use crate::MAX_DMA_BUF_PER_ROUTINE;
use crate::MAX_DMA_BUF_SIZE;
use crate::MAX_DMA_BUF_REMOTE;

#[derive(Clone, Copy)]
pub struct DmaLocalBuf {
    saddr: usize,
    off:   usize,
    len:   usize,
}

impl DmaLocalBuf {
    pub fn new(saddr: usize, off: usize, len: usize) -> Self {
        Self {
            saddr: saddr,
            off:   off,
            len:   len
        }
    }

    pub fn get_len(&self) -> usize {
        self.len  
    }

    pub fn get_off(&self) -> usize {
        self.off
    }

    pub unsafe fn get_const_slice<ITEM: Clone>(&self, count: usize) -> &'static [ITEM] {
        let demand = std::mem::size_of::<ITEM>() * count;
        assert!(demand <= self.len);

        std::slice::from_raw_parts::<ITEM>((self.saddr + self.off) as _, count)
    }

    pub unsafe fn get_mut_slice<ITEM: Clone>(&mut self, count: usize) -> &'static mut [ITEM] {
        let demand = std::mem::size_of::<ITEM>() * count;
        assert!(demand <= self.len);

        std::slice::from_raw_parts_mut::<ITEM>((self.saddr + self.off) as _, count)
    }
}

pub struct DmaLocalBufAllocator {
    dma_buf_pools: Vec<Vec<DmaLocalBuf>>,
    dma_buf_heads: Vec<usize>
}

impl DmaLocalBufAllocator {
    pub fn new(coroutine_num: u32, addr: usize, capacity: usize) -> Self {
        let demand = coroutine_num as usize * MAX_DMA_BUF_PER_ROUTINE * MAX_DMA_BUF_SIZE;
        assert!(demand <= capacity);
        let mut dma_buf_pools = Vec::new();
        let mut dma_buf_heads = Vec::new();

        for i in 0..coroutine_num as usize {
            let mut buf_pool = Vec::new();
            for j in 0..MAX_DMA_BUF_PER_ROUTINE {
                let off = i * (MAX_DMA_BUF_PER_ROUTINE * MAX_DMA_BUF_SIZE) + j * MAX_DMA_BUF_SIZE;
                buf_pool.push(DmaLocalBuf::new(addr, off, MAX_DMA_BUF_SIZE));
            }
            dma_buf_pools.push(buf_pool);
            dma_buf_heads.push(0);
        }

        Self {
            dma_buf_pools: dma_buf_pools,
            dma_buf_heads: dma_buf_heads,
        }
    }

    #[inline]
    pub fn get_local_buf(&mut self, cid: u32) -> DmaLocalBuf {
        let buf = self.dma_buf_pools[cid as usize][self.dma_buf_heads[cid as usize]];
        self.dma_buf_heads[cid as usize] += 1;
        if self.dma_buf_heads[cid as usize] > MAX_DMA_BUF_PER_ROUTINE {
            self.dma_buf_heads[cid as usize] = 0;
        }
        buf
    }
}

pub struct DmaRemoteBuf {
    off:   usize,
    len:   usize,
}

impl DmaRemoteBuf {
    pub fn new(off: usize, len: usize) -> Self {
        Self {
            off: off,
            len: len,
        }
    }

    #[inline]
    pub fn get_off(&self) -> usize {
        self.off
    }
}

pub struct DmaRemoteBufAllocator {
    recycled: Vec<DmaRemoteBuf>,
}

impl DmaRemoteBufAllocator {
    pub fn new() -> Self {
        let mut recyled = Vec::new();
        for i in 0..MAX_DMA_BUF_REMOTE {
            recyled.push(DmaRemoteBuf::new(i * MAX_DMA_BUF_SIZE, MAX_DMA_BUF_SIZE));
        }
        Self {
            recycled: recyled
        }
    }
    pub fn alloc_remote_buf(&mut self) -> DmaRemoteBuf {
        self.recycled.pop().unwrap()
    }

    pub fn dealloc_remote_buf(&mut self, buf: DmaRemoteBuf) {
        self.recycled.push(buf);
    }
}