//! Wrapper for DOCA DMA related. It provides
//! the ability of copying memory using direct memory access (DMA).
//!
//! The core structs include [`DOCADMAJob`], [`DMAEngine`].
//!
//! It basically contains two core structs:
//! - [`DOCADMAJob`]: The DMA request of DOCA. It implements the trait [`ToBaseJob`],
//! which makes it capable for being submitted to the work queue.
//!
//! - [`DMAEngine`]: The DMA Engine of DOCA. Users should create an instance of the engine and
//! execute DMA requests based on the engine.
//!
//! # Examples
//!
//! Create a DMAEngine and get the Context of the engine.
//!
//! ``` rust, no_run
//! use doca::DMAEngine;
//! use doca::context::DOCAContext;
//!
//! let dma = DMAEngine::new().unwrap();
//! let device = doca::device::open_device_with_pci("17:00.0").unwrap();
//!
//! let ctx = DOCAContext::new(&dma, vec![device]).unwrap();
//! ```
//!

use std::ptr::NonNull;
use std::sync::Arc;

use crate::context::work_queue::ToBaseJob;
use crate::context::EngineToContext;
use crate::{DOCABuffer, DOCAError, DOCAResult};

pub use crate::context::work_queue::{DOCAEvent, DOCAWorkQueue};
pub use crate::context::DOCAContext;

/// DOCA DMA engine instance
pub struct DMAEngine {
    inner: NonNull<ffi::doca_dma>,
}

impl Drop for DMAEngine {
    fn drop(&mut self) {
        let ret = unsafe { ffi::doca_dma_destroy(self.inner_ptr()) };
        if ret != DOCAError::DOCA_SUCCESS {
            panic!("Failed to destory dma engine!");
        }

        // Show drop order only in `debug` mode
        #[cfg(debug_assertions)]
        println!("DMA Engine is dropped!");
    }
}

/// Implementation `EngineToContext` Trait for DMA Engine
impl EngineToContext for DMAEngine {
    unsafe fn to_ctx(&self) -> *mut ffi::doca_ctx {
        ffi::doca_dma_as_ctx(self.inner_ptr())
    }
}

impl DMAEngine {
    /// Create a DOCA DMA instance.
    pub fn new() -> DOCAResult<Arc<Self>> {
        let mut dma: *mut ffi::doca_dma = std::ptr::null_mut();
        let ret = unsafe { ffi::doca_dma_create(&mut dma as *mut _) };

        if ret != DOCAError::DOCA_SUCCESS {
            return Err(ret);
        }

        Ok(Arc::new(Self {
            inner: unsafe { NonNull::new_unchecked(dma) },
        }))
    }

    /// Get the inner pointer of the DOCA DMA instance.
    pub unsafe fn inner_ptr(&self) -> *mut ffi::doca_dma {
        self.inner.as_ptr()
    }
}

/// A DOCA DMA request
pub struct DOCADMAJob {
    pub(crate) inner: ffi::doca_dma_job_memcpy,

    // FIXME: do we really need to record the context here?
    #[allow(dead_code)]
    ctx: Arc<DOCAContext<DMAEngine>>,

    src_buff: Option<DOCABuffer>,
    dst_buff: Option<DOCABuffer>,
}

/// Implementation of `ToBaseJob` Trait
impl ToBaseJob for DOCADMAJob {
    fn to_base(&self) -> &ffi::doca_job {
        &self.inner.base
    }
}

impl DOCADMAJob {
    /// Set request's destination buffer
    pub fn set_dst(&mut self, buf: DOCABuffer) -> &mut Self {
        unsafe { self.inner.dst_buff = buf.inner_ptr() };
        self.dst_buff = Some(buf);
        self
    }

    /// Set request's source buffer
    pub fn set_src(&mut self, buf: DOCABuffer) -> &mut Self {
        unsafe { self.inner.src_buff = buf.inner_ptr() };
        self.src_buff = Some(buf);
        self
    }

    /// Set the data pointer of the src buffer
    #[inline]
    pub fn set_src_data(&mut self, offset: usize, payload: usize) {
        if let Some(f) = self.src_buff.as_mut() {
            unsafe { f.set_data(offset, payload).expect("doca fail to set src data!") };
        }
    }

    /// Set the data pointer of the dst buffer
    #[inline]
    pub fn set_dst_data(&mut self, offset: usize, payload: usize) {
        if let Some(f) = self.dst_buff.as_mut() {
            unsafe { f.set_data(offset, payload).expect("doca fail to set dst data!") };
       }
    }

    /// Set mark for user data
    #[inline]
    pub fn set_user_data(&mut self, user_data: u64) -> &mut Self {
        self.inner.base.user_data.u64_ = user_data;
        self
    }

    /// Set request's based context
    fn set_ctx(&mut self) -> &mut Self {
        unsafe { self.inner.base.ctx = self.ctx.inner_ptr() };
        self
    }

    /// Set request's flags
    fn set_flags(&mut self) -> &mut Self {
        self.inner.base.flags = ffi::DOCA_JOB_FLAGS_NONE as i32;
        self
    }

    /// Set request's type
    fn set_type(&mut self) -> &mut Self {
        self.inner.base.type_ = ffi::DOCA_DMA_JOB_MEMCPY as i32;
        self
    }
}

/// A DOCA DMA request
pub struct DOCADMAReusableJob {
    pub(crate) inner_read: ffi::doca_dma_job_memcpy,
    pub(crate) inner_write: ffi::doca_dma_job_memcpy,

    write: bool,

    // FIXME: do we really need to record the context here?
    #[allow(dead_code)]
    ctx: Arc<DOCAContext<DMAEngine>>,

    local_buf: Option<DOCABuffer>,
    remote_buf: Option<DOCABuffer>,
}

/// Implementation of `ToBaseJob` Trait
impl ToBaseJob for DOCADMAReusableJob {
    fn to_base(&self) -> &ffi::doca_job {
        if self.write {
            &self.inner_write.base
        } else {
            &self.inner_read.base
        }
    }
}

impl DOCADMAReusableJob {
    /// Set request's destination buffer
    pub fn set_local(&mut self, buf: DOCABuffer) -> &mut Self {
        unsafe { self.inner_read.dst_buff = buf.inner_ptr() };
        unsafe { self.inner_write.src_buff = buf.inner_ptr() };
        self.local_buf = Some(buf);
        self
    }

    /// Set request's source buffer
    pub fn set_remote(&mut self, buf: DOCABuffer) -> &mut Self {
        unsafe { self.inner_read.src_buff = buf.inner_ptr() };
        unsafe { self.inner_write.dst_buff = buf.inner_ptr() };
        self.remote_buf = Some(buf);
        self
    }

    /// Set the data pointer of the src buffer
    #[inline]
    pub fn set_local_data(&mut self, offset: usize, payload: usize) {
        if let Some(f) = self.local_buf.as_mut() {
            unsafe { f.set_data(offset, payload).expect("doca fail to set local data!") };
        }
    }

    /// Set the data pointer of the dst buffer
    #[inline]
    pub fn set_remote_data(&mut self, offset: usize, payload: usize) {
        if let Some(f) = self.remote_buf.as_mut() {
            unsafe { f.set_data(offset, payload).expect("doca fail to set remote data!") };
       }
    }

    /// Set mark for user data
    #[inline]
    pub fn set_user_data_write(&mut self, user_data: u64, write: bool) {
        self.write = write;
        if write {
            self.inner_write.base.user_data.u64_ = user_data;
        } else {
            self.inner_read.base.user_data.u64_ = user_data;
        }
    }

    /// Set request's based context
    fn set_ctx(&mut self) -> &mut Self {
        unsafe { self.inner_read.base.ctx = self.ctx.inner_ptr() };
        unsafe { self.inner_write.base.ctx = self.ctx.inner_ptr() };
        self
    }

    /// Set request's flags
    fn set_flags(&mut self) -> &mut Self {
        self.inner_read.base.flags = ffi::DOCA_JOB_FLAGS_NONE as i32;
        self.inner_write.base.flags = ffi::DOCA_JOB_FLAGS_NONE as i32;
        self
    }

    /// Set request's type
    fn set_type(&mut self) -> &mut Self {
        self.inner_read.base.type_ = ffi::DOCA_DMA_JOB_MEMCPY as i32;
        self.inner_write.base.type_ = ffi::DOCA_DMA_JOB_MEMCPY as i32;
        self
    }
}

impl DOCAWorkQueue<DMAEngine> {
    /// Create a DMA job
    pub fn create_dma_job(&self, src_buf: DOCABuffer, dst_buf: DOCABuffer) -> DOCADMAJob {
        let mut res = DOCADMAJob {
            inner: Default::default(),
            ctx: self.ctx.clone(),
            src_buff: None,
            dst_buff: None,
        };
        res.set_ctx()
            .set_flags()
            .set_src(src_buf)
            .set_dst(dst_buf)
            .set_type();
        res
    }

    /// Create a DMA job 
    pub fn create_dma_reusable_job(&self, local_buf: DOCABuffer, remote_buf: DOCABuffer) -> DOCADMAReusableJob {
        let mut res = DOCADMAReusableJob {
            inner_read: Default::default(),
            inner_write: Default::default(),
            write: false,
            ctx: self.ctx.clone(),
            local_buf: None,
            remote_buf: None,
        };
        res.set_ctx()
            .set_flags()
            .set_local(local_buf)
            .set_remote(remote_buf)
            .set_type();
        res
    }
}

mod tests {

    #[test]
    fn test_create_dma_job() {
        use super::*;
        use crate::dma::DMAEngine;
        use crate::*;
        use std::ptr::NonNull;

        let device = devices().unwrap().get(0).unwrap().open().unwrap();

        let dma = DMAEngine::new().unwrap();

        let ctx = DOCAContext::new(&dma, vec![device]).unwrap();

        let workq = DOCAWorkQueue::new(1, &ctx).unwrap();

        // create buffers
        let src_mmap = Arc::new(DOCAMmap::new().unwrap());
        let dst_mmap = Arc::new(DOCAMmap::new().unwrap());
        let inv = BufferInventory::new(1024).unwrap();

        let test_len = 64;
        let mut dpu_buffer = vec![0u8; test_len].into_boxed_slice();
        let mut dpu_buffer_1 = vec![0u8; test_len].into_boxed_slice();

        let raw_pointer = RawPointer {
            inner: NonNull::new(dpu_buffer.as_mut_ptr() as _).unwrap(),
            payload: test_len,
        };

        let raw_pointer_1 = RawPointer {
            inner: NonNull::new(dpu_buffer_1.as_mut_ptr() as _).unwrap(),
            payload: test_len,
        };

        let registered_memory = DOCARegisteredMemory::new(&src_mmap, raw_pointer).unwrap();
        let src_buf = registered_memory.to_buffer(&inv).unwrap();

        let registered_memory = DOCARegisteredMemory::new(&dst_mmap, raw_pointer_1).unwrap();
        let dst_buf = registered_memory.to_buffer(&inv).unwrap();

        let _ = workq.create_dma_job(src_buf, dst_buf);
    }

    #[test]
    fn test_dma_context() {
        use crate::dma::DMAEngine;
        use crate::dma::DOCAContext;

        let device = crate::device::devices()
            .unwrap()
            .get(0)
            .unwrap()
            .open()
            .unwrap();

        let dma = DMAEngine::new().unwrap();
        let ctx = DOCAContext::new(&dma, vec![device]).unwrap();
        unsafe { assert_eq!(ctx.engine.inner_ptr(), dma.inner_ptr()) };
    }
}
