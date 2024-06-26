//! WorkQueue module.
//!
//! - [`DOCAWorkQueue`] enables submitting jobs to DOCA libraries and track job progress
//! (supports both polling mode and event-driven mode). It introduces three main operations:
//! 1. Submission of jobs.
//! 2. Checking progress/status of submitted jobs.
//! 3. Querying job completion status.
//!
//! The trait [`ToBaseJob`] is designed to receive requests from all data-path
//! libraries since each one has its own work request.
//!
//! - [`DOCAEvent`] is an activity completion event. It is used to keep track of which
//! the submitted job has finished.

use std::{ptr::NonNull, sync::Arc};

use ffi::{doca_error, doca_event, doca_job};

use crate::{DOCAError, DOCAResult};

use super::{DOCAContext, EngineToContext};

/// The trait makes WorkQueue capable for various DOCA requests
pub trait ToBaseJob {
    /// Get the base `doca_job` from a specific job
    fn to_base(&self) -> &doca_job;
}

///Event structure defines activity completion of:
/// 1. Completion event of submitted job.
/// 2. CTX received event as a result of some external activity.
#[derive(Default)]
#[repr(C)]
pub struct DOCAEvent {
    inner: doca_event,
}

impl DOCAEvent {
    /// Get a DOCA Event Instance
    pub fn new() -> Self {
        Self {
            inner: doca_event::default(),
        }
    }

    /// Get the return value of the event
    pub fn result(&self) -> DOCAError {
        unsafe {
            // FIXME: what if DOCAError is not u32?
            let res: DOCAError = std::mem::transmute(self.inner.result.u64_ as u32);
            res
        }
    }

    /// Get the user mark
    pub fn user_mark(&self) -> u64 {
        unsafe {
            self.inner.user_data.u64_
        }
    }
}

/// a logical representation of DOCA thread of execution (non-thread-safe).
/// WorkQ is used to submit jobs to the relevant context/library (hardware offload most of the time)
/// and query the job's completion status.
/// To start submitting jobs, however, the WorkQ must be configured to accept that type of job.
/// Each WorkQ can be configured to accept any number of job types depending on how it initialized.
pub struct DOCAWorkQueue<T: EngineToContext> {
    inner: NonNull<ffi::doca_workq>,
    depth: u32,
    #[allow(dead_code)]
    pub(crate) ctx: Arc<DOCAContext<T>>,
}

impl<T: EngineToContext> Drop for DOCAWorkQueue<T> {
    fn drop(&mut self) {
        // remove the worker queue from the context
        let ret = unsafe { ffi::doca_ctx_workq_rm(self.ctx.inner_ptr(), self.inner_ptr()) };
        assert_eq!(
            ret,
            DOCAError::DOCA_SUCCESS,
            "failed to remove workq from context"
        );
        unsafe { ffi::doca_workq_destroy(self.inner_ptr()) };

        // Show drop order only in `debug` mode
        #[cfg(debug_assertions)]
        println!("DOCA WorkQ is dropped!");
    }
}

impl<T: EngineToContext> DOCAWorkQueue<T> {
    /// Creates empty DOCA WorkQ object with default attributes.
    pub fn new(depth: u32, ctx: &Arc<DOCAContext<T>>) -> DOCAResult<Self> {
        let mut workq: *mut ffi::doca_workq = std::ptr::null_mut();
        let ret = unsafe { ffi::doca_workq_create(depth, &mut workq as *mut _) };

        if ret != DOCAError::DOCA_SUCCESS {
            return Err(ret);
        }

        let res = Self {
            inner: unsafe { NonNull::new_unchecked(workq) },
            depth: depth,
            ctx: ctx.clone(),
        };

        // add the myself to the context
        let ret = unsafe { ffi::doca_ctx_workq_add(ctx.inner_ptr(), res.inner_ptr()) };

        if ret != DOCAError::DOCA_SUCCESS {
            return Err(ret);
        }

        Ok(res)
    }

    /// Add the job into the work queue
    pub fn submit<Job: ToBaseJob>(&mut self, job: &Job) -> DOCAResult<()> {
        let ret = unsafe { ffi::doca_workq_submit(self.inner_ptr(), job.to_base() as *const _) };
        if ret != DOCAError::DOCA_SUCCESS {
            return Err(ret);
        }

        Ok(())
    }

    /// Check whether there's a job finished in the work queue
    #[inline]
    pub fn poll_completion(&mut self) -> DOCAResult<DOCAEvent> {
        let mut event = DOCAEvent::new();
        let ret = unsafe {
            ffi::doca_workq_progress_retrieve(
                self.inner_ptr(),
                &mut event.inner as *mut _,
                ffi::DOCA_WORKQ_RETRIEVE_FLAGS_NONE as i32,
            )
        };
        if ret != DOCAError::DOCA_SUCCESS {
            return Err(ret);
        }
        Ok(event)
    }

    /// Return the full event
    #[inline]
    pub fn progress_retrieve(&mut self) -> (DOCAEvent, doca_error) {
        let mut event = DOCAEvent::new();
        let ret = unsafe {
            ffi::doca_workq_progress_retrieve(
                self.inner_ptr(),
                &mut event.inner as *mut _,
                ffi::DOCA_WORKQ_RETRIEVE_FLAGS_NONE as i32,
            )
        };
        (event, ret)
    }

    /// Get the inner pointer of the DOCA WorkQ.
    pub unsafe fn inner_ptr(&self) -> *mut ffi::doca_workq {
        self.inner.as_ptr()
    }

    /// Get the max depth of the work queue
    pub fn depth(&self) -> u32 {
        self.depth
    }
}

mod tests {
    #[test]
    fn test_worker_queue_create() {
        use crate::context::DOCAContext;
        use crate::dma::DMAEngine;
        use crate::DOCAWorkQueue;

        let device = crate::device::devices()
            .unwrap()
            .get(0)
            .unwrap()
            .open()
            .unwrap();

        let dma = DMAEngine::new().unwrap();

        let ctx = DOCAContext::new(&dma, vec![device]).unwrap();

        let workq = DOCAWorkQueue::new(1, &ctx).unwrap();

        assert_eq!(workq.depth(), 1);
    }
}
