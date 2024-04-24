//! Wrapper for DOCA Comm Channel between host and dpu
//! the ability of send reqs between host and dpu using pcie switch

use std::time::Duration;
use std::{sync::Arc, thread::sleep};
use std::ptr::NonNull;
use ffi::doca_error;

use crate::RawPointer;

use crate::{device::DevRepContext, DOCAError, DevContext};

/// DOCA Comm Channel
pub struct CommChannel {
    inner: NonNull<ffi::doca_comm_channel_ep_t>,
    peer_addr: NonNull<ffi::doca_comm_channel_addr_t>,
    dev: Arc<DevContext>,
    dev_rep: Option<Arc<DevRepContext>>,
}

impl Drop for CommChannel {
    fn drop(&mut self) {
        unsafe { ffi::doca_comm_channel_ep_disconnect(self.inner_ptr(), self.peer_addr.as_ptr()); }
        
        let ret = unsafe { ffi::doca_comm_channel_ep_destroy(self.inner_ptr()) };
        if ret != DOCAError::DOCA_SUCCESS {
            panic!("Failed to destroy comm channel endpoint!");
        }
    }
}

impl CommChannel {

    /// Create a Comm Channel Server Instance
    pub fn create_server(server_name: &str, dev: &Arc<DevContext>, dev_rep: &Arc<DevRepContext>) -> Arc<Self> {
        let mut ep: *mut ffi::doca_comm_channel_ep_t = std::ptr::null_mut();
        let mut peer_addr: *mut ffi::doca_comm_channel_addr_t = std::ptr::null_mut();

        let mut ret = unsafe { ffi::doca_comm_channel_ep_create(&mut ep) };
        if ret != DOCAError::DOCA_SUCCESS {
            panic!("Failed to create Comm Channel endpoint");
        }

        ret = unsafe { ffi::doca_comm_channel_ep_set_device(ep, dev.inner_ptr()) };
        if ret != DOCAError::DOCA_SUCCESS {
            panic!("Failed to set device property");
        }

        ret = unsafe { ffi::doca_comm_channel_ep_set_max_msg_size(ep, 4080) };
        if ret != DOCAError::DOCA_SUCCESS {
            panic!("Failed to set max_msg_size property");
        }

        ret = unsafe { ffi::doca_comm_channel_ep_set_send_queue_size(ep, 10) };
        if ret != DOCAError::DOCA_SUCCESS {
            panic!("Failed to set snd_queue_size property");
        }

        ret = unsafe { ffi::doca_comm_channel_ep_set_recv_queue_size(ep, 10) };
        if ret != DOCAError::DOCA_SUCCESS {
            panic!("Failed to set rcv_queue_size property");
        }

        ret = unsafe { ffi::doca_comm_channel_ep_set_device_rep(ep, dev_rep.inner_ptr()) };
        if ret != DOCAError::DOCA_SUCCESS {
            panic!("Failed to set rcv_queue_size property");
        }

        /* Start listen for new connections */
        ret = unsafe { ffi::doca_comm_channel_ep_listen(ep, server_name.as_ptr() as _) };
        if ret != DOCAError::DOCA_SUCCESS {
            panic!("Comm Channel server couldn't start listening");
        }

        let mut temp_buf= [0u8; 10];
        let mut len: usize = 2;

        loop {
            let res = unsafe { ffi::doca_comm_channel_ep_recvfrom(ep, &mut temp_buf as *mut _ as _, &mut len, 0, &mut peer_addr) };
            if res == DOCAError::DOCA_SUCCESS {
                break;
            }
            if res != DOCAError::DOCA_ERROR_AGAIN {
                panic!("unexpected {:?} ", res);
            }

            sleep(Duration::from_millis(1));
        }

        Arc::new(Self {
            inner: NonNull::new(ep).unwrap(),
            peer_addr: NonNull::new(peer_addr).unwrap(),
            dev: dev.clone(),
            dev_rep: Some(dev_rep.clone())
        })
    }

    /// Create a Comm Channel Client Instance
    pub fn create_client(server_name: &str, dev: &Arc<DevContext>,) -> Arc<Self> {
        let mut ep: *mut ffi::doca_comm_channel_ep_t = std::ptr::null_mut();
        let mut peer_addr: *mut ffi::doca_comm_channel_addr_t = std::ptr::null_mut();

        let mut ret = unsafe { ffi::doca_comm_channel_ep_create(&mut ep) };
        if ret != DOCAError::DOCA_SUCCESS {
            panic!("Failed to create Comm Channel endpoint");
        }

        ret = unsafe { ffi::doca_comm_channel_ep_set_device(ep, dev.inner_ptr()) };
        if ret != DOCAError::DOCA_SUCCESS {
            panic!("Failed to set device property");
        }

        ret = unsafe { ffi::doca_comm_channel_ep_set_max_msg_size(ep, 4080) };
        if ret != DOCAError::DOCA_SUCCESS {
            panic!("Failed to set max_msg_size property");
        }

        ret = unsafe { ffi::doca_comm_channel_ep_set_send_queue_size(ep, 10) };
        if ret != DOCAError::DOCA_SUCCESS {
            panic!("Failed to set snd_queue_size property");
        }

        ret = unsafe { ffi::doca_comm_channel_ep_set_recv_queue_size(ep, 10) };
        if ret != DOCAError::DOCA_SUCCESS {
            panic!("Failed to set rcv_queue_size property");
        }

        ret = unsafe{ ffi::doca_comm_channel_ep_connect(ep, server_name.as_ptr() as _, &mut peer_addr) };
        if ret != DOCAError::DOCA_SUCCESS {
            panic!("Couldn't establish a connection with the server");
        }

        loop {
            let res = unsafe { ffi::doca_comm_channel_peer_addr_update_info(peer_addr) };
            if res != DOCAError::DOCA_ERROR_CONNECTION_INPROGRESS {
                break;
            }
            sleep(Duration::from_millis(1));
        }

        let temp_buf= [1u8; 10];
        let len: usize = 2;

        loop {
            let res = unsafe { ffi::doca_comm_channel_ep_sendto(ep, &temp_buf as *const _ as _, len, 0, peer_addr) };
            if res == DOCAError::DOCA_SUCCESS {
                break;
            }
            if res != DOCAError::DOCA_ERROR_AGAIN {
                panic!("unexpected");
            }

            sleep(Duration::from_millis(1));
        }

        Arc::new(Self {
            inner: NonNull::new(ep).unwrap(),
            peer_addr: NonNull::new(peer_addr).unwrap(),
            dev: dev.clone(),
            dev_rep: None
        })
    }

    /// block send req
    pub fn block_send_req(&self, raw: &RawPointer) {
        loop {
            let res = unsafe { ffi::doca_comm_channel_ep_sendto(self.inner_ptr(), raw.inner.as_ptr(), raw.payload, 0, self.peer_addr.as_ptr()) };
            if res == DOCAError::DOCA_SUCCESS {
                break;
            }
            if res != DOCAError::DOCA_ERROR_AGAIN {
                panic!("unexpected");
            }

            sleep(Duration::from_millis(1));
        }
    }

    /// block recv req
    pub fn block_recv_req(&self, raw: &mut RawPointer) {
        let mut peer_addr: *mut ffi::doca_comm_channel_addr_t = std::ptr::null_mut();
        loop {
            let res = unsafe { ffi::doca_comm_channel_ep_recvfrom(self.inner_ptr(), raw.inner.as_ptr(), &mut raw.payload, 0, &mut peer_addr) };
            if res == DOCAError::DOCA_SUCCESS {
                break;
            }
            if res != DOCAError::DOCA_ERROR_AGAIN {
                panic!("unexpected");
            }

            sleep(Duration::from_millis(1));
        }
    }

    /// recv req
    pub fn recv_req(&self, raw: &mut RawPointer) -> doca_error {
        let mut peer_addr: *mut ffi::doca_comm_channel_addr_t = std::ptr::null_mut();
        let res = unsafe { ffi::doca_comm_channel_ep_recvfrom(self.inner_ptr(), raw.inner.as_ptr(), &mut raw.payload, 0, &mut peer_addr) };
        res
    }

    /// Get the inner pointer of the DOCA COMM CHANNEL
    pub unsafe fn inner_ptr(&self) -> *mut ffi::doca_comm_channel_ep_t {
        self.inner.as_ptr()
    }
}