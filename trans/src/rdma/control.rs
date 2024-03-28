#![allow(unused)]
use std::alloc::Layout;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use errno::errno;
use libc::free;
use libc::{malloc, memalign};
use ll_alloc::LockedHeap;
use rdma_sys::*;

use super::rcconn::RdmaRcConn;

use crate::{TransError, TransResult, MAX_RECV_SIZE, MAX_SEND_SIZE, NPAGES, PEERNUMS, PORTS};

#[derive(Clone, Copy, Debug)]
struct RemoteMeta {
    peer_id: u64,
    raddr: u64,
    rid: u32,
}

pub struct RdmaControl<'a> {
    self_id: u64,
    listen_fd: *mut rdma_cm_id,
    connections: HashMap<u64, Arc<Mutex<RdmaRcConn<'a>>>>,
    lm: *mut u8,
    allocator: Arc<LockedHeap>,
}

impl<'a> RdmaControl<'a> {
    pub fn new(self_id: u64) -> Self {
        let mr_length = 4096 * NPAGES as usize;
        let lm = unsafe { memalign(4096, mr_length) };
        let allocator = Arc::new(unsafe { LockedHeap::new(lm as _, (NPAGES * 4096) as usize) });

        Self {
            self_id: self_id,
            listen_fd: std::ptr::null_mut(),
            connections: HashMap::new(),
            lm: lm as *mut u8,
            allocator: allocator,
        }
    }

    #[inline]
    fn default_init_attr() -> ibv_qp_init_attr {
        let mut init_attr = unsafe { std::mem::zeroed::<ibv_qp_init_attr>() };
        init_attr.cap.max_send_wr = MAX_SEND_SIZE as _;
        init_attr.cap.max_recv_wr = MAX_RECV_SIZE as _;
        init_attr.cap.max_send_sge = 1;
        init_attr.cap.max_recv_sge = 1;
        init_attr.cap.max_inline_data = 16;
        init_attr.sq_sig_all = 0;

        init_attr
    }

    pub fn init(&mut self, server_ip: &str, port: &str) {
        let mut hints = unsafe { std::mem::zeroed::<rdma_addrinfo>() };
        let mut res: *mut rdma_addrinfo = std::ptr::null_mut();
        hints.ai_flags = RAI_PASSIVE.try_into().unwrap();
        hints.ai_port_space = rdma_port_space::RDMA_PS_TCP.try_into().unwrap();
        let mut ret = unsafe {
            rdma_getaddrinfo(
                server_ip.as_ptr().cast(),
                port.as_ptr().cast(),
                &hints,
                &mut res,
            )
        };

        if ret != 0 {
            println!("rdma_getaddrinfo");
        }

        let mut listen_id = std::ptr::null_mut();

        let mut init_attr = Self::default_init_attr();
        ret = unsafe { rdma_create_ep(&mut listen_id, res, std::ptr::null_mut(), &mut init_attr) };
        // Check to see if we got inline data allowed or not
        if ret != 0 {
            println!("rdma_create_ep");
            unsafe {
                rdma_freeaddrinfo(res);
            }
        }
        ret = unsafe { rdma_listen(listen_id, 10) };

        println!("rdma listen sucessfully");
        if ret != 0 {
            println!("rdma_listen");
            unsafe {
                rdma_destroy_ep(listen_id);
            }
        }

        self.listen_fd = listen_id;
    }

    pub fn connect(&mut self, peer_id: u64, ip: &str, port: &str) -> TransResult<()> {
        let mut hints = unsafe { std::mem::zeroed::<rdma_addrinfo>() };
        let mut res: *mut rdma_addrinfo = std::ptr::null_mut();

        hints.ai_port_space = rdma_port_space::RDMA_PS_TCP as i32;
        let mut ret =
            unsafe { rdma_getaddrinfo(ip.as_ptr().cast(), port.as_ptr().cast(), &hints, &mut res) };

        if ret != 0 {
            println!("rdma_getaddrinfo");
            return Err(TransError::TransRdmaError);
        }

        let mut attr = Self::default_init_attr();
        let mut id: *mut rdma_cm_id = std::ptr::null_mut();

        ret = unsafe { rdma_create_ep(&mut id, res, std::ptr::null_mut(), &mut attr) };

        if ret != 0 {
            println!("rdma_create_ep");
            return Err(TransError::TransRdmaError);
        }

        // bind mr and send meta
        // let mr_length = 4096 * NPAGES as usize;
        // let lm = unsafe { memalign(4096, mr_length) };
        let mr_length = 4096 * NPAGES as usize;
        let lm = self.lm as _;
        let access = ibv_access_flags::IBV_ACCESS_LOCAL_WRITE.0
            | ibv_access_flags::IBV_ACCESS_REMOTE_READ.0
            | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE.0;
        let lmr = unsafe { ibv_reg_mr((*id).pd, lm, mr_length, access as _) };

        let send_recv_layout = Layout::from_size_align(
            std::mem::size_of::<RemoteMeta>(),
            std::mem::align_of::<RemoteMeta>(),
        )
        .unwrap();
        let send_addr = unsafe { self.allocator.alloc(send_recv_layout) };

        unsafe {
            *(send_addr as *mut RemoteMeta) = RemoteMeta {
                peer_id: self.self_id,
                raddr: unsafe { (*lmr).addr } as u64,
                rid: unsafe { (*lmr).rkey },
            };
        }

        let recv_addr = unsafe { self.allocator.alloc(send_recv_layout) };
        ret = unsafe {
            rdma_post_recv(
                id,
                recv_addr as _,
                recv_addr as _,
                std::mem::size_of::<RemoteMeta>(),
                lmr,
            )
        };
        if ret != 0 {
            println!("rdma_post_recv");
            return Err(TransError::TransRdmaError);
        }

        ret = unsafe { rdma_connect(id, std::ptr::null_mut()) };
        if ret != 0 {
            println!("rdma_connect, errno = {}", errno());
            return Err(TransError::TransRdmaError);
        }

        ret = unsafe {
            rdma_post_send(
                id,
                send_addr as _,
                send_addr as _,
                std::mem::size_of::<RemoteMeta>(),
                lmr,
                ibv_send_flags::IBV_SEND_SIGNALED.0 as _,
            )
        };

        if ret != 0 {
            println!("rdma_post_send");
            return Err(TransError::TransRdmaError);
        }

        let mut wc = unsafe { std::mem::zeroed::<ibv_wc>() };
        while ret == 0 {
            ret = unsafe { rdma_get_send_comp(id, &mut wc) };
        }
        if ret < 0 {
            println!("rdma_get_send_comp");
            return Err(TransError::TransRdmaError);
        }

        ret = 0;
        while ret == 0 {
            ret = unsafe { rdma_get_recv_comp(id, &mut wc) };
        }

        println!("connect successfully!");
        let recv_data = unsafe { *(recv_addr as *mut RemoteMeta) };
        println!(
            "{:}:{:}:{:}",
            recv_data.peer_id, recv_data.raddr, recv_data.rid
        );

        let connection = RdmaRcConn::new(
            id,
            lm as *mut u8,
            lmr,
            recv_data.raddr,
            recv_data.rid,
            &self.allocator,
        );

        unsafe {
            self.allocator.dealloc(send_addr, send_recv_layout);
            self.allocator.dealloc(recv_addr, send_recv_layout);
        }

        self.connections
            .insert(peer_id, Arc::new(Mutex::new(connection)));

        Ok(())
    }

    pub fn get_connection(&self, peer_id: u64) -> Arc<Mutex<RdmaRcConn<'a>>> {
        self.connections.get(&peer_id).unwrap().clone()
    }

    fn accept(&mut self) {
        let mut id: *mut rdma_cm_id = std::ptr::null_mut();
        let mut ret = unsafe { rdma_get_request(self.listen_fd, &mut id) };
        if ret != 0 {
            println!("rdma_get_request");
            return;
        }

        let mut init_attr = Self::default_init_attr();

        let mut qp_attr = unsafe { std::mem::zeroed::<ibv_qp_attr>() };
        ret = unsafe {
            ibv_query_qp(
                (*id).qp,
                &mut qp_attr,
                ibv_qp_attr_mask::IBV_QP_CAP.0.try_into().unwrap(),
                &mut init_attr,
            )
        };

        // bind mr and send meta
        // let mr_length = 4096 * NPAGES as usize;
        // let lm = unsafe { memalign(4096, mr_length) };
        let mr_length = 4096 * NPAGES as usize;
        let lm = self.lm as _;
        let access = ibv_access_flags::IBV_ACCESS_LOCAL_WRITE.0
            | ibv_access_flags::IBV_ACCESS_REMOTE_READ.0
            | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE.0;
        let lmr = unsafe { ibv_reg_mr((*id).pd, lm, mr_length, access as _) };

        let send_recv_layout = Layout::from_size_align(
            std::mem::size_of::<RemoteMeta>(),
            std::mem::align_of::<RemoteMeta>(),
        )
        .unwrap();
        let send_addr = unsafe { self.allocator.alloc(send_recv_layout) };

        unsafe {
            *(send_addr as *mut RemoteMeta) = RemoteMeta {
                peer_id: self.self_id,
                raddr: unsafe { (*lmr).addr } as u64,
                rid: unsafe { (*lmr).rkey },
            };
        }

        let recv_addr = unsafe { self.allocator.alloc(send_recv_layout) };
        ret = unsafe {
            rdma_post_recv(
                id,
                recv_addr as _,
                recv_addr as _,
                std::mem::size_of::<RemoteMeta>(),
                lmr,
            )
        };
        if ret != 0 {
            println!("rdma_post_recv");
        }

        ret = unsafe { rdma_accept(id, std::ptr::null_mut()) };
        if ret != 0 {
            println!("rdma_accept");
            return;
        }

        ret = unsafe {
            rdma_post_send(
                id,
                send_addr as _,
                send_addr as _,
                std::mem::size_of::<RemoteMeta>(),
                lmr,
                ibv_send_flags::IBV_SEND_SIGNALED.0 as _,
            )
        };

        if ret != 0 {
            println!("rdma_post_send");
        }

        let mut wc = unsafe { std::mem::zeroed::<ibv_wc>() };
        while ret == 0 {
            ret = unsafe { rdma_get_send_comp(id, &mut wc) };
        }
        if ret < 0 {
            println!("rdma_get_send_comp");
        }

        ret = 0;
        while ret == 0 {
            ret = unsafe { rdma_get_recv_comp(id, &mut wc) };
        }

        println!("accept successfully!");
        let recv_data = unsafe { *(recv_addr as *mut RemoteMeta) };
        println!(
            "{:}:{:}:{:}",
            recv_data.peer_id, recv_data.raddr, recv_data.rid
        );

        let connection = RdmaRcConn::new(
            id,
            lm as *mut u8,
            lmr,
            recv_data.raddr,
            recv_data.rid,
            &self.allocator,
        );

        unsafe {
            self.allocator.dealloc(send_addr, send_recv_layout);
            self.allocator.dealloc(recv_addr, send_recv_layout);
        }

        self.connections
            .insert(recv_data.peer_id, Arc::new(Mutex::new(connection)));
    }

    pub fn listen_task(&mut self) {
        while self.connections.len() < (PEERNUMS - 1) as usize {
            self.accept();
        }
    }

    pub fn get_allocator(&self) -> Arc<LockedHeap> {
        return self.allocator.clone();
    }
}

impl<'a> Drop for RdmaControl<'a> {
    fn drop(&mut self) {
        unsafe {
            free(self.lm as *mut _);
        }
    }
}
