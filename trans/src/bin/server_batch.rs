use std::sync::Arc;
use std::sync::Mutex;

use rdma_sys::ibv_wr_opcode::IBV_WR_SEND;
use trans::rdma::control::RdmaControl;
use trans::rdma::connection::RdmaRcConn;
use trans::rdma::RdmaRecvCallback;

use rdma_sys::*;

pub struct AddRequest {
    a : u8,
    b : u8,
}

pub struct AddResponse {
    sum : u8
}

struct AddRpcProcess<'a> {
    conn: Arc<RdmaRcConn<'a>>,
}
impl<'a> RdmaRecvCallback for AddRpcProcess<'a> {
    fn rdma_recv_handler(&self, msg: *mut u8) {
        let req = msg as *mut AddRequest;
        let a = unsafe { (*req).a };
        let b = unsafe { (*req).b };
        println!("get add request {:} {:}", a, b);

        let size = std::mem::size_of::<AddResponse>();
        let addr = self.conn.alloc_mr(size).unwrap();
        unsafe {
            (*(addr as *mut AddResponse)).sum = a + b;
        }
        
        self.conn.post_send(IBV_WR_SEND, addr, size as _, 0, 2, 0, 0).unwrap();
    }
}

impl<'a> AddRpcProcess<'a> {
    fn new(conn: &Arc<RdmaRcConn<'a>>) -> Self {
        Self {
            conn: conn.clone()
        }
    }
}

unsafe impl<'a> Send for AddRpcProcess<'a> {}
unsafe impl<'a> Sync for AddRpcProcess<'a> {}

fn main() {
    let mut rdma = RdmaControl::new(1);
    rdma.init("0.0.0.0\0", "7472\0");
    rdma.listen_task();

    let conn = rdma.get_connection(0);
    conn.init_and_start_recvs().unwrap();

    let process = Arc::new(AddRpcProcess::new(&conn));
    conn.register_recv_callback( &process).unwrap();

    let mut num = 0_i32;
    loop {
        let n = conn.poll_comps();
        if n > 0 {
            num = num + n;
            // println!("n {:} , num {:}", n,  num);
        }
        if num >= 4 {
            println!("break");
            break;
        }
    }
}