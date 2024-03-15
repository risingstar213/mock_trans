use std::sync::Arc;
use std::thread;
use libc::sleep;
use rdma_sys::ibv_wr_opcode::IBV_WR_SEND;

use trans::rdma::control::RdmaControl;
use trans::rdma::RdmaRecvCallback;
use trans::rdma::connection::RdmaRcConn;


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
        let req = msg as *mut AddResponse;
        println!("get add response {:}", unsafe { (*req).sum } ) ;
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

#[inline]
fn send_req(conn: &Arc<RdmaRcConn>, a: u8, b: u8) {
    let size = std::mem::size_of::<AddRequest>();
        let addr = conn.alloc_mr(size).unwrap();
        unsafe {
            (*(addr as *mut AddRequest)).a = a;
            (*(addr as *mut AddRequest)).b = b;
        }
        conn.post_send(IBV_WR_SEND, addr, size as _, 0, 2, addr as _, 0).unwrap();
        println!("send req {:} {:}", a, b);
}

fn main() {
    let mut rdma = RdmaControl::new(0);
    rdma.connect(1, "10.10.10.9\0", "7472\0").unwrap();

    let conn = rdma.get_connection(1);
    conn.init_for_recvs().unwrap();

    let process = Arc::new(AddRpcProcess::new(&conn));
    conn.register_recv_callback( &process).unwrap();

    let conn_clone = conn.clone();
    let th = thread::spawn(move || {
        let mut num = 0_i32;
        loop {
            let n = conn_clone.poll_comps();
            if n > 0 {
                num = num + n;
                // println!("n {:} , num {:}", n,  num);
            }
            if num >= 4 {
                println!("break");
                break;
            }
        }
    });

    send_req(&conn, 2, 7);

    // unsafe { sleep(2); }
    send_req(&conn, 9, 11);

    th.join().unwrap();

}