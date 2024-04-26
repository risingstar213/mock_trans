use std::sync::Arc;
use std::sync::Mutex;

use trans::rdma::control::RdmaControl;
use trans::rdma::rcconn::RdmaRcConn;
use trans::rdma::two_sides::TwoSidesComm;
use trans::rdma::RdmaRecvCallback;

#[repr(C)]
pub struct AddRequest {
    a: u8,
    b: u8,
}

#[repr(C)]
pub struct AddResponse {
    sum: u8,
}

struct AddRpcProcess {
    conn: Arc<Mutex<RdmaRcConn>>,
}
impl RdmaRecvCallback for AddRpcProcess {
    fn rdma_recv_handler(&self, src_conn: &mut RdmaRcConn, msg: *mut u8) {
        let req = msg as *mut AddRequest;
        let a = unsafe { (*req).a };
        let b = unsafe { (*req).b };
        println!("get add request {:} {:}", a, b);

        let size = std::mem::size_of::<AddResponse>();
        let addr = src_conn.alloc_mr(size).unwrap();
        unsafe {
            (*(addr as *mut AddResponse)).sum = a + b;
        }

        src_conn.send_pending(addr, size as _).unwrap();
    }
}

impl AddRpcProcess {
    fn new(conn: &Arc<Mutex<RdmaRcConn>>) -> Self {
        Self { conn: conn.clone() }
    }
}

unsafe impl Send for AddRpcProcess {}
unsafe impl Sync for AddRpcProcess {}

fn main() {
    let mut rdma = RdmaControl::new(1);
    rdma.init("0.0.0.0\0", "7472\0");
    rdma.listen_task(1);

    let conn = rdma.get_connection(0);
    conn.lock().unwrap().init_and_start_recvs().unwrap();

    let process = Arc::new(AddRpcProcess::new(&conn));
    conn.lock()
        .unwrap()
        .register_recv_callback(&process)
        .unwrap();

    let mut num = 0_i32;
    loop {
        let n = conn.lock().unwrap().poll_recvs();
        conn.lock().unwrap().flush_pending().unwrap();
        if n > 0 {
            num = num + n;
            // println!("n {:} , num {:}", n,  num);
        }
        if num >= 36 {
            println!("break");
            break;
        }
    }

    // conn.flush_pending_with_signal(true).unwrap();
}
