use std::sync::Arc;
use std::sync::Mutex;

use trans::rdma::control::RdmaControl;
use trans::rdma::rcconn::RdmaRcConn;

use trans::framework::rpc::AsyncRpc;
use trans::framework::rpc::{RpcHandler, RpcProcessMeta};
use trans::framework::scheduler::AsyncScheduler;
use trans::framework::worker::AsyncWorker;

#[repr(C)]
pub struct AddRequest {
    a: u8,
    b: u8,
}

#[repr(C)]
pub struct AddResponse {
    sum: u8,
}

const ADD_ID: u32 = 0;

struct AnswerClientWorker<'worker> {
    scheduler: Arc<AsyncScheduler<'worker>>,
    number: Mutex<u64>,
}

impl<'worker> AnswerClientWorker<'worker> {
    fn new(scheduler: &Arc<AsyncScheduler<'worker>>) -> Self {
        Self {
            scheduler: scheduler.clone(),
            number: Mutex::new(0),
        }
    }
}

// RPC handlers
impl<'worker> AnswerClientWorker<'worker> {
    fn add_handler(
        &self,
        src_conn: &mut RdmaRcConn,
        msg: *mut u8,
        size: u32,
        meta: RpcProcessMeta,
    ) {
        let req = msg as *mut AddRequest;
        let a = unsafe { (*req).a };
        let b = unsafe { (*req).b };
        println!("get add request {:} {:}", a, b);

        let size = std::mem::size_of::<AddResponse>();
        let addr = self.scheduler.get_reply_buf();
        unsafe {
            (*(addr as *mut AddResponse)).sum = a + b;
        }

        self.scheduler.send_reply(
            src_conn,
            addr as _,
            ADD_ID,
            size as _,
            meta.rpc_cid,
            meta.peer_id,
            meta.peer_tid,
        );

        *self.number.lock().unwrap() += 1;
    }
}

impl<'worker> RpcHandler for AnswerClientWorker<'worker> {
    fn rpc_handler(
        &self,
        src_conn: &mut RdmaRcConn,
        rpc_id: u32,
        msg: *mut u8,
        size: u32,
        meta: RpcProcessMeta,
    ) {
        match rpc_id {
            ADD_ID => {
                self.add_handler(src_conn, msg, size, meta);
            }
            _ => {
                unimplemented!();
            }
        }
    }
}

impl<'worker> AsyncWorker<'worker> for AnswerClientWorker<'worker> {
    fn get_scheduler(&self) -> &AsyncScheduler<'worker> {
        return &self.scheduler;
    }

    fn has_stopped(&self) -> bool {
        *self.number.lock().unwrap() >= 10
    }
}

// struct AddRpc<'worker> {
//     worker: Arc<AsyncWorker<'worker>>,
// }

#[tokio::main]
async fn main() {
    let mut rdma = RdmaControl::new(1);
    rdma.init("0.0.0.0\0", "7472\0");
    rdma.listen_task();

    let allocator = rdma.get_allocator();
    let scheduler = Arc::new(AsyncScheduler::new(1, &allocator));

    let conn = rdma.get_connection(0);
    conn.lock().unwrap().init_and_start_recvs().unwrap();

    scheduler.append_conn(0, &conn);
    conn.lock()
        .unwrap()
        .register_recv_callback(&scheduler)
        .unwrap();

    let worker = Arc::new(AnswerClientWorker::new(&scheduler));
    scheduler.register_callback(&worker);

    {
        let worker0 = worker.clone();
        tokio::task::spawn(async move {
            worker0.main_routine().await;
        })
        .await
        .unwrap();
    }
}
