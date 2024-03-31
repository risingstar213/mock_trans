use std::sync::Arc;
use std::sync::Mutex;
// use tokio;

use trans::rdma::control::RdmaControl;
use trans::rdma::rcconn::RdmaRcConn;
// use trans::rdma::two_sides::TwoSidesComm;

use trans::framework::rpc::{rpc_msg_type, AsyncRpc, RpcHandler};
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

struct AskServerWorker<'a> {
    scheduler: Arc<AsyncScheduler<'a>>,
    stopped: Mutex<bool>,
}

impl<'a> AskServerWorker<'a> {
    fn new(scheduler: &Arc<AsyncScheduler<'a>>) -> Self {
        Self {
            scheduler: scheduler.clone(),
            stopped: Mutex::new(false),
        }
    }
}

impl<'a> AskServerWorker<'a> {
    async fn work_routine(&self, cid: u32) {
        let reply_buf = self.scheduler.get_reply_buf() as usize;
        self.scheduler.prepare_multi_replys(cid, reply_buf as _, 10);

        for i in 0..10 {
            let req_buf = self.scheduler.get_req_buf(cid);
            let size = std::mem::size_of::<AddRequest>();

            unsafe {
                (*(req_buf as *mut AddRequest)).a = i + 1;
                (*(req_buf as *mut AddRequest)).b = i * 2;
            }
            self.scheduler.append_pending_req(
                req_buf,
                ADD_ID,
                size as _,
                cid,
                rpc_msg_type::REQ,
                1,
                0,
            );
            println!("send req {:} {:}", i + 1, i * 2);
        }

        self.scheduler.flush_pending();

        self.scheduler.yield_until_ready(cid).await;

        let resp_size = std::mem::size_of::<AddResponse>();
        let resp_addr = reply_buf as *mut u8;
        for i in 0..10 {
            let reply = unsafe { resp_addr.add(i * resp_size) };

            let resp = reply as *mut AddResponse;
            println!("get add response {:}", unsafe { (*resp).sum });
        }

        *self.stopped.lock().unwrap() = true;
    }
}

impl<'a> AsyncWorker<'a> for AskServerWorker<'a> {
    fn get_scheduler(&self) -> &AsyncScheduler<'a> {
        return &self.scheduler;
    }

    fn has_stopped(&self) -> bool {
        *self.stopped.lock().unwrap()
    }
}

impl<'a> RpcHandler for AskServerWorker<'a> {
    #[allow(unused)]
    fn rpc_handler(
        &self,
        src_conn: &mut RdmaRcConn,
        rpc_id: u32,
        msg: *mut u8,
        size: u32,
        meta: trans::framework::rpc::RpcProcessMeta,
    ) {
        unimplemented!();
    }
}

#[tokio::main]
async fn main() {
    let mut rdma = RdmaControl::new(0);
    rdma.connect(1, "10.10.10.7\0", "7472\0").unwrap();

    let allocator = rdma.get_allocator();
    let scheduler = Arc::new(AsyncScheduler::new(2, &allocator));

    let conn = rdma.get_connection(1);
    conn.lock().unwrap().init_and_start_recvs().unwrap();

    scheduler.append_conn(1, &conn);
    conn.lock()
        .unwrap()
        .register_recv_callback(&scheduler)
        .unwrap();

    let worker = Arc::new(AskServerWorker::new(&scheduler));
    scheduler.register_callback(&worker);

    {
        let worker1 = worker.clone();
        tokio::task::spawn(async move {
            worker1.work_routine(1).await;
        });

        let worker0 = worker.clone();
        tokio::task::spawn(async move {
            worker0.main_routine().await;
        })
        .await
        .unwrap();
    }
}
