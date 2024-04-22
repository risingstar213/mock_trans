#![feature(get_mut_unchecked)]
use std::sync::{ Arc, Mutex };
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::mpsc;

use trans::app::small_bank::local_client::SmallBankClient;
use trans::app::small_bank::SmallBankClientReq;
use trans::app::small_bank::loader::SmallBankLoader;
use trans::app::small_bank::SmallBankWorker;
use trans::common::random::FastRandom;
use trans::rdma::control::RdmaControl;
use trans::rdma::rcconn::RdmaRcConn;
use trans::framework::scheduler::AsyncScheduler;
use trans::memstore::memdb::MemDB;

const CONN_PORTS: [&str; 4] = ["7472\0", "7473\0", "7474\0", "7475\0"];

async fn connect_and_run(tid: usize, memdb: Arc<MemDB>, rand_seed: usize, client: Arc<AsyncMutex<mpsc::Receiver<SmallBankClientReq>>>) {
    // scheduler
    let mut rdma = RdmaControl::new(0);
    rdma.connect(1, "10.10.10.6\0", CONN_PORTS[tid]).unwrap();

    let allocator = rdma.get_allocator();
    let mut scheduler = Arc::new(AsyncScheduler::new(tid, 8, &allocator));

    let conn = rdma.get_connection(1);
    conn.lock().unwrap().init_and_start_recvs().unwrap();

    unsafe {
        Arc::get_mut_unchecked(&mut scheduler).append_conn(1, &conn);
    }

    conn.lock()
        .unwrap()
        .register_recv_callback(&scheduler)
        .unwrap();

    let worker = Arc::new(SmallBankWorker::new(0, tid as _, &memdb, &scheduler));
    unsafe {
        Arc::get_mut_unchecked(&mut scheduler).register_callback(&worker);
    }

    worker.run(rand_seed, &client).await;
}

fn main()
{
    let memdb = SmallBankLoader::new_memdb(0);
    let mut sb_client = SmallBankClient::new();

    let mut rand_gen = FastRandom::new(23984543 + 0);

    for i in 0..1 {
        let (sx, rx) = mpsc::channel::<SmallBankClientReq>(32);
        let sender = Arc::new(Mutex::new(sx));
        let receiver = Arc::new(AsyncMutex::new(rx));

        let rand_seed = rand_gen.next();
        let memdb_clone = memdb.clone();

        std::thread::spawn(move || {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async move {
                    connect_and_run(i, memdb_clone, rand_seed, receiver).await;
            });
        });

        sb_client.add_sender(&sender);
    }

    let rand_seed = rand_gen.next();
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async move {
            sb_client.work_loop(rand_seed).await;
        });
}