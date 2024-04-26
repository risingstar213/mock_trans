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
use trans::SMALL_BANK_NTHREADS;

const CONN_PORTS: [&str; 8] = ["7472\0", "7473\0", "7474\0", "7475\0", "7476\0", "7477\0", "7478\0", "7479\0"];

async fn connect_and_run(tid: usize, memdb: Arc<MemDB>, rand_seed: usize, client: Arc<AsyncMutex<mpsc::Receiver<SmallBankClientReq>>>) {
    // scheduler
    let mut rdma = RdmaControl::new(1);
    rdma.connect(0, "10.10.10.6\0", CONN_PORTS[tid]).unwrap();
    rdma.connect(100, "10.10.10.26\0", CONN_PORTS[tid]).unwrap();

    let allocator = rdma.get_allocator();
    let mut scheduler = Arc::new(AsyncScheduler::new(0, 8, &allocator));

    let conn_host = rdma.get_connection(0);
    conn_host.lock().unwrap().init_and_start_recvs().unwrap();
    unsafe {
        Arc::get_mut_unchecked(&mut scheduler).append_conn(0, &conn_host);
    }
    conn_host.lock()
        .unwrap()
        .register_recv_callback(&scheduler)
        .unwrap();

    let conn_dpu = rdma.get_connection(100);
    conn_dpu.lock().unwrap().init_and_start_recvs().unwrap();
    unsafe {
        Arc::get_mut_unchecked(&mut scheduler).append_conn(100, &conn_dpu);
    }
    conn_dpu.lock()
        .unwrap()
        .register_recv_callback(&scheduler)
        .unwrap();

    let worker = Arc::new(SmallBankWorker::new(1, tid as _, &memdb, &scheduler));
    unsafe {
        Arc::get_mut_unchecked(&mut scheduler).register_callback(&worker);
    }

    worker.run(rand_seed, &client).await;
}

fn main()
{
    let memdb = SmallBankLoader::new_memdb(1);
    let mut sb_client = SmallBankClient::new();

    let mut rand_gen = FastRandom::new(23984543 + 0);

    for i in 0..SMALL_BANK_NTHREADS {
        let (sx, rx) = mpsc::channel::<SmallBankClientReq>(32);
        let sender = Arc::new(Mutex::new(sx));
        let receiver = Arc::new(AsyncMutex::new(rx));

        let rand_seed = rand_gen.next();
        let memdb_clone = memdb.clone();

        std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
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
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async move {
            sb_client.work_loop(rand_seed).await;
        });
}