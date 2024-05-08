#![feature(get_mut_unchecked)]
use std::sync::{ Arc, Mutex };
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::mpsc;
use std::env;

use trans::common::logs::init_log;

use trans::app::tpcc::local_client::TpccClient;
use trans::app::tpcc::TpccClientReq;
use trans::app::tpcc::loader::TpccLoader;
use trans::app::tpcc::TpccWorker;
use trans::common::random::FastRandom;
use trans::rdma::control::RdmaControl;
use trans::rdma::rcconn::RdmaRcConn;
use trans::framework::scheduler::AsyncScheduler;
use trans::memstore::memdb::MemDB;
use trans::TPCC_NTHREADS;
use trans::TPCC_NROUTINES;

const CONN_PORTS: [&str; 8] = ["7472\0", "7473\0", "7474\0", "7475\0", "7476\0", "7477\0", "7478\0", "7479\0"];

async fn connect_and_run(tid: usize, memdb: Arc<MemDB>, rand_seed: usize, client: Arc<AsyncMutex<mpsc::Receiver<TpccClientReq>>>) {
    // scheduler
    let mut rdma = RdmaControl::new(0);
    rdma.connect(1, "10.10.10.6\0", CONN_PORTS[tid]).unwrap();

    let allocator = rdma.get_allocator();
    let mut scheduler = Arc::new(AsyncScheduler::new(tid, TPCC_NROUTINES as _, &allocator));

    let conn = rdma.get_connection(1);
    conn.lock().unwrap().init_and_start_recvs().unwrap();

    unsafe {
        Arc::get_mut_unchecked(&mut scheduler).append_conn(1, &conn);
    }

    conn.lock()
        .unwrap()
        .register_recv_callback(&scheduler)
        .unwrap();

    let worker = Arc::new(TpccWorker::new(0, tid as _, &memdb, &scheduler));
    unsafe {
        Arc::get_mut_unchecked(&mut scheduler).register_callback(&worker);
    }

    worker.run(rand_seed, &client).await;
}

fn main()
{
    let log_path = env::current_dir().unwrap().join("test.log");
    init_log(log_path.as_path());
    
    let memdb = TpccLoader::new_memdb(0);
    let mut sb_client = TpccClient::new();

    let mut rand_gen = FastRandom::new(23984543 + 0);

    for i in 0..TPCC_NTHREADS {
        let (sx, rx) = mpsc::channel::<TpccClientReq>(1000);
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