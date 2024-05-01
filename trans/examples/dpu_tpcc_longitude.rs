#![feature(get_mut_unchecked)]
use std::sync::{ Arc, Mutex };
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::mpsc;
use std::env;

use trans::common::logs::init_log;

use trans::app::tpcc::local_client::TpccClient;
use trans::app::tpcc::TpccClientReq;
use trans::app::tpcc::loader_longitude::TpccLongitudeLoader;
use trans::app::tpcc::TpccHostLongitudeWorker;
use trans::common::random::FastRandom;
use trans::rdma::control::RdmaControl;
use trans::rdma::rcconn::RdmaRcConn;
use trans::framework::scheduler::AsyncScheduler;
use trans::memstore::memdb::MemDB;
use trans::TPCC_NTHREADS;
use trans::TPCC_NROUTINES;

const CONN_PORTS: [&str; 8] = ["7472\0", "7473\0", "7474\0", "7475\0", "7476\0", "7477\0", "7478\0", "7479\0"];

async fn listen_and_run(tid: usize, memdb: Arc<MemDB>, rand_seed: usize) {
    // scheduler
    let mut rdma = RdmaControl::new(100);
    rdma.init("0.0.0.0\0", CONN_PORTS[tid]);
    rdma.listen_task(2);

    let allocator = rdma.get_allocator();
    let mut scheduler = Arc::new(AsyncScheduler::new(tid, 1, &allocator));

    let conn_host = rdma.get_connection(0);
    conn_host.lock().unwrap().init_and_start_recvs().unwrap();
    unsafe {
        Arc::get_mut_unchecked(&mut scheduler).append_conn(0, &conn_host);
    }
    conn_host.lock()
        .unwrap()
        .register_recv_callback(&scheduler)
        .unwrap();

    let conn_hybrid = rdma.get_connection(1);
    conn_hybrid.lock().unwrap().init_and_start_recvs().unwrap();
    unsafe {
        Arc::get_mut_unchecked(&mut scheduler).append_conn(1, &conn_hybrid);
    }
    conn_hybrid.lock()
        .unwrap()
        .register_recv_callback(&scheduler)
        .unwrap();

    let worker = Arc::new(TpccHostLongitudeWorker::new(0, tid as _, &memdb, &scheduler));
    unsafe {
        Arc::get_mut_unchecked(&mut scheduler).register_callback(&worker);
    }

    worker.run_main(rand_seed).await;
}

fn main()
{
    let log_path = env::current_dir().unwrap().join("test.log");
    init_log(log_path.as_path());
    
    let memdb = TpccLongitudeLoader::new_dpudb(0);

    let mut rand_gen = FastRandom::new(23984543 + 1);

    let mut ths = Vec::new();
    for i in 0..TPCC_NTHREADS {
        let (sx, rx) = mpsc::channel::<TpccClientReq>(32);
        let sender = Arc::new(Mutex::new(sx));
        let receiver = Arc::new(AsyncMutex::new(rx));

        let rand_seed = rand_gen.next();
        let memdb_clone = memdb.clone();

        ths.push(std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async move {
                    listen_and_run(i, memdb_clone, rand_seed).await;
            });
        }));

    }

    for th in ths {
        th.join().unwrap();
    }
}