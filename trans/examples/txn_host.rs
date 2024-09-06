#![feature(get_mut_unchecked)]
use std::sync::{ Arc, Mutex };
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::mpsc;

use clap::{ Arg, Command };
use trans::app::small_bank::local_client::SmallBankClient;
use trans::app::small_bank::SmallBankClientReq;
use trans::app::small_bank::loader_longitude::SmallBankLongitudeLoader;
use trans::app::small_bank::SmallBankHostLongitudeWorker;
use trans::app::tpcc::local_client::TpccClient;
use trans::app::tpcc::TpccClientReq;
use trans::app::tpcc::loader_longitude::TpccLongitudeLoader;
use trans::app::tpcc::TpccHostLongitudeWorker;
use trans::common::random::FastRandom;
use trans::rdma::control::RdmaControl;
use trans::rdma::rcconn::RdmaRcConn;
use trans::framework::scheduler::AsyncScheduler;
use trans::memstore::memdb::MemDB;

const CONN_PORTS: [&str; 8] = ["7472\0", "7473\0", "7474\0", "7475\0", "7476\0", "7477\0", "7478\0", "7479\0"];

async fn smallbank_connect_and_run(
    tid: usize, 
    memdb: Arc<MemDB>, 
    rand_seed: usize, 
    client: Arc<AsyncMutex<mpsc::Receiver<SmallBankClientReq>>>,
    routine_num: usize,
) {
    // scheduler
    let mut rdma = RdmaControl::new(0);
    rdma.connect(100, "10.10.10.26\0", CONN_PORTS[tid]).unwrap();
    rdma.init("0.0.0.0\0", CONN_PORTS[tid]);
    rdma.listen_task(2);

    let allocator = rdma.get_allocator();
    let mut scheduler = Arc::new(AsyncScheduler::new(tid, routine_num as _, &allocator));

    let conn_dpu = rdma.get_connection(100);
    conn_dpu.lock().unwrap().init_and_start_recvs().unwrap();
    unsafe {
        Arc::get_mut_unchecked(&mut scheduler).append_conn(100, &conn_dpu);
    }
    conn_dpu.lock()
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

    let worker = Arc::new(SmallBankHostLongitudeWorker::new(0, tid as _, &memdb, &scheduler));
    unsafe {
        Arc::get_mut_unchecked(&mut scheduler).register_callback(&worker);
    }

    worker.run(rand_seed, &client, routine_num).await;
}

fn main_smallbank(thread_num: usize, coroutine_num: usize) {
    let memdb = SmallBankLongitudeLoader::new_hostdb(0);
    let mut sb_client = SmallBankClient::new();

    let mut rand_gen = FastRandom::new(23984543 + 0);

    for i in 0..thread_num {
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
                    smallbank_connect_and_run(i, memdb_clone, rand_seed, receiver, coroutine_num).await;
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

async fn tpcc_connect_and_run(
    tid: usize, 
    memdb: Arc<MemDB>, 
    rand_seed: usize, 
    client: Arc<AsyncMutex<mpsc::Receiver<TpccClientReq>>>,
    routine_num: usize,
) {
    // scheduler
    let mut rdma = RdmaControl::new(0);
    rdma.connect(100, "10.10.10.26\0", CONN_PORTS[tid]).unwrap();
    rdma.init("0.0.0.0\0", CONN_PORTS[tid]);
    rdma.listen_task(2);

    let allocator = rdma.get_allocator();
    let mut scheduler = Arc::new(AsyncScheduler::new(tid, routine_num as _, &allocator));

    let conn_dpu = rdma.get_connection(100);
    conn_dpu.lock().unwrap().init_and_start_recvs().unwrap();
    unsafe {
        Arc::get_mut_unchecked(&mut scheduler).append_conn(100, &conn_dpu);
    }
    conn_dpu.lock()
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

    worker.run(rand_seed, &client, routine_num).await;
}

fn main_tpcc(thread_num: usize, coroutine_num: usize) {
    let memdb = TpccLongitudeLoader::new_hostdb(0);
    let mut sb_client = TpccClient::new();

    let mut rand_gen = FastRandom::new(23984543 + 0);

    for i in 0..thread_num {
        let (sx, rx) = mpsc::channel::<TpccClientReq>(32);
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
                    tpcc_connect_and_run(i, memdb_clone, rand_seed, receiver, coroutine_num).await;
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

fn main() {
    let matches = Command::new("txn server")
        .arg(
            Arg::new("thread_num")
                .short('t')
                .long("threads")
                .default_value("4"),
        )
        .arg(
            Arg::new("coroutine_num")
                .short('c')
                .long("coroutines")
                .default_value("4"),
        )
        .arg(
            Arg::new("workload")
                .short('w')
                .long("workload")
                .default_value("4"),
        )
        .get_matches();
    
    let thread_num: usize = matches.get_one::<String>("thread_num")
        .expect("need thread num")
        .trim()
        .parse()
        .expect("thread num should be number");

    let coroutine_num: usize = matches.get_one::<String>("coroutine_num")
        .expect("need coroutine num")
        .trim()
        .parse()
        .expect("coroutine num should be number");

    let workload = matches.get_one::<String>("workload")
        .expect("need workload");

    if workload.eq("smallbank") {
        main_smallbank(thread_num, coroutine_num);
    } else if workload.eq("tpcc") {
        main_tpcc(thread_num, coroutine_num);
    }

}