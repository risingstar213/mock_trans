#![feature(get_mut_unchecked)]
use std::env;
use trans::common::logs::init_log;
#[cfg(feature = "doca_deps")]
mod test_dpu {

use std::sync::{ Arc, Mutex };
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::mpsc;

use trans::app::tpcc::local_client::TpccClient;
use trans::app::tpcc::TpccClientReq;
use trans::app::tpcc::dpu_helpers::loader::TpccDpuLoader;
use trans::app::tpcc::dpu_helpers::host_worker::TpccHostWorker;
use trans::common::random::FastRandom;
use trans::rdma::control::RdmaControl;
use trans::rdma::rcconn::RdmaRcConn;
use trans::framework::scheduler::AsyncScheduler;
use trans::memstore::memdb::ValueDB;
use trans::TPCC_NTHREADS;
use trans::TPCC_NROUTINES;
use trans::doca_comm_chan::connection::DocaCommChannel;

const CONN_PORTS: [&str; 8] = ["7472\0", "7473\0", "7474\0", "7475\0", "7476\0", "7477\0", "7478\0", "7479\0"];
const COMM_NAMES: [&str; 8] = ["comm0\0", "comm1\0", "comm2\0", "comm3\0", "comm4\0", "comm5\0", "comm6\0", "comm7\0"];

async fn init_and_run(tid: usize, valuedb: Arc<ValueDB>, rand_seed: usize, client: Arc<AsyncMutex<mpsc::Receiver<TpccClientReq>>>) {
    let pci_addr = if tid < 4 {
        "af:00.0"
    } else {
        "af:00.1"
    };
    // comm chan
    let comm_chan = DocaCommChannel::new_client(COMM_NAMES[tid], pci_addr);

    // rdma conn
    let mut rdma = RdmaControl::new(0);
    rdma.init("0.0.0.0\0", CONN_PORTS[tid]);
    rdma.listen_task(1);

    // scheduler
    let allocator = rdma.get_allocator();
    let mut scheduler = Arc::new(AsyncScheduler::new(tid, TPCC_NROUTINES as _, &allocator));

    // scheduler add comm chan
    unsafe {
        Arc::get_mut_unchecked(&mut scheduler).set_comm_chan(comm_chan);
    }

    // scheduler add rdma conn
    let conn = rdma.get_connection(1);
    conn.lock().unwrap().init_and_start_recvs().unwrap();
    conn.lock()
        .unwrap()
        .register_recv_callback(&scheduler)
        .unwrap();

    unsafe {
        Arc::get_mut_unchecked(&mut scheduler).append_conn(1, &conn);
    }
    
    // worker
    let worker = Arc::new(TpccHostWorker::new(0, tid as _, &valuedb, &scheduler));
    // worker comm chan handler
    unsafe {
        Arc::get_mut_unchecked(&mut scheduler).register_comm_handler(&worker);
    }

    // worker rdma conn handler
    unsafe {
        Arc::get_mut_unchecked(&mut scheduler).register_callback(&worker);
    }


    worker.run(rand_seed, &client).await;
}

pub fn test()
{
    let valuedb = TpccDpuLoader::new_hostdb(0);
    let mut sb_client = TpccClient::new();

    let mut rand_gen = FastRandom::new(23984543 + 1);

    for i in 0..TPCC_NTHREADS {
        let (sx, rx) = mpsc::channel::<TpccClientReq>(32);
        let sender = Arc::new(Mutex::new(sx));
        let receiver = Arc::new(AsyncMutex::new(rx));

        let rand_seed = rand_gen.next();
        let valuedb_clone = valuedb.clone();

        std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async move {
                    init_and_run(i, valuedb_clone, rand_seed, receiver).await;
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

}

fn main() {
    let log_path = env::current_dir().unwrap().join("test.log");
    init_log(log_path.as_path());
    #[cfg(feature = "doca_deps")]
    test_dpu::test();
}