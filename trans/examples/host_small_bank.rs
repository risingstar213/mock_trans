#![feature(get_mut_unchecked)]

mod test_dpu {

use std::sync::{ Arc, Mutex };
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::mpsc;

use trans::app::small_bank::local_client::SmallBankClient;
use trans::app::small_bank::SmallBankClientReq;
use trans::app::small_bank::dpu_helpers::loader::SmallBankDpuLoader;
use trans::app::small_bank::dpu_helpers::host_worker::SmallBankHostWorker;
use trans::common::random::FastRandom;
use trans::rdma::control::RdmaControl;
use trans::rdma::rcconn::RdmaRcConn;
use trans::framework::scheduler::AsyncScheduler;
use trans::memstore::memdb::ValueDB;
use trans::SMALL_BANK_NTHREADS;
use trans::doca_comm_chan::connection::DocaCommChannel;

const CONN_PORTS: [&str; 8] = ["7472\0", "7473\0", "7474\0", "7475\0", "7476\0", "7477\0", "7478\0", "7479\0"];
const COMM_NAMES: [&str; 8] = ["comm0\0", "comm1\0", "comm2\0", "comm3\0", "comm4\0", "comm5\0", "comm6\0", "comm7\0"];

async fn init_and_run(tid: usize, valuedb: Arc<ValueDB>, rand_seed: usize, client: Arc<AsyncMutex<mpsc::Receiver<SmallBankClientReq>>>) {
    
    // comm chan
    let comm_chan = DocaCommChannel::new_client(COMM_NAMES[tid], "af:00.0");

    // rdma conn
    let mut rdma = RdmaControl::new(1);
    rdma.init("0.0.0.0\0", CONN_PORTS[tid]);
    rdma.listen_task(1);

    // scheduler
    let allocator = rdma.get_allocator();
    let mut scheduler = Arc::new(AsyncScheduler::new(tid, 8, &allocator));

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
    let worker = Arc::new(SmallBankHostWorker::new(1, tid as _, &valuedb, &scheduler));
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
    let valuedb = SmallBankDpuLoader::new_hostdb(0);
    let mut sb_client = SmallBankClient::new();

    let mut rand_gen = FastRandom::new(23984543 + 1);

    for i in 0..SMALL_BANK_NTHREADS {
        let (sx, rx) = mpsc::channel::<SmallBankClientReq>(32);
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
    #[cfg(feature = "doca_deps")]
    test_dpu::test();
}