#![feature(get_mut_unchecked)]

#[cfg(feature = "doca_deps")]
mod test_dpu {

use std::sync::{ Arc, Mutex };

use trans::rdma::control::RdmaControl;
use trans::doca_comm_chan::connection::DocaCommChannel;
use trans::memstore::memdb::MemDB;
use trans::framework::scheduler::AsyncScheduler;
use trans::app::small_bank::dpu_helpers::dpu_worker::SmallBankDpuWorker;
use trans::app::small_bank::dpu_helpers::loader::SmallBankDpuLoader;
use trans::SMALL_BANK_NTHREADS;

const CONN_PORTS: [&str; 8] = ["7472\0", "7473\0", "7474\0", "7475\0", "7476\0", "7477\0", "7478\0", "7479\0"];
const COMM_NAMES: [&str; 8] = ["comm0\0", "comm1\0", "comm2\0", "comm3\0", "comm4\0", "comm5\0", "comm6\0", "comm7\0"];

async fn init_and_run(tid: usize, memdb: Arc<MemDB>) {
    // comm chan
    let comm_chan = DocaCommChannel::new_server(COMM_NAMES[tid], "03:00.0", "af:00.0");

    // rdma conn
    let mut rdma = RdmaControl::new(100);
    rdma.init("0.0.0.0\0", CONN_PORTS[tid]);
    rdma.listen_task(1);

    // scheduler
    let allocator = rdma.get_allocator();
    let mut scheduler = Arc::new(AsyncScheduler::new(0, 1, &allocator));

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
    let worker = Arc::new(SmallBankDpuWorker::new(tid as _, &memdb, &scheduler));
    // worker comm chan handler
    unsafe {
        Arc::get_mut_unchecked(&mut scheduler).register_comm_handler(&worker);
    }

    // worker rdma conn handler
    unsafe {
        Arc::get_mut_unchecked(&mut scheduler).register_callback(&worker);
    }

    worker.run().await;
}

pub fn test() {
    let memdb = SmallBankDpuLoader::new_dpudb(0);

    let mut ths = Vec::new();
    for i in 0..SMALL_BANK_NTHREADS {
        let memdb_clone = memdb.clone();
        ths.push(std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async move {
                    init_and_run(i, memdb_clone).await;
            });
        }));
    }

    for th in ths {
        th.join().unwrap();
    }
}

}

fn main() {
    #[cfg(feature = "doca_deps")]
    test_dpu::test();
}