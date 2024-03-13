use trans::rdma::RdmaControl;

fn main() {
    let mut rdma = RdmaControl::new(0);
    rdma.connect(1, "10.10.10.9\0", "7472\0").unwrap();
}