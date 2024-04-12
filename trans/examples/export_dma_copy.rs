use trans::doca_dma::connection::DocaDmaControl;

fn main() {
    let mut control = DocaDmaControl::new();

    control.connect_and_waiting_loop("03:00.0", "192.168.100.1:7473".parse().unwrap());
}
