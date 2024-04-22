#[cfg(feature = "doca_deps")]
use trans::doca_dma::connection::DocaDmaControl;

#[cfg(feature = "doca_deps")]
fn test() {
    let mut control = DocaDmaControl::new();

    control.connect_and_waiting_loop("af:00.0", "192.168.100.2:7473".parse().unwrap());
}

fn main() {
    #[cfg(feature = "doca_deps")]
    test();
}
