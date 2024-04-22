#[cfg(feature = "doca_deps")]
pub mod config;
#[cfg(feature = "doca_deps")]
pub mod connection;
#[cfg(feature = "doca_deps")]
mod dma_shared_buffer;
#[cfg(feature = "doca_deps")]
pub mod export_helpers;
#[cfg(feature = "doca_deps")]
pub mod process_helpers;

#[derive(Clone, Copy)]
pub struct DmaLocalBuf {
    saddr: usize,
    off:   usize,
    len:   usize,
}

#[derive(Clone)]
pub struct DmaRemoteBuf {
    off:   usize,
    len:   usize,
}