use clap::{arg, App, AppSettings};
use doca::{dma::DOCAContext, *};

use std::sync::Arc;

use trans::doca_dma::process_helpers::{ recv_doca_config, load_doca_config };
use trans::doca_dma::config::{ DocaConnInfo };

use trans::doca_dma::connection::DocaDmaControl;

fn main() {

    let mut control = DocaDmaControl::new();
    control.listen_on("af:00.0", "0.0.0.0:7473".parse().unwrap(), 1);

    let copy_txt = "testtesttesttest";

    let mut conn = control.get_conn().unwrap();

    let remote_buf = Arc::get_mut(&mut conn).unwrap().alloc_remote_buf();

    let mut local_buf = Arc::get_mut(&mut conn).unwrap().get_local_buf(0);
    let local_slice = unsafe { local_buf.get_mut_slice::<u8>(64) };
    local_slice.copy_from_slice(copy_txt.as_bytes());

    Arc::get_mut(&mut conn).unwrap().post_write_dma_reqs(local_buf.get_off(), remote_buf.get_off(), 64, 1234);

    loop {
        let (event, error) = Arc::get_mut(&mut conn).unwrap().poll_completion();

        if error == DOCAError::DOCA_SUCCESS {
            println!("Job finished!");
            break;
        } else {
            if error == DOCAError::DOCA_ERROR_AGAIN {
                continue;
            } else {
                panic!("Job failed!");
            }
        }
    }

    let mut local_buf_dst = Arc::get_mut(&mut conn).unwrap().get_local_buf(0);
    assert!(local_buf_dst.get_off() != local_buf.get_off());

    Arc::get_mut(&mut conn).unwrap().post_read_dma_reqs(remote_buf.get_off(), local_buf_dst.get_off(), 64, 1234);

    loop {
        let (event, error) = Arc::get_mut(&mut conn).unwrap().poll_completion();

        if error == DOCAError::DOCA_SUCCESS {
            println!("Job finished!");
            break;
        } else {
            if error == DOCAError::DOCA_ERROR_AGAIN {
                continue;
            } else {
                panic!("Job failed!");
            }
        }
    }

    let dst_slice = unsafe { local_buf_dst.get_mut_slice::<u8>(64) };
    /* ------- Finalize check ---------- */
    println!(
        "[After] dst_buffer check: {}",
        String::from_utf8(dst_slice.to_vec()).unwrap()
    );
}
