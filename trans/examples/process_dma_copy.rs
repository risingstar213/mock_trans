use doca::*;

use trans::doca_dma::connection::DocaDmaControl;

fn main() {

    let mut control = DocaDmaControl::new();
    control.listen_on("af:00.0", "0.0.0.0:7473".parse().unwrap(), 1);

    let copy_txt = "testtesttesttest";

    let conn = control.get_conn().unwrap();

    let remote_buf = conn.lock().unwrap().alloc_remote_buf();

    let mut local_buf = conn.lock().unwrap().get_local_buf(0);
    let local_slice = unsafe { local_buf.get_mut_slice::<u8>(16) };
    local_slice.copy_from_slice(copy_txt.as_bytes());

    conn.lock().unwrap().post_write_dma_reqs(local_buf.get_off(), remote_buf.get_off(), 32, 1234);

    loop {
        let (_, error) = conn.lock().unwrap().poll_completion();

        if error == DOCAError::DOCA_SUCCESS {
            println!("Job finished!");
            break;
        } else {
            if error == DOCAError::DOCA_ERROR_AGAIN {
                continue;
            } else {
                dbg!(error);
                panic!("Job failed!");
            }
        }
    }

    let mut local_buf_dst = conn.lock().unwrap().get_local_buf(0);
    assert!(local_buf_dst.get_off() != local_buf.get_off());

    conn.lock().unwrap().post_read_dma_reqs(local_buf_dst.get_off(), remote_buf.get_off(), 32, 1234);

    loop {
        let (_, error) = conn.lock().unwrap().poll_completion();

        if error == DOCAError::DOCA_SUCCESS {
            println!("Job finished!");
            break;
        } else {
            if error == DOCAError::DOCA_ERROR_AGAIN {
                continue;
            } else {
                dbg!(error);
                panic!("Job failed!");
            }
        }
    }
    let src_off = local_buf.get_off();
    let dst_off = local_buf_dst.get_off();
    let src_slice = unsafe { local_buf.get_const_slice::<u8>(32) };
    let dst_slice = unsafe { local_buf_dst.get_const_slice::<u8>(32) };
    /* ------- Finalize check ---------- */
    println!(
        "[After] off:{}, src_buffer check: {}",
        src_off,
        String::from_utf8(src_slice.to_vec()).unwrap()
    );
    println!(
        "[After] off:{}, dst_buffer check: {}",
        dst_off,
        String::from_utf8(dst_slice.to_vec()).unwrap()
    );
}
