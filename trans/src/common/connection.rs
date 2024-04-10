/*
 *  This code refer to smartnic-bench from smartnickit-project
 * 
 *  https://github.com/smartnickit-project/smartnic-bench.git
 * 
 */

use std::io::{ Read, Write };
use std::net::{ SocketAddr, TcpStream, TcpListener };

const MAX_CONN_LENGTH: usize = 4096;

pub trait ConfigSerialize {
    fn serialize(data: Self) -> Vec<u8>;
    fn deserialize(data: &[u8]) -> Self;
}

#[inline]
pub fn recv_config<T: ConfigSerialize>(addr: SocketAddr) -> T {
    let mut conn_info = [0u8; MAX_CONN_LENGTH];
    let mut conn_info_len = 0;
    /* receive the buffer message from the host */
    let listener = TcpListener::bind(addr).unwrap();
    loop {
        if let Ok(res) = listener.accept() {
            let (mut stream, _) = res;
            conn_info_len = stream.read(&mut conn_info).unwrap();
            break;
        }
    }

    T::deserialize(&conn_info[0..conn_info_len])
}

#[inline]
pub fn send_config<T: ConfigSerialize>(addr: SocketAddr, config: T) {
    let mut stream = TcpStream::connect(addr).unwrap();
    let data = T::serialize(config);

    stream.write(data.as_slice()).unwrap();
}