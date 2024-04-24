pub mod connection;
pub mod conn_buf;

use byte_struct::*;

pub mod doca_conn_info_type {
    pub type Type = u32;
    pub const REQ: Type = 0;
    pub const REPLY: Type = 1;

}

bitfields!(
    #[derive(PartialEq, Debug)]
    pub DocaConnHeaderMeta: u32 {
        pub info_type:    2,
        pub info_id:      5,
        pub info_payload:  13,
        pub info_pid:     5,
        pub info_cid:     7
    }
);

impl DocaConnHeaderMeta {
    pub fn new(info_type: u32, info_id: u32, info_payload: u32, info_pid: u32, info_cid: u32) -> Self {
        Self {
            info_type: info_type,
            info_id:   info_id,
            info_payload: info_payload,
            info_pid:  info_pid,
            info_cid:  info_cid
        }
    }

    pub fn to_header(&self) -> u32 {
        self.to_raw()
    }

    pub fn from_header(raw: u32) -> Self {
        DocaConnHeaderMeta::from_raw(raw)
    }
}