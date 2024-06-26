#[cfg(feature = "doca_deps")]
pub mod connection;
#[cfg(feature = "doca_deps")]
pub mod comm_buf;

use byte_struct::*;

pub mod doca_comm_info_type {
    pub type Type = u32;
    pub const REQ: Type = 0;
    pub const REPLY: Type = 1;

}

bitfields!(
    #[derive(PartialEq, Debug)]
    pub DocaCommHeaderMeta: u32 {
        pub info_type:    2,
        pub info_id:      5,
        pub info_payload:  10,
        pub info_pid:     5,
        pub info_tid:     3,
        pub info_cid:     7,
    }
);

impl DocaCommHeaderMeta {
    pub fn new(info_type: u32, info_id: u32, info_payload: u32, info_pid: u32, info_tid: u32, info_cid: u32) -> Self {
        Self {
            info_type: info_type as _,
            info_id:   info_id as _,
            info_payload: info_payload as _,
            info_pid:  info_pid as _,
            info_tid:  info_tid as _,
            info_cid:  info_cid as _,
        }
    }

    pub fn to_header(&self) -> u32 {
        self.to_raw()
    }

    pub fn from_header(raw: u32) -> Self {
        DocaCommHeaderMeta::from_raw(raw)
    }
}