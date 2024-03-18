use byte_struct::*;
// two-side information

enum RpcMsgType {
    Req,
    YReq,
    Reply,
}

bitfields!(
    #[derive(PartialEq, Debug)]
    RpcHeaderMeta: u32 {
        pub rpc_type:    2,
        pub rpc_id:      5,
        pub rpc_payload: 18,
        pub rpc_cid:     7
    }
);

pub trait AsyncRpc {
    fn send_reply(peer_id: u64, msg: *mut u8);
}


#[test]
fn test_bitfield() {
    let header = RpcHeaderMeta {
        rpc_type: 1,
        rpc_id:   2,
        rpc_payload: 345,
        rpc_cid:  4
    };

    let meta = header.to_raw();

    dbg!(header);
}