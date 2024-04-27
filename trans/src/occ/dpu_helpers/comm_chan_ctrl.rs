use std::sync::Arc;
use std::collections::HashMap;

use crate::doca_comm_chan::{ doca_comm_info_type, DocaCommHeaderMeta };
use crate::doca_comm_chan::comm_buf::DocaCommBuf;
use crate::framework::scheduler::AsyncScheduler;
use super::super::remote_helpers::{ ReadReqItem, FetchWriteReqItem };
use super::*;

pub struct CommChanCtrl {
    pid:       u32,
    tid:       u32,
    cid:       u32,
    scheduler: Arc<AsyncScheduler>,
    msg_map:   HashMap<u32, DocaCommBuf>,
    read_seqs: Vec<ReadReqItem>,
    update_seqs: Vec<FetchWriteReqItem>,
    success:   bool,
}

impl CommChanCtrl {
    pub fn new(scheduler: &Arc<AsyncScheduler>, pid: u32, tid: u32, cid: u32) -> Self {
        Self {
            pid:       pid,
            tid:       tid,
            cid:       cid,
            scheduler: scheduler.clone(),
            msg_map:  HashMap::new(),
            read_seqs: Vec::new(),
            update_seqs: Vec::new(),
            success: true,
        }
    }

    pub fn restart_batch(&mut self) {
        self.msg_map.clear();
        self.read_seqs.clear();
        self.update_seqs.clear();
        self.success = true;
    }

    pub fn send_comm_info(&mut self) {
        let info_num = self.msg_map.len();
        self.scheduler.prepare_comm_replys(self.cid, info_num);
        
        for info_id in 0u32..6 {
            if !self.msg_map.contains_key(&info_id) {
                continue;
            }
            let mut buf = self.msg_map.remove(&info_id).unwrap();
            let payload = buf.get_payload() as _;
            buf.set_header(DocaCommHeaderMeta{
                info_type: doca_comm_info_type::REQ,
                info_id:   info_id,
                info_payload: payload,
                info_pid:  self.pid,
                info_tid:  self.tid,
                info_cid:  self.cid,
            });

            self.scheduler.block_send_info(&mut buf);
            self.scheduler.comm_chan_dealloc_buf(buf);
        }
    }

    pub fn append_info(&mut self, info_id: u32) {
        match self.msg_map.get_mut(&info_id) {
            Some(_) => {}
            None => {
                let mut buffer = self.scheduler.comm_chan_alloc_buf();
                buffer.set_payload(0);
                self.msg_map.insert(info_id, buffer);
            }
        }
    }

    pub fn append_read(&mut self, table_id: usize, key: u64, read_idx: usize) {
        let info_id = doca_comm_info_id::LOCAL_READ_INFO;
        match self.msg_map.get_mut(&info_id) {
            Some(buf) => {
                unsafe {
                    buf.append_item(LocalReadInfoItem{
                        table_id: table_id,
                        key:      key,
                    });
                }

            }
            None => {
                let mut buf = self.scheduler.comm_chan_alloc_buf();
                buf.set_payload(0);

                unsafe {
                    buf.append_item(LocalReadInfoItem{
                        table_id: table_id,
                        key:      key,
                    });
                }
                self.msg_map.insert(info_id, buf);
            }
        }

        self.read_seqs.push(ReadReqItem{
            table_id: table_id,
            key:      key,
            read_idx: read_idx,
        });
    }

    pub fn append_update(&mut self, table_id: usize, key: u64, update_idx: usize) {
        let info_id = doca_comm_info_id::LOCAL_LOCK_INFO;
        match self.msg_map.get_mut(&info_id) {
            Some(buf) => {
                unsafe {
                    buf.append_item(LocalLockInfoItem{
                        table_id: table_id,
                        key:      key,
                    });
                }

            }
            None => {
                let mut buf = self.scheduler.comm_chan_alloc_buf();
                buf.set_payload(0);

                unsafe {
                    buf.append_item(LocalLockInfoItem{
                        table_id: table_id,
                        key:      key,
                    });
                }
                self.msg_map.insert(info_id, buf);
            }
        }

        self.update_seqs.push(FetchWriteReqItem{
            table_id:   table_id,
            key:        key,
            update_idx: update_idx,
        });
    }

    pub fn append_lock(&mut self, table_id: usize, key: u64) {
        let info_id = doca_comm_info_id::LOCAL_LOCK_INFO;
        match self.msg_map.get_mut(&info_id) {
            Some(buf) => {
                unsafe {
                    buf.append_item(LocalLockInfoItem{
                        table_id: table_id,
                        key:      key,
                    });
                }

            }
            None => {
                let mut buf = self.scheduler.comm_chan_alloc_buf();
                buf.set_payload(0);

                unsafe {
                    buf.append_item(LocalLockInfoItem{
                        table_id: table_id,
                        key:      key,
                    });
                }
                self.msg_map.insert(info_id, buf);
            }
        }
    }

    pub fn get_read_seqs(&self) -> &Vec<ReadReqItem> {
        &self.read_seqs
    }

    pub fn get_update_seqs(&self) -> &Vec<FetchWriteReqItem> {
        &self.update_seqs
    }

    pub async fn wait_until_done(&mut self) {
        let replys = self.scheduler.yield_until_comm_ready(self.cid).await;
    
        let two_answer = (self.update_seqs.len() > 0) && (self.read_seqs.len() > 0);
        
        let success0 = replys.get_item::<DocaCommReply>(0).success;
        let mut success1 = true;
        if two_answer {
            success1 = replys.get_item::<DocaCommReply>(std::mem::size_of::<DocaCommReply>()).success;
        }

        self.success = success0 && success1;
    }

    pub fn get_success(&self) -> bool {
        self.success
    }
}