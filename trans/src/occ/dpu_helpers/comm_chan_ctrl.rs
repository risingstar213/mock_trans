use std::sync::Arc;
use std::collections::{HashMap, HashSet};

use crate::doca_comm_chan::{ doca_comm_info_type, DocaCommHeaderMeta };
use crate::doca_comm_chan::comm_buf::DocaCommBuf;
use crate::framework::scheduler::AsyncScheduler;
use super::super::remote_helpers::{ ReadReqItem, FetchWriteReqItem };
use super::*;

pub struct CommChanCtrl {
    pid:       u32,
    tid:       u32,
    cid:       u32,
    ver:       u32,
    scheduler: Arc<AsyncScheduler>,
    msg_set:   HashSet<u32>,
    read_infos: Vec<LocalReadInfoItem>,
    lock_infos: Vec<LocalLockInfoItem>,
    read_seqs: Vec<ReadReqItem>,
    update_seqs: Vec<FetchWriteReqItem>,
    success:   bool,
}

impl Drop for CommChanCtrl {
    fn drop(&mut self) {
        self.restart_batch();
    }
}

impl CommChanCtrl {
    pub fn new(scheduler: &Arc<AsyncScheduler>, pid: u32, tid: u32, cid: u32) -> Self {
        Self {
            pid:       pid,
            tid:       tid,
            cid:       cid,
            ver:       scheduler.alloc_new_ver(cid),
            scheduler: scheduler.clone(),
            msg_set:  HashSet::new(),
            read_infos: Vec::new(),
            lock_infos: Vec::new(),
            read_seqs: Vec::new(),
            update_seqs: Vec::new(),
            success: true,
        }
    }

    pub fn restart_batch(&mut self) {
        self.msg_set.clear();
        self.read_infos.clear();
        self.lock_infos.clear();
        self.read_seqs.clear();
        self.update_seqs.clear();
        self.success = true;
    }

    pub fn send_comm_info(&mut self) {
        let mut info_num = self.msg_set.len();
        if !self.read_infos.is_empty() {
            info_num += 1;
        }
        if !self.lock_infos.is_empty() {
            info_num += 1;
        }
        self.scheduler.prepare_comm_replys(self.cid, info_num);

        if !self.read_infos.is_empty() {
            let payload = std::mem::size_of::<LocalReadInfoItem>() * self.read_infos.len();
            let header = DocaCommHeaderMeta {
                info_type:    doca_comm_info_type::REQ,
                info_id:      doca_comm_info_id::LOCAL_READ_INFO,
                info_payload: payload as _,
                info_pid:     self.pid,
                info_tid:     self.tid,
                info_cid:     self.cid,
            };

            self.scheduler.comm_chan_append_slice_info(
                header, 
                self.read_infos.as_slice(),
            );
        }

        if !self.lock_infos.is_empty() {
            let payload = std::mem::size_of::<LocalLockInfoItem>() * self.lock_infos.len();
            let header = DocaCommHeaderMeta {
                info_type:    doca_comm_info_type::REQ,
                info_id:      doca_comm_info_id::LOCAL_LOCK_INFO,
                info_payload: payload as _,
                info_pid:     self.pid,
                info_tid:     self.tid,
                info_cid:     self.cid,
            };

            self.scheduler.comm_chan_append_slice_info(
                header, 
                self.lock_infos.as_slice(),
            );
        }
        
        for info_id in &self.msg_set {
            self.scheduler.comm_chan_append_empty_info(DocaCommHeaderMeta{
                info_type:    doca_comm_info_type::REQ,
                info_id:      *info_id,
                info_payload: 0,
                info_pid:     self.pid,
                info_tid:     self.tid,
                info_cid:     self.cid,
            });
        }
    }

    pub fn append_info(&mut self, info_id: u32) {
        match self.msg_set.get(&info_id) {
            Some(_) => {}
            None => {
                self.msg_set.insert(info_id);
            }
        }
    }

    pub fn append_read(&mut self, table_id: usize, key: u64, read_idx: usize) {
        self.read_infos.push(LocalReadInfoItem{
            table_id: table_id,
            key:      key,
        });

        self.read_seqs.push(ReadReqItem{
            table_id: table_id,
            key:      key,
            read_idx: read_idx,
        });
    }

    pub fn append_update(&mut self, table_id: usize, key: u64, update_idx: usize) {
        self.lock_infos.push(LocalLockInfoItem{
            table_id: table_id,
            key:      key,
        });

        self.update_seqs.push(FetchWriteReqItem{
            table_id:   table_id,
            key:        key,
            update_idx: update_idx,
        });
    }

    pub fn append_lock(&mut self, table_id: usize, key: u64) {
        self.lock_infos.push(LocalLockInfoItem{
            table_id: table_id,
            key:      key,
        });
    }

    pub fn get_read_seqs(&self) -> &Vec<ReadReqItem> {
        &self.read_seqs
    }

    pub fn get_update_seqs(&self) -> &Vec<FetchWriteReqItem> {
        &self.update_seqs
    }

    pub async fn wait_until_done(&mut self) {
        self.success = self.scheduler.yield_until_comm_ready(self.cid).await;
    }

    pub fn get_success(&self) -> bool {
        self.success
    }
}