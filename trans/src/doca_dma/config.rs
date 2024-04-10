/*
 *  This code refer to smartnic-bench from smartnickit-project
 * 
 *  https://github.com/smartnickit-project/smartnic-bench.git
 * 
 */

use doca::{ RawPointer, RawPointerMsg, DOCAMmap };
use serde_derive::{ Serialize, Deserialize };

use crate::common::connection::ConfigSerialize;

pub const DOCA_MAX_CONN_LENGTH: usize = 4096;

#[derive(Serialize, Deserialize)]
pub struct DocaConnInfoMsg {
    pub exports: Vec<Vec<u8>>,
    pub buffers: Vec<RawPointerMsg>,
}

#[derive(Clone)]
pub struct DocaConnInfo {
    pub exports: Vec<Vec<u8>>,
    pub buffers: Vec<RawPointer>,
}

impl Default for DocaConnInfo {
    fn default() -> Self {
        Self {
            exports: Vec::new(),
            buffers: Vec::new(),
        }
    }
}

impl From<DocaConnInfo> for DocaConnInfoMsg {
    fn from(info: DocaConnInfo) -> Self {
        Self {
            exports: info.exports,
            buffers: info.buffers.into_iter().map(|v| v.into()).collect(),
        }
    }
}

impl From<DocaConnInfoMsg> for DocaConnInfo {
    fn from(msg: DocaConnInfoMsg) -> Self {
        Self {
            exports: msg.exports,
            buffers: msg.buffers.into_iter().map(|v| v.into()).collect(),
        }
    }
}

impl ConfigSerialize for DocaConnInfoMsg {
    fn serialize(data: DocaConnInfoMsg) -> Vec<u8> {
        serde_json::to_vec(&data).unwrap()
    }

    fn deserialize(data: &[u8]) -> DocaConnInfoMsg {
        let msg: DocaConnInfoMsg = serde_json::from_slice(data).unwrap();
        msg
    }
}