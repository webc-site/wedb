pub mod acl_commands;
pub mod admin_commands;
pub mod array_commands;
pub mod async_processor;
pub mod basic_commands;
pub mod basic_etag_commands;
pub mod bitmap;
pub mod byte_array_comparer;
pub mod client_commands;
pub mod cmd_strings;
pub mod hyperloglog;
pub mod i_resp_serializable;
pub mod key_admin_commands;
pub mod m_get_read_arg_batch;
pub mod objects;
pub mod parser;
pub mod pub_sub_commands;
pub mod purge_bp_command;
pub mod rangeindex;
pub mod rdb_crc64;
pub mod resp_command_argument;
pub mod resp_command_data_common;
pub mod resp_command_data_provider;
pub mod resp_command_docs;
pub mod resp_command_info_simplified_structs;
pub mod resp_command_key_specification;
pub mod resp_commands_info;
pub mod resp_server_session;
pub mod resp_server_session_output;
pub mod resp_server_session_slot_verify;
pub mod session_logger;
pub mod ttl_sync;
pub mod vector;

/// 单测共用装具：临时单文件库 + 批处理纪元会话（各命令文件的 #[cfg(test)] 共享，
/// 一处定义避免逐文件复制）
#[cfg(test)]
pub(crate) mod batch_harness {
  use std::sync::Arc;

  use compio::runtime::Runtime;
  use wdev::SegmentedDevice;
  use wkv::{StoreConfig, WedbStore};

  use super::resp_server_session::RespServerSession;

  pub(crate) type Batch<'a> = wkv::BatchStoreSession<'a, SegmentedDevice>;

  pub(crate) fn with_batch(f: impl FnOnce(&mut RespServerSession, &Batch)) {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
      let dir = tempfile::tempdir().unwrap();
      let device = Arc::new(SegmentedDevice::single_file(dir.path().join("resp.db")).unwrap());
      let mut config = StoreConfig::new(1024, 4096, 16, 0.5).unwrap();
      config.gc.enabled = false;
      let store = Arc::new(WedbStore::open(config, device).unwrap());
      let session = store.new_session().unwrap();
      let batch = session.enter_batch();
      let mut s = RespServerSession;
      f(&mut s, &batch);
    });
  }
}
