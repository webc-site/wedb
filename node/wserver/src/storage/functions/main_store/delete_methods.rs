use super::{main_session_functions::MainSessionFunctions, upsert_methods::LogRecord};

pub struct DeleteInfo {
  pub key_hash: i64,
  pub user_data: u8,
  pub action: i32,
  pub version: i64,
  pub session_id: i64,
}

impl MainSessionFunctions {
  /// garnet相对路径:garnet/libs/server/Storage/Functions/MainStore/DeleteMethods.cs:SingleDeleter
  pub fn single_deleter(
    &self,
    _key: &[u8],
    _value: &mut LogRecord,
    _delete_info: &mut DeleteInfo,
    _record_info: &mut LogRecord,
  ) -> bool {
    true
  }

  /// garnet相对路径:garnet/libs/server/Storage/Functions/MainStore/DeleteMethods.cs:PostSingleDeleter
  pub fn post_single_deleter(&self, _key: &[u8], _delete_info: &mut DeleteInfo) {
    // functionsState.watchVersionMap.IncrementVersion(deleteInfo.KeyHash);
    _delete_info.user_data |= Self::NEED_AOF_LOG;
  }

  /// garnet相对路径:garnet/libs/server/Storage/Functions/MainStore/DeleteMethods.cs:ConcurrentDeleter
  pub fn concurrent_deleter(
    &self,
    _key: &[u8],
    _value: &mut LogRecord,
    _delete_info: &mut DeleteInfo,
    _record_info: &mut LogRecord,
  ) -> bool {
    true
  }
}
