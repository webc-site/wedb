use super::main_session_functions::MainSessionFunctions;

// Dummy structs to match the signature
pub struct LogRecord;
pub struct DeleteInfo {
  pub key_hash: i64,
  pub user_data: u8,
  pub version: i64,
  pub session_id: i64,
}

impl MainSessionFunctions {
  /// garnet相对路径:garnet/libs/server/Storage/Functions/MainStore/DeleteMethods.cs:InitialDeleter
  pub fn initial_deleter(
    &self,
    _log_record: &mut LogRecord,
    _delete_info: &mut DeleteInfo,
  ) -> bool {
    // functionsState.watchVersionMap.IncrementVersion(deleteInfo.KeyHash);
    true
  }

  /// garnet相对路径:garnet/libs/server/Storage/Functions/MainStore/DeleteMethods.cs:PostInitialDeleter
  pub fn post_initial_deleter(&self, _log_record: &mut LogRecord, delete_info: &mut DeleteInfo) {
    // if (functionsState.appendOnlyFile != null)
    delete_info.user_data |= Self::NEED_AOF_LOG;
  }

  /// garnet相对路径:garnet/libs/server/Storage/Functions/MainStore/DeleteMethods.cs:InPlaceDeleter
  pub fn in_place_deleter(
    &self,
    _log_record: &mut LogRecord,
    delete_info: &mut DeleteInfo,
  ) -> bool {
    // logRecord.ClearOptionals();
    // if (!logRecord.Info.Modified) functionsState.watchVersionMap.IncrementVersion(deleteInfo.KeyHash);
    delete_info.user_data |= Self::NEED_AOF_LOG;
    true
  }

  /// garnet相对路径:garnet/libs/server/Storage/Functions/MainStore/DeleteMethods.cs:PostDeleteOperation
  pub fn post_delete_operation(&self, _key: &[u8], delete_info: &mut DeleteInfo) {
    if (delete_info.user_data & Self::NEED_AOF_LOG) == Self::NEED_AOF_LOG {
      // WriteLogDelete(key, deleteInfo.Version, deleteInfo.SessionID, epochAccessor);
    }
  }
}
