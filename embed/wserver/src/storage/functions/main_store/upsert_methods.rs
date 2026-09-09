use super::main_session_functions::MainSessionFunctions;
use crate::inputs::StringInput;

pub struct LogRecord;
pub struct UpsertInfo {
  pub key_hash: i64,
  pub user_data: u8,
  pub action: i32,
  pub version: i64,
  pub session_id: i64,
}
pub struct RecordSizeInfo;
pub struct StringOutput;

impl MainSessionFunctions {
  /// garnet相对路径:garnet/libs/server/Storage/Functions/MainStore/UpsertMethods.cs:InitialWriter
  pub fn initial_writer(
    &self,
    _dst_log_record: &mut LogRecord,
    _size_info: &RecordSizeInfo,
    input: &mut StringInput,
    _src_value: &[u8],
    _output: &mut StringOutput,
    _upsert_info: &mut UpsertInfo,
  ) -> bool {
    // try_set_value_span_and_prepare_optionals(src_value, size_info)
    if input.arg1 != 0 {
      // dstLogRecord.TrySetExpiration(input.arg1)
    }
    true
  }

  /// garnet相对路径:garnet/libs/server/Storage/Functions/MainStore/UpsertMethods.cs:PostInitialWriter
  pub fn post_initial_writer(
    &self,
    _log_record: &mut LogRecord,
    _size_info: &RecordSizeInfo,
    _input: &mut StringInput,
    _src_value: &[u8],
    _output: &mut StringOutput,
    upsert_info: &mut UpsertInfo,
  ) {
    // functionsState.watchVersionMap.IncrementVersion(upsertInfo.KeyHash);
    // if functionsState.appendOnlyFile != null
    upsert_info.user_data |= Self::NEED_AOF_LOG;
  }

  /// garnet相对路径:garnet/libs/server/Storage/Functions/MainStore/UpsertMethods.cs:InPlaceWriter
  pub fn in_place_writer(
    &self,
    _log_record: &mut LogRecord,
    _input: &mut StringInput,
    _src_value: &[u8],
    _output: &mut StringOutput,
    upsert_info: &mut UpsertInfo,
  ) -> bool {
    // Prevent SET from overwriting VectorSet or RangeIndex stubs.
    // var recordType = logRecord.RecordType;
    // if recordType != 0 && (recordType == VectorManager.RecordType || recordType == RangeIndexManager.RangeIndexRecordType) {
    //     upsertInfo.Action = UpsertAction.WrongType;
    //     return false;
    // }

    upsert_info.user_data |= Self::NEED_AOF_LOG;
    true
  }

  /// garnet相对路径:garnet/libs/server/Storage/Functions/MainStore/UpsertMethods.cs:PostUpsertOperation
  pub fn post_upsert_operation(
    &self,
    _key: &[u8],
    _input: &mut StringInput,
    _value_span: &[u8],
    upsert_info: &mut UpsertInfo,
  ) {
    if (upsert_info.user_data & Self::NEED_AOF_LOG) == Self::NEED_AOF_LOG {
      // WriteLogUpsert
    }
  }

  /// SingleWriter (mapped from old tsavorite or wrapper)
  pub fn single_writer(
    &self,
    _key: &[u8],
    input: &mut StringInput,
    src_value: &[u8],
    dst: &mut StringOutput,
    dst_log_record: &mut LogRecord,
    upsert_info: &mut UpsertInfo,
    size_info: &RecordSizeInfo,
  ) -> bool {
    self.initial_writer(
      dst_log_record,
      size_info,
      input,
      src_value,
      dst,
      upsert_info,
    )
  }
}
