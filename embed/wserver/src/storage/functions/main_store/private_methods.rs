use super::{
  main_session_functions::MainSessionFunctions,
  upsert_methods::{LogRecord, StringOutput, UpsertInfo},
};
use crate::inputs::StringInput;

impl MainSessionFunctions {
  /// libs/server/Storage/Functions/MainStore/PrivateMethods.cs:WriteLogUpsert
  pub fn write_log_upsert(
    &self,
    _key: &[u8],
    _input: &mut StringInput,
    _value: &[u8],
    _version: i64,
    _session_id: i64,
  ) {
  }

  /// libs/server/Storage/Functions/MainStore/PrivateMethods.cs:WriteLogRMW
  pub fn write_log_rmw(
    &self,
    _key: &[u8],
    _input: &mut StringInput,
    _version: i64,
    _session_id: i64,
  ) {
  }

  /// libs/server/Storage/Functions/MainStore/PrivateMethods.cs:WriteLogDelete
  pub fn write_log_delete(&self, _key: &[u8], _version: i64, _session_id: i64) {}

  /// libs/server/Storage/Functions/MainStore/PrivateMethods.cs:CopyRespNumber
  pub fn copy_resp_number(&self, _source: &[u8], _dest: &mut [u8]) -> bool {
    true
  }

  /// libs/server/Storage/Functions/MainStore/PrivateMethods.cs:InPlaceWriterForLogRecordValue
  pub fn in_place_writer_for_log_record_value(
    &self,
    _log_record: &mut LogRecord,
    _input: &mut StringInput,
    _input_log_record: &LogRecord,
    _output: &mut StringOutput,
    _upsert_info: &mut UpsertInfo,
  ) -> bool {
    true
  }

  /// libs/server/Storage/Functions/MainStore/PrivateMethods.cs:InPlaceWriterForSpanValue
  pub fn in_place_writer_for_span_value(
    &self,
    _log_record: &mut LogRecord,
    _input: &mut StringInput,
    _src_value: &[u8],
    _output: &mut StringOutput,
    _upsert_info: &mut UpsertInfo,
  ) -> bool {
    true
  }
}
