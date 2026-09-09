use super::{
  main_session_functions::MainSessionFunctions,
  upsert_methods::{LogRecord, StringOutput},
};
use crate::inputs::StringInput;

pub struct ReadInfo {
  pub user_data: u8,
  pub action: i32,
  pub version: i64,
  pub session_id: i64,
}

impl MainSessionFunctions {
  /// garnet相对路径:garnet/libs/server/Storage/Functions/MainStore/ReadMethods.cs:SingleReader
  pub fn single_reader(
    &self,
    _key: &[u8],
    input: &mut StringInput,
    _value: &[u8],
    _dst: &mut StringOutput,
    _read_info: &mut ReadInfo,
  ) -> bool {
    // Fast path for simple GET on a normal inline string key with no optional fields.
    if input.arg1 < 0 {
      // CopyRespTo(value, ref output);
      return true;
    }

    // In a real implementation we'd check DataHeader for HasOptionalOrObjectFields,
    // RecordType, Expiry, etc.
    true
  }

  /// garnet相对路径:garnet/libs/server/Storage/Functions/MainStore/ReadMethods.cs:ConcurrentReader
  pub fn concurrent_reader(
    &self,
    key: &[u8],
    input: &mut StringInput,
    value: &[u8],
    dst: &mut StringOutput,
    read_info: &mut ReadInfo,
    _record_info: &LogRecord,
  ) -> bool {
    self.single_reader(key, input, value, dst, read_info)
  }
}
