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
  /// libs/server/Storage/Functions/MainStore/ReadMethods.cs:SingleReader
  pub fn single_reader(
    &self,
    _key: &[u8],
    _input: &mut StringInput,
    _value: &[u8],
    _dst: &mut StringOutput,
    _read_info: &mut ReadInfo,
  ) -> bool {
    true
  }

  /// libs/server/Storage/Functions/MainStore/ReadMethods.cs:ConcurrentReader
  pub fn concurrent_reader(
    &self,
    _key: &[u8],
    _input: &mut StringInput,
    _value: &[u8],
    _dst: &mut StringOutput,
    _read_info: &mut ReadInfo,
    _record_info: &LogRecord,
  ) -> bool {
    true
  }
}
