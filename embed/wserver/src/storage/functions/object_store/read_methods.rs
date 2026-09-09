use super::{object_session_functions::ObjectSessionFunctions, upsert_methods::GarnetObject};
use crate::{
  inputs::ObjectInput,
  storage::functions::main_store::{read_methods::ReadInfo, upsert_methods::LogRecord},
};

impl ObjectSessionFunctions {
  /// garnet相对路径:garnet/libs/server/Storage/Functions/ObjectStore/ReadMethods.cs:SingleReader
  pub fn single_reader(
    &self,
    _key: &[u8],
    _input: &mut ObjectInput,
    _value: &GarnetObject,
    _dst: &mut (),
    _read_info: &mut ReadInfo,
  ) -> bool {
    true
  }

  /// garnet相对路径:garnet/libs/server/Storage/Functions/ObjectStore/ReadMethods.cs:ConcurrentReader
  pub fn concurrent_reader(
    &self,
    _key: &[u8],
    _input: &mut ObjectInput,
    _value: &GarnetObject,
    _dst: &mut (),
    _read_info: &mut ReadInfo,
    _record_info: &LogRecord,
  ) -> bool {
    true
  }
}
