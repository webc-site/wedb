use super::object_session_functions::ObjectSessionFunctions;
use crate::{
  inputs::ObjectInput, objects::types::object_output::ObjectOutput,
  storage::functions::main_store::upsert_methods::LogRecord,
};

pub struct ReadInfo {
  pub user_data: u8,
  pub action: i32,
  pub version: i64,
  pub session_id: i64,
}

impl ObjectSessionFunctions {
  /// garnet相对路径:garnet/libs/server/Storage/Functions/ObjectStore/ReadMethods.cs:SingleReader
  pub fn single_reader(
    &self,
    _key: &[u8],
    input: &mut ObjectInput,
    _value: &[u8],
    _dst: &mut ObjectOutput,
    _read_info: &mut ReadInfo,
  ) -> bool {
    // Check if value is object
    // if !srcLogRecord.DataHeader.ValueIsObject {
    //    readInfo.Action = ReadAction.WrongType;
    //    return false;
    // }

    // Check expiration
    // if srcLogRecord.DataHeader.HasExpiration && srcLogRecord.Expiration < DateTimeOffset.Now.UtcTicks {
    //    readInfo.Action = ReadAction.Expire;
    //    return false;
    // }

    if input.header.data[0] != 0 {
      if input.header.data[0] == 255 {
        // garnetObject.Operate
        return true;
      }

      // GetCustomObjectCommand
      return true;
    }

    // dst.GarnetObject = (IGarnetObject)srcLogRecord.ValueObject;
    true
  }

  /// garnet相对路径:garnet/libs/server/Storage/Functions/ObjectStore/ReadMethods.cs:ConcurrentReader
  pub fn concurrent_reader(
    &self,
    key: &[u8],
    input: &mut ObjectInput,
    value: &[u8],
    dst: &mut ObjectOutput,
    read_info: &mut ReadInfo,
    _record_info: &LogRecord,
  ) -> bool {
    self.single_reader(key, input, value, dst, read_info)
  }
}
