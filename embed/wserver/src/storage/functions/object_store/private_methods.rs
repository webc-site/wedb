use super::{object_session_functions::ObjectSessionFunctions, upsert_methods::GarnetObject};
use crate::inputs::ObjectInput;

impl ObjectSessionFunctions {
  /// garnet相对路径:garnet/libs/server/Storage/Functions/ObjectStore/PrivateMethods.cs:WriteLogUpsert
  pub fn write_log_upsert(
    &self,
    _key: &[u8],
    _input: &mut ObjectInput,
    _value: &mut GarnetObject,
    _version: i64,
    _session_id: i64,
  ) {
  }

  /// garnet相对路径:garnet/libs/server/Storage/Functions/ObjectStore/PrivateMethods.cs:WriteLogRMW
  pub fn write_log_rmw(
    &self,
    _key: &[u8],
    _input: &mut ObjectInput,
    _version: i64,
    _session_id: i64,
  ) {
  }

  /// garnet相对路径:garnet/libs/server/Storage/Functions/ObjectStore/PrivateMethods.cs:WriteLogDelete
  pub fn write_log_delete(&self, _key: &[u8], _version: i64, _session_id: i64) {}
}
