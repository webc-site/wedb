use super::object_session_functions::ObjectSessionFunctions;
use crate::inputs::ObjectInput;
use crate::storage::functions::main_store::read_methods::ReadInfo;
use crate::storage::functions::main_store::upsert_methods::LogRecord;
use super::upsert_methods::GarnetObject;

impl ObjectSessionFunctions {
    /// garnet相对路径:garnet/libs/server/Storage/Functions/ObjectStore/ReadMethods.cs:SingleReader
    pub fn single_reader(&self, _key: &[u8], _input: &mut ObjectInput, _value: &GarnetObject, _dst: &mut (), _read_info: &mut ReadInfo) -> bool {
        true
    }

    /// garnet相对路径:garnet/libs/server/Storage/Functions/ObjectStore/ReadMethods.cs:ConcurrentReader
    pub fn concurrent_reader(&self, _key: &[u8], _input: &mut ObjectInput, _value: &GarnetObject, _dst: &mut (), _read_info: &mut ReadInfo, _record_info: &LogRecord) -> bool {
        true
    }
}
