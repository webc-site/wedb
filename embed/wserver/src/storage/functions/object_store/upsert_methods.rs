use super::object_session_functions::ObjectSessionFunctions;
use crate::inputs::ObjectInput;
use crate::storage::functions::main_store::upsert_methods::{LogRecord, UpsertInfo, RecordSizeInfo};

pub struct GarnetObject;

impl ObjectSessionFunctions {
    /// garnet相对路径:garnet/libs/server/Storage/Functions/ObjectStore/UpsertMethods.cs:InitialWriter
    pub fn initial_writer(&self, _dst_log_record: &mut LogRecord, _size_info: &RecordSizeInfo, _input: &mut ObjectInput, _src_value: &mut GarnetObject, _output: &mut (), _upsert_info: &mut UpsertInfo) -> bool {
        true
    }

    /// garnet相对路径:garnet/libs/server/Storage/Functions/ObjectStore/UpsertMethods.cs:PostInitialWriter
    pub fn post_initial_writer(&self, _log_record: &mut LogRecord, _size_info: &RecordSizeInfo, _input: &mut ObjectInput, _src_value: &mut GarnetObject, _output: &mut (), upsert_info: &mut UpsertInfo) {
        upsert_info.user_data |= Self::NEED_AOF_LOG;
    }

    /// garnet相对路径:garnet/libs/server/Storage/Functions/ObjectStore/UpsertMethods.cs:InPlaceWriter
    pub fn in_place_writer(&self, _log_record: &mut LogRecord, _input: &mut ObjectInput, _src_value: &mut GarnetObject, _output: &mut (), upsert_info: &mut UpsertInfo) -> bool {
        upsert_info.user_data |= Self::NEED_AOF_LOG;
        true
    }

    /// garnet相对路径:garnet/libs/server/Storage/Functions/ObjectStore/UpsertMethods.cs:PostUpsertOperation
    pub fn post_upsert_operation(&self, _key: &[u8], _input: &mut ObjectInput, _value_span: &mut GarnetObject, upsert_info: &mut UpsertInfo) {
        if (upsert_info.user_data & Self::NEED_AOF_LOG) == Self::NEED_AOF_LOG {
            // WriteLogUpsert
        }
    }
}
