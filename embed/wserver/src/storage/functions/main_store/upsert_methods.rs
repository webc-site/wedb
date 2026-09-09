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
    pub fn initial_writer(&self, _dst_log_record: &mut LogRecord, _size_info: &RecordSizeInfo, _input: &mut StringInput, _src_value: &[u8], _output: &mut StringOutput, _upsert_info: &mut UpsertInfo) -> bool {
        // dstLogRecord.TrySetValueSpanAndPrepareOptionals...
        true
    }

    /// garnet相对路径:garnet/libs/server/Storage/Functions/MainStore/UpsertMethods.cs:PostInitialWriter
    pub fn post_initial_writer(&self, _log_record: &mut LogRecord, _size_info: &RecordSizeInfo, _input: &mut StringInput, _src_value: &[u8], _output: &mut StringOutput, upsert_info: &mut UpsertInfo) {
        upsert_info.user_data |= Self::NEED_AOF_LOG;
    }

    /// garnet相对路径:garnet/libs/server/Storage/Functions/MainStore/UpsertMethods.cs:InPlaceWriter
    pub fn in_place_writer(&self, _log_record: &mut LogRecord, _input: &mut StringInput, _src_value: &[u8], _output: &mut StringOutput, upsert_info: &mut UpsertInfo) -> bool {
        upsert_info.user_data |= Self::NEED_AOF_LOG;
        true
    }

    /// garnet相对路径:garnet/libs/server/Storage/Functions/MainStore/UpsertMethods.cs:PostUpsertOperation
    pub fn post_upsert_operation(&self, _key: &[u8], _input: &mut StringInput, _value_span: &[u8], upsert_info: &mut UpsertInfo) {
        if (upsert_info.user_data & Self::NEED_AOF_LOG) == Self::NEED_AOF_LOG {
            // WriteLogUpsert
        }
    }
}
