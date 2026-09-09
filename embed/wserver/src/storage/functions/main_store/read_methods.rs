use super::main_session_functions::MainSessionFunctions;
use crate::inputs::StringInput;
use super::upsert_methods::{LogRecord, StringOutput};

pub struct ReadInfo {
    pub user_data: u8,
    pub action: i32,
    pub version: i64,
    pub session_id: i64,
}

impl MainSessionFunctions {
    /// garnet相对路径:garnet/libs/server/Storage/Functions/MainStore/ReadMethods.cs:SingleReader
    pub fn single_reader(&self, _key: &[u8], _input: &mut StringInput, _value: &[u8], _dst: &mut StringOutput, _read_info: &mut ReadInfo) -> bool {
        true
    }

    /// garnet相对路径:garnet/libs/server/Storage/Functions/MainStore/ReadMethods.cs:ConcurrentReader
    pub fn concurrent_reader(&self, _key: &[u8], _input: &mut StringInput, _value: &[u8], _dst: &mut StringOutput, _read_info: &mut ReadInfo, _record_info: &LogRecord) -> bool {
        true
    }
}
