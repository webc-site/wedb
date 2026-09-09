use super::main_session_functions::MainSessionFunctions;
use crate::inputs::StringInput;
use super::upsert_methods::{LogRecord, StringOutput, RecordSizeInfo};

pub struct RMWInfo {
    pub key_hash: i64,
    pub user_data: u8,
    pub action: i32,
    pub version: i64,
    pub session_id: i64,
    pub source_address: i64,
}

impl MainSessionFunctions {
    /// garnet相对路径:garnet/libs/server/Storage/Functions/MainStore/RMWMethods.cs:NeedInitialUpdate
    pub fn need_initial_update(&self, _key: &[u8], _input: &mut StringInput, _output: &mut StringOutput, _rmw_info: &mut RMWInfo) -> bool {
        true
    }

    /// garnet相对路径:garnet/libs/server/Storage/Functions/MainStore/RMWMethods.cs:InitialUpdater
    pub fn initial_updater(&self, _key: &[u8], _input: &mut StringInput, _value: &mut LogRecord, _output: &mut StringOutput, _rmw_info: &mut RMWInfo, _record_info: &mut LogRecord) -> bool {
        true
    }

    /// garnet相对路径:garnet/libs/server/Storage/Functions/MainStore/RMWMethods.cs:PostInitialUpdater
    pub fn post_initial_updater(&self, _key: &[u8], _input: &mut StringInput, _value: &mut LogRecord, _output: &mut StringOutput, rmw_info: &mut RMWInfo, _record_info: &mut LogRecord) {
        rmw_info.user_data |= Self::NEED_AOF_LOG;
    }

    /// garnet相对路径:garnet/libs/server/Storage/Functions/MainStore/RMWMethods.cs:InPlaceUpdater
    pub fn in_place_updater(&self, _key: &[u8], _input: &mut StringInput, _value: &mut LogRecord, _output: &mut StringOutput, rmw_info: &mut RMWInfo, _record_info: &mut LogRecord) -> bool {
        rmw_info.user_data |= Self::NEED_AOF_LOG;
        true
    }

    /// garnet相对路径:garnet/libs/server/Storage/Functions/MainStore/RMWMethods.cs:NeedCopyUpdate
    pub fn need_copy_update(&self, _key: &[u8], _input: &mut StringInput, _old_value: &[u8], _output: &mut StringOutput, _rmw_info: &mut RMWInfo) -> bool {
        true
    }

    /// garnet相对路径:garnet/libs/server/Storage/Functions/MainStore/RMWMethods.cs:CopyUpdater
    pub fn copy_updater(&self, _key: &[u8], _input: &mut StringInput, _old_value: &[u8], _new_value: &mut LogRecord, _output: &mut StringOutput, _rmw_info: &mut RMWInfo, _record_info: &mut LogRecord) -> bool {
        true
    }

    /// garnet相对路径:garnet/libs/server/Storage/Functions/MainStore/RMWMethods.cs:PostCopyUpdater
    pub fn post_copy_updater(&self, _key: &[u8], _input: &mut StringInput, _old_value: &[u8], _new_value: &mut LogRecord, _output: &mut StringOutput, rmw_info: &mut RMWInfo, _record_info: &mut LogRecord) -> bool {
        rmw_info.user_data |= Self::NEED_AOF_LOG;
        true
    }

    /// garnet相对路径:garnet/libs/server/Storage/Functions/MainStore/RMWMethods.cs:PostRMWOperation
    pub fn post_rmw_operation(&self, _key: &[u8], _input: &mut StringInput, rmw_info: &mut RMWInfo) {
        if (rmw_info.user_data & Self::NEED_AOF_LOG) == Self::NEED_AOF_LOG {
            // WriteLogRMW
        }
    }
}
