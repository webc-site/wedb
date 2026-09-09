use super::object_session_functions::ObjectSessionFunctions;
use crate::inputs::ObjectInput;
use crate::storage::functions::main_store::upsert_methods::LogRecord;
use crate::storage::functions::main_store::rmw_methods::RMWInfo;
use super::upsert_methods::GarnetObject;

impl ObjectSessionFunctions {
    /// garnet相对路径:garnet/libs/server/Storage/Functions/ObjectStore/RMWMethods.cs:NeedInitialUpdate
    pub fn need_initial_update(&self, _key: &[u8], _input: &mut ObjectInput, _output: &mut (), _rmw_info: &mut RMWInfo) -> bool {
        true
    }

    /// garnet相对路径:garnet/libs/server/Storage/Functions/ObjectStore/RMWMethods.cs:InitialUpdater
    pub fn initial_updater(&self, _key: &[u8], _input: &mut ObjectInput, _value: &mut LogRecord, _output: &mut (), _rmw_info: &mut RMWInfo, _record_info: &mut LogRecord) -> bool {
        true
    }

    /// garnet相对路径:garnet/libs/server/Storage/Functions/ObjectStore/RMWMethods.cs:PostInitialUpdater
    pub fn post_initial_updater(&self, _key: &[u8], _input: &mut ObjectInput, _value: &mut LogRecord, _output: &mut (), rmw_info: &mut RMWInfo, _record_info: &mut LogRecord) {
        rmw_info.user_data |= Self::NEED_AOF_LOG;
    }

    /// garnet相对路径:garnet/libs/server/Storage/Functions/ObjectStore/RMWMethods.cs:InPlaceUpdater
    pub fn in_place_updater(&self, _key: &[u8], _input: &mut ObjectInput, _value: &mut LogRecord, _output: &mut (), rmw_info: &mut RMWInfo, _record_info: &mut LogRecord) -> bool {
        rmw_info.user_data |= Self::NEED_AOF_LOG;
        true
    }

    /// garnet相对路径:garnet/libs/server/Storage/Functions/ObjectStore/RMWMethods.cs:NeedCopyUpdate
    pub fn need_copy_update(&self, _key: &[u8], _input: &mut ObjectInput, _old_value: &mut GarnetObject, _output: &mut (), _rmw_info: &mut RMWInfo) -> bool {
        true
    }

    /// garnet相对路径:garnet/libs/server/Storage/Functions/ObjectStore/RMWMethods.cs:CopyUpdater
    pub fn copy_updater(&self, _key: &[u8], _input: &mut ObjectInput, _old_value: &mut GarnetObject, _new_value: &mut LogRecord, _output: &mut (), _rmw_info: &mut RMWInfo, _record_info: &mut LogRecord) -> bool {
        true
    }

    /// garnet相对路径:garnet/libs/server/Storage/Functions/ObjectStore/RMWMethods.cs:PostCopyUpdater
    pub fn post_copy_updater(&self, _key: &[u8], _input: &mut ObjectInput, _old_value: &mut GarnetObject, _new_value: &mut LogRecord, _output: &mut (), rmw_info: &mut RMWInfo, _record_info: &mut LogRecord) -> bool {
        rmw_info.user_data |= Self::NEED_AOF_LOG;
        true
    }

    /// garnet相对路径:garnet/libs/server/Storage/Functions/ObjectStore/RMWMethods.cs:PostRMWOperation
    pub fn post_rmw_operation(&self, _key: &[u8], _input: &mut ObjectInput, rmw_info: &mut RMWInfo) {
        if (rmw_info.user_data & Self::NEED_AOF_LOG) == Self::NEED_AOF_LOG {
            // WriteLogRMW
        }
    }
}
