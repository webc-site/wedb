use super::object_session_functions::ObjectSessionFunctions;
use crate::inputs::ObjectInput;
use crate::storage::functions::main_store::upsert_methods::LogRecord;
use crate::storage::functions::main_store::delete_methods::DeleteInfo;

impl ObjectSessionFunctions {
    /// garnet相对路径:garnet/libs/server/Storage/Functions/ObjectStore/DeleteMethods.cs:InitialDeleter
    pub fn initial_deleter(&self, _log_record: &mut LogRecord, _delete_info: &mut DeleteInfo) -> bool {
        true
    }

    /// garnet相对路径:garnet/libs/server/Storage/Functions/ObjectStore/DeleteMethods.cs:PostInitialDeleter
    pub fn post_initial_deleter(&self, _log_record: &mut LogRecord, delete_info: &mut DeleteInfo) {
        delete_info.user_data |= Self::NEED_AOF_LOG;
    }

    /// garnet相对路径:garnet/libs/server/Storage/Functions/ObjectStore/DeleteMethods.cs:InPlaceDeleter
    pub fn in_place_deleter(&self, _log_record: &mut LogRecord, delete_info: &mut DeleteInfo) -> bool {
        delete_info.user_data |= Self::NEED_AOF_LOG;
        true
    }

    /// garnet相对路径:garnet/libs/server/Storage/Functions/ObjectStore/DeleteMethods.cs:PostDeleteOperation
    pub fn post_delete_operation(&self, _key: &[u8], delete_info: &mut DeleteInfo) {
        if (delete_info.user_data & Self::NEED_AOF_LOG) == Self::NEED_AOF_LOG {
            // WriteLogDelete
        }
    }
}
