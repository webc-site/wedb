/// 迁移纪元转换等待超时的统一错误文案（C# MigrateSession 等待环失败消息对位；
/// session 换号臂与 driver 各驱动臂共用单源，禁散落裸写字面量）
pub(crate) const ERR_MIGRATE_EPOCH_WAIT: &str = "迁移纪元转换等待失败";

pub mod chunk_reassembler;
pub mod frame_import;
pub mod migrate_driver;
pub mod migrate_session;
pub mod migrate_session_range_index;
pub mod migrate_session_task_store;
pub mod migrate_session_vector_set;
pub mod migrate_state;
pub mod migration_manager;
pub mod sketch;
pub mod sketch_status;
pub mod transfer_option;
