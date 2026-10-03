use wresp::cmd_strings::RESP_ERR_INVALID_MIGRATED_VECTOR_SET_INDEX;

/// Vector Set 预览未启用统一文案（命令面与迁移面共用，单点定义于此）
pub(crate) const ERR_VECTOR_SET_DISABLED: &[u8] =
  b"ERR Vector Set (preview) commands are not enabled";
/// 迁移索引记录非法文案（garnet 无对应文案，字面量单点在 wresp::cmd_strings）
pub(crate) const ERR_MIGRATED_INDEX: &[u8] = RESP_ERR_INVALID_MIGRATED_VECTOR_SET_INDEX.as_bytes();

pub mod resp_server_session_vectors;
pub mod vector_manager;
pub mod vector_manager_cleanup;
pub mod vector_manager_context_metadata;
pub mod vector_manager_filter;
pub mod vector_manager_index;
pub mod vector_manager_locking;
pub mod vector_manager_migration;
pub mod vector_manager_quantization;
pub mod vector_manager_replication;
pub mod vector_registry_recovery;
pub mod vector_store_callbacks;
