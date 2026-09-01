pub const LOG_META_FAMILY: &str = "_log_meta";
pub const LOG_DATA_FAMILY: &str = "_log_data";
pub const SM_META_FAMILY: &str = "_sm_meta";
pub const SM_DATA_FAMILY: &str = "_sm_data";

pub const LAST_APPLIED_LOG_KEY: &[u8] = b"last_applied_log";
pub const NODES_KEY: &[u8] = b"nodes";
pub const TTL_KEY_PREFIX: &[u8] = b"_ttl:";
pub const TTL_IDX_KEY_PREFIX: &[u8] = b"_ttl_idx:";
