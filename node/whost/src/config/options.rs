#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub enum ClusterPreferredEndpointType {
  #[default]
  Default,
}
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub enum LogCompactionType {
  #[default]
  Default,
}
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub enum ConnectionProtectionOption {
  #[default]
  Default,
}
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub enum LuaMemoryManagementMode {
  #[default]
  Default,
}
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub enum ConfigFileType {
  #[default]
  Default,
}
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub enum X509RevocationMode {
  #[default]
  Default,
}
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub enum CommandLineBooleanOption {
  #[default]
  Default,
}
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub enum DeviceType {
  #[default]
  Default,
}
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub enum GarnetAuthenticationMode {
  #[default]
  Default,
}
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub enum LuaLoggingMode {
  #[default]
  Default,
}
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub enum LogLevel {
  #[default]
  Default,
}

use serde::{Deserialize, Serialize};

/// libs/host/Configuration/Options.cs:Options
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Options {
  pub port: Option<i32>,
  pub address: Option<String>,
  pub cluster_announce_port: Option<i32>,
  pub cluster_announce_ip: Option<String>,
  pub cluster_announce_hostname: Option<String>,
  pub cluster_preferred_endpoint_type: Option<ClusterPreferredEndpointType>,
  pub log_memory_size: Option<String>,
  pub page_size: Option<String>,
  pub page_count: Option<i32>,
  pub segment_size: Option<String>,
  pub object_log_segment_size: Option<String>,
  pub index_memory_size: Option<String>,
  pub index_max_memory_size: Option<String>,
  pub use_native_allocator: Option<bool>,
  pub use_legacy_buffer_pool: Option<bool>,
  pub buffer_pool_memory_budget: Option<String>,
  pub mutable_percent: Option<i32>,
  pub enable_read_cache: Option<bool>,
  pub read_cache_memory_size: Option<String>,
  pub read_cache_page_size: Option<String>,
  pub read_cache_page_count: Option<i32>,
  pub enable_storage_tier: Option<bool>,
  pub copy_reads_to_tail: Option<bool>,
  pub log_dir: Option<String>,
  pub checkpoint_dir: Option<String>,
  pub recover: Option<bool>,
  pub disable_pub_sub: Option<bool>,
  pub pub_sub_page_size: Option<String>,
  pub disable_objects: Option<bool>,
  pub enable_cluster: Option<bool>,
  pub clean_cluster_config: Option<bool>,
  pub parallel_migrate_task_count: Option<i32>,
  pub fast_migrate: Option<bool>,
  pub authentication_mode: Option<GarnetAuthenticationMode>,
  pub password: Option<String>,
  pub cluster_username: Option<String>,
  pub cluster_password: Option<String>,
  pub acl_file: Option<String>,
  pub acl_strict_custom_commands: Option<bool>,
  pub aad_authority: Option<String>,
  pub aad_audiences: Option<String>,
  pub aad_issuers: Option<String>,
  pub authorized_aad_application_ids: Option<String>,
  pub aad_validate_username: Option<bool>,
  pub enable_a_o_f: Option<bool>,
  pub aof_memory_size: Option<String>,
  pub aof_page_size: Option<String>,
  pub aof_segment_size: Option<String>,
  pub aof_physical_sublog_count: Option<i32>,
  pub aof_replay_task_count: Option<i32>,
  pub aof_replay_drift_threshold: Option<i32>,
  pub aof_replay_drift_check_freq: Option<i32>,
  pub aof_replay_barrier_spin_us: Option<i32>,
  pub aof_tail_witness_freq_ms: Option<i32>,
  pub commit_frequency_ms: Option<i32>,
  pub wait_for_commit: Option<bool>,
  pub aof_size_limit: Option<String>,
  pub aof_size_limit_enforce_frequency_secs: Option<i32>,
  pub compaction_frequency_secs: Option<i32>,
  pub expired_object_collection_frequency_secs: Option<i32>,
  pub compaction_type: Option<LogCompactionType>,
  pub compaction_force_delete: Option<bool>,
  pub compaction_max_segments: Option<i32>,
  pub enable_lua: Option<bool>,
  pub lua_transaction_mode: Option<bool>,
  pub gossip_sample_percent: Option<i32>,
  pub gossip_delay: Option<i32>,
  pub cluster_timeout: Option<i32>,
  pub cluster_config_flush_frequency_ms: Option<i32>,
  pub cluster_tls_client_target_host: Option<String>,
  pub server_certificate_required: Option<bool>,
  pub enable_t_l_s: Option<bool>,
  pub cert_file_name: Option<String>,
  pub cert_password: Option<String>,
  pub cert_subject_name: Option<String>,
  pub certificate_refresh_frequency: Option<i32>,
  pub client_certificate_required: Option<bool>,
  pub certificate_revocation_check_mode: Option<X509RevocationMode>,
  pub issuer_certificate_path: Option<String>,
  pub latency_monitor: Option<bool>,
  pub command_stats_monitor: Option<bool>,
  pub slow_log_threshold: Option<i32>,
  pub slow_log_max_entries: Option<i32>,
  pub metrics_sampling_frequency: Option<i32>,
  pub quiet_mode: Option<bool>,
  pub log_level: Option<LogLevel>,
  pub logging_frequency: Option<i32>,
  pub disable_console_logger: Option<bool>,
  pub file_logger: Option<String>,
  pub thread_pool_min_threads: Option<i32>,
  pub thread_pool_max_threads: Option<i32>,
  pub thread_pool_min_i_o_completion_threads: Option<i32>,
  pub thread_pool_max_i_o_completion_threads: Option<i32>,
  pub network_connection_limit: Option<i32>,
  pub use_azure_storage: Option<bool>,
  pub azure_storage_service_uri: Option<String>,
  pub azure_storage_managed_identity: Option<String>,
  pub azure_storage_connection_string: Option<String>,
  pub checkpoint_throttle_flush_delay_ms: Option<i32>,
  pub fast_commit_throttle_freq: Option<i32>,
  pub network_send_throttle_max: Option<i32>,
  pub enable_scatter_gather_get: Option<bool>,
  pub replica_sync_delay_ms: Option<i32>,
  pub aof_replay_max_lag_bytes: Option<i32>,
  pub aof_sync_max_lag_bytes: Option<i64>,
  pub main_memory_replication: Option<bool>,
  pub fast_aof_truncate: Option<bool>,
  pub on_demand_checkpoint: Option<bool>,
  pub replica_diskless_sync: Option<bool>,
  pub replica_diskless_sync_delay: Option<i32>,
  pub replica_attach_timeout: Option<i32>,
  pub replica_sync_timeout: Option<i32>,
  pub replica_diskless_sync_full_sync_aof_threshold: Option<String>,
  pub use_aof_null_device: Option<bool>,
  pub config_import_path: Option<String>,
  pub config_import_format: Option<ConfigFileType>,
  pub config_export_format: Option<ConfigFileType>,
  pub use_azure_storage_for_config_import: Option<bool>,
  pub config_export_path: Option<String>,
  pub use_azure_storage_for_config_export: Option<bool>,
  pub use_native_device_linux: Option<bool>,
  pub device_type: Option<DeviceType>,
  pub device_completion_threads: Option<i32>,
  pub device_throttle_limit: Option<i32>,
  pub device_io_contexts: Option<i32>,
  pub device_queue_depth: Option<i32>,
  pub device_uring_sq_poll: Option<bool>,
  pub device_uring_sq_poll_idle_ms: Option<i32>,
  pub device_aio_max_devices: Option<i32>,
  pub reviv_bin_record_sizes: Option<Vec<String>>,
  pub reviv_bin_record_counts: Option<Vec<String>>,
  pub revivifiable_fraction: Option<f64>,
  pub enable_revivification: Option<bool>,
  pub reviv_number_of_bins_to_search: Option<i32>,
  pub reviv_bin_best_fit_scan_limit: Option<i32>,
  pub reviv_in_chain_only: Option<bool>,
  pub object_scan_count_limit: Option<i32>,
  pub enable_debug_command: Option<ConnectionProtectionOption>,
  pub enable_module_command: Option<ConnectionProtectionOption>,
  pub protected_mode: Option<CommandLineBooleanOption>,
  pub extension_bin_paths: Option<Vec<String>>,
  pub load_module_c_s: Option<Vec<String>>,
  pub extension_allow_unsigned_assemblies: Option<bool>,
  pub index_resize_frequency_secs: Option<i32>,
  pub index_resize_threshold: Option<i32>,
  pub max_inline_key_size: Option<String>,
  pub max_inline_value_size: Option<String>,
  pub initial_i_o_record_size: Option<String>,
  pub fail_on_recovery_error: Option<bool>,
  pub lua_memory_management_mode: Option<LuaMemoryManagementMode>,
  pub lua_script_memory_limit: Option<String>,
  pub lua_script_timeout_ms: Option<i32>,
  pub lua_logging_mode: Option<LuaLoggingMode>,
  pub unix_socket_path: Option<String>,
  pub unix_socket_permission: Option<i32>,
  pub max_databases: Option<i32>,
  pub expired_key_deletion_scan_frequency_secs: Option<i32>,
  pub cluster_replication_reestablishment_timeout: Option<i32>,
  pub cluster_replica_resume_with_data: Option<bool>,
  pub enable_vector_set_preview: Option<bool>,
  pub vector_set_replay_task_count: Option<i32>,
  pub enable_range_index_preview: Option<bool>,
  pub vector_set_quantization_task_count: Option<i32>,
  pub unparsed_arguments: Option<Vec<String>>,
  pub runtime_logger: Option<String>,
}
