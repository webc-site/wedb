use std::sync::Arc;

use gxhash::HashSet;

use crate::{
  client::GarnetClient,
  server::{
    cluster_provider::ClusterProvider,
    migration::{migrate_state::MigrateState, migration_manager::TransferOption, sketch::Sketch},
  },
};

/// libs/cluster/Server/Migration/MigrateSession.cs:MigrateSession
pub struct MigrateSession {
  _cluster_provider: Arc<ClusterProvider>,
  pub target_node_id: String,
  slots: HashSet<i32>,
  pub status: MigrateState,
}

impl MigrateSession {
  #[allow(clippy::too_many_arguments)]
  pub fn new(
    cluster_provider: Arc<ClusterProvider>,
    _source_node_id: &str,
    _target_address: &str,
    _target_port: i32,
    target_node_id: &str,
    _username: &str,
    _passwd: &str,
    _copy_option: bool,
    _replace_option: bool,
    _timeout: i32,
    slots: HashSet<i32>,
    _sketch: Sketch,
    _transfer_option: TransferOption,
  ) -> Self {
    Self {
      _cluster_provider: cluster_provider,
      target_node_id: target_node_id.to_string(),
      slots,
      status: MigrateState::Pending,
    }
  }

  /// libs/cluster/Server/Migration/MigrateSession.cs:Dispose
  pub fn dispose(&self) {}

  pub fn get_slots(&self) -> &HashSet<i32> {
    &self.slots
  }

  pub fn can_access_key(&self, _key: &[u8], _slot: i32, _read_only: bool) -> bool {
    true
  }
}

/// libs/cluster/Server/Migration/MigrateOperation.cs:MigrateOperation
pub struct MigrateOperation;

impl MigrateOperation {
  /// libs/cluster/Server/Migration/MigrateOperation.cs:InitializeAsync
  pub async fn initialize_async(&self) -> bool {
    true
  }

  /// libs/cluster/Server/Migration/MigrateOperation.cs:Dispose
  pub fn dispose(&self) {}

  /// libs/cluster/Server/Migration/MigrateOperation.cs:Scan
  pub fn scan(&self, _current_address: &mut i64, _end_address: i64) {}

  /// libs/cluster/Server/Migration/MigrateOperation.cs:TransmitSlotsAsync
  pub async fn transmit_slots_async(&self) -> bool {
    true
  }

  /// libs/cluster/Server/Migration/MigrateOperation.cs:TransmitKeysAsync
  pub async fn transmit_keys_async(&self) -> bool {
    true
  }

  /// libs/cluster/Server/Migration/MigrateOperation.cs:TransmitKeysNamespacesAsync
  pub async fn transmit_keys_namespaces_async(&self) -> bool {
    true
  }

  /// libs/cluster/Server/Migration/MigrateOperation.cs:DeleteKeys
  pub fn delete_keys(&self) {}

  /// libs/cluster/Server/Migration/MigrateOperation.cs:DeleteVectorSet
  pub fn delete_vector_set(&self, _key: &[u8]) {}

  /// libs/cluster/Server/Migration/MigrateOperation.cs:DeleteRangeIndex
  pub fn delete_range_index(&self, _key: &[u8]) {}
}

impl MigrateSession {
  /// libs/cluster/Server/Migration/MigrateSession.cs:Overlap
  pub fn overlap(&self, _other: &MigrateSession) -> bool {
    false
  }

  /// libs/cluster/Server/Migration/MigrateSession.cs:GetGarnetClient
  pub fn get_garnet_client(&self) -> Arc<GarnetClient> {
    Arc::new(GarnetClient::new())
  }

  /// libs/cluster/Server/Migration/MigrateSession.cs:GetLocalSession
  pub fn get_local_session(&self) {}

  /// libs/cluster/Server/Migration/MigrateSession.cs:CheckConnectionAsync
  pub async fn check_connection_async(&self, _client: Arc<GarnetClient>) -> bool {
    true
  }

  /// libs/cluster/Server/Migration/MigrateSession.cs:GetRanges
  pub fn get_ranges(&self) -> Vec<(i32, i32)> {
    vec![]
  }

  /// libs/cluster/Server/Migration/MigrateSession.cs:ResetLocalSlot
  pub fn reset_local_slot(&self, _slot: i32) {}

  /// libs/cluster/Server/Migration/MigrateSession.cs:TryPrepareLocalForMigration
  pub fn try_prepare_local_for_migration(&self) -> bool {
    true
  }

  /// libs/cluster/Server/Migration/MigrateSession.cs:RelinquishOwnership
  pub fn relinquish_ownership(&self) {}

  // --- MigrateSessionSlots.cs ---

  /// libs/cluster/Server/Migration/MigrateSessionSlots.cs:ReserveDestinationVectorSetsAsync
  pub async fn reserve_destination_vector_sets_async(&self) -> bool {
    true
  }

  /// libs/cluster/Server/Migration/MigrateSessionSlots.cs:MigrateSlotsDriverInlineAsync
  pub async fn migrate_slots_driver_inline_async(&self) -> bool {
    true
  }

  /// libs/cluster/Server/Migration/MigrateSessionSlots.cs:CreateAndRunMigrateTasksAsync
  pub async fn create_and_run_migrate_tasks_async(&self) -> bool {
    true
  }

  /// libs/cluster/Server/Migration/MigrateSessionSlots.cs:ScanStoreTaskAsync
  pub async fn scan_store_task_async(&self) -> bool {
    true
  }

  // --- MigrateSessionKeys.cs ---

  /// libs/cluster/Server/Migration/MigrateSessionKeys.cs:MigrateKeysFromStoreAsync
  pub async fn migrate_keys_from_store_async(&self) -> bool {
    true
  }

  /// libs/cluster/Server/Migration/MigrateSessionKeys.cs:ShouldSkipKey
  pub fn should_skip_key(&self, _key: &[u8]) -> bool {
    false
  }

  /// libs/cluster/Server/Migration/MigrateSessionKeys.cs:DeleteKeysAsync
  pub async fn delete_keys_async(&self) -> bool {
    true
  }

  /// libs/cluster/Server/Migration/MigrateSessionKeys.cs:MigrateKeysAsync
  pub async fn migrate_keys_async(&self) -> bool {
    true
  }

  // --- MigrateSessionKeyAccess.cs ---

  /// libs/cluster/Server/Migration/MigrateSessionKeyAccess.cs:WaitForConfigPropagationAsync
  pub async fn wait_for_config_propagation_async(&self) {}

  // --- MigrateSessionCommonUtils.cs ---

  /// libs/cluster/Server/Migration/MigrateSessionCommonUtils.cs:WriteOrSendRecordAsync
  pub async fn write_or_send_record_async(&self) -> bool {
    true
  }

  /// libs/cluster/Server/Migration/MigrateSessionCommonUtils.cs:WriteOrSendAccumulatedRecordAsync
  pub async fn write_or_send_accumulated_record_async(&self) -> bool {
    true
  }

  /// libs/cluster/Server/Migration/MigrateSessionCommonUtils.cs:WriteOrSendChunkedRecordAsync
  pub async fn write_or_send_chunked_record_async(&self) -> bool {
    true
  }

  /// libs/cluster/Server/Migration/MigrateSessionCommonUtils.cs:WriteOrSendSegmentedRecordAsync
  pub async fn write_or_send_segmented_record_async(&self) -> bool {
    true
  }

  /// libs/cluster/Server/Migration/MigrateSessionCommonUtils.cs:WriteOrSendRecordSpanAsync
  pub async fn write_or_send_record_span_async(&self) -> bool {
    true
  }

  /// libs/cluster/Server/Migration/MigrateSessionCommonUtils.cs:RetryAsync
  pub async fn retry_async(&self) -> bool {
    true
  }

  /// libs/cluster/Server/Migration/MigrateSessionCommonUtils.cs:HandleMigrateTaskResponseAsync
  pub async fn handle_migrate_task_response_async(&self) -> bool {
    true
  }

  // --- MigrateSession.RangeIndex.cs ---

  /// libs/cluster/Server/Migration/MigrateSession.RangeIndex.cs:TransmitRangeIndexAsync
  pub async fn transmit_range_index_async(&self) -> bool {
    true
  }

  /// libs/cluster/Server/Migration/MigrateSession.RangeIndex.cs:MigrateRangeIndexKeysAsync
  pub async fn migrate_range_index_keys_async(&self) -> bool {
    true
  }
}

/// libs/cluster/Server/Migration/MigrateScanFunctions.cs:StoreScan
pub struct StoreScan;

impl StoreScan {
  pub fn new() -> Self {
    Self
  }

  /// libs/cluster/Server/Migration/MigrateScanFunctions.cs:StoreScan
  pub fn store_scan(&self) {}

  /// libs/cluster/Server/Migration/MigrateScanFunctions.cs:OnStart
  pub fn on_start(&self) {}

  /// libs/cluster/Server/Migration/MigrateScanFunctions.cs:OnStop
  pub fn on_stop(&self) {}

  /// libs/cluster/Server/Migration/MigrateScanFunctions.cs:OnException
  pub fn on_exception(&self) {}
}

impl Default for StoreScan {
  fn default() -> Self {
    Self::new()
  }
}
