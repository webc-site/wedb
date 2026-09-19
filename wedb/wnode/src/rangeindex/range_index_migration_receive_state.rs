//! 范围索引集群迁移接收会话状态机
//! （1:1 对标 libs/cluster/Session/RangeIndexMigrationReceiveSession.cs:RangeIndexMigrationReceiveState）
//!
//! 单连接/会话状态机：IDLE → RECEIVING → IDLE。
//! 发送端以单连接串行发送同一迁移流块，按序驱动分块反序列化器流式落盘，
//! 流完成后原子校验校验和、槽位状态并发布迁移索引。

use std::sync::Arc;

use wbftree::{RangeIndexChunkedDeserializer, RangeIndexManager};
use wdev::Device;
use wkv::StoreSession;
use wval::GarnetObjectType;

use super::{
  range_index_manager_migration::{PublishMigratedIndexResult, RangeIndexManagerMigration},
  range_index_migration_activities::ReceiveActivity,
};

/// 范围索引迁移接收状态机
/// （对标 libs/cluster/Session/RangeIndexMigrationReceiveSession.cs:RangeIndexMigrationReceiveState）
pub struct RangeIndexMigrationReceiveState {
  range_index_manager: Arc<RangeIndexManager>,
  current_deserializer: Option<RangeIndexChunkedDeserializer>,
  receive_activity: Option<ReceiveActivity>,
  disposed: bool,
}

impl RangeIndexMigrationReceiveState {
  /// 创建新的范围索引接收状态机
  pub fn new(range_index_manager: Arc<RangeIndexManager>) -> Self {
    Self {
      range_index_manager,
      current_deserializer: None,
      receive_activity: None,
      disposed: false,
    }
  }

  /// 是否正在接收流块
  #[inline]
  pub fn is_receiving(&self) -> bool {
    self.current_deserializer.is_some()
  }

  /// libs/cluster/Session/RangeIndexMigrationReceiveSession.cs:ProcessRecord
  ///
  /// 处理单个 `SerializedRangeIndexStream` 迁移分块载荷。库级定槽
  /// （doc/zh/db.md 4.1）：导入槽位门禁已在 CLUSTER MIGRATE 头级统一判定，
  /// 接收态不再逐记录取键判槽（C# 逐键 HashSlot + IsImportingSlot 探测随
  /// 键级哈希废除）
  pub async fn process_record<D: Device>(
    &mut self,
    record_payload: &[u8],
    session: &StoreSession<D>,
    replace_option: bool,
  ) -> bool {
    if self.disposed {
      return false;
    }
    self
      .process_record_internal(record_payload, session, replace_option)
      .await
  }

  /// libs/cluster/Session/RangeIndexMigrationReceiveSession.cs:ProcessRecordInternal
  pub async fn process_record_internal<D: Device>(
    &mut self,
    record_payload: &[u8],
    session: &StoreSession<D>,
    replace_option: bool,
  ) -> bool {
    if record_payload.is_empty() {
      return self.handle_error("Empty payload");
    }

    if self.current_deserializer.is_none() {
      let temp_migration_path = self.range_index_manager.derive_temp_migration_path();
      match RangeIndexChunkedDeserializer::new(temp_migration_path.clone()) {
        Ok(deserializer) => {
          self.current_deserializer = Some(deserializer);
          self.receive_activity = Some(ReceiveActivity::start_activity(&temp_migration_path));
        }
        Err(e) => {
          log::error!("Failed to create RangeIndexChunkedDeserializer: {e}");
          return self.handle_error("Failed to create deserializer");
        }
      }
    }

    if let Some(activity) = &mut self.receive_activity {
      activity.on_chunk_received(record_payload.len());
    }

    let is_complete = {
      let Some(deserializer) = &mut self.current_deserializer else {
        return self.handle_error("Missing deserializer");
      };
      match deserializer.process_chunk(record_payload) {
        Ok(true) => deserializer.is_complete(),
        _ => return self.handle_error("ProcessChunk failed"),
      }
    };

    if is_complete {
      let deserializer = self.current_deserializer.as_ref().unwrap();
      let key = deserializer.key();

      if self.disposed {
        return self.handle_error("Disposed before publish");
      }

      if let Some(activity) = &mut self.receive_activity {
        activity.on_publishing();
      }

      let stub = deserializer.stub();
      let temp_path = deserializer.temp_path();
      let publish_result = RangeIndexManagerMigration::publish_migrated_index(
        session,
        key,
        stub,
        temp_path,
        replace_option,
        GarnetObjectType::RangeIndex,
      )
      .await;

      if let Some(activity) = &mut self.receive_activity {
        activity.on_publish_result(publish_result);
      }

      if publish_result == PublishMigratedIndexResult::Failed {
        return self.handle_error("PublishMigratedIndex failed");
      }

      self.reset();
    }

    true
  }

  /// libs/cluster/Session/RangeIndexMigrationReceiveSession.cs:HandleError
  pub fn handle_error(&mut self, error: &str) -> bool {
    if let Some(activity) = &mut self.receive_activity {
      activity.on_error(error);
    }
    self.reset();
    false
  }

  /// libs/cluster/Session/RangeIndexMigrationReceiveSession.cs:Reset
  pub fn reset(&mut self) {
    if let Some(mut activity) = self.receive_activity.take() {
      let key = self
        .current_deserializer
        .as_ref()
        .map(|d| d.key())
        .unwrap_or_default();
      activity.end_and_log_activity(key);
    }
    if let Some(mut deserializer) = self.current_deserializer.take() {
      deserializer.dispose();
    }
  }

  /// libs/cluster/Session/RangeIndexMigrationReceiveSession.cs:Dispose
  pub fn dispose(&mut self) {
    self.disposed = true;
    self.dispose_internal();
  }

  /// libs/cluster/Session/RangeIndexMigrationReceiveSession.cs:DisposeInternal
  pub fn dispose_internal(&mut self) {
    if let Some(activity) = &mut self.receive_activity {
      activity.on_session_disposed();
    }
    self.reset();
  }
}

impl Drop for RangeIndexMigrationReceiveState {
  fn drop(&mut self) {
    self.dispose();
  }
}
