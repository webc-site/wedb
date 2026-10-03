//! 按需检查点（On-Demand-Checkpoint）重拍门集成测试（自
//! src/server/replication/replica_sync_session.rs 内联测迁入，断言与覆盖
//! 原样保留；暴露面经 [`doc(hidden)`] 测试专用口
//! `ReplicaSyncSession::send_checkpoint_and_recover`）

use std::{fs::create_dir_all, sync::Arc};

use compio::runtime::Runtime;
use waof::{AofAddress, WalConfig, WalLog};
use wdev::SegmentedDevice;
use wedb::server::{
  cluster_provider::ClusterProvider,
  replication::{
    checkpoint_entry::{CheckpointEntry, CheckpointMetadata},
    error::{ConnectStage, ReplicationError},
    replica_sync_session::ReplicaSyncSession,
    sync_metadata::SyncMetadata,
  },
  worker::NodeRole,
};
use wnode::database::{GarnetDatabase, SingleDatabaseManager};

/// 副本协商元数据（采集循环判定载荷；错误路径不触达传输段）
fn replica_meta() -> SyncMetadata {
  SyncMetadata {
    full_sync: true,
    origin_node_role: NodeRole::Replica,
    origin_node_id: 0x0000_0000_0000_0000_0000_0000_0002_E70E,

    current_primary_repl_id: String::new(),
    current_store_version: -1,
    current_aof_begin_address: AofAddress::create(1, 0),
    current_aof_tail_address: AofAddress::create(1, 0),
    checkpoint_entry: None,
  }
}

fn open_test_wal(dir: &tempfile::TempDir) -> Arc<WalLog<SegmentedDevice>> {
  let device =
    Arc::new(SegmentedDevice::single_file(dir.path().join("test.wal")).expect("create wal device"));
  Arc::new(WalLog::new(device, WalConfig::default()).expect("create wal"))
}

#[test]
fn odc_empty_store_advance_truncated_rejects() -> aok::Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempfile::tempdir()?;
    let wal = open_test_wal(&dir);
    let provider = ClusterProvider::new();
    let rm = provider.replication_manager().unwrap();
    let session = ReplicaSyncSession::new(rm.clone());

    // 空库 + 截断线前移：幻影覆盖起点 0 落后截断线触发重拍；
    // database_manager 缺席 → 重拍确定性失败，混尽且未允许丢数据拒绝 attach
    rm.aof_sync_driver_store
      .update_truncated_until(&AofAddress::create(1, 100));
    let res = session
      .send_checkpoint_and_recover(&provider, &wal, "127.0.0.1:1", &replica_meta(), 0)
      .await;
    assert!(
      res
        .as_ref()
        .is_err_and(|e| matches!(e, ReplicationError::CheckpointAcquire { .. })),
      "混尽且未允许丢数据应拒绝 attach: {res:?}"
    );
    Ok(())
  })
}

#[test]
fn odc_disabled_skips_reshoot_entirely() -> aok::Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempfile::tempdir()?;
    let wal = open_test_wal(&dir);
    let provider = ClusterProvider::new();
    // 关掉按需检查点（--on-demand-checkpoint false 的装配形态）：判据命中也
    // 不重拍，直接落回 skip 直推（对标 C# ReplicaSyncSession.cs:280 的短路）
    provider.set_on_demand_checkpoint(false);
    let rm = provider.replication_manager().unwrap();
    let session = ReplicaSyncSession::new(rm.clone());

    rm.aof_sync_driver_store
      .update_truncated_until(&AofAddress::create(1, 100));
    let res = session
      .send_checkpoint_and_recover(&provider, &wal, "127.0.0.1:1", &replica_meta(), 0)
      .await;
    assert!(
      res.as_ref().is_ok_and(|o| o.is_none()),
      "关按需检查点即跳过重拍落回 skip 直推: {res:?}"
    );
    Ok(())
  })
}

#[test]
fn odc_invalid_metadata_entry_triggers_reshoot() -> aok::Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempfile::tempdir()?;
    let wal = open_test_wal(&dir);
    let provider = ClusterProvider::new();
    let rm = provider.replication_manager().unwrap();
    let session = ReplicaSyncSession::new(rm.clone());

    // 在册条目元数据无效（store_version==-1）：!validMetadata 恒触发重拍，
    // database_manager 缺席混尽后拒绝 attach
    rm.checkpoint_store
      .write()
      .add_checkpoint_entry(CheckpointEntry::new(CheckpointMetadata::new(1)), true);
    let res = session
      .send_checkpoint_and_recover(&provider, &wal, "127.0.0.1:1", &replica_meta(), 0)
      .await;
    assert!(
      res
        .as_ref()
        .is_err_and(|e| matches!(e, ReplicationError::CheckpointAcquire { .. })),
      "无效元数据应触发重拍并拒绝 attach: {res:?}"
    );
    Ok(())
  })
}

#[test]
fn odc_exhausted_proceeds_when_data_loss_allowed() -> aok::Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempfile::tempdir()?;
    let wal = open_test_wal(&dir);
    let provider = ClusterProvider::new();
    // 允许丢数据形态 = C# 派生式命中（FastAofTruncate 且关按需检查点），
    // 无直配写口，经两输入装配（对标 GarnetServerOptions.cs:653-654）
    provider.set_fast_aof_truncate(true);
    provider.set_on_demand_checkpoint(false);
    let rm = provider.replication_manager().unwrap();
    let session = ReplicaSyncSession::new(rm.clone());

    // 无效条目 + 截断线前移双触发面，混尽后允许丢数据 → 放行直推（skip 下发）
    rm.checkpoint_store
      .write()
      .add_checkpoint_entry(CheckpointEntry::new(CheckpointMetadata::new(1)), true);
    rm.aof_sync_driver_store
      .update_truncated_until(&AofAddress::create(1, 100));
    let res = session
      .send_checkpoint_and_recover(&provider, &wal, "127.0.0.1:1", &replica_meta(), 0)
      .await;
    assert!(
      res.as_ref().is_ok_and(|o| o.is_none()),
      "允许丢数据应放行并落回 skip 直推: {res:?}"
    );
    Ok(())
  })
}

#[test]
fn fresh_primary_empty_store_skips_checkpoint_send() -> aok::Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempfile::tempdir()?;
    let wal = open_test_wal(&dir);
    let provider = ClusterProvider::new();
    let rm = provider.replication_manager().unwrap();
    let session = ReplicaSyncSession::new(rm);

    // 全新主端（空库 + 截断线 0）：幻影覆盖起点不落后，不触发重拍直推
    let res = session
      .send_checkpoint_and_recover(&provider, &wal, "127.0.0.1:1", &replica_meta(), 0)
      .await;
    assert!(
      res.as_ref().is_ok_and(|o| o.is_none()),
      "全新主端不应触发重拍: {res:?}"
    );
    Ok(())
  })
}

#[test]
fn odc_reshoot_success_flows_into_send_segment() -> aok::Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (store_dir, store) = wtest_base::open_test_store("odc_reshoot")?;
    let wal = open_test_wal(&store_dir);
    let cp_dir = store_dir.path().join("checkpoints");
    create_dir_all(&cp_dir).unwrap();
    let db = Arc::new(GarnetDatabase::new(
      0,
      Arc::clone(&store),
      Arc::clone(&store.device),
      cp_dir.clone(),
      None,
    ));
    let provider = ClusterProvider::new();
    provider.set_checkpoint_dir(cp_dir.clone());
    provider.set_database_manager(Arc::new(SingleDatabaseManager::new(cp_dir, db)));
    let rm = provider.replication_manager().unwrap();
    let session = ReplicaSyncSession::new(rm.clone());

    // 无效条目触发重拍：真身拍摄成功登记有效条目，采集循环放行进入发送段
    rm.checkpoint_store
      .write()
      .add_checkpoint_entry(CheckpointEntry::new(CheckpointMetadata::new(1)), true);
    // 副本端点不可达：错误来自发送段建连而非采集循环（重拍已放行）
    let res = session
      .send_checkpoint_and_recover(&provider, &wal, "127.0.0.1:1", &replica_meta(), 0)
      .await;
    assert!(
      res.as_ref().is_err_and(|e| {
        matches!(
          e,
          ReplicationError::Connect {
            stage: ConnectStage::CheckpointSend,
            ..
          } | ReplicationError::ConnectNotReady(ConnectStage::CheckpointSend)
        )
      }),
      "重拍成功后应推进到发送段建连: {res:?}"
    );
    let latest = rm
      .checkpoint_store
      .read()
      .latest_entry()
      .expect("重拍应登记检查点条目");
    assert_ne!(latest.metadata.store_hlog_token, 0);
    assert_ne!(latest.metadata.store_version, -1);
    Ok(())
  })
}
