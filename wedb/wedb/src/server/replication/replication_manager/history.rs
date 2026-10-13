//! 复制历史（replication.toml）读写与故障转移位点轮转（对标 C# ReplicationHistoryManager.cs）

use super::*;

impl ReplicationManager {
  /// 写锁内改写复制历史、drop 释放守卫后再落盘（flush_config 重取读锁，不先释放则
  /// 同线程写锁重入自死锁）。「改写→释放→落盘」次序在此单源收口，新增改写点免于漏放
  fn mutate_config_and_flush(&self, mutate: impl FnOnce(&mut ReplicationHistory)) {
    let mut config = self.current_replication_config.write();
    mutate(&mut config);
    drop(config);
    self.flush_config();
  }

  /// libs/cluster/Server/Replication/ReplicationHistoryManager.cs:InitializeReplicationHistory
  pub fn initialize_replication_history(&self, aof_physical_sublog_count: usize) {
    self.mutate_config_and_flush(|c| *c = ReplicationHistory::new(aof_physical_sublog_count));
  }

  /// libs/cluster/Server/Replication/ReplicationHistoryManager.cs:RecoverReplicationHistory
  ///
  /// 从 replication.toml 恢复复制历史（读损坏回退初始化新历史 + 落盘）；
  /// 仅构造期门控（[`Self::with_options`]）与测试调用，size 门控由构造方判定
  pub(super) fn recover_replication_history(&self) {
    if let Some(ref path) = self.config_path {
      let mut config = self.current_replication_config.write();
      *config = ReplicationHistory::recover_or_init(path, self.sublog_count);
    }
  }

  /// libs/cluster/Server/Replication/ReplicationHistoryManager.cs:FlushConfig
  ///
  /// 落盘口单一互斥（C# `lock (this)`）：先占 [`Self::history_flush_lock`] 再读当前
  /// 历史，序列化与写设备整段在锁内——后到者必待前者 rename 完成后才取值落盘，
  /// 故设备上恒为某单一完整版本、落盘序与内存版本序一致（读锁仅取值拷贝，
  /// 不跨 IO 持有，互斥由本锁承担）
  pub fn flush_config(&self) {
    let Some(ref path) = self.config_path else {
      return;
    };
    let _flush_guard = self.history_flush_lock.lock();
    let config = self.current_replication_config.read().clone();
    if let Err(e) = config.flush_to_file(path) {
      error!("Failed to flush replication history: {e}");
    }
  }

  /// libs/cluster/Server/Replication/ReplicationHistoryManager.cs:TryUpdateMyPrimaryReplId
  pub fn try_update_my_primary_repl_id(&self, primary_replication_id: &str) {
    self.mutate_config_and_flush(|c| c.update_replication_id(primary_replication_id));
  }

  /// libs/cluster/Server/Replication/ReplicationManager.cs:ReplicationOffset2
  ///
  /// 获取旧主历史复制偏移（failover 后有效）
  pub fn get_replication_offset2(&self) -> AofAddress {
    self.current_replication_config.read().replication_offset2
  }

  /// libs/cluster/Server/Replication/ReplicationManager.cs:PrimaryReplId
  ///
  /// 获取主复制 ID
  pub fn primary_repl_id(&self) -> String {
    self
      .current_replication_config
      .read()
      .primary_repl_id
      .clone()
  }

  /// libs/cluster/Server/Replication/ReplicationManager.cs:PrimaryReplId2
  ///
  /// 获取次级主复制 ID（故障转移旧主 ID）
  pub fn primary_repl_id2(&self) -> String {
    self
      .current_replication_config
      .read()
      .primary_repl_id2
      .clone()
  }

  /// libs/cluster/Server/Replication/ReplicationManager.cs:UpdateLastPrimarySyncTime
  ///
  /// 更新主从同步时间戳
  pub fn update_last_primary_sync_time(&self) {
    let now_ms = time::now_ms_i64();
    self
      .primary_sync_last_timestamp
      .store(now_ms, Ordering::Release);
  }

  /// 距离上次主从同步过去的秒数
  ///
  /// 对标 C# ReplicationManager.cs:38 LastPrimarySyncSeconds
  /// 非 is_recovering() 恒回 0；仅当恢复进行中才返回流逝秒数（恢复期未刷新时间戳时返回 0）
  pub fn last_primary_sync_seconds(&self) -> i64 {
    if !self.is_recovering() {
      return 0;
    }
    let last = self.primary_sync_last_timestamp.load(Ordering::Acquire);
    if last == 0 {
      0
    } else {
      let now_ms = time::now_ms_i64();
      (now_ms.saturating_sub(last)) / 1000
    }
  }

  /// libs/cluster/Server/Replication/ReplicationHistoryManager.cs:TryUpdateForFailover
  ///
  /// 故障转移触发时更新复制位点与复制流 ID 轮转并持久化配置
  pub fn try_update_for_failover(&self) {
    // 对标 C# TryUpdateForFailover 取 storeWrapper.appendOnlyFile.Log
    // .CommittedUntilAddress（动态提交尾），不读冻结的 replication_offset 字段
    let cur_offset = self.get_committed_replication_offset();
    self.mutate_config_and_flush(|c| c.failover_update(cur_offset));
    // C# 尾段 SetPrimaryReplicationId（更新历史 ID 供未来快照签名）：rust 无
    // 对应缓存写面——快照签名 ID 在登记点现取（store_primary_repl_id 读
    // primary_repl_id，见 cluster_provider/traits.rs），此处仅留注释锚
  }
}
