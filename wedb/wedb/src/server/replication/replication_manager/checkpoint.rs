//! 检查点元数据登记、扫盘 seed 与版本切换标记（对标 C# ReplicationCheckpointManagement.cs）

use super::*;

impl ReplicationManager {
  /// libs/cluster/Server/Replication/ReplicationCheckpointManagement.cs:AddCheckpointEntry
  ///
  /// 登记新检查点条目到内存检查点仓库（C# ReplicationCheckpointManagement.cs 委托）
  pub fn add_checkpoint_entry(&self, entry: CheckpointEntry, full_checkpoint: bool) {
    self
      .checkpoint_store
      .write()
      .add_checkpoint_entry(entry, full_checkpoint);
  }

  /// libs/cluster/Server/Replication/ReplicationCheckpointManagement.cs:UpdateCommitSafeAofAddress
  ///
  /// 更新当前待提交检查点的安全 AOF 尾地址（对标 C# UpdateCommitSafeAofAddress）
  pub fn update_commit_safe_aof_address(&self, safe_aof_tail_address: &AofAddress) {
    *self.store_current_safe_aof_address.write() = *safe_aof_tail_address;
  }

  /// libs/server/GarnetCheckpointManager.cs:SetRecoveredSafeAofAddress
  fn set_recovered_safe_aof_address(&self, address: &AofAddress) {
    *self.store_recovered_safe_aof_address.write() = *address;
  }

  /// libs/cluster/Server/Replication/ReplicationCheckpointManagement.cs:GetLatestCheckpointFromMemoryInfo
  pub fn get_latest_checkpoint_from_memory_info(&self) -> String {
    self
      .checkpoint_store
      .read()
      .get_latest_checkpoint_from_memory_info()
  }

  /// 注入快照根目录（集群装配期一次注入，对标 C# rm 构造期持 CheckpointDir；
  /// 同步注入 checkpoint_store，淘汰与孤儿清理由此获得物理删除能力）
  pub fn set_checkpoint_dir(&self, dir: PathBuf) {
    *self.checkpoint_dir.write() = Some(dir.clone());
    self.checkpoint_store.write().set_checkpoint_dir(dir);
  }

  /// libs/cluster/Server/Replication/ReplicationCheckpointManagement.cs:GetLatestCheckpointFromDiskInfo
  ///
  /// 获取磁盘最新检查点格式化信息（专供 Redis `INFO CINFO` 监控段中的
  /// `disk_checkpoint_entry` 指标）
  ///
  /// 【对标 C# 原型】：
  /// C# 经 `checkpointStore.GetLatestCheckpointFromDiskInfo` 触发底层 Tsavorite
  /// 存储引擎扫盘，读取快照文件并反序列化其中的 Cookie，输出
  /// `cEntry.ToString()`（CheckpointMetadata 键值串）；若无快照或异常则
  /// 捕获后返回 `"(empty)"`。
  ///
  /// 【wedb 架构演进与差异】：
  /// 1. 快照磁盘模型由 `wcpr` 承接：快照按单调自增 token（u128）命名，
  ///    元数据文件 `checkpoint_<token>.meta` 经 bitcode 封签落盘。
  /// 2. Cookie 不落盘：wedb 架构公理明确定义 `cookie 属复制域不落地`
  ///    （见 `wcpr/src/meta.rs`），storeVersion / storePrimaryReplId 仅存于
  ///    内存检查点仓库，磁盘快照中不存在——故输出以 token 充当
  ///    storeHlogToken / storeIndexToken，`checkpoint_aof_address` 充当
  ///    storeCheckpointCoveredAofAddress。
  /// 3. 检查点目录经 [`Self::set_checkpoint_dir`] 装配期注入（C# rm 构造期
  ///    即持 CheckpointDir；rust rm 构造早于目录装配，注入时序同
  ///    set_commit_channel 先例）。
  ///
  /// 目录未注入、目录无快照、读取或解码失败一律回退 `"(empty)"`（对标
  /// C# catch 分支）；token 十六进制形态与内存条目 Display 同族。
  pub fn get_latest_checkpoint_from_disk_info(&self) -> String {
    self
      .checkpoint_dir
      .read()
      .as_deref()
      .map(latest_checkpoint_meta_info)
      .unwrap_or_else(|| EMPTY_CHECKPOINT_INFO.to_string())
  }

  /// libs/cluster/Server/Replication/ReplicationManager.cs:GetRecoveredSafeAofAddress
  ///
  /// 获取安全恢复的 AOF 地址
  pub fn get_recovered_safe_aof_address(&self) -> AofAddress {
    *self.store_recovered_safe_aof_address.read()
  }

  /// libs/cluster/Server/Replication/ReplicationManager.cs:GetCurrentSafeAofAddress
  ///
  /// 获取当前安全 AOF 地址
  pub fn get_current_safe_aof_address(&self) -> AofAddress {
    *self.store_current_safe_aof_address.read()
  }

  /// libs/cluster/Server/Replication/ReplicationCheckpointManagement.cs:GetLatestCheckpointEntryFromDisk
  ///
  /// 扫盘构造最新检查点条目（C# 该处为转发面，实现体
  /// CheckpointStore.cs:272 的 GetLatestCheckpointTokens 盘上枚举由
  /// [`wcpr::latest_checkpoint_meta`] 磁盘模型承接，条目组装在此落地）。
  /// 无盘上快照返回 None——C# 返回 storeVersion=-1 的非 null 空条目，空条目
  /// 折算由调用方按需承接（Initialize 置空内存仓、attach 上报折算
  /// with_sublogs 空条目）。磁盘快照无复制域 cookie（`cookie 属复制域不落地`），
  /// 条目组装同 cluster_provider add_new_checkpoint_entry 口径：store_version
  /// 由 token 派生、hlog/index token 同 token、covered 地址取检查点元数据、
  /// repl id 取当前主复制 ID（历史已由 replication.toml 恢复，对标 C# cookie
  /// 的 RecoveredReplicationId）
  pub fn get_latest_checkpoint_entry_from_disk(&self) -> Option<CheckpointEntry> {
    let (token, meta) = self
      .checkpoint_dir
      .read()
      .as_deref()
      .and_then(latest_checkpoint_meta)?;
    let mut metadata = CheckpointMetadata::new(self.sublog_count);
    metadata.store_version = checkpoint_version(token);
    metadata.store_hlog_token = token;
    metadata.store_index_token = token;
    // 从元数据向量按物理子日志逐位还原覆盖位点（对标 C#
    // GetCheckpointCookieMetadata 多子日志分支 AofAddress.Deserialize），缺位
    // 子日志保持 0——严禁标量广播全维
    if let Some(addrs) = &meta.checkpoint_aof_address {
      for (i, &addr) in addrs.iter().enumerate().take(self.sublog_count) {
        metadata
          .store_checkpoint_covered_aof_address
          .set(i, addr as i64);
      }
    }
    metadata.store_primary_repl_id = Some(self.primary_repl_id());
    Some(CheckpointEntry::new(metadata))
  }

  /// libs/cluster/Server/Replication/ReplicationCheckpointManagement.cs:InitializeCheckpointStore
  ///
  /// 扫盘 seed 最新磁盘检查点（C# Initialize 的 GetLatestCheckpointEntryFromDisk
  /// 对位）后初始化内存仓库，并触发除最新条目外的孤儿快照清理；扫盘与条目
  /// 组装单点在 [`Self::get_latest_checkpoint_entry_from_disk`]
  pub fn initialize_checkpoint_store(&self) -> bool {
    let disk_entry = self.get_latest_checkpoint_entry_from_disk();
    let mut store = self.checkpoint_store.write();
    store.initialize(disk_entry);
    if let Some(c_entry) = store.try_get_latest_checkpoint_entry_from_memory() {
      let min_covered = c_entry.get_min_aof_covered_address(0);
      self
        .aof_sync_driver_store
        .update_truncated_until(&min_covered);
      self.set_recovered_safe_aof_address(&c_entry.metadata.store_checkpoint_covered_aof_address);
      c_entry.remove_reader();
      true
    } else {
      false
    }
  }

  /// libs/cluster/Server/Replication/ReplicationManager.cs:CheckpointVersionShiftStart
  /// libs/cluster/Server/Replication/ReplicationManager.cs:CheckpointVersionShiftEnd
  ///
  /// 检查点版本切换标记广播单点（start/end 同形核，AofEntryType 定向）：经
  /// storeWrapper 提交通道向 AOF 广播 CheckpointStartCommit / CheckpointEndCommit
  /// 标记（sessionID = -1，storeVersion = newVersion）。
  ///
  /// 调用方契约（对标 C# 首行 `LocalNodeRole == NodeRole.REPLICA return`）：
  /// 仅 PRIMARY 角色调用——REPLICA 本地检查点不写标记；角色判定由
  /// [`crate::server::cluster_provider::ClusterProvider`] 的 `wnode::ClusterProvider`
  /// 实现承担（Rust rm 不持 clusterManager 引用，判定上移一层，语义等价）。
  /// 统一检查点模型下不区分 main/object store、不走流式标记（C# 注释：We enqueue
  /// a single checkpoint start marker, since we have unified checkpointing），
  /// 故 is_main_store / is_streaming / oldVersion 形参随流式半接口一并移除
  pub(crate) fn checkpoint_version_shift(&self, entry_type: AofEntryType, new_version: i64) {
    if let Some(commit) = self.commit_channel.read().as_ref() {
      commit(entry_type, new_version);
    }
    trace!(
      "Checkpoint version shift {}: {new_version}",
      if matches!(entry_type, AofEntryType::CheckpointStartCommit) {
        "started"
      } else {
        "ended"
      }
    );
  }
}
