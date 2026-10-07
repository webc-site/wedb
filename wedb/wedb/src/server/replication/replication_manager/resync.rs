//! 主备重同步协商：增量/全量策略判定与数据丢失校验（对标 C# ComputeAofSyncReplayAddress/NeedToFullSync）

use super::*;

impl ReplicationManager {
  /// libs/cluster/Server/Replication/PrimaryOps/DiskbasedReplication/ReplicaSyncSession.cs:DataLossCheck
  ///
  /// 校验从节点请求的同步位点是否落后于主节点 AOF 截断安全线
  pub fn data_loss_check(
    &self,
    possible_aof_data_loss: bool,
    sync_from_aof_address: &AofAddress,
    begin_aof_address: &AofAddress,
  ) -> Result<(), ReplicationError> {
    if sync_from_aof_address.any_lesser(begin_aof_address) {
      if !possible_aof_data_loss {
        let msg = ReplicationError::DataLoss {
          sync_from: *sync_from_aof_address,
          begin: *begin_aof_address,
        };
        error!("{msg}");
        return Err(msg);
      } else {
        warn!(
          "AOF truncated, unsafe attach allowed: {sync_from_aof_address:?} < beginAofAddress: {begin_aof_address:?}"
        );
      }
    }
    Ok(())
  }

  /// DataLossCheck 兜底单点（比对基准 = 当下活体日志起点 Log.BeginAddress；
  /// 磁盘臂 begin_replica_recover_clamp / transmit_checkpoint 与无盘臂
  /// establish 三处共用）：副本回传/请求位点低于主端活体日志起点即快照流
  /// 期间发生 AOF 截断删段，推流无法连续衔接——默认拒绝建流，
  /// allow_data_loss（派生式命中）放行仅告警
  pub(crate) fn data_loss_check_vs_live_begin(
    &self,
    allow_data_loss: bool,
    sync_from: &AofAddress,
    wal: &WalLog<SegmentedDevice>,
  ) -> Result<(), ReplicationError> {
    let begin = AofAddress::create(self.sublog_count() as i32, wal.begin_address() as i64);
    self.data_loss_check(allow_data_loss, sync_from, &begin)
  }

  /// AOF 接续位点与检查点下发协商（磁盘/无盘两条策略臂共用的判定素材）
  ///
  /// 在 garnet 中的相对路径: libs/server/AOF/GarnetAppendOnlyFile.cs:ComputeAofSyncReplayAddress
  ///
  /// C# 该函数按 AofPhysicalSublogCount 逐子日志算出 replayAOFMap 并推进
  /// checkpointAofBeginAddress，recoverFromRemote（= !skipLocalMainStoreCheckpoint）
  /// 由调用侧 DiskbasedReplication/ReplicaSyncSession.cs ValidateMetadata 给出；
  /// rust 把同一组入参下的两布尔与位点推进一次算齐，供两条策略臂各取所需，
  /// partial/full、needFullSync 的判定取向不落在此处。
  ///
  /// 与 C# 的形态差异：C# 副本起始位点越过检查点覆盖线时仅记日志（该子日志位
  /// 不进 replayAOFMap），副本尾位点低于覆盖线且非 FastAofTruncate 时抛异常
  /// 终止本次同步；rust 无异常通道，两形态统一收敛为 is_partial_possible = false。
  ///
  /// 主库 AOF 截断下界预检（C# 把同一位点断层的暴露面放在挂载期——
  /// GarnetAppendOnlyFile.cs:DataLossCheck 的 syncFromAofAddress < Log.BeginAddress
  /// 抛异常终止同步、AofSyncDriverStore.cs:TryAddReplicationDriver 的
  /// startAddress.AnyLesser(TruncatedUntil) 拒绝注册，两者都发生在
  /// skipLocalMainStoreCheckpoint 已判定跳过快照之后，从库只剩节流重连一条
  /// 路且重连必复现）；rust 在协商期同点位预检收敛为 is_partial_possible =
  /// false → FullResync，由 send_checkpoint_and_recover 的按需检查点重拍
  /// 自愈把授予位点抬回截断线之上。
  fn negotiate_resync(
    &self,
    replica_meta: &SyncMetadata,
    committed_until: &AofAddress,
    primary_aof_begin: &AofAddress,
    fast_aof_truncate: bool,
  ) -> ResyncNegotiation {
    let local_checkpoint = self.checkpoint_store.read().latest_entry();

    let replica_checkpoint = replica_meta.checkpoint_entry.as_ref();

    // 1. 主从检查点历史判定：双方检查点中记录的 PrimaryReplId 需严格一致。
    // 若两侧均无有效检查点快照（纯 AOF 复制链路），视为主从处于同源 AOF 流历史；
    // 仅当存在有效检查点且 PrimaryReplId 不一致时，才判定为检查点历史分歧。
    let same_main_store_checkpoint_history = match (&replica_checkpoint, &local_checkpoint) {
      (Some(rc), Some(lc))
        if rc.metadata.store_hlog_token != 0 && lc.metadata.store_hlog_token != 0 =>
      {
        rc.metadata
          .store_primary_repl_id
          .as_deref()
          .is_some_and(|id| !id.is_empty())
          && rc.metadata.store_primary_repl_id == lc.metadata.store_primary_repl_id
      }
      (None, None) => true,
      (Some(rc), None) if rc.metadata.store_hlog_token == 0 => true,
      (None, Some(lc)) if lc.metadata.store_hlog_token == 0 => true,
      (Some(rc), Some(lc))
        if rc.metadata.store_hlog_token == 0 && lc.metadata.store_hlog_token == 0 =>
      {
        true
      }
      _ => false,
    };

    // 2. 故障转移跨主历史判定：副本记录的主节点 ID 是否与当前节点的次级主 ID（旧主）匹配
    // 注：C# ReplicaSyncSession.cs:162 系 IsNullOrEmpty && Equals 互斥字面、恒假钳位死码，
    // rust 取设计意图形（修复型偏离），见 deviations §116，严禁按 C# 字面回改。
    let same_history2 = !self.primary_repl_id2().is_empty()
      && self.primary_repl_id2() == replica_meta.current_primary_repl_id;

    // 3. 是否跳过主节点本地检查点全量发送（对标 skipLocalMainStoreCheckpoint）
    let skip_local_checkpoint = match (&local_checkpoint, &replica_checkpoint) {
      (None, _) => true,
      (Some(lc), Some(rc)) => {
        lc.metadata.store_hlog_token == 0
          || (same_main_store_checkpoint_history
            && lc.metadata.store_version == rc.metadata.store_version)
      }
      _ => false,
    };

    let mut replay_aof_mask = 0u64;
    let mut is_partial_possible = true;
    let mut sync_start_address = if let Some(ref lc) = local_checkpoint {
      lc.get_min_aof_covered_address(0)
    } else {
      *primary_aof_begin
    };

    // 4. 若不需下发检查点快照，逐子日志判定 AOF 增量流接续位点
    if skip_local_checkpoint {
      let repl_offset2 = self.get_replication_offset2();
      // 主库截断线（SafeTruncateAof 逻辑删除推进 + 物理日志起点）：低于它的
      // 增量区间在主库磁盘上已不存在，协商出的接续位点过不了挂载期截断拒绝
      let truncated_until = self.aof_sync_driver_store.get_truncated_until();

      for sublog_idx in 0..self.sublog_count {
        let rep_begin = replica_meta
          .current_aof_begin_address
          .get(sublog_idx)
          .unwrap_or(0);
        let rep_tail = replica_meta
          .current_aof_tail_address
          .get(sublog_idx)
          .unwrap_or(0);
        let ckpt_begin = sync_start_address.get(sublog_idx).unwrap_or(0);

        // 主库物理可服务下界判定：接续候选位点（rep_tail）低于截断线或日志
        // 起点，说明断线期间主库发生日志截断，[rep_tail, 下界) 区间已物理
        // 丢失——增量接续不可行，降级全量检查点同步（fast_aof_truncate 模式
        // 豁免口径同下方 ckpt_begin 判定：允许 unsafe attach 直推）
        let trunc_floor = truncated_until
          .get(sublog_idx)
          .unwrap_or(0)
          .max(primary_aof_begin.get(sublog_idx).unwrap_or(0));
        if rep_tail < trunc_floor && !fast_aof_truncate {
          is_partial_possible = false;
          break;
        }

        if rep_begin > 0 && rep_begin > ckpt_begin {
          // 副本自身 AOF 已被物理截断过高，缺失检查点覆盖的起始日志
          is_partial_possible = false;
          break;
        }

        if rep_tail < ckpt_begin && !fast_aof_truncate {
          // 副本尾部位点低于检查点起始覆盖点，无法连续回放
          is_partial_possible = false;
          break;
        }

        let mut replay_until = rep_tail;
        let committed = committed_until.get(sublog_idx).unwrap_or(i64::MAX);
        if committed < replay_until {
          replay_until = committed;
        }

        if replay_until > ckpt_begin {
          replay_aof_mask |= 1 << sublog_idx;
          if same_history2 {
            let limit = repl_offset2.get(sublog_idx).unwrap_or(i64::MAX);
            if replay_until > limit {
              replay_until = limit;
            }
          }
          sync_start_address.set(sublog_idx, replay_until);
        }

        if !same_main_store_checkpoint_history {
          let pri_begin = primary_aof_begin.get(sublog_idx).unwrap_or(0);
          sync_start_address.set(sublog_idx, pri_begin);
          replay_aof_mask &= !(1 << sublog_idx);
        }
      }
    }

    ResyncNegotiation {
      skip_local_checkpoint,
      is_partial_possible,
      replay_aof_mask,
      sync_start_address,
    }
  }

  /// 磁盘链路重同步策略判定：下发本地检查点快照，还是从协商位点直推 AOF 增量
  ///
  /// 在 garnet 中的相对路径: libs/cluster/Server/Replication/PrimaryOps/DiskbasedReplication/ReplicaSyncSession.cs:ValidateMetadata
  ///
  /// C# 磁盘臂的判据只有两条：ValidateMetadata 的 skipLocalMainStoreCheckpoint
  /// （本地无检查点条目 / storeHlogToken 为 0 / 同检查点历史且两侧 CheckpointEntry
  /// 的 storeVersion 相等）决定要不要下发快照，ComputeAofSyncReplayAddress 决定
  /// 增量流从哪一位点接续；skip 为真且位点可接续即 PartialResync，否则 FullResync。
  ///
  /// 副本 store 版本维度只经 CheckpointEntry.metadata.storeVersion 进入判定：
  /// 磁盘入口 NetworkClusterInitiateReplicaSync
  /// （libs/cluster/Session/RespClusterReplicationCommands.cs:259）与
  /// TryBeginDiskbasedSyncAsync 全程不构造、不读带 store 版本的 SyncMetadata，
  /// 故此臂不引用 SyncMetadata.current_store_version。
  pub fn disk_resync_strategy(
    &self,
    replica_meta: &SyncMetadata,
    committed_until: &AofAddress,
    primary_aof_begin: &AofAddress,
    fast_aof_truncate: bool,
  ) -> ResyncStrategy {
    let nego = self.negotiate_resync(
      replica_meta,
      committed_until,
      primary_aof_begin,
      fast_aof_truncate,
    );
    if nego.skip_local_checkpoint && nego.is_partial_possible {
      info!("Disk resync strategy resolved: PartialResync (incremental stream continuation)");
      return ResyncStrategy::PartialResync {
        sync_start_address: nego.sync_start_address,
        replay_aof_mask: nego.replay_aof_mask,
      };
    }
    info!("Disk resync strategy resolved: FullResync (checkpoint snapshot required)");
    ResyncStrategy::FullResync {
      sync_start_address: nego.sync_start_address,
      replay_aof_mask: nego.replay_aof_mask,
    }
  }

  /// 无盘链路重同步策略判定：本会话免快照放行，还是纳入全量扇出
  ///
  /// 在 garnet 中的相对路径: libs/cluster/Server/Replication/PrimaryOps/DisklessReplication/ReplicaSyncSession.cs:NeedToFullSync
  ///
  /// C# 四条件任一成立即全量（needToFullSync 为假的会话在 PrepareForSyncAsync
  /// 里直接 SetStatus(SUCCESS) 摘除，不参与快照扇出）：
  /// 1. 主从历史不一致（PrimaryReplId 与副本上报 currentPrimaryReplId 不等）；
  /// 2. 副本 store 版本 != 主端当下版本（:204 是不等判据，方向非存在性判据，
  ///    主端当下版本由 ReplicationSyncManager.cs:267 自 store.CurrentVersion 取）；
  /// 3. 副本 AOF 尾位点越出主端可服务区间 [Log.BeginAddress, Log.TailAddress]；
  /// 4. 待回放量超 ReplicaDisklessSyncFullSyncAofThreshold。
  ///
  /// 第 4 条在 rust 缺席：仓内无该门限的任何配置面（server_options /
  /// runtime_config 皆无对应项，C# 侧已登记
  /// js/check/ignore/server.yml:ReplicaDisklessSyncFullSyncAofThresholdValue），
  /// 按转写纪律不自造第二套门限常量与默认值，待门限配置单独立项时接上。
  pub fn diskless_resync_strategy(
    &self,
    replica_meta: &SyncMetadata,
    current_store_version: i64,
    committed_until: &AofAddress,
    primary_aof_begin: &AofAddress,
    primary_aof_tail: &AofAddress,
    fast_aof_truncate: bool,
  ) -> ResyncStrategy {
    let send_main_store = self.primary_repl_id() != replica_meta.current_primary_repl_id
      || replica_meta.current_store_version != current_store_version;
    let out_of_range_aof = replica_meta
      .current_aof_tail_address
      .is_out_of_range(primary_aof_begin, primary_aof_tail);
    let full_sync = send_main_store || out_of_range_aof;

    let nego = self.negotiate_resync(
      replica_meta,
      committed_until,
      primary_aof_begin,
      fast_aof_truncate,
    );
    if full_sync {
      info!("Diskless resync strategy resolved: FullResync (streaming snapshot fan-out required)");
      return ResyncStrategy::FullResync {
        sync_start_address: nego.sync_start_address,
        replay_aof_mask: nego.replay_aof_mask,
      };
    }
    info!("Diskless resync strategy resolved: PartialResync (aof replay from negotiated address)");
    ResyncStrategy::PartialResync {
      sync_start_address: nego.sync_start_address,
      replay_aof_mask: nego.replay_aof_mask,
    }
  }
}
