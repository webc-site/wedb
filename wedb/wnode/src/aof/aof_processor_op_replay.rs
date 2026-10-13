//! AOF 数据操作重放分派（对标 libs/server/AOF/AofProcessor.cs 的
//! PrepareKey / ReplayOpDispatch / ReplayOp 与模糊区、事务组重放族）
//!
//! 拓扑预处理（prepare_key）、模糊区缓冲清算、事务组同步回放与数据操作
//! 应用分派；主分派骨架见
//! [`super::aof_processor::AofProcessor::process_aof_record_internal`]。

use std::{borrow::Cow, future::Ready};

use waof::{AofEntryType, AofHeader};
use wdev::Device;
use wval::KeyTag;

use super::{
  aof_processor::{AofProcessor, AofReplayError, KeyContextGuard, ReplayTarget},
  aof_processor_object_replay::{object_store_delete, object_store_rmw, object_store_upsert},
  aof_processor_store_ops::{replay_dbmeta, store_delete, store_rmw, store_upsert},
  garnet_log::GarnetLog,
  record_gate,
  replay_input::ReplayInput,
  replaycoordinator::aof_replay_context::{ReplayOperation, TransactionGroup},
};

/// 预处理产出：key 与负载起点（C# PreparedParameters；key 哈希仅预处理期
/// 推进一致性时间戳消费，不随产出下发）。零外部消费面，模块私有。
struct PreparedParameters<'a> {
  /// key 字节。
  key: Cow<'a, [u8]>,
  /// 负载（key 之后的首个组件起点）。
  payload: Cow<'a, [u8]>,
}

/// 模糊区条目头解析失败错误文案（本域两处复用）。
const ERR_FUZZY_HEADER_CORRUPTED: &str = "模糊区条目头损坏";
/// 未知 AOF 操作类型错误文案（本域三处复用）。
const ERR_UNKNOWN_AOF_OP_TYPE: &str = "未知 AOF 操作类型";
/// AOF 条目负载（key 预处理）损坏错误文案（本域三处复用）。
const ERR_AOF_PAYLOAD_CORRUPTED: &str = "AOF 条目负载损坏";

impl AofProcessor {
  /// libs/server/AOF/AofProcessor.cs:PrepareKey
  ///
  /// 拓扑预处理（C# IPreprocessKey.PrepareKey 三实现 :28/:44/:66 的折叠）：
  /// 解出 key / 哈希 / 负载并按拓扑推进一致性 key 时间戳（零堆分配与零 Arc 克隆）。
  /// 仅本模块回放臂（replay_entry / replay_chunk / replay_op）消费，不外导。
  fn prepare_key<'a>(
    &self,
    virtual_sublog_idx: usize,
    entry: &'a [u8],
    log_address_sequence_number: i64,
  ) -> Option<PreparedParameters<'a>> {
    // 帧游标单点：条目体起点与序列号一律经 waof 头面唯一口径取数
    //（`AofHeader::skip_header` 按头型定长、`sequence_number_of` 按头型取内嵌
    // 序号），与 record_gate 的条目键速览面同一函数；本函数不自带第二套
    //「头尺寸 + 序列号」判定——未知/截断头型即判损坏上抛，绝不按 16B 误读体
    let header_size = AofHeader::skip_header(entry)?;
    let sequence_number = AofHeader::sequence_number_of(entry, log_address_sequence_number)?;
    let payload = entry.get(header_size..)?;
    // 键段切分走 record_gate 单点（与 peek_entry_key / 值段共用），越界即 None
    let (key, rest) = record_gate::split_len_prefixed(payload)?;
    let key_hash = GarnetLog::hash(key);

    // 多回放拓扑（分片 / 单物理多回放）：按序列号推进一致性时间戳（零 Arc 克隆）
    if self.append_only_file().multi_log_enabled() {
      self
        .append_only_file()
        .with_read_consistency_manager(|manager| {
          manager.update_virtual_sublog_key_sequence_number(
            virtual_sublog_idx,
            key_hash,
            sequence_number,
          );
        });
    }
    Some(PreparedParameters {
      key: Cow::Borrowed(key),
      payload: Cow::Borrowed(rest),
    })
  }

  /// libs/server/AOF/AofProcessor.cs:ProcessFuzzyRegionOperations
  /// libs/server/AOF/ReplayCoordinator/AofReplayCoordinator.cs:ProcessFuzzyRegionOperations
  ///
  /// 模糊区操作统一重放：rust 将 C# 处理器侧重放循环（AofProcessor.cs）与
  /// 协调器侧取缓冲 + 逐条分派（AofReplayCoordinator.cs，GetReplayContext +
  /// foreach fuzzyRegionOps → ReplayOpDispatch）折叠为同一体——缓冲所有面
  /// 即 [`AofReplayCoordinator::take_fuzzy_region_operations`]，分派复用
  /// [`Self::replay_op_dispatch`] / [`replay_chunk`](super::aof_processor_chunk_replay::replay_chunk)。
  ///
  /// 条目类型分支（对标 C# AddToFuzzyRegionBuffer 压入的 TxnCommit 提交标记
  /// 与 AddOrReplayTransactionOperation 模糊区支的成对入队）：缓冲条目中
  /// TxnCommit 头型是事务组提交标记（无键载荷），绝不可当常规数据条目派发
  /// （prepare_key 无键可解即判损坏崩溃），必须经
  /// [`Self::process_fuzzy_region_transaction_group`] 从 txn_group_buffer
  /// FIFO 出队整组顺序重放；as_replica 经 CheckpointEndCommit 调用处透传
  ///（对标 C# ProcessFuzzyRegionOperations(sublogIdx, storeVersion, asReplica)），
  /// 决定组重放的加锁/免锁形态。
  pub(crate) async fn process_fuzzy_region_operations<D: Device>(
    &self,
    sublog_idx: usize,
    as_replica: bool,
    target: &ReplayTarget<'_, '_, D>,
  ) -> Result<(), AofReplayError> {
    let operations = self.coordinator().take_fuzzy_region_operations(sublog_idx);
    for op in operations {
      match op {
        ReplayOperation::Record(entry) => {
          let header = AofHeader::parse(&entry).ok_or(ERR_FUZZY_HEADER_CORRUPTED)?;
          let op_type =
            AofEntryType::try_from(header.op_type).map_err(|_| ERR_UNKNOWN_AOF_OP_TYPE)?;
          if op_type == AofEntryType::TxnCommit {
            // 提交标记：携带 TxnCommit 头内提交序列号，交模糊区事务组重放
            self
              .process_fuzzy_region_transaction_group(sublog_idx, &entry, as_replica, target)
              .await?;
            continue;
          }
          // 模糊区缓冲条目入队时已通过新代版本判定，快照完成后清算无条件落库
          //（与下分支 Chunk 直调 replay_chunk 对齐；若再走 replay_op_dispatch 将被
          // 刚拍完检查点推高的 store_version 误判为旧代条目丢弃）
          let prepared = self
            .prepare_key(sublog_idx, &entry, 0)
            .ok_or(ERR_AOF_PAYLOAD_CORRUPTED)?;
          self.replay_op(op_type, prepared, target).await?;
        }
        ReplayOperation::Chunk(acc) => {
          super::aof_processor_chunk_replay::replay_chunk(self, &acc, target).await?;
        }
      }
    }
    Ok(())
  }

  /// libs/server/AOF/AofProcessor.cs:ProcessFuzzyRegionTransactionGroup
  /// libs/server/AOF/ReplayCoordinator/AofReplayCoordinator.cs:ProcessFuzzyRegionTransactionGroup
  ///
  /// 模糊区事务组重放：rust 将处理器侧重放体（AofProcessor.cs）与协调器侧
  /// FIFO 出队 + ProcessTransactionGroup 转调（AofReplayCoordinator.cs）折叠
  /// 为同一体——出队即 [`AofReplayCoordinator::dequeue_txn_group`]，重放交
  /// [`Self::process_transaction_group`]。栅栏参数自 commit 条目头解析
  ///（提交序列号 + 参与者数，对标 C# ProcessTransactionGroup 直读 ptr 头），
  /// as_replica 透传消除单机恢复免锁 / 副本加锁两形态的硬编码分叉。
  pub(crate) async fn process_fuzzy_region_transaction_group<D: Device>(
    &self,
    sublog_idx: usize,
    commit_entry: &[u8],
    as_replica: bool,
    target: &ReplayTarget<'_, '_, D>,
  ) -> Result<(), AofReplayError> {
    let Some(group) = self.coordinator().dequeue_txn_group(sublog_idx) else {
      return Ok(());
    };
    let (sequence_number, participant_count) =
      record_gate::get_synchronized_operation_params(self.replay_task_count(), commit_entry, 0)
        .unwrap_or((group.start_sequence_number, group.participant_count as i16));
    self
      .process_transaction_group(
        sublog_idx,
        sequence_number,
        participant_count,
        &group,
        as_replica,
        target,
      )
      .await?;
    Ok(())
  }

  /// libs/server/AOF/ReplayCoordinator/AofReplayCoordinator.cs:ProcessTransactionGroup
  ///
  /// 事务组同步回放（归组键自本票修复后数据条目与标记同 session_id，组非空）：
  /// - 崩溃恢复（非副本）或单日志拓扑直接顺序重放：恢复期读不会暴露局部中
  ///   间态事务，无需加锁；子日志写入顺序已在入队时确定，无需跨流同步栅栏
  ///   （多日志串行恢复下跨分片组若等栅栏，其余参与者所在子日志的回放驱动
  ///   被当前 await 永久阻塞，确定性死锁）；
  /// - 副本多日志拓扑（本地真多任务并发回放，replay_task_count > 1）经
  ///   Acquire/Release 序列号栅栏保证跨子日志组提交全序，但不持 C# 逐键锁集
  ///   （SaveTransactionGroupKeysToLock+Run/Commit）——组内操作顺序重放期间
  ///   的读者中间态暴露窗口为刻意取舍（逐键事务锁面随 C# 对位裁剪，无在册
  ///   登记条目）；单消费者拓扑（副本回放单线程 / 恢复臂）由协调器栅栏入口
  ///   第二道门直行，杜绝缺员永等（见协调器 replay_task_count 字段注）；
  /// - 两形态均顺序重放事务组全部操作并清理会话事务。
  pub(crate) async fn process_transaction_group<D: Device>(
    &self,
    virtual_sublog_idx: usize,
    sequence_number: i64,
    participant_count: i16,
    group: &TransactionGroup,
    as_replica: bool,
    target: &ReplayTarget<'_, '_, D>,
  ) -> Result<(), AofReplayError> {
    let session_id = group.session_id;
    if !as_replica || !self.append_only_file().multi_log_enabled() {
      let res = self
        .process_transaction_group_operations(virtual_sublog_idx, group, as_replica, target)
        .await;
      self
        .coordinator()
        .clear_session_txn(virtual_sublog_idx, session_id);
      return res;
    }

    // 前置 Acquire 栅栏（TxnStart 序列号；op 恒 None，仅对齐不执行）
    self
      .coordinator()
      .process_synchronized_operation_async(
        virtual_sublog_idx,
        group.start_sequence_number,
        participant_count,
        session_id,
        None::<fn() -> Ready<Result<(), AofReplayError>>>,
      )
      .await?;

    // 重放组内操作
    let res = self
      .process_transaction_group_operations(virtual_sublog_idx, group, as_replica, target)
      .await;

    // 后置 Release 栅栏（TxnCommit 序列号）
    self
      .coordinator()
      .process_synchronized_operation_async(
        virtual_sublog_idx,
        sequence_number,
        participant_count,
        session_id,
        None::<fn() -> Ready<Result<(), AofReplayError>>>,
      )
      .await?;

    self
      .coordinator()
      .clear_session_txn(virtual_sublog_idx, session_id);
    res
  }

  /// libs/server/AOF/ReplayCoordinator/AofReplayCoordinator.cs:ProcessTransactionGroupOperations
  ///
  /// 顺序重放事务组全部操作（组提交原子性由恢复免锁 / 副本锁集保障）；
  /// 组内条目失败即上抛（C# 无 catch，异常沿 Recover 传播至恢复失败）。
  pub async fn process_transaction_group_operations<D: Device>(
    &self,
    sublog_idx: usize,
    group: &TransactionGroup,
    _as_replica: bool,
    target: &ReplayTarget<'_, '_, D>,
  ) -> Result<(), AofReplayError> {
    for op in &group.operations {
      match op {
        ReplayOperation::Record(entry) => match AofHeader::parse(entry) {
          Some(header) => {
            let op_type =
              AofEntryType::try_from(header.op_type).map_err(|_| ERR_UNKNOWN_AOF_OP_TYPE)?;
            let prepared = self
              .prepare_key(sublog_idx, entry, group.start_sequence_number)
              .ok_or(ERR_AOF_PAYLOAD_CORRUPTED)?;
            self.replay_op(op_type, prepared, target).await?;
          }
          None => return Err(ERR_FUZZY_HEADER_CORRUPTED.into()),
        },
        ReplayOperation::Chunk(acc) => {
          super::aof_processor_chunk_replay::replay_chunk(self, acc, target).await?
        }
      };
    }
    Ok(())
  }

  /// libs/server/AOF/AofProcessor.cs:ReplayOpDispatch
  ///
  /// 按拓扑选择预处理路径并分派到 [`Self::replay_op`]。
  pub async fn replay_op_dispatch<D: Device>(
    &self,
    virtual_sublog_idx: usize,
    header: AofHeader,
    entry: &[u8],
    as_replica: bool,
    log_address_sequence_number: i64,
    target: &ReplayTarget<'_, '_, D>,
  ) -> Result<(), AofReplayError> {
    let op_type = AofEntryType::try_from(header.op_type).map_err(|_| ERR_UNKNOWN_AOF_OP_TYPE)?;
    if record_gate::should_skip_record(
      self.coordinator(),
      virtual_sublog_idx,
      entry,
      as_replica,
      target.store_version(),
      log_address_sequence_number,
      // 位点闸按条目所属物理子日志逐位取下界（虚拟下标 → 物理下标换算，
      // 公式真源 GetVirtualSublogIdx 的逆映射）
      target.aof_floor_of(virtual_sublog_idx / self.replay_task_count()),
    ) {
      return Ok(());
    }
    let prepared = self
      .prepare_key(virtual_sublog_idx, entry, log_address_sequence_number)
      .ok_or(ERR_AOF_PAYLOAD_CORRUPTED)?;
    self.replay_op(op_type, prepared, target).await
  }

  /// libs/server/AOF/AofProcessor.cs:ReplayOp
  ///
  /// 数据操作应用：按条目类型分发到主存 / 对象存 / 统一存应用面。
  /// 条目 key 为物理键，经 `KeyContextGuard` 直设条目虚拟域 `(vns, vdb)` 后
  /// 以用户键应用（drop 时恢复重放会话进入前的虚拟域，逻辑槽全程不动）。
  /// 仅本模块预处理臂消费，不外导。
  async fn replay_op<'a, D: Device>(
    &self,
    op_type: AofEntryType,
    prepared: PreparedParameters<'a>,
    target: &ReplayTarget<'_, '_, D>,
  ) -> Result<(), AofReplayError> {
    let PreparedParameters { key, payload } = prepared;
    let guard = KeyContextGuard::enter(target.session, &key)?;
    let key: &[u8] = guard.user_key;
    let tag = guard.tag;
    // DbMeta 镜像条目（0x0E）：映射体系应用单点，不落用户数据域——条目 key 为
    // 根域记录键载荷、val 为定长记录值，交 wkv 应用（doc/zh/db.md「从库完全
    // 继承主库的映射体系，不进行本地二次映射」；C# 单租户 databaseId 无此面）
    if tag == KeyTag::DbMeta {
      let vm = self.append_only_file().vector_manager();
      return replay_dbmeta(target, op_type, key, &payload, vm.as_deref()).await;
    }
    match op_type {
      AofEntryType::StoreUpsert => {
        let (value, _) = Self::split_value_input(&payload).ok_or("StoreUpsert 负载损坏")?;
        store_upsert(target.session, tag, key, value).await
      }
      AofEntryType::StoreRMW => store_rmw(self, target.session, key, &payload).await,
      AofEntryType::StoreDelete => store_delete(target.session, tag, key).await,
      AofEntryType::ObjectStoreUpsert => {
        let (value, _) = Self::split_value_input(&payload).ok_or("ObjectStoreUpsert 负载损坏")?;
        object_store_upsert(target.session, key, value).await
      }
      AofEntryType::ObjectStoreRMW => object_store_rmw(target.session, tag, key, &payload).await,
      AofEntryType::ObjectStoreDelete => object_store_delete(target.session, key).await,
      AofEntryType::UnifiedStoreStringUpsert => {
        let (value, _) =
          Self::split_value_input(&payload).ok_or("UnifiedStoreStringUpsert 负载损坏")?;
        store_upsert(target.session, KeyTag::String, key, value).await
      }
      AofEntryType::UnifiedStoreObjectUpsert => {
        let (value, _) =
          Self::split_value_input(&payload).ok_or("UnifiedStoreObjectUpsert 负载损坏")?;
        object_store_upsert(target.session, key, value).await
      }
      // C# UnifiedStoreRMW / UnifiedStoreDelete：wkv 统一面下与主存 RMW /
      // delete 同形
      AofEntryType::UnifiedStoreRMW => store_rmw(self, target.session, key, &payload).await,
      AofEntryType::UnifiedStoreDelete => store_delete(target.session, tag, key).await,
      // libs/server/AOF/AofProcessor.cs:HandleRangeIndexStreamChunk
      //（迁移索引流块重放；未注入 RI 面即按 C# 同文案失败）
      AofEntryType::RangeIndexStreamChunk => {
        let Some(ri) = self.range_index_manager() else {
          return Err(
            "RangeIndexPreview disabled; Replay failed"
              .to_string()
              .into(),
          );
        };
        let input = ReplayInput::deserialize(&payload).ok_or("StreamChunk input 损坏")?;
        ri.handle_range_index_stream_replay(&target.session.batch, key, &input)
          .await
          .map_err(AofReplayError::from)
      }
      // 已移除自定义事务过程支持（C# ReplayStoredProc 随本仓裁撤），过程条目
      // 到达即判损坏回退，与未知操作臂同走损坏拒收，专属文案保留单点
      AofEntryType::StoredProcedure => Err("已移除自定义事务过程支持，无法回放存储过程条目".into()),
      _ => Err(format!("Unknown AOF header operation type {op_type:?}").into()),
    }
  }
}
