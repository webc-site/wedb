//! 迁移帧导入核心（对标 libs/cluster/Session/RespClusterMigrateCommands.cs
//! 局部函数 Process 的单实体形态）
//!
//! CLUSTER MIGRATE（导槽接收）与 CLUSTER SYNC（无盘全量同步承接）两链共用
//! 本核心完成帧分派与写入：分块重组、RangeIndex 分块流、向量集帧、string /
//! 对象信封记录写入与 TTL 回填。接收态两链同源（C# 两类命令同为
//! RespServerSession 分部，per-connection 的 chunkedRecordReassembler 与
//! rangeIndexMigrationState 字段两链共用一处）：均取 ClusterSession 会话字
//! 段，协议面差异（migrate 头声明槽门禁、文案前缀、覆写口径、登记槽取值）
//! 留在各壳。本模块只消费 wconn::record 已解码的帧视图——帧编解码单点在
//! wconn::record，此处不起第二套解析。
//!
//! C# SYNC 接收面（对标 libs/cluster/Session/RespClusterReplicationCommands
//! .cs 的 NetworkClusterSync）只承接 LogRecord/ChunkedLogRecord，其余 kind
//! 直接抛 InvalidOperationException；本仓 rust 的 SYNC 链自扩展承载 RI/向量
//! 帧，两链共核后意外输入统一显式拒绝，绝不静默写。

use std::sync::Arc;

use async_lock::Mutex as AsyncLockMutex;
use parking_lot::Mutex;
use wbase::convert::expire_at_milliseconds_to_ticks;
use wconn::record::{MigrationFrame, MigrationRecord, parse_record};
use wdev::SegmentedDevice;
use wkv::StoreSession;
use wnode::{StorageSession, rangeindex::RangeIndexMigrationReceiveState};
use wresp::cmd_strings::{RESP_ERR_INVALID_MIGRATED_VECTOR_SET_INDEX, RESP_ERR_SLOW_PATH_STORAGE};
use wval::KeyTag;

use crate::server::{
  cluster_provider::ClusterProvider,
  cluster_session::ERR_CLUSTER_NOT_INITIALIZED,
  migration::{chunk_reassembler::ChunkReassembler, migrate_driver::migratable_object_type},
};

/// 导入核心执行上下文（对标 C# Process 闭包捕获的常量集：两链除帧流与
/// 覆写口径/登记槽外逐项同源；接收态为 ClusterSession 会话字段借用）
pub struct FrameImport<'a> {
  /// provider（向量集管理器句柄取用）
  pub provider: &'a ClusterProvider,
  /// 底层存储会话（RI 接收态流式落盘借用）
  pub session: &'a StoreSession<SegmentedDevice>,
  /// 写回存储会话（`StorageSession::new` + provider 共享版本表，由壳构造）
  pub storage: &'a StorageSession<'a, SegmentedDevice>,
  /// 会话分块重组器
  pub chunks: &'a Mutex<ChunkReassembler>,
  /// 会话 RangeIndex 接收态（store 未装配时为 None，碰 RI 帧按存储拒绝）
  pub ri: &'a Option<Arc<AsyncLockMutex<RangeIndexMigrationReceiveState>>>,
  /// REPLACE 覆写口径（对标 C# replaceOption；SYNC 承接面无存在性跳过
  /// 语义，恒 true = 无条件覆写）
  pub replace: bool,
  /// 向量集帧登记槽（MIGRATE 头声明槽、SYNC 复制会话库槽；库级定槽
  /// doc/zh/db.md 4.1 下单会话单槽常量）
  pub vector_slot: u16,
}

impl FrameImport<'_> {
  /// 错误收场复位两接收态：丢弃半途分块残段与 RI 接收流（发送端停等判败
  /// 走 recover 弃连，残段不得污染本会话后续迁移/同步流；对标 C#
  /// RespClusterMigrateCommands.cs:178/:208 的 chunkedRecordReassembler.
  /// Reset 错误恢复语义）
  pub fn reset_receive_states(&self) {
    self.chunks.lock().reset();
    if let Some(state) = self.ri
      && let Some(mut guard) = state.try_lock()
    {
      guard.reset();
    }
  }
}

/// libs/cluster/Session/RespClusterMigrateCommands.cs:Process
/// libs/cluster/Session/RespClusterMigrateCommands.cs:CompleteChunkedRecordReassembly
///
/// 迁移帧导入核心：逐帧消费已解码载荷帧视图并写入存储会话。帧序与拒绝语义
/// （RI 流 → 协议一致性 → 向量帧 → 分块/记录写入 → envelope 类型门 →
/// REPLACE 存在性门 → TTL 回填）一处定义，MIGRATE / SYNC 两链共用；新帧类
/// 型只在本函数补齐。Err 只带回应答文案，接收态复位由各壳错误收场单点执行
/// （[`FrameImport::reset_receive_states`]）。
pub async fn import_migration_frames(
  frames: Vec<MigrationFrame<'_>>,
  import: &FrameImport<'_>,
) -> Result<(), String> {
  let &FrameImport {
    provider,
    session,
    storage,
    chunks,
    ri: ri_receive_state,
    replace,
    vector_slot,
  } = import;
  for frame in frames {
    // 范围索引分块流 (kind=4, 对标 C# MigrationRecordSpanType.SerializedRangeIndexStream)
    if let MigrationFrame::RangeIndexStream(ri_payload) = frame {
      let Some(ri_state) = ri_receive_state else {
        return Err(RESP_ERR_SLOW_PATH_STORAGE.to_string());
      };
      // MIGRATE 链头声明槽位已由壳在循环前统一门禁，SYNC 承接面无槽门；
      // RI 接收态均不再逐记录判槽
      let mut ri_state = ri_state.lock().await;
      if !ri_state.process_record(ri_payload, session, replace).await {
        return Err("ERR Failed to process RangeIndex migration record".to_string());
      }
      continue;
    }

    // 协议一致性检查：若正处于 RangeIndex 接收流中，拒绝非 RangeIndex 帧
    //（锁被占用 = 他处写回进行中，保守视作接收中）。诚实发送端 RI 流逐键
    // 整流连发（快照面与迁移面均收齐即复位），本检查不误杀正常流，只杀
    // 中断流
    let ri_receiving = ri_receive_state
      .as_ref()
      .is_some_and(|state| match state.try_lock() {
        Some(guard) => guard.is_receiving(),
        None => true,
      });
    if ri_receiving {
      log::error!("Protocol violation: expected SerializedRangeIndexStream continuation");
      return Err(
        "ERR Protocol violation: expected SerializedRangeIndexStream continuation".to_string(),
      );
    }

    // 向量集帧（kind=5/6，对标 C# HandleMigratedIndexKey /
    // HandleMigratedElementKey 的接收段）：索引帧先行建内存索引（上下文
    // 须已 RESERVE 预留），元素帧按 VADD 语义真实插入
    if matches!(
      frame,
      MigrationFrame::VectorSetIndex { .. } | MigrationFrame::VectorSetElement { .. }
    ) {
      let Some(vm) = provider.try_vector_manager() else {
        return Err(ERR_CLUSTER_NOT_INITIALIZED.to_string());
      };
      // 目标端按本端会话域重新复合（迁移帧口径恒为剥域用户键）
      let prefix = storage.batch.session_prefix();
      let prefix = prefix.as_slice();
      let res = match frame {
        MigrationFrame::VectorSetIndex { key, value } => vm
          .import_migrated_index(prefix, key, value, vector_slot)
          .map_err(|msg| String::from_utf8_lossy(msg).into_owned()),
        MigrationFrame::VectorSetElement {
          key,
          element,
          values,
          attributes,
        } => match vm.read_migrated_index(prefix, key) {
          Some(index_value) => vm
            .import_migrated_element(prefix, key, &index_value, element, values, attributes)
            .map_err(|msg| String::from_utf8_lossy(msg).into_owned()),
          None => Err(RESP_ERR_INVALID_MIGRATED_VECTOR_SET_INDEX.to_string()),
        },
        _ => Ok(()),
      };
      res?;
      continue;
    }

    // 分块重组产物缓冲（借用需存活到本条记录处理完）
    let chunk_raw: Option<Vec<u8>>;
    // 分块帧：喂会话重组器，末块到达产出完整记录编码后复用单记录路径
    //（对标 C# ChunkedLogRecord 分支 → CompleteChunkedRecordReassembly）
    let record = match frame {
      MigrationFrame::Chunk { bytes, more } => {
        chunk_raw = chunks.lock().append(bytes, more);
        match chunk_raw.as_deref().map(parse_record) {
          Some(Ok(record)) => record,
          Some(Err(e)) => {
            return Err(format!("ERR Invalid migration payload: {e:?}"));
          }
          None => continue,
        }
      }
      MigrationFrame::Record(record) => record,
      MigrationFrame::RangeIndexStream(_)
      | MigrationFrame::VectorSetIndex { .. }
      | MigrationFrame::VectorSetElement { .. } => unreachable!(),
    };

    // 帧内暂不支持类型显式拒绝（RangeIndex 树 / 向量集走带外专用编排
    // migrate_session_range_index.rs / migrate_session_vector_set.rs，此为
    // 帧内兜底拒绝面，绝不静默当 string 写入；SYNC 链同门——C# SYNC 面意外
    // kind 本就抛，诚实发送端经 read_live_value 门控不触达本分支）
    if let MigrationRecord::Env { env, .. } = &record {
      let inner = env.first().copied().unwrap_or(0);
      if !migratable_object_type(inner) {
        return Err(format!(
          "ERR Unsupported migration record kind 2 (envelope type {inner})"
        ));
      }
    }

    // REPLACE 判定（对标 C# replaceOption || !Exists）：contains_key 双域
    // 存在性——已存在键在 replace=false 下跳过写入（目标端保留原值）；
    // 探测失败保守写入（与 read_string 失败口径一致）
    let should_write = replace
      || match storage.batch.contains_key(record.key()).await {
        Ok(false) => true,
        Ok(true) => false,
        Err(_) => true,
      };

    if should_write {
      let write_res = match &record {
        MigrationRecord::Str { key, val, .. } => storage.upsert_string(key, val).await,
        MigrationRecord::Env { key, env, .. } => {
          // 迁移覆盖写为 SET 语义：信封域 upsert 默认保留 key 级 TTL（RMW
          // 语义），目标键残留旧 TTL 会误作用于迁移值，先显式清退（对标
          // C# 迁移经 basicGarnetApi.SET 的 Upsert 无 Expiration 语义）；
          // 源键 TTL 由下方 expire_at_ticks 回填（清退失败罕见磁盘候选，
          // 与 expire 回填同款容错忽略）
          let _ = storage.persist_key(key).await;
          storage.upsert_tag(key, KeyTag::ObjectEnvelope, env).await
        }
      };
      if let Err(err) = write_res {
        log::error!("迁移帧导入记录写回失败: {err:?}");
        return Err(RESP_ERR_SLOW_PATH_STORAGE.to_string());
      }
      let expire_unix_ms = record.expire_unix_ms();
      if expire_unix_ms > 0 {
        // 换算委托 wbase::convert 单点（钳制版：迁移接收端外部输入可触达，
        // 超 i64::MAX 钳到最大可表示 ticks，杜绝 debug 构建溢出 panic）
        let expire_ticks = expire_at_milliseconds_to_ticks(expire_unix_ms);
        let _ = storage.expire_at_ticks(record.key(), expire_ticks).await;
      }
    }
  }
  Ok(())
}
