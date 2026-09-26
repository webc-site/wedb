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
use wbase::{convert::expire_at_milliseconds_to_ticks, hash_slot::slot_of};
use wconn::record::{MigrationFrame, MigrationRecord, parse_record};
use wdev::SegmentedDevice;
use wkv::{DbMetaRecord, StoreSession};
use wnode::{
  StorageSession,
  range_index::{RangeIndexMigrationReceiveState, TreeStreamMeta},
  resp::vector::vector_store_callbacks::ActiveVectorSessionGuard,
  storage::session::common::ttl_sync::{probe_alive_with_prefix, put_ttl_sync},
};
use wresp::cmd_strings::{RESP_ERR_INVALID_MIGRATED_VECTOR_SET_INDEX, RESP_ERR_SLOW_PATH_STORAGE};
use wval::{GarnetObjectType, KeyTag, SessionPrefixBuf};

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
  /// doc/zh/db.md 4.1 下单会话单槽常量；SYNC 链落域上下文帧到达后逐载荷
  /// 换域，本字段即初始槽）
  pub vector_slot: u16,
  /// 跨域扩展帧承接开关（kind=7 落域上下文 / kind=8 DbMeta 映射，无盘全量
  /// 同步快照链专用）：MIGRATE 导槽链头声明槽位一次性门禁后导入，落域
  /// 上下文帧会把写入换到声明槽之外未门禁的域——导槽链显式拒绝；仅无盘
  /// 同步承接面（副本全量导入，域映射帧为锚前映射唯一通路）放行
  pub accept_domain_frames: bool,
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
    vector_slot: init_vector_slot,
    accept_domain_frames,
  } = import;
  // 当前向量帧登记槽：SYNC 链落域上下文帧到达后换域重算（MIGRATE 链恒为
  // 头声明槽——落域上下文帧已被上方开关拒绝）
  let mut vector_slot = init_vector_slot;
  // 会话前缀外提缓存（transpile SKILL 循环前缀外提准则）：记录写入面单次
  // 外提消逐键 ns/db 原子变量重读与 Varint 重算；SYNC 链落域上下文帧切换
  // 物理域后置空，下一记录懒重提
  let mut prefix_cache: Option<SessionPrefixBuf> = None;
  for frame in frames {
    // wbftree 带外分块流 (kind=4, 对标 C# MigrationRecordSpanType.SerializedRangeIndexStream；
    // rust 泛化：RangeIndex 与升阶分层集合共用，流元随帧携载)
    if let MigrationFrame::RangeIndexStream {
      obj_type,
      next_expiry,
      expire_unix_ms,
      bytes: ri_payload,
    } = frame
    {
      let Some(ri_state) = ri_receive_state else {
        return Err(RESP_ERR_SLOW_PATH_STORAGE.to_string());
      };
      // 帧面判别字节归 wval 类型枚举单点（非法判别值显式拒绝，绝不落端）
      let Some(obj_type) = GarnetObjectType::from_u8(obj_type) else {
        return Err(format!("ERR Invalid tree stream obj_type {obj_type}"));
      };
      // MIGRATE 链头声明槽位已由壳在循环前统一门禁，SYNC 承接面无槽门；
      // RI 接收态均不再逐记录判槽
      let mut ri_state = ri_state.lock().await;
      if !ri_state
        .process_record(
          ri_payload,
          TreeStreamMeta {
            obj_type,
            next_expiry,
            expire_unix_ms,
          },
          session,
          replace,
        )
        .await
      {
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

    // 跨域扩展帧（kind=7/8，无盘全量同步快照链专用；C# 单租户单库无此面）：
    // 落域上下文直设接收会话物理域并换算向量登记槽（从库继承主库映射体系，
    // 零本地二次映射），DbMeta 映射帧交 apply_dbmeta_record 单点应用（映射
    // 装载 + 水位抬升 + 本节点落盘，与 AOF 镜像条目回放同一应用口）
    if matches!(
      frame,
      MigrationFrame::DomainContext { .. } | MigrationFrame::DbMeta { .. }
    ) {
      if !accept_domain_frames {
        return Err("ERR Unexpected domain frame in migration payload".to_string());
      }
      match frame {
        MigrationFrame::DomainContext(ctx) => {
          // 直设臂显式携帧内逻辑域真值（源端下发 ctx.ns/ctx.db，从库继承
          // 映射零二次解析；版本轨=逻辑域种子的透传点）
          session.set_virtual_context(ctx.vns, ctx.vdb, ctx.ns, ctx.db);
          vector_slot = slot_of(ctx.ns, ctx.db);
          // 物理域已切换：外提前缀失效，下一记录懒重提
          prefix_cache = None;
        }
        MigrationFrame::DbMeta { key, value } => {
          let Some(rec) = DbMetaRecord::decode(key, value) else {
            return Err("ERR Invalid DbMeta migration record".to_string());
          };
          let Some(store) = provider.try_store() else {
            return Err(ERR_CLUSTER_NOT_INITIALIZED.to_string());
          };
          if let Err(err) = store.apply_dbmeta_record(rec).await {
            log::error!("迁移帧导入 DbMeta 映射应用失败: {err:?}");
            return Err(RESP_ERR_SLOW_PATH_STORAGE.to_string());
          }
          // 映射换代（NsMap/DbMap/DbSwap bump_generation）：未 virtual 会话的
          // 前缀经代数重解析可能改指，缓存随之失效（当前链路 SYNC 恒覆写短路
          // 探针、MIGRATE 拒域帧，本行为防御性闭合缓存不变式）
          prefix_cache = None;
        }
        _ => unreachable!("matches! 门内仅此两臂"),
      }
      continue;
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
      // 接收臂自持本同步段的向量会话绑定（对标 C# HandleMigratedIndexKey 的
      // `if (ActiveThreadSession == null) ActiveThreadSession = newStorageSession
      // = new StorageSession(...)` 自备会话臂，VectorManager.Migration.cs:164-170
      // 与收尾 :266 解绑）：本会话与写入面 `storage.batch` 同源（壳内
      // `session.enter_batch()`），落域上下文帧已先行换域，元素记录键前缀与
      // 索引登记前缀必一致。守卫只罩本同步段——帧循环他臂含 `.await`，
      // 跨 await 持有即破绑定栈纪律（见 wnode vector_store_callbacks 模块头）
      let _vector_domain = ActiveVectorSessionGuard::bind(session);
      // 目标端按本端会话域重新复合（迁移帧口径恒为剥域用户键）
      let prefix = storage.batch.session_prefix();
      let prefix = prefix.as_slice();
      let res = match frame {
        MigrationFrame::VectorSetIndex { key, value } => vm
          .import_migrated_index(prefix, key, value, vector_slot)
          .await
          .map_err(|msg| String::from_utf8_lossy(msg).into_owned()),
        MigrationFrame::VectorSetElement {
          key,
          element,
          values,
          attributes,
        } => match vm.read_migrated_index(prefix, key) {
          Some(index_value) => vm
            .import_migrated_element(prefix, key, &index_value, element, values, attributes)
            .await
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
      MigrationFrame::RangeIndexStream { .. }
      | MigrationFrame::VectorSetIndex { .. }
      | MigrationFrame::VectorSetElement { .. }
      | MigrationFrame::DomainContext(_)
      | MigrationFrame::DbMeta { .. } => unreachable!(),
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

    // REPLACE 判定（对标 C# replaceOption || !Exists）：同步三域存活探针先行
    //（String / ObjectEnvelope / Meta，先例 MSETNX 慢路径），磁盘候选或同步
    // 存储错误才降级异步三域闭环；异步仍失败保守判缺失写入（与原 contains_key
    // 探测失败即写口径一致）。已存在键在 replace=false 下跳过写入（目标端
    // 保留原值）
    let should_write = replace || {
      let prefix = prefix_cache
        .get_or_insert_with(|| storage.batch.session_prefix())
        .as_slice();
      let alive = match probe_alive_with_prefix(&storage.batch, prefix, record.key()) {
        Ok(Some(alive)) => alive,
        // 磁盘候选 / 同步存储错误：异步三域闭环裁决，错误与缺失同折 false
        //（保守写入）；复用外提前缀，闭环臂零 session_prefix() 重读
        Ok(None) | Err(_) => storage
          .probe_alive_domain_with_prefix(prefix, record.key())
          .await
          .ok()
          .flatten()
          .is_some(),
      };
      !alive
    };

    if should_write {
      let write_res = match &record {
        MigrationRecord::Str { key, val, .. } => storage.upsert_string(key, val).await,
        MigrationRecord::Env { key, env, .. } => {
          // 迁移覆盖写为 SET 语义：信封域 upsert 默认保留 key 级 TTL（RMW
          // 语义），目标键残留旧 TTL 会误作用于迁移值，先显式清退（对标
          // C# 迁移经 basicGarnetApi.SET 的 Upsert 无 Expiration 语义）；
          // 清退失败即判错：值尚未写入零污染面，且与 TTL 回填失败同口径
          //（C# 接收端单步 basicGarnetApi.SET 原子写 TTL 无此独立失败面，
          // 静默吞错即旧 TTL 残留误作用于迁移值）
          if let Err(err) = storage.persist_key(key).await {
            log::error!(
              "迁移帧导入旧 TTL 清退失败 key {}: {err:?}",
              String::from_utf8_lossy(key)
            );
            return Err(RESP_ERR_SLOW_PATH_STORAGE.to_string());
          }
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
        // 超 i64::MAX 钳到最大可表示 ticks，杜绝 debug 构建溢出 panic；
        // 毫秒换算产物恒为 16 对齐，会话入口/内核恒等直通下与直存值逐位
        // 恒等，§143）
        let expire_ticks = expire_at_milliseconds_to_ticks(expire_unix_ms);
        // TTL 裸写同步快路径先行（先例 network_expire）：本循环刚写入该键
        // 且 Str 写已清 TTL / Env 写前已 persist，expire_at 的键存活与既有
        // TTL 判定对本键恒冗余，put_ttl_sync 一次闭环零 await；未闭环
        //（环形页翻转 / 存储错误）降级 expire_at_ticks 全量 RMW 语义收尾。
        // 双降级均失败即判错（与 RI 带外通道 process_record_internal 的
        // TTL 回填失败 log + 判错同口径，消除同流双通道行为分叉）：值已写
        // 而 TTL 丢失即 Str 键变永不过期，迁移应答不得照常 OK，发送端停等
        // 判败走 recover
        if !matches!(
          put_ttl_sync(&storage.batch, record.key(), expire_ticks),
          Ok(true)
        ) && let Err(err) = storage.expire_at_ticks(record.key(), expire_ticks).await
        {
          log::error!(
            "迁移帧导入 TTL 回填失败 key {}: {err:?}",
            String::from_utf8_lossy(record.key())
          );
          return Err(RESP_ERR_SLOW_PATH_STORAGE.to_string());
        }
      }
    }
  }
  Ok(())
}
