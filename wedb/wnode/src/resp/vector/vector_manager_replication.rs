//! 向量操作复制面（对标 libs/server/Resp/Vector/VectorManager.Replication.cs）
//!
//! 本仓副本模型为 AOF 直推（副本/恢复统一走 AOF 记录重放）：
//! - 生产端：VADD/VREM/VSETATTR 主侧成功后以合成 StoreRMW 条目直接入队
//!   AOF（C# ReplicateVectorSetAdd/Remove/SetAttribute 经主日志 RMW 假写的
//!   rust 形态），parseState 参数布局逐参对齐 C# VectorSetAdd 的 10 参形态；
//! - 重放端：AofProcessor 向量分支按命令分派到本文件的重放应用方法
//!   （C# HandleVectorSet*Replication 的 rust 形态），顺序同步应用（C# 的
//!   VADD 后台 channel 消费面不落地，AOF 全序 + 同步应用天然保序）。

use std::sync::{
  Arc, Weak,
  atomic::{AtomicU64, Ordering},
};

use waof::{AofEntryType, Error};
use wbase::group_commit::Broken;
use wkv::VERSION_MASK;
use wresp::command::RespCommand;
use wval::{KeyTag, NamespaceDbCodec};
use wvector::{VectorDistanceMetricType, VectorQuantType, VectorValueType, store::StoreCallbacks};

use super::{
  vector_manager::{
    RECORD_TYPE, VADD_APPEND_LOG_ARG, VREM_APPEND_LOG_ARG, VSETATTR_APPEND_LOG_ARG,
    VSETINDEX_APPEND_LOG_ARG, VectorAddArgs, VectorManager, VectorManagerResult,
  },
  vector_manager_index::Index,
  vector_manager_locking::{CreateIndexParams, VectorSetKeyLocks, registry_key},
};
use crate::aof::{
  aof_processor::AofReplayError,
  garnet_append_only_file::GarnetAppendOnlyFile,
  replay_input::{ReplayInput, ReplayInputSlice},
};

/// AOF 直推注入端口（服务器级装配期注入）。
///
/// `aof` 持 Weak 破解「AOF 门面（重放面）↔ VectorManager（生产面）」的
/// 强引用环；`version` 为 hlog 版本推进窗口合字本体（条目 store_version 按
/// [`whlog::VERSION_MASK`] 掩取版本域，重放端检查点代际过滤用；与引擎
/// current_version 单原子同源，绝无第二套版本源）。
pub struct VectorAofSink {
  aof: Weak<GarnetAppendOnlyFile>,
  version: Arc<AtomicU64>,
}

impl VectorAofSink {
  /// 装配注入端口。
  pub fn new(aof: &Arc<GarnetAppendOnlyFile>, version: Arc<AtomicU64>) -> Self {
    Self {
      aof: Arc::downgrade(aof),
      version,
    }
  }

  /// 合成 StoreRMW 条目入队（用户键 → 物理键编码；零分配切片编码）。
  ///
  /// 条目键带条目所属会话域（`encode_with_session_prefix`），重放端经
  /// KeyContextGuard 切域后按同域重建登记表——消除恒根域入账的跨域错账。
  fn enqueue(
    &self,
    cmd: RespCommand,
    log_arg: i64,
    prefix: &[u8],
    key: &[u8],
    args: &[&[u8]],
  ) -> Result<(), waof::Error> {
    let Some(aof) = self.aof.upgrade() else {
      return Err(Error::PipelineBroken(Broken));
    };
    let input = ReplayInputSlice::new_deterministic(cmd, args).with_args_num(log_arg, 0, 0);
    let physical_key = NamespaceDbCodec::encode_with_session_prefix(prefix, KeyTag::String, key);
    let ver = (self.version.load(Ordering::SeqCst) & VERSION_MASK) as i64;
    aof
      .enqueue_rmw_slices(AofEntryType::StoreRMW, ver, physical_key.as_slice(), &input)
      .map(|_| ())
      .map_err(|e| {
        log::error!("Vector replication AOF enqueue failed: {e}");
        e
      })
  }
}

/// VADD 合成写条目的固定 4B 数值参（C# MemoryMarshal.Cast 形态）。
#[inline]
fn le4(v: u32) -> [u8; 4] {
  v.to_le_bytes()
}

/// 4B LE 数值参解析（越界/长度不符返回 None）。
#[inline]
fn arg_u32(input: &ReplayInput, idx: usize) -> Option<u32> {
  let b = input.args.get(idx)?;
  if b.len() != 4 {
    return None;
  }
  Some(u32::from_le_bytes(b.as_slice().try_into().ok()?))
}

/// 4B LE 枚举判别参解析（i32 判别值 → u8 判别值）。
#[inline]
fn arg_enum_u8(input: &ReplayInput, idx: usize) -> Option<u8> {
  arg_u32(input, idx).map(|v| v as u8)
}

/// i32 判别值 → 量化类型（与 C# VectorQuantType 数值一致）。
fn quant_from_arg(v: u8) -> VectorQuantType {
  super::vector_manager_index::quant_from_i32(i32::from(v))
}

/// i32 判别值 → 距离度量（与 C# VectorDistanceMetricType 数值一致）。
fn metric_from_arg(v: u8) -> VectorDistanceMetricType {
  super::vector_manager_index::metric_from_i32(i32::from(v))
}

/// i32 判别值 → 值格式（FP32=1 / XU8=2 / XI8=3，未知收敛 Invalid）。
fn value_type_from_arg(v: u8) -> VectorValueType {
  match v {
    1 => VectorValueType::FP32,
    2 => VectorValueType::XU8,
    3 => VectorValueType::XI8,
    _ => VectorValueType::Invalid,
  }
}

/// 条目参数缺失误文案（天然无类型源，落 [`AofReplayError::Replay`]；
/// 单源直出，分派臂不再套第二段 stringify）
fn corrupt(cmd: &str) -> AofReplayError {
  AofReplayError::Replay(format!("vector {cmd} replay input corrupted"))
}

impl<S: StoreCallbacks> VectorManager<S> {
  /// libs/server/Resp/Vector/VectorManager.Replication.cs:ReplicateVectorSetAdd
  ///
  /// VADD 成功后的合成写注入（AOF: YES；参数布局对齐 C# VectorSetAdd 的
  /// parseState 10 参：dims/reduceDims/valueType/values/element/quantizer/
  /// buildExplorationFactor/attributes/numLinks/distanceMetric）。
  pub fn replicate_vector_set_add(
    &self,
    prefix: &[u8],
    key: &[u8],
    dims: u32,
    build_exploration_factor: u32,
    args: &VectorAddArgs<'_>,
  ) -> Result<(), waof::Error> {
    let Some(sink) = self.aof_sink.read().clone() else {
      return Ok(());
    };
    let entry_args = [
      &le4(dims)[..],
      &le4(args.reduce_dims)[..],
      &le4(args.value_type as u32)[..],
      args.values,
      args.element,
      &le4(args.quant_type as u32)[..],
      &le4(build_exploration_factor)[..],
      args.attributes,
      &le4(args.num_links)[..],
      &le4(args.distance_metric as u32)[..],
    ];
    sink.enqueue(
      RespCommand::Vadd,
      VADD_APPEND_LOG_ARG,
      prefix,
      key,
      &entry_args,
    )
  }

  /// libs/server/Resp/Vector/VectorManager.Replication.cs:ReplicateVectorSetRemove
  ///
  /// VREM 成功后的合成写注入（AOF: YES；参数 = element）。
  pub fn replicate_vector_set_remove(
    &self,
    prefix: &[u8],
    key: &[u8],
    element: &[u8],
  ) -> Result<(), waof::Error> {
    let Some(sink) = self.aof_sink.read().clone() else {
      return Ok(());
    };
    let args = [element];
    sink.enqueue(RespCommand::Vrem, VREM_APPEND_LOG_ARG, prefix, key, &args)
  }

  /// libs/server/Resp/Vector/VectorManager.Replication.cs:ReplicateVectorSetSetAttribute
  ///
  /// VSETATTR 成功后的合成写注入（AOF: YES；参数 = element + attribute）。
  pub fn replicate_vector_set_set_attribute(
    &self,
    prefix: &[u8],
    key: &[u8],
    element: &[u8],
    attribute: &[u8],
  ) -> Result<(), waof::Error> {
    let Some(sink) = self.aof_sink.read().clone() else {
      return Ok(());
    };
    let args = [element, attribute];
    sink.enqueue(
      RespCommand::Vsetattr,
      VSETATTR_APPEND_LOG_ARG,
      prefix,
      key,
      &args,
    )
  }

  /// libs/server/Resp/Vector/VectorManager.Replication.cs:HandleVectorSetAddReplication
  ///
  /// 重放应用 VADD：解析 10 参 → 读或建索引（对齐 C# ReadOrCreateVectorIndex
  /// 的重放侧形态，键缺失按复制参数补建）→ TryAdd 语义插入。
  /// 非 OK 结果对齐 C# 抛异常语义，返回错误中止重放。
  pub async fn replay_vector_set_add(
    &self,
    prefix: &[u8],
    key: &[u8],
    slot: u16,
    input: &ReplayInput,
  ) -> Result<(), AofReplayError> {
    let dims = arg_u32(input, 0).ok_or_else(|| corrupt("VADD"))?;
    let reduce_dims = arg_u32(input, 1).ok_or_else(|| corrupt("VADD"))?;
    let value_type = value_type_from_arg(arg_enum_u8(input, 2).ok_or_else(|| corrupt("VADD"))?);
    let values = input.args.get(3).ok_or_else(|| corrupt("VADD"))?;
    let element = input.args.get(4).ok_or_else(|| corrupt("VADD"))?;
    let quantizer = quant_from_arg(arg_enum_u8(input, 5).ok_or_else(|| corrupt("VADD"))?);
    let build_ef = arg_u32(input, 6).ok_or_else(|| corrupt("VADD"))?;
    let attributes = input.args.get(7).ok_or_else(|| corrupt("VADD"))?;
    let num_links = arg_u32(input, 8).ok_or_else(|| corrupt("VADD"))?;
    let distance_metric = metric_from_arg(arg_enum_u8(input, 9).ok_or_else(|| corrupt("VADD"))?);

    let params = CreateIndexParams {
      hash_slot: slot,
      dims,
      reduce_dims,
      quant: quantizer,
      build_exploration_factor: build_ef,
      num_links,
      distance_metric,
    };
    let (index, _lock) = self
      .read_or_create_vector_index(prefix, key, Some(&params))
      .await
      .map_err(|e| format!("Failed to read or create Vector Set index during AOF replay: {e:?}"))?;

    // 覆盖语义：重放即重建。登记表虽已写透持久化且恢复链原位复用 context，
    // 此处先清同元素残留（含内存与 vector_key 磁盘记录）再插入，作为纵深防御
    // 保证 AOF 全序重放幂等收敛，其余旧代残留交由后台清理回收。
    self.service.remove(index.context, element).await;

    // await 前释放登记读锁：try_add 只消费索引快照（to_bytes），不依赖
    // 登记条目锁；读锁跨 await 持有虽合法（异步锁）但壅塞并发重建无必要
    let index_bytes = index.to_bytes();
    drop(_lock);

    let add_args = VectorAddArgs {
      element,
      value_type,
      values,
      attributes,
      reduce_dims,
      quant_type: quantizer,
      num_links,
      distance_metric,
    };
    match self.try_add(prefix, key, &index_bytes, &add_args).await {
      Ok(VectorManagerResult::OK) => Ok(()),
      Ok(other) => Err(format!(
        "Failed to add to Vector Set index during AOF replay, this should never happen but will cause data loss if it does: {other:?}"
      )
      .into()),
      Err(e) => Err(format!(
        "Failed to add to Vector Set index during AOF replay: {}",
        String::from_utf8_lossy(&e.message)
      )
      .into()),
    }
  }

  /// libs/server/Storage/Session/UnifiedStore/UnifiedStoreOps.cs 的 RENAME 的
  /// RENAME 合成写注入（C# 经 SET(newKey, RENAME input) 落 AOF 的对偶：
  /// cmd=RENAME、arg1=RecordType 哨兵、参数=[旧名, 新名]、条目键=新名）。
  /// 副本/恢复端 store_rmw 向量分支按哨兵分派到
  /// [`Self::replay_vector_set_rename`] 迁移登记表项。入队失败按 error.rs
  /// AofEnqueue 契约上抛拒绝命令（与 add/remove/set_attribute 注入臂同一
  /// 冒泡机制，禁弃错回成功防主从静默发散）。
  pub fn replicate_vector_set_rename(
    &self,
    prefix: &[u8],
    old_key: &[u8],
    new_key: &[u8],
  ) -> Result<(), waof::Error> {
    let Some(sink) = self.aof_sink.read().clone() else {
      return Ok(());
    };
    let args = [old_key, new_key];
    sink.enqueue(
      RespCommand::Rename,
      i64::from(RECORD_TYPE),
      prefix,
      new_key,
      &args,
    )
  }

  /// libs/server/Resp/Vector/VectorManager.Replication.cs:HandleVectorSetRenameCopy
  ///
  /// 重放应用 RENAME：旧名登记必在（VADD 条目先行重建，对齐 C# 重放端
  /// "Should never fail to find original key during RENAME replay"），
  /// 读旧登记快照写新名并摘除旧名（登记表形态的迁移单条目闭环；
  /// C# 拷贝先于 MarkSuppressCleanup，窗口标志不随拷贝迁移，同口径）。
  /// 槽位元数据随迁移同步（C# 经元数据记录随日志复制收敛，rust 登记表
  /// 形态在重放端就地同步）。
  ///
  /// 锁协议与主端口 [`Self::rename_vector_set_of`] 同锁轴同定序（单套机制，
  /// 严禁第二套锁）：双键 [`VectorSetKeyLocks::acquire`] 条带独占锁全程持有，
  /// 锁内完成登记读改写——副本常驻重放线程不取锁即直写登记表会穿透共享锁
  /// 屏障（VSIM/VEMB 持共享读锁拷出的索引句柄可在存续期内被本臂无锁丢弃，
  /// context 复用后旧读者打到复用后的新集合图上），与用户命令互斥由此屏障
  /// 承接；重放吞吐非热路径，阻塞等锁可接受。
  /// 被顶（displaced）既有登记的清退复用 [`Self::delete_vector_set_of`]
  /// 锁内复核单点（request_deletion + 摘表），不裸调 request_deletion
  /// 另立第二套清退形态；主端 delete_vector_set(new_key) 不入 AOF，
  /// 重放端须自清，否则 VADD 条目重建而来的上下文每次 rename onto
  /// 泄漏一个。
  ///
  /// WATCH 推进（r7-data 条 2）：迁移成功后新旧键各恰一推，复用主端
  /// [`Self::rename_vector_set`] / [`Self::rename_vector_set_of`] 两点分工的
  /// 语义位（旧键 DELETE(old) 一推 + 新键 SET(newKey) 一推）——C# 副本重放
  /// RENAME 经 SET(newKey)+DELETE(oldKey) 写钩子恒 IncrementVersion
  ///（UnifiedStoreOps.cs RENAME 主流程，重放与交互共路径无豁免），rust 常规键
  /// 重放臂（upsert_tag / expire_at_ticks / persist_key）同推进；WatchHook
  /// 未装配零开销旁路
  pub async fn replay_vector_set_rename(
    &self,
    prefix: &[u8],
    new_key: &[u8],
    input: &ReplayInput,
  ) -> Result<(), AofReplayError> {
    let old_key = input.args.first().ok_or_else(|| corrupt("RENAME"))?;
    if old_key == new_key {
      return Ok(());
    }
    let rk_old = registry_key(prefix, old_key);
    let rk_new = registry_key(prefix, new_key);
    let rk_old = rk_old.as_slice();
    let rk_new = rk_new.as_slice();
    // 双键条带独占锁：同条带单次获取，异条带按序号定序（与主端口
    // rename_vector_set_of 同序，杜绝与并发用户 RENAME 交叉互等）
    let _locks = VectorSetKeyLocks::acquire(&self.vector_set_locks, rk_old, rk_new).await;
    let Some(index_value) = self.stored_index_of(rk_old) else {
      return Err(
        "Should never fail to find original key during RENAME replay"
          .to_string()
          .into(),
      );
    };
    // 锁内复核清退被顶的既有登记（见头注：单点复用，非裸 request_deletion）
    self.delete_vector_set_of(rk_new).await;
    if !self.put_stored_index(rk_new, &index_value).await {
      log::error!("replay_vector_set_rename: 新键登记写透失败: {rk_new:?}");
    }
    if !self.remove_stored_index(rk_old).await {
      log::error!("replay_vector_set_rename: 旧键登记摘除失败: {rk_old:?}");
    }
    // 双键 WATCH 恰一推（见头注）：先旧键（DELETE(old) 语义位）后新键
    //（SET(newKey) 语义位），与主端迁移序一致
    self.bump_watch(prefix, old_key);
    self.bump_watch(prefix, new_key);
    Ok(())
  }

  /// libs/server/Resp/Vector/VectorManager.Migration.cs:ReplicateMigratedIndexKey
  ///
  /// 迁移索引导入成功后的合成写注入（AOF: YES；C# 为 HandleMigratedIndexKey
  /// 内嵌局部函数，arg1 = MigrateIndexKeyLogArg 三参假写 [key, value,
  /// context]；rust 复用 Vadd 命令通道，arg1 = [`VSETINDEX_APPEND_LOG_ARG`]
  /// 哨兵区分于用户 VADD，参数 = [dims, reduceDims, quantizer, buildEF,
  /// numLinks, distanceMetric] 六参紧凑形态）。登记表虽已写透持久化，
  /// 但副本端无迁移帧，仍需合成写注入 AOF 复制链供副本端同步。
  pub fn replicate_vector_set_index(
    &self,
    prefix: &[u8],
    key: &[u8],
    index: &Index,
  ) -> Result<(), waof::Error> {
    let Some(sink) = self.aof_sink.read().clone() else {
      return Ok(());
    };
    let entry_args = [
      &le4(index.dimensions)[..],
      &le4(index.reduce_dims)[..],
      &le4(index.quant_type as u32)[..],
      &le4(index.build_exploration_factor)[..],
      &le4(index.num_links)[..],
      &le4(index.distance_metric as u32)[..],
    ];
    sink.enqueue(
      RespCommand::Vadd,
      VSETINDEX_APPEND_LOG_ARG,
      prefix,
      key,
      &entry_args,
    )
  }

  /// 迁移索引条目的重放应用：按六参几何补建登记表与内存索引（零元素
  /// 插入，card=0 合法空集语义）。既有登记命中时几何参数原样放行
  ///（read_or_create 对已初始化索引不校验建造参），元素条目随后照常重放
  pub async fn replay_vector_set_index(
    &self,
    prefix: &[u8],
    key: &[u8],
    slot: u16,
    input: &ReplayInput,
  ) -> Result<(), AofReplayError> {
    let dims = arg_u32(input, 0).ok_or_else(|| corrupt("VADD-INDEX"))?;
    let reduce_dims = arg_u32(input, 1).ok_or_else(|| corrupt("VADD-INDEX"))?;
    let quantizer = quant_from_arg(arg_enum_u8(input, 2).ok_or_else(|| corrupt("VADD-INDEX"))?);
    let build_ef = arg_u32(input, 3).ok_or_else(|| corrupt("VADD-INDEX"))?;
    let num_links = arg_u32(input, 4).ok_or_else(|| corrupt("VADD-INDEX"))?;
    let distance_metric =
      metric_from_arg(arg_enum_u8(input, 5).ok_or_else(|| corrupt("VADD-INDEX"))?);
    let params = CreateIndexParams {
      hash_slot: slot,
      dims,
      reduce_dims,
      quant: quantizer,
      build_exploration_factor: build_ef,
      num_links,
      distance_metric,
    };
    let (_index, _lock) = self
      .read_or_create_vector_index(prefix, key, Some(&params))
      .await
      .map_err(|e| format!("Failed to read or create Vector Set index during AOF replay: {e:?}"))?;
    Ok(())
  }

  /// libs/server/Resp/Vector/VectorManager.Replication.cs:HandleVectorSetRemoveReplication
  ///
  /// 重放应用 VREM：读索引（重放条目主端必成功，键必在）→ TryRemove 语义。
  pub async fn replay_vector_set_remove(
    &self,
    prefix: &[u8],
    key: &[u8],
    input: &ReplayInput,
  ) -> Result<(), AofReplayError> {
    let element = input.args.first().ok_or_else(|| corrupt("VREM"))?;
    let (index, _lock) = self.read_vector_index(prefix, key).await;
    let index = index
      .ok_or_else(|| AofReplayError::from("Failed to read Vector Set index during AOF replay"))?;
    match self
      .try_remove(prefix, key, &index.to_bytes(), element)
      .await
    {
      VectorManagerResult::OK => Ok(()),
      other => {
        Err(format!("Failed to remove from Vector Set index during AOF replay: {other:?}").into())
      }
    }
  }

  /// libs/server/Resp/Vector/VectorManager.Replication.cs:HandleVectorSetSetAttributeReplication
  ///
  /// 重放应用 VSETATTR：读索引 → TrySetAttribute 语义。
  pub async fn replay_vector_set_set_attribute(
    &self,
    prefix: &[u8],
    key: &[u8],
    input: &ReplayInput,
  ) -> Result<(), AofReplayError> {
    let element = input.args.first().ok_or_else(|| corrupt("VSETATTR"))?;
    let attribute = input.args.get(1).ok_or_else(|| corrupt("VSETATTR"))?;
    let (index, _lock) = self.read_vector_index(prefix, key).await;
    let index = index
      .ok_or_else(|| AofReplayError::from("Failed to read Vector Set index during AOF replay"))?;
    if self
      .try_set_attribute(prefix, key, &index.to_bytes(), element, attribute)
      .await
    {
      Ok(())
    } else {
      Err(
        "Failed to set attribute on Vector Set during AOF replay, this should never happen but will cause data loss if it does"
          .to_string()
          .into(),
      )
    }
  }
}
