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
  atomic::{AtomicI64, Ordering},
};

use wbase::{entry_type::AofEntryType, hash_slot::hash_slot as cluster_slot};
use wcol::RespInputFlags;
use wresp::RespCommand;
use wval::{KeyTag, NamespaceDbCodec};
use wvector::{VectorDistanceMetricType, VectorQuantType, VectorValueType, store::StoreCallbacks};

use super::{
  vector_manager::{
    VADD_APPEND_LOG_ARG, VREM_APPEND_LOG_ARG, VSETATTR_APPEND_LOG_ARG, VectorAddArgs,
    VectorManager, VectorManagerResult,
  },
  vector_manager_locking::CreateIndexParams,
};
use crate::aof::{
  aof_processor::{ReplayInput, ReplayInputSlice},
  garnet_append_only_file::GarnetAppendOnlyFile,
  garnet_log::RecordShape,
};

/// AOF 直推注入端口（服务器级装配期注入）。
///
/// `aof` 持 Weak 破解「AOF 门面（重放面）↔ VectorManager（生产面）」的
/// 强引用环；`version` 为存储版本源快照（条目 store_version，重放端
/// 检查点代际过滤用）。
pub struct VectorAofSink {
  aof: Weak<GarnetAppendOnlyFile>,
  version: Arc<AtomicI64>,
}

impl VectorAofSink {
  /// 装配注入端口。
  pub fn new(aof: &Arc<GarnetAppendOnlyFile>, version: Arc<AtomicI64>) -> Self {
    Self {
      aof: Arc::downgrade(aof),
      version,
    }
  }

  /// 合成 StoreRMW 条目入队（用户键 → 物理键编码；零分配切片编码）。
  fn enqueue(&self, cmd: RespCommand, log_arg: i64, key: &[u8], args: &[&[u8]]) {
    let Some(aof) = self.aof.upgrade() else {
      return;
    };
    let input = ReplayInputSlice::new(cmd, args)
      .with_flags(RespInputFlags::DETERMINISTIC.bits())
      .with_args_num(log_arg, 0, 0);
    ReplayInput::with_encoded_slices(&input, |serialized| {
      aof.log().enqueue(&RecordShape {
        op_type: AofEntryType::StoreRMW,
        version: self.version.load(Ordering::Acquire),
        session_id: 0,
        key: NamespaceDbCodec::encode_tagged_key(0, 0, KeyTag::String, key).as_slice(),
        value: &[],
        input: serialized,
        database_id: 0,
      });
    });
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

/// 条目参数缺失误文案。
fn corrupt(cmd: &str) -> String {
  format!("vector {cmd} replay input corrupted")
}

impl<S: StoreCallbacks> VectorManager<S> {
  /// libs/server/Resp/Vector/VectorManager.Replication.cs:ReplicateVectorSetAdd
  ///
  /// VADD 成功后的合成写注入（AOF: YES；参数布局对齐 C# VectorSetAdd 的
  /// parseState 10 参：dims/reduceDims/valueType/values/element/quantizer/
  /// buildExplorationFactor/attributes/numLinks/distanceMetric）。
  pub fn replicate_vector_set_add(
    &self,
    key: &[u8],
    dims: u32,
    build_exploration_factor: u32,
    args: &VectorAddArgs<'_>,
  ) {
    let Some(sink) = self.aof_sink.read().clone() else {
      return;
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
    sink.enqueue(RespCommand::Vadd, VADD_APPEND_LOG_ARG, key, &entry_args);
  }

  /// libs/server/Resp/Vector/VectorManager.Replication.cs:ReplicateVectorSetRemove
  ///
  /// VREM 成功后的合成写注入（AOF: YES；参数 = element）。
  pub fn replicate_vector_set_remove(&self, key: &[u8], element: &[u8]) {
    let Some(sink) = self.aof_sink.read().clone() else {
      return;
    };
    let args = [element];
    sink.enqueue(RespCommand::Vrem, VREM_APPEND_LOG_ARG, key, &args);
  }

  /// libs/server/Resp/Vector/VectorManager.Replication.cs:ReplicateVectorSetSetAttribute
  ///
  /// VSETATTR 成功后的合成写注入（AOF: YES；参数 = element + attribute）。
  pub fn replicate_vector_set_set_attribute(&self, key: &[u8], element: &[u8], attribute: &[u8]) {
    let Some(sink) = self.aof_sink.read().clone() else {
      return;
    };
    let args = [element, attribute];
    sink.enqueue(RespCommand::Vsetattr, VSETATTR_APPEND_LOG_ARG, key, &args);
  }

  /// libs/server/Resp/Vector/VectorManager.Replication.cs:HandleVectorSetAddReplication
  ///
  /// 重放应用 VADD：解析 10 参 → 读或建索引（对齐 C# ReadOrCreateVectorIndex
  /// 的重放侧形态，键缺失按复制参数补建）→ TryAdd 语义插入。
  /// 非 OK 结果对齐 C# 抛异常语义，返回错误中止重放。
  pub fn replay_vector_set_add(&self, key: &[u8], input: &ReplayInput) -> Result<(), String> {
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
      hash_slot: cluster_slot(key),
      dims,
      reduce_dims,
      quant: quantizer,
      build_exploration_factor: build_ef,
      num_links,
      distance_metric,
    };
    let (index, _lock) = self
      .read_or_create_vector_index(key, Some(&params))
      .map_err(|_| "Failed to read or create Vector Set index during AOF replay".to_string())?;

    // 覆盖语义：重放即重建。索引记录不随检查点持久化，重建分配的 context
    // 可能与磁盘残留元素数据同域（C# 索引记录入检查点、context 原位复用，
    // 无此形态）；先清同元素残留（含内存与 vector_key 磁盘记录）再插入，
    // 保证 AOF 全序重放幂等收敛，其余旧代残留交由后台清理回收。
    self.service.remove(index.context, element);

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
    match self.try_add(key, &index.to_bytes(), &add_args) {
      Ok(VectorManagerResult::OK) => Ok(()),
      Ok(other) => Err(format!(
        "Failed to add to Vector Set index during AOF replay, this should never happen but will cause data loss if it does: {other:?}"
      )),
      Err(e) => Err(format!(
        "Failed to add to Vector Set index during AOF replay: {}",
        String::from_utf8_lossy(&e.message)
      )),
    }
  }

  /// libs/server/Resp/Vector/VectorManager.Replication.cs:HandleVectorSetRemoveReplication
  ///
  /// 重放应用 VREM：读索引（重放条目主端必成功，键必在）→ TryRemove 语义。
  pub fn replay_vector_set_remove(&self, key: &[u8], input: &ReplayInput) -> Result<(), String> {
    let element = input.args.first().ok_or_else(|| corrupt("VREM"))?;
    let (index, _lock) = self.read_vector_index(key);
    let index =
      index.ok_or_else(|| "Failed to read Vector Set index during AOF replay".to_string())?;
    match self.try_remove(&index.to_bytes(), element) {
      VectorManagerResult::OK => Ok(()),
      other => Err(format!(
        "Failed to remove from Vector Set index during AOF replay: {other:?}"
      )),
    }
  }

  /// libs/server/Resp/Vector/VectorManager.Replication.cs:HandleVectorSetSetAttributeReplication
  ///
  /// 重放应用 VSETATTR：读索引 → TrySetAttribute 语义。
  pub fn replay_vector_set_set_attribute(
    &self,
    key: &[u8],
    input: &ReplayInput,
  ) -> Result<(), String> {
    let element = input.args.first().ok_or_else(|| corrupt("VSETATTR"))?;
    let attribute = input.args.get(1).ok_or_else(|| corrupt("VSETATTR"))?;
    let (index, _lock) = self.read_vector_index(key);
    let index =
      index.ok_or_else(|| "Failed to read Vector Set index during AOF replay".to_string())?;
    if self.try_set_attribute(&index.to_bytes(), element, attribute) {
      Ok(())
    } else {
      Err(
        "Failed to set attribute on Vector Set during AOF replay, this should never happen but will cause data loss if it does"
          .to_string(),
      )
    }
  }
}
