//! 对象存公共操作与信封编码（对标 libs/server/Storage/Session/ObjectStore/Common.cs，C# 为 StorageSession partial）
//!
//! 存储表示：C# 侧对象存为独立 Tsavorite 实例 + IGarnetObject 堆对象；Rust 侧
//! wkv 单库模型下，对象以"1 字节类型标签 + wobject bitcode 载荷"信封存于
//! 同一主存的普通键槽，类型标签对标 libs/server/Objects/Types/GarnetObjectType.cs
//! （SortedSet=1 / List=2 / Hash=3 / Set=4），实现 WRONTYPE 判定与持久化语义对齐。

use wdev::Device;

use super::super::storage_session::StorageSession;
use crate::api::garnet_status::GarnetStatus;

/// 对象类型标签：有序集合（GarnetObjectType.SortedSet）
pub(crate) const OBJ_TAG_SORTED_SET: u8 = 1;
/// 对象类型标签：列表（GarnetObjectType.List）
pub(crate) const OBJ_TAG_LIST: u8 = 2;
/// 对象类型标签：哈希（GarnetObjectType.Hash）
pub(crate) const OBJ_TAG_HASH: u8 = 3;
/// 对象类型标签：集合（GarnetObjectType.Set）
pub(crate) const OBJ_TAG_SET: u8 = 4;

/// 对象键读取状态（三态：缺失 / 类型不符 / 命中载荷）
pub(crate) enum ObjState {
  /// 键不存在
  Absent,
  /// 存在但类型不符
  WrongType,
  /// 命中并返回剥壳载荷
  Present(Vec<u8>),
}

impl ObjState {
  /// 提取载荷（缺失/类型不符返回 None）
  pub(crate) fn into_payload(self) -> Option<Vec<u8>> {
    match self {
      Self::Present(p) => Some(p),
      _ => None,
    }
  }
}

/// 对象值信封编码：[类型标签][wobject bitcode 载荷]
pub(crate) fn obj_encode(tag: u8, payload: &[u8]) -> Vec<u8> {
  let mut out = Vec::with_capacity(payload.len() + 1);
  out.push(tag);
  out.extend_from_slice(payload);
  out
}

/// 对象值信封解码：校验类型标签后返回载荷切片
pub(crate) fn obj_decode(raw: &[u8], want: u8) -> Option<&[u8]> {
  raw
    .split_first()
    .filter(|(t, _)| **t == want)
    .map(|(_, p)| p)
}

impl<'a, D: Device> StorageSession<'a, D> {
  /// 读取对象键并校验类型
  pub(crate) async fn obj_load(&self, key: &[u8], tag: u8) -> wkv::Result<ObjState> {
    match self.read_string(key).await? {
      None => Ok(ObjState::Absent),
      Some(raw) => Ok(match obj_decode(&raw, tag) {
        Some(p) => ObjState::Present(p.to_vec()),
        None => ObjState::WrongType,
      }),
    }
  }

  /// 写入对象键（覆盖既有信封）
  pub(crate) async fn obj_save(&self, key: &[u8], tag: u8, payload: &[u8]) -> wkv::Result<()> {
    self.upsert_string(key, &obj_encode(tag, payload)).await
  }

  /// 对象存读-改-写统一入口：读现载荷（缺失为 None），闭包产出 (新载荷, 结果)，
  /// 返回 `Ok(None)` 表示闭包放弃写入
  ///
  /// libs/server/Storage/Session/ObjectStore/Common.cs:RMWObjectStoreOperation
  pub(crate) async fn rmw_object_store_operation<R>(
    &self,
    key: &[u8],
    tag: u8,
    on_load: impl FnOnce(Option<Vec<u8>>) -> Option<(Vec<u8>, R)>,
  ) -> wkv::Result<Option<R>> {
    // 类型校验：存在但标签不符即 WRONGTYPE（闭包不感知）
    match self.read_string(key).await? {
      Some(raw) if obj_decode(&raw, tag).is_none() => Ok(None),
      current => {
        let input = current.and_then(|raw| obj_decode(&raw, tag).map(<[u8]>::to_vec));
        if let Some((payload, r)) = on_load(input) {
          self.obj_save(key, tag, &payload).await?;
          Ok(Some(r))
        } else {
          Ok(None)
        }
      }
    }
  }

  /// 对象存通用读入口：返回剥壳载荷
  ///
  /// libs/server/Storage/Session/ObjectStore/Common.cs:ReadObjectStoreOperation
  pub(crate) async fn read_object_store_operation(
    &self,
    key: &[u8],
    tag: u8,
  ) -> wkv::Result<(GarnetStatus, Option<Vec<u8>>)> {
    match self.obj_load(key, tag).await? {
      ObjState::Absent => Ok((GarnetStatus::NotFound, None)),
      ObjState::WrongType => Ok((GarnetStatus::WrongType, None)),
      ObjState::Present(p) => Ok((GarnetStatus::Ok, Some(p))),
    }
  }

  /// 对象键 SCAN（SCAN 语义：游标 = 上次返回的最后一个成员）
  ///
  /// `members_of` 由各类型操作面提供（按成员字节序排序后交付）
  ///
  /// libs/server/Storage/Session/ObjectStore/Common.cs:ObjectScan
  pub(crate) async fn object_scan(
    &self,
    key: &[u8],
    tag: u8,
    pattern: &[u8],
    cursor: &[u8],
    count: usize,
    members_of: impl Fn(&[u8]) -> Option<Vec<Vec<u8>>>,
  ) -> wkv::Result<(GarnetStatus, Vec<u8>, Vec<Vec<u8>>)> {
    let Some(payload) = self.obj_load(key, tag).await?.into_payload() else {
      return Ok((GarnetStatus::Ok, Vec::new(), Vec::new()));
    };
    let Some(mut members) = members_of(&payload) else {
      return Ok((GarnetStatus::WrongType, Vec::new(), Vec::new()));
    };
    members.sort();
    let mut items = Vec::new();
    let mut last: Option<Vec<u8>> = None;
    for m in members {
      if last.is_none() && !cursor.is_empty() && m == cursor {
        continue;
      }
      if items.len() >= count {
        break;
      }
      if pattern.is_empty()
        || super::super::common::array_key_iteration_functions::glob_match(pattern, &m)
      {
        items.push(m.clone());
      }
      last = Some(m);
    }
    Ok((GarnetStatus::Ok, last.unwrap_or_default(), items))
  }

  /// 删除对象存键（不区分类型，含信封整体删除）
  ///
  /// libs/server/Storage/Session/ObjectStore/Common.cs:DELETE_ObjectStore
  pub async fn delete_object_store(&self, key: &[u8]) -> wkv::Result<GarnetStatus> {
    if self.delete_string(key).await? {
      Ok(GarnetStatus::Ok)
    } else {
      Ok(GarnetStatus::NotFound)
    }
  }

  /// 对象存收集扫描（对象回收任务入口：遍历全部对象键并回调 (标签, 用户键)）
  ///
  /// 缺口说明：C# 侧 ObjectCollect 遍历对象存并逐对象做引用计数回收；
  /// wkv 对象生命周期由引擎 GC 统一管理，此处退化为对象键枚举统计。
  ///
  /// libs/server/Storage/Session/ObjectStore/Common.cs:ObjectCollect
  pub async fn object_collect(
    &self,
    mut on_object: impl FnMut(u8, &[u8]) -> bool,
  ) -> wkv::Result<usize> {
    let map = self.collect_records().await?;
    let mut n = 0usize;
    let mut keys: Vec<&Vec<u8>> = map.keys().collect();
    keys.sort();
    for key in keys {
      if let Some(Some(v)) = map.get(key)
        && let Some(&tag) = v.first()
        && (OBJ_TAG_SORTED_SET..=OBJ_TAG_SET).contains(&tag)
      {
        n += 1;
        if !on_object(tag, key) {
          break;
        }
      }
    }
    Ok(n)
  }

  /// 对象存未初始化异常检查（禁 panic 约束下的非抛出等价物）
  ///
  /// C# 侧对象存未挂载时抛 InvalidOperationException；wkv 单库模型对象存
  /// 与主存同体，恒已初始化，返回 true（等价"未抛异常"）。
  ///
  /// libs/server/Storage/Session/ObjectStore/Common.cs:ThrowObjectStoreUninitializedException
  pub fn throw_object_store_uninitialized_exception(&self) -> bool {
    true
  }

  /// 完成 pending 并返回统一 GarnetStatus
  ///
  /// wkv 读写调用同步闭环，恒无遗留 pending，直接返回传入的最终状态。
  ///
  /// libs/server/Storage/Session/ObjectStore/Common.cs:CompletePendingAndGetGarnetStatus
  pub fn complete_pending_and_get_garnet_status(&self, status: GarnetStatus) -> GarnetStatus {
    status
  }

  /// 物化批量 span 到自有缓冲（对标 scratch buffer 拷贝，Rust 侧为所有权转移）
  ///
  /// libs/server/Storage/Session/ObjectStore/Common.cs:CopyPinnedSpanByteArrayToScratchBuffer
  pub fn copy_pinned_span_byte_array_to_scratch_buffer(items: &[Vec<u8>]) -> Vec<Vec<u8>> {
    items.to_vec()
  }

  /// 物化键值对批量 span 到自有缓冲
  ///
  /// libs/server/Storage/Session/ObjectStore/Common.cs:CopyPinnedSpanBytePairsToScratchBuffer
  pub fn copy_pinned_span_byte_pairs_to_scratch_buffer(
    pairs: &[(Vec<u8>, Vec<u8>)],
  ) -> Vec<(Vec<u8>, Vec<u8>)> {
    pairs.to_vec()
  }

  /// 类型标签是否受支持的可迭代数组对象
  ///
  /// libs/server/Storage/Session/ObjectStore/Common.cs:IsSupportedArrayType
  pub fn is_supported_array_type(tag: u8) -> bool {
    (OBJ_TAG_SORTED_SET..=OBJ_TAG_SET).contains(&tag)
  }

  /// RESP 数组输出（通用扁平序列，RESP2/3 共用布局）
  ///
  /// libs/server/Storage/Session/ObjectStore/Common.cs:ProcessRespArrayOutput
  pub fn process_resp_array_output(&self, output: &mut Vec<u8>, items: &[Vec<u8>]) {
    push_resp_array(output, &items.iter().map(Vec::as_slice).collect::<Vec<_>>());
  }

  /// RESP2 数组输出（扁平：成员与值交错）
  ///
  /// libs/server/Storage/Session/ObjectStore/Common.cs:ProcessResp2ArrayOutput
  pub fn process_resp2_array_output(&self, output: &mut Vec<u8>, items: &[Vec<u8>]) {
    push_resp_array(output, &items.iter().map(Vec::as_slice).collect::<Vec<_>>());
  }

  /// RESP3 数组输出（同扁平布局；RESP3 映射类型由 RESP 层按命令包装）
  ///
  /// libs/server/Storage/Session/ObjectStore/Common.cs:ProcessResp3ArrayOutput
  pub fn process_resp3_array_output(&self, output: &mut Vec<u8>, items: &[Vec<u8>]) {
    self.process_resp2_array_output(output, items);
  }

  /// 整数数组 RESP 输出
  ///
  /// libs/server/Storage/Session/ObjectStore/Common.cs:ProcessRespIntegerArrayOutput
  pub fn process_resp_integer_array_output(&self, output: &mut Vec<u8>, items: &[i64]) {
    output.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
    for v in items {
      output.push(b':');
      output.extend_from_slice(itoa::Buffer::new().format(*v).as_bytes());
      output.extend_from_slice(b"\r\n");
    }
  }

  /// i64 数组 RESP 输出（同整数数组，语义分层保留）
  ///
  /// libs/server/Storage/Session/ObjectStore/Common.cs:ProcessRespInt64ArrayOutput
  pub fn process_resp_int64_array_output(&self, output: &mut Vec<u8>, items: &[i64]) {
    self.process_resp_integer_array_output(output, items);
  }

  /// 成员-分值对数组 RESP 输出（扁平交错）
  ///
  /// libs/server/Storage/Session/ObjectStore/Common.cs:ProcessRespArrayOutputAsPairs
  pub fn process_resp_array_output_as_pairs(&self, output: &mut Vec<u8>, pairs: &[(Vec<u8>, f64)]) {
    let mut flat = Vec::with_capacity(pairs.len() * 2);
    for (m, s) in pairs {
      flat.push(m.clone());
      flat.push(format_score(*s).into_bytes());
    }
    self.process_resp2_array_output(output, &flat);
  }

  /// 单 token RESP 输出
  ///
  /// libs/server/Storage/Session/ObjectStore/Common.cs:ProcessRespSingleTokenOutput
  pub fn process_resp_single_token_output(&self, output: &mut Vec<u8>, token: &[u8]) {
    output.push(b'$');
    output.extend_from_slice(itoa::Buffer::new().format(token.len()).as_bytes());
    output.extend_from_slice(b"\r\n");
    output.extend_from_slice(token);
    output.extend_from_slice(b"\r\n");
  }

  /// 尝试输出 i64 简单整数，越界返回 false（调用方回退批量字符串）
  ///
  /// libs/server/Storage/Session/ObjectStore/Common.cs:TryProcessRespSimple64IntOutput
  pub fn try_process_resp_simple64_int_output(&self, output: &mut Vec<u8>, value: i64) -> bool {
    output.push(b':');
    output.extend_from_slice(itoa::Buffer::new().format(value).as_bytes());
    output.extend_from_slice(b"\r\n");
    true
  }
}

/// 追加 RESP 数组（批量字符串扁平序列）
pub(crate) fn push_resp_array(output: &mut Vec<u8>, items: &[&[u8]]) {
  output.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
  for item in items {
    output.push(b'$');
    output.extend_from_slice(itoa::Buffer::new().format(item.len()).as_bytes());
    output.extend_from_slice(b"\r\n");
    output.extend_from_slice(item);
    output.extend_from_slice(b"\r\n");
  }
}

/// 分值 RESP 文本化（整数分值省略小数点，对齐 Redis 输出口径）
pub(crate) fn format_score(score: f64) -> String {
  if score == score.trunc() && score.abs() < 1e17 {
    format!("{}", score as i64)
  } else {
    format!("{score:.17}")
  }
}
