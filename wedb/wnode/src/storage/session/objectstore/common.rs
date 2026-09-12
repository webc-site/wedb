//! 对象存公共操作与信封编码（对标 libs/server/Storage/Session/ObjectStore/Common.cs，C# 为 StorageSession partial）
//!
//! 存储表示：C# 侧对象存为独立 Tsavorite 实例 + IGarnetObject 堆对象；Rust 侧
//! wkv 单库模型下，对象以"1 字节类型标签 + bitcode 载荷"信封存于
//! 同一主存的普通键槽，类型标签对标 libs/server/Objects/Types/GarnetObjectType.cs
//! （SortedSet=1 / List=2 / Hash=3 / Set=4），实现 WRONTYPE 判定与持久化语义对齐。

use std::{cell::Cell, io::Cursor};

use wbase::glob::glob_match;
use wdev::Device;
use wval::GarnetObjectType;

use super::super::storage_session::StorageSession;
use crate::{
  objects::{
    hash::hash_object::HashObject, list::list_object::ListObject, set::set_object::SetObject,
    sortedset::sorted_set_object::SortedSetObject,
  },
  types::GarnetStatus,
};

/// 对象存载荷通用行为契约（对标 Garnet IGarnetObject 与 ObjectStoreRMW 抽象）
pub(crate) trait GarnetObjectPayload: Sized + Default {
  /// 权威对象类型标识（对标 GarnetObjectType）
  const OBJECT_TAG: u8;

  /// 从载荷切片反序列化对象
  fn deserialize_from_slice(slice: &[u8]) -> Self;

  /// 序列化对象为字节向量
  fn serialize_to_vec(&self) -> Option<Vec<u8>>;

  /// 检查集合对象是否为空
  fn is_empty(&self) -> bool;
}

impl GarnetObjectPayload for HashObject {
  const OBJECT_TAG: u8 = GarnetObjectType::Hash as u8;

  #[inline]
  fn deserialize_from_slice(slice: &[u8]) -> Self {
    Self::deserialize(&mut Cursor::new(slice)).unwrap_or_default()
  }

  #[inline]
  fn serialize_to_vec(&self) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    self.serialize(&mut out).ok()?;
    Some(out)
  }

  #[inline]
  fn is_empty(&self) -> bool {
    self.hash.is_empty()
  }
}

impl GarnetObjectPayload for SetObject {
  const OBJECT_TAG: u8 = GarnetObjectType::Set as u8;

  #[inline]
  fn deserialize_from_slice(slice: &[u8]) -> Self {
    Self::deserialize(&mut Cursor::new(slice)).unwrap_or_default()
  }

  #[inline]
  fn serialize_to_vec(&self) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    self.serialize(&mut out).ok()?;
    Some(out)
  }

  #[inline]
  fn is_empty(&self) -> bool {
    self.set.is_empty()
  }
}

impl GarnetObjectPayload for SortedSetObject {
  const OBJECT_TAG: u8 = GarnetObjectType::SortedSet as u8;

  #[inline]
  fn deserialize_from_slice(slice: &[u8]) -> Self {
    Self::deserialize(&mut Cursor::new(slice)).unwrap_or_default()
  }

  #[inline]
  fn serialize_to_vec(&self) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    self.serialize(&mut out).ok()?;
    Some(out)
  }

  #[inline]
  fn is_empty(&self) -> bool {
    self.sorted_set_dict.is_empty()
  }
}

impl GarnetObjectPayload for ListObject {
  const OBJECT_TAG: u8 = GarnetObjectType::List as u8;

  #[inline]
  fn deserialize_from_slice(slice: &[u8]) -> Self {
    Self::deserialize(&mut Cursor::new(slice)).unwrap_or_default()
  }

  #[inline]
  fn serialize_to_vec(&self) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    self.serialize(&mut out).ok()?;
    Some(out)
  }

  #[inline]
  fn is_empty(&self) -> bool {
    self.list.is_empty()
  }
}

/// 对象键读取状态（三态：缺失 / 类型不符 / 命中载荷）
pub(crate) enum ObjState {
  /// 键不存在
  Absent,
  /// 存在但类型不符
  WrongType,
  /// 命中并返回剥壳载荷
  Present(Vec<u8>),
}

/// 对象键只读探测结果（三态：未找到 / 类型不符 / 命中）
#[derive(Debug)]
pub(crate) enum ObjReadResult<R> {
  NotFound,
  WrongType,
  Ok(R),
}

/// 对象值信封编码：[类型标签][bitcode 载荷]
pub fn obj_encode(tag: u8, payload: &[u8]) -> Vec<u8> {
  let mut out = Vec::with_capacity(payload.len() + 1);
  out.push(tag);
  out.extend_from_slice(payload);
  out
}

/// 对象存 RMW 结果三态（一处定义，供 hash/set/list/zset 各 rmw 通路统一判定）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RmwOutcome<R> {
  /// 闭包完成并已写回存储
  Written(R),
  /// 放弃写入（键缺失不物化 / 闭包拒绝 / 序列化失败），无存储变更
  Aborted,
  /// 键存在但信封类型不符（WRONGTYPE）
  WrongType,
}

impl<R> RmwOutcome<R> {
  /// 写回结果提取（Aborted 返回 `fallback`；WrongType 同样折算，调用方
  /// 在状态已单独分派的前提下使用）
  #[inline]
  pub(crate) fn unwrap_or(self, fallback: R) -> R {
    match self {
      Self::Written(r) => r,
      Self::Aborted | Self::WrongType => fallback,
    }
  }
}

/// 对象值信封解码：校验类型标签后返回载荷切片
pub fn obj_decode(raw: &[u8], want: u8) -> Option<&[u8]> {
  raw
    .split_first()
    .filter(|(t, _)| **t == want)
    .map(|(_, p)| p)
}

impl<'a, D: Device, CR: wkv::ConsistentReadFunctions> StorageSession<'a, D, CR> {
  /// 零拷贝读取对象键并校验类型：闭包直接在载荷切片上执行，避免整个 payload 堆拷贝
  pub(crate) async fn obj_load_with<R>(
    &self,
    key: &[u8],
    tag: u8,
    f: impl Fn(&[u8]) -> R,
  ) -> wkv::Result<ObjReadResult<R>> {
    match self
      .read_string_with(key, |raw| match obj_decode(raw, tag) {
        None => ObjReadResult::WrongType,
        Some(p) => ObjReadResult::Ok(f(p)),
      })
      .await?
    {
      None => Ok(ObjReadResult::NotFound),
      Some(res) => Ok(res),
    }
  }

  /// 读取对象键并校验类型
  pub(crate) async fn obj_load(&self, key: &[u8], tag: u8) -> wkv::Result<ObjState> {
    match self.obj_load_with(key, tag, |p| p.to_vec()).await? {
      ObjReadResult::NotFound => Ok(ObjState::Absent),
      ObjReadResult::WrongType => Ok(ObjState::WrongType),
      ObjReadResult::Ok(p) => Ok(ObjState::Present(p)),
    }
  }

  /// 写入对象键（覆盖既有信封）
  pub async fn obj_save(&self, key: &[u8], tag: u8, payload: &[u8]) -> wkv::Result<()> {
    self.upsert_string(key, &obj_encode(tag, payload)).await
  }

  /// 对象存读-改-写统一入口：读现载荷（缺失为 None），闭包产出 (新载荷, 结果)，
  /// 返回 [`RmwOutcome`] 三态（写回 / 放弃 / 类型不符）
  ///
  /// 单次读取闭环：类型判定与载荷解码共用一次读，调用方无需预先
  /// obj_load 探型（消除各 ops 的双读模式）。
  ///
  /// libs/server/Storage/Session/ObjectStore/Common.cs:RMWObjectStoreOperation
  pub(crate) async fn rmw_object_store_operation<R>(
    &self,
    key: &[u8],
    tag: u8,
    on_load: impl FnOnce(Option<&[u8]>) -> Option<(Vec<u8>, R)>,
  ) -> wkv::Result<RmwOutcome<R>> {
    let on_load = Cell::new(Some(on_load));
    let outcome = match self
      .read_string_with(key, |raw| match obj_decode(raw, tag) {
        None => RmwOutcome::WrongType,
        Some(p) => match on_load.take() {
          Some(f) => match f(Some(p)) {
            Some((payload, r)) => RmwOutcome::Written((payload, r)),
            None => RmwOutcome::Aborted,
          },
          None => RmwOutcome::Aborted,
        },
      })
      .await?
    {
      None => match on_load.take() {
        Some(f) => match f(None) {
          Some((payload, r)) => RmwOutcome::Written((payload, r)),
          None => RmwOutcome::Aborted,
        },
        None => RmwOutcome::Aborted,
      },
      Some(outcome) => outcome,
    };

    match outcome {
      RmwOutcome::Written((payload, r)) => {
        self.obj_save(key, tag, &payload).await?;
        Ok(RmwOutcome::Written(r))
      }
      RmwOutcome::Aborted => Ok(RmwOutcome::Aborted),
      RmwOutcome::WrongType => Ok(RmwOutcome::WrongType),
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

  /// 对象存类型化 RMW 统一入口（对标 Garnet ObjectStoreRMW / InPlaceUpdaterWorker）
  ///
  /// `create` 为真时键缺失自动物化空对象；为假时键缺失直接返回 Aborted。
  /// 闭包返回 None 时表示拒绝写回（Aborted），无存储变更。
  pub(crate) async fn typed_rmw<O: GarnetObjectPayload, R>(
    &self,
    key: &[u8],
    create: bool,
    f: impl FnOnce(&mut O) -> Option<R>,
  ) -> wkv::Result<RmwOutcome<R>> {
    self
      .rmw_object_store_operation(key, O::OBJECT_TAG, |payload| {
        let mut obj = match payload {
          Some(bytes) => O::deserialize_from_slice(bytes),
          None if create => O::default(),
          None => return None,
        };
        let r = f(&mut obj)?;
        let out = obj.serialize_to_vec()?;
        Some((out, r))
      })
      .await
  }

  /// 对象存类型化只读装载（读路径专用，返回 NotFound / WrongType / Ok(O) 三态）
  pub(crate) async fn typed_load<O: GarnetObjectPayload>(
    &self,
    key: &[u8],
  ) -> wkv::Result<Result<Option<O>, GarnetStatus>> {
    Ok(
      match self
        .obj_load_with(key, O::OBJECT_TAG, O::deserialize_from_slice)
        .await?
      {
        ObjReadResult::NotFound => Ok(None),
        ObjReadResult::WrongType => Err(GarnetStatus::WrongType),
        ObjReadResult::Ok(obj) => Ok(Some(obj)),
      },
    )
  }

  /// 对象存类型化只读探测（闭包直接在借用的反序列化对象上执行）
  pub(crate) async fn typed_read_with<O: GarnetObjectPayload, R>(
    &self,
    key: &[u8],
    f: impl Fn(&O) -> R,
  ) -> wkv::Result<Result<Option<R>, GarnetStatus>> {
    Ok(
      match self
        .obj_load_with(key, O::OBJECT_TAG, |p| f(&O::deserialize_from_slice(p)))
        .await?
      {
        ObjReadResult::NotFound => Ok(None),
        ObjReadResult::WrongType => Err(GarnetStatus::WrongType),
        ObjReadResult::Ok(r) => Ok(Some(r)),
      },
    )
  }

  /// 移除族通用 RMW 操作（自动核查 is_empty 并触发删空自愈）
  ///
  /// 自动对齐 Garnet C# NeedToCreate=false 语义：
  /// - 键缺失 → NOTFOUND
  /// - 键类型不匹配 → WRONGTYPE
  /// - 成功移除后若集合被删空，原子触发 finalize_removal（清理整键、墓碑与随键 TTL）
  ///
  /// 对象存类型化只读查询统一内核（收敛 Err / Ok(None) / Ok(Some) 样板）
  pub(crate) async fn typed_query<O: GarnetObjectPayload, R>(
    &self,
    key: &[u8],
    fallback: R,
    f: impl Fn(&O) -> R,
  ) -> wkv::Result<(GarnetStatus, R)> {
    match self.typed_read_with::<O, R>(key, f).await? {
      Err(s) => Ok((s, fallback)),
      Ok(None) => Ok((GarnetStatus::NotFound, fallback)),
      Ok(Some(v)) => Ok((GarnetStatus::Ok, v)),
    }
  }

  /// 对象存类型化删除/弹出统一入口（删空时整键回收）
  ///
  /// 缺键时默认返回 `NotFound`（对标 Garnet NeedToCreate=false 语义）。
  pub(crate) async fn typed_remove<O: GarnetObjectPayload, R>(
    &self,
    key: &[u8],
    fallback: R,
    f: impl FnOnce(&mut O) -> Option<R>,
  ) -> wkv::Result<(GarnetStatus, R)> {
    self
      .typed_remove_with_status::<O, R>(key, GarnetStatus::NotFound, fallback, f)
      .await
  }

  /// 对象存类型化删除/弹出统一入口（支持指定缺键状态码，对标 Garnet NeedToCreate=true/false 差异）
  pub(crate) async fn typed_remove_with_status<O: GarnetObjectPayload, R>(
    &self,
    key: &[u8],
    missing_status: GarnetStatus,
    fallback: R,
    f: impl FnOnce(&mut O) -> Option<R>,
  ) -> wkv::Result<(GarnetStatus, R)> {
    let mut deleted = false;
    let mut deleted_result = None;
    let outcome = self
      .typed_rmw::<O, R>(key, false, |obj| {
        let res = f(obj)?;
        if obj.is_empty() {
          deleted = true;
          deleted_result = Some(res);
          None // 拒绝写回空载荷，避免落盘空对象与无效 I/O
        } else {
          Some(res)
        }
      })
      .await?;
    match outcome {
      RmwOutcome::WrongType => Ok((GarnetStatus::WrongType, fallback)),
      RmwOutcome::Aborted if deleted => {
        let _ = self.delete_string(key).await?;
        Ok((GarnetStatus::Ok, deleted_result.unwrap_or(fallback)))
      }
      RmwOutcome::Aborted => Ok((missing_status, fallback)),
      RmwOutcome::Written(r) => Ok((GarnetStatus::Ok, r)),
    }
  }

  /// 对象键 SCAN（SCAN 语义：游标 = 上次返回的最后一个成员）
  ///
  /// 缺键返回 NOTFOUND、错误类型键传播 WRONGTYPE（对齐 C# ReadObjectStoreOperation
  /// 的三态；RESP 层对 NOTFOUND 同答 ["0", 空数组]），`members_of` 由各类型
  /// 操作面提供（只需返回全部成员，内部统一排序）
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
    let payload = match self.obj_load(key, tag).await? {
      ObjState::Absent => return Ok((GarnetStatus::NotFound, Vec::new(), Vec::new())),
      ObjState::WrongType => return Ok((GarnetStatus::WrongType, Vec::new(), Vec::new())),
      ObjState::Present(p) => p,
    };
    let Some(mut members) = members_of(&payload) else {
      return Ok((GarnetStatus::WrongType, Vec::new(), Vec::new()));
    };
    members.sort();
    // count 下限钳制为 1：count=0 时若仍按"满页截断"处理会立即空游标终止，
    // SCAN 第一页未产出任何成员即死（Redis 侧 COUNT<1 在 RESP 层拒绝）
    let count = count.max(1);
    let mut items = Vec::new();
    let mut last: Option<Vec<u8>> = None;
    // 本页是否因 count 截断：仅截断时报告新游标，自然收尽返回空游标（终态）
    let mut truncated = false;
    for m in members {
      // 成员已按字节序升序：跳过游标及之前的全部成员（游标成员可能在两页
      // 之间被删，按 <= 比较可从其后的首个成员无缝续扫）
      if !cursor.is_empty() && m.as_slice() <= cursor {
        continue;
      }
      if items.len() >= count {
        truncated = true;
        break;
      }
      if pattern.is_empty() || glob_match(pattern, &m) {
        items.push(m.clone());
      }
      last = Some(m);
    }
    let next = if truncated {
      last.unwrap_or_default()
    } else {
      Vec::new()
    };
    Ok((GarnetStatus::Ok, next, items))
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
    let mut entries: Vec<(&[u8], &[u8])> = map
      .iter()
      .filter_map(|(k, v)| v.as_deref().map(|val| (k.as_slice(), val)))
      .collect();
    entries.sort_unstable_by_key(|(k, _)| *k);
    for (key, v) in entries {
      if let Some(&tag) = v.first()
        && (GarnetObjectType::SortedSet as u8..=GarnetObjectType::Set as u8).contains(&tag)
      {
        n += 1;
        if !on_object(tag, key) {
          break;
        }
      }
    }
    Ok(n)
  }
}
