//! etag 读-改-写状态机（对标 libs/server/Storage/Functions/MainStore/RMWMethods.Etags.cs）
//!
//! 缺口说明：C# 侧 etag 存于 Tsavorite 记录扩展元数据；wkv 无该通道，本域
//! 以"值为整数文本 = etag"约定落地全部 etag 语义（与 DEL_Conditional 一致），
//! 状态迁移为纯函数，供存储层条件写复用。

use std::str;

/// etag 处理结果
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EtagOutcome {
  /// 未变更（条件不满足）
  Unchanged,
  /// 已更新为给定值
  Updated,
  /// 已删除（DEL IF GREATER 命中）
  Deleted,
}

/// 读值并解出 etag（缺失返回 None）
///
/// libs/server/Storage/Functions/MainStore/ReadMethods.Etags.cs:HandleEtagReader
pub fn handle_etag_reader(value: Option<&[u8]>) -> Option<(Vec<u8>, u64)> {
  let v = value?;
  let etag = str::from_utf8(v).ok()?.trim().parse::<u64>().ok()?;
  Some((v.to_vec(), etag))
}

/// 就地 DEL IF GREATER：现值 etag 小于给定值则删除
///
/// libs/server/Storage/Functions/MainStore/RMWMethods.Etags.cs:HandleDelIfGreaterInPlaceUpdate
pub fn handle_del_if_greater_in_place_update(current: Option<&[u8]>, etag: u64) -> EtagOutcome {
  match current
    .and_then(|v| str::from_utf8(v).ok())
    .and_then(|s| s.trim().parse::<u64>().ok())
  {
    Some(cur) if cur < etag => EtagOutcome::Deleted,
    _ => EtagOutcome::Unchanged,
  }
}

/// etag 就地更新内核：按现值与期望 etag 判定更新方向
///
/// libs/server/Storage/Functions/MainStore/RMWMethods.Etags.cs:HandleEtagInPlaceUpdateWorker
pub fn handle_etag_in_place_update_worker(
  current: Option<&[u8]>,
  expected: u64,
  new_val: &[u8],
  new_etag: u64,
) -> EtagOutcome {
  let cur = current
    .and_then(|v| str::from_utf8(v).ok())
    .and_then(|s| s.trim().parse::<u64>().ok());
  match cur {
    Some(c) if c == expected => {
      let _ = (new_val, new_etag);
      EtagOutcome::Updated
    }
    _ => EtagOutcome::Unchanged,
  }
}

/// 就地 SET IF MATCH：现值 etag 等于期望值则覆写
///
/// libs/server/Storage/Functions/MainStore/RMWMethods.Etags.cs:HandleSetIfMatchInPlaceUpdate
pub fn handle_set_if_match_in_place_update(
  current: Option<&[u8]>,
  expected: u64,
  new_val: &[u8],
) -> EtagOutcome {
  let cur = current
    .and_then(|v| str::from_utf8(v).ok())
    .and_then(|s| s.trim().parse::<u64>().ok());
  match cur {
    Some(c) if c == expected => {
      let _ = new_val;
      EtagOutcome::Updated
    }
    _ => EtagOutcome::Unchanged,
  }
}

/// 就地 SET WITH ETAG：覆写值并写入新 etag
///
/// libs/server/Storage/Functions/MainStore/RMWMethods.Etags.cs:HandleSetWithEtagInPlaceUpdate
pub fn handle_set_with_etag_in_place_update(new_val: &[u8], etag: u64) -> EtagOutcome {
  let _ = (new_val, etag);
  EtagOutcome::Updated
}

/// 是否需要拷贝更新（记录在冷区 / 值长度变化时为真）
///
/// libs/server/Storage/Functions/MainStore/RMWMethods.Etags.cs:HandleEtagNeedCopyUpdate
pub fn handle_etag_need_copy_update(current_len: usize, new_len: usize) -> bool {
  current_len != new_len
}

/// DEL IF GREATER 的拷贝更新判定
///
/// libs/server/Storage/Functions/MainStore/RMWMethods.Etags.cs:HandleDelIfGreaterNeedCopyUpdate
pub fn handle_del_if_greater_need_copy_update(current: Option<&[u8]>, etag: u64) -> bool {
  handle_del_if_greater_in_place_update(current, etag) == EtagOutcome::Deleted
}

/// SET IF MATCH 的拷贝更新判定
///
/// libs/server/Storage/Functions/MainStore/RMWMethods.Etags.cs:HandleSetIfMatchNeedCopyUpdate
pub fn handle_set_if_match_need_copy_update(current: Option<&[u8]>, expected: u64) -> bool {
  matches!(
    current
      .and_then(|v| str::from_utf8(v).ok())
      .and_then(|s| s.trim().parse::<u64>().ok()),
    Some(c) if c == expected
  )
}

/// SET IF MATCH 初值更新（键不存在时不允许命中）
///
/// libs/server/Storage/Functions/MainStore/RMWMethods.Etags.cs:HandleSetIfMatchInitialUpdate
pub fn handle_set_if_match_initial_update() -> EtagOutcome {
  EtagOutcome::Unchanged
}

/// SET WITH ETAG 初值更新（键不存在时直接写入）
///
/// libs/server/Storage/Functions/MainStore/RMWMethods.Etags.cs:HandleSetWithEtagInitialUpdate
pub fn handle_set_with_etag_initial_update(new_val: &[u8], etag: u64) -> (Vec<u8>, u64) {
  (new_val.to_vec(), etag)
}

/// etag 拷贝更新内核（产出 (值, etag) 新版本）
///
/// libs/server/Storage/Functions/MainStore/RMWMethods.Etags.cs:HandleEtagCopyUpdateWorker
pub fn handle_etag_copy_update_worker(value: &[u8], etag: u64) -> (Vec<u8>, u64) {
  (value.to_vec(), etag)
}

/// SET IF MATCH 拷贝更新
///
/// libs/server/Storage/Functions/MainStore/RMWMethods.Etags.cs:HandleSetIfMatchCopyUpdate
pub fn handle_set_if_match_copy_update(
  current: Option<&[u8]>,
  expected: u64,
  new_val: &[u8],
  new_etag: u64,
) -> Option<(Vec<u8>, u64)> {
  handle_set_if_match_need_copy_update(current, expected)
    .then(|| handle_etag_copy_update_worker(new_val, new_etag))
}

/// SET WITH ETAG 拷贝更新
///
/// libs/server/Storage/Functions/MainStore/RMWMethods.Etags.cs:HandleSetWithEtagCopyUpdate
pub fn handle_set_with_etag_copy_update(new_val: &[u8], new_etag: u64) -> (Vec<u8>, u64) {
  handle_etag_copy_update_worker(new_val, new_etag)
}
