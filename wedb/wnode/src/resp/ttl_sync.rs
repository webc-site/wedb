//! key 级 TTL 记录的同步快路径读写（批处理纪元上下文专用）
//!
//! wkv 的 `expire_at`/`persist`/`pttl_ms` 全为异步入口（内含磁盘 I/O 与物理清除），
//! 而 RESP 命令层是同步函数、经 `Ok(false)` 向调用方发降级信号。本模块在
//! [`wkv::BatchStoreSession`] 纪元保护下，用 wkv 公开的 raw 同步内核
//! （`try_read_raw_in_memory`/`try_modify_raw_in_place_unprotected`/
//! `try_upsert_raw_sync`/`try_delete_raw_sync`）实现 TTL 记录的最深同步路径：
//! 可变区命中的读写删全部零 I/O 闭环；遇磁盘候选或环形页翻转时返回降级信号，
//! 由调用方按已实现命令的既有约定整体转异步路由。
//!
//! TTL 记录值编码直接复用 `wval::TtlCodec`：8 字节大端 i64 .NET Ticks
//! （100ns 单位，0001-01-01 纪元），与 C# Garnet RecordDataHeader 的 expiration
//! 同域；Unix 秒/毫秒 ↔ ticks 的 RESP 边界换算见 `wbase::convert`
//! （garnet/libs/common/ConvertUtils.cs 镜像），本模块零手抄镜像。

use wbase::time::now_ticks;
use wdev::Device;
use wkv::{BatchStoreSession, Result, TTL_VALUE_LEN};
use wval::TtlCodec;

/// 读 key 的 TTL 记录（同步）
///
/// 三态对齐 `try_read_sync` 约定：
/// - `Ok(Some(Some(exp)))`：内存命中，绝对过期 .NET Ticks；
/// - `Ok(Some(None))`：无 TTL 记录（哈希探针未命中即零 I/O 判定；墓碑与
///   非法值长度视同无 TTL）；
/// - `Ok(None)`：TTL 记录有磁盘候选，须降级异步裁决。
pub fn ttl_of_sync<D: wdev::Device>(
  session: &BatchStoreSession<'_, D>,
  key: &[u8],
) -> Result<Option<Option<i64>>> {
  let ttl_k = session.ttl_key(key);
  match session.try_read_raw_in_memory(&ttl_k, TtlCodec::decode)? {
    // 磁盘候选：降级异步裁决
    None => Ok(None),
    // 内存闭环：墓碑视同无 TTL，非法值长度按无 TTL 容错
    Some(v) => Ok(Some(v.flatten())),
  }
}

/// 写 key 的 TTL 记录（同步）：可变区原位改写优先，失败降级 RCU 盲插
///
/// 返回 `Ok(true)` 已闭环；`Ok(false)` 遭遇环形页翻转须降级异步
/// （镜像 `wkv::StoreSession::put_ttl` 的取舍，见其注释；长度守卫比 wkv
/// 原版更严——记录值非定长 8B 时放弃原位改写转 RCU，杜绝
/// `copy_from_slice` 长度失配 panic）
pub fn put_ttl_sync<D: wdev::Device>(
  session: &BatchStoreSession<'_, D>,
  key: &[u8],
  expire_at_ticks: i64,
) -> Result<bool> {
  let bytes = TtlCodec::encode(expire_at_ticks);
  let ttl_k = session.ttl_key(key);
  let in_place = session
    .try_modify_raw_in_place_unprotected(&ttl_k, |slot| {
      if slot.len() == TTL_VALUE_LEN {
        slot.copy_from_slice(&bytes);
        Some(())
      } else {
        None
      }
    })?
    .is_some();
  if in_place {
    return Ok(true);
  }
  Ok(session.try_upsert_raw_sync(&ttl_k, &bytes)?.is_ok())
}

/// 删 key 的 TTL 记录（同步）
///
/// 返回 `Ok(true)` 已闭环（含本就无记录：删除内核纯查找探针，未命中零写入
/// 亦不建槽）；`Ok(false)` 须降级异步
pub fn del_ttl_sync<D: wdev::Device>(
  session: &BatchStoreSession<'_, D>,
  key: &[u8],
) -> Result<bool> {
  let ttl_k = session.ttl_key(key);
  Ok(session.try_delete_raw_sync(&ttl_k)?.is_ok())
}

/// 裸数据存活探针（镜像 `contains_key_ignore_ttl`：剥离 TTL 门控，仅判数据记录）
///
/// 三态：`Ok(Some(true))` 数据在内存存活；`Ok(Some(false))` 内存确认不存在；
/// `Ok(None)` 数据有磁盘候选须降级。
pub fn data_alive_sync<D: wdev::Device>(
  session: &BatchStoreSession<'_, D>,
  key: &[u8],
) -> Result<Option<bool>> {
  Ok(
    session
      .try_read_in_memory_unprotected(key, |_| ())?
      .map(|found| found.is_some()),
  )
}

/// 带 TTL 裁决的同步数据读（键值读不因 TTL 标签墓碑而降级）
///
/// 三态对齐 [`BatchStoreSession::try_read_sync`]，但门控更精细：数据在内存且
/// TTL 记录可内存裁决（无记录 / 有记录未过期 / 记录为墓碑）时直接闭环；仅
/// 数据有磁盘候选、TTL 值有磁盘候选、或键已过期须物理清除时才降级异步
pub fn read_adjudicated_sync<R, D: Device>(
  session: &BatchStoreSession<'_, D>,
  key: &[u8],
  f: impl FnOnce(&[u8]) -> R,
) -> Result<Option<Option<R>>> {
  let raw = session.try_read_in_memory_unprotected(key, f)?;
  if raw.as_ref().is_some_and(|v| v.is_some()) {
    match ttl_of_sync(session, key)? {
      None => return Ok(None),
      // 到期判定严格小于（读路径口径，对标 LogRecordUtils.cs:20）：
      // exp == now 未过期，同步闭环免异步绕路
      Some(Some(exp)) if exp < now_ticks() => return Ok(None),
      _ => {}
    }
  }
  Ok(raw)
}

/// 键存活探针（数据存活 + TTL 未过期才视为存活）
///
/// 返回 `Ok(None)` 须降级：数据有磁盘候选，或 TTL 记录需磁盘裁决，或键已
/// 过期须异步物理清除（C# 由存储层原子完成过期判定）。条件写（SET NX/XX）、
/// RESTORE NX、EXPIRE 族等依赖"键在否"判定的命令共用
pub fn probe_alive<D: Device>(
  session: &BatchStoreSession<'_, D>,
  key: &[u8],
) -> Result<Option<bool>> {
  let alive = match data_alive_sync(session, key)? {
    None => return Ok(None),
    Some(alive) => alive,
  };
  if !alive {
    return Ok(Some(false));
  }
  match ttl_of_sync(session, key)? {
    None => Ok(None),
    // 到期判定严格小于（读路径口径，对标 LogRecordUtils.cs:20）；
    // exp == now 视为存活，已过期键降级异步物理清除
    Some(Some(exp)) if exp < now_ticks() => Ok(None),
    Some(_) => Ok(Some(true)),
  }
}

#[cfg(test)]
mod tests {
  use super::TtlCodec;

  /// TTL 记录编解码与落库值同域（.NET Ticks）：TtlCodec 往返
  #[test]
  fn ttl_codec_roundtrip() {
    let ticks = 638_600_000_000_000_000_i64;
    let bytes = TtlCodec::encode(ticks);
    assert_eq!(TtlCodec::decode(&bytes), Some(ticks));
    // 非法长度按无 TTL 容错
    assert_eq!(TtlCodec::decode(&[]), None);
    assert_eq!(TtlCodec::decode(&[0, 0, 0]), None);
  }
}
