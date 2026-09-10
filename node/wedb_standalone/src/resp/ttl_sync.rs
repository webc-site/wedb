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
//! TTL 记录值编码镜像 `wval::TtlCodec`：8 字节大端 u64 绝对毫秒时间戳。

use wdev::Device;
use wkv::{BatchStoreSession, Result};

/// TTL 记录值定长字节数（镜像 wkv::TTL_VALUE_LEN / wval::TTL_VAL_LEN）
const TTL_VAL_LEN: usize = 8;

/// 当前 Unix 毫秒时间戳（TTL 域时间基准）
#[inline]
pub fn now_unix_ms() -> u64 {
  coarsetime::Clock::now_since_epoch().as_millis()
}

/// 解码 TTL 记录值（镜像 wkv::ttl::ttl_val：非法长度按无 TTL 容错）
#[inline]
const fn ttl_val_decode(v: &[u8]) -> Option<u64> {
  match v.split_first_chunk::<TTL_VAL_LEN>() {
    Some((&arr, _)) => Some(u64::from_be_bytes(arr)),
    None => None,
  }
}

/// 读 key 的 TTL 记录（同步）
///
/// 三态对齐 `try_read_sync` 约定：
/// - `Ok(Some(Some(exp)))`：内存命中，绝对过期毫秒时间戳；
/// - `Ok(Some(None))`：无 TTL 记录（标签哈希探针未命中即零 I/O 判定）；
/// - `Ok(None)`：TTL 记录有磁盘候选，须降级异步裁决。
pub fn ttl_of_sync<D: wdev::Device>(
  session: &BatchStoreSession<'_, D>,
  key: &[u8],
) -> Result<Option<Option<u64>>> {
  let ttl_k = session.ttl_key(key);
  if !has_ttl_record(session, &ttl_k)? {
    return Ok(Some(None));
  }
  match session.try_read_raw_in_memory(&ttl_k, ttl_val_decode)? {
    // 磁盘候选：降级异步裁决
    None => Ok(None),
    // 内存闭环：墓碑视同无 TTL，非法值长度按无 TTL 容错
    Some(v) => Ok(Some(v.flatten())),
  }
}

/// 写 key 的 TTL 记录（同步）：可变区原位改写优先，失败降级 RCU 盲插
///
/// 返回 `Ok(true)` 已闭环；`Ok(false)` 遭遇环形页翻转须降级异步
/// （镜像 `wkv::StoreSession::put_ttl` 的取舍，见其注释）
pub fn put_ttl_sync<D: wdev::Device>(
  session: &BatchStoreSession<'_, D>,
  key: &[u8],
  expire_at_ms: u64,
) -> Result<bool> {
  let bytes = expire_at_ms.to_be_bytes();
  let ttl_k = session.ttl_key(key);
  let in_place = session
    .try_modify_raw_in_place_unprotected(&ttl_k, |slot| {
      if slot.len() == TTL_VAL_LEN {
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

/// 删 key 的 TTL 记录（同步）：先单次哈希探针初筛，无记录零写入直接闭环
///
/// 返回 `Ok(true)` 已闭环（含本就无记录）；`Ok(false)` 须降级异步
pub fn del_ttl_sync<D: wdev::Device>(
  session: &BatchStoreSession<'_, D>,
  key: &[u8],
) -> Result<bool> {
  let ttl_k = session.ttl_key(key);
  if !has_ttl_record(session, &ttl_k)? {
    return Ok(true);
  }
  Ok(session.try_delete_raw_sync(&ttl_k)?.is_ok())
}

/// 物理键直探哈希索引（ 为 pub(crate)，持
/// 已编码 TTL 物理键时经此等价探针判定，零额外编码）
#[inline]
fn has_ttl_record<D: wdev::Device>(
  session: &BatchStoreSession<'_, D>,
  ttl_k: &[u8],
) -> Result<bool> {
  Ok(session.store.index.find_tag(ttl_k).is_some())
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
      Some(Some(exp)) if exp <= now_unix_ms() => return Ok(None),
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
    Some(Some(exp)) if exp <= now_unix_ms() => Ok(None),
    Some(_) => Ok(Some(true)),
  }
}

#[cfg(test)]
mod tests {
  use super::ttl_val_decode;

  #[test]
  fn ttl_val_decode_roundtrip() {
    let bytes = 1_700_000_000_123_u64.to_be_bytes();
    assert_eq!(ttl_val_decode(&bytes), Some(1_700_000_000_123));
    // 非法长度按无 TTL 容错
    assert_eq!(ttl_val_decode(&[]), None);
    assert_eq!(ttl_val_decode(&[0, 0, 0]), None);
  }
}
