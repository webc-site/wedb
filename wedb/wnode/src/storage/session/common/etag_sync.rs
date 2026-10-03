//! key 级 ETag 记录的同步快路径读写（批处理纪元上下文专用）
//!
//! 镜像 [`super::ttl_sync`] 的取舍：RESP 命令层是同步函数，wkv 的
//! `etag_of`/`put_etag` 为异步入口（内含磁盘 I/O），本模块在
//! [`wkv::BatchStoreSession`] 纪元保护下用 wkv 公开的 raw 同步内核实现
//! ETag 旁路记录的最深同步路径：可变区命中的读写删全部零 I/O 闭环；
//! 遇磁盘候选或环形页翻转时返回降级信号，由调用方按既有约定整体转
//! 异步路由。
//!
//! ETag 记录值编码用 `wval::I64Codec`：8 字节大端 i64（对标 C#
//! Tsavorite LogRecord.cs:ETagSize 记录尾可选字段），缺省 NoETag = 0。

use wdev::Device;
use wkv::{BatchStoreSession, Result, StoreResult};
use wval::{I64_VAL_LEN, I64Codec, NO_ETAG};

/// 读 key 的 ETag（同步）
///
/// 三态对齐 `ttl_sync::ttl_of_sync`：
/// - `Ok(StoreResult::Success(Some(etag)))`：内存命中；
/// - `Ok(StoreResult::Success(NO_ETAG))` / `NotFound`：无 ETag 记录（哈希探针
///   未命中即零 I/O 判定；墓碑与非法值长度视同无 etag，即 0）；
/// - `Ok(StoreResult::RecordOnDisk)`：ETag 记录有磁盘候选，须降级异步裁决。
pub fn etag_of_sync<D: Device>(
  session: &BatchStoreSession<'_, D>,
  key: &[u8],
) -> Result<Option<i64>> {
  let prefix = session.session_prefix();
  etag_of_sync_with_prefix(session, prefix.as_slice(), key)
}

/// 显式前缀读 key 的 ETag（循环前缀外提对位，语义与 [`etag_of_sync`] 一致；
/// rust 工程优化无 c# 对应：镜像 [`super::ttl_sync::ttl_of_sync_with_prefix`]，
/// 复用编码单点 [`wkv::StoreSession::etag_key_with_prefix`]，不另起键构造路径）
pub fn etag_of_sync_with_prefix<D: wdev::Device>(
  session: &BatchStoreSession<'_, D>,
  prefix: &[u8],
  key: &[u8],
) -> Result<Option<i64>> {
  let etag_k = wkv::StoreSession::<D>::etag_key_with_prefix(prefix, key);
  match session.try_read_raw_in_memory(&etag_k, I64Codec::decode)? {
    // 内存闭环：非法值长度按无 etag 容错（NoETag = 0）
    StoreResult::Success(v) => Ok(Some(v.unwrap_or(NO_ETAG))),
    // 墓碑 / 无候选视同无 ETag 记录
    StoreResult::NotFound => Ok(Some(NO_ETAG)),
    // 磁盘候选：降级异步裁决
    StoreResult::RecordOnDisk => Ok(None),
  }
}

/// 写 key 的 ETag 记录（同步）：可变区原位改写优先，失败降级 RCU 盲插
///
/// 返回 `Ok(true)` 已闭环；`Ok(false)` 遭遇环形页翻转须降级异步
/// （镜像 `ttl_sync::put_ttl_sync`；长度守卫杜绝 `copy_from_slice`
/// 长度失配 panic）
pub fn put_etag_sync<D: Device>(
  session: &BatchStoreSession<'_, D>,
  key: &[u8],
  etag: i64,
) -> Result<bool> {
  let bytes = I64Codec::encode(etag);
  let etag_k = session.etag_key(key);
  let in_place = session
    .try_modify_raw_in_place_unprotected(&etag_k, |slot| {
      if slot.len() == I64_VAL_LEN {
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
  Ok(session.try_upsert_raw_sync(&etag_k, &bytes)?.is_ok())
}

/// 删 key 的 ETag 记录（同步）
///
/// 返回 `Ok(true)` 已闭环（含本就无记录）；`Ok(false)` 须降级异步
pub fn del_etag_sync<D: Device>(session: &BatchStoreSession<'_, D>, key: &[u8]) -> Result<bool> {
  let etag_k = session.etag_key(key);
  Ok(session.try_delete_raw_sync(&etag_k)?.is_ok())
}
