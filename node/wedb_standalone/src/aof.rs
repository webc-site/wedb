//! AOF 逻辑层：类型化条目头 + 零拷贝解码 + 回放分发
//!
//! 对标 Garnet `AofHeader`/`AofProcessor` 的职责切分：物理顺序日志由
//! `waof::WalLog` 承担（TsavoriteLog 角色），本模块只定义"操作镜像"条目
//! 格式与回放分发契约。WAL 载荷 = 本层条目帧，协议字节（RESP）只作为
//! 特定操作的 blob 载荷出现，回放端按 `AofOp` 分发，不解析协议。

use core::result;

/// 条目头定长字节数：`op` u8 + `flags` u8 + 保留 u16 + `version` u32 = 8B
pub const AOF_HEADER_LEN: usize = 8;

/// 键长度前缀字节数（u32 小端）
const KEY_LEN_PREFIX: usize = 4;
/// blob 长度前缀字节数（u32 小端）
const BLOB_LEN_PREFIX: usize = 4;

/// AOF 条目操作类型（对齐 Garnet `AofEntryType`，覆盖 wedb_standalone 当前落日志的操作面）
///
/// `KvUpsert`/`KvDelete` 为预留位：对应 KV 编排方法尚未接入 service，
/// 回放端可先按 UnknownOp 兜底处理
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum AofOp {
  /// 字符串键 upsert
  KvUpsert = 1,
  /// 字符串键删除
  KvDelete = 2,
  /// 创建范围索引
  RiCreate = 3,
  /// 范围索引设置字段
  RiSet = 4,
  /// 范围索引删除字段
  RiDel = 5,
  /// TTL 过期物理清除（单条确定性逻辑条目）
  ///
  /// 对标 Garnet `InputHeader` 的 `RespInputFlags.Deterministic`（携带绝对过期
  /// 时间的单条目入流，见 libs/server/Storage/Functions/MainStore/
  /// PrivateMethods.cs 的 WriteLogRMW）：整次过期清除折叠为一条携带
  /// `(ns, db, 用户键, expire_at_ms)` 的逻辑条目，替代"删 TTL 记录 + 删数据"
  /// 两条物理墓碑条目，副本重放零时钟漂移且流内条目数最小。载荷布局见
  /// [`TtlPurgePayload`]
  TtlPurge = 6,
}

impl TryFrom<u8> for AofOp {
  type Error = Error;

  #[inline]
  fn try_from(v: u8) -> AofResult<Self> {
    match v {
      1 => Ok(Self::KvUpsert),
      2 => Ok(Self::KvDelete),
      3 => Ok(Self::RiCreate),
      4 => Ok(Self::RiSet),
      5 => Ok(Self::RiDel),
      6 => Ok(Self::TtlPurge),
      _ => Err(Error::UnknownOp(v)),
    }
  }
}

/// 借用视图的 AOF 条目（零拷贝解码，字段指向输入缓冲区）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AofEntryRef<'a> {
  /// 操作类型
  pub op: AofOp,
  /// 条目版本号（单调递增，由写入方分配；0 表示未标注）
  pub version: u32,
  /// 主键字节
  pub key: &'a [u8],
  /// 操作体载荷（op 专属：RiCreate/RiSet/RiDel 为 RESP 命令帧，Kv* 为值字节）
  pub blob: &'a [u8],
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
  /// 条目字节数不足以容纳定长头或长度前缀（半截尾部）
  #[error("AOF entry truncated: need {need} bytes, got {got}")]
  Truncated { need: usize, got: usize },
  /// 长度前缀声明的字节数超出缓冲区
  #[error("AOF entry length prefix overflow: declared {declared}, remaining {remaining}")]
  Overflow { declared: usize, remaining: usize },
  /// 未知操作类型
  #[error("unknown AOF op: {0}")]
  UnknownOp(u8),
}

pub type AofResult<T> = result::Result<T, Error>;

/// [`AofOp::TtlPurge`] 条目的二进制载荷（定长 24B，字段全大端）
///
/// # 字节布局（偏移 + 宽度 + 字节序）
///
/// ```text
/// 偏移 0..8    ns            u64 大端   命名空间编号
/// 偏移 8..16   db            u64 大端   逻辑数据库编号
/// 偏移 16..24  expire_at_ms  u64 大端   绝对过期毫秒时间戳（Unix epoch）
/// ```
///
/// 字段大端为刻意选择：对齐 wval `TtlCodec`（TTL 记录值 8B 大端 u64）与引擎内
/// 时间戳编码口径；条目外层帧（定长头/长度前缀）仍为小端，见 [`encode_entry`]。
///
/// 语义对标 Garnet `RespInputFlags.Deterministic`：载荷携带绝对过期时间，
/// 回放端不依赖本地时钟即可确定性地应用过期清除，主从零漂移
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TtlPurgePayload {
  /// 命名空间编号
  pub ns: u64,
  /// 逻辑数据库编号
  pub db: u64,
  /// 绝对过期毫秒时间戳
  pub expire_at_ms: u64,
}

impl TtlPurgePayload {
  /// 载荷定长字节数
  pub const LEN: usize = 24;

  /// 编码为定长大端字节序列
  #[inline]
  pub fn encode(self) -> [u8; Self::LEN] {
    let mut buf = [0u8; Self::LEN];
    buf[0..8].copy_from_slice(&self.ns.to_be_bytes());
    buf[8..16].copy_from_slice(&self.db.to_be_bytes());
    buf[16..24].copy_from_slice(&self.expire_at_ms.to_be_bytes());
    buf
  }

  /// 从条目 blob 解码（长度不符即报错）
  #[inline]
  pub fn decode(blob: &[u8]) -> AofResult<Self> {
    match blob.len() {
      Self::LEN => {}
      n if n < Self::LEN => {
        return Err(Error::Truncated {
          need: Self::LEN,
          got: n,
        });
      }
      n => {
        return Err(Error::Overflow {
          declared: Self::LEN,
          remaining: n,
        });
      }
    }
    // SAFETY: 上面已校验定长 24B，三段切片边界 100% 安全
    Ok(Self {
      ns: u64::from_be_bytes(unsafe { blob.get_unchecked(0..8).try_into().unwrap_unchecked() }),
      db: u64::from_be_bytes(unsafe { blob.get_unchecked(8..16).try_into().unwrap_unchecked() }),
      expire_at_ms: u64::from_be_bytes(unsafe {
        blob.get_unchecked(16..24).try_into().unwrap_unchecked()
      }),
    })
  }
}

/// 编码一条 AOF 条目：`[头 8B][key_len u32][key][blob_len u32][blob]`
///
/// 预估容量一次分配，避免多次扩容。key/blob 长度依赖 u32 前缀，上限
/// 4GiB——由 waof 环形缓冲容量（默认 16MB）跨层保证，本地不再校验
pub fn encode_entry(op: AofOp, version: u32, key: &[u8], blob: &[u8]) -> Vec<u8> {
  let total = AOF_HEADER_LEN + KEY_LEN_PREFIX + key.len() + BLOB_LEN_PREFIX + blob.len();
  let mut buf = Vec::with_capacity(total);

  buf.push(op as u8);
  buf.push(0); // flags 位域，保留
  buf.extend_from_slice(&[0, 0]); // 保留 u16
  buf.extend_from_slice(&version.to_le_bytes());

  buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
  buf.extend_from_slice(key);
  buf.extend_from_slice(&(blob.len() as u32).to_le_bytes());
  buf.extend_from_slice(blob);
  buf
}

impl<'a> AofEntryRef<'a> {
  /// 从 WAL 载荷解码条目（零拷贝，全部字段借用输入切片）
  pub fn decode(buf: &'a [u8]) -> AofResult<Self> {
    if buf.len() < AOF_HEADER_LEN {
      return Err(Error::Truncated {
        need: AOF_HEADER_LEN,
        got: buf.len(),
      });
    }
    let op = AofOp::try_from(buf[0])?;
    let version =
      u32::from_le_bytes(unsafe { buf.get_unchecked(4..8).try_into().unwrap_unchecked() });

    let key_len = read_len_prefix(buf, AOF_HEADER_LEN, KEY_LEN_PREFIX)?;
    let key_end = AOF_HEADER_LEN + KEY_LEN_PREFIX + key_len;
    if buf.len() < key_end {
      return Err(Error::Overflow {
        declared: key_len,
        remaining: buf.len() - AOF_HEADER_LEN - KEY_LEN_PREFIX,
      });
    }
    let key = &buf[AOF_HEADER_LEN + KEY_LEN_PREFIX..key_end];

    let blob_len = read_len_prefix(buf, key_end, BLOB_LEN_PREFIX)?;
    let blob_end = key_end + BLOB_LEN_PREFIX + blob_len;
    if buf.len() < blob_end {
      return Err(Error::Overflow {
        declared: blob_len,
        remaining: buf.len() - key_end - BLOB_LEN_PREFIX,
      });
    }
    let blob = &buf[key_end + BLOB_LEN_PREFIX..blob_end];

    Ok(Self {
      op,
      version,
      key,
      blob,
    })
  }
}

/// 从 `buf[offset..]` 读取小端 u32 长度前缀，不足即报半截
#[inline]
fn read_len_prefix(buf: &[u8], offset: usize, width: usize) -> AofResult<usize> {
  let end = offset + width;
  if buf.len() < end {
    return Err(Error::Truncated {
      need: end,
      got: buf.len(),
    });
  }
  Ok(
    u32::from_le_bytes(unsafe { buf.get_unchecked(offset..end).try_into().unwrap_unchecked() })
      as usize,
  )
}

/// 回放分发契约（对标 Garnet `AofProcessor` 的条目分发口）
///
/// 恢复端与副本端各自实现：本端按 `AofOp` 将操作镜像重新作用到存储引擎，
/// blob 载荷按 op 专属格式解释（协议帧回放由上层决策，引擎层不感知协议）
pub trait Replay {
  /// 处理一条 AOF 条目；返回 Err 中止回放
  ///
  /// `AofOp::TtlPurge` 条目不经本方法分发：[`NodeService::replay`](crate::service::NodeService::replay)
  /// 解码 24B 定长载荷后改走 [`Self::on_ttl_purge`] 专用分发口
  fn on_entry(&mut self, entry: AofEntryRef<'_>) -> AofResult<()>;

  /// 处理一条 TTL 过期物理清除条目（`AofOp::TtlPurge` 专用分发口）
  ///
  /// 默认实现按预留位兜底语义跳过（对齐 KvUpsert/KvDelete），既有实现者零破坏；
  /// 副本端/恢复端覆写以本地执行过期清除——语义与主端 `purge_expired` 等效且
  /// 幂等，可经 [`crate::service::NodeService::apply_ttl_purge`] 对接
  fn on_ttl_purge(&mut self, _key: &[u8], _payload: TtlPurgePayload) -> AofResult<()> {
    Ok(())
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn encode_decode_round_trip() {
    let buf = encode_entry(AofOp::RiSet, 7, b"idx", b"*4\r\n...");
    assert_eq!(buf.len(), 8 + 4 + 3 + 4 + 7);

    let e = AofEntryRef::decode(&buf).unwrap();
    assert_eq!(e.op, AofOp::RiSet);
    assert_eq!(e.version, 7);
    assert_eq!(e.key, b"idx");
    assert_eq!(e.blob, b"*4\r\n...");
  }

  #[test]
  fn decode_empty_blob() {
    let buf = encode_entry(AofOp::KvDelete, 0, b"k", &[]);
    let e = AofEntryRef::decode(&buf).unwrap();
    assert_eq!(e.op, AofOp::KvDelete);
    assert_eq!(e.blob, &[] as &[u8]);
  }

  #[test]
  fn decode_truncated_rejected() {
    let buf = encode_entry(AofOp::RiDel, 1, b"idx", b"x");
    for cut in [0, 4, 8, 8 + 2, 8 + 4 + 2] {
      assert!(AofEntryRef::decode(&buf[..cut]).is_err(), "cut={cut}");
    }
  }

  #[test]
  fn decode_unknown_op_rejected() {
    let mut buf = encode_entry(AofOp::RiDel, 1, b"idx", b"x");
    buf[0] = 0xFF;
    assert!(matches!(
      AofEntryRef::decode(&buf),
      Err(Error::UnknownOp(0xFF))
    ));
  }

  #[test]
  fn ttl_purge_payload_round_trip() {
    let payload = TtlPurgePayload {
      ns: 7,
      db: 3,
      expire_at_ms: 0x0102_0304_0506_0708,
    };
    let blob = payload.encode();
    assert_eq!(blob.len(), TtlPurgePayload::LEN);
    // 大端字节序：字段首字节即最高位字节
    assert_eq!(&blob[0..8], &7u64.to_be_bytes());
    assert_eq!(&blob[8..16], &3u64.to_be_bytes());
    assert_eq!(&blob[16..24], &0x0102_0304_0506_0708u64.to_be_bytes());
    assert_eq!(TtlPurgePayload::decode(&blob).unwrap(), payload);
  }

  #[test]
  fn ttl_purge_entry_round_trip_and_malformed_blob() {
    let blob = TtlPurgePayload {
      ns: 1,
      db: 2,
      expire_at_ms: 42,
    }
    .encode();
    let buf = encode_entry(AofOp::TtlPurge, 9, b"glitch", &blob);
    let e = AofEntryRef::decode(&buf).unwrap();
    assert_eq!(e.op, AofOp::TtlPurge);
    assert_eq!(e.key, b"glitch");
    assert_eq!(TtlPurgePayload::decode(e.blob).unwrap().expire_at_ms, 42);

    // 载荷过短/过长一律拒绝（半截与超长都不允许静默截断）
    assert!(matches!(
      TtlPurgePayload::decode(&blob[..23]),
      Err(Error::Truncated { need: 24, got: 23 })
    ));
    let mut long = blob.to_vec();
    long.push(0);
    assert!(matches!(
      TtlPurgePayload::decode(&long),
      Err(Error::Overflow {
        declared: 24,
        remaining: 25
      })
    ));
  }
}
