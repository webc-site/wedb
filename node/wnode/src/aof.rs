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

/// AOF 条目操作类型（对齐 Garnet `AofEntryType`，覆盖 wnode 当前落日志的操作面）
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
  fn on_entry(&mut self, entry: AofEntryRef<'_>) -> AofResult<()>;
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
}
