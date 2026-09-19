//! 迁移/复制分块记录帧编解码 (Record Codec) —— 帧协议单点
//!
//! 归属层：客户端会话层（对标 C# 帧协议长在 GarnetClientSession：
//! libs/client/ClientSession/GarnetClientSessionIncremental.cs:TryWriteRecordSpan /
//! :TryWriteChunkedRecordSpan，迁移面与复制面各自转发本会话，互不 import）。
//! 本 crate 的帧面认领：
//! libs/cluster/Server/Migration/MigrateSessionCommonUtils.cs
//! （帧流式写入与分块切片）、libs/server/Resp/Vector/VectorManager.Migration.cs
//! （向量集索引与元素帧序列化/反序列化）、libs/client/ClientSession/
//! GarnetClientSessionIncremental.cs（分块重组规范）。
//!
//! 帧格式定义 (M1/M2 单一真值源)：
//! `payload = [u32 LE recordCount][frame]*`
//! `frame = [u8 kind][frame body]`
//! - kind=1 STRING：`[u32 LE keyLen][key][u32 LE valLen][val][i64 LE expire_unix_ms(0=无TTL)]`
//! - kind=2 ENVELOPE：`[u32 LE keyLen][key][u32 LE envLen][env][i64 LE expire_unix_ms(0=无TTL)]`，
//!   env 为 wcol 对象信封整值 `[1B GarnetObjectType 内层标签][bitcode 载荷]`，
//!   内层类型标签随信封首字节在途携带，帧层不重复类型字段
//! - kind=3 CHUNKED：`[u32 LE chunkLen | CONTINUATION_FLAG][chunk bytes]`，
//!   全部 chunk 载荷顺序拼接 = 完整单条 kind=1/2 frame，续块标志置于除末块
//!   外每帧（对标 C# MigrationRecordSpanType.ChunkedLogRecord，
//!   libs/client/ClientSession/GarnetClientSessionIncremental.cs）
//! - kind=4 RANGE_INDEX：`[u32 LE len][bytes]` 范围索引分块流
//! - kind=5 VECTOR_SET_INDEX：`[u32 LE keyLen][key][u32 LE valLen][val]`
//! - kind=6 VECTOR_SET_ELEMENT：`[u32 LE keyLen][key][u32 LE elemLen][element][u32 LE valLen][values][u32 LE attrLen][attrs]`

use std::result;

use crate::{Error, Result};

/// 迁移帧记录类型：字符串记录
pub const MIGRATION_RECORD_KIND_STRING: u8 = 1;
/// 迁移帧记录类型：对象信封记录（wcol ObjectEnvelope 整值）
pub const MIGRATION_RECORD_KIND_ENVELOPE: u8 = 2;
/// 迁移帧记录类型：分块记录（载荷拼接为完整 kind=1/2 frame）
pub const MIGRATION_RECORD_KIND_CHUNKED: u8 = 3;
/// 迁移帧记录类型：范围索引分块流（对标 C# MigrationRecordSpanType.SerializedRangeIndexStream）
pub const MIGRATION_RECORD_KIND_RANGE_INDEX: u8 = 4;
/// 迁移帧记录类型：向量集索引记录（对标 C# MigrationRecordSpanType.VectorSetIndex；
/// 帧体 `[u32 keyLen][key][u32 valLen][val]`，val 为 56B 索引记录且已由发送端
/// 完成上下文重映射 + index_ptr 清零，对标 VectorManager.SerializeMigratedIndexKey）
pub const MIGRATION_RECORD_KIND_VECTOR_SET_INDEX: u8 = 5;
/// 迁移帧记录类型：向量集元素记录（VADD 语义载荷；帧体
/// `[u32 keyLen][key][u32 elemLen][element][u32 vecLen][values][u32 attrLen][attrs]`，
/// values 恒为量化器原生格式——目标端按已导入索引零转换直插，
/// 对标 C# 迁移元素记录 SerializeMigratedElementKey + HandleMigratedElementKey）
pub const MIGRATION_RECORD_KIND_VECTOR_SET_ELEMENT: u8 = 6;
/// CHUNKED 帧块长字段最高位续块标志（单名，一处定义）
///
/// 在 garnet 中的相对路径: libs/storage/Tsavorite/cs/src/core/Allocator/ObjectSerialization/ChunkedRecordConstants.cs:ContinuationFlag
pub const CONTINUATION_FLAG: u32 = 0x8000_0000;

/// 单条迁移记录解码视图（kind=1/2，分块重组产物同型）
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MigrationRecord<'a> {
  /// 字符串记录 (kind=1)
  Str {
    /// 键字节
    key: &'a [u8],
    /// 值字节
    val: &'a [u8],
    /// 绝对过期 Unix 时间戳 (毫秒，0 表示无 TTL)
    expire_unix_ms: i64,
  },
  /// 对象信封记录 (kind=2)：env = `[1B GarnetObjectType 内层标签][bitcode 载荷]`
  Env {
    /// 键字节
    key: &'a [u8],
    /// 信封整值
    env: &'a [u8],
    /// 绝对过期 Unix 时间戳 (毫秒，0 表示无 TTL)
    expire_unix_ms: i64,
  },
}

impl MigrationRecord<'_> {
  /// 记录键视图
  #[inline]
  pub fn key(&self) -> &[u8] {
    match self {
      Self::Str { key, .. } | Self::Env { key, .. } => key,
    }
  }

  /// 绝对过期 Unix 时间戳 (毫秒，0 表示无 TTL)
  #[inline]
  pub fn expire_unix_ms(&self) -> i64 {
    match self {
      Self::Str { expire_unix_ms, .. } | Self::Env { expire_unix_ms, .. } => *expire_unix_ms,
    }
  }
}

/// 迁移载荷帧视图（kind=1/2 完整记录、kind=3 分块、kind=4 RangeIndex 流、
/// kind=5/6 向量集索引与元素）
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MigrationFrame<'a> {
  /// 完整记录 (kind=1/2)
  Record(MigrationRecord<'a>),
  /// 分块帧 (kind=3)：载荷片段与续块标志
  Chunk {
    /// 块载荷字节
    bytes: &'a [u8],
    /// 是否后续还有本记录的块
    more: bool,
  },
  /// 范围索引分块流 (kind=4)
  RangeIndexStream(&'a [u8]),
  /// 向量集索引记录 (kind=5)
  VectorSetIndex {
    /// 向量集键
    key: &'a [u8],
    /// 56B 索引记录（上下文已重映射、index_ptr=0）
    value: &'a [u8],
  },
  /// 向量集元素记录 (kind=6)
  VectorSetElement {
    /// 向量集键
    key: &'a [u8],
    /// 元素 id
    element: &'a [u8],
    /// 原生格式向量字节
    values: &'a [u8],
    /// 元素属性
    attributes: &'a [u8],
  },
}

/// 发送端批内条目值（存储读取产物）
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MigrateVal {
  /// string 记录值
  Str(Vec<u8>),
  /// 对象信封整值（[内层标签][载荷]）
  Env(Vec<u8>),
}

impl MigrateVal {
  /// 帧记录类型
  #[inline]
  pub fn kind(&self) -> u8 {
    match self {
      Self::Str(_) => MIGRATION_RECORD_KIND_STRING,
      Self::Env(_) => MIGRATION_RECORD_KIND_ENVELOPE,
    }
  }

  /// 值字节
  #[inline]
  pub fn bytes(&self) -> &[u8] {
    match self {
      Self::Str(v) | Self::Env(v) => v,
    }
  }
}

/// 发送端批内条目（键、值、TTL 毫秒）
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchItem<'a> {
  /// 键字节
  pub key: &'a [u8],
  /// 值
  pub val: MigrateVal,
  /// 绝对过期 Unix 时间戳 (毫秒，0 表示无 TTL)
  pub expire_unix_ms: i64,
}

impl BatchItem<'_> {
  /// 单条 frame 编码字节数（kind 1B + keyLen 4B + key + valLen 4B + val + expire 8B）
  #[inline]
  pub fn frame_len(&self) -> usize {
    17 + self.key.len() + self.val.bytes().len()
  }
}

/// 发送端向量集元素批内条目（键 + 导出元素三段）
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrateVectorElement {
  /// 向量集键
  pub key: Vec<u8>,
  /// 元素 id
  pub element: Vec<u8>,
  /// 原生格式向量字节
  pub values: Vec<u8>,
  /// 元素属性
  pub attributes: Vec<u8>,
}

impl MigrateVectorElement {
  /// 单条帧编码字节数（kind 1B + 4 段各 4B 长度头 + 段字节）
  #[inline]
  pub fn frame_len(&self) -> usize {
    17 + self.key.len() + self.element.len() + self.values.len() + self.attributes.len()
  }
}

/// 读取单个 `[u32 LE len][bytes]` 前缀段（越界显式报错）
fn take_len_prefixed<'a>(payload: &mut &'a [u8]) -> Result<&'a [u8]> {
  let Some((len_bytes, rest)) = payload.split_first_chunk::<4>() else {
    return Err(Error::InvalidRecord("载荷意外截断 (len)".into()));
  };
  let len = u32::from_le_bytes(*len_bytes) as usize;
  if rest.len() < len {
    return Err(Error::InvalidRecord("载荷意外截断 (segment)".into()));
  }
  let (seg, rest) = rest.split_at(len);
  *payload = rest;
  Ok(seg)
}

/// 单条记录体解码（kind + 除去 kind 的 body 字节；返回解码产物与消耗字节数）
fn parse_typed_record<'a>(kind: u8, mut body: &'a [u8]) -> Result<(MigrationRecord<'a>, usize)> {
  let initial_len = body.len();
  let key = take_len_prefixed(&mut body)?;
  let val = take_len_prefixed(&mut body)?;
  let Some((exp_bytes, rest)) = body.split_first_chunk::<8>() else {
    return Err(Error::InvalidRecord("迁移记录意外截断 (expire)".into()));
  };
  let expire_unix_ms = i64::from_le_bytes(*exp_bytes);
  let consumed = initial_len - rest.len();
  let record = match kind {
    MIGRATION_RECORD_KIND_STRING => MigrationRecord::Str {
      key,
      val,
      expire_unix_ms,
    },
    MIGRATION_RECORD_KIND_ENVELOPE => MigrationRecord::Env {
      key,
      env: val,
      expire_unix_ms,
    },
    _ => {
      return Err(Error::InvalidRecord(format!(
        "Unsupported migration record kind {kind}"
      )));
    }
  };
  Ok((record, consumed))
}

/// 解析分块重组产物（完整单条 frame 编码）
pub fn parse_record(raw: &[u8]) -> Result<MigrationRecord<'_>> {
  let Some((kind, body)) = raw.split_first() else {
    return Err(Error::InvalidRecord("迁移记录为空".into()));
  };
  let (record, _) = parse_typed_record(*kind, body)?;
  Ok(record)
}

/// 解析迁移载荷帧
///
/// 在 garnet 中的相对路径: libs/server/Resp/Vector/VectorManager.Migration.cs:DeserializeMigratedIndexKey
///（kind=5 分支 [keyLen][key][valLen][val] 前缀段展开）
/// 在 garnet 中的相对路径: libs/server/Resp/Vector/VectorManager.Migration.cs:DeserializeMigratedElementKey
///（kind=6 分支 [keyLen][key][elemLen][element][valLen][values][attrLen][attributes] 前缀段展开）
pub fn parse_migration_payload(payload: &[u8]) -> Result<(u32, Vec<MigrationFrame<'_>>)> {
  let Some((count_bytes, mut cur_slice)) = payload.split_first_chunk::<4>() else {
    return Err(Error::InvalidRecord("载荷长度不足 4 字节".into()));
  };
  let record_count = u32::from_le_bytes(*count_bytes);
  if record_count == 0 {
    return Ok((0, Vec::new()));
  }
  let mut frames = Vec::with_capacity(record_count as usize);
  for _ in 0..record_count {
    let Some((&kind, rest)) = cur_slice.split_first() else {
      return Err(Error::InvalidRecord("载荷意外截断 (kind)".into()));
    };
    cur_slice = rest;
    match kind {
      MIGRATION_RECORD_KIND_STRING | MIGRATION_RECORD_KIND_ENVELOPE => {
        let (record, consumed) = parse_typed_record(kind, cur_slice)?;
        cur_slice = &cur_slice[consumed..];
        frames.push(MigrationFrame::Record(record));
      }
      MIGRATION_RECORD_KIND_CHUNKED => {
        let Some((head_bytes, rest)) = cur_slice.split_first_chunk::<4>() else {
          return Err(Error::InvalidRecord("载荷意外截断 (chunk_len)".into()));
        };
        let head = u32::from_le_bytes(*head_bytes);
        let more = head & CONTINUATION_FLAG != 0;
        let chunk_len = (head & !CONTINUATION_FLAG) as usize;
        if rest.len() < chunk_len {
          return Err(Error::InvalidRecord("载荷意外截断 (chunk)".into()));
        }
        let (chunk_bytes, rest) = rest.split_at(chunk_len);
        cur_slice = rest;
        frames.push(MigrationFrame::Chunk {
          bytes: chunk_bytes,
          more,
        });
      }
      MIGRATION_RECORD_KIND_RANGE_INDEX => {
        let bytes = take_len_prefixed(&mut cur_slice)?;
        frames.push(MigrationFrame::RangeIndexStream(bytes));
      }
      MIGRATION_RECORD_KIND_VECTOR_SET_INDEX => {
        let key = take_len_prefixed(&mut cur_slice)?;
        let value = take_len_prefixed(&mut cur_slice)?;
        frames.push(MigrationFrame::VectorSetIndex { key, value });
      }
      MIGRATION_RECORD_KIND_VECTOR_SET_ELEMENT => {
        let key = take_len_prefixed(&mut cur_slice)?;
        let element = take_len_prefixed(&mut cur_slice)?;
        let values = take_len_prefixed(&mut cur_slice)?;
        let attributes = take_len_prefixed(&mut cur_slice)?;
        frames.push(MigrationFrame::VectorSetElement {
          key,
          element,
          values,
          attributes,
        });
      }
      other => {
        return Err(Error::InvalidRecord(format!(
          "Unsupported migration record kind {other}"
        )));
      }
    }
  }
  Ok((record_count, frames))
}

/// 追加一条完整记录帧（kind + [keyLen][key][valLen][val][expire]）
///
/// 在 garnet 中的相对路径: libs/cluster/Server/Migration/MigrateSessionCommonUtils.cs:WriteOrSendRecordAsync
/// 在 garnet 中的相对路径: libs/cluster/Server/Migration/MigrateSessionCommonUtils.cs:WriteOrSendRecordSpanAsync
/// 在 garnet 中的相对路径: libs/cluster/Server/Migration/MigrateSessionCommonUtils.cs:WriteOrSendSegmentedRecordAsync
fn write_record_frame(buf: &mut Vec<u8>, item: &BatchItem<'_>) {
  buf.push(item.val.kind());
  buf.extend_from_slice(&(item.key.len() as u32).to_le_bytes());
  buf.extend_from_slice(item.key);
  buf.extend_from_slice(&(item.val.bytes().len() as u32).to_le_bytes());
  buf.extend_from_slice(item.val.bytes());
  buf.extend_from_slice(&item.expire_unix_ms.to_le_bytes());
}

/// 编码迁移载荷帧（批量完整记录，单趟流式序列化零中间分配）
///
/// 在 garnet 中的相对路径: libs/cluster/Server/Migration/MigrateSessionCommonUtils.cs:WriteOrSendAccumulatedRecordAsync
pub fn encode_migration_payload(items: &[BatchItem<'_>]) -> Vec<u8> {
  let body: usize = items.iter().map(BatchItem::frame_len).sum();
  let mut buf = Vec::with_capacity(4 + body);
  buf.extend_from_slice(&(items.len() as u32).to_le_bytes());
  for item in items {
    write_record_frame(&mut buf, item);
  }
  buf
}

/// 流式发送超限单条记录的分块载荷（免整帧临时缓冲分配）
///
/// 在 garnet 中的相对路径: libs/cluster/Server/Migration/MigrateSessionCommonUtils.cs:WriteOrSendChunkedRecordAsync
pub async fn send_chunked_record<E>(
  item: &BatchItem<'_>,
  max_chunk: usize,
  mut send_chunk: impl AsyncFnMut(&[u8]) -> result::Result<(), E>,
) -> result::Result<(), E> {
  if max_chunk == 0 {
    return Ok(());
  }
  let total_len = item.frame_len();
  let kind_bytes = [item.val.kind()];
  let key_len_bytes = (item.key.len() as u32).to_le_bytes();
  let val_len_bytes = (item.val.bytes().len() as u32).to_le_bytes();
  let exp_bytes = item.expire_unix_ms.to_le_bytes();

  let pieces: [&[u8]; 6] = [
    &kind_bytes,
    &key_len_bytes,
    item.key,
    &val_len_bytes,
    item.val.bytes(),
    &exp_bytes,
  ];

  let mut chunk_payload = Vec::with_capacity(9 + max_chunk);
  let mut piece_idx = 0;
  let mut piece_off = 0;
  let mut sent_total = 0;

  while sent_total < total_len {
    let chunk_len = (total_len - sent_total).min(max_chunk);
    let more = sent_total + chunk_len < total_len;

    chunk_payload.clear();
    chunk_payload.extend_from_slice(&[1, 0, 0, 0, MIGRATION_RECORD_KIND_CHUNKED]);
    let head = (chunk_len as u32) | if more { CONTINUATION_FLAG } else { 0 };
    chunk_payload.extend_from_slice(&head.to_le_bytes());

    let mut remaining_chunk = chunk_len;
    while remaining_chunk > 0 && piece_idx < pieces.len() {
      let piece = pieces[piece_idx];
      let available = piece.len() - piece_off;
      let take = available.min(remaining_chunk);
      chunk_payload.extend_from_slice(&piece[piece_off..piece_off + take]);
      piece_off += take;
      remaining_chunk -= take;
      if piece_off == piece.len() {
        piece_idx += 1;
        piece_off = 0;
      }
    }

    send_chunk(&chunk_payload).await?;
    sent_total += chunk_len;
  }

  Ok(())
}

/// 编码单个 RangeIndex 分块流迁移载荷（写入指定缓冲）
pub fn encode_range_index_stream_payload_into(chunk: &[u8], buf: &mut Vec<u8>) {
  buf.clear();
  buf.reserve(4 + 1 + 4 + chunk.len());
  buf.extend_from_slice(&1u32.to_le_bytes()); // 1 record
  buf.push(MIGRATION_RECORD_KIND_RANGE_INDEX);
  buf.extend_from_slice(&(chunk.len() as u32).to_le_bytes());
  buf.extend_from_slice(chunk);
}

/// 追加一条向量集索引帧 (kind=5)：`[u32 keyLen][key][u32 valLen][val]`
///
/// 在 garnet 中的相对路径: libs/server/Resp/Vector/VectorManager.Migration.cs:SerializeMigratedIndexKey
///（帧布局逐段同构；存储尺寸预计算 GetMigratedIndexKeySerializationSize
/// 随调用方 Vec::with_capacity 容量内联消失）
fn write_vector_set_index_frame(buf: &mut Vec<u8>, key: &[u8], value: &[u8]) {
  buf.push(MIGRATION_RECORD_KIND_VECTOR_SET_INDEX);
  buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
  buf.extend_from_slice(key);
  buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
  buf.extend_from_slice(value);
}

/// 追加一条向量集元素帧 (kind=6)
///
/// 在 garnet 中的相对路径: libs/server/Resp/Vector/VectorManager.Migration.cs:SerializeMigratedElementKey
///（C# 手工 [nsLen][ns][keyLen][key][valLen][value] 布局在此展开为
/// [keyLen][key][elemLen][element][valLen][values][attrLen][attributes] 四段；
/// namespace 段不随帧传输——上下文重映射由发送端 remap_index_for_migration
/// 单点完成，接收端以已导入索引的 context 插入；存储尺寸预计算
/// GetMigratedElementKeySerializationSize 随调用方容量预计算内联消失）
fn write_vector_set_element_frame(
  buf: &mut Vec<u8>,
  key: &[u8],
  element: &[u8],
  values: &[u8],
  attributes: &[u8],
) {
  buf.push(MIGRATION_RECORD_KIND_VECTOR_SET_ELEMENT);
  for seg in [key, element, values, attributes] {
    buf.extend_from_slice(&(seg.len() as u32).to_le_bytes());
    buf.extend_from_slice(seg);
  }
}

/// 编码单条向量集索引迁移载荷（单消息控制载荷，对标 C# 逐键
/// TryWriteRecordSpan(VectorSetIndex) + 强制冲刷的停等形态）
pub fn encode_vector_set_index_payload(key: &[u8], value: &[u8]) -> Vec<u8> {
  let mut buf = Vec::with_capacity(4 + 1 + 8 + key.len() + value.len());
  buf.extend_from_slice(&1u32.to_le_bytes());
  write_vector_set_index_frame(&mut buf, key, value);
  buf
}

/// 编码一批向量集元素迁移载荷（多帧单趟停等，对标 C# 迭代缓冲批量冲刷）
pub fn encode_vector_set_element_payload(items: &[MigrateVectorElement]) -> Vec<u8> {
  let body: usize = items.iter().map(MigrateVectorElement::frame_len).sum();
  let mut buf = Vec::with_capacity(4 + body);
  buf.extend_from_slice(&(items.len() as u32).to_le_bytes());
  for it in items {
    write_vector_set_element_frame(&mut buf, &it.key, &it.element, &it.values, &it.attributes);
  }
  buf
}

#[cfg(test)]
mod tests {
  use compio::runtime::Runtime;

  use super::*;

  /// 完整记录帧金字节：编码输出逐字节钉死 + 同字节解码往返等价
  #[test]
  fn record_frame_golden_bytes_roundtrip() {
    let item = BatchItem {
      key: b"ka",
      val: MigrateVal::Str(b"vv".to_vec()),
      expire_unix_ms: 5,
    };
    let payload = encode_migration_payload(&[item]);
    let mut expect = Vec::new();
    expect.extend_from_slice(&1u32.to_le_bytes());
    expect.push(MIGRATION_RECORD_KIND_STRING);
    expect.extend_from_slice(&2u32.to_le_bytes());
    expect.extend_from_slice(b"ka");
    expect.extend_from_slice(&2u32.to_le_bytes());
    expect.extend_from_slice(b"vv");
    expect.extend_from_slice(&5i64.to_le_bytes());
    assert_eq!(payload, expect);

    let (count, frames) = parse_migration_payload(&payload).unwrap();
    assert_eq!(count, 1);
    assert_eq!(
      frames[0],
      MigrationFrame::Record(MigrationRecord::Str {
        key: b"ka",
        val: b"vv",
        expire_unix_ms: 5,
      })
    );
  }

  /// 分块出帧金字节：48B 单帧切 32+16 两块，首块置续块位、末块不置，
  /// 两块载荷拼接 = 完整单条记录帧（下沉前后帧字节口径）
  #[test]
  fn send_chunked_record_frame_golden_bytes() {
    let item = BatchItem {
      key: b"k",
      val: MigrateVal::Str(vec![7u8; 30]),
      expire_unix_ms: 1,
    };
    let total = item.frame_len();
    assert_eq!(total, 48);
    let mut full = Vec::new();
    write_record_frame(&mut full, &item);

    let mut frames: Vec<Vec<u8>> = Vec::new();
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
      send_chunked_record(&item, 32, async |chunk| {
        frames.push(chunk.to_vec());
        Ok::<(), ()>(())
      })
      .await
      .unwrap();
    });
    assert_eq!(frames.len(), 2);
    let head0 = u32::from_le_bytes(frames[0][5..9].try_into().unwrap());
    assert_eq!(head0, 32 | CONTINUATION_FLAG);
    let head1 = u32::from_le_bytes(frames[1][5..9].try_into().unwrap());
    assert_eq!(head1, 16);
    for frame in &frames {
      assert_eq!(&frame[..4], &1u32.to_le_bytes());
      assert_eq!(frame[4], MIGRATION_RECORD_KIND_CHUNKED);
    }
    let joined: Vec<u8> = frames.iter().flat_map(|f| f[9..].iter().copied()).collect();
    assert_eq!(joined, full);
  }

  /// RangeIndex 分块流帧与向量集帧的编解码往返
  #[test]
  fn range_index_and_vector_frames_roundtrip() {
    let mut buf = Vec::new();
    encode_range_index_stream_payload_into(b"chunk", &mut buf);
    let (count, frames) = parse_migration_payload(&buf).unwrap();
    assert_eq!(count, 1);
    assert_eq!(frames[0], MigrationFrame::RangeIndexStream(b"chunk"));

    let payload = encode_vector_set_index_payload(b"vk", b"idx56");
    let (_, frames) = parse_migration_payload(&payload).unwrap();
    assert_eq!(
      frames[0],
      MigrationFrame::VectorSetIndex {
        key: b"vk",
        value: b"idx56"
      }
    );

    let elem = MigrateVectorElement {
      key: b"vk".to_vec(),
      element: b"e1".to_vec(),
      values: vec![1, 2, 3, 4],
      attributes: b"a".to_vec(),
    };
    let payload = encode_vector_set_element_payload(&[elem]);
    let (_, frames) = parse_migration_payload(&payload).unwrap();
    assert_eq!(
      frames[0],
      MigrationFrame::VectorSetElement {
        key: b"vk",
        element: b"e1",
        values: &[1, 2, 3, 4],
        attributes: b"a"
      }
    );
  }
}
