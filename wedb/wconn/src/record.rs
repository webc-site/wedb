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
//! - kind=4 TREE_STREAM：`[u8 obj_type][i64 LE next_expiry][i64 LE expire_unix_ms]
//!   [u32 LE len][bytes]` wbftree 带外分块流（RangeIndex 与升阶分层集合共用，
//!   只差元记录判别类型）；流元三元组随每块携载（诚实发送端同流同值，与
//!   AOF 流通道 ReplayInput 逐条目携 obj_type/arg2 同形）：obj_type 为发布后
//!   元记录的集合判别（GarnetObjectType 判别值），next_expiry 为成员 TTL 水位
//!   （.NET Ticks，i64::MAX = 无成员挂 TTL），expire_unix_ms 为键级绝对过期
//!   Unix 毫秒（0 = 无 TTL）
//! - kind=5 VECTOR_SET_INDEX：`[u32 LE keyLen][key][u32 LE valLen][val]`
//! - kind=6 VECTOR_SET_ELEMENT：`[u32 LE keyLen][key][u32 LE elemLen][element][u32 LE valLen][values][u32 LE attrLen][attrs]`
//! - kind=7 DOMAIN_CONTEXT：`[u64 LE vns][u64 LE vdb][u64 LE ns][u64 LE db]`
//!   跨域落域上下文（本仓多租户扩展）
//! - kind=8 DB_META：`[u32 LE keyLen][键载荷][u32 LE valLen][记录值]`
//!   DbMeta 映射记录（本仓多租户扩展）

use std::result;

use strum::FromRepr;

use crate::{Error, Result};

/// 迁移帧记录类型
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, FromRepr)]
pub enum MigrationRecordKind {
  /// 字符串记录
  String = 1,
  /// 对象信封记录（wcol ObjectEnvelope 整值）
  Envelope = 2,
  /// 分块记录（载荷拼接为完整 kind=1/2 frame）
  Chunked = 3,
  /// wbftree 带外分块流记录（对标 C# MigrationRecordSpanType.
  /// SerializedRangeIndexStream；rust 泛化：RangeIndex 与升阶分层集合共用，
  /// 帧头流元见模块头 kind=4 格式说明）
  RangeIndex = 4,
  /// 向量集索引记录（对标 C# MigrationRecordSpanType.VectorSetIndex；
  /// 帧体 `[u32 keyLen][key][u32 valLen][val]`，val 为 56B 索引记录且已由发送端
  /// 完成上下文重映射 + index_ptr 清零，对标 VectorManager.SerializeMigratedIndexKey）
  VectorSetIndex = 5,
  /// 向量集元素记录（VADD 语义载荷；帧体
  /// `[u32 keyLen][key][u32 elemLen][element][u32 vecLen][values][u32 attrLen][attrs]`，
  /// values 恒为量化器原生格式——目标端按已导入索引零转换直插，
  /// 对标 C# 迁移元素记录 SerializeMigratedElementKey + HandleMigratedElementKey）
  VectorSetElement = 6,
  /// 跨域落域上下文（本仓多租户多库扩展，C# 单租户单库无此面；
  /// 帧体 `[u64 LE vns][u64 LE vdb][u64 LE ns][u64 LE db]` 定长 32B）——接收会话
  /// 逐命令重建，跨域扇出载荷必须逐载荷携带落域上下文，接收端据此直设会话
  /// 物理域并换算向量帧登记槽
  DomainContext = 7,
  /// DbMeta 映射记录（本仓多租户多库扩展，C# 无此面；帧体
  /// `[u32 keyLen][键载荷][u32 valLen][记录值]`，键载荷与记录值为
  /// `wkv::DbMetaRecord` 单点编码的根域系统记录字节）——全量同步快照链不重放
  /// 锚前 AOF，副本域映射体系经本帧以 `apply_dbmeta_record` 单点收敛（映射
  /// 装载 + 水位抬升 + 本节点落盘，与 AOF 镜像条目回放同一应用口）
  DbMeta = 8,
}
/// CHUNKED 帧块长字段最高位续块标志（单名，一处定义）
///
/// 在 garnet 中的相对路径: libs/storage/Tsavorite/cs/src/core/Allocator/ObjectSerialization/ChunkedRecordConstants.cs:ContinuationFlag
pub const CONTINUATION_FLAG: u32 = 0x8000_0000;

/// kind=4 带外流帧的流元定长段字节数（`[u8 obj_type][i64 next_expiry]
/// [i64 expire_unix_ms]`，编解码两处共用单点）
const TREE_STREAM_META_LEN: usize = 17;

/// kind=1/2 记录帧与 kind=6 元素帧的定长开销（kind 1B + 两段 4B 长度头 +
/// 8B 过期/第四段长度头，编码 frame_len 两处共用；编译期求值）
const TYPED_FRAME_OVERHEAD: usize = 1 + 4 + 4 + 8;

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

/// 迁移载荷帧视图（kind=1/2 完整记录、kind=3 分块、kind=4 wbftree 带外流
///（RangeIndex 与升阶分层集合）、kind=5/6 向量集索引与元素）
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
  /// wbftree 带外分块流 (kind=4，对标 C# MigrationRecordSpanType.
  /// SerializedRangeIndexStream；rust 泛化为 RangeIndex 与升阶分层集合共用，
  /// 帧头携流元三元组，见模块头 kind=4 格式说明)
  RangeIndexStream {
    /// 发布后元记录集合判别（GarnetObjectType 判别值字节）
    obj_type: u8,
    /// 成员 TTL 水位（.NET Ticks，i64::MAX = 无成员挂 TTL）
    next_expiry: i64,
    /// 键级绝对过期 Unix 毫秒（0 = 无 TTL）
    expire_unix_ms: i64,
    /// 流块字节
    bytes: &'a [u8],
  },
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
  /// 跨域落域上下文 (kind=7，本仓多租户扩展)
  DomainContext(MigrationDomainContext),
  /// DbMeta 映射记录 (kind=8，本仓多租户扩展)：键载荷与记录值为
  /// `wkv::DbMetaRecord` 单点编码字节，接收端解码后经 apply_dbmeta_record
  /// 单点应用（映射装载 + 水位抬升 + 本节点落盘）
  DbMeta {
    /// DbMeta 键载荷字节
    key: &'a [u8],
    /// DbMeta 记录值字节
    value: &'a [u8],
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

/// 跨域落域上下文帧载荷（kind=7）：物理域 `(vns, vdb)` + 逻辑域 `(ns, db)`
/// 四标量。物理域供接收端直设会话上下文（从库继承主库映射体系，零本地
/// 二次映射），逻辑域供接收端换算库级定槽的向量帧登记槽
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MigrationDomainContext {
  /// 物理命名空间号
  pub vns: u64,
  /// 物理库号
  pub vdb: u64,
  /// 逻辑命名空间号
  pub ns: u64,
  /// 逻辑库号
  pub db: u64,
}

impl MigrationDomainContext {
  /// 域上下文帧体定长字节数（4 × u64 LE）
  pub const BODY_LEN: usize = 32;

  /// 编码落域上下文帧（kind + 32B 定长体）
  #[inline]
  pub fn encode_frame(&self) -> [u8; 1 + Self::BODY_LEN] {
    let mut buf = [MigrationRecordKind::DomainContext as u8; 1 + Self::BODY_LEN];
    for (slot, v) in buf[1..]
      .as_chunks_mut::<8>()
      .0
      .iter_mut()
      .zip([self.vns, self.vdb, self.ns, self.db])
    {
      slot.copy_from_slice(&v.to_le_bytes());
    }
    buf
  }

  /// 解码落域上下文帧体（32B 定长；长度不符 None）
  #[inline]
  pub fn decode_body(body: &[u8]) -> Option<Self> {
    if body.len() != Self::BODY_LEN {
      return None;
    }
    let (vns, rest) = body.split_first_chunk::<8>()?;
    let (vdb, rest) = rest.split_first_chunk::<8>()?;
    let (ns, db) = rest.split_first_chunk::<8>()?;
    Some(Self {
      vns: u64::from_le_bytes(*vns),
      vdb: u64::from_le_bytes(*vdb),
      ns: u64::from_le_bytes(*ns),
      db: u64::from_le_bytes(db.try_into().ok()?),
    })
  }
}

impl MigrateVal {
  /// 帧记录类型
  #[inline]
  pub fn kind(&self) -> u8 {
    match self {
      Self::Str(_) => MigrationRecordKind::String as u8,
      Self::Env(_) => MigrationRecordKind::Envelope as u8,
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
    TYPED_FRAME_OVERHEAD + self.key.len() + self.val.bytes().len()
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
    let segs = [&self.key, &self.element, &self.values, &self.attributes];
    TYPED_FRAME_OVERHEAD + segs.iter().map(|s| s.len()).sum::<usize>()
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

/// 追加 `[u32 LE len][bytes]` 前缀段（写向单点，与 [`take_len_prefixed`] 读向对偶）
#[inline]
fn write_len_prefixed(buf: &mut Vec<u8>, seg: &[u8]) {
  buf.extend_from_slice(&(seg.len() as u32).to_le_bytes());
  buf.extend_from_slice(seg);
}

/// 追加一条「kind 头 + 两段 `[u32 LE len][bytes]`」帧（kind=5 向量集索引与
/// kind=8 DbMeta 映射共用 `[keyLen][key][valLen][val]` 帧布局）
fn write_two_seg_frame(buf: &mut Vec<u8>, kind: MigrationRecordKind, key: &[u8], value: &[u8]) {
  buf.push(kind as u8);
  for seg in [key, value] {
    write_len_prefixed(buf, seg);
  }
}

/// u8 → 帧记录类型，未知 kind 具名拒绝（解码两处的同文错误单源）
#[inline]
fn record_kind(kind: u8) -> Result<MigrationRecordKind> {
  MigrationRecordKind::from_repr(kind)
    .ok_or_else(|| Error::InvalidRecord(format!("Unsupported migration record kind {kind}")))
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
  let kind_enum = record_kind(kind)?;
  let record = match kind_enum {
    MigrationRecordKind::String => MigrationRecord::Str {
      key,
      val,
      expire_unix_ms,
    },
    MigrationRecordKind::Envelope => MigrationRecord::Env {
      key,
      env: val,
      expire_unix_ms,
    },
    other => {
      return Err(Error::InvalidRecord(format!(
        "Unsupported migration record kind {other:?}"
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
  // 协议违约循环前收口：每条 frame 至少 1 字节 kind，声名条数超剩余字节数
  // 即结构性非法——提前具名拒绝（原「载荷意外截断 (kind)」的循环内截断
  // 歧义收口为显式违约）；预约容量随之钳在载荷可证上界内，伪造
  // count=0xFFFFFFFF 的 5 字节短帧只触发即时拒绝，绝无数百 GB 单次分配
  // 请求（分配失败 abort 全进程的协议面 DoS）。C# 对标
  // libs/cluster/Session/RespClusterMigrateCommands.cs:102-114 逐记录边读
  // 边校验、无预分配面
  if record_count as usize > cur_slice.len() {
    return Err(Error::RecordCountExceedsPayload(
      record_count,
      cur_slice.len(),
    ));
  }
  let mut frames = Vec::with_capacity(record_count as usize);
  for _ in 0..record_count {
    let Some((&kind, rest)) = cur_slice.split_first() else {
      return Err(Error::InvalidRecord("载荷意外截断 (kind)".into()));
    };
    cur_slice = rest;
    let kind_enum = record_kind(kind)?;
    match kind_enum {
      MigrationRecordKind::String | MigrationRecordKind::Envelope => {
        let (record, consumed) = parse_typed_record(kind, cur_slice)?;
        cur_slice = &cur_slice[consumed..];
        frames.push(MigrationFrame::Record(record));
      }
      MigrationRecordKind::Chunked => {
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
      MigrationRecordKind::RangeIndex => {
        // 流元定长段 `[u8 obj_type][i64 LE next_expiry][i64 LE expire_unix_ms]`
        // 随每块携载（17B，格式见模块头 kind=4）
        if cur_slice.len() < TREE_STREAM_META_LEN {
          return Err(Error::InvalidRecord(
            "载荷意外截断 (tree_stream_meta)".into(),
          ));
        }
        let (meta_bytes, rest) = cur_slice.split_at(TREE_STREAM_META_LEN);
        let obj_type = meta_bytes[0];
        let next_expiry = i64::from_le_bytes(meta_bytes[1..9].try_into().unwrap());
        let expire_unix_ms = i64::from_le_bytes(meta_bytes[9..17].try_into().unwrap());
        cur_slice = rest;
        let bytes = take_len_prefixed(&mut cur_slice)?;
        frames.push(MigrationFrame::RangeIndexStream {
          obj_type,
          next_expiry,
          expire_unix_ms,
          bytes,
        });
      }
      MigrationRecordKind::VectorSetIndex => {
        let key = take_len_prefixed(&mut cur_slice)?;
        let value = take_len_prefixed(&mut cur_slice)?;
        frames.push(MigrationFrame::VectorSetIndex { key, value });
      }
      MigrationRecordKind::VectorSetElement => {
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
      MigrationRecordKind::DomainContext => {
        if cur_slice.len() < MigrationDomainContext::BODY_LEN {
          return Err(Error::InvalidRecord(
            "落域上下文帧体长度不足 (domain context)".into(),
          ));
        }
        let (body, rest) = cur_slice.split_at(MigrationDomainContext::BODY_LEN);
        cur_slice = rest;
        let ctx = MigrationDomainContext::decode_body(body)
          .ok_or_else(|| Error::InvalidRecord("落域上下文帧体非法 (domain context)".into()))?;
        frames.push(MigrationFrame::DomainContext(ctx));
      }
      MigrationRecordKind::DbMeta => {
        let key = take_len_prefixed(&mut cur_slice)?;
        let value = take_len_prefixed(&mut cur_slice)?;
        frames.push(MigrationFrame::DbMeta { key, value });
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
  for seg in [item.key, item.val.bytes()] {
    write_len_prefixed(buf, seg);
  }
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

/// 编码 DbMeta 映射帧（kind=8：`[u32 keyLen][键载荷][u32 valLen][记录值]`；
/// 键载荷/记录值出自 `wkv::DbMetaRecord::key`/`value` 单点编码）
pub fn encode_dbmeta_frame(key: &[u8], value: &[u8]) -> Vec<u8> {
  let mut buf = Vec::with_capacity(9 + key.len() + value.len());
  write_two_seg_frame(&mut buf, MigrationRecordKind::DbMeta, key, value);
  buf
}

/// 编码自描述帧序列载荷（`[u32 LE count][frame]*`，帧字节已含 kind 头；
/// 供 DbMeta 映射帧等非 BatchItem 帧的载荷组装，格式与
/// [`encode_migration_payload`] 同一载荷头口径）
pub fn encode_frame_payload(frames: &[Vec<u8>]) -> Vec<u8> {
  let body: usize = frames.iter().map(Vec::len).sum();
  let mut buf = Vec::with_capacity(4 + body);
  buf.extend_from_slice(&(frames.len() as u32).to_le_bytes());
  for frame in frames {
    buf.extend_from_slice(frame);
  }
  buf
}

/// 载荷首帧前插（跨域扇出：接收会话逐命令重建，落域上下文必须逐载荷携带
/// ——记录帧数头 +1 后重排，原载荷帧字节原样后移，一次拷贝代价）
pub fn prepend_migration_frame(frame: &[u8], payload: &[u8]) -> Vec<u8> {
  let count = match payload.split_first_chunk::<4>() {
    Some((b, _)) => u32::from_le_bytes(*b).wrapping_add(1),
    None => 1,
  };
  let mut buf = Vec::with_capacity(payload.len() + frame.len() + 4);
  buf.extend_from_slice(&count.to_le_bytes());
  buf.extend_from_slice(frame);
  buf.extend_from_slice(&payload[4.min(payload.len())..]);
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
    chunk_payload.extend_from_slice(&[1, 0, 0, 0, MigrationRecordKind::Chunked as u8]);
    let head = (chunk_len as u32) | if more { CONTINUATION_FLAG } else { 0 };
    chunk_payload.extend_from_slice(&head.to_le_bytes());

    let mut remaining_chunk = chunk_len;
    while remaining_chunk > 0 && piece_idx < pieces.len() {
      let piece = pieces[piece_idx];
      let take = (piece.len() - piece_off).min(remaining_chunk);
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

/// 编码单个 wbftree 带外分块流迁移载荷（流元三元组随每块携载，格式见
/// 模块头 kind=4；RangeIndex 与升阶分层集合共用本口）
pub fn encode_range_index_stream_payload_into(
  chunk: &[u8],
  obj_type: u8,
  next_expiry: i64,
  expire_unix_ms: i64,
  buf: &mut Vec<u8>,
) {
  buf.clear();
  buf.reserve(4 + 1 + TREE_STREAM_META_LEN + 4 + chunk.len());
  buf.extend_from_slice(&1u32.to_le_bytes()); // 1 record
  buf.push(MigrationRecordKind::RangeIndex as u8);
  buf.push(obj_type);
  buf.extend_from_slice(&next_expiry.to_le_bytes());
  buf.extend_from_slice(&expire_unix_ms.to_le_bytes());
  write_len_prefixed(buf, chunk);
}

/// 编码单条向量集索引迁移载荷（单消息控制载荷，对标 C# 逐键
/// TryWriteRecordSpan(VectorSetIndex) + 强制冲刷的停等形态）
///
/// 在 garnet 中的相对路径: libs/server/Resp/Vector/VectorManager.Migration.cs:SerializeMigratedIndexKey
///（帧布局逐段同构；存储尺寸预计算 GetMigratedIndexKeySerializationSize
/// 随调用方 Vec::with_capacity 容量内联消失）
pub fn encode_vector_set_index_payload(key: &[u8], value: &[u8]) -> Vec<u8> {
  let mut buf = Vec::with_capacity(4 + 1 + 8 + key.len() + value.len());
  buf.extend_from_slice(&1u32.to_le_bytes());
  write_two_seg_frame(&mut buf, MigrationRecordKind::VectorSetIndex, key, value);
  buf
}

/// 编码一批向量集元素迁移载荷（多帧单趟停等，对标 C# 迭代缓冲批量冲刷；
/// 单帧 kind=6 逐条追加，布局见下 garnet 对照）
///
/// 在 garnet 中的相对路径: libs/server/Resp/Vector/VectorManager.Migration.cs:SerializeMigratedElementKey
///（C# 手工 [nsLen][ns][keyLen][key][valLen][value] 布局在此展开为
/// [keyLen][key][elemLen][element][valLen][values][attrLen][attributes] 四段；
/// namespace 段不随帧传输——上下文重映射由发送端 remap_index_for_migration
/// 单点完成，接收端以已导入索引的 context 插入；存储尺寸预计算
/// GetMigratedElementKeySerializationSize 随调用方容量预计算内联消失）
pub fn encode_vector_set_element_payload(items: &[MigrateVectorElement]) -> Vec<u8> {
  let body: usize = items.iter().map(MigrateVectorElement::frame_len).sum();
  let mut buf = Vec::with_capacity(4 + body);
  buf.extend_from_slice(&(items.len() as u32).to_le_bytes());
  for it in items {
    buf.push(MigrationRecordKind::VectorSetElement as u8);
    for seg in [&it.key, &it.element, &it.values, &it.attributes] {
      write_len_prefixed(&mut buf, seg);
    }
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
    expect.push(MigrationRecordKind::String as u8);
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
      assert_eq!(frame[4], MigrationRecordKind::Chunked as u8);
    }
    let joined: Vec<u8> = frames.iter().flat_map(|f| f[9..].iter().copied()).collect();
    assert_eq!(joined, full);
  }

  /// 巨值计数短帧协议违约回归：伪造 count=0xFFFFFFFF 的 5 字节帧在解析
  /// 循环前具名拒绝（RecordCountExceedsPayload），绝无按 u32 全量预分配
  ///（分配失败 abort 全进程的协议面 DoS 守卫）；计数恰等可证下界
  ///（每记录 1 字节 kind）的载荷不得被违约门误伤，仍由逐帧截断收口
  #[test]
  fn parse_rejects_forged_record_count_upfront() {
    let mut forged = Vec::new();
    forged.extend_from_slice(&u32::MAX.to_le_bytes());
    forged.push(MigrationRecordKind::String as u8);
    let err = parse_migration_payload(&forged).unwrap_err();
    assert!(
      matches!(err, Error::RecordCountExceedsPayload(u32::MAX, 1)),
      "巨值计数须循环前具名违约，不 abort: {err:?}"
    );

    let mut tight = Vec::new();
    tight.extend_from_slice(&2u32.to_le_bytes());
    tight.extend_from_slice(&[
      MigrationRecordKind::String as u8,
      MigrationRecordKind::Envelope as u8,
    ]);
    let err = parse_migration_payload(&tight).unwrap_err();
    assert!(
      matches!(err, Error::InvalidRecord(_)),
      "计数=可证下界时放行入循环，由逐帧截断收口: {err:?}"
    );
  }

  /// RangeIndex 分块流帧（含流元三元组金字节）与向量集帧的编解码往返
  #[test]
  fn range_index_and_vector_frames_roundtrip() {
    let mut buf = Vec::new();
    encode_range_index_stream_payload_into(b"chunk", 5, i64::MAX, 1234, &mut buf);
    // 帧体金字节：kind 后紧跟 17B 流元（obj_type + next_expiry + expire），
    // 再 [u32 LE len][chunk]
    let mut golden = Vec::new();
    golden.extend_from_slice(&1u32.to_le_bytes());
    golden.push(MigrationRecordKind::RangeIndex as u8);
    golden.push(5);
    golden.extend_from_slice(&i64::MAX.to_le_bytes());
    golden.extend_from_slice(&1234i64.to_le_bytes());
    golden.extend_from_slice(&5u32.to_le_bytes());
    golden.extend_from_slice(b"chunk");
    assert_eq!(buf, golden);
    let (count, frames) = parse_migration_payload(&buf).unwrap();
    assert_eq!(count, 1);
    assert_eq!(
      frames[0],
      MigrationFrame::RangeIndexStream {
        obj_type: 5,
        next_expiry: i64::MAX,
        expire_unix_ms: 1234,
        bytes: b"chunk"
      }
    );
    // 流元段截断拒绝
    let mut short = golden.clone();
    short.truncate(golden.len() - 3);
    assert!(parse_migration_payload(&short).is_err());

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

  /// 跨域扩展帧（kind=7 落域上下文 / kind=8 DbMeta 映射）编解码往返与
  /// 域上下文前插：前插后帧数 +1、首帧即上下文帧、原帧字节原样保留
  #[test]
  fn domain_context_and_dbmeta_frames_roundtrip() {
    let ctx = MigrationDomainContext {
      vns: 3,
      vdb: 9,
      ns: 7,
      db: 2,
    };
    let ctx_frame = ctx.encode_frame().to_vec();
    let meta_frame = encode_dbmeta_frame(&[0x01, 1, 2, 3, 4, 5, 6, 7, 8], &9u64.to_be_bytes());

    let payload = encode_frame_payload(&[ctx_frame.clone(), meta_frame]);
    let (count, frames) = parse_migration_payload(&payload).unwrap();
    assert_eq!(count, 2);
    assert_eq!(frames[0], MigrationFrame::DomainContext(ctx));
    assert_eq!(
      frames[1],
      MigrationFrame::DbMeta {
        key: &[0x01, 1, 2, 3, 4, 5, 6, 7, 8],
        value: &9u64.to_be_bytes()
      }
    );

    // 前插：原载荷（单记录帧）帧数头 +1，首帧为上下文帧，其余字节不动
    let item = BatchItem {
      key: b"ka",
      val: MigrateVal::Str(b"vv".to_vec()),
      expire_unix_ms: 5,
    };
    let payload = encode_migration_payload(&[item]);
    let prepended = prepend_migration_frame(&ctx_frame, &payload);
    let (count, frames) = parse_migration_payload(&prepended).unwrap();
    assert_eq!(count, 2);
    assert_eq!(frames[0], MigrationFrame::DomainContext(ctx));
    assert_eq!(&prepended[4 + ctx_frame.len()..], &payload[4..]);

    // 帧体截断拒绝
    let short = [MigrationRecordKind::DomainContext as u8, 0, 0, 0];
    let mut bad = Vec::new();
    bad.extend_from_slice(&1u32.to_le_bytes());
    bad.extend_from_slice(&short);
    assert!(parse_migration_payload(&bad).is_err());
  }
}
