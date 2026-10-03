//! 分块 AOF 记录重组（对标 libs/server/AOF/AofChunkedRecordReader.cs:
//! ChunkedAccumulator + AofChunkedRecordReader）
//!
//! 大记录（key+value+input 超过最小分配尺寸）写入端按块拆分；本域依
//! AofChunkHeader 声明的全量长度把各块数据流式装回完整组件
//!（key → value → input 顺序），完成后交重放分派（不物化连续记录镜像）。
//!
//! 线协议（对标 C# TsavoriteLog.Chunked.cs:WriteOneRecord）：每个分块帧均
//! 携带完整帧头（帧头 + \[序列号\] + 分块帧头），同一逻辑大记录各帧的
//! op_type / key_hash / object_id 完全一致；本读取器统一按帧头解析每条
//! chunk 记录，以 objectId 在 [`AofChunkedRecordReader::in_progress`] 中
//! 索引累积（C# ReadChunk 同构），组件边界由声明长度驱动，组件顺序与
//! C# 一致。帧组经写端 enqueue_frames 原子入队绝不插花，顺序消费下同
//! objectId 的进行中记录天然不重叠。

use std::{
  borrow::Cow,
  collections::{VecDeque, hash_map::Entry},
};

use waof::{AofChunkHeader, AofEntryType, AofHeader, AofHeaderType, AofShardedHeader};
use wbase::map::{HashMap, HashSet};

/// libs/server/AOF/AofChunkedRecordReader.cs:Component
///
/// 分块记录组件顺序（C# ChunkedAccumulator.Component）。
#[derive(Debug, Copy, PartialEq, Eq, Clone)]
pub enum Component {
  /// key
  Key,
  /// value
  Value,
  /// input
  Input,
}

/// libs/server/AOF/AofChunkedRecordReader.cs:ChunkedAccumulator
///
/// 分块重组累积器（C# ChunkedAccumulator）。
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct ChunkedAccumulator {
  /// 操作类型（取自首块非分块头）。
  pub op_type: AofEntryType,
  /// 重组后的非分块头类型：BasicHeader 或 ShardedHeader。
  pub header_type: AofHeaderType,
  /// 会话 id（事务分组用）。
  pub session_id: i32,
  /// 存储版本（SkipRecord 版本判定）。
  pub store_version: i64,
  /// 分片序列号（Basic/单日志形态为 0）。
  pub sequence_number: i64,
  /// key 哈希（写端盖入分块帧头）。
  pub key_hash: i64,

  /// key 缓冲（容量 = 帧头声明的全量 key 长度）。
  pub key: Vec<u8>,
  /// value 缓冲（溢出 span 值）。
  pub value: Vec<u8>,
  /// 流式对象值块列表（长度未知，累积为块序列）。
  pub value_chunks: Vec<Vec<u8>>,
  /// input 缓冲。
  pub input: Vec<u8>,

  /// 当前累积组件。
  pub current_component: Component,
  /// 全部在场组件已积满。
  pub is_complete: bool,

  /// 帧头声明的全量组件长度（校验/边界判定面）。
  pub overflow_key_length: usize,
  pub overflow_value_length: usize,
  pub input_length: usize,
  /// 累积的对象值字节总长。
  pub value_bytes_len: usize,
  /// 组件在场标志。
  pub has_value: bool,
  pub has_input: bool,
  /// 值是否为流式对象（长度未知，累积块列表）。
  pub is_object_value: bool,
}

impl ChunkedAccumulator {
  /// 依据条目类型判定组件形状并新建累积器（容量预分配对齐 C# 一次性缓冲）。
  pub fn new(op_type: AofEntryType, chunk_header: &AofChunkHeader) -> Self {
    let has_value = op_type.has_chunk_value();
    let has_input = op_type.has_chunk_input();
    let is_object_value = op_type.has_chunk_object_value();
    let mut acc = Self {
      op_type,
      header_type: AofHeaderType::BasicHeader,
      session_id: 0,
      store_version: 0,
      sequence_number: 0,
      key_hash: chunk_header.key_hash,
      key: Vec::with_capacity(chunk_header.overflow_key_length as usize),
      value: Vec::with_capacity(if has_value && !is_object_value {
        chunk_header.overflow_value_length as usize
      } else {
        0
      }),
      value_chunks: Vec::new(),
      input: Vec::with_capacity(if has_input {
        chunk_header.input_length as usize
      } else {
        0
      }),
      current_component: Component::Key,
      is_complete: false,
      overflow_key_length: chunk_header.overflow_key_length as usize,
      overflow_value_length: chunk_header.overflow_value_length as usize,
      input_length: chunk_header.input_length as usize,
      value_bytes_len: 0,
      has_value,
      has_input,
      is_object_value,
    };
    acc.current_component = acc.first_component();
    acc
  }

  /// 重组 key（C# KeySpan）。
  pub fn key_span(&self) -> &[u8] {
    &self.key
  }

  /// 重组 span 值（C# ValueSpan；仅 has_value 且非对象值形态有效）。
  pub fn value_span(&self) -> &[u8] {
    &self.value
  }

  /// 重组 input（C# InputSpan）。
  pub fn input_span(&self) -> &[u8] {
    &self.input
  }

  /// 对象值切片/Cow 视图（单块或非流式零拷贝借用，多块流式按需物化拼接，
  /// 替代 C# ReadOnlySequence 逐块读取）。
  ///
  /// 在 garnet 中的相对路径:libs/server/AOF/AofChunkedRecordReader.cs:GetValueSequence
  pub fn object_value_bytes(&self) -> Cow<'_, [u8]> {
    if self.value_chunks.is_empty() {
      Cow::Borrowed(&self.value)
    } else if self.value_chunks.len() == 1 {
      Cow::Borrowed(&self.value_chunks[0])
    } else {
      Cow::Owned(self.value_chunks.concat())
    }
  }

  /// libs/server/AOF/AofChunkedRecordReader.cs:Verify
  ///
  /// 各组件累积长度与帧头声明一致。
  ///
  /// 与 C# 的形态差：C# `Verify` 对对象值不核长度（其对象值走 `valueChunks`
  /// 逐块挂链、无累积偏移计数可核），rust 流式装填即记账 `value_bytes_len`，
  /// 故本处一并按声明长度核验——写端 `overflow_value_length` 与对象值字节总数
  /// 同源（`enqueue_span_chunked` 单次盖头），无第二真值源，核验不误伤合法帧。
  pub fn verify(&self) -> Result<(), String> {
    if self.key.len() != self.overflow_key_length {
      return Err(format!(
        "Chunked key length mismatch: read {}, header {}",
        self.key.len(),
        self.overflow_key_length
      ));
    }
    if self.has_value {
      let actual = if self.is_object_value {
        self.value_bytes_len
      } else {
        self.value.len()
      };
      if actual != self.overflow_value_length {
        return Err(format!(
          "Chunked value length mismatch: read {}, header {}",
          actual, self.overflow_value_length
        ));
      }
    }
    if self.has_input && self.input.len() != self.input_length {
      return Err(format!(
        "Chunked input length mismatch: read {}, header {}",
        self.input.len(),
        self.input_length
      ));
    }
    Ok(())
  }

  /// libs/server/AOF/AofChunkedRecordReader.cs:FirstComponent
  ///
  /// 首个在场组件（现行全部分块操作带 key）。
  #[inline]
  pub const fn first_component(&self) -> Component {
    if self.overflow_key_length > 0 {
      Component::Key
    } else if self.has_value {
      Component::Value
    } else {
      Component::Input
    }
  }

  /// libs/server/AOF/AofChunkedRecordReader.cs:NextComponent
  ///
  /// 推进到下一在场组件；最后一个组件消费完毕即置完成并返回 false。
  pub fn next_component(&mut self) -> bool {
    if self.current_component == Component::Key && self.has_value {
      self.current_component = Component::Value;
      return true;
    }
    if self.current_component != Component::Input && self.has_input {
      self.current_component = Component::Input;
      return true;
    }
    self.is_complete = true;
    false
  }

  /// 当前组件全部积满（在场组件逐一核验）。
  pub fn all_components_filled(&self) -> bool {
    let key_full = self.key.len() == self.overflow_key_length;
    let value_full = !self.has_value
      || if self.is_object_value {
        self.value_bytes_len == self.overflow_value_length
      } else {
        self.value.len() == self.overflow_value_length
      };
    let input_full = !self.has_input || self.input.len() == self.input_length;
    key_full && value_full && input_full
  }

  /// libs/server/AOF/AofChunkedRecordReader.cs:AppendChunk
  /// libs/server/AOF/AofChunkedRecordReader.cs:CopyInto
  ///
  /// 块数据流式装填：C# AppendChunk 按 currentComponent 消费数据、组件写满
  /// 经 NextComponent 跨界，CopyInto 为组件内的越界核验拷贝内核——rust 将
  /// 两层折叠为本单函数（组件写入即 CopyInto 等价语义，越界即损坏返回
  /// false）。依声明长度自动跨越组件边界；越界数据为损坏
  ///（返回 false，调用方判 ComponentOverflow 上抛，残簿按 C# 形态留簿不淘汰）。
  pub fn feed(&mut self, mut data: &[u8]) -> bool {
    while !data.is_empty() {
      let (buffer, capacity): (&mut Vec<u8>, usize) = match self.current_component {
        Component::Key => (&mut self.key, self.overflow_key_length),
        Component::Value if self.is_object_value => {
          // 对象值：本段整块并入块列表，完成度由声明全量长度核对
          let declared = self.overflow_value_length;
          let remaining = declared.saturating_sub(self.value_bytes_len);
          if remaining == 0 {
            if !self.next_component() {
              return data.is_empty();
            }
            continue;
          }
          let take = remaining.min(data.len());
          self.value_chunks.push(data[..take].to_vec());
          self.value_bytes_len += take;
          data = &data[take..];
          if self.value_bytes_len >= declared && !data.is_empty() && !self.next_component() {
            return false;
          }
          continue;
        }
        Component::Value => (&mut self.value, self.overflow_value_length),
        Component::Input => (&mut self.input, self.input_length),
      };
      let remaining = capacity.saturating_sub(buffer.len());
      if remaining == 0 {
        // 当前组件已满：推进
        if !self.next_component() {
          return data.is_empty();
        }
        continue;
      }
      let take = remaining.min(data.len());
      buffer.extend_from_slice(&data[..take]);
      data = &data[take..];
      if take < remaining && !data.is_empty() {
        return false;
      }
      if buffer.len() >= capacity && !data.is_empty() && !self.next_component() {
        return false;
      }
    }
    true
  }
}

/// 分块 AOF 记录读取损坏错误（逐条对位 C# AofChunkedRecordReader 四道 throw）
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum AofChunkReadError {
  /// 已完成记录的重复块（对标 C# ReadChunk:211 throw GarnetException）
  #[error("已完成分块记录的重复块: object_id={0}")]
  DuplicateChunk(u64),

  /// 段长越界（对标 C# ReadChunk:229 throw）
  #[error("分块记录段长越界: entry_len={entry_len}, required={required}")]
  SegmentOutOfBounds { entry_len: usize, required: usize },

  /// 组件/分块累加溢出（对标 C# CopyInto:273 throw）
  #[error("分块记录组件溢出: object_id={object_id}, component={component:?}")]
  ComponentOverflow {
    object_id: u64,
    component: Component,
  },

  /// verify 长度不符（对标 C# ChunkedAccumulator.Verify:85-90 throw）
  #[error("分块记录验证长度不符: object_id={object_id}, reason={reason}")]
  VerifyMismatch { object_id: u64, reason: String },
}

/// 记忆已完成组 id 的 FIFO 上限容量（现界论证：有界淘汰保障 O(1) 空间占用，
/// 防止长时间运行下 completed_ids 无界增长；残簿本体不强制淘汰与 C# 同形，
/// 因组 id 记录级唯一天然惰性绝不交叉污染）
const MAX_COMPLETED_IDS: usize = 4096;

/// libs/server/AOF/AofChunkedRecordReader.cs:AofChunkedRecordReader
///
/// 分块记录读取器：按 objectId 聚合各块至完成（C# AofChunkedRecordReader）。
#[derive(Default)]
pub struct AofChunkedRecordReader {
  pub in_progress: HashMap<u64, ChunkedAccumulator>,
  completed_ids: HashSet<u64>,
  completed_order: VecDeque<u64>,
}

impl AofChunkedRecordReader {
  /// libs/server/AOF/AofChunkedRecordReader.cs:AofChunkedRecordReader
  ///
  /// 新建读取器（每子日志一份）。
  pub fn new() -> Self {
    Self::default()
  }

  /// 登记已完成组 id（有界 FIFO 淘汰）
  fn record_completed(&mut self, object_id: u64) {
    if self.completed_ids.insert(object_id) {
      self.completed_order.push_back(object_id);
      if self.completed_order.len() > MAX_COMPLETED_IDS
        && let Some(oldest) = self.completed_order.pop_front()
      {
        self.completed_ids.remove(&oldest);
      }
    }
  }

  /// libs/server/AOF/AofChunkedRecordReader.cs:ReadChunk
  ///
  /// 累积一块；逻辑记录完成时返回已校验的累积器（所有权移交调用方，并自
  /// 进行中映射移除），否则 None。
  /// 四道损坏态响亮抛错（逐条对位 C# 四道 throw）：
  /// 1) 已完成记录的重复块（DuplicateChunk，对位 GarnetException）；
  /// 2) 段长越界（SegmentOutOfBounds）；
  /// 3) 组件/分块累加溢出（ComponentOverflow，对位 CopyInto）；
  /// 4) verify 长度不符（VerifyMismatch，对位 Verify）。
  ///
  /// 每条 chunk 记录统一按帧头解析（写端每帧携带完整帧头，续块绝非裸
  /// 数据），以 `chunkHeader.object_id` 在进行中映射索引：未在簿即首块，
  /// 解析组件形状并建累积器；在簿即续块，数据并入既有累积器。
  pub fn read_chunk(
    &mut self,
    entry: &[u8],
  ) -> Result<Option<ChunkedAccumulator>, AofChunkReadError> {
    // 调用方按帧头 is_chunked 分发，进入本读取器者必为分块帧；
    // 帧体不足以容纳帧头即段长越界（对位 C# ReadChunk 段长校验 throw）
    let Some(header) = AofHeader::parse(entry) else {
      return Err(AofChunkReadError::SegmentOutOfBounds {
        entry_len: entry.len(),
        required: AofHeader::TOTAL_SIZE,
      });
    };
    let Some((chunk_header_offset, chunk_header)) = AofHeader::get_chunked_header_ref(entry) else {
      let required = match header.header_type() {
        Some(AofHeaderType::ShardedChunkHeader) => {
          AofShardedHeader::TOTAL_SIZE + AofChunkHeader::TOTAL_SIZE
        }
        _ => AofHeader::TOTAL_SIZE + AofChunkHeader::TOTAL_SIZE,
      };
      return Err(AofChunkReadError::SegmentOutOfBounds {
        entry_len: entry.len(),
        required,
      });
    };
    let object_id = chunk_header.object_id;
    let data_offset = chunk_header_offset + AofChunkHeader::TOTAL_SIZE;
    let Some(data) = entry.get(data_offset..) else {
      return Err(AofChunkReadError::SegmentOutOfBounds {
        entry_len: entry.len(),
        required: data_offset,
      });
    };

    // 1) 已完成记录重复块检测（已出簿组再次收到块，对位 C# :211 throw GarnetException）
    if self.completed_ids.contains(&object_id) {
      return Err(AofChunkReadError::DuplicateChunk(object_id));
    }

    // 首块：解析组件形状并建累积器（非分块头字段只在此解析一次）
    if let Entry::Vacant(e) = self.in_progress.entry(object_id) {
      let Ok(op_type) = AofEntryType::try_from(header.op_type) else {
        return Err(AofChunkReadError::VerifyMismatch {
          object_id,
          reason: format!("Unknown op_type {}", header.op_type),
        });
      };
      let mut acc = ChunkedAccumulator::new(op_type, &chunk_header);
      if header.header_type() == Some(AofHeaderType::ShardedChunkHeader) {
        if let Some(sh) = AofShardedHeader::parse(entry) {
          acc.header_type = AofHeaderType::ShardedHeader;
          acc.session_id = sh.basic.session_id;
          acc.store_version = sh.basic.store_version;
          acc.sequence_number = sh.sequence_number;
        }
      } else {
        acc.session_id = header.session_id;
        acc.store_version = header.store_version;
      }
      e.insert(acc);
    }

    let acc = self
      .in_progress
      .get_mut(&object_id)
      .expect("已建簿或命中在途累积器");
    if acc.is_complete {
      return Err(AofChunkReadError::DuplicateChunk(object_id));
    }

    // 续块核验分块头声明长度一致性
    if chunk_header.overflow_key_length as usize != acc.overflow_key_length
      || chunk_header.overflow_value_length as usize != acc.overflow_value_length
      || chunk_header.input_length as usize != acc.input_length
    {
      return Err(AofChunkReadError::VerifyMismatch {
        object_id,
        reason: format!(
          "分块头声明长度不一致: expected ({}, {}, {}), got ({}, {}, {})",
          acc.overflow_key_length,
          acc.overflow_value_length,
          acc.input_length,
          chunk_header.overflow_key_length,
          chunk_header.overflow_value_length,
          chunk_header.input_length
        ),
      });
    }

    // 3) 组件累加溢出检测（对标 CopyInto:273 throw）
    if !acc.feed(data) {
      return Err(AofChunkReadError::ComponentOverflow {
        object_id,
        component: acc.current_component,
      });
    }

    // 4) 完成时 verify 长度核验（对标 ChunkedAccumulator.Verify:85-90 throw）
    if acc.all_components_filled() {
      if let Err(reason) = acc.verify() {
        return Err(AofChunkReadError::VerifyMismatch { object_id, reason });
      }
      let mut acc = self.in_progress.remove(&object_id).unwrap();
      acc.is_complete = true;
      self.record_completed(object_id);
      return Ok(Some(acc));
    }

    Ok(None)
  }
}
