//! 分块 AOF 记录重组（对标 libs/server/AOF/AofChunkedRecordReader.cs:
//! ChunkedAccumulator + AofChunkedRecordReader）
//!
//! 大记录（key+value+input 超过最小分配尺寸）写入端按块拆分；本域依
//! AofChunkHeader 声明的全量长度把各块数据流式装回完整组件
//!（key → value → input 顺序），完成后交重放分派（不物化连续记录镜像）。
//!
//! 写侧布局说明：周期 1 的 [`GarnetLog::enqueue_span_chunked`] 以“帧头 +
//! 分块帧头 + 组件数据顺序拼接”落盘（无 [前缀|续位] 段帧，objectId 恒 0）；
//! 本读取器以声明长度驱动组件边界，与该布局闭环，组件顺序与 C# 一致。
//! objectId 映射为并发重组预留（顺序回放下天然成立）。

use gxhash::HashMap;

use super::{
  aof_entry_type::AofEntryType,
  aof_header::{AofChunkHeader, AofHeader, AofHeaderType, AofShardedHeader},
};

/// 分块记录组件顺序（C# ChunkedAccumulator.Component）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Component {
  /// key
  Key,
  /// value
  Value,
  /// input
  Input,
}

/// 分块重组累积器（C# ChunkedAccumulator）。
#[derive(Debug, Clone)]
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
  overflow_key_length: usize,
  overflow_value_length: usize,
  input_length: usize,
  /// 组件在场标志。
  has_value: bool,
  has_input: bool,
  /// 值是否为流式对象（长度未知，累积块列表）。
  is_object_value: bool,
}

impl ChunkedAccumulator {
  /// 依据条目类型判定组件形状并新建累积器（容量预分配对齐 C# 一次性缓冲）。
  pub(crate) fn new(op_type: AofEntryType, chunk_header: &AofChunkHeader) -> Self {
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

  /// 流式对象值块序列（C# GetValueSequence；rust 以块列表承接
  /// ReadOnlySequence，反序列化方逐块流式读取）。
  pub fn get_value_sequence(&self) -> &[Vec<u8>] {
    &self.value_chunks
  }

  /// libs/server/AOF/AofChunkedRecordReader.cs:Verify
  ///
  /// 各组件累积长度与帧头声明一致。
  pub fn verify(&self) -> Result<(), String> {
    if self.key.len() != self.overflow_key_length {
      return Err(format!(
        "Chunked key length mismatch: read {}, header {}",
        self.key.len(),
        self.overflow_key_length
      ));
    }
    if self.has_value && !self.is_object_value && self.value.len() != self.overflow_value_length {
      return Err(format!(
        "Chunked value length mismatch: read {}, header {}",
        self.value.len(),
        self.overflow_value_length
      ));
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
  pub fn first_component(&self) -> Component {
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
  fn all_components_filled(&self) -> bool {
    let key_full = self.key.len() == self.overflow_key_length;
    let value_full = !self.has_value
      || self.value_chunks.iter().map(Vec::len).sum::<usize>() == self.overflow_value_length
      || self.value.len() == self.overflow_value_length;
    let input_full = !self.has_input || self.input.len() == self.input_length;
    key_full && value_full && input_full
  }

  /// 块数据流式装填（C# AppendChunk 的顺序消费形态；组件写入经
  /// CopyInto 等价语义——越界即损坏返回 false）。依声明长度自动跨越
  /// 组件边界；越界数据为损坏
  ///（返回 false，调用方弃置该记录）。
  pub(crate) fn feed(&mut self, mut data: &[u8]) -> bool {
    while !data.is_empty() {
      let (buffer, capacity): (&mut Vec<u8>, usize) = match self.current_component {
        Component::Key => (&mut self.key, self.overflow_key_length),
        Component::Value if self.is_object_value => {
          // 对象值：本段整块并入块列表，完成度由声明全量长度核对
          let filled = self.value_chunks.iter().map(Vec::len).sum::<usize>();
          let declared = self.overflow_value_length;
          let take = declared.saturating_sub(filled).min(data.len());
          self.value_chunks.push(data[..take].to_vec());
          data = &data[take..];
          if data.is_empty() {
            return true;
          }
          if !self.next_component() {
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

/// 分块记录读取器：按 objectId 聚合各块至完成（C# AofChunkedRecordReader）。
#[derive(Default)]
pub struct AofChunkedRecordReader {
  in_progress: HashMap<u64, ChunkedAccumulator>,
}

impl AofChunkedRecordReader {
  /// 新建读取器（每子日志一份）。
  pub fn new() -> Self {
    Self::default()
  }

  /// libs/server/AOF/AofChunkedRecordReader.cs:ReadChunk
  ///
  /// 累积一块；逻辑记录完成时返回已校验的累积器（所有权移交调用方，并自
  /// 进行中映射移除），否则 None。已完成记录的重复块与组件溢出均为损坏。
  ///
  /// 布局对接：周期 1 写端把全部块落在同一子日志且续块为纯数据（无帧头、
  /// objectId 不复现），故续块按顺序并入唯一进行中记录；objectId 映射保留
  /// C# 并发重组形态（当前消费模式下至多一条进行中）。
  pub fn read_chunk(&mut self, entry: &[u8]) -> Option<ChunkedAccumulator> {
    // 续块：并入进行中记录（纯组件数据）
    if !self.in_progress.is_empty() {
      let object_id = *self.in_progress.keys().next()?;
      let acc = self.in_progress.get_mut(&object_id)?;
      if acc.is_complete {
        return None;
      }
      if !acc.feed(entry) {
        self.in_progress.remove(&object_id);
        return None;
      }
      if acc.all_components_filled() && acc.verify().is_ok() {
        let mut acc = acc.clone();
        acc.is_complete = true;
        self.in_progress.remove(&object_id);
        return Some(acc);
      }
      return None;
    }

    // 首块：解析帧头 + 分块帧头，建立累积器
    let header = AofHeader::parse(entry)?;
    let (chunk_header_offset, chunk_header) = AofHeader::get_chunked_header_ref(entry)?;
    let object_id = chunk_header.object_id;
    let op_type = AofEntryType::try_from(header.op_type).ok()?;
    let data_offset = chunk_header_offset + AofChunkHeader::TOTAL_SIZE;
    if entry.len() < data_offset {
      return None;
    }

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

    if !acc.feed(&entry[data_offset..]) {
      return None;
    }
    if acc.all_components_filled() && acc.verify().is_ok() {
      acc.is_complete = true;
      return Some(acc);
    }
    self.in_progress.insert(object_id, acc);
    None
  }

  /// 进行中记录数（测试/诊断面）。
  pub fn in_progress_count(&self) -> usize {
    self.in_progress.len()
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::{
    aof::garnet_log::{ChunkedShape, GarnetLog, InMemorySublog, RecordShape, SublogBackend},
    config::runtime_server_options::RuntimeServerOptions,
  };

  fn chunked_upsert_entry(value_len: usize) -> (GarnetLog, Vec<Vec<u8>>) {
    let options = RuntimeServerOptions::default();
    let backends: Vec<Arc<dyn SublogBackend>> =
      vec![Arc::new(InMemorySublog::new()) as Arc<dyn SublogBackend>];
    let log = GarnetLog::new(&options, backends);
    let value = vec![b'x'; value_len];
    log.enqueue_object_chunked(&ChunkedShape {
      record: RecordShape {
        op_type: AofEntryType::ObjectStoreUpsert,
        version: 3,
        session_id: 9,
        key: b"big",
        value: &value,
        input: &[],
        database_id: 0,
      },
      write_value: true,
      write_input: false,
    });
    let records = log
      .scan_single(0, 1, i64::MAX)
      .into_iter()
      .map(|r| r.payload)
      .collect();
    (log, records)
  }

  use std::sync::Arc;

  #[test]
  fn first_and_next_component_progression() {
    let chunk_header = AofChunkHeader {
      overflow_key_length: 3,
      overflow_value_length: 5,
      input_length: 2,
      object_id: 1,
      key_hash: 7,
    };
    let acc = ChunkedAccumulator::new(AofEntryType::StoreUpsert, &chunk_header);
    assert_eq!(acc.first_component(), Component::Key);
    let mut acc = acc;
    assert!(acc.next_component());
    assert_eq!(acc.current_component, Component::Value);
    assert!(acc.next_component());
    assert_eq!(acc.current_component, Component::Input);
    assert!(!acc.next_component());
    assert!(acc.is_complete);

    // delete 形状：无 value/input
    let del = AofChunkHeader {
      overflow_key_length: 3,
      overflow_value_length: 0,
      input_length: 0,
      object_id: 2,
      key_hash: 8,
    };
    let mut acc = ChunkedAccumulator::new(AofEntryType::StoreDelete, &del);
    assert!(!acc.next_component());
    assert!(acc.is_complete);
  }

  #[test]
  fn feed_splits_across_component_boundaries() {
    let chunk_header = AofChunkHeader {
      overflow_key_length: 2,
      overflow_value_length: 3,
      input_length: 2,
      object_id: 3,
      key_hash: 9,
    };
    let mut acc = ChunkedAccumulator::new(AofEntryType::StoreUpsert, &chunk_header);
    // 单块数据连拼 key+value+input
    assert!(acc.feed(b"ab"));
    assert!(acc.feed(b"cde"));
    assert!(acc.feed(b"fg"));
    assert!(acc.all_components_filled());
    assert_eq!(acc.key_span(), b"ab");
    assert_eq!(acc.value_span(), b"cde");
    assert_eq!(acc.input_span(), b"fg");
    assert!(acc.verify().is_ok());
  }

  #[test]
  fn verify_detects_length_mismatch() {
    let chunk_header = AofChunkHeader {
      overflow_key_length: 4,
      overflow_value_length: 0,
      input_length: 0,
      object_id: 4,
      key_hash: 1,
    };
    let mut acc = ChunkedAccumulator::new(AofEntryType::StoreDelete, &chunk_header);
    assert!(acc.feed(b"ab"));
    assert!(acc.verify().is_err(), "key 未积满应判不一致");
    assert!(acc.feed(b"cd"));
    assert!(acc.verify().is_ok());
    // 越界数据为损坏
    assert!(!acc.feed(b"x"));
  }

  #[test]
  fn read_chunk_reassembles_from_cycle1_writer() {
    let (_log, records) = chunked_upsert_entry(200);
    assert!(records.len() >= 2, "大值拆首块 + 数据块");
    let mut reader = AofChunkedRecordReader::new();
    let mut completed = None;
    for record in &records {
      if let Some(acc) = reader.read_chunk(record) {
        completed = Some(acc);
      }
    }
    let acc = completed.expect("全部块后应完成重组");
    assert_eq!(acc.op_type, AofEntryType::ObjectStoreUpsert);
    assert_eq!(acc.key_span(), b"big");
    let value: Vec<u8> = acc.get_value_sequence().concat();
    assert_eq!(value.len(), 200);
    assert_eq!(acc.session_id, 9);
    assert_eq!(acc.store_version, 3);
    assert!(reader.in_progress_count() == 0);
  }

  #[test]
  fn read_chunk_accumulates_until_complete() {
    let (_log, records) = chunked_upsert_entry(200);
    let mut reader = AofChunkedRecordReader::new();
    // 首块尚不完整
    assert!(reader.read_chunk(&records[0]).is_none());
    assert!(reader.in_progress_count() == 1);
    // 余块闭合
    for record in &records[1..] {
      if let Some(acc) = reader.read_chunk(record) {
        assert_eq!(acc.key_span(), b"big");
        assert!(reader.in_progress_count() == 0);
        return;
      }
    }
    panic!("数据块未能闭合重组");
  }

  #[test]
  fn duplicate_chunk_after_completion_is_corrupt() {
    let (_log, records) = chunked_upsert_entry(200);
    let mut reader = AofChunkedRecordReader::new();
    let mut last = None;
    for record in &records {
      last = reader.read_chunk(record);
    }
    assert!(last.is_some());
    // 首块重放：objectId 相同且已完成 → 损坏返回 None
    assert!(reader.read_chunk(&records[0]).is_none());
  }
}
