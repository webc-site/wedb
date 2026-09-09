use std::{mem::size_of, ptr::read_unaligned};

use crate::types::{AofEntryType, AofHeader};

/// libs/server/AOF/AofProcessor.cs:AofProcessor
pub struct AofProcessor {
  pub current_address: i64,
}

impl AofProcessor {
  pub fn new() -> Self {
    Self { current_address: 0 }
  }

  /// libs/server/AOF/AofProcessor.cs:ProcessAofRecord
  ///
  /// type 字节来自磁盘不可信输入：C# 侧枚举强转任意字节均合法，Rust 端
  /// transmute 出越界判别值属即时 UB，必须经 [`AofEntryType::from_u8`]
  /// 校验解码，未知类型按 no-op 跳过
  pub fn process_aof_record(&mut self, header: &AofHeader, record: &[u8]) {
    if let Some(
      AofEntryType::MainStoreTxn
      | AofEntryType::ObjectStoreTxn
      | AofEntryType::MainStoreStoreCommand
      | AofEntryType::ObjectStoreStoreCommand,
    ) = AofEntryType::from_u8(header.type_)
    {
      self.current_address += record.len() as i64;
    }
  }
}

impl Default for AofProcessor {
  fn default() -> Self {
    Self::new()
  }
}

/// libs/server/AOF/AofProcessor.ChunkReplay.cs:AofProcessor
impl AofProcessor {
  pub fn process_chunk(&mut self, chunk: &[u8]) {
    let header_size = size_of::<AofHeader>();
    if chunk.len() < header_size {
      return;
    }

    // AofHeader 为 repr(C, packed)，须按非对齐读取
    let header = unsafe { read_unaligned(chunk.as_ptr() as *const AofHeader) };
    self.process_aof_record(&header, &chunk[header_size..]);
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn unknown_type_byte_is_noop_not_ub() {
    // 6..=255 均非合法 AofEntryType：原 transmute 实现此路径为即时 UB
    for type_ in 6..=255u8 {
      let mut p = AofProcessor::new();
      let header = AofHeader {
        op_type: 0,
        session_id: 0,
        type_,
      };
      p.process_aof_record(&header, b"payload");
      let mut buf = vec![0u8; size_of::<AofHeader>()];
      buf[size_of::<AofHeader>() - 1] = type_;
      buf.extend_from_slice(b"payload");
      p.process_chunk(&buf);
      assert_eq!(p.current_address, 0);
    }
  }

  #[test]
  fn known_types_advance_address() {
    for type_ in [1u8, 2, 4, 5] {
      let mut p = AofProcessor::new();
      p.process_aof_record(
        &AofHeader {
          op_type: 0,
          session_id: 0,
          type_,
        },
        b"12345",
      );
      assert_eq!(p.current_address, 5);
    }
    for type_ in [0u8, 3] {
      let mut p = AofProcessor::new();
      p.process_aof_record(
        &AofHeader {
          op_type: 0,
          session_id: 0,
          type_,
        },
        b"12345",
      );
      assert_eq!(p.current_address, 0);
    }
  }
}
