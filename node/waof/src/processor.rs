use std::{
  mem::{size_of, transmute},
  ptr::read_unaligned,
};

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
  pub fn process_aof_record(&mut self, header: &AofHeader, record: &[u8]) {
    let entry_type = unsafe { transmute::<u8, AofEntryType>(header.type_) };
    match entry_type {
      AofEntryType::MainStoreTxn | AofEntryType::ObjectStoreTxn => {
        self.current_address += record.len() as i64;
      }
      AofEntryType::MainStoreStoreCommand | AofEntryType::ObjectStoreStoreCommand => {
        self.current_address += record.len() as i64;
      }
      _ => {}
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

    let mut offset = 0;
    if offset + header_size <= chunk.len() {
      let header_bytes = &chunk[offset..offset + header_size];
      let header = unsafe { read_unaligned(header_bytes.as_ptr() as *const AofHeader) };
      offset += header_size;

      self.process_aof_record(&header, &chunk[offset..]);
    }
  }
}
