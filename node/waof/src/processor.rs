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
    let entry_type = unsafe { std::mem::transmute::<u8, AofEntryType>(header.type_) };
    match entry_type {
      AofEntryType::MainStoreTxn | AofEntryType::ObjectStoreTxn => {
        self.current_address += record.len() as i64;
      }
      AofEntryType::MainStoreStoreCommand => {
        self.current_address += record.len() as i64;
      }
      AofEntryType::ObjectStoreStoreCommand => {
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
    if chunk.len() < std::mem::size_of::<AofHeader>() {
      return;
    }

    let mut offset = 0;
    while offset + std::mem::size_of::<AofHeader>() <= chunk.len() {
      let header_bytes = &chunk[offset..offset + std::mem::size_of::<AofHeader>()];
      let header = unsafe { std::ptr::read_unaligned(header_bytes.as_ptr() as *const AofHeader) };
      offset += std::mem::size_of::<AofHeader>();

      self.process_aof_record(&header, &chunk[offset..]);
      break;
    }
  }
}
