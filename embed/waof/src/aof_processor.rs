/// libs/server/AOF/AofProcessor.cs:AofProcessor
pub struct AofProcessor {}

impl AofProcessor {
  pub fn new() -> Self {
    Self {}
  }
}

impl Default for AofProcessor {
  fn default() -> Self {
    Self::new()
  }
}

/// libs/server/AOF/AofProcessor.ChunkReplay.cs:AofProcessor
impl AofProcessor {
  pub fn process_chunk(&self) {}
}
