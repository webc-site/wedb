/// libs/cluster/Server/Migration/SketchStatus.cs:SketchStatus
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SketchStatus {
  Initializing = 0,
  Transmitting = 1,
  Deleting = 2,
  Migrated = 3,
}
