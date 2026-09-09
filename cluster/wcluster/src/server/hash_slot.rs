use strum::{EnumString, FromRepr, IntoStaticStr};

/// garnet相对路径:Server:SlotState
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, FromRepr, EnumString, IntoStaticStr)]
#[repr(u8)]
pub enum SlotState {
  /// Slot not assigned
  #[default]
  Offline = 0x0,
  /// Slot assigned and ready to be used.
  Stable = 0x1,
  /// Slot is being moved to another node.
  Migrating = 0x2,
  /// Reverse of migrating, preparing node to receive commands for that slot.
  Importing = 0x3,
  /// Slot in FAIL state.
  Fail = 0x4,
  /// Not a slot state. Used with SETSLOT
  Node = 0x5,
  /// Invalid slot state
  Invalid = 0x6,
}

/// garnet相对路径:Server:HashSlot
#[derive(Debug, Clone, Copy, Default)]
pub struct HashSlot {
  pub worker_id: u16,
  pub state: SlotState,
}

impl HashSlot {
  /// Slot in migrating state points to target node though still owned by local node until migration completes.
  /// garnet相对路径:Server:HashSlot:workerId
  #[inline]
  pub fn eff_worker_id(&self) -> u16 {
    if self.state == SlotState::Migrating {
      1
    } else {
      self.worker_id
    }
  }
}
