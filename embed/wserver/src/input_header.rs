use coarsetime::Clock;

use crate::types::{GarnetObjectType, RespCommand, RespInputFlags};

/// garnet相对路径:garnet/libs/server/InputHeader.cs:RespInputHeader
/// Header for RESP inputs. Occupies 3 bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RespInputHeader {
  pub data: [u8; 3],
}

impl RespInputHeader {
  pub const SIZE: usize = 3;

  #[inline]
  pub fn new_with_cmd(cmd: RespCommand, flags: RespInputFlags) -> Self {
    let cmd_val: u16 = cmd.into();
    let mut data = [0; 3];
    data[0..2].copy_from_slice(&cmd_val.to_le_bytes());
    data[2] = flags.bits();
    Self { data }
  }

  #[inline]
  pub fn new_with_type(obj_type: GarnetObjectType, flags: RespInputFlags) -> Self {
    let mut data = [0; 3];
    data[0] = obj_type as u8;
    data[2] = flags.bits();
    Self { data }
  }

  #[inline]
  pub fn set_header(&mut self, cmd: u16, flags: u8) {
    self.data[0..2].copy_from_slice(&cmd.to_le_bytes());
    self.data[2] = flags;
  }

  #[inline]
  pub fn sub_id(&self) -> u8 {
    self.data[1]
  }

  #[inline]
  pub fn set_sub_id(&mut self, sub_id: u8) {
    self.data[1] = sub_id;
  }

  #[inline]
  pub fn set_expired_flag(&mut self) {
    self.data[2] |= RespInputFlags::EXPIRED.bits();
  }

  #[inline]
  pub fn set_set_get_flag(&mut self) {
    self.data[2] |= RespInputFlags::SET_GET.bits();
  }

  /// garnet相对路径:garnet/libs/server/InputHeader.cs:CheckExpiry
  #[inline]
  pub fn check_expiry(&self, expire_time: i64) -> bool {
    let flags = RespInputFlags::from_bits_truncate(self.data[2]);
    if flags.contains(RespInputFlags::DETERMINISTIC) {
      flags.contains(RespInputFlags::EXPIRED)
    } else {
      // C# DateTimeOffset.Now.UtcTicks offset from Unix epoch is 62135596800000 ms
      let now_ms = Clock::now_since_epoch().as_millis() as i64;
      let now_ticks = (now_ms + 62_135_596_800_000) * 10000;
      expire_time < now_ticks
    }
  }

  /// garnet相对路径:garnet/libs/server/InputHeader.cs:CheckSetGetFlag
  #[inline]
  pub fn check_set_get_flag(&self) -> bool {
    let flags = RespInputFlags::from_bits_truncate(self.data[2]);
    flags.contains(RespInputFlags::SET_GET)
  }
}
