//! 集合 RESP 输入结构（对标 libs/server/InputHeader.cs）

use std::{mem::size_of, ptr::copy_nonoverlapping};

use bitflags::bitflags;
use coarsetime::Clock;
use wresp::{RespCommand, SessionParseState};
use wval::GarnetObjectType;

bitflags! {
  #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
  pub struct RespInputFlags: u8 {
    const SET_GET = 32;
    const DETERMINISTIC = 64;
    const EXPIRED = 128;
  }
}

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

  #[inline]
  pub fn check_expiry(&self, expire_time: i64) -> bool {
    let flags = RespInputFlags::from_bits_truncate(self.data[2]);
    if flags.contains(RespInputFlags::DETERMINISTIC) {
      flags.contains(RespInputFlags::EXPIRED)
    } else {
      let now_ticks =
        Clock::now_since_epoch().as_millis() as i64 * 10_000 + 621_355_968_000_000_000;
      expire_time < now_ticks
    }
  }

  #[inline]
  pub fn check_set_get_flag(&self) -> bool {
    let flags = RespInputFlags::from_bits_truncate(self.data[2]);
    flags.contains(RespInputFlags::SET_GET)
  }
}

/// 集合操作输入对象
#[derive(Debug, Clone)]
pub struct ObjectInput {
  pub header: RespInputHeader,
  pub arg1: i32,
  pub arg2: i32,
  pub parse_state: SessionParseState,
}

impl ObjectInput {
  pub fn new(header: RespInputHeader, arg1: i32, arg2: i32) -> Self {
    Self {
      header,
      arg1,
      arg2,
      parse_state: SessionParseState::new(),
    }
  }

  pub fn new_with_state(
    header: RespInputHeader,
    parse_state: &SessionParseState,
    arg1: i32,
    arg2: i32,
  ) -> Self {
    Self {
      header,
      arg1,
      arg2,
      parse_state: parse_state.clone(),
    }
  }

  pub fn new_with_state_offset(
    header: RespInputHeader,
    parse_state: &SessionParseState,
    start_idx: usize,
    arg1: i32,
    arg2: i32,
  ) -> Self {
    Self {
      header,
      arg1,
      arg2,
      parse_state: parse_state.slice(start_idx),
    }
  }

  /// 取第 i 个参数切片
  #[inline]
  pub fn arg(&self, i: usize) -> &[u8] {
    self.parse_state.get_arg_slice_by_ref(i).as_slice()
  }

  pub fn serialized_length(&self) -> usize {
    RespInputHeader::SIZE + (2 * size_of::<i32>()) + self.parse_state.get_serialized_length()
  }

  /// # Safety
  /// `length` 不小于 [`Self::serialized_length`]
  pub unsafe fn copy_to(&self, dest: *mut u8, length: usize) -> usize {
    unsafe {
      debug_assert!(length >= self.serialized_length());
      let mut curr = dest;

      copy_nonoverlapping(self.header.data.as_ptr(), curr, RespInputHeader::SIZE);
      curr = curr.add(RespInputHeader::SIZE);

      (curr as *mut i32).write_unaligned(self.arg1);
      curr = curr.add(size_of::<i32>());

      (curr as *mut i32).write_unaligned(self.arg2);
      curr = curr.add(size_of::<i32>());

      let remaining = length - ((curr as usize) - (dest as usize));
      let len = self.parse_state.serialize_to(curr, remaining);
      curr = curr.add(len);

      (curr as usize) - (dest as usize)
    }
  }

  /// # Safety
  /// `src` 须指向一段由 `copy_to` 产出的完整布局前缀
  pub unsafe fn deserialize_from(&mut self, src: *const u8) -> usize {
    unsafe {
      let mut curr = src;

      copy_nonoverlapping(curr, self.header.data.as_mut_ptr(), RespInputHeader::SIZE);
      curr = curr.add(RespInputHeader::SIZE);

      self.arg1 = (curr as *const i32).read_unaligned();
      curr = curr.add(size_of::<i32>());

      self.arg2 = (curr as *const i32).read_unaligned();
      curr = curr.add(size_of::<i32>());

      let len = self.parse_state.deserialize_from(curr);
      curr = curr.add(len);

      (curr as usize) - (src as usize)
    }
  }
}

/// 扫描输入上下文
#[derive(Debug, Clone)]
pub struct ScanInput {
  pub cursor: usize,
  pub pattern: Option<Vec<u8>>,
  pub count: usize,
}
