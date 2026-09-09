use std::{mem::size_of, ptr::copy_nonoverlapping};

use crate::{
  input_header::RespInputHeader,
  session_parse_state::SessionParseState,
  types::{RespCommand, RespInputFlags},
};

/// 各 Input 类型统一的序列化布局 `[3B header][arg1][parse_state]` 的公共骨架
///
/// # Safety
/// - `dest` 须指向至少 `length` 字节的可写缓冲，且 `length ≥ serialized_length()`；
/// - `parse_state` 内 ArgSlice 的裸指针须在本调用期间保持解引用有效
unsafe fn write_input_layout(
  header: &[u8; RespInputHeader::SIZE],
  arg1: i64,
  parse_state: &SessionParseState,
  dest: *mut u8,
  length: usize,
) -> usize {
  unsafe {
    let mut curr = dest;

    copy_nonoverlapping(header.as_ptr(), curr, RespInputHeader::SIZE);
    curr = curr.add(RespInputHeader::SIZE);

    *(curr as *mut i64) = arg1;
    curr = curr.add(size_of::<i64>());

    let len = parse_state.serialize_to(curr, length - ((curr as usize) - (dest as usize)));
    curr = curr.add(len);

    (curr as usize) - (dest as usize)
  }
}

/// [`write_input_layout`] 的逆操作：从 `src` 反序列化布局并回填字段
///
/// # Safety
/// `src` 须指向一段由 `write_input_layout` 产出的完整布局前缀
/// （可读字节数不小于布局总长），否则为越界读
unsafe fn read_input_layout(
  header: &mut [u8; RespInputHeader::SIZE],
  arg1: &mut i64,
  parse_state: &mut SessionParseState,
  src: *const u8,
) -> usize {
  unsafe {
    let mut curr = src;

    copy_nonoverlapping(curr, header.as_mut_ptr(), RespInputHeader::SIZE);
    curr = curr.add(RespInputHeader::SIZE);

    *arg1 = *(curr as *const i64);
    curr = curr.add(size_of::<i64>());

    let len = parse_state.deserialize_from(curr);
    curr = curr.add(len);

    (curr as usize) - (src as usize)
  }
}

/// garnet相对路径:garnet/libs/server/InputHeader.cs:ObjectInput
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
    parse_state: &mut SessionParseState,
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
    parse_state: &mut SessionParseState,
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

  pub fn serialized_length(&self) -> usize {
    RespInputHeader::SIZE + (2 * size_of::<i32>()) + self.parse_state.get_serialized_length()
  }

  /// # Safety
  /// 见 [`write_input_layout`]；`length` 另须不小于 [`Self::serialized_length`]，
  /// ArgSlice 指针须在调用期间有效
  pub unsafe fn copy_to(&self, dest: *mut u8, length: usize) -> usize {
    unsafe {
      debug_assert!(length >= self.serialized_length());
      let mut curr = dest;

      copy_nonoverlapping(self.header.data.as_ptr(), curr, RespInputHeader::SIZE);
      curr = curr.add(RespInputHeader::SIZE);

      *(curr as *mut i32) = self.arg1;
      curr = curr.add(4);

      *(curr as *mut i32) = self.arg2;
      curr = curr.add(4);

      let remaining = length - ((curr as usize) - (dest as usize));
      let len = self.parse_state.serialize_to(curr, remaining);
      curr = curr.add(len);

      (curr as usize) - (dest as usize)
    }
  }

  /// # Safety
  /// 见 [`read_input_layout`]；`src` 布局另含 arg2（i32），须一并可读
  pub unsafe fn deserialize_from(&mut self, src: *const u8) -> usize {
    unsafe {
      let mut curr = src;

      copy_nonoverlapping(curr, self.header.data.as_mut_ptr(), RespInputHeader::SIZE);
      curr = curr.add(RespInputHeader::SIZE);

      self.arg1 = *(curr as *const i32);
      curr = curr.add(4);

      self.arg2 = *(curr as *const i32);
      curr = curr.add(4);

      let len = self.parse_state.deserialize_from(curr);
      curr = curr.add(len);

      (curr as usize) - (src as usize)
    }
  }
}

/// garnet相对路径:garnet/libs/server/InputHeader.cs:StringInput
#[derive(Debug, Clone)]
pub struct StringInput {
  pub header: RespInputHeader,
  pub arg1: i64,
  pub parse_state: SessionParseState,
}

impl StringInput {
  pub fn new(cmd: RespCommand, flags: RespInputFlags, arg1: i64) -> Self {
    Self {
      header: RespInputHeader::new_with_cmd(cmd, flags),
      arg1,
      parse_state: SessionParseState::new(),
    }
  }

  pub fn new_with_state(
    cmd: RespCommand,
    parse_state: &mut SessionParseState,
    arg1: i64,
    flags: RespInputFlags,
  ) -> Self {
    Self {
      header: RespInputHeader::new_with_cmd(cmd, flags),
      arg1,
      parse_state: parse_state.clone(),
    }
  }

  pub fn new_with_state_offset(
    cmd: RespCommand,
    parse_state: &mut SessionParseState,
    start_idx: usize,
    arg1: i64,
    flags: RespInputFlags,
  ) -> Self {
    Self {
      header: RespInputHeader::new_with_cmd(cmd, flags),
      arg1,
      parse_state: parse_state.slice(start_idx),
    }
  }

  pub fn serialized_length(&self) -> usize {
    RespInputHeader::SIZE + size_of::<i64>() + self.parse_state.get_serialized_length()
  }

  /// # Safety
  /// 见 [`write_input_layout`]；`length` 另须不小于 [`Self::serialized_length`]
  pub unsafe fn copy_to(&self, dest: *mut u8, length: usize) -> usize {
    unsafe {
      debug_assert!(length >= self.serialized_length());
      write_input_layout(
        &self.header.data,
        self.arg1,
        &self.parse_state,
        dest,
        length,
      )
    }
  }

  /// # Safety
  /// 见 [`read_input_layout`]
  pub unsafe fn deserialize_from(&mut self, src: *const u8) -> usize {
    unsafe {
      read_input_layout(
        &mut self.header.data,
        &mut self.arg1,
        &mut self.parse_state,
        src,
      )
    }
  }
}

/// garnet相对路径:garnet/libs/server/InputHeader.cs:UnifiedInput
#[derive(Debug, Clone)]
pub struct UnifiedInput {
  pub header: RespInputHeader,
  pub arg1: i64,
  pub parse_state: SessionParseState,
}

impl UnifiedInput {
  pub fn new(cmd: RespCommand, flags: RespInputFlags, arg1: i64) -> Self {
    Self {
      header: RespInputHeader::new_with_cmd(cmd, flags),
      arg1,
      parse_state: SessionParseState::new(),
    }
  }

  pub fn new_with_state(
    cmd: RespCommand,
    parse_state: &mut SessionParseState,
    arg1: i64,
    flags: RespInputFlags,
  ) -> Self {
    Self {
      header: RespInputHeader::new_with_cmd(cmd, flags),
      arg1,
      parse_state: parse_state.clone(),
    }
  }

  pub fn new_with_state_offset(
    cmd: RespCommand,
    parse_state: &mut SessionParseState,
    start_idx: usize,
    arg1: i64,
    flags: RespInputFlags,
  ) -> Self {
    Self {
      header: RespInputHeader::new_with_cmd(cmd, flags),
      arg1,
      parse_state: parse_state.slice(start_idx),
    }
  }

  pub fn serialized_length(&self) -> usize {
    RespInputHeader::SIZE + size_of::<i64>() + self.parse_state.get_serialized_length()
  }

  /// # Safety
  /// 见 [`write_input_layout`]；`length` 另须不小于 [`Self::serialized_length`]
  pub unsafe fn copy_to(&self, dest: *mut u8, length: usize) -> usize {
    unsafe {
      debug_assert!(length >= self.serialized_length());
      write_input_layout(
        &self.header.data,
        self.arg1,
        &self.parse_state,
        dest,
        length,
      )
    }
  }

  /// # Safety
  /// 见 [`read_input_layout`]
  pub unsafe fn deserialize_from(&mut self, src: *const u8) -> usize {
    unsafe {
      read_input_layout(
        &mut self.header.data,
        &mut self.arg1,
        &mut self.parse_state,
        src,
      )
    }
  }
}

/// garnet相对路径:garnet/libs/server/InputHeader.cs:CustomProcedureInput
#[derive(Debug, Clone)]
pub struct CustomProcedureInput {
  pub parse_state: SessionParseState,
  pub resp_version: u8,
}

impl CustomProcedureInput {
  pub fn new(parse_state: &mut SessionParseState, resp_version: u8) -> Self {
    Self {
      parse_state: parse_state.clone(),
      resp_version,
    }
  }

  pub fn new_with_state_offset(
    parse_state: &mut SessionParseState,
    start_idx: usize,
    resp_version: u8,
  ) -> Self {
    Self {
      parse_state: parse_state.slice(start_idx),
      resp_version,
    }
  }

  pub fn serialized_length(&self) -> usize {
    self.parse_state.get_serialized_length()
  }

  /// # Safety
  /// 见 [`SessionParseState::serialize_to`]；`length` 另须不小于
  /// [`Self::serialized_length`]
  pub unsafe fn copy_to(&self, dest: *mut u8, length: usize) -> usize {
    unsafe { self.parse_state.serialize_to(dest, length) }
  }

  /// # Safety
  /// 见 [`SessionParseState::deserialize_from`]
  pub unsafe fn deserialize_from(&mut self, src: *const u8) -> usize {
    unsafe { self.parse_state.deserialize_from(src) }
  }
}

/// garnet相对路径:garnet/libs/server/InputHeader.cs:VectorInput
#[derive(Debug, Clone, Default)]
pub struct VectorInput {
  pub read_desired_size: i32,
  pub write_desired_size: i32,
  pub index: i32,
  pub callback_context: isize,
  pub callback: isize,
  pub alignment_expected: bool,
  pub max_migration_heap_allocation_size: Option<i32>,
}

impl VectorInput {
  pub fn is_migration_read(&self) -> bool {
    self.max_migration_heap_allocation_size.is_some()
  }

  pub fn serialized_length(&self) -> usize {
    unimplemented!()
  }

  /// # Safety
  /// 未实现；接线时须满足 [`write_input_layout`] 的前缀条件
  pub unsafe fn copy_to(&self, _dest: *mut u8, _length: usize) -> usize {
    unimplemented!()
  }

  /// # Safety
  /// 未实现；接线时须满足 [`read_input_layout`] 的前缀条件
  pub unsafe fn deserialize_from(&mut self, _src: *const u8) -> usize {
    unimplemented!()
  }
}
