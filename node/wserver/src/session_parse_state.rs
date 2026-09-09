use std::{cmp::max, mem::size_of, ptr::null};

use crate::arg_slice::ArgSlice;

/// garnet相对路径:garnet/libs/server/Resp/Parser/SessionParseState.cs:SessionParseState
#[derive(Debug, Clone)]
pub struct SessionParseState {
  pub count: usize,
  pub root_buffer: Vec<ArgSlice>,
  pub offset: usize,
}

impl Default for SessionParseState {
  fn default() -> Self {
    Self::new()
  }
}

impl SessionParseState {
  pub const MIN_PARAMS: usize = 5;

  #[inline]
  pub fn new() -> Self {
    Self {
      count: 0,
      root_buffer: Vec::with_capacity(Self::MIN_PARAMS),
      offset: 0,
    }
  }

  #[inline]
  pub fn initialize(&mut self, count: usize) {
    self.count = count;
    self.offset = 0;
    let cap = max(count, Self::MIN_PARAMS);
    self.root_buffer.resize(cap, ArgSlice::new(null(), 0));
  }

  #[inline]
  pub fn initialize_with_arg(&mut self, arg: ArgSlice) {
    self.initialize(1);
    self.root_buffer[0] = arg;
  }

  #[inline]
  pub fn initialize_with_args(&mut self, args: &[ArgSlice]) {
    self.initialize(args.len());
    for (i, &arg) in args.iter().enumerate() {
      self.root_buffer[i] = arg;
    }
  }

  #[inline]
  pub fn slice(&self, idx_offset: usize) -> Self {
    debug_assert!(idx_offset <= self.root_buffer.len());
    Self {
      count: self.count.saturating_sub(idx_offset),
      root_buffer: self.root_buffer.clone(),
      offset: self.offset + idx_offset,
    }
  }

  #[inline]
  pub fn slice_len(&self, idx_offset: usize, count: usize) -> Self {
    Self {
      count,
      root_buffer: self.root_buffer.clone(),
      offset: self.offset + idx_offset,
    }
  }

  #[inline]
  pub fn get_arg_slice_by_ref(&self, i: usize) -> ArgSlice {
    debug_assert!(i < self.count);
    self.root_buffer[self.offset + i]
  }

  pub fn get_serialized_length(&self) -> usize {
    let mut len = size_of::<i32>();
    for i in 0..self.count {
      len += self.root_buffer[self.offset + i].total_size();
    }
    len
  }

  pub unsafe fn serialize_to(&self, dest: *mut u8, _length: usize) -> usize {
    unsafe {
      let mut curr = dest;
      *(curr as *mut i32) = self.count as i32;
      curr = curr.add(4);

      for i in 0..self.count {
        let arg = &self.root_buffer[self.offset + i];
        arg.serialize_to(curr);
        curr = curr.add(arg.total_size());
      }
      (curr as usize) - (dest as usize)
    }
  }

  pub unsafe fn deserialize_from(&mut self, src: *const u8) -> usize {
    unsafe {
      let mut curr = src;
      let arg_count = *(curr as *const i32) as usize;
      curr = curr.add(4);

      self.initialize(arg_count);

      for i in 0..arg_count {
        let arg = ArgSlice::from_length_prefixed_ptr(curr);
        self.root_buffer[i] = arg;
        curr = curr.add(arg.total_size());
      }

      (curr as usize) - (src as usize)
    }
  }
}
