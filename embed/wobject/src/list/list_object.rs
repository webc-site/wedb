use std::{
  collections::VecDeque,
  io::{Read, Write},
};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ListOperation {
  Lpop = 0,
  Lpush = 1,
  Lpushx = 2,
  Rpop = 3,
  Rpush = 4,
  Rpushx = 5,
  Llen = 6,
  Ltrim = 7,
  Lrange = 8,
  Lindex = 9,
  Linsert = 10,
  Lrem = 11,
  Rpoplpush = 12,
  Lmove = 13,
  Lset = 14,
  Brpop = 15,
  Blpop = 16,
  Lpos = 17,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum OperationDirection {
  Left = 0,
  Right = 1,
  Unknown = 2,
}

/// garnet相对路径:garnet/libs/server/Objects/List/ListObject.cs:ListObject
pub struct ListObject {
  // Using VecDeque instead of LinkedList for better cache locality and performance
  pub list: parking_lot::Mutex<VecDeque<Vec<u8>>>,
}

impl ListObject {
  pub fn new() -> Self {
    Self {
      list: parking_lot::Mutex::new(VecDeque::new()),
    }
  }

  /// garnet相对路径:garnet/libs/server/Objects/List/ListObject.cs:ListObject(BinaryReader)
  pub fn deserialize<R: Read>(reader: &mut R) -> std::io::Result<Self> {
    let count = reader.read_i32::<LittleEndian>()?;
    let mut list = VecDeque::with_capacity(count as usize);
    for _ in 0..count {
      let item_len = reader.read_i32::<LittleEndian>()?;
      let mut item = vec![0u8; item_len as usize];
      reader.read_exact(&mut item)?;
      list.push_back(item);
    }

    Ok(Self {
      list: parking_lot::Mutex::new(list),
    })
  }

  /// garnet相对路径:garnet/libs/server/Objects/List/ListObject.cs:Serialize
  pub fn serialize<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
    let list = self.list.lock();
    writer.write_i32::<LittleEndian>(list.len() as i32)?;
    for item in list.iter() {
      writer.write_i32::<LittleEndian>(item.len() as i32)?;
      writer.write_all(item)?;
    }
    Ok(())
  }

  /// garnet相对路径:garnet/libs/server/Objects/List/ListObject.cs:Operate
  pub fn operate(&self, op: ListOperation, item: &[u8]) -> Option<Vec<u8>> {
    let mut list = self.list.lock();
    match op {
      ListOperation::Lpush => {
        list.push_front(item.to_vec());
        None
      }
      ListOperation::Rpush => {
        list.push_back(item.to_vec());
        None
      }
      ListOperation::Lpop => list.pop_front(),
      ListOperation::Rpop => list.pop_back(),
      _ => None,
    }
  }

  /// garnet相对路径:garnet/libs/server/Objects/List/ListObject.cs:ListIndex
  pub fn index(&self, index: isize) -> Option<Vec<u8>> {
    let list = self.list.lock();
    let len = list.len() as isize;
    let actual_idx = if index < 0 { len + index } else { index };
    if actual_idx < 0 || actual_idx >= len {
      None
    } else {
      list.get(actual_idx as usize).cloned()
    }
  }

  /// garnet相对路径:garnet/libs/server/Objects/List/ListObject.cs:ListRange
  pub fn range(&self, start: isize, stop: isize) -> Vec<Vec<u8>> {
    let list = self.list.lock();
    let len = list.len() as isize;
    let mut s = if start < 0 { len + start } else { start };
    let mut e = if stop < 0 { len + stop } else { stop };

    if s < 0 {
      s = 0;
    }
    if e >= len {
      e = len - 1;
    }
    if s > e || s >= len {
      return vec![];
    }

    list.range((s as usize)..=(e as usize)).cloned().collect()
  }

  /// garnet相对路径:garnet/libs/server/Objects/List/ListObject.cs:ListTrim
  pub fn trim(&self, start: isize, stop: isize) {
    let mut list = self.list.lock();
    let len = list.len() as isize;
    let mut s = if start < 0 { len + start } else { start };
    let mut e = if stop < 0 { len + stop } else { stop };

    if s < 0 {
      s = 0;
    }
    if e >= len {
      e = len - 1;
    }
    if s > e || s >= len {
      list.clear();
      return;
    }

    // Truncate from end first, then from start
    list.truncate((e + 1) as usize);
    for _ in 0..s {
      list.pop_front();
    }
  }

  /// garnet相对路径:garnet/libs/server/Objects/List/ListObject.cs:Count
  pub fn count(&self) -> usize {
    self.list.lock().len()
  }
}

impl Default for ListObject {
  fn default() -> Self {
    Self::new()
  }
}
