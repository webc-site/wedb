use std::collections::VecDeque;

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
