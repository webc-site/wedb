use std::{collections::VecDeque, sync::RwLock};

/// libs/server/Objects/List/ListObject.cs:ListObject
pub struct ListObject {
  pub list: RwLock<VecDeque<Vec<u8>>>,
}

impl ListObject {
  pub fn new() -> Self {
    Self {
      list: RwLock::new(VecDeque::new()),
    }
  }
}

impl Default for ListObject {
  fn default() -> Self {
    Self::new()
  }
}
