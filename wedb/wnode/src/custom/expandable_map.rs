pub struct ExpandableMap<T> {
  items: Vec<Option<T>>,
}

impl<T> Default for ExpandableMap<T> {
  fn default() -> Self {
    Self::new()
  }
}

impl<T> ExpandableMap<T> {
  pub fn new() -> Self {
    Self { items: Vec::new() }
  }

  /// libs/server/Custom/ExpandableMap.cs:TrySetValueByRef
  pub fn try_set_value_by_ref(&mut self, id: usize, val: T) -> bool {
    self.try_set_value(id, val)
  }

  /// libs/server/Custom/ExpandableMap.cs:TrySetValue
  pub fn try_set_value(&mut self, id: usize, val: T) -> bool {
    if id >= self.items.len() {
      self.items.resize_with(id + 1, || None);
    }
    self.items[id] = Some(val);
    true
  }

  /// libs/server/Custom/ExpandableMap.cs:TryGetFirstId
  pub fn try_get_first_id(&self) -> Option<usize> {
    self.items.iter().position(|x| x.is_some())
  }

  /// libs/server/Custom/ExpandableMap.cs:TryGetNextId
  pub fn try_get_next_id(&self, current: usize) -> Option<usize> {
    if current + 1 >= self.items.len() {
      None
    } else {
      self
        .items
        .iter()
        .enumerate()
        .skip(current + 1)
        .find_map(|(i, x)| {
          // 仅检索有效槽位索引，忽略元素值
          x.as_ref().map(|_val| i)
        })
    }
  }

  /// libs/server/Custom/ExpandableMap.cs:TryUpdateActualSize
  pub fn try_update_actual_size(&mut self, size: usize) -> bool {
    if size > self.items.len() {
      self.items.resize_with(size, || None);
    }
    true
  }

  /// libs/server/Custom/ExpandableMap.cs:TrySetValueUnsafe
  pub fn try_set_value_unsafe(&mut self, id: usize, val: T) -> bool {
    self.try_set_value(id, val)
  }
}
