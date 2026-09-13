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
    let next_start = current.checked_add(1)?;
    self
      .items
      .get(next_start..)?
      .iter()
      .position(|x| x.is_some())
      .map(|offset| next_start + offset)
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

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_expandable_map_basic() {
    let mut map: ExpandableMap<&'static str> = ExpandableMap::new();
    assert_eq!(map.try_get_first_id(), None);
    assert_eq!(map.try_get_next_id(0), None);

    assert!(map.try_set_value(2, "two"));
    assert_eq!(map.try_get_first_id(), Some(2));
    assert_eq!(map.try_get_next_id(0), Some(2));
    assert_eq!(map.try_get_next_id(1), Some(2));
    assert_eq!(map.try_get_next_id(2), None);

    assert!(map.try_set_value(5, "five"));
    assert_eq!(map.try_get_next_id(2), Some(5));
    assert_eq!(map.try_get_next_id(5), None);
    assert_eq!(map.try_get_next_id(usize::MAX), None);
  }
}
