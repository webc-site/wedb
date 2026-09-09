use crate::dummy_object::DummyObject;

/// 表示虚拟对象上的无操作读取
pub struct DummyObjectNoOpRead;

impl DummyObjectNoOpRead {
  /// 读取操作
  /// garnet相对路径:garnet/modules/NoOpModule/DummyObjectNoOpRead.cs:Reader
  pub fn reader(&self, _key: &[u8], _input: &[u8], _value: &DummyObject) -> bool {
    true
  }
}
