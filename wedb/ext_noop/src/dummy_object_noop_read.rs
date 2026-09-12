use crate::dummy_object::DummyObject;

/// 表示虚拟对象上的无操作读取
pub struct DummyObjectNoOpRead;

impl DummyObjectNoOpRead {
  /// 读取操作
  /// 在 garnet 中的相对路径:garnet/modules/NoOpModule/DummyObjectNoOpRead.cs:Reader
  /// 满足 Garnet 扩展模块读取回调规范，保留形参以匹配固定函数签名
  pub fn reader(&self, _key: &[u8], _input: &[u8], _value: &DummyObject) -> bool {
    true
  }
}
