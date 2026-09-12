use crate::dummy_object::DummyObject;

/// 表示虚拟对象上的无操作 RMW（读-改-写）
pub struct DummyObjectNoOpRmw;

impl DummyObjectNoOpRmw {
  /// 是否需要初始更新
  /// 在 garnet 中的相对路径:garnet/modules/NoOpModule/DummyObjectNoOpRMW.cs:NeedInitialUpdate
  /// 满足 Garnet 扩展对象 RMW 更新接口规范，保留形参以匹配固定签名
  pub fn need_initial_update(&self, _key: &[u8], _input: &[u8]) -> bool {
    true
  }

  /// 更新操作
  /// 在 garnet 中的相对路径:garnet/modules/NoOpModule/DummyObjectNoOpRMW.cs:Updater
  /// 满足 Garnet 扩展对象 RMW 更新接口规范，保留形参以匹配固定签名
  pub fn updater(&self, _key: &[u8], _input: &[u8], _value: &mut DummyObject) -> bool {
    true
  }
}
