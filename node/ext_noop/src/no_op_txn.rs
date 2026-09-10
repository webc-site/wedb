/// 表示一个无操作的事务
pub struct NoOpTxn;

impl NoOpTxn {
  /// 准备操作
  /// 在 garnet 中的相对路径:garnet/modules/NoOpModule/NoOpTxn.cs:Prepare
  pub fn prepare(&self, _api: &(), _proc_input: &()) -> bool {
    true
  }

  /// 主操作
  /// 在 garnet 中的相对路径:garnet/modules/NoOpModule/NoOpTxn.cs:Main
  pub fn main(&self, _api: &(), _proc_input: &(), _output: &mut ()) {}
}
