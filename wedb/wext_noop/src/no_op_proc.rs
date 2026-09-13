/// 表示一个无操作的过程
pub struct NoOpProc;

impl NoOpProc {
  /// 执行操作
  /// 在 garnet 中的相对路径:garnet/modules/NoOpModule/NoOpProc.cs:Execute
  /// 满足 Garnet 存储过程执行接口规范，保留形参以匹配固定签名
  pub fn execute(&self, _input: &(), _output: &(), _state: &mut ()) -> bool {
    true
  }
}
