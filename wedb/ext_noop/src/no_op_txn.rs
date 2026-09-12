/// 表示一个无操作的事务
pub struct NoOpTxn;

impl NoOpTxn {
  /// 准备操作
  /// 在 garnet 中的相对路径:garnet/modules/NoOpModule/NoOpTxn.cs:Prepare
  /// 满足 Garnet 事务准备阶段接口规范，保留形参以匹配固定签名
  pub fn prepare(&self, _input: &(), _output: &()) -> bool {
    true
  }

  /// 主操作
  /// 在 garnet 中的相对路径:garnet/modules/NoOpModule/NoOpTxn.cs:Main
  /// 满足 Garnet 事务执行阶段接口规范，保留形参以匹配固定签名
  pub fn main(&self, _input: &(), _output: &(), _state: &mut ()) {}
}
