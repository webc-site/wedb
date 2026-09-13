/// 包含所有无操作的模块
pub struct NoOpModule;

impl NoOpModule {
  /// 加载模块时的操作
  /// 在 garnet 中的相对路径:garnet/modules/NoOpModule/NoOpModule.cs:OnLoad
  /// 满足 Garnet 模块加载生命周期接口规范，保留形参以匹配固定签名
  pub fn on_load(&self, _context: &mut (), _args: &[String]) {
    // 无操作
  }
}
