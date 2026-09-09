/// 包含所有无操作的模块
pub struct NoOpModule;

impl NoOpModule {
  /// 加载模块时的操作
  /// garnet相对路径:garnet/modules/NoOpModule/NoOpModule.cs:OnLoad
  pub fn on_load(&self, _context: &mut (), _args: &[String]) {
    // 无操作
  }
}
