/// 表示一个无操作的过程
pub struct NoOpProc;

impl NoOpProc {
    /// 执行操作
    /// garnet相对路径:garnet/modules/NoOpModule/NoOpProc.cs:Execute
    pub fn execute(&self, _api: &(), _proc_input: &(), _output: &mut ()) -> bool {
        true
    }
}
