//! 自定义事务过程基座（对标 libs/server/Custom/CustomTransactionProcedure.cs）

/// 自定义事务过程（C# abstract CustomTransactionProcedure 的托管承接位）。
///
/// wcustom 不依赖 wtxn/wnode，三段式以本域最小签名承接；宿主
/// （wnode::txn_resp_commands）经 `wtxn::TxnProcedure` 适配后接入事务面，
/// 键登记与暂存缓冲由适配层/事务管理器承接。
#[derive(Debug, Clone)]
pub struct CustomTransactionProcedure {
  /// 过程注册 id（C# entry.proc() 实例化后的归属 id）
  pub id: u8,
}

impl CustomTransactionProcedure {
  /// 实例化过程体（C# Func&lt;CustomTransactionProcedure&gt; 工厂的目标形态）
  pub fn new(id: u8) -> Self {
    Self { id }
  }

  /// 准备段：登记读写键集（C# Prepare；默认空集 = 无键事务）
  pub fn prepare(&mut self) -> bool {
    true
  }

  /// 主段：锁内执行并产出应答（C# Main；默认无输出 → 空事务提交回 +OK）
  pub fn main(&mut self, output: &mut Vec<u8>) {
    let _ = output;
  }

  /// 收尾段（C# Finalize；AOF 回放期间跳过）
  pub fn finalize(&mut self, output: &mut Vec<u8>) {
    let _ = output;
  }
}
