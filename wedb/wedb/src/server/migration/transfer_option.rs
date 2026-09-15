/// libs/cluster/Session/TransferOption.cs:TransferOption
///
/// MIGRATE 支持的传输形态（C# 标 byte，三态一一对应）
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
pub enum TransferOption {
  /// 无传输形态（键占位与选项均缺省，命令空跑）
  None,
  /// 迁移提供的全部键
  Keys,
  /// 迁移单个槽位内的全部键
  Slots,
}
