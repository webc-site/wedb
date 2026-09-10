//! 命令哈希查找数据（对标 libs/server/Resp/Parser/RespCommandHashLookupData.cs）
//!
//! C# 静态预生成 主表/子命令表 字节序列，供命令解析热路径按哈希查表；
//! rust 解析器（wresp + resp_command 域）以自有分派承接，预生成静态表
//! 无对应语义。

/// 命令哈希查找数据
pub struct RespCommandHashLookupData;

impl RespCommandHashLookupData {
  /// libs/server/Resp/Parser/RespCommandHashLookupData.cs:PopulatePrimaryTable
  ///
  /// 缺口说明：C# 预生成静态查表字节；rust 解析分派由 wresp / resp_command
  /// 域承接（豁免登记见 js/check/ignore/libs/server/Resp/Parser/
  /// RespCommandHashLookupData.yml）
  pub fn populate_primary_table() {}
}
