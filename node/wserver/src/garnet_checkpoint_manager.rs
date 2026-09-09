//! 检查点管理器辅助（对标 libs/server/GarnetCheckpointManager.cs）
//!
//! 缺口总述：C# 侧 GarnetCheckpointManager 聚合检查点目录、安全 AOF 地址与
//! 恢复 Cookie 元数据；wserver 检查点面由 wkv [`CheckpointManager`] +
//! databases 域（DatabaseManagerBase）承担，本域保留 C# 静态入口的语义
//! 占位（禁 panic 约束下的一致性缺省返回）。

/// 检查点管理器辅助入口
pub struct GarnetCheckpointManager;

impl GarnetCheckpointManager {
  /// 记录当前安全 AOF 地址（检查点元数据回写）
  ///
  /// 缺口说明：wkv 检查点令牌内嵌存储尾地址，无独立安全 AOF 地址通道，
  /// 本方法退化为空操作。
  ///
  /// libs/server/GarnetCheckpointManager.cs:SetCurrentSafeAofAddress
  pub fn set_current_safe_aof_address(_aof_address: u64) {}

  /// 记录恢复出的安全 AOF 地址（恢复元数据回写）
  ///
  /// 缺口说明：同 [`Self::set_current_safe_aof_address`]，恢复面地址由
  /// wkv 恢复令牌内部承载。
  ///
  /// libs/server/GarnetCheckpointManager.cs:SetRecoveredSafeAofAddress
  pub fn set_recovered_safe_aof_address(_aof_address: u64) {}

  /// 读取检查点 Cookie 元数据
  ///
  /// 缺口说明：wkv 检查点元数据由 CheckpointManager::find_latest_checkpoint
  /// 与令牌文件承担，本域无 Cookie 通道，恒返回 None。
  ///
  /// libs/server/GarnetCheckpointManager.cs:GetCookie
  pub fn get_cookie() -> Option<Vec<u8>> {
    None
  }
}
