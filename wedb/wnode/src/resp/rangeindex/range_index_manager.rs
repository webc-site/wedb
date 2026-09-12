//! 范围索引会话域启用门控（对标 libs/server/Resp/RangeIndex/RangeIndexManager.cs 的会话装配面）
//!
//! 单一机制声明：C# RangeIndexManager 的引擎面（liveIndexes / 条带锁 / 检查点
//! 快照 / 延迟释放）由 [`wkv::RangeIndexManager`]（wedb/wbftree/src/manager/）
//! 唯一承接，全部方法与文件路径族均在该引擎一处定义；本类型不复制任何引擎
//! 逻辑，仅承担 C# `storeWrapper.activeRangeIndexManager is null` 的会话域
//! 装配角色——RESP 命令层以 `Option<&RangeIndexManager>` 判定预览开关
//! （None = 未启用），`engine()` 暴露引擎实例供装配与恢复流程取用。
//!
//! 命名差异说明：C# 文件名前缀为 32 字符十六进制（XxHash128 → Guid("N")），
//! Rust 引擎为 26 字符 Base32（同一 128 位 key_id 的定长编码），路径形态由
//! 引擎统一决定。

use std::{path::PathBuf, sync::Arc};

use wkv::RangeIndexManager as EngineRangeIndexManager;

/// 范围索引管理器（会话域启用门控）
pub struct RangeIndexManager {
  /// 引擎管理器实例（liveIndexes / 条带锁 / 快照原语的唯一持有者）
  engine: Arc<EngineRangeIndexManager>,
}

impl RangeIndexManager {
  /// 创建管理器（C# 构造子：riLogRoot 必建，migration-tmp 清理重建由引擎承接；
  /// 根目录创建失败上抛，对标 C# 构造器抛异常）
  pub fn new(ri_log_root: impl Into<PathBuf>, cpr_dir: impl Into<PathBuf>) -> wkv::Result<Self> {
    Ok(Self {
      engine: Arc::new(EngineRangeIndexManager::new(ri_log_root, cpr_dir)?),
    })
  }

  /// 引擎实例引用
  #[inline]
  pub fn engine(&self) -> &Arc<EngineRangeIndexManager> {
    &self.engine
  }
}
