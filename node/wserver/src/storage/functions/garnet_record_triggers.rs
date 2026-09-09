//! 记录生命周期触发器（对标 libs/server/Storage/Functions/GarnetRecordTriggers.cs）
//!
//! 缺口总述：C# 侧五个触发点挂接 Tsavorite 记录生命周期（删除/落盘/驱逐/
//! 恢复/检查点）做对象引用清理；wkv 引擎内部化记录生命周期（记录删除即
//! 物理墓碑、对象生命周期由信封值承担），无外部触发面。全部触发器退化为
//! 可观测空操作，保留真实返回语义（是否已处理）。

/// 记录生命周期触发器集合
pub struct GarnetRecordTriggers;

impl GarnetRecordTriggers {
  /// 内存记录释放触发
  ///
  /// 缺口说明：wkv 记录内存由 hlog 页与 GC 统一回收，无外部释放钩子。
  ///
  /// libs/server/Storage/Functions/GarnetRecordTriggers.cs:OnDispose
  pub fn on_dispose(&self) -> bool {
    true
  }

  /// 磁盘记录释放触发
  ///
  /// 缺口说明：同 [`Self::on_dispose`]。
  ///
  /// libs/server/Storage/Functions/GarnetRecordTriggers.cs:OnDisposeDiskRecord
  pub fn on_dispose_disk_record(&self) -> bool {
    true
  }

  /// 记录驱逐触发
  ///
  /// 缺口说明：wkv 驱逐由 hlog 只读段回滚（shift head）内部完成。
  ///
  /// libs/server/Storage/Functions/GarnetRecordTriggers.cs:OnEvict
  pub fn on_evict(&self) -> bool {
    true
  }

  /// 恢复触发
  ///
  /// 缺口说明：wkv 恢复由 CheckpointManager/CPR 快照内部完成。
  ///
  /// libs/server/Storage/Functions/GarnetRecordTriggers.cs:OnRecovery
  pub fn on_recovery(&self) -> bool {
    true
  }

  /// 检查点触发
  ///
  /// 缺口说明：wkv 检查点由 take_cpr_snapshots / BfTreeService 内部完成。
  ///
  /// libs/server/Storage/Functions/GarnetRecordTriggers.cs:OnCheckpoint
  pub fn on_checkpoint(&self) -> bool {
    true
  }
}
