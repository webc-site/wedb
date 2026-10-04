//! 快照下发在途读者登记/注销单点（对标 C# CheckpointStore.cs 活日志截断钳制）

use super::*;

impl ReplicationManager {
  /// 快照读钉注册单点（对标 C# CheckpointStore.cs:196-210 的 reader 登记半边）
  ///
  /// 由 `ReplicaSyncSession`（`replica_sync_session`）在取得检查点条目读者的
  /// 同一单点调用：`token` = 条目 store_hlog_token，`aligned_begin` = 该条目
  /// 对应 wcpr meta 的 `hlog_meta.begin_address` 扇区对齐下界（与
  /// `send_store_checkpoint` 的读取起址同源）。在册表按 token 计数（多会话读
  /// 同条目共享计数），锁内重算聚合 min begin 经 `publish` 回调前向写入 whlog
  /// 的 reader_pin 水位（回调持锁，写序=变更序，杜绝并发互相覆盖）。
  /// 返回注册后的聚合水位（观测/测试用）。
  pub fn register_snapshot_reader(
    &self,
    token: u128,
    aligned_begin: u64,
    publish: impl FnOnce(u64),
  ) -> u64 {
    let mut pins = self.snapshot_reader_pins.lock();
    match pins.get_mut(&token) {
      // 同 token 已有在册条目：计数 +1，begin 取更保守（更小）者
      Some((begin, count)) => {
        *begin = (*begin).min(aligned_begin);
        *count += 1;
      }
      None => {
        pins.insert(token, (aligned_begin, 1));
      }
    }
    let agg = aggregate_reader_pin(&pins);
    publish(agg);
    agg
  }

  /// 快照读钉注销单点（reader 撤销半边）
  ///
  /// 与 [`Self::register_snapshot_reader`] 逆序配对，由会话释放条目读者的同一
  /// 单点调用：该 token 计数归零才摘除条目（单会话释放不抬他会话钉），锁内
  /// 重算聚合并经 `publish` 前向写回水位。返回注销后的聚合水位。
  pub fn unregister_snapshot_reader(&self, token: u128, publish: impl FnOnce(u64)) -> u64 {
    let mut pins = self.snapshot_reader_pins.lock();
    if let Some((_, count)) = pins.get_mut(&token) {
      *count -= 1;
      if *count == 0 {
        pins.remove(&token);
      }
    }
    let agg = aggregate_reader_pin(&pins);
    publish(agg);
    agg
  }

  /// 当前在册快照读钉条目数（诊断与测试观测面：全部会话注销后应归零）
  pub fn snapshot_reader_count(&self) -> usize {
    self.snapshot_reader_pins.lock().len()
  }
}
