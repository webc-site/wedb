//! 慢路径阻塞等待面（[`BlockWaitFace`] 等待闭环 + 观察者注销守卫单源；
//! 列表族与有序集合族慢臂共用，禁另起机制——机制全貌见 slow 模块头注）

use std::sync::{
  Arc,
  atomic::{AtomicU64, Ordering},
};

use wcol::itembroker::item_broker_face::BlockedWait;
use wresp::command::RespCommand;

use super::super::write_collection_item_result;
use crate::resp::ItemBroker;

/// 慢路径阻塞等待面（经纪句柄 + 发起会话域；`list()` 与 `move_core_cold`
/// 的阻塞族臂据此在装载未取到时闭环等待，None = 经纪未注入的独立会话域）。
/// 经纪持具体 `SharedItemBroker`（wcol trait 仅存于 [`BlockedWait`] 泛型
/// 约束，无 dyn 擦除）
pub(crate) struct BlockWaitFace<'a> {
  pub(crate) broker: &'a Arc<ItemBroker>,
  pub(crate) domain: (u64, u64),
}

impl BlockWaitFace<'_> {
  /// 慢路径阻塞等待闭环（C# BlockingWait 内联阻塞的 compio 投影）：
  /// 登记观察者后在执行域内竞速超时，出件/超时经应答单源统一出帧。
  ///
  /// 观察者 id 取慢路径专用递减域（自 `usize::MAX` 递减，与 i64 会话 id
  /// 空间不相交）：执行域无会话可达面，专用域保证并发等待互不顶替映射；
  /// 且 CLIENT UNBLOCK 命令对 client_id < 0 设单点门禁恒回 0，不命中本域
  /// （偏差登记见 doc/zh/deviations.md §63；C# 侧慢路径重放后仍持原会话可
  /// 解除，rust 慢路径等待体由超时/出件/注销守卫自治收口）。
  ///
  /// 执行体注销收口由 [`ObserverDropGuard`] 承担：慢执行体未闭环即被丢弃
  /// （网络泵终止广播胜出 / 会话 dispose 收口 / 脚本挂起槽就地取消）时经
  /// 经纪 `handle_session_disposed` 摘除观察者——对位 C# RespServerSession
  /// Dispose :408 `itemBroker?.HandleSessionDisposed(this)` 的无条件注销，
  /// 杜绝观察者滞留经纪等待队列成僵尸（新写入元素被误弹出后写回 BrokenPipe
  /// 丢弃，真数据丢失）
  pub(crate) async fn wait(
    &self,
    cmd: RespCommand,
    timeout: f64,
    keys: Vec<Vec<u8>>,
    cmd_args: Vec<Vec<u8>>,
    resp_version: u8,
    output: &mut Vec<u8>,
  ) {
    static NEXT_ID: AtomicU64 = AtomicU64::new(usize::MAX as u64);
    let session_id = NEXT_ID.fetch_sub(1, Ordering::Relaxed) as usize;
    let observer = self
      .broker
      .start_wait(cmd, keys, session_id, cmd_args, self.domain);
    let mut guard = ObserverDropGuard {
      broker: Arc::clone(self.broker),
      session_id,
      done: false,
    };
    let mut wait = BlockedWait::new(Arc::clone(self.broker), observer, cmd, timeout);
    let (_, result) = wait.resolve().await;
    // 正常闭环（出件/超时）：resolve 内 finish_wait 已摘会话映射，
    // drop 守卫为 no-op
    guard.done = true;
    write_collection_item_result(cmd, &result, resp_version, output);
  }
}

/// 慢路径阻塞观察者注销守卫（内嵌于 `BlockWaitFace::wait` 执行体）：执行体
/// 未闭环即被丢弃时经经纪摘除观察者。守卫随执行体走，泵终止/dispose/脚本
/// 槽取消全部丢弃点单一覆盖，与阻塞臂 `BlockedWait::abort` 同一经纪单点，
/// 不新增第二套注销机制
struct ObserverDropGuard {
  broker: Arc<ItemBroker>,
  session_id: usize,
  done: bool,
}

impl Drop for ObserverDropGuard {
  fn drop(&mut self) {
    if !self.done {
      self.broker.handle_session_disposed(self.session_id);
    }
  }
}
