//! CLIENT KILL/注销终止哨兵（令牌域）
//!
//! 在 garnet 中的相对路径: `libs/server/Servers/GarnetServerTcp.cs`（CLIENT KILL 直关套接字等价承接）

use std::sync::Arc;

use compio::runtime::{CancelToken, spawn};

use crate::servers::consumer_registry::ConsumerEntry;

/// KILL/注销哨兵：监听终止广播并打断挂起中的读
///
/// C# CLIENT KILL 经 `networkSender.TryClose()` 直关套接字；rust 连接任务
/// 独占套接字，等价物为「注册条目 kill 位 + CancelToken」——被杀连接的
/// 挂起 `read` 由哨兵秒级打断，泵随之走 dispose（注销 + 会话释放）。
/// 令牌与泵同运行时驱动（compio CancelToken 线程亲和；KILL 方仅置原子位
/// 与广播事件，跨线程安全）
pub(super) fn spawn_kill_watcher(entry: Arc<ConsumerEntry>, kill_token: CancelToken) {
  spawn(async move {
    loop {
      // 双重检查防错过唤醒（对齐 ShutdownCoordinator::wait 防竞态模式）
      if entry.is_terminating() {
        break;
      }
      let listener = entry.listen_terminate();
      if entry.is_terminating() {
        break;
      }
      listener.await;
    }
    kill_token.cancel();
  })
  .detach();
}
