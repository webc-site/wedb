//! CLIENT KILL/注销终止哨兵（令牌域）
//!
//! 在 garnet 中的相对路径: `libs/server/Servers/GarnetServerTcp.cs`（CLIENT KILL 直关套接字等价承接）

use std::sync::Arc;

use compio::runtime::{CancelToken, spawn};

use crate::servers::consumer_registry::ConsumerEntry;

/// KILL/注销哨兵：监听终止广播并打断挂起读与在途写出/提交等待（含 TLS 握手期）
///
/// C# CLIENT KILL 经 `networkSender.TryClose()` 直关套接字；rust 连接任务
/// 独占套接字，等价物为「注册条目 kill 位 + CancelToken」——被杀连接的一切
/// 在途收发（读写段经 drive.rs `killable` 挂接）由哨兵秒级打断，泵随之走
/// dispose（注销 + 会话释放）。令牌与泵同运行时驱动（compio CancelToken 线程
/// 亲和；KILL 方仅置原子位与广播事件，跨线程安全）。挂接点随预注册前移到
/// accept 循环（C# GarnetServerTcp.cs:256 TryAdd 注册于 handler.Start 之前，
/// 握手期连接同样处于被杀可达域）
pub(crate) fn spawn_kill_watcher(entry: Arc<ConsumerEntry>, kill_token: CancelToken) {
  spawn(async move {
    entry.wait_terminate().await;
    kill_token.cancel();
  })
  .detach();
}
