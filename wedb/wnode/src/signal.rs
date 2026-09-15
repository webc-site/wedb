//! 停机信号监听：覆盖 Ctrl+C (SIGINT) 与 Docker/systemd stop (SIGTERM)
//!
//! 1:1 对标微软 Garnet 与生产级服务停机标准，驱动 compio 全异步事件循环。

use compio::{runtime::spawn, signal};
use crossfire::mpsc::bounded_async;
use log::error;

use crate::error::{Error, Result};

/// glibc 信号编号: SIGTERM (`kill <pid>` 默认信号，docker stop 与 systemd stop 均发送它)
pub const SIGTERM_NUM: i32 = 15;

pub const SIGINT_LABEL: &str = "SIGINT (Ctrl+C)";
pub const SIGTERM_LABEL: &str = "SIGTERM";

/// 竞速等待首个停机信号到达，返回信号名供日志展示
///
/// - unix: `compio::signal::ctrl_c` 与 `compio::signal::unix::signal(15)` 并驱竞速，
///   Ctrl+C 与 docker stop / systemd stop 均可走优雅停机路径；
/// - windows: 仅 CTRL_C_EVENT (SIGTERM 不存在于 windows)。
///
/// 任一监听任务注册失败仅记录日志并让出其发送端；全部失败时通道断开，本函数返回错误。
///
/// 返回后未触发的监听任务被取消：任务取消会 drop 内部的 SignalListener 并将信号恢复为默认处置，
/// 因此优雅停机期间再次到来的 SIGINT/SIGTERM 会按默认行为立即终止进程（强杀逃生通道）。
pub async fn wait_shutdown_signal() -> Result<&'static str> {
  let (tx, rx) = bounded_async::<&'static str>(1);
  let int_task = spawn({
    let tx = tx.clone();
    async move {
      if let Err(e) = signal::ctrl_c().await {
        error!("注册 SIGINT 监听失败: {e}");
        return;
      }
      let _ = tx.try_send(SIGINT_LABEL);
    }
  });

  #[cfg(unix)]
  let term_task = spawn(async move {
    use compio::signal::unix::signal as unix_signal;
    if let Err(e) = unix_signal(SIGTERM_NUM).await {
      error!("注册 SIGTERM 监听失败: {e}");
      return;
    }
    let _ = tx.try_send(SIGTERM_LABEL);
  });
  #[cfg(windows)]
  drop(tx);

  let label = rx
    .recv()
    .await
    .map_err(|e| Error::SignalChannelBroken(e.to_string()))?;

  // 取消未触发的监听任务，恢复信号默认处置（强杀逃生通道）
  drop(int_task);
  #[cfg(unix)]
  drop(term_task);

  Ok(label)
}
