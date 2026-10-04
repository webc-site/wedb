//! 停机信号监听：覆盖 Ctrl+C (SIGINT) 与 Docker/systemd stop (SIGTERM)
//!
//! 1:1 对标微软 Garnet 与生产级服务停机标准，驱动 compio 全异步事件循环。

use core::pin::pin;

use compio::signal::ctrl_c;
#[cfg(unix)]
use compio::signal::unix::signal as unix_signal;
use futures_util::future::{Either, select};
use log::error;

use crate::error::{Error, Result};

/// glibc 信号编号: SIGTERM (`kill <pid>` 默认信号，docker stop 与 systemd stop 均发送它)
pub(crate) const SIGTERM_NUM: i32 = 15;

pub const SIGINT_LABEL: &str = "SIGINT (Ctrl+C)";
pub(crate) const SIGTERM_LABEL: &str = "SIGTERM";

/// 竞速等待首个停机信号到达，返回信号名供日志展示
///
/// - unix: `compio::signal::ctrl_c` 与 `compio::signal::unix::signal(15)` 并驱竞速，
///   Ctrl+C 与 docker stop / systemd stop 均可走优雅停机路径；
/// - windows: 仅 CTRL_C_EVENT (SIGTERM 不存在于 windows)。
///
/// 纯栈上 select 竞速：零堆分配、零 channel、无任务调度依赖。未触发信号的
/// listener 在 select 完成时同步 Drop，即刻解除 handler 并恢复 SIG_DFL，
/// 优雅停机期间再次到来的 SIGINT/SIGTERM 立即按默认行为终止进程（强杀逃生通道）。
pub async fn wait_shutdown_signal() -> Result<&'static str> {
  #[cfg(unix)]
  {
    let int_fut = pin!(ctrl_c());
    let term_fut = pin!(unix_signal(SIGTERM_NUM));

    match select(int_fut, term_fut).await {
      Either::Left((Ok(()), _)) => Ok(SIGINT_LABEL),
      Either::Right((Ok(()), _)) => Ok(SIGTERM_LABEL),
      Either::Left((Err(e), term_fut)) => {
        error!("注册 SIGINT 监听失败: {e}");
        term_fut
          .await
          .map_err(|e| Error::SignalChannelBroken(e.to_string()))?;
        Ok(SIGTERM_LABEL)
      }
      Either::Right((Err(e), int_fut)) => {
        error!("注册 SIGTERM 监听失败: {e}");
        int_fut
          .await
          .map_err(|e| Error::SignalChannelBroken(e.to_string()))?;
        Ok(SIGINT_LABEL)
      }
    }
  }

  #[cfg(windows)]
  {
    ctrl_c()
      .await
      .map_err(|e| Error::SignalChannelBroken(e.to_string()))?;
    Ok(SIGINT_LABEL)
  }
}
