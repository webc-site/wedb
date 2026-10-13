//! 日志面键预览单点（一处定义，全仓错误臂键打印统一截断）
//!
//! 大键（主存页容量量级）整包入日志可致单条日志 MiB 级膨胀、刷盘放大，
//! 键内容（业务数据命名）亦进日志面；C# 对位 RMWMethods.cs 错误日志无键
//! 原文。rust 保留前缀可读性 + 总长提示的有界形态：单条日志键段长度恒有界。

use std::borrow::Cow;

/// 键预览前缀截断上限（字节）。
pub const LOG_KEY_MAX: usize = 64;

/// 键 → 日志安全预览：前 [`LOG_KEY_MAX`] 字节 lossy 可读；越界时截断于
/// UTF-8 字符边界并附总长提示 `…(len=N)`，短键零分配借用透传。
#[inline]
pub fn log_key(key: &[u8]) -> Cow<'_, str> {
  if key.len() <= LOG_KEY_MAX {
    return String::from_utf8_lossy(key);
  }
  // 回退 UTF-8 续字节（10xxxxxx）到字符边界；end > 0 兜住全续字节流的
  // 退化输入，杜绝下溢
  let mut end = LOG_KEY_MAX;
  while end > 0 && (key[end] & 0xC0) == 0x80 {
    end -= 1;
  }
  Cow::Owned(format!(
    "{}…(len={})",
    String::from_utf8_lossy(&key[..end]),
    key.len()
  ))
}
