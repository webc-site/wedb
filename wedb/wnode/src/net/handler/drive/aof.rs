//! 出网 armed 闩（AOF 提交等待闸的时点标记面）
//!
//! 在 garnet 中的相对路径:
//! - `libs/server/Resp/RespServerSession.cs:1453`（Send 内 waitForAofBlocking 读点）
//!
//! 对齐 C# `RespServerSession` 的 partial 分文件组织：本件为 drive_loop
//! 出网前置等待的闩位分体。

use crate::traits::MessageConsumerFace;

/// 出网 armed 闩单点（drive_loop 六处冲应答出口共用）
///
/// 应答字节经 take_output_into 出会话冲入 resp_pooled 后，内层循环重入消费
/// 解析流水线后续 AOF 无关命令（PING/ECHO 族）会把会话 wait_for_aof_blocking
/// 复位（复位判据 pending_output_len()==0，C# dcurr==head 含未出网字节故不存
/// 在此窗口）。本闩在每处冲出口以「出会话时点」读会话标记：resp_pooled 非空
/// 且标记为真即置 armed，出网臂按 armed || 会话字段等待，杜绝停泊-续跑轮漏等
/// 提交落盘。仅读标记、不新建第二套等待调用（wait_for_commit_async 仍出网臂单点）。
#[inline]
pub(super) fn arm_aof_latch<C: MessageConsumerFace>(
  armed: &mut bool,
  resp_buf: &[u8],
  session: &C,
) {
  if !resp_buf.is_empty() && session.wait_for_aof_blocking() {
    *armed = true;
  }
}
