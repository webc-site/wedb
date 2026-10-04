//! 写出段：连接泵写出单点与外出缓冲低水位收敛
//!
//! 在 garnet 中的相对路径:
//! - `libs/server/Servers/GarnetTcpNetworkSender.cs`（SendResponse 分片写出）
//!
//! 对齐 C# `RespServerSession` 的 partial 分文件组织：本件为 drive_loop
//! 写出阶段分体（命令臂 / 推送臂共用）。

use std::io;

use compio::{
  BufResult,
  runtime::{CancelToken, Cancelled},
};
use wbase::pool::PooledRefBuffer;

use super::killable;
use crate::net::stream::ConnectionStream;

/// 写出端借用形态：独占（命令臂 `write_all`）/ 共享（推送臂 `write_all_shared`，
/// 读 future 挂起存活期唯一合法借用）。引用包装零成本
pub(super) enum WriteStream<'a> {
  Owned(&'a mut ConnectionStream),
  Shared(&'a ConnectionStream),
}

/// 连接泵写出单点（命令臂 / 推送臂同构段收口）：取池缓冲 → 取消钩内在途
/// 写出 → 缓冲清零复位回池基准水位。
///
/// 返回 `None` = 在途写出被取消（KILL/停机终止域，缓冲随 future 失，
/// compio 取消语义）；`Some(res)` = 写出终局，错误尾分派（断连 / 上抛）
/// 留调用方。
///
/// 归还臂注释对标 C# SendAndReset 分片复位——响应缓冲恒为配置规格不因大
/// 应答扩容驻留，不重新借出：重借会从池队列弹出闲置块、旧扩容块析构迁移至
/// 高级层级，突发大应答逐轮蚕食基准层级闲置配额致池空退化为堆分配抖动
#[inline]
pub(super) async fn pooled_write(
  stream: WriteStream<'_>,
  resp_pooled: &mut PooledRefBuffer<'_>,
  kill_token: &Option<CancelToken>,
  buffer_size: usize,
) -> Option<io::Result<()>> {
  let payload = resp_pooled
    .take_buffer()
    .expect("pooled send buffer active");
  let write_outcome = match stream {
    WriteStream::Owned(stream) => killable(stream.write_all(payload), kill_token).await,
    WriteStream::Shared(stream) => killable(stream.write_all_shared(payload), kill_token).await,
  };
  let BufResult(write_res, mut reclaimed) = match write_outcome {
    Err(Cancelled) => return None,
    Ok(pair) => pair,
  };
  reclaimed.clear();
  shrink_to_base(&mut reclaimed, buffer_size);
  resp_pooled.set_buffer(reclaimed);
  Some(write_res)
}

/// 外出缓冲低水位容量收敛：曾超池基准规格的块就地缩回常驻水位
///
/// 对标 C# 响应缓冲恒为配置 sendBufferSize（GarnetTcpNetworkSender.cs:210
/// SendResponse 满 64KB 分片写出，RespServerSession.cs:1348 SendAndReset 复位
/// 游标不换块）与 NetworkHandler.cs:529 ShrinkNetworkReceiveBuffer 的低水位
/// 收缩语义。就地收敛不重新借出，连接存活全程仅占用建连时借出的单一池配额。
/// `shrink_to` 为容量下界请求，分配器未收敛到位时换配精确基准规格新块，
/// 保证归还端层级精确配平
#[inline]
pub(super) fn shrink_to_base(buf: &mut Vec<u8>, base: usize) {
  if buf.capacity() > base {
    buf.shrink_to(base);
    if buf.capacity() > base {
      // 换块臂 try_reserve 收口（票 zcode-r55-alloc）：精确基准新块分配失败
      // 即保持原块——收缩是低水位优化非正确性依赖，失败退化为暂驻峰值容量，
      // 杜绝 std 扩容触顶 handle_alloc_error abort 全进程
      let mut fresh = Vec::new();
      if fresh.try_reserve(base).is_ok() {
        *buf = fresh;
      }
    }
  }
}
