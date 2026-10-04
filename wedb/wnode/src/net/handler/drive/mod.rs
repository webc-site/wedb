//! KILL/注销终止哨兵与连接泵主循环（读泵/写回域）
//!
//! 在 garnet 中的相对路径:
//! - `libs/common/Networking/NetworkHandler.cs:Start`
//!
//! 对齐 C# `RespServerSession` 的 partial 分文件组织（RespServerSession.cs
//! 拆 +Output/+SlotVerify 等分件），本目录按泵状态机阶段拆分：
//! - [`consume`]：消费批派发循环（drive_loop 本体）；
//! - [`handshake`]：握手/入账（首批识别与会话装配）；
//! - [`read`]：网络读取段（ReadEnd / 推送臂双路等待）；
//! - [`write`]：写出段（pooled_write/WriteStream/shrink_to_base）；
//! - [`aof`]：出网 armed 闩（arm_aof_latch）；
//! - [`race`]：竞速探测（RaceEnd/probe_race/wait_terminate/preserve_probe）。
//!
//! 终止/清理（process_stream 收场序、killable/is_dead_conn 判定）留本文件。

mod aof;
mod consume;
mod handshake;
mod race;
mod read;
mod write;

use std::{any::Any, future::Future, io, panic::AssertUnwindSafe, sync::Arc};

use compio::{
  runtime::{CancelToken, Cancelled, FutureExt},
  time,
};
use futures_util::FutureExt as _;
use log::error;

use super::NetworkHandler;
use crate::{
  net::stream::ConnectionStream,
  traits::{MessageConsumerFace, SessionProviderFace},
};

/// 握手/读取段共用泵环境（散参聚合，免 too_many_arguments；字段与
/// drive_loop 局部同名，段内解构后代码与原内联形态逐字对齐）
struct PumpEnv<'a, P> {
  session_provider: &'a Arc<P>,
  sender_id: u64,
  kill_token: &'a Option<CancelToken>,
  buffer_size: usize,
}

impl<C: MessageConsumerFace> NetworkHandler<C> {
  /// 驱动统一连接流（TCP / Unix / TLS）异步读写与协议切片泵
  ///
  /// 泵循环无论从何路径收场，本函数都先走一遍流的关闭序再释放会话（见函数体内
  /// 顺序说明），故它是连接退出的唯一收场点。
  ///
  /// 在 garnet 中的相对路径:
  /// - `libs/common/Networking/NetworkHandler.cs:Start`
  pub async fn process_stream<P: SessionProviderFace<Consumer = C>>(
    &mut self,
    mut stream: ConnectionStream,
    session_provider: Arc<P>,
    sender_id: u64,
  ) -> io::Result<()> {
    // 会话隔离（C# RespServerSession.TryConsumeMessages 最外层 catch(Exception)
    // → Dispose 对位）：泵循环内 panic 只断本连接，不穿出连接任务。断连走
    // 下方与四条退出路径同一条收场尾巴（shutdown → dispose），accept 泵与
    // 其他连接任务不受影响。AssertUnwindSafe：panic 后不复用 drive_loop 的
    // 任何中间状态，直接收场，无跨 await 观察半更新状态的面。
    let res = match AssertUnwindSafe(self.drive_loop(&mut stream, session_provider, sender_id))
      .catch_unwind()
      .await
    {
      Ok(res) => res,
      Err(payload) => {
        error!(
          "连接 {sender_id}({}) 泵 panic，隔离断连: {}",
          self.remote_endpoint,
          panic_payload_text(&payload)
        );
        Err(io::Error::other("会话泵 panic"))
      }
    };
    // 关闭序（C# TcpNetworkHandlerBase.Dispose 的 Shutdown → Close → DisposeImpl
    // 序）：先 FIN / close_notify，后 dispose。顺序不可颠倒——dispose 释放会话与
    // 订阅推送通道，其后推送侧再写已关闭的流没有意义，与泵内「缓冲归还先于一切
    // 退出路径」同一顺序口径。退出路径（取消、对端 EOF、意外 EOF、错误上抛、
    // 泵 panic）共用此一条尾巴。对端已断时错误弃用（与 wconn 客户端泵同口径），
    // 绝不覆盖 drive_loop 的原返回值。
    // 尾帧有界、弃帧不弃注销：TLS 臂 close_notify 须把连接发送队列写尽 socket
    // 才返回（rustls Stream::poll_close），黑洞对端令其永久 Pending、dispose 永不
    // 执行（C# Dispose 链 Shutdown/Close 为 syscall 先行、注销无条件跟进，结构上
    // 不可残留）。故收场尾 shutdown 与泵内一切在途 await 同挂 KILL/停机令牌，并
    // 加确定性超时双边界（timeout 内层、取消外层，口径同 server.rs TLS 握手
    // 臂）：KILL 先行时令牌已触发即刻弃帧不吃超时界；超时即弃帧照常落 dispose
    // 收口——ConnectionStream 随连接任务返回 Drop，底层 fd 关闭发 FIN/RST 兜底
    // （对位 C# socket.Close 内核接管尾帧）。明文臂 shutdown 即刻就绪，包裹零
    // 成本无行为变化。
    let _ = killable(
      time::timeout(self.shutdown_timeout, stream.shutdown()),
      &self.kill_token,
    )
    .await;
    self.dispose();
    res
  }
}

/// 挂 KILL/注销终止令牌执行一个在途 await 点（读/写出/提交等待统一机制）
///
/// 对位 C# GarnetTcpNetworkSender.TryClose（:285）的跨线程 `socket.Close()`
///（其注释自认 "should cause all outstanding requests to fail"，:291）：C#
/// 直关套接字令任何在途 send/提交等待即刻失败，handler 随即走 Dispose 链摘除
/// 注册表条目；rust 连接任务独占套接字无从外关，等价承接为一切在途 await 点
///（握手读、主读取段、命令臂 write_all 与推送臂 write_all_shared、两臂
/// wait_for_commit_async）统一挂本令牌——取消撤销在途 compio op，缓冲随
/// future 失（compio 取消语义），调用方与既有终止胜出臂同走 break 收场尾巴
///（shutdown → dispose 注销）。无令牌（未装配注册表的哑桩宿主）直落不挂取消
async fn killable<F: Future>(fut: F, token: &Option<CancelToken>) -> Result<F::Output, Cancelled> {
  match token {
    Some(token) => fut.with_cancel(token.clone()).fail_fast().await,
    None => Ok(fut.await),
  }
}

/// 对端脱机/连接死亡的错误类别（C# SocketError != Success → Dispose 判据）：
/// 意外 EOF、连接重置、破管三类视为正常断连收场，其余上抛
#[inline]
fn is_dead_conn(e: &io::Error) -> bool {
  matches!(
    e.kind(),
    io::ErrorKind::UnexpectedEof | io::ErrorKind::ConnectionReset | io::ErrorKind::BrokenPipe
  )
}

/// panic 载荷转可读文本（&str / String 直取，其余退回类型描述）
pub(crate) fn panic_payload_text(payload: &(dyn Any + Send)) -> String {
  if let Some(s) = payload.downcast_ref::<&str>() {
    (*s).into()
  } else if let Some(s) = payload.downcast_ref::<String>() {
    s.clone()
  } else {
    "非文本 panic 载荷".into()
  }
}
