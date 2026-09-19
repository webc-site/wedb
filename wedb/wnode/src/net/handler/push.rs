//! 订阅推送双路等待（读泵挂起期推送直写面）
//!
//! 在 garnet 中的相对路径: `libs/common/Networking/NetworkHandler.cs`（广播线程直写订阅会话网络发送器的等价承接）

use std::{
  future::{Future, poll_fn},
  pin::Pin,
  task::Poll,
};

use event_listener::EventListener;
use wpubsub::subscriber::PubSubMailbox;

/// 订阅推送双路等待结果
pub(super) enum PushOutcome<R> {
  /// 读完成（网络字节就绪，含取消失败外的全部读出口）
  Read(R),
  /// 邮箱到达推送（读仍在途，future 存活于调用方，处理后带同一 future
  /// 重入等待）
  Push,
}

/// 订阅态读等待：读 future（存活于调用方，绝不 drop——compio 读取消丢
/// 半包字节）+ 邮箱到达事件双路竞速
///
/// 读挂起期间被邮箱事件唤醒即返回 [`PushOutcome::Push`]，调用方排空邮箱
/// 写推送帧后带着同一读 future 重入（TCP/Unix 全双工读 op 挂起中写合法；
/// TLS 读写句柄逐轮 poll 互斥，读挂起即释锁，写侧据此推进）。
/// 唤醒零丢失：listener 注册后双检 + 读 future 复查两道防线（对齐
/// EventWorkQueue::wait_to_read 的防竞态模式）
pub(super) async fn wait_read_or_push<F, R>(
  read_fut: &mut Pin<Box<F>>,
  mailbox: &PubSubMailbox,
) -> PushOutcome<R>
where
  F: Future<Output = R>,
{
  let mut listener: Option<EventListener> = None;
  poll_fn(|cx| {
    loop {
      if let Poll::Ready(res) = read_fut.as_mut().poll(cx) {
        return Poll::Ready(PushOutcome::Read(res));
      }
      if mailbox.has_messages() {
        return Poll::Ready(PushOutcome::Push);
      }
      let mut lis = match listener.take() {
        Some(l) => l,
        None => {
          let lis = mailbox.listen();
          // 注册后双检：事件可能已发（唤醒丢失防线）
          if mailbox.has_messages() {
            return Poll::Ready(PushOutcome::Push);
          }
          lis
        }
      }; // 读复查（listener 注册期间读可能已就绪，driver waker 已在册）
      if let Poll::Ready(res) = read_fut.as_mut().poll(cx) {
        return Poll::Ready(PushOutcome::Read(res));
      }
      match Pin::new(&mut lis).poll(cx) {
        Poll::Ready(()) => continue, // 到达事件 → 回环（drain 或重检）
        Poll::Pending => {
          listener = Some(lis);
          return Poll::Pending;
        }
      }
    }
  })
  .await
}
