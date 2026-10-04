//! 网络读取段（下一批；读完回到循环头消费）
//!
//! 在 garnet 中的相对路径:
//! - `libs/common/Networking/NetworkHandler.cs`（SessLoop waitForReceive 等待读取）
//!
//! 对齐 C# `RespServerSession` 的 partial 分文件组织：本件为 drive_loop
//! 读取阶段分体（原内联块逐字迁移）。

use std::{io, sync::Arc};

use compio::{
  BufResult,
  runtime::{Cancelled, FutureExt},
};
use log::error;
use wbase::{pool::PooledRefBuffer, primed::PrimedVec};

use super::{
  super::{
    buffer::MIN_READ_SPACE,
    push::{PushOutcome, wait_read_or_push},
  },
  PumpEnv, killable,
  write::{WriteStream, pooled_write},
};
use crate::{
  net::stream::ConnectionStream,
  servers::consumer_registry::ConsumerEntry,
  traits::{MessageConsumerFace, SessionProviderFace},
};

/// 返回 false = 断连/取消/EOF 收场（对位原泵循环 break 'drive）；true =
/// 读到下一批字节，回到循环头消费
///
/// resp_pooled 持 buffer_pool 共享借用（借还锚定 handler 字段），本段
/// 不能整取 &mut self——按字段拆分借用：会话槽 / 注册条目 / 端点名由
/// 调用方分字段传入
pub(super) async fn read_segment<C: MessageConsumerFace, P: SessionProviderFace<Consumer = C>>(
  session_slot: &mut Option<C>,
  consumer_entry: &Option<Arc<ConsumerEntry>>,
  remote_endpoint: &str,
  stream: &mut ConnectionStream,
  resp_pooled: &mut PooledRefBuffer<'_>,
  env: &PumpEnv<'_, P>,
) -> io::Result<bool> {
  let PumpEnv {
    session_provider,
    sender_id,
    kill_token,
    ..
  } = *env;
  let Some(session) = session_slot.as_mut() else {
    return Ok(false);
  };
  // 空闲段初始化代际随缓冲进出（TLS 读清零每缓冲付一次的跨读记忆，
  // wbase::primed 契约；键驻会话、与借出的缓冲同生命周期）
  let prime_key = session.recv_prime_key();
  let mut scratch = session.take_recv_scratch();
  // 空闲空间不足预留阈值：会话层消费收尾已平移半包残余至首部并截断
  // 长度（C# ShiftTransportReceiveBuffer 对偶，事务在途批除外），取出
  // 的缓冲长度恒为残余实际长度（通常仅数字节至单个大参数长度），
  // 补足一个读取阈值量即可（常驻水位由会话层
  // DEFAULT_RECV_BUFFER_CAPACITY 单点回归），与握手段同一预留口径，
  // 不再硬编码 65536 抬高单批内存驻留
  if scratch.capacity().saturating_sub(scratch.len()) < MIN_READ_SPACE {
    scratch.reserve(MIN_READ_SPACE);
  }
  // 订阅推送双路等待面（C# 广播线程直写订阅会话网络发送器的等价
  // 承接：读挂起期间邮箱到达事件唤醒本连接任务直写推送帧）。传输形态
  // 不分流：TCP/Unix 全双工共享读句柄，TLS 走读写互斥句柄对（rustls 单
  // 连接状态机，逐轮 poll 取锁、挂起即释锁），两形态共用同一双路等待
  // 与同一条写出路径
  let push_mailbox = session.pubsub_mailbox();
  let before = scratch.len();

  // 读完成形态：字节结果 / 取消（取消时读缓冲随 future 已失，
  // compio 读取消语义，断连收尾——与 KILL 主路径一致）
  enum ReadEnd {
    Bytes(BufResult<usize, PrimedVec>),
    Cancelled,
  }
  let read_end = match push_mailbox {
    Some(mailbox) => {
      // 读 future 存活于推送处理全程（绝不 drop 重建——半包字节
      // 会随 future 丢失）；无令牌形态以永不触发的哑令牌统一类型
      let token = kill_token.clone().unwrap_or_default();
      let mut read_fut = Box::pin(
        stream
          .read_shared(PrimedVec::from_primed(scratch, prime_key))
          .with_cancel(token)
          .fail_fast(),
      );
      loop {
        match wait_read_or_push(&mut read_fut, &mailbox).await {
          PushOutcome::Read(Ok(bytes)) => break ReadEnd::Bytes(bytes),
          PushOutcome::Read(Err(Cancelled)) => break ReadEnd::Cancelled,
          PushOutcome::Push => {
            // 读仍在途：排空邮箱推送帧直写网络（TCP/Unix 全双工读 op
            // 挂起中写合法；TLS 读句柄挂起即释锁，写句柄据此推进同一
            // 连接）
            let Some(session) = session_slot.as_mut() else {
              break ReadEnd::Cancelled;
            };
            resp_pooled.clear();
            session.drain_pubsub_into(resp_pooled.vec_mut());
            if resp_pooled.is_empty() {
              continue; // 唤醒竞态：邮箱已被他方排空
            }
            // 推送实发字节数（与命令臂 written 同一「发出即计」口径，
            // 取缓冲前先量）
            let push_bytes = resp_pooled.len();
            // 推送帧与命令应答同一出网规则（C# Publish 经会话 Send
            // 写出，waitForAofBlocking 置位即先等 AOF 提交落盘）；
            // 提交失败同命令臂：帧不发出，断连收尾；在途提交等待与
            // 在途写出同样挂取消钩（KILL/停机终止域补全）
            if session.wait_for_aof_blocking() {
              match killable(session_provider.wait_for_commit_async(), kill_token).await {
                Err(Cancelled) => break ReadEnd::Cancelled,
                Ok(Err(e)) => {
                  error!(
                    "连接 {sender_id}({}) 推送帧 AOF 提交落盘等待失败，断连: {e}",
                    remote_endpoint
                  );
                  break ReadEnd::Cancelled;
                }
                Ok(Ok(_)) => {}
              }
            }
            let Some(write_res) =
              pooled_write(WriteStream::Shared(stream), resp_pooled, kill_token).await
            else {
              break ReadEnd::Cancelled;
            };
            // 推送写失败 = 连接异常，中断等待断连收尾
            if write_res.is_err() {
              break ReadEnd::Cancelled;
            }
            // pubsub 推送字节镜像累加（C# RespServerSession.cs:1462 Send
            // 唯一出向记账点的空闲等待期承接：订阅会话常驻读挂起，消费段
            // 的 written 与收尾 add_net_bytes 永不重临，推送实发字节就地
            // 计入条目镜像，否则出向流量在监视器/瞬时吞吐/会话采样口径
            // 全盲；会话计数同步随行，杜绝复位与动态字段投影盲区）
            if let Some(entry) = consumer_entry.as_ref() {
              entry.add_net_bytes(0, push_bytes as u64);
              session.mirror_session_counters(entry);
            }
          }
        }
      }
    }
    None => {
      // 纯读阻塞形态：零拷贝网络字节追加直入会话自有接收缓冲
      //（半包残余驻留缓冲头部）；有注册条目时挂取消令牌
      //（KILL/注销打断挂起读）
      match killable(
        stream.read(PrimedVec::from_primed(scratch, prime_key)),
        kill_token,
      )
      .await
      {
        Err(Cancelled) => ReadEnd::Cancelled,
        Ok(bytes) => ReadEnd::Bytes(bytes),
      }
    }
  };

  let BufResult(read_res, wrapped) = match read_end {
    ReadEnd::Cancelled => return Ok(false),
    ReadEnd::Bytes(bytes) => bytes,
  };
  // into_parts 就地展开：代际键先读出，再消耗取缓冲（PrimedVec 单一实现）
  let prime_key = wrapped.prime_key();
  let scratch = wrapped.into_inner();
  let net_in = scratch.len() - before;
  // 取回缓冲，归还先于一切退出路径（半包残余字节必须驻留会话；
  // 代际先于缓冲回写会话，跨读记忆闭环）
  if let Some(session) = session_slot.as_mut() {
    session.set_recv_prime_key(prime_key);
    session.return_recv_scratch(scratch);
  }
  // 入向净增字节就地镜像（字节到达即入账的记账前置点：检查点位于消费
  // 段之后，消费段任意提前退出路径——阻塞挂起 Disposed、慢路径终止、
  // AOF 失败、写错误——与读取段 Ok(0)/EOF/Err 收场都不再重临
  // 检查点，末批入向字节就地入账才不漏计；C# 对位为 RespServerSession
  // .cs:600 TryConsumeMessages 尾部对全部消费字节的 incr_total_net_input
  // _bytes——消费口径真值单源仍在会话侧，此处只补条目镜像）
  if net_in > 0
    && let Some(entry) = consumer_entry.as_ref()
  {
    entry.add_net_bytes(net_in as u64, 0);
  }
  match read_res {
    Ok(0) => return Ok(false), // 对端正常关闭
    Ok(_) => {}
    Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(false),
    Err(e) => return Err(e),
  }
  Ok(true)
}
