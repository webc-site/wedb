//! 握手/入账段：收首批识别 WireFormat 并装配会话
//!
//! 在 garnet 中的相对路径:
//! - `libs/common/Networking/NetworkHandler.cs`（Process → serverHook.TryCreateMessageConsumer 装配点）
//!
//! 对齐 C# `RespServerSession` 的 partial 分文件组织：本件为 drive_loop
//! 握手阶段分体（原内联块逐字迁移）。

use std::io;

use compio::{BufResult, runtime::Cancelled};
use wbase::primed::PrimedVec;

use super::{
  super::{
    NetworkHandler,
    buffer::{MIN_HANDSHAKE_BYTES, MIN_READ_SPACE},
  },
  PumpEnv, killable,
  write::shrink_to_base,
};
use crate::{
  net::stream::ConnectionStream,
  traits::{MessageConsumerFace, SessionProviderFace, WireFormat},
};

impl<C: MessageConsumerFace> NetworkHandler<C> {
  /// 返回 false = 握手期收场（对端关闭 / 令牌取消 / 意外 EOF），泵循环
  /// 就此退出；true = 会话已装配，进入消费驱动序
  pub(super) async fn handshake<P: SessionProviderFace<Consumer = C>>(
    &mut self,
    stream: &mut ConnectionStream,
    env: &PumpEnv<'_, P>,
  ) -> io::Result<bool> {
    let PumpEnv {
      session_provider,
      sender_id,
      kill_token,
      buffer_size,
    } = *env;
    // 握手批净入字节（并入消费段首轮镜像，监视器字节口径与批次数对齐）
    let mut handshake_net_in = 0usize;

    // ── 握手段：收首批识别 WireFormat 并装配会话（C# Process →
    // serverHook.TryCreateMessageConsumer 装配点）。握手期会话未建，批次
    // 字节先落池化缓冲；会话创建点把未消费字节一次性迁入会话自有接收
    // 缓冲（此迁移点会话游标必为零，字节流无缝衔接、无重复并入），旧池
    // 缓冲随即 RAII 归还，此后网络字节零拷贝直入会话缓冲。
    // 注册表条目由 accept 循环预注册（C# GarnetServerTcp.cs:256 TryAdd
    // 先于 handler.Start），本函数不再注册
    {
      let mut pooled = self.buffer_pool.get_ref(0);
      loop {
        let mut raw_buf = pooled.take_buffer().expect("pooled buffer active");
        // 空闲空间不足预留阈值：补足一个读取阈值量（握手期无消费，无需平移）。
        // 预留量随阈值走、不随硬编码规格——池块扩容经 amortized 倍增保持
        // 2 的幂，恒落池层级域内，归还端按容量精准配平回池；曾以
        // DEFAULT_BUFFER_SIZE(65536) 硬编码预留，非默认 network_buffer_size
        // 下借出块被扩至越界容量，归还判失配就地丢弃，池失一块常驻配额
        if raw_buf.capacity() - raw_buf.len() < MIN_READ_SPACE {
          raw_buf.reserve(MIN_READ_SPACE);
        }
        let before = raw_buf.len();
        // KILL/停机打断握手期挂起读（预注册条目治理面）：取消时读缓冲随
        // future 失（compio 读取消语义，与消费段同口径），走收场尾巴
        //（shutdown → dispose 注销）
        let BufResult(read_res, wrapped) =
          match killable(stream.read(PrimedVec::new(raw_buf)), kill_token).await {
            Err(Cancelled) => return Ok(false),
            Ok(pair) => pair,
          };
        raw_buf = wrapped.into_inner();
        handshake_net_in += raw_buf.len() - before;
        pooled.set_buffer(raw_buf);

        match read_res {
          Ok(0) => return Ok(false), // 对端在会话建立前关闭
          Ok(_) => {}
          Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(false),
          Err(e) => return Err(e),
        }

        if pooled.vec_ref().len() < MIN_HANDSHAKE_BYTES {
          continue; // 首批不足识别字节数，继续读
        }

        let mut session = session_provider
          .get_session(WireFormat::Ascii, sender_id)
          .ok_or_else(|| {
            io::Error::new(io::ErrorKind::ConnectionRefused, "会话提供者拒绝建立会话")
          })?;

        // 未消费批次字节迁入会话自有接收缓冲（握手期无消费，整段迁移）
        let mut scratch = session.take_recv_scratch();
        scratch.extend_from_slice(pooled.vec_ref());
        session.return_recv_scratch(scratch);
        // 握手批净入字节就地镜像（C# RespServerSession.cs:600 TryConsumeMessages
        // 尾部对全部消费字节 incr_total_net_input_bytes 的条目侧承接：迁移字节
        // 驻留会话缓冲，但会话侧消费口径被条目镜像覆盖屏蔽（consumer_registry
        // monitor_sample 以 net_input_bytes 覆盖快照），收尾检查点前置至此——
        // 字节到达即入账，首轮消费段任意提前退出路径不再漏计）
        if let Some(entry) = &self.consumer_entry {
          entry.add_net_bytes(handshake_net_in as u64, 0);
        }
        // 握手块低水位收敛：分片到站触发阈值预留扩容的块，清空后缩回池基准
        // 规格再归还，恒回基础层级——扩容块若原样析构将漂移高级层级，逐连接
        // 蚕食基础层级闲置配额致穿透堆分配（对标 C# NetworkHandler.cs:529
        // ShrinkNetworkReceiveBuffer 低水位收缩，与响应缓冲写出复位同一收敛单点）
        pooled.vec_mut().clear();
        shrink_to_base(pooled.vec_mut(), buffer_size);
        drop(pooled);

        // 会话装配（端点对取 handler 构造期捕获单源——remote/local 同为
        // accept 侧已知值，TLS 臂流层擦除不可见故前置捕获，见
        // NetworkHandler::set_session）
        self.set_session(session);
        break;
      }
    }
    Ok(true)
  }
}
