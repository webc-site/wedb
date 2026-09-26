//! 网络缓冲池借还契约回归（task/ing/wnode-network-buffer-pool-swap-leak-and-capacity-desync）
//!
//! 缺陷面一（身份置换泄漏）：`RespServerSession::take_output_into` 曾以
//! `swap` 整段换出会话输出与目标池化缓冲，池化块与会话私有块所有权边界
//! 被撕裂——池常驻块随会话析构释放进系统堆（永久泄漏），超规块在
//! `PooledRefBuffer::drop` 判定丢弃永不回池。C# 原型（
//! libs/common/Networking/GarnetTcpNetworkSender.cs:EnterAndGetResponseObject /
//! libs/server/Resp/RespServerSession.cs:Send）中会话直接向网络发送器的
//! 池化响应缓冲写出、发送完毕无损归还，不存在任何身份置换。
//!
//! 缺陷面二（容量硬编码脱节）：驱动泵曾以 DEFAULT_BUFFER_SIZE(65536) 常量
//! 借出/复位响应缓冲，无视 ServerBootstrap/GarnetServer 配置的
//! network_buffer_size；配置非默认值时借出规格恒不等于池基准规格，
//! 池弹出与归还双双落空，缓冲池彻底失效。

use std::{net::SocketAddr, num::NonZeroUsize, sync::Arc, time::Duration};

use compio::{
  BufResult,
  io::{AsyncRead, AsyncWriteExt},
  net::TcpStream,
  runtime::Runtime,
  time::sleep,
};
use wbase::pool::{DEFAULT_BUFFER_SIZE, LimitedFixedBufferPool};
use wnode::{
  GarnetServer, SessionProviderFace, WireFormat,
  resp::{
    garnet_api::GarnetApiFace,
    resp_server_session::{RespServerSession, RespServerSessionOptions},
    resp_session_consumer::RespSessionConsumer,
    slow_path::SlowFuture,
  },
};
use wresp::command::RespCommand;

const PING_FRAME: &[u8] = b"*1\r\n$4\r\nPING\r\n";
const PONG_REPLY: &[u8] = b"+PONG\r\n";
const QUIT_FRAME: &[u8] = b"*1\r\n$4\r\nQUIT\r\n";

/// 冲取出面身份不变式：`take_output_into` 只搬运字节，绝不置换缓冲身份。
/// 池化块留在句柄并完整回池，会话私有块留在会话——两条生命周期各自独立
#[test]
fn take_output_into_preserves_buffer_identity() {
  let pool = LimitedFixedBufferPool::new(DEFAULT_BUFFER_SIZE, 16);
  let mut session = RespServerSession::default();
  let private_ptr = session.output.as_ptr();
  // 60KB 载荷：低于池基准块与私有块的 64KB 容量，全程零扩容干扰
  let payload = vec![b'x'; 60 * 1024];
  session.output.extend_from_slice(&payload);

  let mut handle = pool.get_ref(DEFAULT_BUFFER_SIZE);
  let issued_ptr = handle.vec_ref().as_ptr();
  session.take_output_into(handle.vec_mut());

  assert_eq!(&handle[..], &payload[..], "冲出字节一致");
  assert!(session.output.is_empty(), "冲出后会话输出复位");
  assert_eq!(
    handle.vec_ref().as_ptr(),
    issued_ptr,
    "池化块必须留在句柄内，take_output_into 不得将其与会话私有块置换（swap 泄漏根因）"
  );
  assert_eq!(
    session.output.as_ptr(),
    private_ptr,
    "会话 output 必须保留自身私有块，不得接管池化块身份"
  );

  drop(handle);
  assert_eq!(pool.borrowed_count(), 0);
  assert_eq!(pool.free_count(), 1, "池化块须完整回池");
  assert_eq!(pool.allocated_count(), 1, "全程零穿透堆分配");

  // 后续借出复用同一池常驻块（置换泄漏形态下池已永久失去该块，此处穿透堆）
  let h2 = pool.get_ref(DEFAULT_BUFFER_SIZE);
  assert_eq!(
    h2.vec_ref().as_ptr(),
    issued_ptr,
    "后续借出必须命中池常驻块，不得穿透系统堆"
  );
  assert_eq!(pool.allocated_count(), 1);
  drop(h2);

  // 会话析构：私有块归系统堆，池存量分毫不动
  drop(session);
  assert_eq!(pool.free_count(), 1, "会话析构不得带走池常驻块");
  assert_eq!(pool.allocated_count(), 1);
}

/// 存储执行域桩：本用例只驱动 PING/QUIT（会话进程内自处理），执行域不可达
struct NeverApi;

impl GarnetApiFace for NeverApi {
  fn exec(&self, session: &mut RespServerSession, cmd: RespCommand, _args: &[&[u8]]) {
    let _ = session;
    panic!("PING/QUIT 不经存储执行域，cmd={cmd:?} 意外下达");
  }

  fn exec_slow(
    self: Arc<Self>,
    _cmd: RespCommand,
    _args: Vec<Vec<u8>>,
    _resp_version: u8,
  ) -> SlowFuture {
    SlowFuture::new(async { Vec::new() })
  }
}

struct PongProvider;

impl SessionProviderFace for PongProvider {
  type Consumer = RespSessionConsumer;

  fn get_session(&self, _wf: WireFormat, network_sender: u64) -> Option<RespSessionConsumer> {
    Some(RespSessionConsumer::new(
      network_sender,
      RespServerSessionOptions::default(),
      Arc::new(NeverApi),
    ))
  }
}

/// 期望字节数读完（累计，容忍 TCP 分段）
async fn read_until(stream: &mut TcpStream, acc: &mut Vec<u8>, expect: usize) {
  while acc.len() < expect {
    let BufResult(res, ret) = stream.read(vec![0u8; 4096]).await;
    let n = res.expect("对端提前关闭");
    assert!(n > 0, "对端提前关闭（已收 {} 字节）", acc.len());
    acc.extend_from_slice(&ret[..n]);
  }
}

/// 单连接双轮收发：批内双 PING（一轮冲出）→ QUIT 断连（次轮冲出），
/// 驱动泵两段写出各经历一次冲取出面
async fn drive_connection(addr: SocketAddr) {
  let mut stream = TcpStream::connect(addr).await.unwrap();
  stream
    .write_all([PING_FRAME, PING_FRAME].concat().to_vec())
    .await
    .unwrap();
  let mut acc = Vec::new();
  read_until(&mut stream, &mut acc, 2 * PONG_REPLY.len()).await;
  assert_eq!(&acc, &[PONG_REPLY, PONG_REPLY].concat()[..]);

  stream.write_all(QUIT_FRAME.to_vec()).await.unwrap();
  let mut acc = Vec::new();
  read_until(&mut stream, &mut acc, b"+OK\r\n".len()).await;
  assert_eq!(&acc, &b"+OK\r\n"[..]);
  drop(stream);
  // 等在途泵收场（resp_pooled 随 drive_loop 返回 RAII 归还）
  sleep(Duration::from_millis(80)).await;
}

/// 配置面契约：network_buffer_size 非默认值（4096）下，驱动泵借出/复位
/// 必须按池基准规格（`buffer_pool.buffer_size()`）进行——池命中归还成立、
/// allocated_count 不随连接数单调自增。
///
/// 修复前两处红态：硬编码 65536 使每次借出穿透越界分配且归还判超规丢弃
/// （free 恒 0、out_of_bound 累计）；即便仅解除硬编码，swap 身份置换仍令
/// 每连接私有超规块被丢弃、池化块被会话析构带走（allocated 线性自增）。
#[test]
fn drive_pool_reuses_configured_buffer_size() {
  let provider = Arc::new(PongProvider);
  let server =
    GarnetServer::new(&["127.0.0.1:0".to_string()], 4096, 8, Arc::clone(&provider)).unwrap();
  server.start(NonZeroUsize::new(1)).unwrap();
  let addr = server.local_addr().unwrap();
  let pool = server.buffer_pool().clone();

  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    for _ in 0..4 {
      drive_connection(addr).await;
    }
  });

  // 借出/归还全链路命中池基准规格：零越界穿透
  assert_eq!(
    pool.out_of_bound_allocations(),
    0,
    "配置 4096 时驱动泵不得以硬编码 65536 越界借出（修复前恒 >0）"
  );
  assert_eq!(pool.borrowed_count(), 0, "全部连接收场后无在途借出");
  assert!(
    pool.free_count() >= 1,
    "连接收场后池化块须回池（修复前恒 0：归还判超规丢弃）"
  );

  // 预热后追加连接：池常驻块复用，allocated_count 不再自增
  let warmed = pool.allocated_count();
  rt.block_on(async {
    for _ in 0..3 {
      drive_connection(addr).await;
    }
  });
  assert_eq!(
    pool.allocated_count(),
    warmed,
    "后续连接必须复用池常驻块，allocated_count 不随连接单调自增（swap 泄漏/越界穿透形态下线性增长）"
  );
  assert_eq!(pool.out_of_bound_allocations(), 0);

  server.stop();
}

/// 分片握手段契约：首批不足识别字节数时，握手段池块空闲不足阈值只补足
/// 一个读取阈值量（MIN_READ_SPACE），握手迁移后清空并低水位收敛回池基准
/// 规格，归还恒回基础层级——allocated_count 不随连接数自增。
///
/// 修复前红态：以 DEFAULT_BUFFER_SIZE(65536) 硬编码预留，4096 池下每连接
/// 握手块被扩至 65537（非层级容量）归还判失配就地丢弃，基础层级配额逐
/// 连接流失，池穿透堆分配。
#[test]
fn handshake_fragment_preserves_pool_quota() {
  let provider = Arc::new(PongProvider);
  let server =
    GarnetServer::new(&["127.0.0.1:0".to_string()], 4096, 8, Arc::clone(&provider)).unwrap();
  server.start(NonZeroUsize::new(1)).unwrap();
  let addr = server.local_addr().unwrap();
  let pool = server.buffer_pool().clone();

  // 分片驱动：首字节单发（服务端读后 len=1 < 4 续读，触发阈值预留扩容），
  // 间隔确保分片不合并到站，余帧续发完成识别
  async fn fragmented_connection(addr: SocketAddr) {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream.write_all(b"*".to_vec()).await.unwrap();
    sleep(Duration::from_millis(60)).await;
    stream
      .write_all(b"1\r\n$4\r\nPING\r\n".to_vec())
      .await
      .unwrap();
    let mut acc = Vec::new();
    read_until(&mut stream, &mut acc, PONG_REPLY.len()).await;
    assert_eq!(&acc, PONG_REPLY);
    drop(stream);
    // 等在途泵收场（握手块与响应块随 drive_loop 返回 RAII 归还）
    sleep(Duration::from_millis(80)).await;
  }

  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    for _ in 0..3 {
      fragmented_connection(addr).await;
    }
  });
  let warmed = pool.allocated_count();
  rt.block_on(async {
    for _ in 0..3 {
      fragmented_connection(addr).await;
    }
  });

  assert_eq!(
    pool.allocated_count(),
    warmed,
    "分片握手扩容块须低水位收敛回基础层级复用，allocated_count 不随连接自增（修复前扩容块逐连接失配丢弃）"
  );
  assert_eq!(pool.borrowed_count(), 0, "全部连接收场后无在途借出");
  assert!(pool.free_count() >= 1, "池常驻块须留存供后续连接复用");

  server.stop();
}
