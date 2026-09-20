//! Wnode 集成测试共用装配 (`wnode_test`)
//!
//! wnode 层测试夹具：单机会话工厂、服务器启动架、RESP 套接字 IO。
//! 依赖 wnode，故仅限测试 crate 消费（wnode 自身测试经 dev-dependencies 引入）。

use std::{io, mem::forget, net::SocketAddr, num::NonZeroUsize, sync::Arc};

use compio::{
  buf::BufResult,
  io::{AsyncRead, AsyncWriteExt},
  net::TcpStream,
  runtime::Runtime,
};
use tempfile::tempdir;
use waof::{WalConfig, WalLog};
use wdev::SegmentedDevice;
use wkv::{StoreSession, WedbStore};
use wnode::{
  GarnetServer, RespSessionConsumer, SessionProviderFace,
  aof::waof_sublog::{AofSublog, WaofSublog},
  resp::{
    garnet_api::StoreGarnetApi,
    resp_server_session::{RespServerSession, RespServerSessionOptions},
  },
  service::StorageSessionProvider,
};
use wresp::cmd_strings::write_error_raw;
use wtest_base::test_store_config;

/// 构造 RESP 错误帧：期望帧由 `wresp::cmd_strings::write_error_raw`
/// 从单点常量派生，集成测试不再裸写整帧字面量
pub fn err_frame(msg: &str) -> Vec<u8> {
  let mut out = Vec::with_capacity(msg.len() + 3);
  write_error_raw(&mut out, msg);
  out
}

pub type Batch<'a> = wkv::BatchStoreSession<'a, SegmentedDevice>;

/// 会话冲取出面的测试侧读法：应答字节一律经产线单点
/// `RespServerSession::take_output_into` 并入本地缓冲后返回
///（会话侧只此一枚冲出口，测试不再自带同义副本）
pub fn drain_output(session: &mut RespServerSession) -> Vec<u8> {
  let mut out = Vec::new();
  session.take_output_into(&mut out);
  out
}

/// 构造测试环境：`with_range_index` 决定是否挂载 RangeIndex 目录
pub fn test_env(
  with_range_index: bool,
) -> (
  tempfile::TempDir,
  StoreSession<SegmentedDevice>,
  RespServerSession,
) {
  let dir = tempfile::tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("resp.db")).unwrap());
  let mut config = test_store_config();
  config.gc.enabled = false;
  if with_range_index {
    config.range_index_dir = Some(dir.path().join("ri"));
  }
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  let session = store.new_session().unwrap();
  let resp = RespServerSession::default();
  (dir, session, resp)
}

/// 批量写会话环境：统一 setup（[`test_env`]）后进入 batch，闭包内分派 RESP 命令
pub fn with_batch(f: impl FnOnce(&mut RespServerSession, &Batch)) {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (_dir, session, resp) = test_env(false);
    let batch = session.enter_batch();
    let mut s = resp;
    f(&mut s, &batch);
  });
}

/// 单机形态会话工厂（StorageSessionProvider 装饰钩子）
pub type SessionFactory = fn(u64, StoreGarnetApi<SegmentedDevice>) -> Option<RespSessionConsumer>;

/// 单连接会话工厂（无集群切面的单机形态）
#[must_use]
pub fn session_factory(
  network_sender_id: u64,
  api: StoreGarnetApi<SegmentedDevice>,
) -> Option<RespSessionConsumer> {
  Some(RespSessionConsumer::new(
    network_sender_id,
    RespServerSessionOptions::default(),
    Arc::new(api),
  ))
}

/// 起一台随机端口服务器并返回（服务器句柄，实际监听地址）
///
/// # Panics
/// 服务器启动或地址解析失败时 panic（测试环境不预期发生）
pub fn start_server<P: SessionProviderFace + 'static>(
  provider: Arc<P>,
) -> (Arc<GarnetServer<P>>, SocketAddr) {
  let server = Arc::new(
    GarnetServer::new(&["127.0.0.1:0".to_string()], 1 << 16, 8, provider)
      .expect("合法端点构造失败"),
  );
  server.start(NonZeroUsize::new(1)).expect("server start");
  let addr = server.local_addr().expect("local addr");
  (server, addr)
}

/// 起一台临时目录 AOF 节点并返回（服务器句柄，实际监听地址）
///
/// 单机节点装配形态；集群形态装配是 `wedb_test::start_node`（入参
/// ClusterProvider、返回 NodeAssembly 元组），二者签名与语义均不同，勿混引
///
/// 数据目录经 `forget` 随进程存活（测试期反复可连），进程退出由系统清理；
/// `StorageSessionProvider<F>` 的 `F` 即 decorate 类型，故将 [`session_factory`]
/// 显式收敛为函数指针 [`SessionFactory`]，返回类型方可命名
#[must_use]
pub fn start_node() -> (
  Arc<GarnetServer<StorageSessionProvider<SessionFactory>>>,
  SocketAddr,
) {
  let dir = tempdir().expect("tempdir");
  let provider = Arc::new(
    StorageSessionProvider::open_with_config_and_aof(
      test_store_config(),
      dir.path().join("node.db"),
      None,
      None,
      session_factory as SessionFactory,
    )
    .expect("open with aof"),
  );
  forget(dir);
  start_server(provider)
}

/// 写出命令载荷（RESP 数组帧）
pub async fn send_cmd(stream: &mut TcpStream, args: &[&[u8]]) -> io::Result<()> {
  let frame = wtest_base::resp_frame(args);
  let BufResult(res, _) = stream.write_all(frame).await;
  res
}

/// 写出原始字节载荷（内联命令 / 流水线字节流）
pub async fn send(stream: &mut TcpStream, data: &[u8]) -> io::Result<()> {
  let BufResult(res, _) = stream.write_all(data.to_vec()).await;
  res
}

/// 读取一条完整行式应答（+OK / -ERR / :N；对端断开返回已收字节）
pub async fn read_line_reply(stream: &mut TcpStream) -> Vec<u8> {
  let mut acc = Vec::new();
  let mut buf = vec![0u8; 512];
  loop {
    let BufResult(res, returned) = stream.read(buf).await;
    buf = returned;
    match res {
      Ok(0) | Err(_) => return acc,
      Ok(n) => {
        acc.extend_from_slice(&buf[..n]);
        // 行式应答：读到行尾即完整
        if acc.ends_with(b"\r\n") {
          return acc;
        }
      }
    }
  }
}

/// 读取一条完整 RESP 应答（行式 / bulk / 数组逐帧累积；对端断开返回已收字节）
pub async fn read_reply(stream: &mut TcpStream) -> Vec<u8> {
  let mut acc = Vec::new();
  let mut buf = vec![0u8; 4096];
  loop {
    let BufResult(res, returned) = stream.read(buf).await;
    buf = returned;
    match res {
      Ok(0) | Err(_) => return acc,
      Ok(n) => {
        acc.extend_from_slice(&buf[..n]);
        if complete_len(&acc).is_some() {
          return acc;
        }
      }
    }
  }
}

/// 计算缓冲区首条完整 RESP 应答的字节长度（不完整返回 None）
#[must_use]
pub fn complete_len(data: &[u8]) -> Option<usize> {
  if data.is_empty() {
    return None;
  }
  let kind = data[0];
  let nl = data.iter().position(|&b| b == b'\n')?;
  match kind {
    // 行式帧（+OK / -ERR / :N）：头部行到齐即完整（header 非数字，不可先解析）
    b'+' | b'-' | b':' => Some(nl + 1),
    b'$' => {
      let header: i64 = String::from_utf8_lossy(&data[1..nl])
        .trim_end()
        .parse()
        .ok()?;
      if header < 0 {
        Some(nl + 1)
      } else {
        (data.len() >= nl + 1 + header as usize + 2).then(|| nl + 1 + header as usize + 2)
      }
    }
    // 聚合帧（* 数组 / % map）：header 为子帧数（map 的 header 是对数，子帧翻倍）
    b'*' | b'%' => {
      let header: i64 = String::from_utf8_lossy(&data[1..nl])
        .trim_end()
        .parse()
        .ok()?;
      if header < 0 {
        return Some(nl + 1);
      }
      let items = if kind == b'%' { header * 2 } else { header };
      let mut rest = &data[nl + 1..];
      let mut total = nl + 1;
      for _ in 0..items {
        let used = complete_len(rest)?;
        rest = &rest[used..];
        total += used;
      }
      Some(total)
    }
    _ => None,
  }
}

/// 便利封装：发送 RESP 数组帧命令并取回一条完整应答
pub async fn cmd(stream: &mut TcpStream, args: &[&[u8]]) -> Vec<u8> {
  send_cmd(stream, args).await.expect("send cmd");
  read_reply(stream).await
}

/// 读取 bulk 应答载荷（$N\r\n<body>\r\n；nil 帧返回 None）
pub async fn read_bulk_reply(stream: &mut TcpStream) -> Option<Vec<u8>> {
  let mut acc = Vec::new();
  let mut buf = vec![0u8; 512];
  loop {
    let BufResult(res, returned) = stream.read(buf).await;
    buf = returned;
    match res {
      Ok(0) | Err(_) => return None,
      Ok(n) => {
        acc.extend_from_slice(&buf[..n]);
        if acc.starts_with(b"$-1\r\n") {
          return None;
        }
        // 帧完整性：头部行 + 定长载荷到齐
        if let Some(nl) = acc.iter().position(|&b| b == b'\n')
          && let Ok(len) = String::from_utf8_lossy(&acc[1..nl])
            .trim_end()
            .parse::<usize>()
          && acc.len() >= nl + 1 + len + 2
        {
          return Some(acc[nl + 1..nl + 1 + len].to_vec());
        }
      }
    }
  }
}

/// 轻量真实段设备子日志：tempdir + `SegmentedDevice` 单文件 + `WalLog`
/// 默认配置（测试统一走真实设备，杜绝 mock 抽象；目录随 [`TempDir`]
/// 存活，Drop 自动清理段文件）
pub fn test_sublog(tag: &str) -> (tempfile::TempDir, Arc<AofSublog>) {
  test_sublog_with_config(tag, WalConfig::default())
}

/// 指定 [`WalConfig`] 的子日志变体：大记录分块写入场景（单条预留须完整
/// 落入环形窗口）按写入总量放大 `buffer_size`
pub fn test_sublog_with_config(
  tag: &str,
  config: WalConfig,
) -> (tempfile::TempDir, Arc<AofSublog>) {
  let dir = tempfile::tempdir().unwrap();
  let device =
    Arc::new(SegmentedDevice::single_file(dir.path().join(format!("{tag}.wal"))).unwrap());
  let wal = WalLog::new(device, config).unwrap();
  (dir, Arc::new(WaofSublog::new(Arc::new(wal))))
}

/// 批量构造轻量真实段设备子日志（分片拓扑测试装配）
pub fn test_sublogs(tag: &str, count: usize) -> (Vec<tempfile::TempDir>, Vec<Arc<AofSublog>>) {
  test_sublogs_with_config(tag, count, WalConfig::default())
}

/// 批量构造指定配置的子日志（分片拓扑测试装配）
pub fn test_sublogs_with_config(
  tag: &str,
  count: usize,
  config: WalConfig,
) -> (Vec<tempfile::TempDir>, Vec<Arc<AofSublog>>) {
  let pairs: Vec<_> = (0..count)
    .map(|i| test_sublog_with_config(&format!("{tag}_{i}"), config))
    .collect();
  let (dirs, logs) = pairs.into_iter().unzip();
  (dirs, logs)
}
