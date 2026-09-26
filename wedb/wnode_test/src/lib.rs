//! Wnode 集成测试共用装配 (`wnode_test`)
//!
//! wnode 层测试夹具：单机会话工厂、服务器启动架、RESP 套接字 IO。
//! 依赖 wnode，故仅限测试 crate 消费（wnode 自身测试经 dev-dependencies 引入）。

use std::{
  io,
  mem::{forget, take},
  net::SocketAddr,
  num::NonZeroUsize,
  sync::Arc,
};

use compio::{
  buf::BufResult,
  io::{AsyncRead, AsyncWriteExt},
  net::TcpStream,
  runtime::Runtime,
};
use tempfile::tempdir;
use waof::{WalConfig, WalLog};
use wconf::RuntimeServerOptions;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, StoreSession, WedbStore};
use wmetric::SessionMetricsHandle;
use wnode::{
  GarnetServer, MessageConsumerFace, RespSessionConsumer, SessionProviderFace,
  aof::{
    replay_input::ReplayInput,
    waof_sublog::{AofSublog, WaofSublog},
  },
  resp::{
    garnet_api::{GarnetApi, StoreGarnetApi},
    resp_server_session::{RespServerSession, RespServerSessionOptions},
    vector::{
      vector_manager::{VectorManager, VectorManagerOptions},
      vector_store_callbacks::{OwnedActiveVectorSession, WedbVectorStoreCallbacks},
    },
  },
  service::StorageSessionProvider,
  storage::session::storage_session::version_map_watch_hook,
};
use wresp::{cmd_strings::write_error_raw, command::RespCommand};
use wtest_base::{open_test_store, resp_frame, test_store_config};
use wtxn::WatchVersionMap;
use wvector::Callbacks;

/// 构造 RESP 错误帧：期望帧由 `wresp::cmd_strings::write_error_raw`
/// 从单点常量派生，集成测试不再裸写整帧字面量
pub fn err_frame(msg: &str) -> Vec<u8> {
  let mut out = Vec::with_capacity(msg.len() + 3);
  write_error_raw(&mut out, msg);
  out
}

pub type Batch<'a> = wkv::BatchStoreSession<'a, SegmentedDevice>;

/// AOF 回放记录输入段装配单源：`ReplayInput` 九字段形中除 `cmd` 与 `args` 外恒为 0，
/// 测试侧 17 处逐字节同形的「建缓冲 → 填九字段 → serialize」收口于此
/// （`args` 由入参原样承接，字段序与取值零变化）
#[must_use]
pub fn replay_input_bytes(cmd: RespCommand, args: Vec<Vec<u8>>) -> Vec<u8> {
  let mut input = Vec::new();
  ReplayInput {
    cmd,
    flags: 0,
    sub_id: 0,
    obj_type: 0,
    arg1: 0,
    arg2: 0,
    arg3: 0,
    args,
  }
  .serialize(&mut input);
  input
}

/// 会话冲取出面的测试侧读法：应答字节一律经产线单点
/// `RespServerSession::take_output_into` 并入本地缓冲后返回
///（会话侧只此一枚冲出口，测试不再自带同义副本）
pub fn drain_output(session: &mut RespServerSession) -> Vec<u8> {
  let mut out = Vec::new();
  session.take_output_into(&mut out);
  out
}

/// 单命令同步执行泵：快路径应答直取，挂起慢路径（SlowWait）则阻塞闭环取应答
///
/// 对标测试侧 22 份同形泵收口（wnode/tests 各集成测试私义 `auto_exec`，
/// 函数体逐字节一致，panic 文案保持原样以免期望漂移）
pub fn auto_exec(
  api: &GarnetApi,
  rt: &Runtime,
  s: &mut RespServerSession,
  cmd: RespCommand,
  args: &[&[u8]],
) -> Vec<u8> {
  s.output.clear();
  api.exec(s, cmd, args);
  if !s.output.is_empty() {
    return take(&mut s.output);
  }
  let slow = s
    .take_slow_wait()
    .unwrap_or_else(|| panic!("命令 {cmd} 无输出且未挂起慢路径"));
  let out = rt.block_on(slow.resolve());
  s.output.clear();
  out
}

/// 投帧后跑一次同步消费，回 `(剩余未消费字节数, 同步段应答)`（不含慢路径闭环）
///
/// 对标测试侧 25 份同形泵收口：各集成测试私义 `pump` 函数体逐字节一致
pub fn pump(consumer: &mut RespSessionConsumer, frame: &[u8]) -> (Option<usize>, Vec<u8>) {
  let mut scratch = consumer.take_recv_scratch();
  scratch.extend_from_slice(frame);
  consumer.return_recv_scratch(scratch);
  let mut resp = Vec::new();
  let remaining = consumer.try_consume_messages_into(&mut resp);
  (remaining, resp)
}

/// 命令入参经 RESP2 数组帧单源组帧后投喂，取同步段应答（剩余量不入判）
pub fn feed(c: &mut RespSessionConsumer, args: &[&[u8]]) -> Vec<u8> {
  let (_, out) = pump(c, &resp_frame(args));
  out
}

/// 快慢两臂闭环往返：同步段应答在前，挂起慢路径则阻塞续跑并并入同一应答
pub fn roundtrip(rt: &Runtime, c: &mut RespSessionConsumer, args: &[&[u8]]) -> Vec<u8> {
  let mut out = feed(c, args);
  if let Some(slow) = c.take_slow_wait() {
    rt.block_on(async { out.extend_from_slice(&slow.resolve().await) });
  }
  out
}

/// read_accounting 簇快慢两臂共持同一采样句柄的环境单源：api（慢路径存储
/// 会话经 with_session_metrics 下传）+ 消费者（快臂会话经 attach 注入）
/// （对标 wnode/tests 四文件同形 `fn env(tag)` 副本收口，字段序与装配步序
/// 逐字保留；`_dir` 置于返回元组倒数第二位，Drop 时机与原局部形等价）
#[must_use]
pub fn metrics_env(
  tag: &str,
) -> (
  Runtime,
  RespSessionConsumer,
  GarnetApi,
  Arc<SessionMetricsHandle>,
  tempfile::TempDir,
  Arc<WedbStore<SegmentedDevice>>,
) {
  let (_dir, store) = open_test_store(tag).unwrap();
  let handle = Arc::new(SessionMetricsHandle::default());
  let api: GarnetApi = Arc::new(
    StoreGarnetApi::new(store.new_session().unwrap())
      .with_session_metrics(Some(Arc::clone(&handle))),
  );
  let mut c = RespSessionConsumer::new(1, RespServerSessionOptions::default(), api.clone());
  c.attach_session_metrics(Some(Arc::clone(&handle)));
  (Runtime::new().unwrap(), c, api, handle, _dir, store)
}

/// (found, notfound) 指标读数对
#[must_use]
pub fn counts(handle: &SessionMetricsHandle) -> (u64, u64) {
  let m = handle.snapshot();
  (m.get_total_found(), m.get_total_notfound())
}

/// 向量窗共享执行域装配单源：真存储 + 向量登记表 + 两个独立会话消费者
/// （thread-per-core 生产形态：注入者与 victim 各持一份会话，同挂一份
/// StoreGarnetApi；对标 wnode/tests 两文件同形 `fn env(tag)` 副本收口，
/// 字段序与装配步序逐字保留）
#[must_use]
pub fn vector_env(
  tag: &str,
) -> (
  GarnetApi,
  RespSessionConsumer,
  RespSessionConsumer,
  Arc<VectorManager>,
  Arc<WedbStore<SegmentedDevice>>,
) {
  let dir = tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag)).unwrap());
  let config = test_store_config();
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  let session = store.new_session().unwrap();

  // 回调无状态：向量会话按执行域绑定（先例 resp_vector_set_wrong_type.rs）
  let _vector_domain = OwnedActiveVectorSession::new(store.new_session().unwrap());
  let callbacks = Callbacks::new(Arc::new(WedbVectorStoreCallbacks::<SegmentedDevice>::new()));
  let vm = Arc::new(VectorManager::new(
    VectorManagerOptions {
      is_enabled: true,
      ..Default::default()
    },
    callbacks,
  ));
  forget(dir);
  let api: GarnetApi = Arc::new(StoreGarnetApi::new(session).with_vector_manager(Arc::clone(&vm)));
  let c1 = RespSessionConsumer::new(1, RespServerSessionOptions::default(), Arc::clone(&api));
  let c2 = RespSessionConsumer::new(2, RespServerSessionOptions::default(), Arc::clone(&api));
  (api, c1, c2, vm, store)
}

/// 停车臂驱动循环（网络泵 drive_loop 消费后臂的测试对位）
///
/// 循环驱动至无挂起：ACL 挂载刷新（重驱型，刷新后重入消费重解析重评）、
/// AUTH/HELLO/ACL 族（产应答型，应答冲入 `resp_buf` 后续消费流水线余量）、
/// 慢路径挂起（应答按流水线顺序并回）。续消费与应答冲出经
/// `RespServerSession` 的产线单点（`try_consume_messages` +
/// `take_output_into`），与泵侧 `try_consume_messages_into` 同口径
pub async fn drive_pending_parks(
  session: &mut RespServerSession,
  resp_buf: &mut Vec<u8>,
  drive_slow: bool,
) {
  loop {
    // 重驱型刷新臂：点查后重入消费，门链以新挂载重评
    if session.take_pending_acl_refresh() {
      if let Some(api) = session.garnet_api.clone() {
        api.exec_acl_refresh(session).await;
      }
      if session.try_consume_messages().is_some() {
        session.take_output_into(resp_buf);
      }
      continue;
    }
    // 产应答型异步臂：认证/规则读写回写会话本地态，应答冲出后续消费
    if let Some((cmd, args, parked_output_len)) = session.take_pending_auth_acl() {
      let views: Vec<&[u8]> = args.iter().map(|a| a.as_slice()).collect();
      if let Some(api) = session.garnet_api.clone() {
        let _ = api.exec_auth_acl(session, cmd, &views).await;
      }
      // 泵侧闭环补计（与产线 drive.rs 同口）：await 完成后、冲出前
      session.account_parked_auth_acl_failure(cmd, parked_output_len);
      session.take_output_into(resp_buf);
      if session.try_consume_messages().is_some() {
        session.take_output_into(resp_buf);
      }
      continue;
    }
    // 慢路径挂起（如 AUTH 冷上下文装载）：await 闭环，应答按流水线并回；
    // 冷上下文撕裂断言等需自驱慢路径的 harness 传 false 保留挂起体
    if drive_slow && let Some(slow) = session.take_slow_wait() {
      let reply = slow.resolve().await;
      session.resolve_slow_wait_into(&reply, resp_buf);
      continue;
    }
    // 脚本内挂起（EVAL 内阻塞/慢命令命中，协程化承接）：泵同款 async 续跑
    // 脚本协程至完成，应答随续跑窗口并入会话输出
    if session.has_script_suspend() {
      session.resume_suspended_script(resp_buf).await;
      session.take_output_into(resp_buf);
      if session.try_consume_messages().is_some() {
        session.take_output_into(resp_buf);
      }
      continue;
    }
    break;
  }
}

/// 停车臂驱动循环的消费者面变体（续消费经
/// `MessageConsumerFace::try_consume_messages_into`，应答冲出同一 `resp_buf`）
pub async fn drive_pending_parks_consumer(
  consumer: &mut RespSessionConsumer,
  resp_buf: &mut Vec<u8>,
) {
  loop {
    if consumer.take_pending_acl_refresh() {
      consumer.pending_acl_refresh_fut().await;
      consumer.flush_output_into(resp_buf);
      consumer.try_consume_messages_into(resp_buf);
      continue;
    }
    if let Some((cmd, args, parked_output_len)) = consumer.take_pending_auth_acl() {
      let views: Vec<&[u8]> = args.iter().map(|a| a.as_slice()).collect();
      consumer.pending_auth_acl_fut(cmd, &views).await;
      consumer.account_parked_auth_acl_failure(cmd, parked_output_len);
      consumer.flush_output_into(resp_buf);
      consumer.try_consume_messages_into(resp_buf);
      continue;
    }
    if let Some(slow) = consumer.take_slow_wait() {
      let reply = slow.resolve().await;
      consumer.resolve_slow_wait_into(&reply, resp_buf);
      continue;
    }
    // 脚本内挂起（EVAL 内阻塞/慢命令命中，协程化承接）：泵同款 async 续跑
    // 脚本协程至完成，续跑后由调用方重入消费承接挂起窗口之后的流水线余量
    if consumer.has_script_suspend() {
      consumer.resume_suspended_script_fut(resp_buf).await;
      consumer.try_consume_messages_into(resp_buf);
      continue;
    }
    break;
  }
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

/// 分层族集成测试存储别名（各测试本地 `type TestStore` 副本收口）
pub type TestStore = WedbStore<SegmentedDevice>;

/// 分层族测试环境：真实引擎 + 版本表写面钩子 + 存储门面 API（字段直曝，
/// 对标 wnode/tests 各集成测试本地 `Env` 四字段同形副本）
pub struct TestEnv {
  pub rt: Runtime,
  pub store: Arc<TestStore>,
  pub api: GarnetApi,
  pub _dir: tempfile::TempDir,
}

/// 分层族 `fn env(tag)` 样板单源收口：tempdir + 页容 2048/1MB/16 盘/0.5
/// 预算 + 单文件段设备 + 引擎级写面钩子首挂断言（构造参数与断言文案逐字
/// 保留，勿与 [`test_store_config`] 推导形混同）
#[must_use]
pub fn tiered_env(tag: &str) -> TestEnv {
  let dir = tempdir().unwrap();
  let config = StoreConfig::new(2048, 1024 * 1024, 16, 0.5).unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag)).unwrap());
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  assert!(
    store.set_watch_hook(version_map_watch_hook(Arc::new(WatchVersionMap::new(
      1 << 10
    )))),
    "引擎级写面钩子应首次挂载"
  );
  let api: GarnetApi = Arc::new(StoreGarnetApi::new(store.new_session().unwrap()));
  TestEnv {
    rt: Runtime::new().unwrap(),
    store,
    api,
    _dir: dir,
  }
}

/// 分层族会话装配：默认选项会话挂 [`TestEnv`] 存储门面 API
#[must_use]
pub fn session_with(env: &TestEnv) -> RespServerSession {
  let mut s = RespServerSession::new(1, RespServerSessionOptions::default());
  s.set_garnet_api(env.api.clone());
  s
}

/// 分层族会话装配指定协议版变体：[`session_with`] 装配后覆写 RESP 协议版本
///（对标 wnode/tests/tiered_output_frame_head 本地带版 `session_with` 副本）
#[must_use]
pub fn session_with_ver(env: &TestEnv, resp_protocol_version: u8) -> RespServerSession {
  let mut s = session_with(env);
  s.resp_protocol_version = resp_protocol_version;
  s
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

/// 起一台临时目录 AOF 节点并返回（临时目录，服务器句柄，实际监听地址）
///
/// 单机节点装配形态；集群形态装配是 `wedb_test::start_node`（入参
/// ClusterProvider、返回 NodeAssembly 元组），二者签名与语义均不同，勿混引
///
/// 数据目录经返回值外提，调用方持 [`tempfile::TempDir`] 至测试结束随 RAII
/// 清理（局部变量析构逆序保证 `GarnetServer` Drop→stop 先于目录删除）；
/// `StorageSessionProvider<F>` 的 `F` 即 decorate 类型，故将 [`session_factory`]
/// 显式收敛为函数指针 [`SessionFactory`]，返回类型方可命名
#[must_use]
pub fn start_node() -> (
  tempfile::TempDir,
  Arc<GarnetServer<StorageSessionProvider<SessionFactory>>>,
  SocketAddr,
) {
  let dir = tempdir().expect("tempdir");
  let provider = Arc::new(
    StorageSessionProvider::open_with_config_and_aof(
      test_store_config(),
      dir.path().join("node.db"),
      None,
      RuntimeServerOptions::default(),
      session_factory as SessionFactory,
    )
    .expect("open with aof"),
  );
  let (server, addr) = start_server(provider);
  (dir, server, addr)
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
///
/// 全帧型：RESP2 三型 + RESP3 单帧型（`_` nil / `#` bool / `,` double /
/// `(` bignum 行式，`!` blob error / `=` verbatim 定长，`~` set / `>` push
/// 聚合），对标 C# RespReadUtils 的 TryReadSignedMapLength /
/// TryReadSignedSetLength / TryReadVerbatimStringLength 全帧面，消除
/// HELLO 3 会话上 [`read_reply`] 对单帧型判不出完整而永久挂起
#[must_use]
pub fn complete_len(data: &[u8]) -> Option<usize> {
  if data.is_empty() {
    return None;
  }
  let kind = data[0];
  let nl = data.iter().position(|&b| b == b'\n')?;
  match kind {
    // 行式帧（+OK / -ERR / :N / _ / # / , / (）：头行到齐即完整（header 非数字，不可先解析）
    b'+' | b'-' | b':' | b'_' | b'#' | b',' | b'(' => Some(nl + 1),
    // 定长帧（$ bulk / ! blob error / = verbatim）：header 为载荷字节数（负值即 nil 行式）
    b'$' | b'!' | b'=' => {
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
    // 聚合帧（* 数组 / % map / ~ set / > push）：header 为子帧数（map 的 header 是对数，子帧翻倍）
    b'*' | b'%' | b'~' | b'>' => {
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

#[cfg(test)]
mod tests {
  use super::complete_len;

  /// RESP3 单帧型全帧面（票 r21 发现一回归：HELLO 3 会话 read_reply 不再挂起）
  #[test]
  fn complete_len_resp3_frames() {
    // 行式：_ nil / # bool / , double / ( bignum —— 头行到齐即完整
    assert_eq!(complete_len(b"_\r\n"), Some(3));
    assert_eq!(complete_len(b"#t\r\n"), Some(4));
    assert_eq!(complete_len(b",3.14\r\n"), Some(7));
    assert_eq!(complete_len(b"(12345678901234567890\r\n"), Some(23));
    assert_eq!(complete_len(b",3.14\r"), None);
    // 定长：! blob error / = verbatim —— header 定长 + CRLF
    assert_eq!(complete_len(b"!21\r\nSYNTAX invalid syntax\r\n"), Some(28));
    assert_eq!(complete_len(b"=15\r\ntxt:Some string\r\n"), Some(22));
    assert_eq!(complete_len(b"=15\r\ntxt:Some"), None);
    // 聚合：~ set / > push —— 子帧递归（push 子帧内嵌数组）
    assert_eq!(complete_len(b"~2\r\n+a\r\n+b\r\n"), Some(12));
    assert_eq!(
      complete_len(b">2\r\n*2\r\n$7\r\nmessage\r\n$2\r\nch\r\n$3\r\nfoo\r\n"),
      Some(38)
    );
    // map 对数翻倍（RESP3 HELLO 应答形态）
    assert_eq!(complete_len(b"%1\r\n+k\r\n:v\r\n"), Some(12));
    // 未知首字节仍 None
    assert_eq!(complete_len(b"@x\r\n"), None);
  }
}
