//! 快路径 TYPE / MGET 存储错误吞噬回归（票 wnode-quickpath-ioerr-swallow）
//!
//! 缺陷：快路径 TYPE / MGET 把 `wkv::Error` 折叠进缺省应答——TYPE 回 none、
//! MGET 逐键回 nil 且污染 notfound 计数，与慢路径错误帧
//! （RESP_ERR_SLOW_PATH_STORAGE）及同域快路径 GET / STRLEN（RESP_ERR_GENERIC）
//! 三态撕裂：同一故障客户端一会拿到「键不存在」一会拿到错误帧，无法建立
//! 一致的错误处理策略，且客户端「TYPE 无则建」类逻辑会基于假缺席态做决策。
//!
//! 收口：三处吞 Err 独立拆出（TYPE 外层 String 域臂、Meta 域 `_ =>` 兜底臂、
//! 信封域组合臂；MGET Err 臂改整体错误出口回滚数组头），Err 一律回
//! RESP_ERR_GENERIC（与 GET 同线面字节），notfound 不入账。
//!
//! 注入形态（快路径）：快路径 Err 无法用设备故障触达——快路径内存直读不
//! 触碰设备（冷键恒 Deferred 降级慢路径），其真机制面是一致读 pre 协议
//! （`wkv::StoreSession::with_session_consistent_read` 内的
//! `pre_single_key_consistent_read`，副本回放滞后时以
//! `Error::ConsistentReadTimeout` 上抛，对标 C# TimeoutException）。本文件在
//! 该附着点挂「pre 超时门」：按记录物理键哈希（经
//! `StoreSession::consistent_read_hash` 生产同源单点派生）精确选域注入，
//! 确定性复现真组件超时臂。慢路径对照沿用物理介质故障口径（段文件截断，
//! 同仓 `mget_slow_path_storage_error.rs` 同款）。
//!
//! 对标 C#：`ArrayCommands.cs:NetworkTYPE` 的 status 兜底臂仅覆盖 NOTFOUND /
//! WRONGTYPE 状态枚举，`NetworkMGET` 的 `MGetReadArgBatch.SetStatus` 消费的
//! 也是纯状态枚举域；真实 IO 异常一律沿调用栈上抛掐断连接，绝无「存储错误
//! 伪装键不存在」形态。

use std::{fs::OpenOptions, sync::Arc};

use wconf::RuntimeServerConfig;
use wkv::{ConsistentReadFunctions, Error, StoreSession};
use wmetric::SessionMetricsHandle;
use wnode::{
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSession},
  service::SharedStore,
};
use wnode_test::{complete_len, err_frame, test_env};
use wresp::{
  cmd_strings::{RESP_ERR_GENERIC, RESP_ERR_SLOW_PATH_STORAGE},
  ext::RespVecExt,
};
use wtest_base::resp_frame;
use wval::KeyTag;

/// RESP_ERR_GENERIC 错误帧期望字节（经 `RespVecExt::write_resp_error` 成帧
/// 单点派生，与 GET 快路径同线面：`-ERR generic error\r\n`）
fn generic_err_frame() -> Vec<u8> {
  let mut out = Vec::new();
  out.write_resp_error(RESP_ERR_GENERIC);
  out
}

/// 一致读 pre 超时门（[`ConsistentReadFunctions`] 测试侧确定性替身）
///
/// 命中 `fail` 内记录物理键哈希的读回合以 `Error::ConsistentReadTimeout`
/// 失败（复现真组件 `ReadSessionState::pre_single_key_consistent_read` 的
/// 回放滞后超时臂，错误变体一字不差）；未命中直通。批量族默认实现直通
/// （本文件不触达批量协议面）
struct PreTimeoutGate {
  fail: Vec<i64>,
}

impl ConsistentReadFunctions for PreTimeoutGate {
  fn pre_single_key_consistent_read(&self, hash: i64) -> wkv::Result<()> {
    if self.fail.contains(&hash) {
      Err(Error::ConsistentReadTimeout)
    } else {
      Ok(())
    }
  }

  fn post_single_key_consistent_read_callback(&self) {}
}

/// `test_env` 的设备具体类型（`gated_env` 返回类型可命名化）
type EnvDevice = wdev::SegmentedDevice;

/// 域选注入会话装配：干净会话承接写阶段，门控会话按需派生承接读阶段
///
/// `gated` 派生的门控会话按 fail 哈希列表挂读门；`hash` 持同库探测会话
/// （ns0/db0 同前缀），与生产 `with_session_consistent_read` 的取哈希同源，
/// 杜绝跨域错读
struct GatedEnv {
  session: RespServerSession,
  store: SharedStore<EnvDevice>,
  probe: StoreSession<EnvDevice>,
}

impl GatedEnv {
  /// 门控会话派生器：同库新会话挂 `PreTimeoutGate` 读门
  fn gated(&self, fail: Vec<i64>) -> StoreSession<EnvDevice> {
    self
      .store
      .new_session()
      .expect("派生门控会话")
      .with_read_session_state(Some(Arc::new(PreTimeoutGate { fail })))
  }

  /// 键哈希派生器
  fn hash(&self, tag: KeyTag, key: &[u8]) -> i64 {
    self.probe.consistent_read_hash(tag, key)
  }
}

/// 返回（临时目录——保活至测试尾，装配体）
fn gated_env() -> (tempfile::TempDir, GatedEnv) {
  let (dir, session, mut s) = test_env(false);
  s.runtime_config = Arc::new(RuntimeServerConfig::with_defaults());
  let store = Arc::clone(session.store());
  let probe = store.new_session().expect("派生探测会话");
  s.set_garnet_api(Arc::new(StoreGarnetApi::new(session)));
  (
    dir,
    GatedEnv {
      session: s,
      store,
      probe,
    },
  )
}

/// 驱动一轮消费并回线面应答字节，返回 `(应答字节, 是否走慢路径)`
///（与 `mget_slow_path_storage_error.rs` 同款泵：慢路径应答经产线冲出口
/// `resolve_slow_wait_into` 并入，与真实网络泵同一写出面）
async fn pump(session: &mut RespServerSession, input: &[u8]) -> (Vec<u8>, bool) {
  session.recv_buffer.clear();
  session.recv_buffer.extend_from_slice(input);
  session.bytes_read = input.len();
  session.read_head = 0;
  session.end_read_head = 0;
  session.output.clear();
  assert_eq!(
    session.try_consume_messages(),
    Some(0),
    "输入命令应整段消费完毕"
  );

  let mut wire = Vec::new();
  match session.take_slow_wait() {
    Some(slow) => {
      let reply = slow.resolve().await;
      session.resolve_slow_wait_into(&reply, &mut wire);
      (wire, true)
    }
    None => {
      session.take_output_into(&mut wire);
      (wire, false)
    }
  }
}

/// 快路径 TYPE 三域 Err 臂逐一独立回错误帧，缺键态保持 none
#[compio::test]
async fn type_quickpath_domain_storage_errors_reply_generic_frame() -> aok::Result<()> {
  let (_dir, mut g) = gated_env();

  // 素材：字符串键 + wcol 信封对象键（写阶段走干净 API，无门）
  let (out, _) = pump(&mut g.session, &resp_frame(&[b"SET", b"t:str", b"v"])).await;
  assert_eq!(out, b"+OK\r\n");
  let (out, _) = pump(
    &mut g.session,
    &resp_frame(&[b"HSET", b"t:obj", b"f", b"v"]),
  )
  .await;
  assert_eq!(out, b":1\r\n");

  // 干净对照：TYPE 各态线面基线（string / hash / none）
  let (out, went_slow) = pump(&mut g.session, &resp_frame(&[b"TYPE", b"t:str"])).await;
  assert!(!went_slow, "热键 TYPE 须走快路径");
  assert_eq!(out, b"+string\r\n");
  let (out, _) = pump(&mut g.session, &resp_frame(&[b"TYPE", b"t:obj"])).await;
  assert_eq!(out, b"+hash\r\n");
  let (out, _) = pump(&mut g.session, &resp_frame(&[b"TYPE", b"t:absent"])).await;
  assert_eq!(out, b"+none\r\n");

  let h_str = g.hash(KeyTag::String, b"t:str");
  let h_meta_obj = g.hash(KeyTag::Meta, b"t:obj");
  let h_env_obj = g.hash(KeyTag::ObjectEnvelope, b"t:obj");

  // String 域 Err（外层臂）：独立回 generic 错误帧，绝不伪装 none
  g.session
    .set_garnet_api(Arc::new(StoreGarnetApi::new(g.gated(vec![h_str]))));
  let (out, went_slow) = pump(&mut g.session, &resp_frame(&[b"TYPE", b"t:str"])).await;
  assert!(!went_slow, "热键 String 域读失败须留在快路径");
  assert_eq!(out, generic_err_frame(), "String 域 Err 不得伪装 none");
  // 同会话未受门的键：Ok(Missing) 保持 none（Err 与缺键不合流佐证）
  let (out, _) = pump(&mut g.session, &resp_frame(&[b"TYPE", b"t:absent"])).await;
  assert_eq!(out, b"+none\r\n");

  // Meta 域 Err（r92 附修法第三处）：String 域读通过判 WrongType 后，
  // Meta 域读失败独立回错误帧，不得落信封域兜底吞错
  g.session
    .set_garnet_api(Arc::new(StoreGarnetApi::new(g.gated(vec![h_meta_obj]))));
  let (out, went_slow) = pump(&mut g.session, &resp_frame(&[b"TYPE", b"t:obj"])).await;
  assert!(!went_slow, "内存对象键 TYPE 须走快路径");
  assert_eq!(out, generic_err_frame(), "Meta 域 Err 不得折信封域兜底");

  // 信封域 Err：Meta 域读通过（无元记录）后信封域读失败独立回错误帧
  g.session
    .set_garnet_api(Arc::new(StoreGarnetApi::new(g.gated(vec![h_env_obj]))));
  let (out, _) = pump(&mut g.session, &resp_frame(&[b"TYPE", b"t:obj"])).await;
  assert_eq!(out, generic_err_frame(), "信封域 Err 不得伪装 none");

  // 换空门 API 复常：TYPE 回正常态（连接与会话无残留错位）
  g.session
    .set_garnet_api(Arc::new(StoreGarnetApi::new(g.gated(vec![]))));
  let (out, _) = pump(&mut g.session, &resp_frame(&[b"TYPE", b"t:str"])).await;
  assert_eq!(out, b"+string\r\n");
  Ok(())
}

/// 快路径 MGET Err 臂整命令回滚：单条错误帧、零数组头与元素残帧、
/// notfound 计数不虚增
#[compio::test]
async fn mget_quickpath_storage_error_single_frame_and_metrics_intact() -> aok::Result<()> {
  let (_dir, mut g) = gated_env();

  let (out, _) = pump(&mut g.session, &resp_frame(&[b"SET", b"m:a", b"v1"])).await;
  assert_eq!(out, b"+OK\r\n");

  // 采样句柄装配（生产由 service.rs 采样门控创建后经 attach 注入）
  let handle = Arc::new(SessionMetricsHandle::default());
  g.session.attach_session_metrics(Some(Arc::clone(&handle)));

  let h_str_b = g.hash(KeyTag::String, b"m:b");

  // 首键命中已写出 bulk、次键读回合失败：整命令回滚换单条错误帧，
  // 既无 `*2` 数组头也无 `$2\r\nv1` 残帧
  g.session
    .set_garnet_api(Arc::new(StoreGarnetApi::new(g.gated(vec![h_str_b]))));
  let (out, went_slow) = pump(&mut g.session, &resp_frame(&[b"MGET", b"m:a", b"m:b"])).await;
  assert!(!went_slow, "热键批须走快路径");
  assert_eq!(
    out,
    generic_err_frame(),
    "MGET Err 臂须单条错误帧整命令替换"
  );
  // 帧完整性自证：全部线面字节恰构成一条完整 RESP 应答，无尾随半帧
  assert_eq!(complete_len(&out), Some(out.len()));

  // notfound 不虚增：错误整命令逐键计数一并丢弃（修复前 m:b 会入账 +1）
  let m = handle.snapshot();
  assert_eq!(m.get_total_found(), 0, "Err 臂已写键 found 不入账");
  assert_eq!(m.get_total_notfound(), 0, "Err 臂错误键 notfound 不得入账");

  // 换空门 API 复常：连接续用无应答错位，命中/缺键正常入账（正向对照）
  g.session
    .set_garnet_api(Arc::new(StoreGarnetApi::new(g.gated(vec![]))));
  let (out, _) = pump(&mut g.session, &resp_frame(&[b"PING"])).await;
  assert_eq!(out, b"+PONG\r\n");
  let (out, _) = pump(
    &mut g.session,
    &resp_frame(&[b"MGET", b"m:a", b"m:missing"]),
  )
  .await;
  assert_eq!(out, b"*2\r\n$2\r\nv1\r\n$-1\r\n");
  let m = handle.snapshot();
  assert_eq!(m.get_total_found(), 1);
  assert_eq!(m.get_total_notfound(), 1);
  Ok(())
}

/// 同故障慢路径对照：物理介质读失败经慢路径回 RESP_ERR_SLOW_PATH_STORAGE，
/// 同库热键快路径应答正常——同一存储不再三态撕裂
///（MGET 慢路径批量口同形态已由 `mget_slow_path_storage_error.rs` 锁死，
/// 本用例补 TYPE 慢路径臂与热键对照）
#[compio::test]
async fn type_slow_path_storage_error_contrasts_quickpath() -> aok::Result<()> {
  let (_dir, session, mut s) = test_env(false);
  s.runtime_config = Arc::new(RuntimeServerConfig::with_defaults());

  {
    let batch = session.enter_batch();
    batch
      .try_upsert_sync(b"tp:cold", b"v_cold")
      .unwrap()
      .unwrap();
  }
  session.store.flush_and_evict_all().await.unwrap();
  {
    let batch = session.enter_batch();
    batch.try_upsert_sync(b"tp:hot", b"v_hot").unwrap().unwrap();
  }

  // 物理介质故障：段文件截断为空（冷读经 wdev 读口 UnexpectedEof 上抛）
  OpenOptions::new()
    .write(true)
    .open(session.store().device.segment_path(0))
    .expect("段文件应已随 store 打开创建")
    .set_len(0)
    .expect("截断段文件注入磁盘故障");

  s.set_garnet_api(Arc::new(StoreGarnetApi::new(session)));

  // 冷键 TYPE：磁盘候选降级慢路径，收割失败回慢路径存储错误帧（非 none）
  let (out, went_slow) = pump(&mut s, &resp_frame(&[b"TYPE", b"tp:cold"])).await;
  assert!(
    went_slow,
    "冷键 TYPE 须降级慢路径，否则不触达磁盘收割失败臂"
  );
  assert_eq!(out, err_frame(RESP_ERR_SLOW_PATH_STORAGE));
  assert_eq!(complete_len(&out), Some(out.len()));

  // 热键 TYPE：同故障下快路径内存直读正常应答（健康路径不受殃及）
  let (out, went_slow) = pump(&mut s, &resp_frame(&[b"TYPE", b"tp:hot"])).await;
  assert!(!went_slow, "热键 TYPE 须走快路径");
  assert_eq!(out, b"+string\r\n");
  Ok(())
}
