//! HLL 冷数据降级语义回归测试
//!
//! 修复前：load_hll 把磁盘候选（wkv 读面 `Ok(None)` 降级信号）视同键缺失，
//! PFADD 对冷区 HLL 按新键盲插空寄存器覆盖写，历史基数丢失。修复后：
//! `HllLoad::Degrade` 独立成态，命令臂 `Ok(false)` 转 exec_slow 异步装载
//! 裁决（对标 garnet libs/server/Storage/Session/MainStore/HyperLogLogOps.cs:
//! HyperLogLogAdd 的 RMW 挂起 pending 磁盘读后重放、NOTFOUND 才允许新建），
//! 冷数据基数保留；PFCOUNT/PFMERGE 同款整体降级。

use std::sync::Arc;

use compio::runtime::Runtime;
use tempfile::tempdir;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::resp::{
  garnet_api::{GarnetApi, StoreGarnetApi},
  resp_server_session::{RespServerSession, RespServerSessionOptions},
};
use wnode_test::err_frame;
use wresp::{cmd_strings::RESP_ERR_WRONG_TYPE_HLL, command::RespCommand};

/// 小容量单文件存储执行域（msetnx_atomic.rs 同款配置，附 store 句柄供驱逐）
fn open_env(
  tag: &str,
) -> (
  Runtime,
  GarnetApi,
  Arc<WedbStore<SegmentedDevice>>,
  tempfile::TempDir,
) {
  let dir = tempdir().unwrap();
  let config = StoreConfig::new(1024, 64 * 1024, 16, 0.5).unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag)).unwrap());
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  let api: GarnetApi = Arc::new(StoreGarnetApi::new(store.new_session().unwrap()));
  (Runtime::new().unwrap(), api, store, dir)
}

/// 挂接分派器的会话（降级链路须经 session.garnet_api 挂起 SlowWait）
fn session_with(api: &GarnetApi) -> RespServerSession {
  let mut s = RespServerSession::new(1, RespServerSessionOptions::default());
  s.set_garnet_api(api.clone());
  s
}

/// 修复核心场景：冷区 HLL 的 PFADD 必须转慢路径异步装载后累加，
/// 原基数保留（修复前盲插空寄存器覆盖，基数坍缩为本次新元素数）
#[test]
fn pfadd_cold_degrade_preserves_cardinality() {
  let (rt, api, store, _dir) = open_env("hll-cold-pfadd.db");
  let mut s = session_with(&api);

  // 建键：5 元素写入（小基数 HLL 精确计数）
  api.exec(
    &mut s,
    RespCommand::Pfadd,
    &[b"hll", b"e1", b"e2", b"e3", b"e4", b"e5"],
  );
  assert_eq!(s.output, b":1\r\n");
  s.output.clear();

  // 冷化：全量刷盘驱逐，键仅驻留磁盘（磁盘候选）
  rt.block_on(store.flush_and_evict_all()).unwrap();

  // 冷数据 PFADD：快路径必须降级（无应答写出 = Ok(false)，绝不盲插覆盖）
  api.exec(&mut s, RespCommand::Pfadd, &[b"hll", b"e6"]);
  assert!(
    s.output.is_empty(),
    "冷数据 PFADD 必须降级慢路径而非盲插写：{:?}",
    String::from_utf8_lossy(&s.output)
  );

  // 慢路径异步装载后累加
  s.output.clear();
  let out = rt.block_on(Arc::clone(&api).exec_slow(
    RespCommand::Pfadd,
    vec![b"hll".to_vec(), b"e6".to_vec()],
    wconf::DEFAULT_RESP_VERSION,
  ));
  assert_eq!(out, b":1\r\n");

  // 旧基数保留 + 新元素累加：6（修复前 = 1，历史基数被覆盖）
  s.output.clear();
  let out = rt.block_on(Arc::clone(&api).exec_slow(
    RespCommand::Pfcount,
    vec![b"hll".to_vec()],
    wconf::DEFAULT_RESP_VERSION,
  ));
  assert_eq!(out, b":6\r\n");
}

/// 冷区 PFCOUNT：快路径整体降级无应答，慢路径异步装载后回真实基数
#[test]
fn pfcount_cold_degrade_returns_cardinality() {
  let (rt, api, store, _dir) = open_env("hll-cold-pfcount.db");
  let mut s = session_with(&api);

  api.exec(
    &mut s,
    RespCommand::Pfadd,
    &[b"hll", b"a", b"b", b"c", b"d"],
  );
  assert_eq!(s.output, b":1\r\n");
  s.output.clear();

  rt.block_on(store.flush_and_evict_all()).unwrap();

  // 冷数据 PFCOUNT：快路径降级（修复前回 :0）
  api.exec(&mut s, RespCommand::Pfcount, &[b"hll"]);
  assert!(
    s.output.is_empty(),
    "冷数据 PFCOUNT 必须降级而非按缺失回 0：{:?}",
    String::from_utf8_lossy(&s.output)
  );

  let out = rt.block_on(Arc::clone(&api).exec_slow(
    RespCommand::Pfcount,
    vec![b"hll".to_vec()],
    wconf::DEFAULT_RESP_VERSION,
  ));
  assert_eq!(out, b":4\r\n");
}

/// 冷区 PFMERGE：dest 冷数据转慢路径合并，历史元素与源元素并存
#[test]
fn pfmerge_cold_degrade_merges_into_disk_dest() {
  let (rt, api, store, _dir) = open_env("hll-cold-pfmerge.db");
  let mut s = session_with(&api);

  api.exec(&mut s, RespCommand::Pfadd, &[b"h1", b"x1", b"x2", b"x3"]);
  assert_eq!(s.output, b":1\r\n");
  s.output.clear();
  api.exec(&mut s, RespCommand::Pfadd, &[b"h2", b"y1", b"y2"]);
  assert_eq!(s.output, b":1\r\n");
  s.output.clear();

  rt.block_on(store.flush_and_evict_all()).unwrap();

  // dest h1 已冷：快路径降级，不盲插空寄存器
  api.exec(&mut s, RespCommand::Pfmerge, &[b"h1", b"h2"]);
  assert!(
    s.output.is_empty(),
    "冷 dest PFMERGE 必须降级而非盲插覆盖：{:?}",
    String::from_utf8_lossy(&s.output)
  );

  // 慢路径合并后 h1 = {x1,x2,x3,y1,y2}
  let out = rt.block_on(Arc::clone(&api).exec_slow(
    RespCommand::Pfmerge,
    vec![b"h1".to_vec(), b"h2".to_vec()],
    wconf::DEFAULT_RESP_VERSION,
  ));
  assert_eq!(out, b"+OK\r\n");
  let out = rt.block_on(Arc::clone(&api).exec_slow(
    RespCommand::Pfcount,
    vec![b"h1".to_vec()],
    wconf::DEFAULT_RESP_VERSION,
  ));
  assert_eq!(out, b":5\r\n");
}

/// 快路径对照组：内存命中累加、新键初始化、无变更 :0，行为不变
#[test]
fn pfadd_warm_fastpath_unchanged() {
  let (_rt, api, _store, _dir) = open_env("hll-warm-fastpath.db");
  let mut s = session_with(&api);

  // 新键初始化
  api.exec(&mut s, RespCommand::Pfadd, &[b"warm", b"m1", b"m2"]);
  assert_eq!(s.output, b":1\r\n");
  s.output.clear();

  // 内存命中新元素累加
  api.exec(&mut s, RespCommand::Pfadd, &[b"warm", b"m3"]);
  assert_eq!(s.output, b":1\r\n");
  s.output.clear();

  // 重复元素无变更
  api.exec(&mut s, RespCommand::Pfadd, &[b"warm", b"m3"]);
  assert_eq!(s.output, b":0\r\n");
  s.output.clear();

  api.exec(&mut s, RespCommand::Pfcount, &[b"warm"]);
  assert_eq!(s.output, b":3\r\n");
}

/// 对象键拦截不回归：信封域命中（含冷区）仍答 WRONGTYPE，绝不盲插覆盖对象键
#[test]
fn pfadd_object_key_wrongtype_unchanged() {
  let (rt, api, store, _dir) = open_env("hll-object-wrongtype.db");
  let mut s = session_with(&api);

  api.exec(&mut s, RespCommand::Hset, &[b"obj", b"f", b"v"]);
  s.output.clear();

  api.exec(&mut s, RespCommand::Pfadd, &[b"obj", b"e"]);
  assert_eq!(s.output, err_frame(RESP_ERR_WRONG_TYPE_HLL));

  // 冷化后同款拦截（慢路径信封域探测）
  rt.block_on(store.flush_and_evict_all()).unwrap();
  s.output.clear();
  let out = rt.block_on(Arc::clone(&api).exec_slow(
    RespCommand::Pfadd,
    vec![b"obj".to_vec(), b"e".to_vec()],
    wconf::DEFAULT_RESP_VERSION,
  ));
  assert_eq!(out, err_frame(RESP_ERR_WRONG_TYPE_HLL));
}

/// test/standalone/Garnet.test.complexstring/HyperLogLogTests.cs:PFADDImmutableRegionValidation
#[test]
fn pfadd_forged_hll_validation() {
  for buffer_size in [18, 32 * 1024] {
    let mut forged = vec![0_u8; buffer_size];
    forged[3] = 0;
    forged[16..18].copy_from_slice(&3000_u16.to_le_bytes());

    let (rt, api, store, _dir) = open_env("hll-forged-pfadd.db");
    let mut s = session_with(&api);

    api.exec(&mut s, RespCommand::Set, &[b"k", &forged]);
    s.output.clear();

    // 热区：PFADD 遇伪造载荷报错 WRONGTYPE
    api.exec(&mut s, RespCommand::Pfadd, &[b"k", b"foo"]);
    assert_eq!(s.output, err_frame(RESP_ERR_WRONG_TYPE_HLL));
    s.output.clear();

    // 冷化至磁盘冷区（immutable region）
    rt.block_on(store.flush_and_evict_all()).unwrap();

    // 冷区：快路径降级，慢路径执行报错 WRONGTYPE
    api.exec(&mut s, RespCommand::Pfadd, &[b"k", b"bar"]);
    assert!(s.output.is_empty());
    s.output.clear();

    let out = rt.block_on(Arc::clone(&api).exec_slow(
      RespCommand::Pfadd,
      vec![b"k".to_vec(), b"bar".to_vec()],
      wconf::DEFAULT_RESP_VERSION,
    ));
    assert_eq!(out, err_frame(RESP_ERR_WRONG_TYPE_HLL));

    // 校验原数据未被破坏
    s.output.clear();
    let out = rt.block_on(Arc::clone(&api).exec_slow(
      RespCommand::Get,
      vec![b"k".to_vec()],
      wconf::DEFAULT_RESP_VERSION,
    ));
    let mut expected = format!("${}\r\n", forged.len()).into_bytes();
    expected.extend_from_slice(&forged);
    expected.extend_from_slice(b"\r\n");
    assert_eq!(out, expected);
  }
}

/// test/standalone/Garnet.test.complexstring/HyperLogLogTests.cs:PFMERGEImmutableRegionValidation
#[test]
fn pfmerge_forged_hll_validation() {
  for buffer_size in [18, 32 * 1024] {
    let mut forged = vec![0_u8; buffer_size];
    forged[3] = 0;
    forged[16..18].copy_from_slice(&3000_u16.to_le_bytes());

    let (rt, api, store, _dir) = open_env("hll-forged-pfmerge.db");
    let mut s = session_with(&api);

    api.exec(&mut s, RespCommand::Set, &[b"k0", &forged]);
    api.exec(&mut s, RespCommand::Set, &[b"k1", &forged]);
    s.output.clear();

    // 热区：PFMERGE 遇伪造载荷报错 WRONGTYPE
    api.exec(&mut s, RespCommand::Pfmerge, &[b"k2", b"k0", b"k1"]);
    assert_eq!(s.output, err_frame(RESP_ERR_WRONG_TYPE_HLL));
    s.output.clear();

    // 冷化至磁盘冷区（immutable region）
    rt.block_on(store.flush_and_evict_all()).unwrap();

    // 冷区：慢路径执行报错 WRONGTYPE
    let out = rt.block_on(Arc::clone(&api).exec_slow(
      RespCommand::Pfmerge,
      vec![b"k2".to_vec(), b"k0".to_vec(), b"k1".to_vec()],
      wconf::DEFAULT_RESP_VERSION,
    ));
    assert_eq!(out, err_frame(RESP_ERR_WRONG_TYPE_HLL));

    // 校验原数据未被破坏
    for key in [b"k0", b"k1"] {
      let out = rt.block_on(Arc::clone(&api).exec_slow(
        RespCommand::Get,
        vec![key.to_vec()],
        wconf::DEFAULT_RESP_VERSION,
      ));
      let mut expected = format!("${}\r\n", forged.len()).into_bytes();
      expected.extend_from_slice(&forged);
      expected.extend_from_slice(b"\r\n");
      assert_eq!(out, expected);
    }
  }
}
