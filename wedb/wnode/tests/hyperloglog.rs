use core::{fmt::Write, ops::Range, str};
use std::{
  fs::OpenOptions,
  io,
  sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
  },
};

use compio::runtime::Runtime;
use tempfile::tempdir;
use wbase::{
  pool::{AlignedBuf, BufferPool},
  time::now_ticks,
};
use wdev::{Device, Error as WdevError, SegmentedDevice};
use whyperlog::{HyperLogLog, SPARSE_MEMORY_SECTOR_SIZE, SPARSE_SIZE_MAX_CAP, murmur_hash_2_x64_a};
use wkv::{BatchStoreSession, StoreConfig, StoreResult, WedbStore};
use wnode::{
  resp::{
    RespServerSession,
    garnet_api::{GarnetApi, StoreGarnetApi},
    resp_server_session::RespServerSessionOptions,
  },
  storage::session::{common::ttl_sync::put_ttl_sync, storage_session::version_map_watch_hook},
};
use wnode_test::{err_frame, with_batch};
use wresp::{
  cmd_strings::{RESP_ERR_SLOW_PATH_STORAGE, RESP_ERR_WRONG_TYPE_HLL},
  command::RespCommand,
};
use wtest_base::test_store_config;
use wtxn::{TxnKeyEntryComparison, WatchVersionMap};
use wval::SessionPrefixBuf;

fn parse_resp_int(out: &[u8]) -> i64 {
  let text = str::from_utf8(&out[1..out.len() - 2]).unwrap();
  text.parse().unwrap()
}

/// 期望 HLL WRONGTYPE 帧：由 wresp 单点常量派生（对标 CmdStrings.RESP_ERR_WRONG_TYPE_HLL）
fn hll_wrongtype_frame() -> Vec<u8> {
  err_frame(RESP_ERR_WRONG_TYPE_HLL)
}

/// test/standalone/Garnet.test.complexstring/HyperLogLogTests.cs:SimpleHyperLogLogAddCount
#[test]
fn simple_hyper_log_log_add_count() {
  with_batch(|sess, batch| {
    let data: [&[u8]; 6] = [b"a", b"b", b"c", b"d", b"e", b"f"];
    let key: &[u8] = b"hllKey";

    // HLL updated
    for item in &data {
      let mut out = Vec::new();
      sess
        .hyper_log_log_add(&[key, item], batch, &mut out)
        .unwrap();
      assert_eq!(parse_resp_int(&out), 1);
    }

    // HLL not updated
    for item in &data {
      let mut out = Vec::new();
      sess
        .hyper_log_log_add(&[key, item], batch, &mut out)
        .unwrap();
      assert_eq!(parse_resp_int(&out), 0);
    }

    // estimate cardinality
    let mut out = Vec::new();
    sess.hyper_log_log_length(&[key], batch, &mut out).unwrap();
    assert_eq!(parse_resp_int(&out), 6);
  });
}

/// test/standalone/Garnet.test.complexstring/HyperLogLogTests.cs:SimpleHyperLogLogMerge
#[test]
fn simple_hyper_log_log_merge() {
  with_batch(|sess, batch| {
    let key_x: &[u8] = b"x";
    let key_y: &[u8] = b"y";
    let key_w: &[u8] = b"w";

    let mut out = Vec::new();
    sess
      .hyper_log_log_add(&[key_x, b"h", b"e", b"l", b"l", b"o"], batch, &mut out)
      .unwrap();
    assert_eq!(parse_resp_int(&out), 1);

    let mut out = Vec::new();
    sess
      .hyper_log_log_length(&[key_x], batch, &mut out)
      .unwrap();
    assert_eq!(parse_resp_int(&out), 4);

    let mut out = Vec::new();
    sess
      .hyper_log_log_add(&[key_y, b"w", b"o", b"r", b"l", b"d"], batch, &mut out)
      .unwrap();
    assert_eq!(parse_resp_int(&out), 1);

    let mut out = Vec::new();
    sess
      .hyper_log_log_length(&[key_y], batch, &mut out)
      .unwrap();
    assert_eq!(parse_resp_int(&out), 5);

    let mut out = Vec::new();
    sess
      .hyper_log_log_merge(&[key_w, key_x], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"+OK\r\n");

    let mut out = Vec::new();
    sess
      .hyper_log_log_length(&[key_w], batch, &mut out)
      .unwrap();
    assert_eq!(parse_resp_int(&out), 4);

    let mut out = Vec::new();
    sess
      .hyper_log_log_merge(&[key_w, key_y], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"+OK\r\n");

    let mut out = Vec::new();
    sess
      .hyper_log_log_length(&[key_w], batch, &mut out)
      .unwrap();
    assert_eq!(parse_resp_int(&out), 7);
  });
}

/// test/standalone/Garnet.test.complexstring/HyperLogLogTests.cs:HyperLogLogSimpleInvalidHLLTypeTest
#[test]
fn hyper_log_log_simple_invalid_hll_type_test() {
  with_batch(|sess, batch| {
    let key_x: &[u8] = b"x";
    let key_y: &[u8] = b"y";
    let key_w: &[u8] = b"w";

    let mut out = Vec::new();
    sess
      .hyper_log_log_add(&[key_x, b"h", b"e", b"l", b"l", b"o"], batch, &mut out)
      .unwrap();
    assert_eq!(parse_resp_int(&out), 1);

    let mut out = Vec::new();
    sess
      .hyper_log_log_add(&[key_y, b"w", b"o", b"r", b"l", b"d"], batch, &mut out)
      .unwrap();
    assert_eq!(parse_resp_int(&out), 1);

    let _ = batch.try_upsert_sync(key_w, b"100");

    let mut out = Vec::new();
    sess
      .hyper_log_log_add(&[key_w, b"h", b"e", b"l", b"l", b"o"], batch, &mut out)
      .unwrap();
    assert_eq!(out, hll_wrongtype_frame());

    let mut out = Vec::new();
    sess
      .hyper_log_log_length(&[key_w], batch, &mut out)
      .unwrap();
    assert_eq!(out, hll_wrongtype_frame());

    let mut out = Vec::new();
    sess
      .hyper_log_log_merge(&[key_w, key_y, key_x], batch, &mut out)
      .unwrap();
    assert_eq!(out, hll_wrongtype_frame());

    let mut out = Vec::new();
    sess
      .hyper_log_log_merge(&[key_y, key_w, key_x], batch, &mut out)
      .unwrap();
    assert_eq!(out, hll_wrongtype_frame());

    let mut out = Vec::new();
    sess
      .hyper_log_log_merge(&[key_y, key_x, key_w], batch, &mut out)
      .unwrap();
    assert_eq!(out, hll_wrongtype_frame());
  });
}

/// test/standalone/Garnet.test.complexstring/HyperLogLogTests.cs:HyperLogLogMultiCountTest
#[test]
fn hyper_log_log_multi_count_test() {
  with_batch(|sess, batch| {
    let key_a: &[u8] = b"HyperLogLogMultiCountTestA";
    let key_b: &[u8] = b"HyperLogLogMultiCountTestB";
    let key_c: &[u8] = b"HyperLogLogMultiCountTestC";

    let mut out = Vec::new();
    sess
      .hyper_log_log_add(&[key_a, b"h", b"e", b"l", b"l", b"o"], batch, &mut out)
      .unwrap();
    sess
      .hyper_log_log_add(&[key_b, b"w", b"o", b"r", b"l", b"d"], batch, &mut out)
      .unwrap();
    sess
      .hyper_log_log_add(
        &[key_c, b"a", b"b", b"c", b"d", b"e", b"f"],
        batch,
        &mut out,
      )
      .unwrap();

    let mut out = Vec::new();
    sess
      .hyper_log_log_length(&[key_a], batch, &mut out)
      .unwrap();
    assert_eq!(parse_resp_int(&out), 4);

    let mut out = Vec::new();
    sess
      .hyper_log_log_length(&[key_b], batch, &mut out)
      .unwrap();
    assert_eq!(parse_resp_int(&out), 5);

    let mut out = Vec::new();
    sess
      .hyper_log_log_length(&[key_c], batch, &mut out)
      .unwrap();
    assert_eq!(parse_resp_int(&out), 6);

    let mut out = Vec::new();
    sess
      .hyper_log_log_length(&[key_a, key_b, key_c], batch, &mut out)
      .unwrap();
    assert_eq!(parse_resp_int(&out), 11);
  });
}

/// PFADD 零元素：对标 C# HyperLogLogAdd 元素循环零次 pfaddUpdated==0，不触达存储
/// 不建键，直答 :0
#[test]
fn pfadd_without_elements_skips_storage() {
  with_batch(|sess, batch| {
    let key: &[u8] = b"pfadd-no-elem";

    let mut out = Vec::new();
    sess.hyper_log_log_add(&[key], batch, &mut out).unwrap();
    assert_eq!(out, b":0\r\n");

    // 不建键：String 域内存直读确认不存在
    assert_eq!(
      batch.try_read_sync(key, |_| true).unwrap(),
      StoreResult::NotFound,
      "零元素 PFADD 不得创建键"
    );
  });
}

/// PFMERGE 零源：对标 C# HyperLogLogMerge 源循环零次，不 GET 不 SET，dest 不建
/// 亦不探测类型，直答 +OK
#[test]
fn pfmerge_without_sources_skips_storage() {
  with_batch(|sess, batch| {
    let dest: &[u8] = b"pfmerge-no-src";

    // 缺失 dest：+OK 且不建键
    let mut out = Vec::new();
    sess.hyper_log_log_merge(&[dest], batch, &mut out).unwrap();
    assert_eq!(out, b"+OK\r\n");
    assert_eq!(
      batch.try_read_sync(dest, |_| true).unwrap(),
      StoreResult::NotFound,
      "零源 PFMERGE 不得创建 dest"
    );

    // String 键 dest：零源不探测 WRONGTYPE，仍 +OK（C# 循环零次同款）
    let _ = batch.try_upsert_sync(b"pfmerge-str", b"100");
    let mut out = Vec::new();
    sess
      .hyper_log_log_merge(&[b"pfmerge-str"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"+OK\r\n");
  });
}

/// PFMERGE 错误路径部分提交：对标 C# HyperLogLogMerge 逐源 GET 合法后立即
/// SET_Conditional 写 dst + finally Commit——已并入的合法源随错误提交落盘，
/// 后续非法源报 WRONGTYPE；零并入出错 dst 从未被 SET（缺失不被凭空建键）
#[test]
fn pfmerge_partial_commit_on_wrongtype_source() {
  with_batch(|sess, batch| {
    let x: &[u8] = b"pfm-x";
    let y: &[u8] = b"pfm-y";
    let bad: &[u8] = b"pfm-bad";

    let mut out = Vec::new();
    sess
      .hyper_log_log_add(&[x, b"h", b"e", b"l", b"l", b"o"], batch, &mut out)
      .unwrap();
    assert_eq!(parse_resp_int(&out), 1);

    // String 非法载荷源（"100" 非 HYLL 编码 → WRONGTYPE_HLL）
    let _ = batch.try_upsert_sync(bad, b"100");

    // 零并入出错：dest 缺失 + 首源即非法 → 错误帧且 dest 不建键（C# 无 SET）
    let mut out = Vec::new();
    sess
      .hyper_log_log_merge(&[y, bad, x], batch, &mut out)
      .unwrap();
    assert_eq!(out, hll_wrongtype_frame());
    assert_eq!(
      batch.try_read_sync(y, |_| true).unwrap(),
      StoreResult::NotFound,
      "零并入出错不得创建 dest"
    );

    // 部分提交：dest 缺失 + [合法源, 非法源] → 错误帧但合法源已落盘
    let mut out = Vec::new();
    sess
      .hyper_log_log_merge(&[y, x, bad], batch, &mut out)
      .unwrap();
    assert_eq!(out, hll_wrongtype_frame());
    let mut out = Vec::new();
    sess.hyper_log_log_length(&[y], batch, &mut out).unwrap();
    assert_eq!(parse_resp_int(&out), 4, "已并入的合法源须随错误路径提交");
  });
}

/// 慢臂存储错误夹具环境（mget_slow_path_storage_error.rs 同款真设备真故障口径，
/// 非 mock）：坏源键先写后 `flush_and_evict_all` 冷化（地址落入 head 之下 →
/// 冷读走磁盘），随后截断段文件令其磁盘读以 IO 错误上抛；冷化之后新写的键落在
/// 可变区，内存直读不受截断影响
fn slow_fault_env(
  tag: &str,
) -> (
  Runtime,
  GarnetApi,
  Arc<WedbStore<SegmentedDevice>>,
  tempfile::TempDir,
) {
  let dir = tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag)).unwrap());
  let store = Arc::new(WedbStore::open(test_store_config(), device).unwrap());
  let api: GarnetApi = Arc::new(StoreGarnetApi::new(store.new_session().unwrap()));
  (Runtime::new().unwrap(), api, store, dir)
}

/// 截断段文件注入磁盘介质故障（冷区读必败、可变区新写不触达旧段）
fn truncate_segment(store: &WedbStore<SegmentedDevice>) {
  OpenOptions::new()
    .write(true)
    .open(store.device.segment_path(0))
    .expect("段文件应已随 store 打开创建")
    .set_len(0)
    .expect("截断段文件注入磁盘故障");
}

/// 读回存储 String 域原始载荷（缺失即 panic）：热面同步内存直读；冷化记录
/// `try_read_sync` 按 wkv 契约回 `RecordOnDisk`（read.rs:236 三态钉形，
/// `defense.rs` 同款锁），与产品慢臂 `UserRead::Deferred` 同口径降级全异步
/// `read().await` 磁盘读回——夹具不得把该在册态当读回失败
fn raw_blob(rt: &Runtime, store: &Arc<WedbStore<SegmentedDevice>>, key: &[u8]) -> Vec<u8> {
  let probe = store.new_session().unwrap();
  let batch = probe.enter_batch();
  match batch.try_read_sync(key, |v| v.to_vec()).unwrap() {
    StoreResult::Success(v) => v,
    StoreResult::RecordOnDisk => rt
      .block_on(batch.read(key))
      .expect("冷区磁盘读回应成功")
      .expect("读回存储载荷缺失"),
    other => panic!("读回存储载荷失败: {other:?}"),
  }
}

/// PFMERGE 慢臂存储错误部分提交（票 whll-pfmerge-storage-error-partial-commit）：
/// 对标 C# HyperLogLogOps.cs:217-271 全路径 try/finally + finally Commit
///（createTransaction 门内 autocommit 恒提交）与快臂 Err 补写臂
///（hyper_log_log_commands.rs:611-630）——源冷区读 I/O 错误时已并入源须先补写
/// 落盘再上抛，调用方统一答 RESP_ERR_SLOW_PATH_STORAGE 单行帧、零成功面；
/// 修复前 `?` 早退弃置已并入 dst，dest 终态随执行臂别漂移
#[test]
fn pfmerge_slowpath_storage_error_partial_commits_merged_source() {
  let (rt, api, store, _dir) = slow_fault_env("hll-pfmerge-slowfault.db");
  let mut s = RespServerSession::new(1, RespServerSessionOptions::default());
  s.set_garnet_api(Arc::clone(&api));

  // 坏源：先建后冷化，截断后其冷区读必败
  api.exec(&mut s, RespCommand::Pfadd, &[b"sf-bad", b"z1"]);
  assert_eq!(s.output, b":1\r\n");
  rt.block_on(store.flush_and_evict_all()).unwrap();
  // 合法源：冷化之后新写 → 内存热读，先于坏源成功并入
  s.output.clear();
  api.exec(
    &mut s,
    RespCommand::Pfadd,
    &[b"sf-good", b"g1", b"g2", b"g3"],
  );
  assert_eq!(s.output, b":1\r\n");
  truncate_segment(&store);

  // dest 缺失 + [合法源, 存储错误源]：已并入部分补写落盘后上抛，调用方统一答
  // RESP_ERR_SLOW_PATH_STORAGE 单行帧（应答面零成功帧、零 +OK 残留）
  let out = rt.block_on(Arc::clone(&api).exec_slow(
    RespCommand::Pfmerge,
    vec![b"sf-dest".to_vec(), b"sf-good".to_vec(), b"sf-bad".to_vec()],
    wconf::DEFAULT_RESP_VERSION,
  ));
  assert_eq!(out, err_frame(RESP_ERR_SLOW_PATH_STORAGE));

  // dest 终态含已并入合法源（部分提交，对齐快臂 Err 臂与 C# finally Commit）
  let out = rt.block_on(Arc::clone(&api).exec_slow(
    RespCommand::Pfcount,
    vec![b"sf-dest".to_vec()],
    wconf::DEFAULT_RESP_VERSION,
  ));
  assert_eq!(out, b":3\r\n", "已并入的合法源须随存储错误臂补写落盘");
}

/// PFMERGE 慢臂存储错误零并入面：首源即存储错误（merged 为假）不写回——
/// dest 既有载荷原样不动、dest 缺失不建键（C# dst 从未被 SET），与
/// `pfmerge_partial_commit_on_wrongtype_source` 零并入臂同口径
#[test]
fn pfmerge_slowpath_storage_error_zero_merge_no_writeback() {
  let (rt, api, store, _dir) = slow_fault_env("hll-pfmerge-zeromerge.db");
  let mut s = RespServerSession::new(1, RespServerSessionOptions::default());
  s.set_garnet_api(Arc::clone(&api));

  // 坏源先建后冷化；dest 既有键与合法源冷化后新写恒热
  api.exec(&mut s, RespCommand::Pfadd, &[b"zm-bad", b"z1"]);
  assert_eq!(s.output, b":1\r\n");
  rt.block_on(store.flush_and_evict_all()).unwrap();
  s.output.clear();
  api.exec(&mut s, RespCommand::Pfadd, &[b"zm-dest", b"d1", b"d2"]);
  assert_eq!(s.output, b":1\r\n");
  s.output.clear();
  api.exec(&mut s, RespCommand::Pfadd, &[b"zm-good", b"g1"]);
  assert_eq!(s.output, b":1\r\n");
  truncate_segment(&store);

  let dest_before = raw_blob(&rt, &store, b"zm-dest");

  // 首源即存储错误（零并入）+ dest 既有：错误帧且 dest 载荷原样、不写回
  let out = rt.block_on(Arc::clone(&api).exec_slow(
    RespCommand::Pfmerge,
    vec![b"zm-dest".to_vec(), b"zm-bad".to_vec(), b"zm-good".to_vec()],
    wconf::DEFAULT_RESP_VERSION,
  ));
  assert_eq!(out, err_frame(RESP_ERR_SLOW_PATH_STORAGE));
  assert_eq!(
    raw_blob(&rt, &store, b"zm-dest"),
    dest_before,
    "零并入存储错误不得写回改写 dest"
  );

  // 首源即存储错误（零并入）+ dest 缺失：错误帧且 dest 不建键
  let out = rt.block_on(Arc::clone(&api).exec_slow(
    RespCommand::Pfmerge,
    vec![
      b"zm-absent".to_vec(),
      b"zm-bad".to_vec(),
      b"zm-good".to_vec(),
    ],
    wconf::DEFAULT_RESP_VERSION,
  ));
  assert_eq!(out, err_frame(RESP_ERR_SLOW_PATH_STORAGE));
  let probe = store.new_session().unwrap();
  let batch = probe.enter_batch();
  assert_eq!(
    batch.try_read_sync(b"zm-absent", |_| true).unwrap(),
    StoreResult::NotFound,
    "零并入存储错误不得凭空创建 dest"
  );
}

/// PFMERGE 错误路径 dest 终态快/慢双臂对拍：同一输入（合法源先并、次源出错）
/// ——快臂热区 WRONGTYPE 源（store_hll 补写后答 WRONGTYPE_HLL）、慢臂冷区存储
/// 错误源（rmw_string 补写后上抛统一错误帧），帧类别为第二层分叉、dest 终态
/// 载荷须逐字节全等（修复前慢臂 `?` 早退弃置已并入部分致 dest 缺失，双臂漂移）
#[test]
fn pfmerge_error_partial_commit_fast_slow_arm_parity() {
  // 快臂环境：全热区，bad 为 String 非法载荷源（热读命中 → WRONGTYPE）
  let fast_dir = tempdir().unwrap();
  let fast_device = Arc::new(
    SegmentedDevice::single_file(fast_dir.path().join("hll-pfmerge-parity-fast.db")).unwrap(),
  );
  let fast_store = Arc::new(WedbStore::open(test_store_config(), fast_device).unwrap());
  let fast_rt = Runtime::new().unwrap();
  let fast_api: GarnetApi = Arc::new(StoreGarnetApi::new(fast_store.new_session().unwrap()));
  let mut sf = RespServerSession::new(1, RespServerSessionOptions::default());
  sf.set_garnet_api(Arc::clone(&fast_api));
  fast_api.exec(&mut sf, RespCommand::Pfadd, &[b"px", b"g1", b"g2", b"g3"]);
  assert_eq!(sf.output, b":1\r\n");
  {
    let probe = fast_store.new_session().unwrap();
    let batch = probe.enter_batch();
    let _ = batch.try_upsert_sync(b"pbad", b"100");
  }
  sf.output.clear();
  fast_api.exec(&mut sf, RespCommand::Pfmerge, &[b"py", b"px", b"pbad"]);
  assert_eq!(
    sf.output,
    hll_wrongtype_frame(),
    "快臂错误臂须答 WRONGTYPE_HLL"
  );
  let fast_blob = raw_blob(&fast_rt, &fast_store, b"py");

  // 慢臂环境：同键同序，bad 冷化 + 截断（冷区读 I/O 错误 → 存储错误）
  let (rt, slow_api, slow_store, _slow_dir) = slow_fault_env("hll-pfmerge-parity-slow.db");
  let mut ss = RespServerSession::new(1, RespServerSessionOptions::default());
  ss.set_garnet_api(Arc::clone(&slow_api));
  {
    let probe = slow_store.new_session().unwrap();
    let batch = probe.enter_batch();
    let _ = batch.try_upsert_sync(b"pbad", b"100");
  }
  rt.block_on(slow_store.flush_and_evict_all()).unwrap();
  ss.output.clear();
  slow_api.exec(&mut ss, RespCommand::Pfadd, &[b"px", b"g1", b"g2", b"g3"]);
  assert_eq!(ss.output, b":1\r\n");
  truncate_segment(&slow_store);
  let out = rt.block_on(Arc::clone(&slow_api).exec_slow(
    RespCommand::Pfmerge,
    vec![b"py".to_vec(), b"px".to_vec(), b"pbad".to_vec()],
    wconf::DEFAULT_RESP_VERSION,
  ));
  assert_eq!(
    out,
    err_frame(RESP_ERR_SLOW_PATH_STORAGE),
    "慢臂存储错误臂答统一错误帧（帧类别为第二层分叉）"
  );
  let slow_blob = raw_blob(&rt, &slow_store, b"py");

  // dest 终态逐字节全等：同一 PFMERGE 失败后终态不随执行臂漂移
  assert_eq!(fast_blob, slow_blob, "快/慢双臂部分提交 dest 终态须全等");
  let hll = HyperLogLog::new();
  let mut probe = slow_blob.clone();
  assert!(hll.is_valid_hyll(&probe), "补写载荷须为合法 HYLL 编码");
  assert_eq!(hll.count(&mut probe), 3, "终态 = 稀疏种子并入全部合法源");
}

/// 设备故障注入开关共享态（wkv/tests/store/rename_semantics.rs:InjectSwitches
/// 同款计数定点恒败口径：0 = 关闭不计数，N = 自第 N 次对应 I/O 起恒败粘性）
#[derive(Default)]
struct InjectSwitches {
  fail_read_from: AtomicU64,
  fail_write_from: AtomicU64,
  reads: AtomicU64,
  writes: AtomicU64,
}

/// 真设备故障注入包装：全方法委托 SegmentedDevice，读写按开关定点恒败——
/// 生产写失败同形的真 IO 错误上抛（非假 mock 虚设应答）
struct InjectFailDevice {
  inner: SegmentedDevice,
  switches: Arc<InjectSwitches>,
}

impl InjectFailDevice {
  fn tripped(counter: &AtomicU64, from: &AtomicU64) -> bool {
    match from.load(Ordering::Relaxed) {
      0 => false,
      n => counter.fetch_add(1, Ordering::Relaxed) + 1 >= n,
    }
  }
}

impl Device for InjectFailDevice {
  fn sector_size(&self) -> usize {
    self.inner.sector_size()
  }

  fn segment_size(&self) -> u64 {
    self.inner.segment_size()
  }

  fn direct_io(&self) -> bool {
    self.inner.direct_io()
  }

  fn start_segment(&self) -> u32 {
    self.inner.start_segment()
  }

  fn end_segment(&self) -> Option<u32> {
    self.inner.end_segment()
  }

  fn capacity(&self) -> Option<u64> {
    self.inner.capacity()
  }

  fn pool(&self) -> &Arc<BufferPool> {
    self.inner.pool()
  }

  async fn write_aligned(&self, offset: u64, buf: AlignedBuf) -> (wdev::Result<usize>, AlignedBuf) {
    if Self::tripped(&self.switches.writes, &self.switches.fail_write_from) {
      return (
        Err(WdevError::Io(io::Error::other(
          "injected device write failure",
        ))),
        buf,
      );
    }
    self.inner.write_aligned(offset, buf).await
  }

  async fn read_aligned(&self, offset: u64, buf: AlignedBuf) -> (wdev::Result<usize>, AlignedBuf) {
    if Self::tripped(&self.switches.reads, &self.switches.fail_read_from) {
      return (
        Err(WdevError::Io(io::Error::other(
          "injected device read failure",
        ))),
        buf,
      );
    }
    self.inner.read_aligned(offset, buf).await
  }

  async fn read_raw(&self, offset: u64, buf: AlignedBuf) -> (wdev::Result<usize>, AlignedBuf) {
    if Self::tripped(&self.switches.reads, &self.switches.fail_read_from) {
      return (
        Err(WdevError::Io(io::Error::other(
          "injected device read failure",
        ))),
        buf,
      );
    }
    self.inner.read_raw(offset, buf).await
  }

  async fn sync(&self) -> wdev::Result<()> {
    self.inner.sync().await
  }

  fn get_file_size(&self, segment_id: u32) -> wdev::Result<u64> {
    self.inner.get_file_size(segment_id)
  }

  async fn remove_segment(&self, segment_id: u32) -> wdev::Result<()> {
    self.inner.remove_segment(segment_id).await
  }

  async fn truncate_until_segment(&self, segment_id: u32) -> wdev::Result<()> {
    self.inner.truncate_until_segment(segment_id).await
  }

  fn reset(&self) {
    self.inner.reset();
  }

  fn recover(&self) -> wdev::Result<()> {
    self.inner.recover()
  }
}

/// PFMERGE 慢臂存储错误臂补写自身失败（票 whll-pfmerge-storage-error-partial-commit
/// 测试点四）：源出错后已并入部分的 rmw_string 补写遇真设备写故障（小环形日志
/// 饱和 + InjectFailDevice 写开关恒败，victim 页驱逐刷盘即生产写失败形态）→
/// 径直上抛 Err(())——应答面恒单行 RESP_ERR_SLOW_PATH_STORAGE、绝不落成功面
///（+OK 零出现）；段文件截断令坏源冷读必败（读开关不启用，双臂判点分离）
#[test]
fn pfmerge_slowpath_partial_commit_write_failure_never_answers_success() {
  let rt = Runtime::new().unwrap();
  let dir = tempdir().unwrap();
  let switches = Arc::new(InjectSwitches::default());
  let inner = SegmentedDevice::single_file(dir.path().join("hll-pfmerge-commitfail.db")).unwrap();
  let device = Arc::new(InjectFailDevice {
    inner,
    switches: Arc::clone(&switches),
  });
  // 小环形日志（page_size 16KB × 4 页，degrade_env 同款）：饱和期命令期内
  // 任何新 append 都须驱逐并刷写 victim 脏页（触达写开关注入面）
  let store =
    Arc::new(WedbStore::open(StoreConfig::new(1024, 16 * 1024, 4, 0.5).unwrap(), device).unwrap());
  let api: GarnetApi = Arc::new(StoreGarnetApi::new(store.new_session().unwrap()));
  let mut s = RespServerSession::new(1, RespServerSessionOptions::default());
  s.set_garnet_api(Arc::clone(&api));

  // 坏源先建后冷化（截断后冷区读必败）
  api.exec(&mut s, RespCommand::Pfadd, &[b"cf-bad", b"z1"]);
  assert_eq!(s.output, b":1\r\n");
  rt.block_on(store.flush_and_evict_all()).unwrap();

  // 环形饱和：大值 SET 流推 head 进入回绕驱逐区
  let filler = vec![b'v'; 2048];
  for i in 0..200 {
    let key = format!("cf-fill{i}");
    s.output.clear();
    api.exec(&mut s, RespCommand::Set, &[key.as_bytes(), &filler]);
    if s.output.is_empty() {
      // 降级笔转慢路径闭环（保持恒 +OK 供给，不残留未裁决会话）
      let out = rt.block_on(Arc::clone(&api).exec_slow(
        RespCommand::Set,
        vec![key.into_bytes(), filler.clone(), b"".to_vec()],
        wconf::DEFAULT_RESP_VERSION,
      ));
      assert_eq!(out, b"+OK\r\n");
    }
  }
  // 合法源最后写（恒内存热读，先于坏源成功并入）；再以一枚大填充逼近页界，
  // 令 dest 补写 append 必落新页触发 victim 驱逐刷盘
  s.output.clear();
  api.exec(
    &mut s,
    RespCommand::Pfadd,
    &[b"cf-good", b"g1", b"g2", b"g3"],
  );
  assert_eq!(s.output, b":1\r\n");
  s.output.clear();
  api.exec(&mut s, RespCommand::Set, &[b"cf-tail", &[b'w'; 14 * 1024]]);
  if s.output.is_empty() {
    let out = rt.block_on(Arc::clone(&api).exec_slow(
      RespCommand::Set,
      vec![b"cf-tail".to_vec(), vec![b'w'; 14 * 1024], b"".to_vec()],
      wconf::DEFAULT_RESP_VERSION,
    ));
    assert_eq!(out, b"+OK\r\n");
  }

  OpenOptions::new()
    .write(true)
    .open(store.device.inner.segment_path(0))
    .expect("段文件应已随 store 打开创建")
    .set_len(0)
    .expect("截断段文件注入磁盘读故障");
  switches.fail_write_from.store(1, Ordering::Relaxed);

  // dest 缺失 + [合法源, 存储错误源]：补写遇设备写故障径直上抛——单行错误帧、
  // 零成功帧（修复面自证：错误帧类别不因补写成败漂移）
  let out = rt.block_on(Arc::clone(&api).exec_slow(
    RespCommand::Pfmerge,
    vec![b"cf-dest".to_vec(), b"cf-good".to_vec(), b"cf-bad".to_vec()],
    wconf::DEFAULT_RESP_VERSION,
  ));
  assert_eq!(out, err_frame(RESP_ERR_SLOW_PATH_STORAGE));
  assert!(
    !out.windows(3).any(|w| w == b"+OK"),
    "补写失败路径绝不落成功面: {out:?}"
  );
}

/// PFCOUNT 单键短路：稀疏、稠密、缺失键以及多键联合计数测试
#[test]
fn pfcount_single_key_sparse_and_dense_and_missing() {
  with_batch(|sess, batch| {
    let key_missing: &[u8] = b"hll-missing";
    let key_sparse: &[u8] = b"hll-sparse";
    let key_dense: &[u8] = b"hll-dense";

    // 1. 缺失键单键 PFCOUNT 返回 0
    let mut out = Vec::new();
    sess
      .hyper_log_log_length(&[key_missing], batch, &mut out)
      .unwrap();
    assert_eq!(parse_resp_int(&out), 0);

    // 2. 稀疏单键 PFCOUNT
    for i in 0..10 {
      let elem = format!("sparse_elem_{i}");
      let mut out = Vec::new();
      sess
        .hyper_log_log_add(&[key_sparse, elem.as_bytes()], batch, &mut out)
        .unwrap();
    }
    let mut out = Vec::new();
    sess
      .hyper_log_log_length(&[key_sparse], batch, &mut out)
      .unwrap();
    assert_eq!(parse_resp_int(&out), 10);

    // 3. 稠密单键 PFCOUNT（插入大量不同元素触发稀疏转稠密）
    for i in 0..2000 {
      let elem = format!("dense_elem_{i}");
      let mut out = Vec::new();
      sess
        .hyper_log_log_add(&[key_dense, elem.as_bytes()], batch, &mut out)
        .unwrap();
    }
    let mut out = Vec::new();
    sess
      .hyper_log_log_length(&[key_dense], batch, &mut out)
      .unwrap();
    let dense_card = parse_resp_int(&out);
    // HLL 估算误差在合理范围内（2000 左右）
    assert!((dense_card - 2000).abs() < 100);

    // 4. 多键联合 PFCOUNT（包含缺失键、稀疏键、稠密键）
    let mut out = Vec::new();
    sess
      .hyper_log_log_length(&[key_missing, key_sparse, key_dense], batch, &mut out)
      .unwrap();
    let union_card = parse_resp_int(&out);
    assert!((union_card - (10 + 2000)).abs() < 100);
  });
}

/// 批量写入元素：消除 needless_lifetimes 警告，外提缓冲区消除循环内堆分配
fn batch_add_elements<D: Device>(
  sess: &mut RespServerSession,
  batch: &BatchStoreSession<'_, D>,
  key: &[u8],
  prefix: &str,
  range: Range<usize>,
) {
  let mut out = Vec::with_capacity(32);
  let mut elem = String::with_capacity(prefix.len() + 16);
  for i in range {
    elem.clear();
    let _ = write!(&mut elem, "{prefix}_{i}");
    out.clear();
    sess
      .hyper_log_log_add(&[key, elem.as_bytes()], batch, &mut out)
      .unwrap();
  }
}

/// 对标 Garnet HyperLogLogTestPFMERGE_SparseToDenseV2 / SparseToSparseV2 / DenseToDenseV2
/// 覆盖 sparse->sparse、sparse->dense、dense->sparse、dense->dense 与多源合并方向矩阵及 PFCOUNT 精确性
#[test]
fn pfmerge_direction_matrix_and_multi_source_count() {
  with_batch(|sess, batch| {
    let hll = HyperLogLog::new();

    let key_s1: &[u8] = b"matrix_s1";
    let key_s2: &[u8] = b"matrix_s2";
    let key_d1: &[u8] = b"matrix_d1";
    let key_d2: &[u8] = b"matrix_d2";
    let key_missing: &[u8] = b"matrix_missing";

    batch_add_elements(sess, batch, key_s1, "s_elem", 0..30);
    batch_add_elements(sess, batch, key_s2, "s_elem", 20..50);
    batch_add_elements(sess, batch, key_d1, "d_elem", 0..3000);
    batch_add_elements(sess, batch, key_d2, "d_elem", 2000..5000);

    // 验证编码状态：s1 为稀疏，d1 为稠密
    let is_s1_sparse = batch.try_read_sync(key_s1, |v| hll.is_sparse(v)).unwrap();
    let is_d1_dense = batch.try_read_sync(key_d1, |v| hll.is_dense(v)).unwrap();
    assert_eq!(is_s1_sparse, StoreResult::Success(true));
    assert_eq!(is_d1_dense, StoreResult::Success(true));

    // 1. Sparse -> Sparse 合并（目标初始为稀疏，并入稀疏源，保持稀疏）
    let dst_ss: &[u8] = b"dst_sparse_to_sparse";
    let mut out = Vec::new();
    sess
      .hyper_log_log_merge(&[dst_ss, key_s1, key_s2], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"+OK\r\n");

    let is_ss_sparse = batch.try_read_sync(dst_ss, |v| hll.is_sparse(v)).unwrap();
    assert_eq!(is_ss_sparse, StoreResult::Success(true));

    out.clear();
    sess
      .hyper_log_log_length(&[dst_ss], batch, &mut out)
      .unwrap();
    // 0..50 共 50 个不重复元素，稀疏基数精确为 50
    assert_eq!(parse_resp_int(&out), 50);

    // 2. Sparse -> Dense 升级合并（目标初始为稀疏，并入稠密源，自动升级为稠密）
    let dst_sd: &[u8] = b"dst_sparse_to_dense";
    batch_add_elements(sess, batch, dst_sd, "s_elem", 0..30);
    out.clear();
    sess
      .hyper_log_log_merge(&[dst_sd, key_d1], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"+OK\r\n");

    let is_sd_dense = batch.try_read_sync(dst_sd, |v| hll.is_dense(v)).unwrap();
    assert_eq!(
      is_sd_dense,
      StoreResult::Success(true),
      "稀疏目标并入稠密源后应升级为稠密"
    );

    out.clear();
    sess
      .hyper_log_log_length(&[dst_sd], batch, &mut out)
      .unwrap();
    let sd_card = parse_resp_int(&out);
    // 30 个 s_elem 与 3000 个 d_elem 互不重叠，基数约 3030
    assert!((sd_card - 3030).abs() < 150, "sd_card = {sd_card}");

    // 3. Dense -> Sparse 合并（目标初始为稠密，并入稀疏源，保持稠密）
    let dst_ds: &[u8] = b"dst_dense_to_sparse";
    batch_add_elements(sess, batch, dst_ds, "d_elem", 0..3000);
    out.clear();
    sess
      .hyper_log_log_merge(&[dst_ds, key_s1], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"+OK\r\n");

    let is_ds_dense = batch.try_read_sync(dst_ds, |v| hll.is_dense(v)).unwrap();
    assert_eq!(is_ds_dense, StoreResult::Success(true));

    out.clear();
    sess
      .hyper_log_log_length(&[dst_ds], batch, &mut out)
      .unwrap();
    let ds_card = parse_resp_int(&out);
    assert!((ds_card - 3030).abs() < 150, "ds_card = {ds_card}");

    // 4. Dense -> Dense 合并（目标稠密，源稠密）
    let dst_dd: &[u8] = b"dst_dense_to_dense";
    batch_add_elements(sess, batch, dst_dd, "d_elem", 0..3000);
    out.clear();
    sess
      .hyper_log_log_merge(&[dst_dd, key_d2], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"+OK\r\n");

    let is_dd_dense = batch.try_read_sync(dst_dd, |v| hll.is_dense(v)).unwrap();
    assert_eq!(is_dd_dense, StoreResult::Success(true));

    out.clear();
    sess
      .hyper_log_log_length(&[dst_dd], batch, &mut out)
      .unwrap();
    let dd_card = parse_resp_int(&out);
    // d1 (0..3000) 与 d2 (2000..5000) 并集为 5000
    assert!((dd_card - 5000).abs() < 200, "dd_card = {dd_card}");

    // 5. 多源合并（空 dest + 多个稀疏 + 多个稠密 + 缺失键）
    let dst_multi: &[u8] = b"dst_multi_sources";
    out.clear();
    sess
      .hyper_log_log_merge(
        &[dst_multi, key_missing, key_s1, key_s2, key_d1, key_d2],
        batch,
        &mut out,
      )
      .unwrap();
    assert_eq!(out, b"+OK\r\n");

    out.clear();
    sess
      .hyper_log_log_length(&[dst_multi], batch, &mut out)
      .unwrap();
    let merged_count = parse_resp_int(&out);

    // 与多键联合 PFCOUNT 对比
    out.clear();
    sess
      .hyper_log_log_length(
        &[key_missing, key_s1, key_s2, key_d1, key_d2],
        batch,
        &mut out,
      )
      .unwrap();
    let multi_count = parse_resp_int(&out);

    // 合并后单键计数必须与多键联合 PFCOUNT 估算一致（同一并集状态）
    assert_eq!(merged_count, multi_count);
    // 总独立元素数：50 (s_elem) + 5000 (d1/d2 并集) = 5050
    assert!(
      (merged_count - 5050).abs() < 200,
      "merged_count = {merged_count}"
    );

    // 验证各源键在被合并后保持只读不变量（基数未被更改）
    out.clear();
    sess
      .hyper_log_log_length(&[key_s1], batch, &mut out)
      .unwrap();
    assert_eq!(parse_resp_int(&out), 30);

    out.clear();
    sess
      .hyper_log_log_length(&[key_s2], batch, &mut out)
      .unwrap();
    assert_eq!(parse_resp_int(&out), 30);

    out.clear();
    sess
      .hyper_log_log_length(&[key_d1], batch, &mut out)
      .unwrap();
    let d1_card = parse_resp_int(&out);
    assert!((d1_card - 3000).abs() < 150);

    out.clear();
    sess
      .hyper_log_log_length(&[key_d2], batch, &mut out)
      .unwrap();
    let d2_card = parse_resp_int(&out);
    assert!((d2_card - 3000).abs() < 150);
  });
}

/// 畸形 HLL 载荷（非法的 HLL 头、错误的魔数、超长或截断的稀疏字节）写入与合并时拒绝测试
#[test]
fn malformed_hyperloglog_payloads_rejected_on_write_and_merge() {
  with_batch(|sess, batch| {
    let hll = HyperLogLog::new();
    let mut valid_sparse = vec![0_u8; hll.sparse_bytes()];
    hll.init_sparse(&mut valid_sparse);

    let test_cases: [(&str, Vec<u8>); 8] = [
      // 1. 非法头：长度不足 16 字节
      ("truncated_header", valid_sparse[..10].to_vec()),
      // 2. 非法头：未知数据类型 (offset 3 = 0x42)
      ("invalid_dtype", {
        let mut p = valid_sparse.clone();
        p[3] = 0x42;
        p
      }),
      // 3. 非法头：声称稠密但长度不足 12304 字节
      ("dense_length_mismatch", {
        let mut p = valid_sparse.clone();
        p[3] = 1;
        p
      }),
      // 4. 错误魔数：offset 4..8 非 "HYLL"
      ("corrupted_magic", {
        let mut p = valid_sparse.clone();
        p[4..8].copy_from_slice(b"FAIL");
        p
      }),
      // 5. 截断稀疏字节：长度小于 sparse_initial_length(1) = 146
      ("sparse_truncated_bytes", valid_sparse[..100].to_vec()),
      // 6. 稀疏 RLE 声明长度超长（超出实际载荷容量）
      ("sparse_rle_size_overflow", {
        let mut p = valid_sparse.clone();
        p[16..18].copy_from_slice(&65000_u16.to_le_bytes());
        p
      }),
      // 7. 稀疏 RLE 寄存器覆盖不完整（首字节篡改为 0x80，覆盖不足 16384）
      ("sparse_coverage_mismatch", {
        let mut p = valid_sparse.clone();
        p[18] = 0x80;
        p
      }),
      // 8. 超长稀疏字节：超过 4KB 上限
      ("sparse_overlong_cap", {
        let mut p = vec![0_u8; 5000];
        p[..valid_sparse.len()].copy_from_slice(&valid_sparse);
        p
      }),
    ];

    let valid_key = b"valid_hll_key";
    let mut out = Vec::new();
    sess
      .hyper_log_log_add(&[valid_key, b"init"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    for (name, bad_payload) in test_cases {
      let bad_key = format!("bad_hll_{name}");
      let bad_key_bytes = bad_key.as_bytes();

      // 先将畸形载荷写入存储
      let _ = batch.try_upsert_sync(bad_key_bytes, &bad_payload).unwrap();

      // 1. PFADD 写入时拒绝，返回 WRONGTYPE 帧
      out.clear();
      sess
        .hyper_log_log_add(&[bad_key_bytes, b"elem"], batch, &mut out)
        .unwrap();
      assert_eq!(
        out,
        hll_wrongtype_frame(),
        "{name}: PFADD 应对畸形载荷返回 WRONGTYPE"
      );

      // 2. PFMERGE 作为目标键写入时拒绝，返回 WRONGTYPE 帧
      out.clear();
      sess
        .hyper_log_log_merge(&[bad_key_bytes, valid_key], batch, &mut out)
        .unwrap();
      assert_eq!(
        out,
        hll_wrongtype_frame(),
        "{name}: PFMERGE(dest) 应对畸形目标载荷返回 WRONGTYPE"
      );

      // 3. PFMERGE 作为源键读取合并时拒绝，返回 WRONGTYPE 帧
      let dest_merge = format!("dst_merge_{name}");
      out.clear();
      sess
        .hyper_log_log_merge(&[dest_merge.as_bytes(), bad_key_bytes], batch, &mut out)
        .unwrap();
      assert_eq!(
        out,
        hll_wrongtype_frame(),
        "{name}: PFMERGE(src) 应对畸形源载荷返回 WRONGTYPE"
      );

      // 4. PFCOUNT 单键读取拒绝
      out.clear();
      sess
        .hyper_log_log_length(&[bad_key_bytes], batch, &mut out)
        .unwrap();
      assert_eq!(
        out,
        hll_wrongtype_frame(),
        "{name}: PFCOUNT 应对畸形载荷返回 WRONGTYPE"
      );

      // 5. 多键 PFCOUNT 回路中遇畸形键立刻短路返回 WRONGTYPE
      out.clear();
      sess
        .hyper_log_log_length(&[valid_key, bad_key_bytes], batch, &mut out)
        .unwrap();
      assert_eq!(
        out,
        hll_wrongtype_frame(),
        "{name}: 多键 PFCOUNT 遇畸形键应返回 WRONGTYPE"
      );

      // 6. 验证存储中的畸形载荷未被篡改覆盖（就地切片比对，零拷贝）
      let raw_matched = batch
        .try_read_sync(bad_key_bytes, |v| v == bad_payload.as_slice())
        .unwrap();
      assert_eq!(
        raw_matched,
        StoreResult::Success(true),
        "{name}: 畸形键不得被写入操作修改"
      );
    }
  });
}

/// test/standalone/Garnet.test.complexstring/HyperLogLogTests.cs:HyperLogLogRestoreCorruptedDumpPayloadIsRejected
/// 验证 RESTORE 命令拒绝被篡改（校验和不匹配）的 HLL DUMP 载荷
#[test]
fn hyperloglog_restore_corrupted_dump_payload_is_rejected() {
  with_batch(|sess, batch| {
    let src_key: &[u8] = b"hll_dump_src";
    let restore_key: &[u8] = b"hll_dump_dst";

    // 1. 创建源 HLL
    let mut out = Vec::new();
    sess
      .hyper_log_log_add(&[src_key, b"elem0", b"elem1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    // 2. DUMP 出载荷
    out.clear();
    sess.network_dump(&[src_key], batch, &mut out).unwrap();
    assert!(out.starts_with(b"$"));
    let first_crlf = out.iter().position(|&b| b == b'\r').unwrap();
    let payload = &out[first_crlf + 2..out.len() - 2];
    let mut corrupted_dump = payload.to_vec();

    // 3. 翻转非 CRC 字节（末尾 8 字节为 CRC64，前置 2 字节为版本），防下溢防御
    assert!(corrupted_dump.len() > 10, "dump 载荷过短");
    let corrupt_idx = 20.min(corrupted_dump.len() - 11);
    corrupted_dump[corrupt_idx] ^= 0x01;

    // 4. RESTORE 尝试恢复被篡改载荷，应返回错误帧
    out.clear();
    sess
      .network_restore(&[restore_key, b"0", &corrupted_dump], batch, None, &mut out)
      .unwrap();
    assert_eq!(
      out, b"-ERR DUMP payload version or checksum are wrong\r\n",
      "RESTORE 应拒绝损坏校验和的 HLL dump"
    );

    // 5. 目标键未被创建
    assert_eq!(
      batch.try_read_sync(restore_key, |_| true).unwrap(),
      StoreResult::NotFound,
      "被拒后目标键不得被创建"
    );
  });
}

/// 键哈希（与版本表分桶同一哈希面：根域 scoped，与默认 (0,0) 写会话同源）
fn h(key: &[u8]) -> u64 {
  TxnKeyEntryComparison::scoped_key_hash(SessionPrefixBuf::ROOT.as_slice(), key) as u64
}

/// PFADD 扩容分支（copy_update 迁移）重复元素：对标 C# CopyUpdater 的
/// `updated = HyperLogLog.DefaultHLL.CopyUpdate(...)` 回写
/// （RMWMethods.cs:1237）→ `*output = updated ? 1 : 0`，命令层仅
/// pfaddUpdated > 0 才回 :1（HyperLogLogCommands.cs:49-58）——原位余量不足
/// 触发载荷迁移时，重复元素无寄存器变更须答 :0 且不写回（载荷字节不虚胖、
/// WATCH 版本不推进）。修复前丢弃 copy_update 的 bool 结果无条件视为已
/// 更新：误答 :1 并脏写推进 WATCH 版本
///
/// C# 对照事实（deviations §132，纯台账注）：同形 C# :0 臂仍推版本落
/// AOF（RMWMethods.cs:425-430），且无余量/磁盘驻形拷贝臂迁移写回使
/// STRLEN 变 current+128 或升稠密 12304（VarLenInputMethods.cs:280-287）、
/// EXEC 观察者中止——rust 零写回系有意偏差，严禁对齐
#[test]
fn pfadd_grow_branch_duplicates_answer_zero_without_writeback() {
  let rt = Runtime::new().unwrap();
  let dir = tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("hll-grow.db")).unwrap());
  let store = Arc::new(WedbStore::open(test_store_config(), device).unwrap());
  let map = Arc::new(WatchVersionMap::new(1 << 10));
  store.set_watch_hook(version_map_watch_hook(Arc::clone(&map)));
  let session = store.new_session().unwrap();
  let batch = session.enter_batch();
  let mut sess = RespServerSession::default();

  let hll = HyperLogLog::new();
  let key: &[u8] = b"hll-grow";
  // 50 元素：建键出形 274B——C# SparseInitialLength(50)=146 折叠形经案一
  // sparse_fits 峰值复检拒（146+100 ≥ 146），按扇区自零段基座上探至 274
  // （146+100 < 274 成立）；RLE 增长上限 128+100=228 < 容量 256，init 尾移
  // 写峰绝触分配界——原「init 原位溢出面 C# 裸指针同源缺陷另案处理」已由
  // task zcode-r151c-pfconv 案一以唯一谓词折叠收口；第二批同规模重放时
  // current+2*50 ≥ 274 恒走扩容迁移分支
  let elems: Vec<Vec<u8>> = (0..50)
    .map(|i| format!("grow-elem-{i}").into_bytes())
    .collect();
  let mut args: Vec<&[u8]> = vec![key];
  args.extend(elems.iter().map(|v| v.as_slice()));

  // 1. 建键：50 新元素 → :1，版本推进
  let mut out = Vec::new();
  sess.hyper_log_log_add(&args, &batch, &mut out).unwrap();
  assert_eq!(parse_resp_int(&out), 1);
  let v1 = map.read_version(h(key));
  assert_ne!(v1, 0, "建键写回应推进 WATCH 版本");

  // 2. 钉死触发前提：载荷稀疏且原位余量不足（同规模第二批必走迁移分支）
  let blob_before = match batch.try_read_sync(key, |v| v.to_vec()).unwrap() {
    StoreResult::Success(v) => v,
    other => panic!("读回失败: {other:?}"),
  };
  assert!(hll.is_sparse(&blob_before));
  assert!(
    !hll.can_grow_in_place(&blob_before, blob_before.len(), elems.len()),
    "测试前提：第二批同规模 PFADD 必走扩容迁移分支"
  );

  // 3. 快路径同批老元素重放：无寄存器变更 → :0、零写回
  out.clear();
  sess.hyper_log_log_add(&args, &batch, &mut out).unwrap();
  assert_eq!(parse_resp_int(&out), 0, "扩容分支重复元素须答 :0");
  assert_eq!(
    map.read_version(h(key)),
    v1,
    "无变更不得写回推进 WATCH 版本"
  );
  match batch.try_read_sync(key, |v| v.to_vec()).unwrap() {
    StoreResult::Success(blob_after) => assert_eq!(
      blob_after, blob_before,
      "无变更不得改写存储载荷（杜绝内存虚胖）"
    ),
    other => panic!("读回失败: {other:?}"),
  }

  // 4. 冷化走慢路径同款：快路径降级无应答，exec_slow 重复元素 → :0 且
  //    版本不动（rmw_string 不触发）
  drop(batch);
  drop(session);
  rt.block_on(store.flush_and_evict_all()).unwrap();
  let api: GarnetApi = Arc::new(StoreGarnetApi::new(store.new_session().unwrap()));
  let mut s = RespServerSession::new(1, RespServerSessionOptions::default());
  s.set_garnet_api(Arc::clone(&api));
  api.exec(&mut s, RespCommand::Pfadd, &args);
  assert!(
    s.output.is_empty(),
    "冷数据 PFADD 快路径须降级慢路径：{:?}",
    String::from_utf8_lossy(&s.output)
  );

  let slow_args: Vec<Vec<u8>> = Some(key.to_vec())
    .into_iter()
    .chain(elems.iter().map(|v| v.to_vec()))
    .collect();
  let out = rt.block_on(Arc::clone(&api).exec_slow(
    RespCommand::Pfadd,
    slow_args,
    wconf::DEFAULT_RESP_VERSION,
  ));
  assert_eq!(out, b":0\r\n", "慢路径扩容分支重复元素须答 :0");
  assert_eq!(
    map.read_version(h(key)),
    v1,
    "慢路径无变更不得推进 WATCH 版本"
  );

  // 5. 对照：扩容分支遇新元素仍 :1 且写回推进版本（迁移有变更路径不受影响）
  let new_elems: Vec<Vec<u8>> = (50..100)
    .map(|i| format!("grow-elem-{i}").into_bytes())
    .collect();
  let slow_new: Vec<Vec<u8>> = Some(key.to_vec())
    .into_iter()
    .chain(new_elems.iter().map(|v| v.to_vec()))
    .collect();
  let out = rt.block_on(Arc::clone(&api).exec_slow(
    RespCommand::Pfadd,
    slow_new,
    wconf::DEFAULT_RESP_VERSION,
  ));
  assert_eq!(out, b":1\r\n", "扩容分支新元素须答 :1");
  assert!(map.read_version(h(key)) > v1, "新元素写回应推进 WATCH 版本");
}

/// PFMERGE 全源缺失（快路径）：对标 HyperLogLogOps.cs:224-265 的
/// SET_Conditional 仅在源 GET 命中后于循环内执行，全 NOTFOUND 零 SET——
/// dest 缺失不被凭空建键（幽灵键），dest 既有不盲写（载荷不变、WATCH
/// 版本不推进）。修复前循环外无条件 store_hll 写回空稀疏载荷
#[test]
fn pfmerge_all_sources_missing_fastpath_no_phantom_no_blind_write() {
  let dir = tempdir().unwrap();
  let device =
    Arc::new(SegmentedDevice::single_file(dir.path().join("hll-merge-noop.db")).unwrap());
  let store = Arc::new(WedbStore::open(test_store_config(), device).unwrap());
  let map = Arc::new(WatchVersionMap::new(1 << 10));
  store.set_watch_hook(version_map_watch_hook(Arc::clone(&map)));
  let session = store.new_session().unwrap();
  let batch = session.enter_batch();
  let mut sess = RespServerSession::default();

  // 1. dest 缺失 + 全源缺失：+OK 且 dest 不建键
  let dest: &[u8] = b"pfm-noop-dest";
  let mut out = Vec::new();
  sess
    .hyper_log_log_merge(&[dest, b"miss1", b"miss2"], &batch, &mut out)
    .unwrap();
  assert_eq!(out, b"+OK\r\n");
  assert_eq!(
    batch.try_read_sync(dest, |_| true).unwrap(),
    StoreResult::NotFound,
    "全源缺失 PFMERGE 不得凭空创建 dest"
  );

  // 2. dest 既有（合法 HLL）+ 全源缺失：+OK、载荷不变、WATCH 版本不推进
  let mut out = Vec::new();
  sess
    .hyper_log_log_add(&[dest, b"a", b"b", b"c"], &batch, &mut out)
    .unwrap();
  assert_eq!(parse_resp_int(&out), 1);
  let v1 = map.read_version(h(dest));
  let before = match batch.try_read_sync(dest, |v| v.to_vec()).unwrap() {
    StoreResult::Success(v) => v,
    other => panic!("读回失败: {other:?}"),
  };

  out.clear();
  sess
    .hyper_log_log_merge(&[dest, b"miss1", b"miss2"], &batch, &mut out)
    .unwrap();
  assert_eq!(out, b"+OK\r\n");
  assert_eq!(
    map.read_version(h(dest)),
    v1,
    "全源缺失不得盲写推进 WATCH 版本"
  );
  let after = match batch.try_read_sync(dest, |v| v.to_vec()).unwrap() {
    StoreResult::Success(v) => v,
    other => panic!("读回失败: {other:?}"),
  };
  assert_eq!(after, before, "全源缺失不得改写 dest 载荷");
}

/// PFMERGE 全源缺失（慢路径 StorageSession 异步域）：与快路径同款零写回
/// 口径——dest 缺失不建键，dest 既有 rmw_string 不触发（WATCH 版本不动、
/// 载荷不变）。修复前循环外无条件 rmw_string 写回
#[test]
fn pfmerge_all_sources_missing_slowpath_no_phantom_no_blind_write() {
  let rt = Runtime::new().unwrap();
  let dir = tempdir().unwrap();
  let device =
    Arc::new(SegmentedDevice::single_file(dir.path().join("hll-merge-noop-slow.db")).unwrap());
  let store = Arc::new(WedbStore::open(test_store_config(), device).unwrap());
  let map = Arc::new(WatchVersionMap::new(1 << 10));
  store.set_watch_hook(version_map_watch_hook(Arc::clone(&map)));
  let api: GarnetApi = Arc::new(StoreGarnetApi::new(store.new_session().unwrap()));
  let mut s = RespServerSession::new(1, RespServerSessionOptions::default());
  s.set_garnet_api(Arc::clone(&api));

  // 1. dest 缺失 + 全源缺失：+OK 且 dest 不建键
  let out = rt.block_on(Arc::clone(&api).exec_slow(
    RespCommand::Pfmerge,
    vec![
      b"pfm-slow-dest".to_vec(),
      b"miss1".to_vec(),
      b"miss2".to_vec(),
    ],
    wconf::DEFAULT_RESP_VERSION,
  ));
  assert_eq!(out, b"+OK\r\n");

  let probe = store.new_session().unwrap();
  let batch = probe.enter_batch();
  assert_eq!(
    batch.try_read_sync(b"pfm-slow-dest", |_| true).unwrap(),
    StoreResult::NotFound,
    "全源缺失慢路径 PFMERGE 不得凭空创建 dest"
  );
  drop(batch);

  // 2. dest 既有（合法 HLL）+ 全源缺失：+OK、载荷不变、WATCH 版本不推进
  api.exec(
    &mut s,
    RespCommand::Pfadd,
    &[b"pfm-slow-dest", b"a", b"b", b"c"],
  );
  assert_eq!(s.output, b":1\r\n");
  let v1 = map.read_version(h(b"pfm-slow-dest"));
  let probe = store.new_session().unwrap();
  let batch = probe.enter_batch();
  let before = match batch
    .try_read_sync(b"pfm-slow-dest", |v| v.to_vec())
    .unwrap()
  {
    StoreResult::Success(v) => v,
    other => panic!("读回失败: {other:?}"),
  };
  drop(batch);
  drop(probe);

  let out = rt.block_on(Arc::clone(&api).exec_slow(
    RespCommand::Pfmerge,
    vec![
      b"pfm-slow-dest".to_vec(),
      b"miss1".to_vec(),
      b"miss2".to_vec(),
    ],
    wconf::DEFAULT_RESP_VERSION,
  ));
  assert_eq!(out, b"+OK\r\n");
  assert_eq!(
    map.read_version(h(b"pfm-slow-dest")),
    v1,
    "全源缺失慢路径不得盲写推进 WATCH 版本"
  );

  let probe = store.new_session().unwrap();
  let batch = probe.enter_batch();
  let after = match batch
    .try_read_sync(b"pfm-slow-dest", |v| v.to_vec())
    .unwrap()
  {
    StoreResult::Success(v) => v,
    other => panic!("读回失败: {other:?}"),
  };
  assert_eq!(after, before, "全源缺失慢路径不得改写 dest 载荷");
}

/// 单元素 PFADD（本组用例统一驱动同步快路径）
fn pfadd_one<D: Device>(
  sess: &mut RespServerSession,
  batch: &BatchStoreSession<'_, D>,
  key: &[u8],
  elem: &[u8],
) -> i64 {
  let mut out = Vec::new();
  sess
    .hyper_log_log_add(&[key, elem], batch, &mut out)
    .unwrap();
  parse_resp_int(&out)
}

/// 读回存储中的完整载荷（缺失/降级即失败：本组用例全程热区）
fn load_blob<D: Device>(batch: &BatchStoreSession<'_, D>, key: &[u8]) -> Vec<u8> {
  match batch.try_read_sync(key, |v| v.to_vec()).unwrap() {
    StoreResult::Success(v) => v,
    other => panic!("读回存储载荷失败: {other:?}"),
  }
}

/// 追加一枚按序生成的新元素并返回其字节
fn push_elem(elems: &mut Vec<Vec<u8>>, prefix: &str) -> Vec<u8> {
  elems.push(format!("{prefix}-{}", elems.len()).into_bytes());
  elems.last().unwrap().clone()
}

/// C# EstimationError 同口径（HyperLogLogTests.cs：`< 4.0` 百分比误差）
fn estimation_error(estimate: i64, actual: usize) -> f64 {
  (estimate as f64 - actual as f64).abs() / actual as f64 * 100.0
}

/// PFADD 应答与写回同源校验
///
/// C# TryUpdate 仅在无任何寄存器变更时返回 false，故 `:0` ⟺ 载荷逐字节不动
/// （不得写回）。`:1` 表示至少一枚寄存器值增大：单元素只映射一枚位置，非零
/// 寄存器数增量至多 1（位置已非零、仅值增大时增量为 0），且载荷必然落笔变化
fn assert_add_landed(hll: &HyperLogLog, before: &[u8], after: &[u8], nz: usize, updated: i64) {
  if updated == 0 {
    assert_eq!(after, before, "应答 :0 即无寄存器变更，不得写回载荷");
  } else {
    assert_ne!(after, before, "应答 :1 必有寄存器变更，载荷须落笔");
    let delta = hll.sparse_count_non_zero(after) as i64 - nz as i64;
    assert!(
      delta <= 1,
      "单元素至多新激活一枚非零寄存器: 增量 {delta}（{nz} -> {}）",
      hll.sparse_count_non_zero(after)
    );
  }
}

/// 稀疏扇区预留与原位增长防回归
///
/// 对标 test/standalone/Garnet.test.complexstring/HyperLogLogTests.cs:
/// HyperLogLogTestPFADDV2 的单元素连续 PFADD 稀疏链路，加上 C# 存储层的原位
/// 前提——MainStore/RMWMethods.cs:InPlaceUpdater :660 的
/// `valueLen = logRecord.ValueSpan.Length` 与 VarLenInputMethods.cs:
/// GetRMWModifiedFieldInfo 的 PFADD 臂 `UpdateGrow`：记录物理长度恒为分配长度
/// （含尾部 SparseMemorySectorSize 预留扇区，绝不截断），CanGrowInPlace 才能在
/// 后续连续插入上恒命中原位臂。rust 侧写回若按实际占用截断，该判定恒假，每一发
/// PFADD 都退化为重分配 + 全量拷贝的新版本记录（写放大）。
///
/// 断言四段：①新键与首次扩容均按分配口径落盘（扩容恰推进一个预留扇区）；
/// ②预留扇区内连续 50 发单元素 PFADD 记录长度恒定（原位改写，零二次扩容），
/// 且每发应答与载荷落笔同源（:0 ⟺ 零写回，:1 ⟺ 有落笔且非零寄存器增量 ≤ 1）；
/// ③重复元素无变更不写回（既有正确行为不回归）；④按 UpdateGrow 口径晋升稠密
/// （恒 12304B）且估算误差 < 4%（C# EstimationError 同口径）
#[test]
fn pfadd_sparse_growth_keeps_sector_slack_and_updates_in_place() {
  with_batch(|sess, batch| {
    let hll = HyperLogLog::new();
    let key: &[u8] = b"hll-inplace";
    let initial = hll.sparse_bytes();
    let mut elems: Vec<Vec<u8>> = Vec::new();

    // ① 建键：初始载荷即 SparseBytes 分配长度（含一个预留扇区）
    assert_eq!(
      pfadd_one(sess, batch, key, &push_elem(&mut elems, "inplace")),
      1
    );
    let mut blob = load_blob(batch, key);
    assert_eq!(blob.len(), initial, "新键须按 SparseInitialLength 落盘");
    assert!(hll.is_sparse(&blob) && hll.is_valid_hyll(&blob));
    assert!(
      hll.can_grow_in_place(&blob, blob.len(), 1),
      "测试前提：初始分配须含原位增长余量"
    );

    // ① 逐元素推进到首次扩容：新长度 = 扩容前实际占用 + 一个预留扇区
    loop {
      let current = hll.sparse_current_size_in_bytes(&blob);
      let nz = hll.sparse_count_non_zero(&blob);
      let updated = pfadd_one(sess, batch, key, &push_elem(&mut elems, "inplace"));
      let after = load_blob(batch, key);
      assert!(hll.is_valid_hyll(&after), "写回载荷须始终合法");
      assert_add_landed(&hll, &blob, &after, nz, updated);
      if after.len() > initial {
        assert_eq!(
          after.len(),
          current + SPARSE_MEMORY_SECTOR_SIZE,
          "首次扩容须按 UpdateGrow 口径恰推进一个预留扇区"
        );
        blob = after;
        break;
      }
      assert_eq!(after.len(), initial, "预留扇区内原位改写：记录长度恒定");
      blob = after;
    }

    // ② 预留扇区内连续 50 发单元素 PFADD：长度恒不变（逐发原位命中）
    let grown = blob.len();
    assert!(
      hll.can_grow_in_place(&blob, grown, 50),
      "测试前提：{grown}B 记录须容得下 50 发单元素原位增长"
    );
    for _ in 0..50 {
      let nz = hll.sparse_count_non_zero(&blob);
      let updated = pfadd_one(sess, batch, key, &push_elem(&mut elems, "inplace"));
      let after = load_blob(batch, key);
      assert_eq!(after.len(), grown, "原位窗口内不得二次扩容（重分配写回）");
      assert!(hll.is_sparse(&after) && hll.is_valid_hyll(&after));
      assert_add_landed(&hll, &blob, &after, nz, updated);
      blob = after;
    }

    // 原位链路上的基数不丢：与 C# 同口径的估算误差上限
    let mut out = Vec::new();
    sess.hyper_log_log_length(&[key], batch, &mut out).unwrap();
    assert!(
      estimation_error(parse_resp_int(&out), elems.len()) < 4.0,
      "稀疏原位链路基数估算异常: {out:?} vs {} 元素",
      elems.len()
    );

    // ③ 重复元素：无寄存器变更 → :0 且载荷逐字节不动（不写回、不虚胖）
    let before_dup = load_blob(batch, key);
    assert_eq!(pfadd_one(sess, batch, key, &elems[0]), 0);
    assert_eq!(load_blob(batch, key), before_dup, "无变更不得改写存储载荷");

    // ④ 继续插入直至按 UpdateGrow 口径晋升稠密，稠密恒为 dense_bytes
    while !hll.is_dense(&blob) {
      assert!(elems.len() < 8000, "稀疏→稠密晋升未发生：载荷口径异常");
      let updated = pfadd_one(sess, batch, key, &push_elem(&mut elems, "inplace"));
      assert!(updated == 0 || updated == 1, "PFADD 应答须为 :0/:1");
      blob = load_blob(batch, key);
      assert!(hll.is_valid_hyll(&blob));
    }
    assert_eq!(blob.len(), hll.dense_bytes(), "稠密载荷须为完整分配长度");

    out.clear();
    sess.hyper_log_log_length(&[key], batch, &mut out).unwrap();
    assert!(
      estimation_error(parse_resp_int(&out), elems.len()) < 4.0,
      "C# EstimationError 同口径（< 4%）: {out:?} vs {} 元素",
      elems.len()
    );
  });
}

/// 冷区慢路径（`slow_hll_add`）写回同款防回归
///
/// C# 磁盘候选经 CompletePending 重放后走的仍是同一 GetRMWModifiedFieldInfo
/// 长度口径（VarLenInputMethods.cs:GetRMWModifiedFieldInfo 的 PFADD 臂），慢路径
/// 写回亦不得切除尾部预留扇区：冷区一发 PFADD 后记录长度仍等于冷化前的分配
/// 长度，回暖后的 PFADD 才能继续命中 can_grow_in_place 原位臂
#[test]
fn cold_pfadd_slowpath_keeps_sector_slack_for_in_place_update() {
  let rt = Runtime::new().unwrap();
  let dir = tempdir().unwrap();
  let device =
    Arc::new(SegmentedDevice::single_file(dir.path().join("hll-cold-inplace.db")).unwrap());
  let store = Arc::new(WedbStore::open(test_store_config(), device).unwrap());
  let key: &[u8] = b"hll-cold-inplace";
  let hll = HyperLogLog::new();

  let mut elems: Vec<Vec<u8>> = Vec::new();
  // 推到首次扩容，钉住冷化前的分配长度
  let grown = {
    let session = store.new_session().unwrap();
    let batch = session.enter_batch();
    let mut sess = RespServerSession::default();
    loop {
      let _ = pfadd_one(&mut sess, &batch, key, &push_elem(&mut elems, "cold"));
      let len = load_blob(&batch, key).len();
      if len > hll.sparse_bytes() {
        break len;
      }
    }
  };

  rt.block_on(store.flush_and_evict_all()).unwrap();
  let api: GarnetApi = Arc::new(StoreGarnetApi::new(store.new_session().unwrap()));
  let mut s = RespServerSession::new(1, RespServerSessionOptions::default());
  s.set_garnet_api(Arc::clone(&api));

  // 冷区单元素：快路径降级无应答，慢路径装载磁盘冷区后并入写回
  let cold_elem = push_elem(&mut elems, "cold");
  let cold_args: Vec<Vec<u8>> = Some(key.to_vec())
    .into_iter()
    .chain(Some(cold_elem.clone()))
    .collect();
  api.exec(&mut s, RespCommand::Pfadd, &[key, &cold_elem]);
  assert!(
    s.output.is_empty(),
    "冷数据 PFADD 快路径须降级慢路径，不残留应答"
  );
  let out = rt.block_on(Arc::clone(&api).exec_slow(
    RespCommand::Pfadd,
    cold_args,
    wconf::DEFAULT_RESP_VERSION,
  ));
  assert!(out == b":0\r\n" || out == b":1\r\n", "PFADD 应答须为 :0/:1");

  // 慢路径写回保留预留扇区：长度仍等于冷化前的分配长度，且余量可判原位增长
  let probe = store.new_session().unwrap();
  let batch = probe.enter_batch();
  let blob = load_blob(&batch, key);
  assert_eq!(blob.len(), grown, "慢路径写回不得切除尾部预留扇区");
  assert!(hll.can_grow_in_place(&blob, blob.len(), 1));

  // 回暖后继续原位改写：长度恒定，无二次扩容
  let _ = pfadd_one(&mut s, &batch, key, &push_elem(&mut elems, "cold"));
  assert_eq!(
    load_blob(&batch, key).len(),
    grown,
    "冷区写回后原位臂须继续命中，不得逐发扩容"
  );
}

/// 压力翻转执行域：小环形日志（page_size 16KB × 4 页）持续写入必触发
/// PageNotReady 翻转（whlog 回绕复用槽位遇未驱逐旧页，append.rs:40），即
/// 「环形页水位逼近」生产形态的确定性微缩；页容仍容下 12304B 稠密整载荷。
struct DegradeEnv {
  rt: Runtime,
  api: GarnetApi,
  _store: Arc<WedbStore<SegmentedDevice>>,
  map: Arc<WatchVersionMap>,
  _dir: tempfile::TempDir,
}

fn degrade_env(tag: &str) -> DegradeEnv {
  let dir = tempdir().unwrap();
  let config = StoreConfig::new(1024, 16 * 1024, 4, 0.5).unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag)).unwrap());
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  let map = Arc::new(WatchVersionMap::new(1 << 10));
  store.set_watch_hook(version_map_watch_hook(Arc::clone(&map)));
  let api: GarnetApi = Arc::new(StoreGarnetApi::new(store.new_session().unwrap()));
  DegradeEnv {
    rt: Runtime::new().unwrap(),
    api,
    _store: store,
    map,
    _dir: dir,
  }
}

/// 发现一（zcode-r16-sethll）写回降级分流回归：PFADD 快路径 store_hll 遇
/// try_rmw_sync 环形页翻转（Ok(Err(page_id))）必须零应答整体转慢路径闭环，
/// 绝不容「:1 应答已出而载荷未落库、WATCH 版本未推进」（C# 同位失败经
/// IPUResult.Failed 上抛，RMWMethods.cs:665，无吞信号路径；对照组 set_pop
/// 同信号 Ok(false) 整体转慢路径重放）。修复前 store_hll 吞降级信号照常
/// 应答 :1，该键整包载荷未落库（元素基数贡献蒸发、AOF 零传播）。
///
/// 构造（全真 wkv 原语，非 mock）：小环形日志（16KB × 4 页）上跑 300 键
/// 新建流——HLL 载荷长度稳定后写回走原位更新零分配，唯一持续 append 供给
/// 是新键初始写回，多键轮转使日志总 append 越过环形容量，回绕复用槽位必遇
/// PageNotReady（append.rs:40，无后台刷盘下旧页未驱逐）。页驱逐后的装载面
/// 磁盘候选降级同走本闭环，一并锁定：两类降级的正确出口都是零应答 + 慢路径
/// 重放，终态基数零蒸发
#[test]
fn pfadd_writeback_degrade_never_answers_success() {
  let DegradeEnv {
    rt,
    api,
    _store,
    map,
    _dir,
  } = degrade_env("hll-pfadd-degrade.db");
  let mut s = RespServerSession::new(1, RespServerSessionOptions::default());
  s.set_garnet_api(Arc::clone(&api));

  let v0 = map.read_version(h(b"k0"));
  let total = 300;
  let per_key: Vec<Vec<u8>> = (0..50).map(|i| format!("e{i}").into_bytes()).collect();
  let mut degraded = 0usize;
  for i in 0..total {
    let key = format!("k{i}");
    let mut args: Vec<&[u8]> = vec![key.as_bytes()];
    args.extend(per_key.iter().map(|v| v.as_slice()));

    s.output.clear();
    api.exec(&mut s, RespCommand::Pfadd, &args);
    if s.output.is_empty() {
      // 降级（写回翻转 / 装载磁盘候选）：快路径零应答，慢路径闭环
      degraded += 1;
      let slow_args: Vec<Vec<u8>> = Some(key.as_bytes().to_vec())
        .into_iter()
        .chain(per_key.iter().cloned())
        .collect();
      let out = rt.block_on(Arc::clone(&api).exec_slow(
        RespCommand::Pfadd,
        slow_args,
        wconf::DEFAULT_RESP_VERSION,
      ));
      assert!(
        out == b":0\r\n" || out == b":1\r\n",
        "降级闭环慢路径应答须为 :0/:1: {out:?}"
      );
    }
  }
  assert!(
    degraded > 0,
    "测试前提：环形日志 append 总量越过 4 页容量应至少触发一次降级"
  );

  // 降级闭环后元素零蒸发：逐键稀疏精确计数 == 元素数（修复前翻转笔 :1
  // 吞掉整包写回，该键基数缺口 50）
  for i in 0..total {
    let out = rt.block_on(Arc::clone(&api).exec_slow(
      RespCommand::Pfcount,
      vec![format!("k{i}").into_bytes()],
      wconf::DEFAULT_RESP_VERSION,
    ));
    assert_eq!(parse_resp_int(&out), 50, "k{i} 降级闭环后基数须零蒸发");
  }

  // 真写回（快路径命中笔与降级闭环笔）须推进 WATCH 版本（修复前降级笔
  // 被吞，版本不推进、并发 WATCH 误通过）
  assert!(map.read_version(h(b"k0")) > v0, "写回闭环须推进 WATCH 版本");
}

/// 发现一（zcode-r16-sethll）PFMERGE 收尾臂同款分流回归：写回（按源并入
/// 事实门控后的 store_hll）遇环形页翻转必须零应答转慢路径，绝不容「+OK 已
/// 出而 dest 未更新」（源与 dest 并集承诺失实，主从基数发散）。每轮写全新
/// dest——新 dest 的 merged 写回（稀疏种子并入源载荷后整包尾部追加 ~4KB）
/// 是持续 append 供给，单 dest 重复合并恒原位更新永不翻转
#[test]
fn pfmerge_writeback_degrade_never_answers_ok() {
  let DegradeEnv {
    rt,
    api,
    _store,
    map: _map,
    _dir,
  } = degrade_env("hll-pfmerge-degrade.db");
  let mut s = RespServerSession::new(1, RespServerSessionOptions::default());
  s.set_garnet_api(Arc::clone(&api));

  // 源键：50 元素稀疏 HLL（热区，逐轮装载命中）
  let elems: Vec<Vec<u8>> = (0..50).map(|i| format!("f{i}").into_bytes()).collect();
  {
    let mut args: Vec<&[u8]> = vec![b"ms".as_slice()];
    args.extend(elems.iter().map(|v| v.as_slice()));
    api.exec(&mut s, RespCommand::Pfadd, &args);
    assert_eq!(s.output, b":1\r\n");
    s.output.clear();
  }

  let total = 300;
  let mut degraded = 0usize;
  for i in 0..total {
    let dest = format!("md{i}");
    s.output.clear();
    api.exec(&mut s, RespCommand::Pfmerge, &[dest.as_bytes(), b"ms"]);
    if s.output.is_empty() {
      // dest 写回翻转：+OK 绝不出，慢路径闭环补并
      degraded += 1;
      let out = rt.block_on(Arc::clone(&api).exec_slow(
        RespCommand::Pfmerge,
        vec![dest.as_bytes().to_vec(), b"ms".to_vec()],
        wconf::DEFAULT_RESP_VERSION,
      ));
      assert_eq!(out, b"+OK\r\n", "降级闭环慢路径须回 +OK: {out:?}");
    } else {
      assert_eq!(s.output, b"+OK\r\n");
    }
  }
  assert!(
    degraded > 0,
    "测试前提：环形日志 append 总量越过 4 页容量应至少触发一次 dest 写回降级"
  );

  // 终态：每个 dest 基数守恒 == 50（修复前翻转笔 +OK 掩盖丢写，该 dest
  // 键缺失 / 载荷未落库，并集承诺失实）
  for i in 0..total {
    let out = rt.block_on(Arc::clone(&api).exec_slow(
      RespCommand::Pfcount,
      vec![format!("md{i}").into_bytes()],
      wconf::DEFAULT_RESP_VERSION,
    ));
    assert_eq!(parse_resp_int(&out), 50, "md{i} 降级闭环后并集承诺须守恒");
  }
}

/// 发现二（zcode-r16-sethll）语义锁：PFCOUNT 多键尾随键缺失时按空集并入回
/// 真实并集基数（Redis 语义），不对齐 C# HyperLogLogLength 仅在尾键命中时
/// 赋值 count、尾键 NOTFOUND 直接 continue 恒回 0 的上游缺陷
///（HyperLogLogOps.cs:118-174，:135-137）。见 doc/zh/deviations.md 第 16 条，
/// 严禁回改归 0（归 0 才是真回归）；若上游修复 C# 该臂，此条按登记撤销
#[test]
fn pfcount_trailing_missing_key_returns_real_union_cardinality() {
  with_batch(|sess, batch| {
    let a: &[u8] = b"pfcm-a";
    let b: &[u8] = b"pfcm-b";
    let c: &[u8] = b"pfcm-c";

    for e in [b"e1".as_slice(), b"e2", b"e3", b"e4"] {
      let mut out = Vec::new();
      sess.hyper_log_log_add(&[a, e], batch, &mut out).unwrap();
      assert_eq!(parse_resp_int(&out), 1);
    }

    // 尾键缺失：C# 回 :0（缺陷），Rust 回真实并集基数 4
    let mut out = Vec::new();
    sess.hyper_log_log_length(&[a, b], batch, &mut out).unwrap();
    assert_eq!(
      parse_resp_int(&out),
      4,
      "尾键缺失须回真实并集基数（deviations 第 16 条）"
    );

    // 多个尾随键全缺失：同口径
    let mut out = Vec::new();
    sess
      .hyper_log_log_length(&[a, b, c], batch, &mut out)
      .unwrap();
    assert_eq!(parse_resp_int(&out), 4);

    // 尾键存在（逆序）：并集计数不受键序影响
    let mut out = Vec::new();
    sess.hyper_log_log_length(&[b, a], batch, &mut out).unwrap();
    assert_eq!(parse_resp_int(&out), 4);

    // 全键缺失：:0（全部按空集并入的退化形态）
    let mut out = Vec::new();
    sess.hyper_log_log_length(&[b, c], batch, &mut out).unwrap();
    assert_eq!(parse_resp_int(&out), 0);
  });
}

/// 发现四（zcode-r16-sethll）语义锁：PFADD/PFMERGE 对带 TTL 键恒保留 key 级
/// TTL（Redis 语义，与 C# CopyUpdater 臂 TryCopyOptionals 同口径），到期真删；
/// 不对齐 C# InPlaceUpdater PFADD/PFMERGE 臂的 RemoveExpiration
///（RMWMethods.cs:666/:677/:707/:718，与 Copy 臂自相矛盾且随记录可变性漂移）。
/// 见 doc/zh/deviations.md 第 17 条与 try_rmw_sync 头注裁决声明，严禁按
/// C# InPlace 臂改回清 TTL
#[test]
fn pfadd_pfmerge_preserve_key_ttl_until_expiry() {
  let rt = Runtime::new().unwrap();
  let dir = tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("hll-ttl-keep.db")).unwrap());
  let store = Arc::new(WedbStore::open(test_store_config(), device).unwrap());
  let api: GarnetApi = Arc::new(StoreGarnetApi::new(store.new_session().unwrap()));
  let probe = store.new_session().unwrap();
  let mut s = RespServerSession::new(1, RespServerSessionOptions::default());
  s.set_garnet_api(Arc::clone(&api));

  // 1. PFADD 建键 + 键级 TTL 60s
  api.exec(&mut s, RespCommand::Pfadd, &[b"k", b"e1", b"e2", b"e3"]);
  assert_eq!(s.output, b":1\r\n");
  s.output.clear();
  api.exec(&mut s, RespCommand::Expire, &[b"k", b"60"]);
  assert_eq!(s.output, b":1\r\n");
  s.output.clear();

  // 2. 带 TTL 键快路径 PFADD：TtlGate::Pass 不触碰 TTL 记录恒保留
  //（修复场景：若误走 SET 语义清退，此处 pttl = -1）
  api.exec(&mut s, RespCommand::Pfadd, &[b"k", b"e4"]);
  assert_eq!(s.output, b":1\r\n");
  s.output.clear();
  let ttl = rt.block_on(probe.pttl_ms(b"k")).unwrap();
  assert!(
    ttl > 0 && ttl <= 60_000,
    "PFADD 必须保留键级 TTL（deviations 第 17 条）：{ttl}"
  );

  // 3. dest 带 TTL 的 PFMERGE：快路径写回保留 dest 键级 TTL
  api.exec(&mut s, RespCommand::Pfadd, &[b"g", b"x1", b"x2"]);
  s.output.clear();
  api.exec(&mut s, RespCommand::Pfmerge, &[b"g", b"k"]);
  assert_eq!(s.output, b"+OK\r\n");
  s.output.clear();
  api.exec(&mut s, RespCommand::Expire, &[b"g", b"60"]);
  assert_eq!(s.output, b":1\r\n");
  s.output.clear();
  api.exec(&mut s, RespCommand::Pfmerge, &[b"g", b"k"]);
  assert_eq!(s.output, b"+OK\r\n");
  s.output.clear();
  let ttl = rt.block_on(probe.pttl_ms(b"g")).unwrap();
  assert!(
    ttl > 0 && ttl <= 60_000,
    "PFMERGE 必须保留 dest 键级 TTL（deviations 第 17 条）：{ttl}"
  );

  // 4. 越期真删（TTL 保留的生效面）：直写过期时刻（确定性构造，
  // hll_slow_ttl 同款 put_ttl_sync 手法）后 EXISTS 回 :0
  {
    let batch = probe.enter_batch();
    put_ttl_sync(&batch, b"k", now_ticks() - 1).unwrap();
    put_ttl_sync(&batch, b"g", now_ticks() - 1).unwrap();
  }
  api.exec(&mut s, RespCommand::Exists, &[b"k"]);
  assert_eq!(s.output, b":0\r\n", "PFADD 后 TTL 保留须到期真删");
  s.output.clear();
  api.exec(&mut s, RespCommand::Exists, &[b"g"]);
  assert_eq!(s.output, b":0\r\n", "PFMERGE 后 TTL 保留须到期真删");
}

/// 布点满占用等号合法稀疏载荷（task zcode-r151c-pfconv 案一/案二构造原语）：
/// 4096B 全容量缓冲上逐枚隔位（idx=4i+1、cntlz=1）布点非零寄存器——每枚
/// 恒 Δ=2 最坏流（与 whyperlog/tests/sparse_peak_margin.rs 同源算形），随后
/// 截断至实际占用，成 rle=payload 等号满容量形：帧校验含等号即收（对标 C#
/// IsValidHLLLength 不查写裕度），然稀疏写容纳谓词 sparse_fits 于该形裕度
/// 恰缺 1B，原位并入/合并必被拒。经 try_upsert_sync 直写存储（:1045 坏载荷
/// 构造先例，真实存储接口，非 mock）
fn craft_sparse_full(hll: &HyperLogLog, from: usize, count: usize) -> Vec<u8> {
  let mut blob = vec![0_u8; SPARSE_SIZE_MAX_CAP];
  hll.init_sparse(&mut blob);
  for i in 0..count {
    let idx = (4 * (from + i) + 1) as u16;
    assert!(
      hll.update_sparse_reg(&mut blob, idx, 1),
      "布点 {idx} 须发生变更"
    );
  }
  let current = hll.sparse_current_size_in_bytes(&blob);
  assert_eq!(current, 146 + 2 * count, "隔位布点占形：零段基座 + 2B/枚");
  blob.truncate(current);
  assert!(
    hll.is_sparse(&blob) && hll.is_valid_hyll(&blob),
    "等号满容量形须过帧校验"
  );
  blob
}

/// murmur 选元：为槽位 4(from+i)+1（i < count，彼此隔 ≥3 零格）各选一枚
/// clz==1（单字节非零码，逐枚恒 Δ=2 最坏流）的真实元素，按寄存器下标升序
/// 返回。穷举真实哈希（murmur_hash_2_x64_a + reg_idx/clz 公开口径），PFADD
/// 全链走真实命令，绝非 mock
fn select_isolated_elems(hll: &HyperLogLog, from: usize, count: usize) -> Vec<Vec<u8>> {
  let mut slots: Vec<Option<Vec<u8>>> = vec![None; count];
  let mut left = count;
  for n in 0_u32.. {
    assert!(
      n < 8_000_000,
      "murmur 选元扫描未收敛（{from}.. 窗 {left} 槽未集齐）"
    );
    let name = format!("pfconv-iso-{from}-{n}").into_bytes();
    let hv = murmur_hash_2_x64_a(&name);
    if hll.clz(hv) != 1 {
      continue;
    }
    let idx = hll.reg_idx(hv) as usize;
    if idx % 4 != 1 {
      continue;
    }
    if let Some(k) = (idx / 4).checked_sub(from)
      && k < count
      && slots[k].is_none()
    {
      slots[k] = Some(name);
      left -= 1;
      if left == 0 {
        break;
      }
    }
  }
  slots.into_iter().map(Option::unwrap).collect()
}

/// 案一 a 命令级形锁：PFADD 100 元素新建键折叠单发——C#
/// SparseInitialLength(100)=18+roundup128(200)=274 系折叠出形，缺 128B 零段
/// 基座与 1B 尾移写峰裕度（init 盲插即越界 panic，同源
/// HyperLogLog.cs:SparseInitialLength/Init 裸指针面）；rust 以唯一谓词
/// sparse_fits 自零段基座按扇区上探至最小容纳形 402（146+200 < 402）。
/// 钉：应答 :1、STRLEN 恒等于声明分配形 402、is_valid_hyll 逐字节合法、
/// PFCOUNT 与载荷直读基数同源
#[test]
fn pfadd_new_key_hundred_elements_fold_alloc_covers_peak() {
  with_batch(|sess, batch| {
    let hll = HyperLogLog::new();
    let key: &[u8] = b"hll-fold-init";
    let elems: Vec<Vec<u8>> = (0..100)
      .map(|i| format!("fold-init-{i}").into_bytes())
      .collect();
    let mut args: Vec<&[u8]> = vec![key];
    args.extend(elems.iter().map(|v| v.as_slice()));

    let mut out = Vec::new();
    sess.hyper_log_log_add(&args, batch, &mut out).unwrap();
    assert_eq!(parse_resp_int(&out), 1);

    let blob = load_blob(batch, key);
    assert_eq!(
      blob.len(),
      402,
      "折叠分配须自零段基座上探至最小容纳形（274 形缺基座裕度恒拒）"
    );
    assert!(hll.is_sparse(&blob) && hll.is_valid_hyll(&blob));

    out.clear();
    sess.hyper_log_log_length(&[key], batch, &mut out).unwrap();
    let card = parse_resp_int(&out);
    let mut probe = blob.clone();
    assert_eq!(
      card,
      hll.count(&mut probe),
      "PFCOUNT 须与存储载荷直读基数同源"
    );
    assert!(
      (88..=112).contains(&card),
      "100 元素基数估算越常规容差带: {card}"
    );
  });
}

/// 案一 a 慢臂同款：缺失键 exec_slow（slow_hll_add 冷区闭环 →
/// hll_init_payload）建键 100 元素，分配形须与快臂全等 402、应答 :1——
/// 快慢两路共用单判据，写回口径零漂移
#[test]
fn pfadd_new_key_hundred_elements_slow_arm_same_alloc_shape() {
  let rt = Runtime::new().unwrap();
  let dir = tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("hll-fold-slow.db")).unwrap());
  let store = Arc::new(WedbStore::open(test_store_config(), device).unwrap());
  let api: GarnetApi = Arc::new(StoreGarnetApi::new(store.new_session().unwrap()));

  let key: &[u8] = b"hll-fold-slow-key";
  let elems: Vec<Vec<u8>> = (0..100)
    .map(|i| format!("fold-slow-{i}").into_bytes())
    .collect();
  let slow_args: Vec<Vec<u8>> = Some(key.to_vec())
    .into_iter()
    .chain(elems.iter().map(|v| v.to_vec()))
    .collect();
  let out = rt.block_on(Arc::clone(&api).exec_slow(
    RespCommand::Pfadd,
    slow_args,
    wconf::DEFAULT_RESP_VERSION,
  ));
  assert_eq!(out, b":1\r\n");

  let hll = HyperLogLog::new();
  let probe = store.new_session().unwrap();
  let batch = probe.enter_batch();
  let blob = load_blob(&batch, key);
  assert_eq!(
    blob.len(),
    402,
    "慢臂建键分配形须与快臂全等（hll_init_payload 共用）"
  );
  assert!(hll.is_sparse(&blob) && hll.is_valid_hyll(&blob));
}

/// 案一 b 真实界面回归（murmur 选元构造隔位 64 批）：两批各 64 枚隔位
/// 最坏流元素全程真实 PFADD 命令。首批建键：折叠形 274 对 146+2×64 恰等界
/// 缺 1B 裕度（146+128 < 274 假）被谓词拒、上探 402；第二批 update_grow
/// (64)=current+roundup128(128) 的等界出形同缺裕度（N%64==0 之形
/// roundup128(2N)==2N），复检即按既有稠密化口径升稠密 12304（修复前原位
/// 裸插/拷贝裸插皆击穿 panic）；PFCOUNT 同源且 C# EstimationError < 4%
/// 同口径
#[test]
fn pfadd_murmur_isolated_64_batch_equal_bound_grow_promotes_dense() {
  with_batch(|sess, batch| {
    let hll = HyperLogLog::new();
    let key: &[u8] = b"hll-fold-64";

    // 首批 64 枚隔位布点：建键分配上探至 402，实占恰 274
    let first = select_isolated_elems(&hll, 0, 64);
    let mut args: Vec<&[u8]> = vec![key];
    args.extend(first.iter().map(|v| v.as_slice()));
    let mut out = Vec::new();
    sess.hyper_log_log_add(&args, batch, &mut out).unwrap();
    assert_eq!(parse_resp_int(&out), 1);
    let blob = load_blob(batch, key);
    assert_eq!(
      blob.len(),
      402,
      "建键折叠形 274 恰等界（146+128 < 274 假）必拒，按扇区上探"
    );
    assert_eq!(
      hll.sparse_current_size_in_bytes(&blob),
      274,
      "隔位布点恒 Δ=2：实占 = 基座 146 + 2×64"
    );
    assert!(hll.is_sparse(&blob) && hll.is_valid_hyll(&blob));

    // 钉死前提：第二批 64 枚原位臂裕度恰缺 1B（274+128 ≥ 402 即拒）
    assert!(
      !hll.can_grow_in_place(&blob, blob.len(), 64),
      "测试前提：等界 64 批原位臂必拒"
    );

    // 第二批（后 64 槽隔位）：update_grow 出形 402 同缺裕度 → 升稠密
    let second = select_isolated_elems(&hll, 64, 64);
    let mut args2: Vec<&[u8]> = vec![key];
    args2.extend(second.iter().map(|v| v.as_slice()));
    out.clear();
    sess.hyper_log_log_add(&args2, batch, &mut out).unwrap();
    assert_eq!(parse_resp_int(&out), 1);
    let blob2 = load_blob(batch, key);
    assert_eq!(
      blob2.len(),
      hll.dense_bytes(),
      "等界扩容形经唯一谓词复检须按既有口径升稠密（修复前此形裸插击穿）"
    );
    assert!(hll.is_dense(&blob2) && hll.is_valid_hyll(&blob2));

    out.clear();
    sess.hyper_log_log_length(&[key], batch, &mut out).unwrap();
    let card = parse_resp_int(&out);
    let mut probe = blob2.clone();
    assert_eq!(card, hll.count(&mut probe), "PFCOUNT 须同源");
    assert!(
      estimation_error(card, 128) < 4.0,
      "128 枚隔位单值寄存器小范围校正估算异常: {card}"
    );
  });
}

/// 案一 c 命令级形锁：crafted rle=payload 等号合法满容量稀疏源（64 枚
/// 隔位非零、274B 实占即分配，帧校验含等号即收——对标 C#
/// IsValidHLLLength/TryMerge 不查写裕度的等号接受形），PFMERGE 入缺失
/// dest（274B 稀疏种、current 146）：merge_grow 出形恰等原位长 274，
/// 146+2×64=274 缺 1B 峰裕度——try_merge 原位臂与 hll_merge_payload 升
/// 稠密侧同源单谓词，必拒并按既有口径升稠密 12304（修复前该形原位并入
/// 即击穿缓冲界）；PFCOUNT 与终形载荷直读同源
#[test]
fn pfmerge_equality_full_sparse_source_promotes_dense_not_overflow() {
  with_batch(|sess, batch| {
    let hll = HyperLogLog::new();
    let src = craft_sparse_full(&hll, 0, 64);
    assert_eq!(src.len(), 274);
    assert_eq!(hll.sparse_count_non_zero(&src), 64);
    let src_key: &[u8] = b"hll-craft-src";
    let dest: &[u8] = b"hll-craft-dest";
    let _ = batch.try_upsert_sync(src_key, &src).unwrap();

    let mut out = Vec::new();
    sess
      .hyper_log_log_merge(&[dest, src_key], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"+OK\r\n");

    let blob = load_blob(batch, dest);
    assert_eq!(
      blob.len(),
      hll.dense_bytes(),
      "等界合并形须按既有口径升稠密，杜绝原位击穿"
    );
    assert!(hll.is_dense(&blob) && hll.is_valid_hyll(&blob));

    out.clear();
    sess.hyper_log_log_length(&[dest], batch, &mut out).unwrap();
    let card = parse_resp_int(&out);
    let mut probe = blob.clone();
    assert_eq!(card, hll.count(&mut probe), "PFCOUNT 须同源");
    assert!(
      (60..=68).contains(&card),
      "64 枚单值寄存器小范围校正估算异常: {card}"
    );
  });
}

/// 案二 §135 分配形锁（登记口径·折叠形为唯一法定形·严禁回改逐元素交错形）：
/// crafted 满占用等号稀疏键（current=分配=3800、1827 枚隔位非零），批量
/// PFADD 127 枚新元素——rust 折叠单发：update_grow(127) = 3800 +
/// roundup128(254) = 4056，谓词 3800+254 < 4056 成立（裕度 2B）→ 稀疏迁移
/// 4056B 落盘；C# 逐元素原位/拷贝两臂交错于同形须先扩 3928、再扩 4056、
/// 第三次上探 4056+128 > 4096 越 cap 即 CompleteUpdate 拷成稠密——终态
/// 12304B 稠密。同命令同元素流两侧 STRLEN/ dtype 终形互异（deviations
/// §135 登记面，基数同值、危害面仅 STRLEN/DUMP 字节流治理观测），本用例
/// 钉死折叠终形 4056 为本仓唯一法定形
#[test]
fn pfadd_127_batch_on_full_sparse_keeps_folded_sparse_shape() {
  with_batch(|sess, batch| {
    let hll = HyperLogLog::new();
    let key: &[u8] = b"hll-fold-shape";
    let seed = craft_sparse_full(&hll, 0, 1827);
    assert_eq!(seed.len(), 3800, "等号满占用形：146 + 2×1827 = 3800");
    let _ = batch.try_upsert_sync(key, &seed).unwrap();

    let elems: Vec<Vec<u8>> = (0..127)
      .map(|i| format!("fold-shape-{i}").into_bytes())
      .collect();
    // 钉住元素全落单字节非零码（val ≤ 32，Δ ≤ 2 已证算形）；双字节操作码
    // 属另一未证面，勿入本形锁
    assert!(
      elems.iter().all(|e| hll.clz(murmur_hash_2_x64_a(e)) <= 32),
      "形锁前提：127 枚元素寄存器值须 ≤ 32（单字节操作码）"
    );
    let mut args: Vec<&[u8]> = vec![key];
    args.extend(elems.iter().map(|v| v.as_slice()));

    let mut out = Vec::new();
    sess.hyper_log_log_add(&args, batch, &mut out).unwrap();
    assert_eq!(parse_resp_int(&out), 1);

    let blob = load_blob(batch, key);
    assert_eq!(
      blob.len(),
      4056,
      "折叠单发法定形：update_grow(127) = 3800 + roundup128(254) = 4056（§135）"
    );
    assert!(
      hll.is_sparse(&blob) && hll.is_valid_hyll(&blob),
      "折叠形终态锁：C# 同形臂轨迹终态为稠密 12304——互异系 §135 登记法定形，禁回改"
    );

    out.clear();
    sess.hyper_log_log_length(&[key], batch, &mut out).unwrap();
    let card = parse_resp_int(&out);
    let mut probe = blob.clone();
    assert_eq!(
      card,
      hll.count(&mut probe),
      "两臂形互异而基数同值：PFCOUNT 须与终形载荷直读同源"
    );
  });
}

// ==== deviations §144/§145 登记面锁（票 zcode-r147c-pfmerge，登记级零行为
// 改动）：格一至三 dest 前置拒三形与 PFMERGE dest 分配长度 MergeGrow 单臂形。
// C# 对照形态（坏形 dest + 源命中恒 +OK 掩盖、热臂保留 274/冷臂 275 漂移）
// 以注释锚记入各用例，仿 §113 resp_set.rs 夹具族对照形态注释形制。

/// §144 格一形锁：dest 为非 HYLL string（SET d "xx"）+ ≥1 合法源命中——
/// rust 快/慢双臂恒 dest 前置拒 `-WRONGTYPE_HLL` 独占帧、d 逐字节 "xx" 原样、
/// 源零并入痕迹、双臂应答逐字节全等。C# 对照：HyperLogLogOps.cs:262 弃
/// SET_Conditional 返回值 + RESP 层唯判源侧 status/error（HyperLogLogCommands.cs
/// :99-126）→ 该形恒 +OK（热形 dest 原样 / 磁盘驻 Copy 臂 :1303-1350 无
/// IsValidHYLL 门垃圾盲并污写）——修复型分叉只登记不回改（§144）
#[test]
fn pfmerge_corrupt_dest_string_wrongtype_fast_slow_arm_parity() {
  // 快臂环境：全热区，dest 为 String "xx"（load_hll 借用切片校验即 Err）
  let fast_dir = tempdir().unwrap();
  let fast_device = Arc::new(
    SegmentedDevice::single_file(fast_dir.path().join("hll-pfmerge-destguard-fast.db")).unwrap(),
  );
  let fast_store = Arc::new(WedbStore::open(test_store_config(), fast_device).unwrap());
  let fast_rt = Runtime::new().unwrap();
  let fast_api: GarnetApi = Arc::new(StoreGarnetApi::new(fast_store.new_session().unwrap()));
  let mut sf = RespServerSession::new(1, RespServerSessionOptions::default());
  sf.set_garnet_api(Arc::clone(&fast_api));
  sf.output.clear();
  fast_api.exec(&mut sf, RespCommand::Pfadd, &[b"dg-s", b"e1"]);
  assert_eq!(sf.output, b":1\r\n");
  {
    let probe = fast_store.new_session().unwrap();
    let batch = probe.enter_batch();
    let _ = batch.try_upsert_sync(b"dg-d", b"xx");
  }
  sf.output.clear();
  let src_before = raw_blob(&fast_rt, &fast_store, b"dg-s");
  fast_api.exec(&mut sf, RespCommand::Pfmerge, &[b"dg-d", b"dg-s"]);
  assert_eq!(
    sf.output,
    hll_wrongtype_frame(),
    "格一快臂：坏形 dest 前置拒须为独占 WRONGTYPE_HLL 帧"
  );
  assert_eq!(
    raw_blob(&fast_rt, &fast_store, b"dg-d"),
    b"xx",
    "dest 前置拒零触碰：逐字节原样不污写"
  );
  assert_eq!(
    raw_blob(&fast_rt, &fast_store, b"dg-s"),
    src_before,
    "dest 拒裁即止：任一源不读、零并入痕迹"
  );
  let fast_frame = sf.output.clone();

  // 慢臂环境：同输入，dest 冷化（load_hll_cold WrongType 拒帧与快臂同串）
  let (rt, slow_api, slow_store, _slow_dir) = slow_fault_env("hll-pfmerge-destguard-slow.db");
  let mut ss = RespServerSession::new(1, RespServerSessionOptions::default());
  ss.set_garnet_api(Arc::clone(&slow_api));
  ss.output.clear();
  slow_api.exec(&mut ss, RespCommand::Pfadd, &[b"dg-s", b"e1"]);
  {
    let probe = slow_store.new_session().unwrap();
    let batch = probe.enter_batch();
    let _ = batch.try_upsert_sync(b"dg-d", b"xx");
  }
  rt.block_on(slow_store.flush_and_evict_all()).unwrap();
  let slow_out = rt.block_on(Arc::clone(&slow_api).exec_slow(
    RespCommand::Pfmerge,
    vec![b"dg-d".to_vec(), b"dg-s".to_vec()],
    wconf::DEFAULT_RESP_VERSION,
  ));
  assert_eq!(
    slow_out, fast_frame,
    "格一慢臂拒帧与快臂逐字节全等（load_hll_cold 拒裁同串，多路径同构）"
  );
  assert_eq!(
    raw_blob(&rt, &slow_store, b"dg-d"),
    b"xx",
    "慢臂 dest 冷拒同样零触碰"
  );
}

/// §144 格二形锁：dest 坏形 + ≥1 源且全源缺失——rust dest 前置拒即止
/// `-WRONGTYPE_HLL`；C# 源循环零 SET 直 +OK（dest 免检）——帧分叉登记形。
/// 存量 `pfmerge_without_sources_skips_storage` 仅钉零源两形，本用例防该形
/// 被当作源循环免检面回归改动旁路
#[test]
fn pfmerge_corrupt_dest_all_sources_missing_wrongtype() {
  with_batch(|sess, batch| {
    let _ = batch.try_upsert_sync(b"g2-d", b"xx");
    let mut out = Vec::new();
    sess
      .hyper_log_log_merge(&[b"g2-d", b"g2-absent1", b"g2-absent2"], batch, &mut out)
      .unwrap();
    assert_eq!(
      out,
      hll_wrongtype_frame(),
      "格二：坏形 dest + 全源缺失 rust 前置拒（C# 该形恒 +OK 免检，§144 对照注）"
    );
    assert_eq!(
      batch.try_read_sync(b"g2-d", |v| v.to_vec()).unwrap(),
      StoreResult::Success(b"xx".to_vec()),
      "dest 逐字节原样：拒裁零写回"
    );
  });
}

/// §144 格三形锁：dest=集合对象键 + 合法源——rust dest 前置拒 WRONGTYPE_HLL
/// 帧，对象成员原样、String 域零幽灵值记录；C# InPlaceUpdater :390-394
/// WrongType 动作经弃返回值的 SET_Conditional 掩盖仍 +OK（零覆写 dest 副作用
/// 两侧同净，唯帧分叉，与 §113 形 c 同判据）
#[test]
fn pfmerge_object_dest_wrongtype_keeps_members_no_ghost() {
  with_batch(|sess, batch| {
    let mut out = Vec::new();
    sess.set_add(&[b"g3-o", b"m"], batch, &mut out).unwrap();
    assert_eq!(parse_resp_int(&out), 1);
    out.clear();
    sess
      .hyper_log_log_add(&[b"g3-s", b"e1"], batch, &mut out)
      .unwrap();
    assert_eq!(parse_resp_int(&out), 1);

    out.clear();
    sess
      .hyper_log_log_merge(&[b"g3-o", b"g3-s"], batch, &mut out)
      .unwrap();
    assert_eq!(
      out,
      hll_wrongtype_frame(),
      "格三：对象键 dest 前置拒（C# 该形 +OK 掩盖帧分叉，§144 登记）"
    );
    // 对象域成员原样（未覆写）
    out.clear();
    sess
      .set_is_member(&[b"g3-o", b"m"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n", "拒裁后对象成员须原样仍在");
    // String 域零幽灵记录（无值记录凭空建）
    assert_eq!(
      batch.try_read_sync(b"g3-o", |_| true).unwrap(),
      StoreResult::NotFound,
      "格三 dest 拒裁不得在 String 域落幽灵值记录"
    );
  });
}

/// §145 案二 STRLEN 形锁：PFMERGE 成功后 dest 稀疏分配长度恒取 merge_grow
/// 单臂形（装载占用 + 扇区，形如 275/276），非 C# 热臂保留的原分配 274——
/// C# 双臂自相矛盾事实（TryMerge :945-947 裸判保 274 / MergeGrow :434-447
/// 出 275）以注释锚记，rust 单臂自洽严禁按 C# 热臂改回、亦严禁据此判冷臂
/// 形为缺陷。基数面不损（PFCOUNT 同值），稠密 dest 形恒 12304 零变
#[test]
fn pfmerge_dest_alloc_length_follows_merge_grow_single_arm() {
  with_batch(|sess, batch| {
    let hll = HyperLogLog::new();
    // g1/g2 落互异寄存器：pfmerge_error_partial_commit_fast_slow_arm_parity
    // 既锁 g1..g3 三元素 PFCOUNT==3，两两寄存器互异在册
    let mut out = Vec::new();
    sess
      .hyper_log_log_add(&[b"ag-k", b"g1"], batch, &mut out)
      .unwrap();
    out.clear();
    sess
      .hyper_log_log_add(&[b"ag-k2", b"g2"], batch, &mut out)
      .unwrap();

    let dst_before = load_blob(batch, b"ag-k");
    let src_before = load_blob(batch, b"ag-k2");
    // 新建单元素折叠形 274B（§135「N≤63 维持 274 零漂移」在册）
    assert_eq!(dst_before.len(), 274, "单元素新建键恒稀疏 274B 分配形");
    let expected = hll.merge_grow(&src_before, &dst_before);
    assert!(
      (275..=276).contains(&expected),
      "current∈{{147,148}}+128 扇区出形须为 275/276，实得 {expected}"
    );

    out.clear();
    sess
      .hyper_log_log_merge(&[b"ag-k", b"ag-k2"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"+OK\r\n");

    let merged = load_blob(batch, b"ag-k");
    assert_eq!(
      merged.len(),
      expected,
      "dest 分配长度恒取 merge_grow 单臂形（§145 法定形锁定）"
    );
    assert_ne!(
      merged.len(),
      274,
      "C# InPlace 热臂该形保留 274（TryMerge 裸判 147+2<274 恒真）——rust 不采热臂保留形"
    );
    // 基数面不损：PFCOUNT == 2 且与终形载荷直读同源
    out.clear();
    sess
      .hyper_log_log_length(&[b"ag-k"], batch, &mut out)
      .unwrap();
    let mut probe = merged.clone();
    assert_eq!(
      parse_resp_int(&out),
      hll.count(&mut probe),
      "PFCOUNT 与终形载荷同源"
    );
    assert_eq!(
      parse_resp_int(&out),
      2,
      "g1/g2 寄存器互异在册，并入基数恰 2"
    );
    // 源键零触碰
    assert_eq!(load_blob(batch, b"ag-k2"), src_before, "源载荷零改写");

    // 稠密 dest 形恒 12304 零变：merge_grow 对稠密目标恒回 DenseBytes，
    // 与装载分配长度相等 → 原位并入不迁移
    let mut dense = vec![0_u8; hll.dense_bytes()];
    hll.init_dense(&mut dense);
    assert!(hll.update_dense_register(&mut dense, 5, 3));
    let kd: &[u8] = b"ag-dense";
    let _ = batch.try_upsert_sync(kd, &dense).unwrap();
    out.clear();
    sess
      .hyper_log_log_merge(&[kd, b"ag-k2"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"+OK\r\n");
    let dense_after = load_blob(batch, kd);
    assert_eq!(dense_after.len(), 12304, "稠密 dest 形恒 12304 零变");
    assert!(hll.is_dense(&dense_after) && hll.is_valid_hyll(&dense_after));
    out.clear();
    sess.hyper_log_log_length(&[kd], batch, &mut out).unwrap();
    let mut probe = dense_after.clone();
    assert_eq!(
      parse_resp_int(&out),
      hll.count(&mut probe),
      "稠密轨 PFCOUNT 同源"
    );
  });
}
