//! MSETNX 原子性回归（全有或全无 + 降级重放语义一致）
//!
//! 对标 C# MainStoreOps.cs:MSET_Conditional（全键排他锁内 EXISTS 判定 +
//! 锁内批量 SET + Commit）与 Resp/ArrayCommands.cs:NetworkMSETNX（按
//! status 回 :0/:1）：快路径部分存在全不写回 :0、全不存在全写回 :1；
//! 判定段 / 写入段降级整体移交慢路径（exec_slow C::Msetnx）续跑，应答
//! 与存储终态恒一致，杜绝半提交误答。

use std::{
  io,
  sync::Arc,
  time::{SystemTime, UNIX_EPOCH},
};

use compio::runtime::Runtime;
use itoa::Buffer as ItoaBuffer;
use tempfile::tempdir;
use wbase::{convert::TICKS_PER_SECOND, time::now_ticks};
use wdev::SegmentedDevice;
use wkv::{Error, StoreConfig, StoreEvent, StoreEventSink, WedbStore};
use wnode::{
  resp::{
    garnet_api::{GarnetApi, StoreGarnetApi},
    resp_server_session::{RespServerSession, RespServerSessionOptions},
  },
  storage::session::common::ttl_sync::put_ttl_sync,
};
use wnode_test::auto_exec;
use wresp::command::RespCommand;
use wtest_base::open_test_store;

/// 小容量单文件存储执行域（tests/store_garnet_api_dispatch.rs 同款配置）
fn open_api(path: &str) -> (GarnetApi, tempfile::TempDir) {
  let dir = tempdir().unwrap();
  let config = StoreConfig::new(1024, 64 * 1024, 16, 0.5).unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(path)).unwrap());
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  (
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
    dir,
  )
}

/// 挂接分派器的会话（降级链路须经 session.garnet_api 挂起 SlowWait）
fn session_with(api: &GarnetApi) -> RespServerSession {
  let mut s = RespServerSession::new(1, RespServerSessionOptions::default());
  s.set_garnet_api(api.clone());
  s
}

/// 升阶建键执行域（collection_adaptive_tiering.rs 同款大容量配置）
fn open_api_big(path: &str) -> (GarnetApi, tempfile::TempDir) {
  let dir = tempdir().unwrap();
  let config = StoreConfig::new(2048, 1024 * 1024, 16, 0.5).unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(path)).unwrap());
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  (
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
    dir,
  )
}

#[test]
fn msetnx_all_missing_writes_all() {
  let (api, _dir) = open_api("msetnx-all-missing.db");
  let mut s = session_with(&api);

  // 全不存在：全写入回 :1（C# NOTFOUND → 1）
  api.exec(&mut s, RespCommand::Msetnx, &[b"k1", b"v1", b"k2", b"v2"]);
  assert_eq!(s.output, b":1\r\n");

  // 两键均已生效
  s.output.clear();
  api.exec(&mut s, RespCommand::Get, &[b"k1"]);
  assert_eq!(s.output, b"$2\r\nv1\r\n");
  s.output.clear();
  api.exec(&mut s, RespCommand::Get, &[b"k2"]);
  assert_eq!(s.output, b"$2\r\nv2\r\n");
}

#[test]
fn msetnx_any_existing_writes_none() {
  let (api, _dir) = open_api("msetnx-any-existing.db");
  let mut s = session_with(&api);

  // k1 预置
  api.exec(&mut s, RespCommand::Set, &[b"k1", b"old"]);
  s.output.clear();

  // 任一键已存在：全不写入回 :0
  api.exec(&mut s, RespCommand::Msetnx, &[b"k1", b"new", b"k2", b"v2"]);
  assert_eq!(s.output, b":0\r\n");

  // 已存在键值不变、缺失键保持缺失（全有或全无）
  s.output.clear();
  api.exec(&mut s, RespCommand::Get, &[b"k1"]);
  assert_eq!(s.output, b"$3\r\nold\r\n");
  s.output.clear();
  api.exec(&mut s, RespCommand::Get, &[b"k2"]);
  assert_eq!(s.output, b"$-1\r\n");
}

#[test]
fn msetnx_object_key_counts_as_existing() {
  let (api, _dir) = open_api("msetnx-object-key.db");
  let mut s = session_with(&api);

  // 对象键（信封域）同计存在（C# EXISTS 走 unified 域同口径）
  api.exec(&mut s, RespCommand::Hset, &[b"obj", b"f", b"v"]);
  s.output.clear();

  api.exec(&mut s, RespCommand::Msetnx, &[b"obj", b"x", b"k2", b"v2"]);
  assert_eq!(s.output, b":0\r\n");

  s.output.clear();
  api.exec(&mut s, RespCommand::Get, &[b"k2"]);
  assert_eq!(s.output, b"$-1\r\n");
}

#[test]
fn msetnx_expired_key_treated_as_missing() {
  let (api, _dir) = open_api("msetnx-expired-missing.db");
  let mut s = session_with(&api);

  // k1 预置后打上已过期的绝对过期（PEXPIREAT 过去毫秒即删，wkv applied=2
  // 物理删除语义）：NX 判定视同缺失，MSETNX 通过并覆盖写入（Redis MSETNX
  // 对过期键同语义）
  api.exec(&mut s, RespCommand::Set, &[b"k1", b"v1"]);
  s.output.clear();
  let past_ms = (SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .unwrap()
    .as_millis() as u64)
    - 10_000;
  api.exec(
    &mut s,
    RespCommand::Pexpireat,
    &[b"k1", past_ms.to_string().as_bytes()],
  );
  s.output.clear();

  api.exec(&mut s, RespCommand::Msetnx, &[b"k1", b"x", b"k2", b"v2"]);
  assert_eq!(s.output, b":1\r\n");

  // 全部键已生效：k1 为本次 MSETNX 的值（旧值连同过期记录一并清退）
  s.output.clear();
  api.exec(&mut s, RespCommand::Get, &[b"k1"]);
  assert_eq!(s.output, b"$1\r\nx\r\n");
  s.output.clear();
  api.exec(&mut s, RespCommand::Get, &[b"k2"]);
  assert_eq!(s.output, b"$2\r\nv2\r\n");
}

#[test]
fn msetnx_slow_full_verdict_matches_fast_semantics() {
  let rt = Runtime::new().unwrap();
  let (api, _dir) = open_api("msetnx-slow-verdict.db");

  // 判定段降级模式（尾参 b"0"）· 全不存在：裁决通过 → 全写回 :1
  let out = rt.block_on(Arc::clone(&api).exec_slow(
    RespCommand::Msetnx,
    vec![
      b"k1".to_vec(),
      b"v1".to_vec(),
      b"k2".to_vec(),
      b"v2".to_vec(),
      b"0".to_vec(),
    ],
    wconf::DEFAULT_RESP_VERSION,
  ));
  assert_eq!(out, b":1\r\n");

  // 判定段降级模式 · 部分存活（k1 已由上一段写入）：整体不写回 :0
  let out = rt.block_on(Arc::clone(&api).exec_slow(
    RespCommand::Msetnx,
    vec![
      b"k1".to_vec(),
      b"x".to_vec(),
      b"k3".to_vec(),
      b"v3".to_vec(),
      b"0".to_vec(),
    ],
    wconf::DEFAULT_RESP_VERSION,
  ));
  assert_eq!(out, b":0\r\n");

  // 终态：k3 未写入、k1 值不变（裁决失败绝不半提交）
  let mut s = session_with(&api);
  api.exec(&mut s, RespCommand::Get, &[b"k3"]);
  assert_eq!(s.output, b"$-1\r\n");
  s.output.clear();
  api.exec(&mut s, RespCommand::Get, &[b"k1"]);
  assert_eq!(s.output, b"$2\r\nv1\r\n");
}

#[test]
fn msetnx_slow_resume_completes_partial_write() {
  let rt = Runtime::new().unwrap();
  let (api, _dir) = open_api("msetnx-slow-resume.db");
  let mut s = session_with(&api);

  // 模拟快路径写入段降级现场：判定已整体通过、前缀键 k1 已持久写入
  api.exec(&mut s, RespCommand::Set, &[b"k1", b"v1"]);
  s.output.clear();

  // 补写模式（尾参 b"1"）：不重判存活，续写全部键值（重写已写键同值
  // 幂等）回 :1——降级前判定为"全部不存在将写入"，终应答与终态一致
  let out = rt.block_on(Arc::clone(&api).exec_slow(
    RespCommand::Msetnx,
    vec![
      b"k1".to_vec(),
      b"v1".to_vec(),
      b"k2".to_vec(),
      b"v2".to_vec(),
      b"1".to_vec(),
    ],
    wconf::DEFAULT_RESP_VERSION,
  ));
  assert_eq!(out, b":1\r\n");

  s.output.clear();
  api.exec(&mut s, RespCommand::Get, &[b"k2"]);
  assert_eq!(s.output, b"$2\r\nv2\r\n");
  s.output.clear();
  api.exec(&mut s, RespCommand::Get, &[b"k1"]);
  assert_eq!(s.output, b"$2\r\nv1\r\n");
}

/// 升阶冷键存活判定回归：大集合键升阶只 upsert Meta 元记录 + delete 信封
///（object_store_utils.rs:promote_collection_to_bftree），Meta 域是该键在
/// 存储里的唯一身份。FLUSHANDEVICT 使元记录成磁盘候选后，快路径 NX 判定
/// 整体降级慢路径，三域裁决（String / ObjectEnvelope / Meta，对标 C#
/// unified 域 EXISTS 非 NOTFOUND 即存在）必须判存活 → :0 零写入。修复前
/// 慢路径只探两域漏 Meta，误判不存在 → :1 写出双域键：String 域遮蔽原
/// 集合、wbftree 树文件成无主孤儿
#[test]
fn msetnx_slow_meta_only_promoted_key_counts_as_existing() {
  let rt = Runtime::new().unwrap();
  let (api, _dir) = open_api_big("msetnx-meta-only.db");
  let mut s = session_with(&api);

  // 建大集合键跨升阶门限（collection_adaptive_tiering.rs 同款建键）
  let total = wcol::TIERED_PROMOTE_THRESHOLD + 10;
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Hset,
      &[b"big", b"f1", b"v1", b"f2", b"v2"]
    ),
    b":2\r\n"
  );
  let mut buf = ItoaBuffer::new();
  for chunk_start in (3..=total).step_by(16384) {
    let chunk_end = (chunk_start + 16383).min(total);
    let mut args: Vec<Vec<u8>> = Vec::with_capacity((chunk_end - chunk_start + 1) * 2 + 1);
    args.push(b"big".to_vec());
    for i in chunk_start..=chunk_end {
      args.push(format!("f{i}").into_bytes());
      args.push(buf.format(i).as_bytes().to_vec());
    }
    let arg_slices: Vec<&[u8]> = args.iter().map(|v| v.as_slice()).collect();
    auto_exec(&api, &rt, &mut s, RespCommand::Hset, &arg_slices);
  }

  // 升阶确认：O(1) 计数照答
  let expect_len = format!(":{total}\r\n");
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Hlen, &[b"big"]),
    expect_len.as_bytes()
  );

  // FLUSHANDEVICT：全部页刷盘驱逐，Meta 元记录成磁盘候选（快路径探针
  // Ok(None) 的降级源）
  let out = rt.block_on(Arc::clone(&api).exec_slow(
    RespCommand::Debug,
    vec![b"FLUSHANDEVICT".to_vec()],
    wconf::DEFAULT_RESP_VERSION,
  ));
  assert!(out.starts_with(b"+OK head="));

  // MSETNX：快路径 NX 判定遇磁盘候选整体降级慢路径，三域裁决判存活
  // → :0 零写入（修复前两域皆空 → 误判不存在 → :1 并写出 String 域）
  s.output.clear();
  api.exec(&mut s, RespCommand::Msetnx, &[b"big", b"x", b"k2", b"v2"]);
  assert!(s.output.is_empty(), "MSETNX 磁盘候选应整体降级慢路径");
  let slow = s.take_slow_wait().expect("MSETNX 判定段降级应挂起慢路径");
  assert_eq!(rt.block_on(slow.resolve()), b":0\r\n");

  // 零写入核验：原集合身份不变、String 域无记录、其余键未写
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Hlen, &[b"big"]),
    expect_len.as_bytes()
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Exists, &[b"big"]),
    b":1\r\n"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Get, &[b"big"]),
    b"$-1\r\n"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Get, &[b"k2"]),
    b"$-1\r\n"
  );
}

/// 写入段中途失败注入 sink：victim 键的数据写事件（tombstone = false）恒败，
/// 墓碑与其它键全部放行——精确模拟「k1 已落盘、victim 写入报错」的中途失败
/// 现场且不干扰回滚路径（rename_semantics.rs 选择性失败 sink 同款手法）
fn fail_victim_data_write_sink(
  _: &(),
  _ver: i64,
  _aof_session_id: i32,
  event: StoreEvent<'_>,
) -> wkv::Result<()> {
  match event {
    StoreEvent::Write {
      key,
      tombstone: false,
      ..
    } if key.ends_with(b"victim") => Err(Error::Io(io::Error::other("injected mid-write failure"))),
    _ => Ok(()),
  }
}

/// 慢路径写入段中途失败全有或全无（M13 回归）：判定整体通过后 victim 键
/// 写入失败，已落盘的 k1 必须被回滚删除，整命令报错而非半提交——修复前
/// 逐键 upsert_string 中途 Err 直接报错，前序键残留破坏全有或全无
#[test]
fn msetnx_slow_midwrite_failure_rolls_back_all() {
  let rt = Runtime::new().unwrap();
  let dir = tempdir().unwrap();
  let config = StoreConfig::new(1024, 64 * 1024, 16, 0.5).unwrap();
  let device =
    Arc::new(SegmentedDevice::single_file(dir.path().join("msetnx-midfail.db")).unwrap());
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  // 注入 sink 须先于任何会话创建（wkv tests/store/rename_semantics.rs 同序）
  assert!(store.set_event_sink(StoreEventSink::new(
    Arc::new(()),
    fail_victim_data_write_sink
  )));
  let api: GarnetApi = Arc::new(StoreGarnetApi::new(store.new_session().unwrap()));
  let mut s = session_with(&api);

  // 无关预置键证明注入不误伤正常写入
  api.exec(&mut s, RespCommand::Set, &[b"keep", b"v0"]);
  assert_eq!(s.output, b"+OK\r\n");
  s.output.clear();

  // 判定段降级模式（尾参 b"0"）全链：同步探针三键全缺失 → 折叠写入 k1
  // 成功、victim 写事件失败 → 兜底重放 k1 幂等成功、victim 再失败 →
  // 回滚全部涉及键 → 存储错误应答（绝不 :1 / :0 误答半提交态）
  let out = rt.block_on(Arc::clone(&api).exec_slow(
    RespCommand::Msetnx,
    vec![
      b"k1".to_vec(),
      b"v1".to_vec(),
      b"victim".to_vec(),
      b"v2".to_vec(),
      b"k3".to_vec(),
      b"v3".to_vec(),
      b"0".to_vec(),
    ],
    wconf::DEFAULT_RESP_VERSION,
  ));
  assert_eq!(out, b"-ERR slow path storage error\r\n");

  // 全有或全无：本命令三键全 nil，无关键不受扰
  let verdicts: [(&[u8], &str); 3] = [
    (b"k1", "先写成功键必须回滚"),
    (b"victim", "失败键零残留"),
    (b"k3", "未达键零写入"),
  ];
  for (key, name) in verdicts {
    s.output.clear();
    api.exec(&mut s, RespCommand::Get, &[key]);
    assert_eq!(s.output, b"$-1\r\n", "{name}");
  }
  s.output.clear();
  api.exec(&mut s, RespCommand::Get, &[b"keep"]);
  assert_eq!(s.output, b"$2\r\nv0\r\n");
}

/// 慢路径补写模式（resume）中途失败同样全有或全无：快路径已写入的前缀键
///（本测试以 SET 预置模拟）随回滚一并删除——全有或全无面向整条命令，与
/// 降级前写入归属无关
#[test]
fn msetnx_slow_resume_midwrite_failure_rolls_back_prefix_keys() {
  let rt = Runtime::new().unwrap();
  let dir = tempdir().unwrap();
  let config = StoreConfig::new(1024, 64 * 1024, 16, 0.5).unwrap();
  let device =
    Arc::new(SegmentedDevice::single_file(dir.path().join("msetnx-resume-midfail.db")).unwrap());
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  assert!(store.set_event_sink(StoreEventSink::new(
    Arc::new(()),
    fail_victim_data_write_sink
  )));
  let api: GarnetApi = Arc::new(StoreGarnetApi::new(store.new_session().unwrap()));
  let mut s = session_with(&api);

  // 模拟快路径写入段降级现场：判定已通过、前缀键 k1 已持久写入
  api.exec(&mut s, RespCommand::Set, &[b"k1", b"v1"]);
  assert_eq!(s.output, b"+OK\r\n");
  s.output.clear();

  // 补写模式（尾参 b"1"）：跳过判定续写，victim 写入失败 → 回滚含 k1
  let out = rt.block_on(Arc::clone(&api).exec_slow(
    RespCommand::Msetnx,
    vec![
      b"k1".to_vec(),
      b"v1".to_vec(),
      b"victim".to_vec(),
      b"v2".to_vec(),
      b"1".to_vec(),
    ],
    wconf::DEFAULT_RESP_VERSION,
  ));
  assert_eq!(out, b"-ERR slow path storage error\r\n");

  // 命令级全有或全无：快路径已写的前缀键 k1 同被回滚
  for key in [b"k1" as &[u8], b"victim"] {
    s.output.clear();
    api.exec(&mut s, RespCommand::Get, &[key]);
    assert_eq!(s.output, b"$-1\r\n");
  }
}
/// 慢路径回滚收尾模式（尾参 b"r"）内容条件化（票 zcode-r37-lockfix 发现 B）：
/// 仅删内容即本命令所写的键——被并发盲写 SET 抢覆写的键（内容非本命令值）
/// 保留其已确认写不删；回 :0 表达全有或全无失败面。修复前回滚为盲删，
/// 已写键上的并发已确认写会被整体吞掉
#[test]
fn msetnx_slow_rollback_mode_is_content_conditioned() {
  let rt = Runtime::new().unwrap();
  let (api, _dir) = open_api("msetnx-slow-rollback.db");
  let mut s = session_with(&api);

  // 现场：k1 已被本命令写入 v1 后遭并发盲写覆写为 rival；k2 仍是本命令
  // 写入的 v2（待清残留）
  api.exec(&mut s, RespCommand::Set, &[b"k1", b"rival"]);
  assert_eq!(s.output, b"+OK\r\n");
  s.output.clear();
  api.exec(&mut s, RespCommand::Set, &[b"k2", b"v2"]);
  assert_eq!(s.output, b"+OK\r\n");
  s.output.clear();

  // 回滚收尾（尾参 b"r"）：k1 内容非本命令值保留不删，k2 同值清除 → :0
  let out = rt.block_on(Arc::clone(&api).exec_slow(
    RespCommand::Msetnx,
    vec![
      b"k1".to_vec(),
      b"v1".to_vec(),
      b"k2".to_vec(),
      b"v2".to_vec(),
      b"r".to_vec(),
    ],
    wconf::DEFAULT_RESP_VERSION,
  ));
  assert_eq!(out, b":0\r\n");

  // 并发已确认写存活：k1 = rival（盲删形态下会被吞成 nil）
  s.output.clear();
  api.exec(&mut s, RespCommand::Get, &[b"k1"]);
  assert_eq!(s.output, b"$5\r\nrival\r\n");
  // 本命令所写残留已清：k2 nil
  s.output.clear();
  api.exec(&mut s, RespCommand::Get, &[b"k2"]);
  assert_eq!(s.output, b"$-1\r\n");
}

struct MsetnxRaceContext {
  store: Arc<WedbStore<SegmentedDevice>>,
}

fn race_victim_sink(
  ctx: &MsetnxRaceContext,
  _ver: i64,
  _aof_session_id: i32,
  event: StoreEvent<'_>,
) -> wkv::Result<()> {
  match event {
    StoreEvent::Write {
      key,
      tombstone: false,
      ..
    } if key.ends_with(b"victim") => {
      // 模拟并发盲写：在 MSETNX 已写入 k1、正在写 victim 时，并发 SET k1 = "concurrent_val"
      let sess = ctx
        .store
        .new_session()
        .map_err(|e| Error::Io(io::Error::other(e.to_string())))?;
      let _ = sess
        .try_upsert_sync(b"k1", b"concurrent_val")
        .map_err(|e| Error::Io(io::Error::other(e.to_string())))?;
      // 触发 victim 写入失败，令 MSETNX 进入回滚臂
      Err(Error::Io(io::Error::other("injected mid-write failure")))
    }
    _ => Ok(()),
  }
}

/// MSETNX 快路径回滚臂复验待删内容：
/// 并发盲写篡改已写键值后，回滚不得吞掉并发盲写值
#[test]
fn msetnx_fast_rollback_preserves_concurrent_blind_write() {
  let dir = tempdir().unwrap();
  let config = StoreConfig::new(1024, 64 * 1024, 16, 0.5).unwrap();
  let device =
    Arc::new(SegmentedDevice::single_file(dir.path().join("msetnx-race-preserve.db")).unwrap());
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  assert!(store.set_event_sink(StoreEventSink::new(
    Arc::new(MsetnxRaceContext {
      store: Arc::clone(&store),
    }),
    race_victim_sink,
  )));
  let api: GarnetApi = Arc::new(StoreGarnetApi::new(store.new_session().unwrap()));
  let mut s = session_with(&api);

  // 执行快路径 MSETNX，k1 写入后 victim 失败触发回滚
  api.exec(
    &mut s,
    RespCommand::Msetnx,
    &[b"k1", b"v1", b"victim", b"v2"],
  );
  assert_eq!(s.output, b"-ERR generic error\r\n");
  s.output.clear();

  // k1 上的并发写没有被回滚盲删吞掉
  api.exec(&mut s, RespCommand::Get, &[b"k1"]);
  assert_eq!(s.output, b"$14\r\nconcurrent_val\r\n");
}

/// MSETNX 快路径回滚臂正常回滚：
/// 待删内容确系本次所写且无并发写时，回滚将其正常删除
#[test]
fn msetnx_fast_rollback_deletes_when_no_concurrent_write() {
  let dir = tempdir().unwrap();
  let config = StoreConfig::new(1024, 64 * 1024, 16, 0.5).unwrap();
  let device =
    Arc::new(SegmentedDevice::single_file(dir.path().join("msetnx-clean-rollback.db")).unwrap());
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  assert!(store.set_event_sink(StoreEventSink::new(
    Arc::new(()),
    fail_victim_data_write_sink
  )));
  let api: GarnetApi = Arc::new(StoreGarnetApi::new(store.new_session().unwrap()));
  let mut s = session_with(&api);

  // 执行快路径 MSETNX，k1 写入后 victim 失败触发回滚
  api.exec(
    &mut s,
    RespCommand::Msetnx,
    &[b"k1", b"v1", b"victim", b"v2"],
  );
  assert_eq!(s.output, b"-ERR generic error\r\n");
  s.output.clear();

  // 无并发写时，k1 确已被回滚删除
  api.exec(&mut s, RespCommand::Get, &[b"k1"]);
  assert_eq!(s.output, b"$-1\r\n");
}

/// 构造「逻辑已死、物理在场」过期残留态：SET 后 TTL 同步侧裸写过去刻度
/// （RESP 面无法自然构造过期未清退，先例 probe_alive_single_point.rs:
/// expire_in_past；open_test_store GC 关闭，残留恒在场待惰性清退）
fn expire_residual(store: &Arc<WedbStore<SegmentedDevice>>, key: &[u8]) {
  let session = store.new_session().unwrap();
  let batch = session.enter_batch();
  put_ttl_sync(&batch, key, now_ticks() - TICKS_PER_SECOND).unwrap();
}

/// 票 zcode-r141c-msetbig 案一主锁：过期残留键（存活探针判死、String 物理
/// 记录 Active 待清退）MSETNX 快臂恒回 :1 并 upsert 覆写、旧 TTL 随之清退
/// ——对标 C# MainStoreOps.cs:MSET_Conditional EXISTS（CheckExpiry 判
/// NOTFOUND）→ 锁内 SET 无条件覆写残留记录恒回 :1 同构，与 SETNX 窗内探针
/// 判死直 upsert 回 :1 逐字节同型。修复前写入段 try_insert_sync 物理在场
/// 第二判据对判死残留键拒写，闩窗内确定性触发假回 :0
#[test]
fn msetnx_fast_arm_expired_residual_single_verdict_source() {
  let (_dir, store) = open_test_store("msetnx-expired-residual.db").unwrap();
  let api: GarnetApi = Arc::new(StoreGarnetApi::new(store.new_session().unwrap()));
  let mut s = session_with(&api);

  // 前置形态确证：残留键 GET 判死（TTL 门 Due→NotFound，物理不清退）
  api.exec(&mut s, RespCommand::Set, &[b"r1", b"old"]);
  assert_eq!(s.output, b"+OK\r\n");
  s.output.clear();
  expire_residual(&store, b"r1");
  api.exec(&mut s, RespCommand::Get, &[b"r1"]);
  assert_eq!(s.output, b"$-1\r\n", "残留键必先判死（案一前提）");
  s.output.clear();

  // 快臂直接闭环（应答即出=未降级慢路径）：探针判死 → upsert 覆写回 :1
  api.exec(&mut s, RespCommand::Msetnx, &[b"r1", b"new"]);
  assert_eq!(
    s.output, b":1\r\n",
    "存活探针=唯一 NX 判据，残留不得假回 :0"
  );
  s.output.clear();

  // 新值生效、旧 TTL 随 upsert 清退自愈（PTTL -1=键在而无 TTL）
  api.exec(&mut s, RespCommand::Get, &[b"r1"]);
  assert_eq!(s.output, b"$3\r\nnew\r\n");
  s.output.clear();
  api.exec(&mut s, RespCommand::Pttl, &[b"r1"]);
  assert_eq!(s.output, b":-1\r\n", "旧 TTL 必被 upsert 清退");
  s.output.clear();

  // SETNX 同判据族对照：同残留形态回 :1 且落值，修复后两臂同型同源
  api.exec(&mut s, RespCommand::Set, &[b"r2", b"old"]);
  assert_eq!(s.output, b"+OK\r\n");
  s.output.clear();
  expire_residual(&store, b"r2");
  api.exec(&mut s, RespCommand::Setnx, &[b"r2", b"new"]);
  assert_eq!(s.output, b":1\r\n");
  s.output.clear();
  api.exec(&mut s, RespCommand::Get, &[b"r2"]);
  assert_eq!(s.output, b"$3\r\nnew\r\n");
  s.output.clear();

  // 存活键面零漂移：r1 已实存后再 MSETNX 判存活回 :0 且全有或全无零写入
  api.exec(
    &mut s,
    RespCommand::Msetnx,
    &[b"r1", b"never", b"r3", b"v3"],
  );
  assert_eq!(s.output, b":0\r\n");
  s.output.clear();
  api.exec(&mut s, RespCommand::Get, &[b"r3"]);
  assert_eq!(s.output, b"$-1\r\n");
  s.output.clear();
  api.exec(&mut s, RespCommand::Get, &[b"r1"]);
  assert_eq!(s.output, b"$3\r\nnew\r\n");
}
