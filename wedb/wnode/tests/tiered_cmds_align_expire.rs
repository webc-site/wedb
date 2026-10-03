//! 升阶到期出账语义对齐集成测试（自 tiered_cmds_align.rs 按主题拆分）
//!
//! 覆盖：
//! 1. 双态到期出账计数 parity：同数据同序列（存活字段 + 挂 TTL 字段，越线后
//!    HLEN 出账）分层态与内存信封态应答逐字节一致——分层态水位越过出账按
//!    `live.len()` 实存直赋，与信封态堆序惰性剔除同口径收敛存活数；
//! 2. HSET 覆写到期字段三向全等：页缓存预算耗尽推迟重灌窗内 / 预算充足重灌
//!    路径 / 内存信封态的 HSET 新增计数应答逐字节一致（到期视同缺席补偿叠加，
//!    对齐 C# HashObjectImpl.cs:206 `!exists || IsExpired` 臂 set++）。

use std::{sync::Arc, time::Duration};

use compio::runtime::Runtime;
use tempfile::tempdir;
use wbase::{convert::TICKS_PER_SECOND, time::now_ticks};
use wcol::types::member_ttl::encode_member;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::resp::garnet_api::{GarnetApi, StoreGarnetApi};
use wnode_test::{auto_exec, open_env, session_on as session_with};
use wresp::command::RespCommand;
use wtest_base::wait_assert_sync;
use wval::GarnetObjectType;

/// 双态到期出账计数 parity（票 wnode-tiered-expire-rebuild-swapin-size-overcount）：
/// 同数据同序列（一存活字段 + 一挂 3s TTL 字段，越线后 HLEN 出账）分层态与
/// 内存信封态应答逐字节一致——分层态水位越过出账按 `live.len()` 实存直赋
///（与 promote 落盘 bulk_load 去重计数同源同值），与信封态堆序惰性剔除
/// 同口径收敛存活数；虚高不收敛（旧差量扣减形）即双态分叉
#[test]
fn test_tiered_expire_count_parity_with_envelope() {
  let (rt, api, store, _dir) = open_env("tiered-expire-count-parity.db");
  let mut s = session_with(&api);
  let future = now_ticks() + 3 * TICKS_PER_SECOND;

  // 内存信封态基准：双字段，其一挂 3s TTL
  for (f, v) in [(b"a1", b"va"), (b"e1", b"ve")] {
    assert_eq!(
      auto_exec(&api, &rt, &mut s, RespCommand::Hset, &[b"small", f, v]),
      b":1\r\n"
    );
  }
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Hexpire,
      &[b"small", b"3", b"FIELDS", b"1", b"e1"]
    ),
    b"*1\r\n:1\r\n",
    "信封基准 HEXPIRE 应回 ExpireUpdated"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Hlen, &[b"small"]),
    b":2\r\n",
    "未到期信封基准 HLEN 直读 2"
  );

  // 分层态：同数据同 TTL 手工升阶（水位 = e1 到期刻度随灌入批落盘）
  rt.block_on(store.new_session().unwrap().promote_collection_to_bftree(
    b"big",
    GarnetObjectType::Hash,
    vec![
      (b"a1".to_vec(), encode_member(b"va", None)),
      (b"e1".to_vec(), encode_member(b"ve", Some(future))),
    ],
    future,
    false,
  ))
  .unwrap();
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Hlen, &[b"big"]),
    b":2\r\n",
    "水位内分层态 HLEN 直读 size 恒精确"
  );

  // 越线后双态 HLEN 出账应答逐字节一致（分层态实存直赋，内存态堆序剔除）。
  // 轮询到期可观测量：HGET small e1 → nil（3s TTL 自然过期，两键同刻度到期；
  // 替换固定 3500ms 睡眠，超时即测试失败）
  wait_assert_sync(
    || auto_exec(&api, &rt, &mut s, RespCommand::Hget, &[b"small", b"e1"]) == b"$-1\r\n",
    Duration::from_secs(5),
    Duration::from_millis(50),
    "small.e1 须在超时窗口内自然过期（HGET → nil）",
  );
  let envelope = auto_exec(&api, &rt, &mut s, RespCommand::Hlen, &[b"small"]);
  assert_eq!(envelope, b":1\r\n", "信封基准：剔除到期字段后存活 1");
  let tiered = auto_exec(&api, &rt, &mut s, RespCommand::Hlen, &[b"big"]);
  assert_eq!(
    tiered, envelope,
    "分层态出账计数应与内存信封态逐字节一致（实存直赋收敛存活数）"
  );
}

/// [`open_env`] 的压预算形态：单环页缓存预算首升阶占满后，出账重灌的
/// scratch 建树恒被拒——构造 [`expire_sweep_or_rebuild`] 推迟重灌窗
///（与 tiered_expire_sweep_failure 的 budget_exhausted_defers_rebuild_
/// keeps_watch_frozen 同款口径，两文件各自内聚不共享测试私有态）
fn open_env_with_budget(
  tag: &str,
  budget: usize,
) -> (
  Runtime,
  GarnetApi,
  Arc<WedbStore<SegmentedDevice>>,
  tempfile::TempDir,
) {
  let dir = tempdir().unwrap();
  let config = StoreConfig::new(2048, 1024 * 1024, 16, 0.5)
    .unwrap()
    .with_tree_cache_budget(budget);
  let store = wnode_test::store_open(&dir, tag, config);
  (
    Runtime::new().unwrap(),
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())).into(),
    store,
    dir,
  )
}

/// RESP2 bulk 应答帧（HGET 值探针断言用，$<len>\r\n<val>\r\n）
fn bulk_frame(val: &[u8]) -> Vec<u8> {
  let mut out = format!("${}\r\n", val.len()).into_bytes();
  out.extend_from_slice(val);
  out.extend_from_slice(b"\r\n");
  out
}

/// HSET 覆写到期字段三向全等（票
/// wnode-tiered-hash-deferred-sweep-hset-newfield-undercount）：
/// 同数据同序列（一存活字段 + 三带到期 TTL 字段）下，页缓存预算耗尽推迟
/// 重灌窗内 / 预算充足重灌路径 / 内存信封态的 HSET 新增计数应答逐字节一致
/// ——推迟臂内到期成员仍滞留树（树原样、磁盘元记录未动），HSET 批量折叠
/// 的 upsert 按键已存在覆盖不计 new_fields，命中推迟键集的字段按「到期视同
/// 缺席」补偿叠加（对齐 C# HashObjectImpl.cs:206 `!exists || IsExpired` 臂
/// set++）；修复前推迟窗单字段覆写应答 :0、混合批 :1，与另两态分叉。
/// HMSET 同窗 +OK 无计数面；窗口后首个计数命令零到期水位前移臂 live.len()
/// 实存直赋收敛（不建树不占预算，预算耗尽下照常收敛）
#[test]
fn test_tiered_hset_expired_overwrite_deferred_parity() {
  // ── 内存信封态基准（默认预算 env；小键不升阶，走 wcol hash_set 净语义）──
  let (rt, api, store, _dir) = open_env("tiered-hset-defer-parity.db");
  let mut s = session_with(&api);
  for (f, v) in [
    (&b"a1"[..], &b"va"[..]),
    (b"e1", b"ve1"),
    (b"e2", b"ve2"),
    (b"e3", b"ve3"),
  ] {
    assert_eq!(
      auto_exec(&api, &rt, &mut s, RespCommand::Hset, &[b"small", f, v]),
      b":1\r\n"
    );
  }
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Hexpire,
      &[b"small", b"1", b"FIELDS", b"3", b"e1", b"e2", b"e3"]
    ),
    b"*3\r\n:1\r\n:1\r\n:1\r\n",
    "信封基准 HEXPIRE 三字段全部 ExpireUpdated"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Hlen, &[b"small"]),
    b":4\r\n",
    "未到期信封基准 HLEN 直读 4"
  );
  // 轮询到期可观测量：HGET small e1 → nil（1s TTL 自然过期，同刻度三字段
  // e1/e2/e3 一并到期；替换固定 1100ms 睡眠，超时即测试失败）
  wait_assert_sync(
    || auto_exec(&api, &rt, &mut s, RespCommand::Hget, &[b"small", b"e1"]) == b"$-1\r\n",
    Duration::from_secs(5),
    Duration::from_millis(50),
    "small.e1 须在超时窗口内自然过期（HGET → nil）",
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Hget, &[b"small", b"e1"]),
    b"$-1\r\n",
    "信封基准到期确认（读面惰性剔除 null）"
  );
  let env_single = auto_exec(
    &api,
    &rt,
    &mut s,
    RespCommand::Hset,
    &[b"small", b"e1", b"vnew"],
  );
  assert_eq!(env_single, b":1\r\n", "信封基准：覆写到期字段计新增 1");
  let env_multi = auto_exec(
    &api,
    &rt,
    &mut s,
    RespCommand::Hset,
    &[b"small", b"e2", b"v2", b"a1", b"va2", b"newf", b"vn"],
  );
  assert_eq!(
    env_multi, b":2\r\n",
    "信封基准：混合批 = 到期视缺席 1 + 真缺席 1 + 存活覆盖 0"
  );
  let env_hmset = auto_exec(
    &api,
    &rt,
    &mut s,
    RespCommand::Hmset,
    &[b"small", b"e3", b"v3"],
  );
  assert_eq!(env_hmset, b"+OK\r\n", "HMSET 应答 +OK 无计数面");
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Hlen, &[b"small"]),
    b":5\r\n",
    "信封基准终态：a1/e1/e2/newf/e3 五字段存活"
  );

  // ── 分层态重灌成功路径（同 env 默认预算，出账前置物理摘除到期成员）──
  let past = now_ticks() - TICKS_PER_SECOND;
  let entries = vec![
    (b"a1".to_vec(), encode_member(b"va", None)),
    (b"e1".to_vec(), encode_member(b"ve1", Some(past))),
    (b"e2".to_vec(), encode_member(b"ve2", Some(past))),
    (b"e3".to_vec(), encode_member(b"ve3", Some(past))),
  ];
  rt.block_on(store.new_session().unwrap().promote_collection_to_bftree(
    b"big",
    GarnetObjectType::Hash,
    entries.clone(),
    past,
    false,
  ))
  .unwrap();
  let steady_single = auto_exec(
    &api,
    &rt,
    &mut s,
    RespCommand::Hset,
    &[b"big", b"e1", b"vnew"],
  );
  assert_eq!(
    steady_single, b":1\r\n",
    "重灌成功：出账后到期字段真缺席计 1"
  );
  let steady_multi = auto_exec(
    &api,
    &rt,
    &mut s,
    RespCommand::Hset,
    &[b"big", b"e2", b"v2", b"a1", b"va2", b"newf", b"vn"],
  );
  assert_eq!(steady_multi, b":2\r\n", "重灌成功路径混合批与信封基准同值");
  let steady_hmset = auto_exec(
    &api,
    &rt,
    &mut s,
    RespCommand::Hmset,
    &[b"big", b"e3", b"v3"],
  );
  assert_eq!(steady_hmset, b"+OK\r\n");
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Hlen, &[b"big"]),
    b":5\r\n",
    "重灌成功路径终态与信封基准同值"
  );

  // ── 推迟重灌窗（单环预算 env：首升阶占满，重灌 scratch 建树恒被拒）──
  let (rt2, api2, store2, _dir2) =
    open_env_with_budget("tiered-hset-defer-window.db", 16 * 1024 * 1024);
  let mut s2 = session_with(&api2);
  rt2
    .block_on(store2.new_session().unwrap().promote_collection_to_bftree(
      b"big2",
      GarnetObjectType::Hash,
      entries,
      past,
      false,
    ))
    .unwrap();
  let deferred_single = auto_exec(
    &api2,
    &rt2,
    &mut s2,
    RespCommand::Hset,
    &[b"big2", b"e1", b"vnew"],
  );
  assert_eq!(
    deferred_single, env_single,
    "推迟窗覆写到期字段应答须与信封基准逐字节一致（补偿计数，修复前 :0）"
  );
  let deferred_multi = auto_exec(
    &api2,
    &rt2,
    &mut s2,
    RespCommand::Hset,
    &[b"big2", b"e2", b"v2", b"a1", b"va2", b"newf", b"vn"],
  );
  assert_eq!(
    deferred_multi, env_multi,
    "推迟窗混合批应答须与信封基准逐字节一致（修复前 :1）"
  );
  let deferred_hmset = auto_exec(
    &api2,
    &rt2,
    &mut s2,
    RespCommand::Hmset,
    &[b"big2", b"e3", b"v3"],
  );
  assert_eq!(deferred_hmset, env_hmset, "推迟窗 HMSET +OK 三态同形");
  assert_eq!(
    auto_exec(&api2, &rt2, &mut s2, RespCommand::Hlen, &[b"big2"]),
    b":5\r\n",
    "推迟窗后首个计数命令收敛实存 5（零到期水位前移臂 live.len() 直赋）"
  );
  // 覆写值全面生效（物理覆盖正确性探针，逐字段 HGET）
  for (f, v) in [
    (&b"a1"[..], &b"va2"[..]),
    (b"e1", b"vnew"),
    (b"e2", b"v2"),
    (b"newf", b"vn"),
    (b"e3", b"v3"),
  ] {
    let out = auto_exec(&api2, &rt2, &mut s2, RespCommand::Hget, &[b"big2", f]);
    assert_eq!(out, bulk_frame(v), "推迟窗覆写字段值须生效（物理覆盖）");
  }
}
