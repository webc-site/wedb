//! 分层态后台懒降阶评估轮集成测试（doc/zh/collection.md 3.3「后台紧缩异步降阶」）
//!
//! 覆盖口径：
//! 1. 冷分层键（升阶后仅经分层原生删减臂削到双低水位、再无物化型写入）不降阶、
//!    仅由后台评估轮降阶回内存信封（条目维与体积维双流程各一例）；
//! 2. 迟滞死区不误降：体积落 (2MB, 4MB] 的键预筛命中但单点谓词 AND 不放行、
//!    零副作用零版本推进；条目落 (32768, 65536] 的键预筛直读即排除、零候选；
//! 3. WATCH 栅栏：降阶信封写回臂恰一次推进（树清退不双计，对齐
//!    tiered_watch_fence.rs 既有口径），死区轮与零命中轮零推进；
//! 4. 降阶后形态回归 KeyTag::ObjectEnvelope，元记录/树存根释放，数据与计数不丢。

use std::{iter::repeat_n, mem::take, str::from_utf8, sync::Arc, thread::sleep, time::Duration};

use compio::runtime::Runtime;
use itoa::Buffer as ItoaBuffer;
use tempfile::tempdir;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::{
  resp::{
    garnet_api::{GarnetApi, StoreGarnetApi},
    objects::tiered_demote::{TieredDemoteStats, tiered_demote_round},
    resp_server_session::{RespServerSession, RespServerSessionOptions},
  },
  storage::session::storage_session::version_map_watch_hook,
};
use wresp::command::RespCommand;
use wtxn::{TransactionManager, TxnKeyEntryComparison, TxnLockTable, WatchVersionMap};
use wval::{GarnetObjectType, SessionPrefixBuf};

type TestStore = WedbStore<SegmentedDevice>;

/// 测试环境：真实引擎 + 引擎级写面钩子挂载共享版本表（与生产装配同径）
struct Env {
  rt: Runtime,
  store: Arc<TestStore>,
  map: Arc<WatchVersionMap>,
  lock_table: TxnLockTable,
  api: GarnetApi,
  _dir: tempfile::TempDir,
}

fn env_with_page_size(tag: &str, page_size: usize) -> Env {
  let dir = tempdir().unwrap();
  let config = StoreConfig::new(2048, page_size, 16, 0.5).unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag)).unwrap());
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  let map = Arc::new(WatchVersionMap::new(1 << 10));
  assert!(
    store.set_watch_hook(version_map_watch_hook(Arc::clone(&map))),
    "引擎级写面钩子应首次挂载"
  );
  let api: GarnetApi = Arc::new(StoreGarnetApi::new(store.new_session().unwrap()));
  Env {
    rt: Runtime::new().unwrap(),
    store,
    map,
    lock_table: TxnLockTable::new(),
    api,
    _dir: dir,
  }
}

fn env(tag: &str) -> Env {
  env_with_page_size(tag, 1024 * 1024)
}

fn session_with(env: &Env) -> RespServerSession {
  let mut s = RespServerSession::new(1, RespServerSessionOptions::default());
  s.set_garnet_api(env.api.clone());
  s
}

/// 绑定指定逻辑域会话的存储 API（跨租户跨库写入/读回用）
fn api_at(env: &Env, ns: u64, db: u64) -> GarnetApi {
  let sess = env.store.new_session().unwrap();
  assert!(sess.set_context(ns, db), "逻辑域 ({ns},{db}) 应可物化");
  Arc::new(StoreGarnetApi::new(sess))
}

fn session_with_api(api: &GarnetApi) -> RespServerSession {
  let mut s = RespServerSession::new(1, RespServerSessionOptions::default());
  s.set_garnet_api(api.clone());
  s
}

/// 慢路径命令同步求值并回帧字节（与 tiered_watch_fence 同款泵）
fn auto_exec(env: &Env, s: &mut RespServerSession, cmd: RespCommand, args: &[&[u8]]) -> Vec<u8> {
  auto_exec_on(env, &env.api, s, cmd, args)
}

/// [`auto_exec`] 的指定 API 对位（跨域用例绑域会话存储面）
fn auto_exec_on(
  env: &Env,
  api: &GarnetApi,
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
  let out = env.rt.block_on(slow.resolve());
  s.output.clear();
  out
}

/// 分层态判据：BfTree 元记录存根在册（wkv load_collection_stub 权威读）
fn is_tiered(env: &Env, key: &[u8]) -> bool {
  let sess = env.store.new_session().unwrap();
  env
    .rt
    .block_on(sess.load_collection_stub(key))
    .unwrap()
    .is_some()
}

/// [`is_tiered`] 的指定逻辑域对位（域会话装载，根域会话看不到他域元记录）
fn is_tiered_at(env: &Env, ns: u64, db: u64, key: &[u8]) -> bool {
  let sess = env.store.new_session().unwrap();
  assert!(sess.set_context(ns, db));
  env
    .rt
    .block_on(sess.load_collection_stub(key))
    .unwrap()
    .is_some()
}

/// 版本表读点（与 wtxn 校验同一哈希面：根域 scoped，与默认写会话前缀同源）
fn ver(env: &Env, key: &[u8]) -> u64 {
  env
    .map
    .read_version(
      TxnKeyEntryComparison::scoped_key_hash(SessionPrefixBuf::ROOT.as_slice(), key) as u64,
    )
}

/// 新开登记单键 WATCH 的事务管理器（根域归属）
fn watch(env: &Env, key: &[u8]) -> TransactionManager {
  let mut txn = TransactionManager::new(env.lock_table.clone(), Arc::clone(&env.map), None);
  txn.watch(SessionPrefixBuf::ROOT.as_slice(), key);
  txn
}

/// EXEC 校验（失效返回 false）
fn exec(txn: &mut TransactionManager) -> bool {
  txn.run(
    SessionPrefixBuf::ROOT.as_slice(),
    false,
    false,
    Duration::ZERO,
  )
}

/// 哈希键体积维全流程：字节门槛真实升阶 → 分层 HDEL 削到死区仍分层（预筛命中
/// 但单点谓词不放行）→ 削至双维齐低即在前台整值写回臂就地懒降阶、形态与数据
/// 回归信封 → 后续后台轮零候选静默
///
/// 降阶落点自「后台轮」改记为「前台写回臂」是本票写形收敛的直接后果：分层
/// HDEL 已无树内逐成员删除臂，一律物化 → wcol 对象层求值 → apply_rmw_post_operate
/// 整值写回（task/ing/tiered-zset-demote-bf-tree-recursion-stack-overflow.md 主代理
/// 裁决 B），降阶判据因此与其余整值写回命令同点生效；树内不留墓碑正是本票要的
/// 栈深不变量
#[test]
fn hash_byte_dim_deadzone_holds_and_low_dims_demote_at_writeback() {
  let env = env("tbd-hash.db");
  let mut s = session_with(&env);

  // 8000 字段 × 600B ≈ 4.8MB：单条 HSET 越 wcol::TIERED_PROMOTE_BYTES 真实升阶
  let value = vec![b'v'; 600];
  let fields: Vec<Vec<u8>> = (0..8000_usize)
    .map(|i| format!("f{i}").into_bytes())
    .collect();
  let mut args: Vec<&[u8]> = vec![b"h"];
  for f in &fields {
    args.push(f.as_slice());
    args.push(&value);
  }
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hset, &args),
    b":8000\r\n"
  );
  assert!(is_tiered(&env, b"h"), "超字节门槛应升阶为 wbftree 分层态");

  // 削 4000：条目维 4000 ≤ 32768 过预筛，体积 2.4MB ∈ (2MB, 4MB] 落迟滞死区
  let mut hd: Vec<&[u8]> = vec![b"h"];
  hd.extend(fields[..4000].iter().map(Vec::as_slice));
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hdel, &hd),
    b":4000\r\n"
  );
  assert!(
    is_tiered(&env, b"h"),
    "死区（2MB, 4MB] 不放行降阶：整值写回臂按既有迟滞留在分层态（重灌零墓碑）"
  );
  let mut txn = watch(&env, b"h");
  let before = ver(&env, b"h");
  let stats = env.rt.block_on(tiered_demote_round(&env.store));
  assert_eq!(
    stats,
    TieredDemoteStats {
      candidates: 1,
      demoted: 0,
      aborted: 1
    },
    "死区键预筛命中但单点谓词 AND 不放行"
  );
  assert_eq!(
    ver(&env, b"h"),
    before,
    "死区轮零副作用：版本栅栏零推进、零写放大"
  );
  assert!(exec(&mut txn), "死区轮后 WATCH 事务必须仍可提交");
  assert!(is_tiered(&env, b"h"));
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hlen, &[b"h"]),
    b":4000\r\n"
  );

  // 再削 2000 → 2000 × 600B ≈ 1.2MB 且条目 ≤ 32768：双维齐低，前台写回臂即降阶
  let mut hd2: Vec<&[u8]> = vec![b"h"];
  hd2.extend(fields[4000..6000].iter().map(Vec::as_slice));
  let mut txn2 = watch(&env, b"h");
  let before2 = ver(&env, b"h");
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hdel, &hd2),
    b":2000\r\n"
  );
  assert_eq!(
    ver(&env, b"h"),
    before2 + 1,
    "整值写回降阶臂恰一次推进、树清退不双计（一命令一推进）"
  );
  assert!(
    !exec(&mut txn2),
    "前台降阶即客户端可见变更，WATCH 事务必须失效"
  );
  assert!(
    !is_tiered(&env, b"h"),
    "双维齐低的分层 HDEL 在写回臂就地降阶，不留待后台轮"
  );

  let stats2 = env.rt.block_on(tiered_demote_round(&env.store));
  assert_eq!(
    stats2,
    TieredDemoteStats::default(),
    "键已随前台写回降阶：后台轮零候选零扫树"
  );
  assert!(
    !is_tiered(&env, b"h"),
    "降阶后元记录与树存根（含旁表登记）应全部释放"
  );

  // 形态回归内存信封且数据不丢
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Type, &[b"h"]),
    b"+hash\r\n"
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hlen, &[b"h"]),
    b":2000\r\n"
  );
  let mut want = Vec::new();
  want.extend_from_slice(format!("${}\r\n", value.len()).as_bytes());
  want.extend_from_slice(&value);
  want.extend_from_slice(b"\r\n");
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hget, &[b"h", b"f6000"]),
    want
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hget, &[b"h", b"f0"]),
    b"$-1\r\n",
    "已删字段不得借降阶物化复活"
  );

  // 降阶后下轮零候选静默（冷分层键不被反复扫读）
  let stats3 = env.rt.block_on(tiered_demote_round(&env.store));
  assert_eq!(stats3.candidates, 0, "信封态键不入候选清单");
  assert_eq!(ver(&env, b"h"), before2 + 1, "零命中轮零推进");
}

/// ZSet 条目维全流程：计数门槛真实升阶 → 削到 (32768, 65536] 条目死区预筛
/// 零候选零副作用 → 削至 ≤ 32768 双维齐低即在前台整值写回臂就地降阶
#[test]
fn zset_count_dim_deadzone_excluded_and_cold_key_demoted() {
  let env = env("tbd-zset.db");
  let mut s = session_with(&env);

  // 66000 成员（8192 员一批 × 每员配分值 1）：条目数越 wcol::TIERED_PROMOTE_THRESHOLD
  // 升阶。ZADD 入参为 score-member 成对形态（C# SortedSetCommands.cs:SortedSetAdd
  // 与内存对象同源的成对解析），单值带多名会被判为非法分值而整批拒写
  let mut buf = ItoaBuffer::new();
  let mut members: Vec<Vec<u8>> = Vec::with_capacity(66000);
  for i in 0..66000_usize {
    let s = buf.format(i).as_bytes();
    let mut m = Vec::with_capacity(s.len() + 1);
    m.push(b'm');
    m.extend_from_slice(s);
    members.push(m);
  }
  let mut exp = Vec::with_capacity(16);
  for chunk in members.chunks(8192) {
    let mut args: Vec<&[u8]> = Vec::with_capacity(chunk.len() * 2 + 1);
    args.push(b"z");
    for m in chunk {
      args.push(b"1");
      args.push(m.as_slice());
    }
    exp.clear();
    exp.push(b':');
    exp.extend_from_slice(buf.format(chunk.len()).as_bytes());
    exp.extend_from_slice(b"\r\n");
    assert_eq!(
      auto_exec(&env, &mut s, RespCommand::Zadd, &args),
      exp,
      "批量成员入集"
    );
  }
  assert!(is_tiered(&env, b"z"), "条目数越 65536 应升阶");
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Zcard, &[b"z"]),
    b":66000\r\n"
  );

  // 削 26000 → 40000 ∈ (32768, 65536]：条目维预筛直读即排除，零候选
  let mut r1: Vec<&[u8]> = vec![b"z"];
  r1.extend(members[..26000].iter().map(Vec::as_slice));
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Zrem, &r1),
    b":26000\r\n"
  );
  let mut txn = watch(&env, b"z");
  let before = ver(&env, b"z");
  let stats = env.rt.block_on(tiered_demote_round(&env.store));
  assert_eq!(
    stats,
    TieredDemoteStats::default(),
    "条目死区键预筛排除：无候选、无物化扫树、无评估副作用"
  );
  assert_eq!(ver(&env, b"z"), before, "死区轮零推进版本栅栏");
  assert!(exec(&mut txn));
  assert!(is_tiered(&env, b"z"));

  // 再削 30000 → 10000 ≤ 32768 且体积远低于 2MB：双维齐低，前台写回臂就地降阶
  // （本用例末段在收敛前的 dev 形态上正是栈溢出点：ZREM 56000 条前缀在树内留
  // 连跑墓碑，后台轮物化扫描按连跑深度递归爆栈；删除臂改走整值重灌后树内零墓碑）
  let mut r2: Vec<&[u8]> = vec![b"z"];
  r2.extend(members[26000..56000].iter().map(Vec::as_slice));
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Zrem, &r2),
    b":30000\r\n"
  );
  assert!(
    !is_tiered(&env, b"z"),
    "双维齐低即随整值写回臂就地降阶（同哈希体积维用例）"
  );
  let stats2 = env.rt.block_on(tiered_demote_round(&env.store));
  assert_eq!(stats2, TieredDemoteStats::default());
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Zcard, &[b"z"]),
    b":10000\r\n"
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Zscore, &[b"z", b"m65999"]),
    b"$1\r\n1\r\n",
    "存活成员分值经降阶物化保真"
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Zscore, &[b"z", b"m0"]),
    b"$-1\r\n",
    "已删成员不得复活"
  );
}

/// 跨全量域降阶：ns1/db1 租户库与根域 (ns0/db0) 的冷分层键同轮入候选并降阶，
/// 对标 SKILL「同一个 Namespace 的不同 DB 可以是不同槽位」的多租户扫描承诺
/// （历史实现硬编码会话默认域只扫根域前缀，非根域分层键永不降阶）
#[test]
fn cross_domain_cold_keys_demoted() {
  let env = env("tbd-cross.db");
  let mut s = session_with(&env);

  // 「分层态但双维齐低」冷键构造：wkv promote 直接灌（建树不做降阶评估，
  // 同 tiered_promote_demote_ttl 的 background_demote_preserves_key_ttl 先例），
  // 22000 字段远低于双低水位，无前台写触碰，只能由后台轮回收
  let entries = || {
    let mut buf = ItoaBuffer::new();
    let mut list = Vec::with_capacity(22000);
    for i in 0..22000_usize {
      let s = buf.format(i).as_bytes();
      let mut f = Vec::with_capacity(s.len() + 1);
      f.push(b'f');
      f.extend_from_slice(s);
      list.push((f, b"v".to_vec()));
    }
    list
  };
  {
    let sess = env.store.new_session().unwrap();
    assert!(sess.set_context(1, 1), "ns1/db1 逻辑域应可物化");
    env
      .rt
      .block_on(sess.promote_collection_to_bftree(
        b"h",
        GarnetObjectType::Hash,
        entries(),
        i64::MAX,
        false,
      ))
      .unwrap();
  }
  {
    let sess = env.store.new_session().unwrap();
    env
      .rt
      .block_on(sess.promote_collection_to_bftree(
        b"hr",
        GarnetObjectType::Hash,
        entries(),
        i64::MAX,
        false,
      ))
      .unwrap();
  }
  assert!(is_tiered_at(&env, 1, 1, b"h"), "ns1/db1 分层键应已就位");
  assert!(is_tiered(&env, b"hr"), "根域分层键应已就位");

  let stats = env.rt.block_on(tiered_demote_round(&env.store));
  assert_eq!(
    stats.demoted, 2,
    "跨域与根域冷分层键同轮入候选并降阶（非根域键不再被扫描口径吞掉）"
  );
  assert!(
    !is_tiered_at(&env, 1, 1, b"h"),
    "ns1/db1 键元记录与树存根应释放"
  );
  assert!(!is_tiered(&env, b"hr"));

  // ns1/db1 数据不丢：域会话读回计数与成员
  let api1 = api_at(&env, 1, 1);
  let mut s1 = session_with_api(&api1);
  assert_eq!(
    auto_exec_on(&env, &api1, &mut s1, RespCommand::Type, &[b"h"]),
    b"+hash\r\n",
    "ns1/db1 键形态回归内存信封"
  );
  assert_eq!(
    auto_exec_on(&env, &api1, &mut s1, RespCommand::Hlen, &[b"h"]),
    b":22000\r\n"
  );
  assert_eq!(
    auto_exec_on(&env, &api1, &mut s1, RespCommand::Hget, &[b"h", b"f7"]),
    b"$1\r\nv\r\n",
    "跨域降阶后数据保真"
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hlen, &[b"hr"]),
    b":22000\r\n",
    "根域键行为不随跨域扫描改变"
  );
}

/// 死亡域守卫：ns1/db1 分层键经 FLUSHDB 换号退役后，紧缩未回收的旧域滞留
/// Meta 记录不进候选、零副作用零脏写——跨域扫描对死亡域零触碰（与 reclaim
/// 登记面死亡域拒登、compact 紧缩豁免同一纪律，wkv/vdb 换号即退役）
#[test]
fn dead_domain_leftover_records_not_candidates() {
  let env = env("tbd-dead.db");

  // ns1/db1 建「分层态但双维齐低」冷键（wkv promote 直接灌，同跨域用例构造）
  let mut buf = ItoaBuffer::new();
  let mut entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(22000);
  for i in 0..22000_usize {
    let s = buf.format(i).as_bytes();
    let mut f = Vec::with_capacity(s.len() + 1);
    f.push(b'f');
    f.extend_from_slice(s);
    entries.push((f, b"v".to_vec()));
  }
  {
    let sess = env.store.new_session().unwrap();
    assert!(sess.set_context(1, 1), "ns1/db1 逻辑域应可物化");
    env
      .rt
      .block_on(sess.promote_collection_to_bftree(
        b"h",
        GarnetObjectType::Hash,
        entries,
        i64::MAX,
        false,
      ))
      .unwrap();
  }
  assert!(is_tiered_at(&env, 1, 1, b"h"), "ns1/db1 分层键应已就位");

  // FLUSHDB 换号：旧 vdb 退役入 gc_dead，旧域 Meta 记录滞留 hlog 待紧缩回收
  env.rt.block_on(env.store.flush_database(1, 1)).unwrap();
  assert!(
    !is_tiered_at(&env, 1, 1, b"h"),
    "换号后新域为空：旧域键位对新会话不可见"
  );

  // 守卫生效：旧域滞留记录被判死剔除，轮次零候选零副作用
  // （无守卫时该记录会以候选身份物化已回收的旧域树并空转 abort）
  let stats = env.rt.block_on(tiered_demote_round(&env.store));
  assert_eq!(
    stats,
    TieredDemoteStats::default(),
    "死亡域滞留记录不进候选：预筛判死即剔除，不物化不写回"
  );
}

/// 发现一：后台降阶信封超页守卫（envelope_overflow 放弃降阶保持树态）
///
/// 64KB 页配置下总长 72KB（120 字段 × 600B 异值）的 hash 因信封装不下而升阶入树；
/// 后台轮预筛命中（count=120 <= 32768），物化后因载荷超页命中 envelope_overflow 守卫，
/// 放弃降阶保持树态，stats.aborted 递增、stats.demoted 为 0，数据零丢失。
#[test]
fn tiered_demote_envelope_overflow_keeps_tree() {
  let env = env_with_page_size("tbd-overflow.db", 64 * 1024);
  let mut s = session_with(&env);
  let mut buf = ItoaBuffer::new();

  let count = 120_usize;
  let mut args: Vec<Vec<u8>> = Vec::with_capacity(count * 2 + 1);
  args.push(b"h".to_vec());
  let mut expected_f0_val = Vec::new();
  for i in 0..count {
    let f = format!("f{i}").into_bytes();
    let head = buf.format(i % 64);
    let mut v = Vec::with_capacity(600);
    v.extend_from_slice(head.as_bytes());
    v.extend(repeat_n(b'm', 600 - head.len()));
    if i == 0 {
      expected_f0_val = v.clone();
    }
    args.push(f);
    args.push(v);
  }
  let slices: Vec<&[u8]> = args.iter().map(|v| v.as_slice()).collect();
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hset, &slices),
    format!(":{count}\r\n").as_bytes()
  );
  assert!(
    is_tiered(&env, b"h"),
    "120 字段 × 600B ≈ 72KB 超过 64KB 页容量，应经 envelope_overflow 升阶为分层树态"
  );

  let stats = env.rt.block_on(tiered_demote_round(&env.store));
  assert_eq!(
    stats,
    TieredDemoteStats {
      candidates: 1,
      demoted: 0,
      aborted: 1,
    },
    "信封超页守卫命中：放弃降阶保持树态，计入 aborted，demoted 为 0"
  );
  assert!(is_tiered(&env, b"h"), "放弃降阶后必须保持树态");
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hlen, &[b"h"]),
    format!(":{count}\r\n").as_bytes(),
    "键保持可读且条目不丢"
  );
  let got = auto_exec(&env, &mut s, RespCommand::Hget, &[b"h", b"f0"]);
  assert_eq!(
    got,
    format!(
      "${}\r\n{}\r\n",
      expected_f0_val.len(),
      from_utf8(&expected_f0_val).unwrap()
    )
    .into_bytes(),
    "成员内容完全匹配保真"
  );
}

/// 发现二：后台降阶空物化删空自愈（全成员到期冷树闭环回收）
///
/// 升阶 hash 全字段 HEXPIRE 到期后无前台触碰；
/// 后台降阶轮物化剔除到期成员后变为空对象，改走前台空对象臂同一排空单点
/// handle_bftree_drain_and_delete(false) + bump_watch_version 恰一次彻底注销并删空自愈；
/// 断言树文件注销、元记录墓碑、TYPE 回 none、EXISTS 回 0、下一轮候选数归零。
#[test]
fn tiered_demote_empty_materialized_drains_and_deletes() {
  let env = env_with_page_size("tbd-empty-drain.db", 64 * 1024);
  let mut s = session_with(&env);
  let mut buf = ItoaBuffer::new();

  let count = 120_usize;
  let mut args: Vec<Vec<u8>> = Vec::with_capacity(count * 2 + 1);
  args.push(b"h".to_vec());
  for i in 0..count {
    let f = format!("f{i}").into_bytes();
    let head = buf.format(i % 64);
    let mut v = Vec::with_capacity(600);
    v.extend_from_slice(head.as_bytes());
    v.extend(repeat_n(b'm', 600 - head.len()));
    args.push(f);
    args.push(v);
  }
  let slices: Vec<&[u8]> = args.iter().map(|v| v.as_slice()).collect();
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hset, &slices),
    format!(":{count}\r\n").as_bytes()
  );
  assert!(is_tiered(&env, b"h"), "120 字段 × 600B 应升阶为分层树态");

  // 设置全部 120 个字段过期：1 秒后过期
  let count_str = format!("{count}");
  let mut exp_args: Vec<Vec<u8>> = Vec::with_capacity(count + 4);
  exp_args.push(b"h".to_vec());
  exp_args.push(b"1".to_vec());
  exp_args.push(b"FIELDS".to_vec());
  exp_args.push(count_str.into_bytes());
  for i in 0..count {
    exp_args.push(format!("f{i}").into_bytes());
  }
  let exp_slices: Vec<&[u8]> = exp_args.iter().map(|v| v.as_slice()).collect();
  let exp_res = auto_exec(&env, &mut s, RespCommand::Hexpire, &exp_slices);
  assert!(
    exp_res.starts_with(format!("*{count}\r\n").as_bytes()),
    "批量设置 HEXPIRE 成功"
  );
  assert!(is_tiered(&env, b"h"), "设置 HEXPIRE 后仍保持分层树态");

  // 等待字段到期
  sleep(Duration::from_millis(1100));

  // 此后无任何前台命令触碰冷树。手动驱动后台降阶轮：
  let mut txn = watch(&env, b"h");
  let stats = env.rt.block_on(tiered_demote_round(&env.store));
  assert_eq!(
    stats,
    TieredDemoteStats {
      candidates: 1,
      demoted: 1,
      aborted: 0,
    },
    "全成员到期空冷树应由后台轮自愈收敛回收"
  );

  // 栅栏生效：删空自愈推进 WATCH 版本，并发事务必须失效
  assert!(!exec(&mut txn), "删空自愈推进 WATCH 版本，并发事务必须失效");

  // 断言树文件注销、元记录墓碑、TYPE 回 none、EXISTS 回 0
  assert!(!is_tiered(&env, b"h"), "树存根与树文件必须注销释放");
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Type, &[b"h"]),
    b"+none\r\n",
    "删空后 TYPE 应回 none"
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Exists, &[b"h"]),
    b":0\r\n",
    "删空后 EXISTS 应回 0"
  );

  // 下一轮候选数归零
  let stats2 = env.rt.block_on(tiered_demote_round(&env.store));
  assert_eq!(
    stats2,
    TieredDemoteStats::default(),
    "删空自愈后下一轮候选数归零"
  );
}
