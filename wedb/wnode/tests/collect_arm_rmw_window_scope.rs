//! HCOLLECT / ZCOLLECT 同步臂读改写窗口位次回归（票 wnode-hcollect-zcollect-rmwwindow-probe-drop）
//!
//! 缺陷形：`network_hcollect` / `network_zcollect` 逐键臂以
//! `if store.try_rmw_window(key).is_none()` 条件临时值取闩即放——桶闩在紧随的
//! `hash_load_sync` / `zset_load_sync` 之前已随临时值 Drop 释放，装载 → operate
//! → 落笔前终态复验 → 整对象写回全链实际在闩外执行，窗口只充当一次争用探针。
//! 复验核（`obj_writeback_recheck_sync` → `obj_save_recheck_sync`）只判存活域
//! 归属、不判内容版本，窗内他核并发 HSET/ZADD 后信封依旧在场即复验恒过，
//! 旧视图尾段盲写整对象顶掉并发已回写字段。
//!
//! C# 契约（本回归判据来源）：
//! `libs/server/Storage/Session/ObjectStore/Common.cs:ObjectCollect` :807-834 —
//! `collectLock.TryWriteLock()`（:810，失败回 NOTFOUND）覆盖 DbScan → 逐 hashKey
//! `RMWObjectStoreOperation` 的读改写全尾段，`finally WriteUnlock()`（:834）才放
//! 闩；取放闩对尖见 `libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/
//! Implementation/Locking/TransientLocking.cs`:19-34。语义面锚
//! `garnet/test/standalone/Garnet.test/RespHashTests.cs:CanDoHashCollect`
//!（:1286-1331，显式键 HCOLLECT 恒回 OK 且只清过期字段）与
//! `RespSortedSetTests.cs` 的 ZCOLLECT 同款用例（:2081）。
//!
//! 判据为「守恒等式 + 逐帧精确 + 构造型反空转」，全部与调度无关（旧版以
//! MIN_ACKED=60 / MIN_SYNC_ROUNDS=5 / MAX_COLLECT_ROUNDS 门限立判——他核采样
//! 线程会被整时片抢占、[`wkv`] 同步臂取闩自带 1024 轮自旋已吸收短持窗，写者
//! 又可被轮数上限提前炸出，门限计数随负载漂移即 flake 根源，一律废除；
//! rmw_blindwrite_window_matrix 亦以成交序而非概率立判）：
//! 1. 主判据「终态集合守恒」：终态成员多重集必等于 种子 ∪ 已回执新建，每员
//!    恰 1 份——丢份（旧视图尾段盲写顶掉已 ACK 写）与多份/冒出（复活旧视图
//!    已删态）皆判败。修复前对面写者落笔正落在收集臂「装载 → 写回」裸执行
//!    间隙内（其取闩必成，因收集臂根本不持窗），顶掉即丢份——该形状在任何
//!    串行执行序皆不可达；修复后对面写者只能在收集臂放闩后成交（自旋等待或
//!    转异步重放），任何调度下守恒恒成立。
//! 2. 逐条等式而非门限：对面写者每轮新建写必回执 `:1`（新字段/新成员无并发
//!    删除面，串行化下必恰一次新建回执，杜绝旧版「无一成交即空转」的记档
//!    门限）；收集臂每轮应答必逐字节 `+OK`（C# CanDoHashCollect 口径）。
//! 3. 构造型反空转（判据必落在被修的同步臂上）：写者跑满全部轮数退场后，
//!    收集臂再补 [`TAIL_SYNC_ROUNDS`] 轮无争用收尾轮——彼时唯一持闩者已退场、
//!    桶闩唯一可得，同步臂取闩必成（异步对偶臂 `garnet_api/objects.rs:
//!    collect_hash_key`:166 / `collect_sorted_set_key`:250 本就正确持窗，判据
//!    若只落在异步臂即失去位次意义），故「同步臂在手轮数 ≥ 收尾轮数」为因果
//!    强制等式而非调度赌注。

use std::{
  sync::{
    Arc, Barrier,
    atomic::{AtomicBool, Ordering},
  },
  thread,
};

use compio::runtime::Runtime;
use wbase::map::{HashMap, HashMapExt, HashSet, HashSetExt};
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
};
use wtest_base::{open_test_store, resp_frame as frame};

type TestStore = WedbStore<SegmentedDevice>;

/// 种子成员数：同步臂整轮（装载反序列化 → operate 全字段扫过期 → 整值序列化写回）
/// 在 debug 档跑到数百微秒量级，对面写者的落笔密集撞进该时段；信封载荷（≈72KB）
/// 仍低于测试页容量（16MB 预算推导 256KB 页）与 wcol 升阶门限（65536 条目 / 4MB），
/// 确保写回落在同步臂不触发升阶降级
const SEED_MEMBERS: usize = 4000;
/// 种子成员值载荷长度
const VALUE_PAD: usize = 8;
/// 预置分批条目数（单帧参数个数收敛，杜绝超长帧）
const SEED_CHUNK: usize = 512;
/// 对面写者全速串写轮数（每轮一条新建写命令，无间隔、不提前退场：其与收集臂的
/// 抢占比即交叉覆盖面，覆盖是否成立由终态守恒判据如实呈现，不再以门限赌调度）
const OPPONENT_WRITES: usize = 150;
/// 写者退场后收集臂的无争用收尾轮数：反空转判据由构造强制（见文件头判据 3）
const TAIL_SYNC_ROUNDS: usize = 3;

/// 独立连接装配（生产 thread-per-core 形态：对面写者与收集臂各持一份会话）
fn consumer_on(store: &Arc<TestStore>) -> RespSessionConsumer {
  RespSessionConsumer::new(
    1,
    RespServerSessionOptions::default(),
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
  )
}

/// 单命令往返，回 `(应答帧, 是否降级慢路径)`——降级位即同步臂 `Ok(false)` 交出
/// SlowWait 的事实，收集臂据此区分本轮落在同步臂还是异步对偶臂
fn send(rt: &Runtime, c: &mut RespSessionConsumer, args: &[&[u8]]) -> (Vec<u8>, bool) {
  let mut scratch = c.take_recv_scratch();
  scratch.extend_from_slice(&frame(args));
  c.return_recv_scratch(scratch);
  let mut out = Vec::new();
  assert!(
    c.try_consume_messages_into(&mut out).is_some(),
    "帧须被消费循环消化: {args:?}"
  );
  match c.take_slow_wait() {
    Some(slow) => {
      rt.block_on(async { out.extend_from_slice(&slow.resolve().await) });
      (out, true)
    }
    None => (out, false),
  }
}

/// 定长载荷成员名（种子 `s%05d` / 对面写者 `r%05d`，两域前缀互斥）
fn member_name(prefix: &str, index: usize) -> Vec<u8> {
  format!("{prefix}{index:05}").into_bytes()
}

/// 预置 hash：分批 HSET 灌入种子成员（须留在内存信封态，故不 flush——冷化后
/// 同步臂一律磁盘候选降级，判据面即消失）
fn seed_hash(rt: &Runtime, store: &Arc<TestStore>, key: &[u8]) {
  let mut c = consumer_on(store);
  let value = vec![b'x'; VALUE_PAD];
  let all: Vec<usize> = (0..SEED_MEMBERS).collect();
  for (n, chunk) in all.chunks(SEED_CHUNK).enumerate() {
    let fields: Vec<Vec<u8>> = chunk.iter().map(|i| member_name("s", *i)).collect();
    let mut args: Vec<&[u8]> = vec![b"HSET", key];
    for field in &fields {
      args.push(field);
      args.push(&value);
    }
    assert_eq!(
      send(rt, &mut c, &args).0,
      format!(":{}\r\n", chunk.len()).into_bytes(),
      "预置 HSET 第 {n} 批须全量新建"
    );
  }
}

/// 预置 zset（成员分值恒 1，与 [`seed_hash`] 同规模）
fn seed_zset(rt: &Runtime, store: &Arc<TestStore>, key: &[u8]) {
  let mut c = consumer_on(store);
  let all: Vec<usize> = (0..SEED_MEMBERS).collect();
  for (n, chunk) in all.chunks(SEED_CHUNK).enumerate() {
    let members: Vec<Vec<u8>> = chunk.iter().map(|i| member_name("s", *i)).collect();
    let mut args: Vec<&[u8]> = vec![b"ZADD", key];
    for member in &members {
      args.push(b"1");
      args.push(member);
    }
    assert_eq!(
      send(rt, &mut c, &args).0,
      format!(":{}\r\n", chunk.len()).into_bytes(),
      "预置 ZADD 第 {n} 批须全量新建"
    );
  }
}

/// 双核真并发一趟：对面写者线程全速串写同一键跑满 [`OPPONENT_WRITES`] 轮（每轮
/// 一条新建写命令、逐轮断言 `:1` 回执，绝不提前退场）；收集臂线程跑
/// `collect key` 至写者退场，再补 [`TAIL_SYNC_ROUNDS`] 轮无争用收尾轮。
/// `opponent(round)` 回 `(写命令帧, 该轮新建成员名)`；
/// 回 `(收集臂总轮数, 同步臂在手轮数, 已回执成员集)`
fn run_interleaved_writes(
  collect: &'static [u8],
  key: &'static [u8],
  store: &Arc<TestStore>,
  opponent: impl Fn(usize) -> (Vec<Vec<u8>>, Vec<u8>) + Send + 'static,
) -> (usize, usize, HashSet<Vec<u8>>) {
  let gate = Arc::new(Barrier::new(2));
  let done = Arc::new(AtomicBool::new(false));

  let writer = {
    let store = Arc::clone(store);
    let gate = Arc::clone(&gate);
    let done = Arc::clone(&done);
    thread::spawn(move || {
      let rt = Runtime::new().unwrap();
      let mut c = consumer_on(&store);
      gate.wait();
      let mut acked: HashSet<Vec<u8>> = HashSet::new();
      for round in 0..OPPONENT_WRITES {
        let (args, member) = opponent(round);
        let slices: Vec<&[u8]> = args.iter().map(Vec::as_slice).collect();
        // 逐轮等式而非采样计数：新建写无并发删除面，串行化下必恰一次 `:1`
        assert_eq!(
          send(&rt, &mut c, &slices).0,
          b":1\r\n",
          "对面写者第 {round} 轮新建写须回执 :1"
        );
        acked.insert(member);
      }
      done.store(true, Ordering::Release);
      acked
    })
  };

  let collector = {
    let store = Arc::clone(store);
    let gate = Arc::clone(&gate);
    let done = Arc::clone(&done);
    thread::spawn(move || {
      let rt = Runtime::new().unwrap();
      let mut c = consumer_on(&store);
      gate.wait();
      let mut rounds = 0usize;
      let mut sync_rounds = 0usize;
      let mut one_round = || {
        let (resp, slow) = send(&rt, &mut c, &[collect, key]);
        assert_eq!(
          resp, b"+OK\r\n",
          "收集臂应答须恒 +OK（C# CanDoHashCollect 口径）"
        );
        rounds += 1;
        sync_rounds += usize::from(!slow);
      };
      while !done.load(Ordering::Acquire) {
        one_round();
      }
      // 无争用收尾段：写者已退场置位 done 才补轮，此刻桶闩唯一可得、同步臂
      // 取闩必成——反空转判据由构造强制而非调度赌注（见文件头判据 3）
      for _ in 0..TAIL_SYNC_ROUNDS {
        one_round();
      }
      (rounds, sync_rounds)
    })
  };

  let acked = writer.join().expect("对面写者线程");
  let (rounds, sync_rounds) = collector.join().expect("收集臂线程");
  (rounds, sync_rounds, acked)
}

/// RESP2 bulk 数组解析（HKEYS / ZRANGE 终态对照用）
fn parse_bulk_array(resp: &[u8]) -> Vec<Vec<u8>> {
  fn line_end(buf: &[u8]) -> usize {
    buf.iter().position(|&b| b == b'\r').expect("bulk 帧长度行")
  }
  assert_eq!(
    resp.first(),
    Some(&b'*'),
    "终态对照须为 bulk 数组帧: {:?}",
    String::from_utf8_lossy(&resp[..resp.len().min(48)])
  );
  let nl = line_end(&resp[1..]);
  let count: usize = String::from_utf8_lossy(&resp[1..1 + nl])
    .parse()
    .expect("数组头计数");
  let mut items = Vec::with_capacity(count);
  let mut pos = 1 + nl + 2;
  for _ in 0..count {
    assert_eq!(resp[pos], b'$', "元素须为 bulk string 帧");
    let nl = line_end(&resp[pos + 1..]);
    let len: i64 = String::from_utf8_lossy(&resp[pos + 1..pos + 1 + nl])
      .parse()
      .expect("元素长度");
    pos += 1 + nl + 2;
    if len >= 0 {
      let len = len as usize;
      items.push(resp[pos..pos + len].to_vec());
      pos += len + 2;
    }
  }
  items
}

/// 双核交叉 → 终态守恒主判据 + 同步臂构造成手判据（HCOLLECT / ZCOLLECT 两臂
/// 共用形态）：终态成员多重集必等于 种子 ∪ 已回执、每员恰 1 份；
/// 收集臂同步臂在手轮数必 ≥ 无争用收尾轮数
fn assert_collect_arm_serializes_concurrent_writes(
  collect: &'static [u8],
  key: &'static [u8],
  read_all: &'static [&'static [u8]],
  store: &Arc<TestStore>,
  opponent: impl Fn(usize) -> (Vec<Vec<u8>>, Vec<u8>) + Send + 'static,
) {
  let (rounds, sync_rounds, acked) = run_interleaved_writes(collect, key, store, opponent);
  let name = String::from_utf8_lossy(collect);

  // 判据 3（构造强制）：收尾轮无争用、取闩必成，同步臂在手轮数必 ≥ 收尾轮数
  assert!(
    sync_rounds >= TAIL_SYNC_ROUNDS,
    "{name} 同步臂在手 {sync_rounds} 轮（总 {rounds} 轮）< 收尾轮数 \
     {TAIL_SYNC_ROUNDS}：写者退场后无争用收尾轮竟仍降级转异步，判据未落在被修的\
     同步臂上（异步对偶臂 collect_hash_key/collect_sorted_set_key 本就正确持窗，\
     会掩盖位次判据）"
  );

  // 判据 1（主·守恒等式）：终态多重集 = 种子 ∪ 已回执，每员恰 1 份
  let rt = Runtime::new().unwrap();
  let mut c = consumer_on(store);
  let alive = parse_bulk_array(&send(&rt, &mut c, read_all).0);
  let mut tally: HashMap<&[u8], usize> = HashMap::new();
  for member in &alive {
    *tally.entry(member.as_slice()).or_insert(0) += 1;
  }
  let mut expected: Vec<Vec<u8>> = (0..SEED_MEMBERS).map(|i| member_name("s", i)).collect();
  expected.extend(acked.iter().cloned());
  let expected_set: HashSet<&[u8]> = expected.iter().map(Vec::as_slice).collect();
  let mut lost: Vec<&[u8]> = expected_set
    .iter()
    .copied()
    .filter(|m| !tally.contains_key(m))
    .collect();
  lost.sort();
  assert!(
    lost.is_empty(),
    "已回执的并发写（含全部种子成员）被 {name} 旧视图写回顶掉 {} 个（首例 {:?}）：\
     装载-写回全程无窗，该形状在任何串行序不可达",
    lost.len(),
    String::from_utf8_lossy(lost[0]),
  );
  let offending: Vec<(&[u8], usize)> = tally
    .iter()
    .filter(|(member, count)| **count != 1 || !expected_set.contains(*member))
    .map(|(member, count)| (*member, *count))
    .collect();
  assert!(
    offending.is_empty(),
    "{name} 交叉跑终态多重集不守恒（总 {} 帧，异常 {} 员）：份数≠1 或冒出期望外\
     成员即旧视图写回复活/覆写面，首例 {offending:?}",
    alive.len(),
    offending.len(),
  );
}

/// HCOLLECT 同步臂持窗跨装载-复验-写回全程：与并发 HSET 双核交叉，终态必为
/// 种子 ∪ 已回执字段（票面判据，对标 C# ObjectCollect 写锁全尾段）
#[test]
fn hcollect_concurrent_hset_serializes_behind_window() {
  let (_dir, store) = open_test_store("hcollect-hset-window-scope.db").unwrap();
  let rt = Runtime::new().unwrap();
  seed_hash(&rt, &store, b"cw:hash");
  assert_collect_arm_serializes_concurrent_writes(
    b"HCOLLECT",
    b"cw:hash",
    &[b"HKEYS", b"cw:hash"],
    &store,
    move |round| {
      let field = member_name("r", round);
      (
        vec![
          b"HSET".to_vec(),
          b"cw:hash".to_vec(),
          field.clone(),
          vec![b'v'; VALUE_PAD],
        ],
        field,
      )
    },
  );
}

/// ZCOLLECT 同步臂同款（两臂对称）：并发 ZADD 交叉，终态必为种子 ∪ 已回执成员
#[test]
fn zcollect_concurrent_zadd_serializes_behind_window() {
  let (_dir, store) = open_test_store("zcollect-zadd-window-scope.db").unwrap();
  let rt = Runtime::new().unwrap();
  seed_zset(&rt, &store, b"cw:zset");
  assert_collect_arm_serializes_concurrent_writes(
    b"ZCOLLECT",
    b"cw:zset",
    &[b"ZRANGE", b"cw:zset", b"0", b"-1"],
    &store,
    move |round| {
      let member = member_name("r", round);
      (
        vec![
          b"ZADD".to_vec(),
          b"cw:zset".to_vec(),
          b"1".to_vec(),
          member.clone(),
        ],
        member,
      )
    },
  );
}
