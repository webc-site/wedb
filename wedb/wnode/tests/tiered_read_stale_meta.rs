//! 分层纯读臂元记录锁内刷新回归（票 wnode-tieredread-stalemeta）
//!
//! 缺陷形：LRANGE / LLEN / LINDEX / SCARD / SRANDMEMBER 五读臂的 (meta, stub)
//! 在锁外装载（rmw_helpers 路由探测）、[`tiered_guard`] 读臂取共享读锁但不刷新
//! 元记录——并发 LPOP / RPOP / LREM / LTRIM（物化整值重灌换树并回写 meta）在
//! 「装载之后、取锁之前」完成时，读臂持陈旧 size 配注册表现行树：LRANGE 帧头
//! 按陈旧 count 预写、实扫不足即撤帧上抛 RESP_ERR_SLOW_PATH_STORAGE（有效读折
//! 存储错误）；LLEN / SCARD 应答装载时刻陈旧值与同窗内容读互斥；LINDEX 以陈旧
//! len 折算判界在增长窗回假 null。
//!
//! 修法判据（本文件锁定）：
//! - [`tiered_guard`] 读臂锁内刷新（与 tiered_count 同一刷新序单点共用）：读锁
//!   与写臂「树写 → meta 回写」全程窗口互斥，刷新值与树内容同源——增长窗内
//!   LLEN ≤ LRANGE 条数 ≤ 后采样 LLEN（单调负载逐样本可验）、LINDEX 定位内容
//!   精确、SCARD ≤ SMEMBERS 成员数 ≤ 后采样 SCARD；
//! - 读臂刷新遇迁移 claim（`Err(MigrationBusy)`）**回退装载快照照常执行**：
//!   读面忙拒面不扩大；该回退同一裁决延展到路由装载门（`try_tiered_arm` 对
//!   共享读锁臂 `needs_write == false` 的回退未门禁装载），LRANGE / LLEN /
//!   LINDEX / HLEN 在 claim 窗内应答合法数据帧；写臂与写锁臂（HGETALL）
//!   既有忙拒口径不动（claim 窗内一律存储错误帧）；
//! - LRANGE 帧头预留-回填单源（wresp::ext，与 SMEMBERS 同形）：实际出帧计数
//!   落头，残余失配降级为合法前缀应答，无半帧残留。
//!
//! 并发形态：真实多线程 × 独立 compio Runtime × [`std::sync::Barrier`] 起跑
//! 对齐，逐应答断言不变量，join 后终态一致性收口——无 sleep、无调度碰运气。

use std::{
  mem::take,
  str::from_utf8,
  sync::{Arc, Barrier},
  thread,
};

use compio::runtime::Runtime;
use wbase::map::HashSet;
use wcol::{
  SET_MEMBER_DUMMY_VALUE,
  types::{garnet_object::LIST_SEQ_BASE, member_ttl::encode_member},
};
use wkv::Error as WkvError;
use wnode::resp::{
  garnet_api::{GarnetApi, StoreGarnetApi},
  resp_server_session::{RespServerSession, RespServerSessionOptions},
};
use wnode_test::{TestEnv, TestStore, tiered_env};
use wresp::command::RespCommand;
use wval::{GarnetObjectType, MetaValue};

struct Conn {
  api: GarnetApi,
  s: RespServerSession,
}

/// 独立连接装配（生产 thread-per-core 形态：并发各方各持一份会话）
fn conn_on_store(store: &Arc<TestStore>) -> Conn {
  let api: GarnetApi = Arc::new(StoreGarnetApi::new(store.new_session().unwrap()));
  let mut s = RespServerSession::new(1, RespServerSessionOptions::default());
  s.set_garnet_api(api.clone());
  Conn { api, s }
}

fn conn_on(env: &TestEnv) -> Conn {
  conn_on_store(&env.store)
}

impl Conn {
  /// 单命令往返：同步段无输出且挂起慢路径时，以 block_on 承担网络泵角色闭环
  async fn exec(&mut self, cmd: RespCommand, args: &[&[u8]]) -> Vec<u8> {
    self.s.output.clear();
    self.api.exec(&mut self.s, cmd, args);
    if !self.s.output.is_empty() {
      return take(&mut self.s.output);
    }
    let slow = self
      .s
      .take_slow_wait()
      .unwrap_or_else(|| panic!("命令 {cmd} 无输出且未挂起慢路径"));
    slow.resolve().await
  }
}

/// 手工升阶（entries 与 `IGarnetObject::export_entries` 同构；分层态立即可验）
fn promote(
  env: &TestEnv,
  key: &[u8],
  obj_type: GarnetObjectType,
  entries: Vec<(Vec<u8>, Vec<u8>)>,
) {
  let sess = env.store.new_session().unwrap();
  env
    .rt
    .block_on(sess.promote_collection_to_bftree(key, obj_type, entries, i64::MAX, false))
    .unwrap();
  assert!(
    env
      .rt
      .block_on(sess.load_collection_stub(key))
      .unwrap()
      .is_some(),
    "键应处于 wbftree 分层态"
  );
}

/// 分层列表 N 元素（e0..eN-1，序号自 LIST_SEQ_BASE 连续排布）
fn seed_list(env: &TestEnv, key: &[u8], n: u64) {
  promote(
    env,
    key,
    GarnetObjectType::List,
    (0..n)
      .map(|i| {
        (
          (LIST_SEQ_BASE + i as u128).to_be_bytes().to_vec(),
          encode_member(format!("e{i}").as_bytes(), None),
        )
      })
      .collect(),
  );
}

/// 分层集合 N 成员（s0..sN-1）
fn seed_set(env: &TestEnv, key: &[u8], n: u64) {
  promote(
    env,
    key,
    GarnetObjectType::Set,
    (0..n)
      .map(|i| {
        (
          format!("s{i}").into_bytes(),
          SET_MEMBER_DUMMY_VALUE.to_vec(),
        )
      })
      .collect(),
  );
}

/// 分层哈希 N 字段（f0..fN-1 = v0..vN-1）
fn seed_hash(env: &TestEnv, key: &[u8], n: u64) {
  promote(
    env,
    key,
    GarnetObjectType::Hash,
    (0..n)
      .map(|i| {
        (
          format!("f{i}").into_bytes(),
          encode_member(format!("v{i}").as_bytes(), None),
        )
      })
      .collect(),
  );
}

/// `:N\r\n` 整数应答解析
fn reply_int(resp: &[u8]) -> Option<i64> {
  let body = resp.strip_prefix(b":")?.strip_suffix(b"\r\n")?;
  from_utf8(body).ok()?.parse().ok()
}

/// 单条 bulk 应答解析（`$-1` / `_` 回 None = null）
fn reply_bulk(resp: &[u8]) -> Option<Vec<u8>> {
  let rest = resp.strip_prefix(b"$")?;
  let end = rest
    .windows(2)
    .position(|w| w == b"\r\n")
    .filter(|&i| i + 2 <= rest.len())?;
  let len: usize = from_utf8(&rest[..end]).ok()?.parse().ok()?;
  Some(rest.get(end + 2..)?.get(..len)?.to_vec())
}

/// 批量 bulk 数组应答解析（元素带序：内容级对照用）
fn reply_array(resp: &[u8]) -> Option<Vec<Vec<u8>>> {
  let rest = resp.strip_prefix(b"*")?;
  let end = rest.windows(2).position(|w| w == b"\r\n")?;
  let n: usize = from_utf8(rest.get(..end)?).ok()?.parse().ok()?;
  let mut out = Vec::with_capacity(n);
  let mut cur = rest.get(end + 2..)?;
  for _ in 0..n {
    // 元素行形如 `$len\r\n<bytes>\r\n`：先剥 `$` 与长度行再取载荷
    let item = cur.strip_prefix(b"$")?;
    let len_end = item.windows(2).position(|w| w == b"\r\n")?;
    let len: usize = from_utf8(item.get(..len_end)?).ok()?.parse().ok()?;
    let body = len_end + 2;
    out.push(item.get(body..body + len)?.to_vec());
    cur = item.get(body + len + 2..)?;
  }
  Some(out)
}

/// 应答帧合法性门（本文件核心不变量：有效读绝不折存储错误帧）
fn assert_not_err(resp: &[u8], ctx: &str) {
  assert!(
    !resp.starts_with(b"-ERR "),
    "{ctx} 收到存储错误帧（有效读被折叠）: {}",
    String::from_utf8_lossy(resp)
  );
}

/// 场景一（核心竞态压测）：并发 LPOP（物化降级换树回写 meta）× LRANGE 0 -1
/// 轮询——任一交错下 LRANGE 都不得收存储错误帧，元素必属已知字母表；
/// 终态「已弹出集合 ∪ 剩余列表 == 全部灌入元素」逐元素闭合
#[test]
fn concurrent_lpop_lrange_never_storage_error() {
  const SEED: u64 = 64;
  const REFILL: u64 = 32;
  const POPS: usize = 160;
  const READS: usize = 320;
  let env = tiered_env("trsm-lpop-lrange.db");
  seed_list(&env, b"lq", SEED);
  let barrier = Arc::new(Barrier::new(2));

  let popper = {
    let store = Arc::clone(&env.store);
    let barrier = Arc::clone(&barrier);
    thread::spawn(move || {
      let rt = Runtime::new().unwrap();
      let mut c = conn_on_store(&store);
      let mut pushed: HashSet<Vec<u8>> = (0..SEED).map(|i| format!("e{i}").into_bytes()).collect();
      let mut popped = HashSet::default();
      let mut refills = 0u64;
      rt.block_on(async {
        barrier.wait();
        for _ in 0..POPS {
          // 保底不删空（删空自愈会回收分层态，压测对象是分层稳态读面）
          let len = reply_int(&c.exec(RespCommand::Llen, &[b"lq"]).await).unwrap();
          if len <= 8 {
            for k in 0..REFILL {
              let v = format!("r{refills}-{k}").into_bytes();
              pushed.insert(v.clone());
              reply_int(&c.exec(RespCommand::Rpush, &[b"lq", &v]).await).unwrap();
            }
            refills += 1;
          }
          let out = c.exec(RespCommand::Lpop, &[b"lq"]).await;
          assert_not_err(&out, "LPOP");
          popped.insert(reply_bulk(&out).unwrap());
        }
      });
      (pushed, popped)
    })
  };

  let reader = {
    let store = Arc::clone(&env.store);
    let barrier = Arc::clone(&barrier);
    thread::spawn(move || {
      let rt = Runtime::new().unwrap();
      let mut c = conn_on_store(&store);
      rt.block_on(async {
        barrier.wait();
        for i in 0..READS {
          let out = c.exec(RespCommand::Lrange, &[b"lq", b"0", b"-1"]).await;
          assert_not_err(&out, "并发 LPOP 压测窗内 LRANGE");
          let items = reply_array(&out).unwrap_or_else(|| {
            panic!(
              "LRANGE 第 {i} 轮应答必须是批量数组: {}",
              String::from_utf8_lossy(&out)
            )
          });
          for v in &items {
            assert!(
              v.starts_with(b"e") || v.starts_with(b"r"),
              "LRANGE 出帧越界元素 {v:?}"
            );
          }
        }
      });
    })
  };

  let (pushed, popped) = popper.join().unwrap();
  reader.join().unwrap();

  // 终态逐元素闭合：弹出 ∪ 剩余 == 灌入，且两侧无交
  let rt = &env.rt;
  let mut c = conn_on(&env);
  let rest = reply_array(&rt.block_on(c.exec(RespCommand::Lrange, &[b"lq", b"0", b"-1"]))).unwrap();
  let mut union = popped;
  for v in &rest {
    assert!(union.insert(v.clone()), "元素 {v:?} 同时在弹出集与剩余列表");
  }
  assert_eq!(union, pushed, "弹出 ∪ 剩余 != 灌入全集");
}

/// 场景二：增长窗内 LLEN 与 LRANGE 条数、SCARD 与 SMEMBERS 成员数逐样本一致
///（单调负载可验判据：先采样计数 ≤ 内容读条数 ≤ 后采样计数）；SRANDMEMBER
/// 抽样域随刷新 size 收敛；终态计数与内容精确相等
#[test]
fn growth_window_count_matches_content() {
  const SEED: u64 = 16;
  const PUSHES: u64 = 200;
  const ROUNDS: usize = 300;
  let env = tiered_env("trsm-growth-consistency.db");
  seed_list(&env, b"ll", SEED);
  seed_set(&env, b"ss", SEED);
  let canonical: Vec<Vec<u8>> = (0..SEED + PUSHES)
    .map(|i| {
      if i < SEED {
        format!("e{i}")
      } else {
        format!("w{}", i - SEED)
      }
      .into_bytes()
    })
    .collect();
  let known: HashSet<Vec<u8>> = (0..SEED + PUSHES)
    .map(|i| format!("s{i}").into_bytes())
    .collect();

  // 列表侧：RPUSH 单调增长 × LRANGE 内容级前缀对照
  {
    let store = Arc::clone(&env.store);
    let barrier = Arc::new(Barrier::new(2));
    let writer = {
      let store = Arc::clone(&store);
      let barrier = Arc::clone(&barrier);
      thread::spawn(move || {
        let rt = Runtime::new().unwrap();
        let mut c = conn_on_store(&store);
        rt.block_on(async {
          barrier.wait();
          for i in 0..PUSHES {
            let v = format!("w{i}").into_bytes();
            reply_int(&c.exec(RespCommand::Rpush, &[b"ll", &v]).await).unwrap();
          }
        });
      })
    };
    let reader = {
      let store = Arc::clone(&store);
      let canonical = canonical.clone();
      let barrier = Arc::clone(&barrier);
      thread::spawn(move || {
        let rt = Runtime::new().unwrap();
        let mut c = conn_on_store(&store);
        rt.block_on(async {
          barrier.wait();
          for _ in 0..ROUNDS {
            let l1 = reply_int(&c.exec(RespCommand::Llen, &[b"ll"]).await).unwrap();
            let out = c.exec(RespCommand::Lrange, &[b"ll", b"0", b"-1"]).await;
            assert_not_err(&out, "增长窗 LRANGE");
            let items = reply_array(&out).unwrap();
            let l2 = reply_int(&c.exec(RespCommand::Llen, &[b"ll"]).await).unwrap();
            assert!(
              (l1 as usize..=l2 as usize).contains(&items.len()),
              "LRANGE 条数 {} 越出先采样 LLEN {l1} 与后采样 LLEN {l2} 的单调界",
              items.len()
            );
            for (pos, v) in items.iter().enumerate() {
              assert_eq!(v, &canonical[pos], "LRANGE 第 {pos} 位内容偏离规范序");
            }
          }
        });
      })
    };
    writer.join().unwrap();
    reader.join().unwrap();
  }

  // 集合侧：SADD 单调增长 × SMEMBERS 成员数单调界 + SRANDMEMBER 抽样域
  {
    let store = Arc::clone(&env.store);
    let barrier = Arc::new(Barrier::new(2));
    let writer = {
      let store = Arc::clone(&store);
      let barrier = Arc::clone(&barrier);
      thread::spawn(move || {
        let rt = Runtime::new().unwrap();
        let mut c = conn_on_store(&store);
        rt.block_on(async {
          barrier.wait();
          for i in 0..PUSHES {
            let m = format!("s{}", SEED + i).into_bytes();
            reply_int(&c.exec(RespCommand::Sadd, &[b"ss", &m]).await).unwrap();
          }
        });
      })
    };
    let reader = {
      let store = Arc::clone(&store);
      let barrier = Arc::clone(&barrier);
      thread::spawn(move || {
        let rt = Runtime::new().unwrap();
        let mut c = conn_on_store(&store);
        rt.block_on(async {
          barrier.wait();
          for _ in 0..ROUNDS {
            let c1 = reply_int(&c.exec(RespCommand::Scard, &[b"ss"]).await).unwrap();
            let out = c.exec(RespCommand::Smembers, &[b"ss"]).await;
            assert_not_err(&out, "增长窗 SMEMBERS");
            let members = reply_array(&out).unwrap();
            let c2 = reply_int(&c.exec(RespCommand::Scard, &[b"ss"]).await).unwrap();
            assert!(
              (c1 as usize..=c2 as usize).contains(&members.len()),
              "SMEMBERS 成员数 {} 越出 SCARD 先后采样界 [{c1}, {c2}]",
              members.len()
            );
            let mut distinct = HashSet::default();
            for m in &members {
              assert!(known.contains(m), "SMEMBERS 越界成员 {m:?}");
              assert!(distinct.insert(m.clone()), "SMEMBERS 重复成员 {m:?}");
            }
            // 抽样域：count>0 出帧 ≤ count 且恒属已知成员（钳 min(count, size)）
            let out = c.exec(RespCommand::Srandmember, &[b"ss", b"5"]).await;
            assert_not_err(&out, "增长窗 SRANDMEMBER");
            let sample = reply_array(&out).unwrap();
            assert!(sample.len() <= 5);
            for m in &sample {
              assert!(known.contains(m), "SRANDMEMBER 越界成员 {m:?}");
            }
          }
        });
      })
    };
    writer.join().unwrap();
    reader.join().unwrap();
  }

  // 终态精确一致（静止点：计数 == 内容条数 == 规范全集）
  let rt = &env.rt;
  let mut c = conn_on(&env);
  let llen = reply_int(&rt.block_on(c.exec(RespCommand::Llen, &[b"ll"]))).unwrap();
  let larr = reply_array(&rt.block_on(c.exec(RespCommand::Lrange, &[b"ll", b"0", b"-1"]))).unwrap();
  assert_eq!(llen as usize, canonical.len());
  assert_eq!(larr, canonical);
  let scard = reply_int(&rt.block_on(c.exec(RespCommand::Scard, &[b"ss"]))).unwrap();
  let smem = reply_array(&rt.block_on(c.exec(RespCommand::Smembers, &[b"ss"]))).unwrap();
  assert_eq!(scard as usize, smem.len());
  assert_eq!(
    smem.len(),
    canonical.len(),
    "SCARD/SMEMBERS 终态计数应与灌入数一致"
  );
}

/// 场景三：LINDEX 增长窗不回假 null 且定位内容精确（probe 位元素一经 LLEN
/// 可见即永存——单调负载下「LLEN > probe ⇒ LINDEX probe 必为该位元素」）
#[test]
fn lindex_never_false_null_in_growth_window() {
  const SEED: u64 = 4;
  const PUSHES: u64 = 200;
  let env = tiered_env("trsm-lindex-growth.db");
  seed_list(&env, b"li", SEED);
  let barrier = Arc::new(Barrier::new(2));

  let writer = {
    let store = Arc::clone(&env.store);
    let barrier = Arc::clone(&barrier);
    thread::spawn(move || {
      let rt = Runtime::new().unwrap();
      let mut c = conn_on_store(&store);
      rt.block_on(async {
        barrier.wait();
        for i in 0..PUSHES {
          let v = format!("w{i}").into_bytes();
          reply_int(&c.exec(RespCommand::Rpush, &[b"li", &v]).await).unwrap();
        }
      });
    })
  };

  let reader = {
    let store = Arc::clone(&env.store);
    let barrier = Arc::clone(&barrier);
    thread::spawn(move || {
      let rt = Runtime::new().unwrap();
      let mut c = conn_on_store(&store);
      rt.block_on(async {
        barrier.wait();
        let total = SEED + PUSHES;
        let mut probe = SEED;
        let mut rounds = 0usize;
        while probe < total {
          rounds += 1;
          assert!(rounds < 200_000, "LINDEX 探测推进失活（写者停滞）");
          let len = reply_int(&c.exec(RespCommand::Llen, &[b"li"]).await).unwrap();
          if len as u64 <= probe {
            continue;
          }
          let out = c
            .exec(
              RespCommand::Lindex,
              &[b"li", &probe.to_string().into_bytes()],
            )
            .await;
          assert_not_err(&out, "增长窗 LINDEX");
          let expected = if probe < SEED {
            format!("e{probe}")
          } else {
            format!("w{}", probe - SEED)
          };
          assert_eq!(
            reply_bulk(&out)
              .unwrap_or_else(|| panic!("LINDEX {probe} 假 null：LLEN={len} 已见该位")),
            expected.into_bytes(),
            "LINDEX {probe} 定位内容偏离"
          );
          probe += 1;
        }
        // 尾端负索引对照
        let last = c.exec(RespCommand::Lindex, &[b"li", b"-1"]).await;
        assert_eq!(
          reply_bulk(&last).unwrap(),
          format!("w{}", PUSHES - 1).into_bytes()
        );
      });
    })
  };
  writer.join().unwrap();
  reader.join().unwrap();
}

/// 场景四（确定性）：迁移 claim 在册窗的忙拒口径与零损伤回归——路由装载门
/// （load_collection_stub）对五读臂、HLEN / HGET 与 HRANDFIELD 树内只读抽样臂
/// （票 zcode-r151c-smembers 案一三形：无 count/正负 count/WITHVALUES + count
/// 0 短路）一律回退装载快照合法应答，出读与同窗 HGET 逐字节同构；HGETALL 与
/// 写臂保持既有存储错误帧忙拒口径。claim 释放后内容原态完整、计数精确
/// （claim 门零数据损伤）。claim 以 wbftree 公开判据原语手工登记/释放，
/// 窗界确定性可控
#[test]
fn migration_claim_window_busy_contract() {
  let env = tiered_env("trsm-claim-busy.db");
  seed_list(&env, b"lq", 8);
  seed_hash(&env, b"hh", 3);
  let rt = &env.rt;

  let list_meta = {
    let sess = env.store.new_session().unwrap();
    sess.session_meta_key(b"lq")
  };
  let hash_meta = {
    let sess = env.store.new_session().unwrap();
    sess.session_meta_key(b"hh")
  };
  let mgr = env.store.range_index();
  assert!(
    mgr.try_claim_migration(&list_meta),
    "列表键 claim 应登记成功"
  );
  assert!(
    mgr.try_claim_migration(&hash_meta),
    "哈希键 claim 应登记成功"
  );
  assert!(mgr.migration_claimed(&list_meta));

  {
    let mut c = conn_on(&env);
    // claim 窗内：读臂（共享读锁面）回退装载快照照常执行——换入前旧树内容
    // 自洽，应答合法数据帧（tiered_guard 读臂回退的同一裁决延展到路由门，
    // 票 wnode-tieredread-stalemeta 验收「轮询读零存储错误帧」）；写锁臂
    //（HGETALL）与写臂保持既有忙拒口径（存储错误帧）
    let out = rt.block_on(c.exec(RespCommand::Lrange, &[b"lq", b"0", b"-1"]));
    let items = reply_array(&out).unwrap_or_else(|| {
      panic!(
        "claim 窗 LRANGE 应回退照常应答批量数组: {}",
        String::from_utf8_lossy(&out)
      )
    });
    assert_eq!(items.len(), 8, "claim 窗 LRANGE 应按装载快照出全量");
    assert_eq!(
      reply_int(&rt.block_on(c.exec(RespCommand::Llen, &[b"lq"]))),
      Some(8),
      "claim 窗 LLEN 应回退照常应答计数"
    );
    assert_eq!(
      reply_bulk(&rt.block_on(c.exec(RespCommand::Lindex, &[b"lq", b"0"]))).unwrap(),
      b"e0".to_vec(),
      "claim 窗 LINDEX 应回退照常应答定位"
    );
    assert_eq!(
      reply_int(&rt.block_on(c.exec(RespCommand::Hlen, &[b"hh"]))),
      Some(3),
      "claim 窗 HLEN（读计数臂）应回退照常应答计数"
    );
    let out = rt.block_on(c.exec(RespCommand::Hgetall, &[b"hh"]));
    assert!(
      out.starts_with(b"-ERR "),
      "claim 窗 HGETALL（写锁臂）应保持既有忙拒口径（存储错误帧），实际: {}",
      String::from_utf8_lossy(&out)
    );
    // ---- HRANDFIELD 树内只读抽样臂三形（票 zcode-r151c-smembers 案一）：
    // 共享读锁臂，claim 窗内回退装载快照照常执行、零存储错误帧（路由装载门
    // load_collection_stub_for_read 回退判据，lposrank 案一同窗收口对偶）；
    // 出读与快照 HGET 逐字节同构（deviations 登记：对照基线为 HGET——
    // HGETALL 系写锁臂在册忙拒，非同构面）。
    let known: HashSet<Vec<u8>> = ["f0", "f1", "f2"]
      .iter()
      .map(|s| s.as_bytes().to_vec())
      .collect();
    let out = rt.block_on(c.exec(RespCommand::Hrandfield, &[b"hh"]));
    assert_not_err(&out, "claim 窗 HRANDFIELD 无 count 形");
    let f = reply_bulk(&out).unwrap_or_else(|| {
      panic!(
        "claim 窗 HRANDFIELD 无 count 形应回快照字段 bulk: {}",
        String::from_utf8_lossy(&out)
      )
    });
    assert!(known.contains(&f), "claim 窗抽样字段越域: {f:?}");
    // 同窗快照出读同构：抽样字段经 HGET 读臂回值逐字节一致
    let hget = rt.block_on(c.exec(RespCommand::Hget, &[b"hh", &f]));
    assert_not_err(&hget, "claim 窗 HGET 快照出读");
    assert_eq!(
      reply_bulk(&hget).unwrap(),
      format!("v{}", from_utf8(&f[1..]).unwrap()).into_bytes(),
      "HRANDFIELD 抽样字段与同窗 HGET 出读须逐字节同构"
    );
    // 正 count 3：互异全量域，帧头 *3 锁，集合等价灌入域（快照全出）
    let out = rt.block_on(c.exec(RespCommand::Hrandfield, &[b"hh", b"3"]));
    assert_not_err(&out, "claim 窗 HRANDFIELD 正 count 形");
    assert_eq!(&out[..4], b"*3\r\n", "claim 窗互异形帧头锁");
    let items = reply_array(&out).unwrap();
    let distinct: HashSet<Vec<u8>> = items.iter().cloned().collect();
    assert_eq!(items.len(), 3, "互异形声明头须等实发条数");
    assert_eq!(distinct, known, "claim 窗快照互异全量域须与灌入域等价");
    // 负 count -5：可重复形，5 条恒属域内
    let out = rt.block_on(c.exec(RespCommand::Hrandfield, &[b"hh", b"-5"]));
    assert_not_err(&out, "claim 窗 HRANDFIELD 负 count 形");
    let items = reply_array(&out).unwrap();
    assert_eq!(items.len(), 5, "负 count 形帧头 *5 须实发 5 条");
    for f in &items {
      assert!(known.contains(f), "claim 窗负 count 抽样字段越域: {f:?}");
    }
    // WITHVALUES（RESP2 平铺 4 元）：配对保真同窗零错误帧
    let out = rt.block_on(c.exec(RespCommand::Hrandfield, &[b"hh", b"2", b"WITHVALUES"]));
    assert_not_err(&out, "claim 窗 HRANDFIELD WITHVALUES 形");
    let items = reply_array(&out).unwrap();
    assert_eq!(items.len(), 4, "RESP2 WITHVALUES 平铺 2n 头须实发 4 元");
    for j in 0..2 {
      assert!(
        known.contains(&items[j * 2]),
        "HWV 字段越域: {:?}",
        items[j * 2]
      );
      assert_eq!(
        items[j * 2 + 1],
        format!("v{}", from_utf8(&items[j * 2][1..]).unwrap()).into_bytes(),
        "claim 窗 WITHVALUES 配对须 f<i>→v<i> 保真"
      );
    }
    // count 0：不触后端短路（快照无关），*0 逐字节锁
    let out = rt.block_on(c.exec(RespCommand::Hrandfield, &[b"hh", b"0"]));
    assert_eq!(out, b"*0\r\n", "claim 窗 count 0 短路帧锁");
  }

  // 出窗（claim 成对释放）：内容原态完整、计数精确——claim 门零数据损伤
  mgr.release_migration_claim(&list_meta);
  mgr.release_migration_claim(&hash_meta);
  let mut c = conn_on(&env);
  let out = rt.block_on(c.exec(RespCommand::Lrange, &[b"lq", b"0", b"-1"]));
  assert_not_err(&out, "出窗 LRANGE");
  let items = reply_array(&out).unwrap();
  assert_eq!(items.len(), 8);
  for (i, v) in items.iter().enumerate() {
    assert_eq!(v, &format!("e{i}").into_bytes());
  }
  assert_eq!(
    reply_int(&rt.block_on(c.exec(RespCommand::Llen, &[b"lq"]))),
    Some(8)
  );
  assert_eq!(
    reply_int(&rt.block_on(c.exec(RespCommand::Hlen, &[b"hh"]))),
    Some(3)
  );
  // 出窗抽样臂闭环零损伤：互异形仍出全量域（与 claim 窗内快照同域）
  let out = rt.block_on(c.exec(RespCommand::Hrandfield, &[b"hh", b"3"]));
  assert_not_err(&out, "出窗 HRANDFIELD 互异形");
  let items = reply_array(&out).unwrap();
  assert_eq!(items.len(), 3, "出窗互异形声明头须等实发");
  let distinct: HashSet<Vec<u8>> = items.iter().cloned().collect();
  assert_eq!(distinct.len(), 3, "出窗互异形不得重样（回绕分段互斥）");
}

/// 场景五（确定性，wkv 层）：refresh_tiered_meta 遇在册 claim 精确回
/// `Err(MigrationBusy)`——读臂回退快照分支所匹配的错误判据单点；出窗后刷新
/// 成功且取回现势 size（读臂刷新序「刷新值与树内容互一致」的基元判据）
#[test]
fn refresh_tiered_meta_busy_and_fresh_after_release() {
  let env = tiered_env("trsm-refresh-busy.db");
  seed_list(&env, b"lq", 5);
  let rt = &env.rt;
  let sess = env.store.new_session().unwrap();
  let meta_k = sess.session_meta_key(b"lq");
  let mgr = env.store.range_index();

  assert!(mgr.try_claim_migration(&meta_k));
  let mut meta = MetaValue::new(0, GarnetObjectType::List, 0);
  let mut stub = None;
  let err = rt
    .block_on(sess.refresh_tiered_meta(b"lq", &mut meta, stub.as_mut()))
    .expect_err("claim 在册刷新必须显式拒绝");
  assert!(
    matches!(err, WkvError::MigrationBusy),
    "必须精确回 MigrationBusy: {err}"
  );
  mgr.release_migration_claim(&meta_k);

  let ok = rt
    .block_on(sess.refresh_tiered_meta(b"lq", &mut meta, stub.as_mut()))
    .expect("出窗刷新必须成功");
  assert!(ok);
  assert_eq!(meta.size, 5, "刷新取回现势 size");
}

/// 场景六（真实并发闭环）：RENAME 迁移窗（claim 登记 → 换树回写 → 释放）×
/// LRANGE / LLEN 轮询压测——任一交错下应答只允许三类合法形：迁移门忙拒错误
/// 帧、回退快照的完整内容帧、键消亡空帧；内容级逐字节精确，终态 dst 全量、
/// src 消亡
#[test]
fn rename_window_read_frames_stay_legal() {
  const RENAMES: usize = 12;
  const READS: usize = 240;
  let env = tiered_env("trsm-rename-legality.db");
  seed_list(&env, b"rk0", 8);
  let canonical: Vec<Vec<u8>> = (0..8).map(|i| format!("e{i}").into_bytes()).collect();
  let barrier = Arc::new(Barrier::new(2));

  let renamer = {
    let store = Arc::clone(&env.store);
    let barrier = Arc::clone(&barrier);
    thread::spawn(move || {
      let rt = Runtime::new().unwrap();
      let mut c = conn_on_store(&store);
      rt.block_on(async {
        barrier.wait();
        for i in 0..RENAMES {
          let (src, dst) = (format!("rk{i}"), format!("rk{}", i + 1));
          let out = c
            .exec(RespCommand::Rename, &[src.as_bytes(), dst.as_bytes()])
            .await;
          assert!(out.starts_with(b"+OK"), "RENAME {src}->{dst} 应成功");
        }
      });
    })
  };

  let reader = {
    let store = Arc::clone(&env.store);
    let canonical = canonical.clone();
    let barrier = Arc::clone(&barrier);
    thread::spawn(move || {
      let rt = Runtime::new().unwrap();
      let mut c = conn_on_store(&store);
      rt.block_on(async {
        barrier.wait();
        for i in 0..READS {
          // 轮转探针键：覆盖未迁入 / 在册 / 已迁出三态的交错
          let probe_key = format!("rk{}", i % 3).into_bytes();
          let out = c
            .exec(RespCommand::Lrange, &[&probe_key, b"0", b"-1"])
            .await;
          if out.starts_with(b"-ERR ") {
            // 迁移装载门忙拒（既有口径）：合法形一
            continue;
          }
          match reply_array(&out) {
            // 回退快照完整帧 / 键不存在空帧：内容必须逐字节精确
            Some(items) => assert!(
              items.is_empty() || items == canonical,
              "LRANGE 第 {i} 轮出帧内容非法: {items:?}"
            ),
            None => panic!(
              "LRANGE 第 {i} 轮应答非法: {}",
              String::from_utf8_lossy(&out)
            ),
          }
          let len = c.exec(RespCommand::Llen, &[&probe_key]).await;
          assert!(
            len.starts_with(b"-ERR ") || reply_int(&len).is_some(),
            "LLEN 第 {i} 轮应答非法: {}",
            String::from_utf8_lossy(&len)
          );
        }
      });
    })
  };
  renamer.join().unwrap();
  reader.join().unwrap();

  // 终态：链尾键全量、链首键消亡
  let rt = &env.rt;
  let mut c = conn_on(&env);
  let out = rt.block_on(c.exec(RespCommand::Lrange, &[b"rk0", b"0", b"-1"]));
  assert_eq!(out, b"*0\r\n", "链首键应随 RENAME 消亡");
  let tail = format!("rk{RENAMES}");
  let out = rt.block_on(c.exec(RespCommand::Lrange, &[tail.as_bytes(), b"0", b"-1"]));
  assert_eq!(reply_array(&out).unwrap(), canonical, "链尾键内容应完整");
}

/// 场景七：键消亡（size = 0）应答缺失语义与 LRANGE 语义矩阵回归——正负索引
/// 折算与钳制（C# ListRange 口径）不动，缺失键五读臂应答缺失、无存储错误帧
#[test]
fn missing_key_semantics_and_lrange_matrix() {
  let env = tiered_env("trsm-matrix.db");
  seed_list(&env, b"mx", 5);
  let rt = &env.rt;
  let mut c = conn_on(&env);
  let arr = |n: usize| format!("*{n}\r\n").into_bytes();

  // LRANGE 应答为完整 RESP 数组（含各 bulk string 元素），不能以 arr(5)=*5\r\n 裸头比对全帧
  let full = reply_array(&rt.block_on(c.exec(RespCommand::Lrange, &[b"mx", b"0", b"-1"]))).unwrap();
  assert_eq!(
    full,
    vec![
      b"e0".to_vec(),
      b"e1".to_vec(),
      b"e2".to_vec(),
      b"e3".to_vec(),
      b"e4".to_vec(),
    ]
  );
  let mid = reply_array(&rt.block_on(c.exec(RespCommand::Lrange, &[b"mx", b"1", b"3"]))).unwrap();
  assert_eq!(mid, vec![b"e1".to_vec(), b"e2".to_vec(), b"e3".to_vec()]);
  let tail =
    reply_array(&rt.block_on(c.exec(RespCommand::Lrange, &[b"mx", b"-2", b"-1"]))).unwrap();
  assert_eq!(tail, vec![b"e3".to_vec(), b"e4".to_vec()]);
  assert_eq!(
    rt.block_on(c.exec(RespCommand::Lrange, &[b"mx", b"0", b"-10"])),
    arr(0),
    "start 钳 0 后 start > stop 应回空数组"
  );
  assert_eq!(
    reply_array(&rt.block_on(c.exec(RespCommand::Lrange, &[b"mx", b"0", b"100"]))).unwrap(),
    vec![
      b"e0".to_vec(),
      b"e1".to_vec(),
      b"e2".to_vec(),
      b"e3".to_vec(),
      b"e4".to_vec(),
    ],
    "stop 越界钳 len-1 后回全量"
  );
  assert_eq!(
    reply_int(&rt.block_on(c.exec(RespCommand::Llen, &[b"mx"]))),
    Some(5)
  );
  assert_eq!(
    reply_bulk(&rt.block_on(c.exec(RespCommand::Lindex, &[b"mx", b"-1"]))).unwrap(),
    b"e4".to_vec()
  );
  assert!(
    rt.block_on(c.exec(RespCommand::Lindex, &[b"mx", b"5"]))
      .starts_with(b"$-1"),
    "越界 LINDEX 回 null（RESP2）"
  );

  // 缺失键：五读臂应答缺失语义（无分层树、无存储错误帧）
  let out = rt.block_on(c.exec(RespCommand::Lrange, &[b"nope", b"0", b"-1"]));
  assert_not_err(&out, "缺失键 LRANGE");
  assert_eq!(out, arr(0));
  assert_eq!(
    reply_int(&rt.block_on(c.exec(RespCommand::Llen, &[b"nope"]))),
    Some(0)
  );
  assert!(
    rt.block_on(c.exec(RespCommand::Lindex, &[b"nope", b"0"]))
      .starts_with(b"$-1")
  );
  assert_eq!(
    reply_int(&rt.block_on(c.exec(RespCommand::Scard, &[b"nope"]))),
    Some(0)
  );
  assert_eq!(
    rt.block_on(c.exec(RespCommand::Smembers, &[b"nope"])),
    arr(0)
  );
}
