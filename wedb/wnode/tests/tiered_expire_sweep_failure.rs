#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! 分层到期出账 [`expire_sweep_or_rebuild`] 失败臂脏态分级回归（本票收口核心）
//!
//! 缺陷：出账序「dec_size → 置脏 → 物理出账」的失败臂不回滚——
//! 1. 重灌臂只 match CacheBudgetExhausted，其余 Err fall-through 伪装 Swept
//!    成功且 dirty 保持真；promote 的 build（装载被拒）/ emit_event（流入队
//!    失败）/ publish（换入失败）三个子集树内容零变更，WATCH 版本假推进误夭折
//!    并发事务，存储故障被吞成命令成功；
//! 2. 删空臂 drain 失败 `map_err(|_| ())?` 上抛，dec_size 与置脏均残留，键未
//!    消亡而版本假推进。
//!
//! 收口：wkv::Error 新增 `Error::Swapped` 换入分级（树物理面已实际变更后的
//! 失败）——未换入硬失败与删空墓碑链失败复位 dirty=false（WATCH 零推进 +
//! 存储错误帧），重灌已换入失败与元记录墓碑已落盘后的失败保持置脏（WATCH
//! 真实推进 + 存储错误帧），CacheBudgetExhausted 推迟自愈形回归不变。
//!
//! 对标 C#：对象域写钩子 watchVersionMap.IncrementVersion 只挂成功臂
//!（libs/server/Storage/Functions/ObjectStore/RMWMethods.cs:79/:100/:125/:200，
//! :180 注释明言先确认 CAS 成功才复制推进）；操作失败即命令失败、版本零推进
//! ——「未变不误杀」是 WATCH 双向契约的核心保证。
//!
//! 注入形态：wbftree 测试钩子 `PUBLISH_FAIL_INJECT` / `DELETE_INDEX_FAIL_INJECT`
//!（一次性，消费即复位），分别模拟换入 IO 失败与排空树注销失败。
//!
//! 票 wnode-tiered-expire-rebuild-swapin-size-overcount 追加面：「换入已生效、
//! 元记录未落盘」窗（Swapped / 崩溃滞后态）磁盘旧 meta 的 size 含已物理摘除
//! 到期成员，下个计数命令重扫 expired==0 无扣减点，差量扣减形被零到期水位
//! 前移臂原样落盘固化（HLEN 永久虚高）；出账点与水位前移臂统一 `live.len()`
//! 实存直赋后收敛，注入形态为写监听口 Write 臂对 Meta 域物理写一次性注错
//!（对照 wkv promote_meta_save_failure_rollback，注入面在 wnode 命令面）。

// 全文件围绕 debug-only 注入钩构造，release 整文件剔除
#![cfg(debug_assertions)]
use std::{
  io,
  sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
  },
  time::Duration,
};

use compio::runtime::Runtime;
use tempfile::tempdir;
use wbase::{convert::TICKS_PER_SECOND, time::now_ticks};
use wbftree::{DELETE_INDEX_FAIL_INJECT, PUBLISH_FAIL_INJECT};
use wcol::types::member_ttl::encode_member;
use wkv::{Error as WkvError, Result as WkvResult, StoreConfig, StoreEvent, StoreEventSink};
use wnode::{
  resp::garnet_api::{GarnetApi, StoreGarnetApi},
  storage::session::storage_session::version_map_watch_hook,
};
use wresp::command::RespCommand;
use wtxn::{TxnLockTable, WatchVersionMap};
use wval::{GarnetObjectType, KeyTag, NamespaceDbCodec};
type Env = wnode_test::WatchEnv;
use wnode_test::{
  WatchEnv, auto_exec, watch_is_tiered as is_tiered, watch_session as session_with,
};
/// 版本表读点（[`wnode_test::ver`] 环境适配）
fn ver(env: &Env, key: &[u8]) -> u64 {
  wnode_test::ver(&env.map, key)
}

fn env_with(tag: &str, budget: Option<usize>) -> Env {
  env_full(tag, budget, None)
}

/// [`env_with`] 的全参形态：可选事件 sink 注入先于任何会话创建（wkv 装配
/// 纪律，msetnx_atomic 同序）
fn env_full(tag: &str, budget: Option<usize>, sink: Option<StoreEventSink>) -> Env {
  let dir = tempdir().unwrap();
  let mut config = StoreConfig::new(2048, 1024 * 1024, 16, 0.5).unwrap();
  if let Some(budget) = budget {
    config = config.with_tree_cache_budget(budget);
  }
  let store = wnode_test::store_open(&dir, tag, config);
  let map = Arc::new(WatchVersionMap::new(1 << 10));
  assert!(
    store.set_watch_hook(version_map_watch_hook(Arc::clone(&map))),
    "引擎级写面钩子应首次挂载"
  );
  if let Some(sink) = sink {
    assert!(store.set_event_sink(sink), "事件 sink 注入应首次挂载");
  }
  let api: GarnetApi = Arc::new(StoreGarnetApi::new(store.new_session().unwrap())).into();
  WatchEnv {
    rt: Runtime::new().unwrap(),
    store,
    map,
    lock_table: TxnLockTable::new(),
    api,
    _dir: dir,
  }
}

fn env(tag: &str) -> Env {
  env_with(tag, None)
}

/// 手工升阶（与 apply_rmw_post_operate 升阶臂同函数，不经命令面零 WATCH 推进）
fn promote_with(env: &Env, key: &[u8], entries: Vec<(Vec<u8>, Vec<u8>)>, next_expiry: i64) {
  let sess = env.store.new_session().unwrap();
  env
    .rt
    .block_on(sess.promote_collection_to_bftree(
      key,
      GarnetObjectType::Hash,
      entries,
      next_expiry,
      false,
    ))
    .unwrap();
}

/// 分层 hash：`expired_n` 个字段挂已过期刻度（e1..）、`alive_n` 个存活字段
///（a1 挂未来刻度、a2 不挂 TTL），水位 = 过期刻度（已越过，首条计数即出账）
fn promote_expired_mix(env: &Env, key: &[u8], expired_n: usize, alive_n: usize) {
  let past = now_ticks() - TICKS_PER_SECOND;
  let future = now_ticks() + 60 * TICKS_PER_SECOND;
  let mut entries: Vec<(Vec<u8>, Vec<u8>)> = (1..=expired_n)
    .map(|i| {
      (
        format!("e{i}").into_bytes(),
        encode_member(format!("v{i}").as_bytes(), Some(past)),
      )
    })
    .collect();
  if alive_n > 0 {
    entries.push((b"a1".to_vec(), encode_member(b"va", Some(future))));
  }
  if alive_n > 1 {
    entries.push((b"a2".to_vec(), encode_member(b"vb", None)));
  }
  debug_assert!(alive_n <= 2, "存活字段造数上限 2");
  promote_with(env, key, entries, past);
}

/// nil 应答形态（RESP2 `$-1` / RESP3 `_`，会话缺省协议版本不假设）
fn is_null_frame(out: &[u8]) -> bool {
  out.starts_with(b"$-1") || out.starts_with(b"_")
}

/// 全文件测试串行门：`PUBLISH_FAIL_INJECT` / `DELETE_INDEX_FAIL_INJECT` 是
/// 进程级一次性静态，cargo test 默认并行线程会互偷注入——他人测试偷走本测试
/// 的注入 ⇒ 本测试回正常应答误判红，本文件测试的重灌 promote 又会消费他人的
/// 注入致其误红（新增 sleep 长测试后竞速窗口放大）。各测试体全程持锁串行，
/// 测试总时长秒级，无并行收益可损失（tiered_scan_err_propagate 同款口径）
static INJECT_SERIALIZE: Mutex<()> = Mutex::new(());

/// 换入失败（未换入硬失败）：HLEN/HGETALL 出账臂必回存储错误帧而非伪装成功，
/// WATCH 版本零推进（树内容零变更，杜绝假推进误夭折并发 WATCH 事务）；
/// 注入消费后重试即物理出账成功，计数剔除到期成员、WATCH 真实推进恰一次
#[test]
fn publish_fail_is_error_frame_and_watch_not_bumped() {
  let _inject_gate = INJECT_SERIALIZE.lock().unwrap();
  let env = env("sweep-publish-fail.db");
  let key = b"h";
  promote_expired_mix(&env, key, 3, 2);
  assert_eq!(ver(&env, key), 0, "升阶不经命令面，零推进");
  let mut s = session_with(&env);

  // HLEN 计数臂：换入失败 → 错误帧 + WATCH 零推进
  PUBLISH_FAIL_INJECT.store(true, Ordering::SeqCst);
  let out = auto_exec(&env.api, &env.rt, &mut s, RespCommand::Hlen, &[key]);
  assert!(
    out.starts_with(b"-ERR "),
    "HLEN 换入失败必须回存储错误帧，实际应答: {}",
    String::from_utf8_lossy(&out)
  );
  assert_eq!(ver(&env, key), 0, "未换入硬失败树内容零变更，WATCH 零推进");

  // HGETALL 输出臂同内核同面：错误帧而非按已出账假成功
  PUBLISH_FAIL_INJECT.store(true, Ordering::SeqCst);
  let out = auto_exec(&env.api, &env.rt, &mut s, RespCommand::Hgetall, &[key]);
  assert!(
    out.starts_with(b"-ERR "),
    "HGETALL 换入失败必须回存储错误帧，实际应答: {}",
    String::from_utf8_lossy(&out)
  );
  assert_eq!(ver(&env, key), 0, "假 Swept 不再命中，WATCH 零推进");

  // 键保持分层态、树原样（到期成员仍在树，读臂按刻度过滤输出恒正确）
  assert!(is_tiered(&env, key), "出账失败键不得消亡");
  let out = auto_exec(&env.api, &env.rt, &mut s, RespCommand::Hget, &[key, b"e1"]);
  assert!(
    is_null_frame(&out),
    "到期成员读面必须被过滤，实际应答: {}",
    String::from_utf8_lossy(&out)
  );

  // 注入已消费：重试即出账成功（重灌换树），计数剔除到期成员、WATCH 推进恰一次
  let out = auto_exec(&env.api, &env.rt, &mut s, RespCommand::Hlen, &[key]);
  assert_eq!(
    out,
    b":2\r\n",
    "重试出账后计数应剔除 3 个到期字段，实际: {}",
    String::from_utf8_lossy(&out)
  );
  assert_eq!(ver(&env, key), 1, "真实出账恰推进一次");
  let out = auto_exec(&env.api, &env.rt, &mut s, RespCommand::Hget, &[key, b"e1"]);
  assert!(
    is_null_frame(&out),
    "出账后到期成员应物理消亡，实际应答: {}",
    String::from_utf8_lossy(&out)
  );
}

/// 删空臂树注销失败（元记录墓碑已落盘、键已死）：保持置脏 WATCH 真实推进，
/// 命令回存储错误帧；键路由域随即消亡，后续计数按键不存在口径回零
#[test]
fn drain_delete_fail_after_tombstone_keeps_dirty() {
  let _inject_gate = INJECT_SERIALIZE.lock().unwrap();
  let env = env("sweep-delete-fail.db");
  let key = b"h";
  // 全部字段到期 → 存活全集为空 → 删空臂
  promote_expired_mix(&env, key, 3, 0);
  assert_eq!(ver(&env, key), 0);
  let mut s = session_with(&env);

  DELETE_INDEX_FAIL_INJECT.store(true, Ordering::SeqCst);
  let out = auto_exec(&env.api, &env.rt, &mut s, RespCommand::Hlen, &[key]);
  assert!(
    out.starts_with(b"-ERR "),
    "删空排空失败必须回存储错误帧，实际应答: {}",
    String::from_utf8_lossy(&out)
  );
  assert_eq!(
    ver(&env, key),
    1,
    "元记录墓碑已落盘键已死，保持置脏 WATCH 真实推进"
  );
  assert!(
    !is_tiered(&env, key),
    "键路由域已消亡（元记录墓碑先于树注销落盘）"
  );

  // 注入已消费：后续计数按键不存在口径回零（无幽灵空元记录、无假活）
  let out = auto_exec(&env.api, &env.rt, &mut s, RespCommand::Hlen, &[key]);
  assert_eq!(
    out,
    b":0\r\n",
    "键已消亡后计数应回零，实际: {}",
    String::from_utf8_lossy(&out)
  );
}

/// CacheBudgetExhausted 推迟自愈回归（分支行为不变）：出账重灌建树被页缓存
/// 总闸拒绝 → 命令不失败、计数按内存扣减应答（剔除到期成员）、WATCH 零推进
///（树无物理变更）、键保持分层态下个命令再扫再试自愈
#[test]
fn budget_exhausted_defers_rebuild_keeps_watch_frozen() {
  let _inject_gate = INJECT_SERIALIZE.lock().unwrap();
  // 预算 = 单环（16MiB，恰容一棵默认调参树）：首升阶占满，重灌 scratch 建树被拒
  let env = env_with("sweep-budget-defer.db", Some(16 * 1024 * 1024));
  let key = b"h";
  promote_expired_mix(&env, key, 3, 2);
  assert_eq!(ver(&env, key), 0);
  let mut s = session_with(&env);

  let out = auto_exec(&env.api, &env.rt, &mut s, RespCommand::Hlen, &[key]);
  assert_eq!(
    out,
    b":2\r\n",
    "预算耗尽推迟自愈，计数应按内存扣减应答剔除到期字段，实际: {}",
    String::from_utf8_lossy(&out)
  );
  assert_eq!(ver(&env, key), 0, "推迟重灌树无物理变更，WATCH 零推进");
  assert!(is_tiered(&env, key), "推迟自愈键保持分层态");

  // 水位未落盘（磁盘元记录未动）：再次计数重装载后仍越过 → 再扫再推迟，应答稳定
  let out = auto_exec(&env.api, &env.rt, &mut s, RespCommand::Hlen, &[key]);
  assert_eq!(out, b":2\r\n");
  assert_eq!(ver(&env, key), 0, "反复推迟不得累积推进");
}

/// 本票测试键的树身份键 = 物理 Meta 键（默认会话域 (0,0)，与
/// session_meta_key 同一编码内核）
fn swapin_test_meta_key() -> wval::TaggedKeyBuf {
  NamespaceDbCodec::encode_tagged_key(0, 0, KeyTag::Meta, b"h")
}

/// 换入点元记录落盘一次性注错口：写监听口 Write 臂对本键 Meta 域物理写一次
/// 性失败（消费即复位，与 save_bftree_meta_stub 落盘 I/O 失败同形上抛），
/// 其余事件直通
fn meta_save_fail_once(
  fail_next: &AtomicBool,
  _ver: i64,
  _aof_session_id: i32,
  event: StoreEvent<'_>,
) -> WkvResult<()> {
  if let StoreEvent::Write { key, .. } = event
    && key == swapin_test_meta_key().as_slice()
    && fail_next.swap(false, Ordering::SeqCst)
  {
    return Err(WkvError::Io(io::Error::other(
      "injected meta stub save failure",
    )));
  }
  Ok(())
}

/// 「换入已生效、元记录未落盘」窗 size 实存直赋收敛（票
/// wnode-tiered-expire-rebuild-swapin-size-overcount）：
/// 出账差量扣减对已物理摘除成员结构性失效——重灌换树后到期成员物理消失，
/// 磁盘旧 meta（size 含 E、next_expiry 仍越过）在下个计数命令重扫时
/// expired==0 无扣减点，虚高 size 被零到期水位前移臂原样落盘固化，HLEN 永久
/// 虚高、删空自愈判据同被推迟；出账点与水位前移臂统一 `live.len()` 实存直赋
/// 后：滞后态下个计数命令收敛实存并落盘固化，下一轮到期再出账旧差不残留。
///
/// 两段构造：①写监听口 Write 臂对 Meta 域物理写一次性注错（对照 wkv
/// promote_meta_save_failure_rollback，注入面在 wnode 命令面）锁 Swapped 臂
/// 应答面回归——写内核「通知失败 = 已生效」下磁盘 meta 已是实存值，命令回
/// 错误帧、WATCH 真实推进、下个计数直读收敛；②覆写 stale meta（size 含已
/// 摘除成员、水位越过）摆出「新树 + 旧 meta」崩溃滞后磁盘态（对位
/// promote.rs:68 换入后至元记录落盘前崩溃的恢复形），零到期水位前移臂实存
/// 直赋收敛——修复前 stale size 原样落盘固化，应答恒虚高
#[test]
fn swapin_stale_meta_size_converges_by_live_len() {
  let _inject_gate = INJECT_SERIALIZE.lock().unwrap();
  // a1 挂 3s 未来刻度（第四段再出账源）、e1 已过期（首轮出账源）
  let past = now_ticks() - TICKS_PER_SECOND;
  let future = now_ticks() + 3 * TICKS_PER_SECOND;
  // 注错口装配即挂、首升阶直通（armed=false），升阶完成后置 arm 精准命中
  // 出账重灌的元记录落盘（本键第二个 Meta 物理写）
  let fail_next = Arc::new(AtomicBool::new(false));
  let env = env_full(
    "sweep-swapin-stale.db",
    None,
    Some(StoreEventSink::new(
      Arc::clone(&fail_next),
      meta_save_fail_once,
    )),
  );
  let key = b"h";
  promote_with(
    &env,
    key,
    vec![
      (b"a1".to_vec(), encode_member(b"va", Some(future))),
      (b"e1".to_vec(), encode_member(b"ve", Some(past))),
    ],
    past,
  );
  let mut s = session_with(&env);

  // ── ① Swapped 臂应答面：出账重灌换入成功、元记录落盘注入失败──
  fail_next.store(true, Ordering::SeqCst);
  let out = auto_exec(&env.api, &env.rt, &mut s, RespCommand::Hlen, &[key]);
  assert!(
    out.starts_with(b"-ERR "),
    "换入后落盘失败必须回存储错误帧，实际应答: {}",
    String::from_utf8_lossy(&out)
  );
  assert_eq!(ver(&env, key), 1, "已换入失败保持置脏，WATCH 真实推进");

  // 写内核「通知失败 = 已生效」：磁盘 meta 已是实存 1（水位随灌入批前移），
  // 下个计数装载新值直读收敛（应答面回归，无幽灵回滚）
  let out = auto_exec(&env.api, &env.rt, &mut s, RespCommand::Hlen, &[key]);
  assert_eq!(
    out,
    b":1\r\n",
    "Swapped 后首个计数应答实存（仅 a1 存活），实际: {}",
    String::from_utf8_lossy(&out)
  );
  assert_eq!(ver(&env, key), 1, "水位内直读零树变更，WATCH 零推进");

  // ── ② 崩溃滞后磁盘态（「换入已生效、元记录未落盘」恢复形）：覆写 stale
  // meta——size=2 含已物理摘除的 e1、next_expiry=past 越过（新树 + 旧 meta），
  // 重扫新树 expired==0 无扣减点，零到期水位前移臂实存直赋收敛并落盘──
  {
    let sess = env.store.new_session().unwrap();
    let (mut meta, stub) = env
      .rt
      .block_on(sess.load_collection_stub(key))
      .unwrap()
      .expect("滞后态键仍分层");
    meta.size = 2;
    meta.next_expiry = past;
    env
      .rt
      .block_on(sess.save_bftree_meta_stub(key, &meta, &stub))
      .unwrap();
  }
  let out = auto_exec(&env.api, &env.rt, &mut s, RespCommand::Hlen, &[key]);
  assert_eq!(
    out,
    b":1\r\n",
    "滞后态计数应收敛实存（仅 a1 存活），实际: {}",
    String::from_utf8_lossy(&out)
  );
  assert_eq!(ver(&env, key), 1, "水位前移臂树内容零变更，WATCH 零推进");
  let meta_size = env
    .rt
    .block_on(env.store.new_session().unwrap().load_meta(key))
    .unwrap()
    .map(|m| m.size);
  assert_eq!(
    meta_size,
    Some(1),
    "收敛值须落盘固化，旧差不得只在内存态（虚高固化即永久漂移）"
  );

  // ── ③ a1 到期（3s 未来刻度越过）再出账——旧差不残留，删空自愈不被
  // 虚高推迟（修复前：固化虚高差量扣 1 应答虚高 1）。轮询到期可观测量：
  // HGET h a1 → nil（纯读臂过滤不出账，替换固定 3500ms 睡眠）──
  wtest_base::wait_assert_sync(
    || {
      let out = auto_exec(&env.api, &env.rt, &mut s, RespCommand::Hget, &[key, b"a1"]);
      is_null_frame(&out)
    },
    Duration::from_secs(5),
    Duration::from_millis(50),
    "h.a1 须在超时窗口内自然过期（HGET → nil）",
  );
  let out = auto_exec(&env.api, &env.rt, &mut s, RespCommand::Hlen, &[key]);
  assert_eq!(
    out,
    b":0\r\n",
    "全空出账删空自愈应回 0（旧差残留即虚高 1），实际: {}",
    String::from_utf8_lossy(&out)
  );
  assert_eq!(ver(&env, key), 2, "删空出账树物理面变更，WATCH 真实推进");
  assert!(!is_tiered(&env, key), "删空自愈后键消亡，无幽灵空元记录");
}
