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

use std::sync::{Arc, atomic::Ordering};

use compio::runtime::Runtime;
use tempfile::tempdir;
use wbase::{convert::TICKS_PER_SECOND, time::now_ticks};
use wbftree::{DELETE_INDEX_FAIL_INJECT, PUBLISH_FAIL_INJECT};
use wcol::types::member_ttl::encode_member;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::{
  resp::{
    garnet_api::{GarnetApi, StoreGarnetApi},
    resp_server_session::{RespServerSession, RespServerSessionOptions},
  },
  storage::session::storage_session::version_map_watch_hook,
};
use wresp::command::RespCommand;
use wtxn::{TxnKeyEntryComparison, WatchVersionMap};
use wval::{GarnetObjectType, SessionPrefixBuf};

type TestStore = WedbStore<SegmentedDevice>;

/// 测试环境：真实引擎 + 共享版本表 + 引擎级写面钩子（与生产装配同径）
struct Env {
  rt: Runtime,
  store: Arc<TestStore>,
  map: Arc<WatchVersionMap>,
  api: GarnetApi,
  _dir: tempfile::TempDir,
}

fn env_with(tag: &str, budget: Option<usize>) -> Env {
  let dir = tempdir().unwrap();
  let mut config = StoreConfig::new(2048, 1024 * 1024, 16, 0.5).unwrap();
  if let Some(budget) = budget {
    config = config.with_tree_cache_budget(budget);
  }
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
    api,
    _dir: dir,
  }
}

fn env(tag: &str) -> Env {
  env_with(tag, None)
}

fn session_with(env: &Env) -> RespServerSession {
  let mut s = RespServerSession::new(1, RespServerSessionOptions::default());
  s.set_garnet_api(env.api.clone());
  s
}

/// 慢路径命令同步求值并回帧字节（与 tiered_watch_fence 同款泵）
fn auto_exec(env: &Env, s: &mut RespServerSession, cmd: RespCommand, args: &[&[u8]]) -> Vec<u8> {
  wnode_test::auto_exec(&env.api, &env.rt, s, cmd, args)
}

/// 版本表读点（与 wtxn 校验同一哈希面：根域 scoped，与默认写会话前缀同源）
fn ver(env: &Env, key: &[u8]) -> u64 {
  env
    .map
    .read_version(
      TxnKeyEntryComparison::scoped_key_hash(SessionPrefixBuf::ROOT.as_slice(), key) as u64,
    )
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

fn is_tiered(env: &Env, key: &[u8]) -> bool {
  let sess = env.store.new_session().unwrap();
  env
    .rt
    .block_on(sess.load_collection_stub(key))
    .unwrap()
    .is_some()
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

/// 换入失败（未换入硬失败）：HLEN/HGETALL 出账臂必回存储错误帧而非伪装成功，
/// WATCH 版本零推进（树内容零变更，杜绝假推进误夭折并发 WATCH 事务）；
/// 注入消费后重试即物理出账成功，计数剔除到期成员、WATCH 真实推进恰一次
#[test]
fn publish_fail_is_error_frame_and_watch_not_bumped() {
  let env = env("sweep-publish-fail.db");
  let key = b"h";
  promote_expired_mix(&env, key, 3, 2);
  assert_eq!(ver(&env, key), 0, "升阶不经命令面，零推进");
  let mut s = session_with(&env);

  // HLEN 计数臂：换入失败 → 错误帧 + WATCH 零推进
  PUBLISH_FAIL_INJECT.store(true, Ordering::SeqCst);
  let out = auto_exec(&env, &mut s, RespCommand::Hlen, &[key]);
  assert!(
    out.starts_with(b"-ERR "),
    "HLEN 换入失败必须回存储错误帧，实际应答: {}",
    String::from_utf8_lossy(&out)
  );
  assert_eq!(ver(&env, key), 0, "未换入硬失败树内容零变更，WATCH 零推进");

  // HGETALL 输出臂同内核同面：错误帧而非按已出账假成功
  PUBLISH_FAIL_INJECT.store(true, Ordering::SeqCst);
  let out = auto_exec(&env, &mut s, RespCommand::Hgetall, &[key]);
  assert!(
    out.starts_with(b"-ERR "),
    "HGETALL 换入失败必须回存储错误帧，实际应答: {}",
    String::from_utf8_lossy(&out)
  );
  assert_eq!(ver(&env, key), 0, "假 Swept 不再命中，WATCH 零推进");

  // 键保持分层态、树原样（到期成员仍在树，读臂按刻度过滤输出恒正确）
  assert!(is_tiered(&env, key), "出账失败键不得消亡");
  let out = auto_exec(&env, &mut s, RespCommand::Hget, &[key, b"e1"]);
  assert!(
    is_null_frame(&out),
    "到期成员读面必须被过滤，实际应答: {}",
    String::from_utf8_lossy(&out)
  );

  // 注入已消费：重试即出账成功（重灌换树），计数剔除到期成员、WATCH 推进恰一次
  let out = auto_exec(&env, &mut s, RespCommand::Hlen, &[key]);
  assert_eq!(
    out,
    b":2\r\n",
    "重试出账后计数应剔除 3 个到期字段，实际: {}",
    String::from_utf8_lossy(&out)
  );
  assert_eq!(ver(&env, key), 1, "真实出账恰推进一次");
  let out = auto_exec(&env, &mut s, RespCommand::Hget, &[key, b"e1"]);
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
  let env = env("sweep-delete-fail.db");
  let key = b"h";
  // 全部字段到期 → 存活全集为空 → 删空臂
  promote_expired_mix(&env, key, 3, 0);
  assert_eq!(ver(&env, key), 0);
  let mut s = session_with(&env);

  DELETE_INDEX_FAIL_INJECT.store(true, Ordering::SeqCst);
  let out = auto_exec(&env, &mut s, RespCommand::Hlen, &[key]);
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
  let out = auto_exec(&env, &mut s, RespCommand::Hlen, &[key]);
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
  // 预算 = 单环（16MiB，恰容一棵默认调参树）：首升阶占满，重灌 scratch 建树被拒
  let env = env_with("sweep-budget-defer.db", Some(16 * 1024 * 1024));
  let key = b"h";
  promote_expired_mix(&env, key, 3, 2);
  assert_eq!(ver(&env, key), 0);
  let mut s = session_with(&env);

  let out = auto_exec(&env, &mut s, RespCommand::Hlen, &[key]);
  assert_eq!(
    out,
    b":2\r\n",
    "预算耗尽推迟自愈，计数应按内存扣减应答剔除到期字段，实际: {}",
    String::from_utf8_lossy(&out)
  );
  assert_eq!(ver(&env, key), 0, "推迟重灌树无物理变更，WATCH 零推进");
  assert!(is_tiered(&env, key), "推迟自愈键保持分层态");

  // 水位未落盘（磁盘元记录未动）：再次计数重装载后仍越过 → 再扫再推迟，应答稳定
  let out = auto_exec(&env, &mut s, RespCommand::Hlen, &[key]);
  assert_eq!(out, b":2\r\n");
  assert_eq!(ver(&env, key), 0, "反复推迟不得累积推进");
}
