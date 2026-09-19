//! 分层升阶/降阶迁移臂键级 TTL 保全集成测试（next/qcode8.data-b2.md item 1）
//!
//! 缺陷口径：tiered 重灌臂 / 懒降阶臂 / 后台降阶臂经「handle_bftree_drain_and_delete
//! 与 promote_collection_to_bftree / obj_save」组合迁移，drain 内无条件 del_ttl
//! 曾使一次升/降阶即静默抹掉 EXPIRE 设置的键级过期，且清除经
//! TtlWrite(expire_at=None) 镜像成 Persist 条目扩散到从库与 AOF 回放面。
//!
//! 修复判据（keep_ttl 分流，对标 C# 对象记录重写从不脱落过期字段——
//! ObjectStore/VarLenInputMethods.cs:42 GetRMWModifiedFieldInfo 把 HasExpiration
//! 从源记录原样前移到修改后记录，且零发 TTL 事件；删除臂记录与过期同亡）：
//! 键在迁移臂后仍存活 → TTL 旁路记录不触碰（ttl_of 逐 tick 原值，非快照回写，
//! 杜绝第二写 TTL 路径与并发 EXPIRE 竞态）；键消亡 → 删空自愈臂照常随键清除。
//!
//! C# 无分层引擎（garnet 集合恒驻对象域），本组臂为 rust 自定义架构，
//! C# 测试集无对位用例，属按不变式新增。

use std::{mem::take, str::from_utf8, sync::Arc};

use compio::runtime::Runtime;
use tempfile::tempdir;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::resp::{
  garnet_api::{GarnetApi, StoreGarnetApi},
  objects::tiered_demote::tiered_demote_round,
  resp_server_session::{RespServerSession, RespServerSessionOptions},
};
use wresp::command::RespCommand;

type TestStore = WedbStore<SegmentedDevice>;

struct Env {
  rt: Runtime,
  store: Arc<TestStore>,
  api: GarnetApi,
  _dir: tempfile::TempDir,
}

fn env(tag: &str) -> Env {
  let dir = tempdir().unwrap();
  let config = StoreConfig::new(2048, 1024 * 1024, 16, 0.5).unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag)).unwrap());
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  let api: GarnetApi = Arc::new(StoreGarnetApi::new(store.new_session().unwrap()));
  Env {
    rt: Runtime::new().unwrap(),
    store,
    api,
    _dir: dir,
  }
}

fn session_with(env: &Env) -> RespServerSession {
  let mut s = RespServerSession::new(1, RespServerSessionOptions::default());
  s.set_garnet_api(env.api.clone());
  s
}

/// 慢路径命令同步求值并回帧字节（与 tiered_background_demote 同款泵）
fn auto_exec(env: &Env, s: &mut RespServerSession, cmd: RespCommand, args: &[&[u8]]) -> Vec<u8> {
  s.output.clear();
  env.api.exec(s, cmd, args);
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

/// 键级 TTL 旁路记录权威读（KeyTag::Ttl 绝对 ticks；None = 无 TTL/已清除）
///
/// 取 ttl_of 原值而非 RESP TTL 秒数：迁移臂若做了「快照 + put_ttl 回写」也
/// 会改变记录写入形态，逐值相等断言同时锁死 keep_ttl「不触碰旁路」单机制
fn ttl_ticks(env: &Env, key: &[u8]) -> Option<i64> {
  let sess = env.store.new_session().unwrap();
  env.rt.block_on(sess.ttl_of(key)).unwrap()
}

/// 灌入 66000 元素列表并越条目门槛真实升阶（1000 一批，同 zset 先例）
fn promote_list(env: &Env, s: &mut RespServerSession, key: &[u8]) {
  for chunk in 0..66_usize {
    let mut args: Vec<&[u8]> = vec![key];
    let items: Vec<Vec<u8>> = (0..1000)
      .map(|i| format!("e{}{}", chunk, i).into_bytes())
      .collect();
    for it in &items {
      args.push(it.as_slice());
    }
    auto_exec(env, s, RespCommand::Rpush, &args);
  }
  assert!(
    is_tiered(env, key),
    "66000 元素应越 65536 条目门槛升阶为分层态"
  );
}

/// 重灌臂（已分层键物化写回灌：drain + promote）后键级 TTL 逐 tick 不丢、
/// 键仍分层、数据不丢
#[test]
fn refill_migration_preserves_key_ttl() {
  let env = env("ttl-refill.db");
  let mut s = session_with(&env);
  promote_list(&env, &mut s, b"l");

  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Expire, &[b"l", b"3600"]),
    b":1\r\n"
  );
  let before = ttl_ticks(&env, b"l").expect("EXPIRE 后旁路 TTL 记录必在");

  // LTRIM 属未支持分层原生操作：物化降级求值 → 收尾臂；40001 条 ∈ 迟滞死区
  // 之上（>32768）→ 重灌臂（drain keep_ttl + promote 重建）
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Ltrim, &[b"l", b"0", b"40000"]),
    b"+OK\r\n"
  );
  assert!(is_tiered(&env, b"l"), "重灌臂后键应仍为分层态");
  assert_eq!(
    ttl_ticks(&env, b"l"),
    Some(before),
    "重灌迁移后键级 TTL 必须逐 tick 原样保留（旁路零触碰）"
  );
  let ttl_out = auto_exec(&env, &mut s, RespCommand::Ttl, &[b"l"]);
  let secs: i64 = from_utf8(&ttl_out[1..ttl_out.len() - 2])
    .expect("TTL 应答为整数字节")
    .parse()
    .expect("TTL 应答可解析");
  assert!(
    (3595..=3600).contains(&secs),
    "RESP TTL 面同口径（过期未脱落），实回 {secs}"
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Llen, &[b"l"]),
    b":40001\r\n"
  );
}

/// 懒降阶臂（前台物化写跌回低水位：obj_save + 树清退）后键级 TTL 不丢、
/// 形态回归信封、数据不丢
#[test]
fn lazy_demote_preserves_key_ttl() {
  let env = env("ttl-lazy-demote.db");
  let mut s = session_with(&env);
  promote_list(&env, &mut s, b"l");

  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Expire, &[b"l", b"3600"]),
    b":1\r\n"
  );
  let before = ttl_ticks(&env, b"l").expect("EXPIRE 后旁路 TTL 记录必在");

  // LTRIM 至 10001 条 ≤ 32768 且体积远低于 2MB：双维齐低 → 懒降阶臂
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Ltrim, &[b"l", b"0", b"10000"]),
    b"+OK\r\n"
  );
  assert!(!is_tiered(&env, b"l"), "懒降阶后元记录与树应清退");
  assert_eq!(
    ttl_ticks(&env, b"l"),
    Some(before),
    "懒降阶迁移后键级 TTL 必须逐 tick 原样保留"
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Llen, &[b"l"]),
    b":10001\r\n"
  );
}

/// 后台降阶臂（tiered_demote_round：obj_save + 树清退）后键级 TTL 不丢
#[test]
fn background_demote_preserves_key_ttl() {
  let env = env("ttl-bg-demote.db");
  let mut s = session_with(&env);

  // 「分层态但双维齐低」构造：客户端写路径（删除命令与成员级 TTL 面）自残余
  // 墓碑源收口起一律穿透物化，双维齐低那次写回本身就地懒降阶，后台轮无从
  // 触发；故直接经 wkv promote 灌 22000 字段（建树不做降阶评估，同
  // tiered_watch_fence 手工升阶先例），键自然处于「分层态且已齐低」。
  let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..22000_usize)
    .map(|i| (format!("f{i}").into_bytes(), b"v".to_vec()))
    .collect();
  {
    let sess = env.store.new_session().unwrap();
    env.rt
      .block_on(sess.promote_collection_to_bftree(
        b"h",
        wval::GarnetObjectType::Hash,
        entries,
        i64::MAX,
      ))
      .unwrap();
  }
  assert!(is_tiered(&env, b"h"), "手工升阶后键应处分层态");

  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Expire, &[b"h", b"3600"]),
    b":1\r\n"
  );
  let before = ttl_ticks(&env, b"h").expect("EXPIRE 后旁路 TTL 记录必在");

  let stats = env.rt.block_on(tiered_demote_round(&env.store));
  assert_eq!(stats.demoted, 1, "双维齐低冷分层键应被后台轮降阶");
  assert!(!is_tiered(&env, b"h"));
  assert_eq!(
    ttl_ticks(&env, b"h"),
    Some(before),
    "后台降阶迁移后键级 TTL 必须逐 tick 原样保留"
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hlen, &[b"h"]),
    b":22000\r\n"
  );
}

/// 反面对照（防过度矫正）：删空自愈臂键消亡，随键 TTL 必须照常清除，
/// 杜绝孤儿 TTL 旁路残留
#[test]
fn empty_self_heal_still_clears_key_ttl() {
  let env = env("ttl-empty-heal.db");
  let mut s = session_with(&env);
  promote_list(&env, &mut s, b"l");

  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Expire, &[b"l", b"3600"]),
    b":1\r\n"
  );
  assert!(ttl_ticks(&env, b"l").is_some());

  // LTRIM 1 0 → 空对象 → 删空自愈臂（keep_ttl=false 保留清除语义）
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Ltrim, &[b"l", b"1", b"0"]),
    b"+OK\r\n"
  );
  assert!(!is_tiered(&env, b"l"));
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Exists, &[b"l"]),
    b":0\r\n"
  );
  assert!(
    ttl_ticks(&env, b"l").is_none(),
    "删空自愈后随键 TTL 必须一并清除，不得残留孤儿 TTL 记录"
  );
}
