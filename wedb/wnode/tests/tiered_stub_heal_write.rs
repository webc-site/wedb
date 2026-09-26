//! 分层集合「治愈后紧跟写操作」存根保序集成测试
//!（对标 C# Tsavorite B+ 树存根保序机制）
//!
//! 缺陷口径：acquire_tree_read / acquire_tree_write 执行 RIPROMOTE 清 is_flushed
//! 或惰性恢复 RIRESTORE 重绑句柄清恢复位后，若 TieredCtx.stub 仍持锁外装载的
//! 陈旧快照，写臂 save_tiered_meta 落盘时会把刚治愈的存根状态静默回滚覆写。
//!
//! 修复判据（存根保序不变量）：治愈原语落地即内存同步——RIPROMOTE 在调用方
//! 存根上原位 set_flushed(false)（wkv range_index/stub.rs），写臂 tiered_guard
//! 在独占锁内 refresh_tiered_meta 以治愈后持久化记录回填 ctx.stub——本组测试
//! 钉死：治愈后紧跟写操作落盘的存根 is_flushed / is_recovered / tree_handle
//! 与治愈态一致，绝不被陈旧快照回写；数据与计数治愈前后不变（对齐
//! doc/zh/collection.md 升阶 RIPROMOTE / RIRESTORE 计数值不变承诺）。
//!
//! C# 无分层引擎（garnet 集合恒驻对象域，存根治愈是 RMW in-span 原位改值），
//! 本组用例属 rust 自定义架构按设计承诺新增，C# 测试集无对位。

use std::sync::Arc;

use compio::runtime::Runtime;
use tempfile::tempdir;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::resp::{
  garnet_api::{GarnetApi, StoreGarnetApi},
  resp_server_session::{RespServerSession, RespServerSessionOptions},
};
use wnode_test::auto_exec;
use wresp::command::RespCommand;
use wval::{KeyTag, MetaValue, NamespaceDbCodec, TaggedKeyBuf};

const KEY: &[u8] = b"big_hash";
const FIELDS: usize = 8000;

/// 字段载荷单源（600B 使总体积 ~4.8MB 越 TIERED_PROMOTE_BYTES 门限）
fn payload() -> Vec<u8> {
  vec![b'v'; 600]
}

type TestStore = WedbStore<SegmentedDevice>;

fn open_env(tag: &str) -> (Runtime, GarnetApi, Arc<TestStore>, tempfile::TempDir) {
  let dir = tempdir().unwrap();
  let config = StoreConfig::new(2048, 1024 * 1024, 16, 0.5).unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag)).unwrap());
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  (
    Runtime::new().unwrap(),
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
    store,
    dir,
  )
}

fn session_with(api: &GarnetApi) -> RespServerSession {
  let mut s = RespServerSession::new(1, RespServerSessionOptions::default());
  s.set_garnet_api(api.clone());
  s
}

/// 树身份键 = 物理 Meta 键（会话测试域 ns 0 / db 0，与注册表 / 下线判据同键）
fn tree_id_key(user_key: &[u8]) -> TaggedKeyBuf {
  NamespaceDbCodec::encode_tagged_key(0, 0, KeyTag::Meta, user_key)
}

/// 持久化存根权威读（wkv load_collection_stub 单点）
fn persisted(
  rt: &Runtime,
  store: &Arc<TestStore>,
  key: &[u8],
) -> (MetaValue, wbftree::RangeIndexStub) {
  let sess = store.new_session().unwrap();
  rt.block_on(sess.load_collection_stub(key))
    .unwrap()
    .unwrap_or_else(|| panic!("键 {key:?} 应处于分层态"))
}

/// 写入 8000 x 600B 字段越 4MB TIERED_PROMOTE_BYTES 门限触发自动升阶
///（条目数 8000 < 65536，触发形态对标 collection_adaptive_tiering）
fn promote_hash(rt: &Runtime, api: &GarnetApi, s: &mut RespServerSession) {
  let val = payload();
  for start in (1..=FIELDS).step_by(FIELDS) {
    let end = (start + FIELDS - 1).min(FIELDS);
    let mut args: Vec<Vec<u8>> = Vec::with_capacity((end - start + 1) * 2 + 1);
    args.push(KEY.to_vec());
    for i in start..=end {
      args.push(format!("field_{i}").into_bytes());
      args.push(val.clone());
    }
    let slices: Vec<&[u8]> = args.iter().map(|v| v.as_slice()).collect();
    assert_eq!(
      auto_exec(api, rt, s, RespCommand::Hset, &slices),
      format!(":{}\r\n", end - start + 1).into_bytes()
    );
  }
  assert_eq!(
    auto_exec(api, rt, s, RespCommand::Hlen, &[KEY]),
    format!(":{}\r\n", FIELDS).into_bytes(),
    "升阶后 O(1) 计数应承接全量字段"
  );
}

/// 场景 A：RIPROMOTE 治愈后紧跟写操作，is_flushed 不被写回回滚
///
/// flush_all 置位 Flushed → HSET 写臂触发 acquire_tree_write 的 RIPROMOTE
///（清 Flushed）→ 锁内刷新 + save_tiered_meta 落盘——落盘存根必须保持治愈态
#[test]
fn ripromote_heal_survives_followup_write_test() {
  let (rt, api, store, _dir) = open_env("tiered-heal-ripromote.db");
  let mut s = session_with(&api);
  promote_hash(&rt, &api, &mut s);

  rt.block_on(store.flush_all()).unwrap();
  let (_, stub) = persisted(&rt, &store, KEY);
  assert!(stub.is_flushed(), "flush_all 后存根必须为 Flushed");

  // 治愈后紧跟写操作：新增字段走 RIPROMOTE + 锁内刷新 + 元记录回写全链
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Hset,
      &[KEY, b"field_tail", b"tail"]
    ),
    b":1\r\n"
  );

  let (meta, stub) = persisted(&rt, &store, KEY);
  assert!(
    !stub.is_flushed(),
    "治愈后写回不得把 RIPROMOTE 清除的 Flushed 位回滚"
  );
  assert_eq!(meta.size, (FIELDS + 1) as u64, "治愈后写计数承接");
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Hget, &[KEY, b"field_tail"]),
    b"$4\r\ntail\r\n",
    "治愈后写数据点查可读"
  );
}

/// 场景 B：RIRESTORE 治愈后紧跟写操作，恢复位与句柄不被写回回滚
///
/// 刷盘 + 下线树注销注册表 → HSET 写臂触发惰性恢复（RIRESTORE 重绑在线树
/// 句柄清恢复位）+ RIPROMOTE → 落盘存根必须保持在线绑定态：Flushed 与
/// Recovered 位为假、句柄与在线树 native_ptr 一致
#[test]
fn rirestore_heal_survives_followup_write_test() {
  let (rt, api, store, _dir) = open_env("tiered-heal-rirestore.db");
  let mut s = session_with(&api);
  promote_hash(&rt, &api, &mut s);

  // 刷盘置 Flushed 后下线树：后续首访触发 RIPROMOTE + RIRESTORE 全周期
  store.on_flush_pages(0, 0).unwrap();
  let idk = tree_id_key(KEY);
  assert!(
    store
      .range_index
      .dispose_tree_under_lock(&idk, false)
      .unwrap(),
    "下线在册树应成功"
  );
  assert!(store.range_index.get_tree(&idk).is_none());

  // 治愈后紧跟写操作
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Hset,
      &[KEY, b"field_tail", b"tail"]
    ),
    b":1\r\n"
  );

  let tree = store
    .range_index
    .get_tree(&idk)
    .expect("写臂应已重新激活树");
  let (meta, stub) = persisted(&rt, &store, KEY);
  assert!(
    !stub.is_flushed(),
    "治愈后写回不得把 RIPROMOTE 清除的 Flushed 位回滚"
  );
  assert!(
    !stub.is_recovered(),
    "治愈后写回不得把 RIRESTORE 清除的恢复位回滚"
  );
  assert_eq!(
    stub.tree_handle,
    tree.native_ptr(),
    "落盘句柄必须与在线树一致（存根保序）"
  );
  assert_eq!(meta.size, (FIELDS + 1) as u64, "治愈后写计数承接");
  let val = payload();
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Hget, &[KEY, b"field_500"]),
    [b"$600\r\n".as_slice(), &val, b"\r\n"].concat(),
    "治愈后存量数据点查无损"
  );
}

/// 场景 C：RIRESTORE 治愈后 HGETALL 读校正臂，治愈位不被破坏且数据无损
///
/// 下线树 → HGETALL（hash_needs_write 含读校正臂：独占写锁 + 锁内刷新，
/// 水位未越过时零回写）——治愈落地的持久化存根保持在线绑定态，全量回读完整
#[test]
fn rirestore_heal_survives_read_arm_test() {
  let (rt, api, store, _dir) = open_env("tiered-heal-read-arm.db");
  let mut s = session_with(&api);
  promote_hash(&rt, &api, &mut s);

  store.on_flush_pages(0, 0).unwrap();
  let idk = tree_id_key(KEY);
  assert!(
    store
      .range_index
      .dispose_tree_under_lock(&idk, false)
      .unwrap()
  );

  let out = auto_exec(&api, &rt, &mut s, RespCommand::Hgetall, &[KEY]);
  assert!(
    out.windows(b"field_500".len()).any(|w| w == b"field_500"),
    "HGETALL 回读应含存量字段"
  );
  let val = payload();
  assert!(
    out.windows(val.len()).any(|w| w == val),
    "回读应含完整存量值"
  );

  let tree = store
    .range_index
    .get_tree(&idk)
    .expect("读校正臂应已重新激活树");
  let (_, stub) = persisted(&rt, &store, KEY);
  assert!(!stub.is_flushed(), "治愈后 Flushed 位不得回滚");
  assert!(!stub.is_recovered(), "治愈后恢复位不得回滚");
  assert_eq!(stub.tree_handle, tree.native_ptr(), "句柄应与在线树一致");
}
