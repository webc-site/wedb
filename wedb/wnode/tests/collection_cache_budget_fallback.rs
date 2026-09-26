//! 集合升阶页缓存总闸回落集成测试（task/ing/wbftree-tree-cache-global-budget.md 验收面）
//!
//! 预算 = 单环（16MiB，恰容一棵默认调参升阶树）：
//! 1. 首个跨阈集合正常升阶（树态）；
//! 2. 第二个跨阈集合升阶被总闸拒绝 → 回落信封态，数据完整、计数正确、命令正常；
//! 3. 首树清退后预算归还，第三个集合升阶可继续执行；
//! 4. cache_reserved 记账 = 在线活跃树环容量和；
//! 5. MEMORY USAGE 升阶臂口径 = 元记录 + 树常驻页环（≥ 环容量）。

use std::sync::Arc;

use compio::runtime::Runtime;
use itoa::Buffer as ItoaBuffer;
use tempfile::tempdir;
use wcol::TIERED_PROMOTE_THRESHOLD;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::resp::{
  garnet_api::{GarnetApi, StoreGarnetApi},
  resp_server_session::{RespServerSession, RespServerSessionOptions},
};
use wnode_test::auto_exec;
use wresp::command::RespCommand;

/// 单环预算：恰容一棵 DEFAULT_RI_COLLECTION（16MiB 页环）升阶树
const SINGLE_RING_BUDGET: usize = 16 * 1024 * 1024;

fn open_env(
  tag: &str,
) -> (
  Runtime,
  GarnetApi,
  Arc<WedbStore<SegmentedDevice>>,
  tempfile::TempDir,
) {
  let dir = tempdir().unwrap();
  let config = StoreConfig::new(2048, 1024 * 1024, 16, 0.5)
    .unwrap()
    .with_tree_cache_budget(SINGLE_RING_BUDGET);
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

/// 跨升阶阈值的批量写入（条目数 ≥ TIERED_PROMOTE_THRESHOLD 触发 should_promote）
fn fill_hash(api: &GarnetApi, rt: &Runtime, s: &mut RespServerSession, key: &[u8], total: usize) {
  let mut buf = ItoaBuffer::new();
  let mut field_buf = Vec::with_capacity(total * 16);
  let mut ranges = Vec::with_capacity(total * 2);

  let mut slices: Vec<&[u8]> = Vec::with_capacity(total * 2 + 1);
  slices.push(key);

  for i in 1..=total {
    let num_bytes = buf.format(i).as_bytes();

    let f_start = field_buf.len();
    field_buf.push(b'f');
    field_buf.extend_from_slice(num_bytes);
    let f_end = field_buf.len();
    ranges.push(f_start..f_end);

    let v_start = f_end;
    field_buf.extend_from_slice(num_bytes);
    let v_end = field_buf.len();
    ranges.push(v_start..v_end);
  }

  for r in &ranges {
    slices.push(&field_buf[r.clone()]);
  }
  auto_exec(api, rt, s, RespCommand::Hset, &slices);
}

#[test]
fn test_cache_budget_fallback_and_release() {
  let (rt, api, store, _dir) = open_env("cache-budget-fallback.db");
  let mut s = session_with(&api);
  let total = TIERED_PROMOTE_THRESHOLD + 10;

  // 1. 首个跨阈集合：预算充足正常升阶（树态）
  fill_hash(&api, &rt, &mut s, b"hash_a", total);
  {
    let sess = store.new_session().unwrap();
    let stub = rt.block_on(sess.load_collection_stub(b"hash_a")).unwrap();
    assert!(stub.is_some(), "首集合必须正常升阶为树态");
  }
  assert_eq!(
    store.range_index().cache_reserved(),
    SINGLE_RING_BUDGET,
    "首树登记后记账 = 该树页环容量"
  );

  // 2. 第二个跨阈集合：升阶被总闸拒绝 → 回落信封态，数据完整、命令正常
  fill_hash(&api, &rt, &mut s, b"hash_b", total);
  {
    let sess = store.new_session().unwrap();
    let stub = rt.block_on(sess.load_collection_stub(b"hash_b")).unwrap();
    assert!(stub.is_none(), "预算耗尽的集合必须保持信封态");
  }
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Hlen, &[b"hash_b"]),
    format!(":{total}\r\n").as_bytes(),
    "回落信封态计数必须完整"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Hget, &[b"hash_b", b"f7"]),
    b"$1\r\n7\r\n",
    "回落信封态数据必须可读"
  );
  assert_eq!(
    store.range_index().cache_reserved(),
    SINGLE_RING_BUDGET,
    "回落不得改变记账（仍只有首树的环）"
  );

  // 3. MEMORY USAGE 升阶臂口径：元记录 + 活跃树常驻页环（≥ 环容量）
  let usage = auto_exec(&api, &rt, &mut s, RespCommand::MemoryUsage, &[b"hash_a"]);
  let usage = String::from_utf8(usage)
    .expect("MEMORY USAGE 应回整数帧")
    .trim()
    .strip_prefix(':')
    .and_then(|v| v.parse::<i64>().ok())
    .expect("MEMORY USAGE 应回整数");
  assert!(
    usage as usize >= SINGLE_RING_BUDGET,
    "升阶键 MEMORY USAGE 必须计入树常驻页环: {usage}"
  );

  // 4. 首树清退（删空自愈整键回收）：预算归还，后续升阶可继续执行
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Del, &[b"hash_a"]),
    b":1\r\n"
  );
  assert_eq!(
    store.range_index().cache_reserved(),
    0,
    "树清退后配额必须归零"
  );
  fill_hash(&api, &rt, &mut s, b"hash_c", total);
  {
    let sess = store.new_session().unwrap();
    let stub = rt.block_on(sess.load_collection_stub(b"hash_c")).unwrap();
    assert!(stub.is_some(), "预算归还后升阶必须可继续执行");
  }
  assert_eq!(
    store.range_index().cache_reserved(),
    SINGLE_RING_BUDGET,
    "后续升阶登记后记账与在线树环容量一致"
  );
}
