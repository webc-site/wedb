use std::time::Instant;

use aok::OK;
use regress::harness::{
  DEFAULT_KEY_SIZE, DEFAULT_SCAN_LEN, DEFAULT_VALUE_SIZE, WbftreeHarness, WkvHarness, make_num_key,
};

#[ctor::ctor(unsafe)]
fn init() {
  log_init::init();
}

/// 性能回归门禁：wkv 点写与点读吞吐回归测试
#[test]
fn test_wkv_regression_gate() -> aok::Result<()> {
  let count = 5_000;
  let harness = WkvHarness::default_bench(count * 2)?;
  let val = [b'v'; DEFAULT_VALUE_SIZE];

  // 1. 点写性能回归校验
  let write_start = Instant::now();
  for i in 0..count {
    let key = make_num_key::<DEFAULT_KEY_SIZE>(b"wkv:gate:k:", i);
    assert!(harness.upsert_sync(&key, &val));
  }
  let write_cost = write_start.elapsed().as_secs_f64().max(0.000_001);
  let write_ops = (count as f64) / write_cost;

  // 写入吞吐防退化门禁 (要求至少 10,000 op/s)
  assert!(
    write_ops > 10_000.0,
    "wkv 点写吞吐严重劣化: {:.1} op/s (门禁基线: 10000 op/s)",
    write_ops
  );

  // 2. 点查性能回归校验 (内存命中路径)
  let mut buf = [0u8; 512];
  let read_start = Instant::now();
  for i in 0..count {
    let key = make_num_key::<DEFAULT_KEY_SIZE>(b"wkv:gate:k:", i);
    let len = harness.get_sync(&key, &mut buf);
    assert_eq!(len, Some(DEFAULT_VALUE_SIZE));
  }
  let read_cost = read_start.elapsed().as_secs_f64().max(0.000_001);
  let read_ops = (count as f64) / read_cost;

  // 读取吞吐防退化门禁 (内存路径要求至少 50,000 op/s)
  assert!(
    read_ops > 50_000.0,
    "wkv 内存点查吞吐严重劣化: {:.1} op/s (门禁基线: 50000 op/s)",
    read_ops
  );

  // 3. 删除性能回归校验
  let del_start = Instant::now();
  for i in 0..count {
    let key = make_num_key::<DEFAULT_KEY_SIZE>(b"wkv:gate:k:", i);
    assert!(harness.delete_sync(&key));
  }
  let del_cost = del_start.elapsed().as_secs_f64().max(0.000_001);
  let del_ops = (count as f64) / del_cost;
  assert!(
    del_ops > 10_000.0,
    "wkv 删除吞吐严重劣化: {:.1} op/s (门禁基线: 10000 op/s)",
    del_ops
  );

  OK
}

/// 性能回归门禁：wbftree 点写、点读、范围扫描回归测试
#[test]
fn test_wbftree_regression_gate() -> aok::Result<()> {
  let count = 5_000;
  let harness = WbftreeHarness::default_bench()?;
  let val = [b'w'; DEFAULT_VALUE_SIZE];

  // 1. 点写性能回归校验
  let write_start = Instant::now();
  for i in 0..count {
    let key = make_num_key::<DEFAULT_KEY_SIZE>(b"wbf:gate:k:", i);
    assert!(harness.insert(&key, &val));
  }
  let write_cost = write_start.elapsed().as_secs_f64().max(0.000_001);
  let write_ops = (count as f64) / write_cost;
  assert!(
    write_ops > 10_000.0,
    "wbftree 点写吞吐严重劣化: {:.1} op/s (门禁基线: 10000 op/s)",
    write_ops
  );

  // 2. 点读性能回归校验
  let mut buf = [0u8; 512];
  let read_start = Instant::now();
  for i in 0..count {
    let key = make_num_key::<DEFAULT_KEY_SIZE>(b"wbf:gate:k:", i);
    let len = harness.read(&key, &mut buf);
    assert_eq!(len, Some(DEFAULT_VALUE_SIZE));
  }
  let read_cost = read_start.elapsed().as_secs_f64().max(0.000_001);
  let read_ops = (count as f64) / read_cost;
  assert!(
    read_ops > 20_000.0,
    "wbftree 点查吞吐严重劣化: {:.1} op/s (门禁基线: 20000 op/s)",
    read_ops
  );

  // 3. 范围扫描性能回归校验 (每次扫描 10 项)
  let scan_start = Instant::now();
  let scan_rounds = 1_000;
  let mut total_scanned = 0;
  for i in 0..scan_rounds {
    let key = make_num_key::<DEFAULT_KEY_SIZE>(b"wbf:gate:k:", (i * 3) % (count - 15));
    let (scanned, _) = harness.scan(&key, DEFAULT_SCAN_LEN);
    total_scanned += scanned;
  }
  let scan_cost = scan_start.elapsed().as_secs_f64().max(0.000_001);
  let scan_ops = (total_scanned as f64) / scan_cost;
  assert!(
    scan_ops > 50_000.0,
    "wbftree 范围扫描吞吐严重劣化: {:.1} 项/s (门禁基线: 50000 项/s)",
    scan_ops
  );

  // 4. 删除性能回归校验
  let del_start = Instant::now();
  for i in 0..count {
    let key = make_num_key::<DEFAULT_KEY_SIZE>(b"wbf:gate:k:", i);
    assert!(harness.delete(&key));
  }
  let del_cost = del_start.elapsed().as_secs_f64().max(0.000_001);
  let del_ops = (count as f64) / del_cost;
  assert!(
    del_ops > 10_000.0,
    "wbftree 删除吞吐严重劣化: {:.1} op/s (门禁基线: 10000 op/s)",
    del_ops
  );

  OK
}
