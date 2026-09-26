use std::time::Instant;

use aok::OK;
use regress::harness::{
  DEFAULT_KEY_SIZE, DEFAULT_SCAN_LEN, DEFAULT_VALUE_SIZE, READ_BUF_LEN, WbftreeHarness, WkvHarness,
  WorkloadParams, YcsbHarness, make_num_key,
};

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

  // 2. 点读性能回归校验（读缓冲取 READ_BUF_LEN：容量 >= cb_max_record_size(4097)
  //    才走 read_into 直读快路径，低于门槛均触发 fallback 双拷贝中转）
  let mut buf = [0u8; READ_BUF_LEN];
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

/// 性能回归门禁：wkv YCSB 读写混合工况 (zipf + warmup + validate 回读校验)
#[test]
fn test_wkv_ycsb_mixed_gate() -> aok::Result<()> {
  // YCSB core workload A: 50/50 读写混合、zipf θ=0.99 (对标 KV.benchmark RUMD+zipf 默认档)
  let params = WorkloadParams::ycsb_a(20_000);
  let mut ycsb = YcsbHarness::new(params)?;
  ycsb.load();

  // 工况预校验: 装载后全量回读, 断言零落空零失配 (杜绝测到空/错数据)
  let (mismatches, misses) = ycsb.validate();
  assert_eq!((mismatches, misses), (0, 0), "ycsb 装载回读校验未过");

  // 预热窗丢弃后计时混合操作 (对标 KV.benchmark --warmup-sec 剔除语义)
  ycsb.warmup(20_000);
  let ops = 100_000;
  let start = Instant::now();
  let cnt = ycsb.run_ops(ops);
  let cost = start.elapsed().as_secs_f64().max(0.000_001);
  let mixed_ops = (ops as f64) / cost;

  // 读+写合计须等于总操作数 (无 RMW, 删除回插计入写侧), 且读写均有实采
  assert_eq!(cnt.reads + cnt.writes + cnt.deletes, ops);
  assert!(
    cnt.reads > 0 && cnt.writes > 0,
    "ycsb 混合工况退化: 读或写归零"
  );
  // 混合吞吐防退化门禁 (读写混合含 upsert 落盘路径, debug 档基线 10000 op/s)
  assert!(
    mixed_ops > 10_000.0,
    "wkv YCSB 混合吞吐严重劣化: {:.1} op/s (门禁基线: 10000 op/s)",
    mixed_ops
  );

  // 运行后二次校验: 删除回插保证键常驻, 混合跑完仍须全命中
  let (m2, s2) = ycsb.validate();
  assert_eq!((m2, s2), (0, 0), "ycsb 运行后回读失真");

  OK
}

/// 性能回归门禁：wepoch 纪元保护 + whlog 环形页缓冲微基准回读校验
#[test]
fn test_wepoch_whlog_micro_gate() -> aok::Result<()> {
  use regress::harness::micro::{EpochHarness, PageHarness};
  // wepoch: protected_scope 进出保护态翻转 + 纪元推进单调 (validate 内含断言)
  let epoch = EpochHarness::bench();
  epoch.validate();
  let eops = 200_000u64;
  let start = Instant::now();
  let mut acc = 0u64;
  for _ in 0..eops {
    acc += epoch.protect_once();
  }
  assert_eq!(acc, eops, "wepoch protect 应在域内恒报受保护");
  let ecost = start.elapsed().as_secs_f64().max(0.000_001);
  let e_ops = (eops as f64) / ecost;
  assert!(
    e_ops > 100_000.0,
    "wepoch 纪元保护吞吐严重劣化: {:.1} op/s (门禁基线: 100000 op/s)",
    e_ops
  );

  // whlog: 装载后回读逐字节命中 (validate 内含断言)
  let page = PageHarness::bench()?;
  let data = vec![b'q'; page.page_size];
  page.validate_roundtrip(0);
  let pops = 20_000u64;
  let start = Instant::now();
  for i in 0..pops {
    let pid = i % page.num_pages as u64;
    page.load(pid, &data);
    assert_eq!(page.read_head(pid), b'q', "whlog 页装载后首字节回读失真");
  }
  let pcost = start.elapsed().as_secs_f64().max(0.000_001);
  let p_ops = (pops as f64) / pcost;
  assert!(
    p_ops > 5_000.0,
    "whlog 页装载吞吐严重劣化: {:.1} op/s (门禁基线: 5000 op/s)",
    p_ops
  );

  OK
}

/// 性能回归门禁：wresp RESP 端到端解析/编码回读校验
#[test]
fn test_resp_gate() -> aok::Result<()> {
  use regress::harness::resp;
  const PIPE: usize = 128;
  // 工况预校验: 解析命令名/参数数、编码结构长度精确 (validate 内含断言)
  resp::validate(PIPE, 16, 32);

  let pipeline = resp::build_set_pipeline(PIPE, 16, 32);
  let rounds = 5_000u64;
  let start = Instant::now();
  let mut cmds_total = 0usize;
  for _ in 0..rounds {
    let (cmds, bytes) = resp::parse_pipeline(&pipeline);
    assert_eq!(cmds, PIPE, "RESP 流水线解析命令数不符");
    assert!(bytes > 0, "RESP 解析参数字节为空即失真");
    cmds_total += cmds;
  }
  let pcost = start.elapsed().as_secs_f64().max(0.000_001);
  let p_ops = (cmds_total as f64) / pcost;
  assert!(
    p_ops > 100_000.0,
    "RESP 解析吞吐严重劣化: {:.1} cmd/s (门禁基线: 100000 cmd/s)",
    p_ops
  );

  let start = Instant::now();
  let mut enc_total = 0usize;
  for _ in 0..rounds {
    enc_total += resp::encode_bulks(PIPE, 32);
  }
  let ecost = start.elapsed().as_secs_f64().max(0.000_001);
  let e_ops = ((rounds as usize * PIPE) as f64) / ecost;
  assert!(
    enc_total > 0 && e_ops > 100_000.0,
    "RESP 编码吞吐严重劣化: {:.1} reply/s (门禁基线: 100000 reply/s)",
    e_ops
  );

  OK
}

/// 性能回归门禁：wdev 设备层直写/直读回读校验
#[test]
fn test_wdev_gate() -> aok::Result<()> {
  use regress::harness::wdev::WdevHarness;
  let dev = WdevHarness::bench()?;
  // 工况预校验: 直写一页后回读逐字节命中、传输计数写满整页 (validate 内含断言)
  dev.validate();

  let pages = 200u64;
  let start = Instant::now();
  for i in 0..pages {
    assert_eq!(dev.write_page(i), 16 * 1024, "wdev 短写: 页未写满");
  }
  let wcost = start.elapsed().as_secs_f64().max(0.000_001);
  let w_ops = (pages as f64) / wcost;
  // debug 档真盘写, 吞吐随机器负载浮动大, 仅设绝对下限兜底 (回读/短写断言为主防护)
  assert!(
    w_ops > 100.0,
    "wdev 直写吞吐严重劣化: {:.1} page/s (门禁基线: 100 page/s)",
    w_ops
  );

  let start = Instant::now();
  for i in 0..pages {
    assert!(dev.read_page(i) > 0, "wdev 空读失真");
  }
  let rcost = start.elapsed().as_secs_f64().max(0.000_001);
  let r_ops = (pages as f64) / rcost;
  assert!(
    r_ops > 100.0,
    "wdev 直读吞吐严重劣化: {:.1} page/s (门禁基线: 100 page/s)",
    r_ops
  );

  OK
}
