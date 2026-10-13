use std::hint::black_box;

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use regress::harness::{
  DEFAULT_KEY_SIZE, DEFAULT_SCAN_LEN, DEFAULT_VALUE_SIZE, READ_BUF_LEN, WbftreeHarness, WkvHarness,
  WorkloadParams, YcsbHarness, make_num_key,
  micro::{EpochHarness, PageHarness},
  wdev::WDEV_PAGE_SLOTS,
};

fn bench_wkv_regression(c: &mut Criterion) {
  let mut group = c.benchmark_group("wkv_regression");
  group.significance_level(0.05);
  group.noise_threshold(0.02);

  let harness = WkvHarness::default_bench(100_000).expect("初始化 wkv 失败");
  let sample_val = [b'v'; DEFAULT_VALUE_SIZE];

  // 1. 点写性能回归基准
  let mut write_idx = 0usize;
  group.bench_function("wkv_upsert", |b| {
    b.iter(|| {
      write_idx += 1;
      let key = make_num_key::<DEFAULT_KEY_SIZE>(b"wkv:regress:k:", write_idx % 50_000);
      black_box(harness.upsert_sync(&key, black_box(&sample_val)));
    });
  });

  // 预热预插数据
  for i in 0..10_000 {
    let key = make_num_key::<DEFAULT_KEY_SIZE>(b"wkv:regress:k:", i);
    harness.upsert_sync(&key, &sample_val);
  }

  // 2. 内存点查性能回归基准
  let mut read_buf = [0u8; 512];
  let mut read_idx = 0usize;
  group.bench_function("wkv_get_hot", |b| {
    b.iter(|| {
      read_idx += 1;
      let key = make_num_key::<DEFAULT_KEY_SIZE>(b"wkv:regress:k:", read_idx % 10_000);
      black_box(harness.get_sync(&key, black_box(&mut read_buf)));
    });
  });

  // 3. 删除性能回归基准（独立 del: 键空间，setup 先插后删，删的始终是存在键；
  //    不再覆盖 wkv:regress:k: 读键空间，保证后续 get_immutable 工况真实命中）
  let mut del_idx = 0usize;
  group.bench_function("wkv_delete", |b| {
    b.iter_batched(
      || {
        del_idx += 1;
        let key = make_num_key::<DEFAULT_KEY_SIZE>(b"wkv:regress:del:", del_idx % 10_000);
        harness.upsert_sync(&key, &sample_val);
        key
      },
      |key| black_box(harness.delete_sync(&key)),
      BatchSize::SmallInput,
    );
  });

  // 4. 不可变区（只读区）点查性能回归基准：flush + 封区后数据驻留内存但已封存，
  //    点查走纯指针直读路径（生产 flush 后稳态主工况，与可变区 get_hot 互补）
  harness.seal_immutable();
  // 工况预校验：抽样断言封区后键可读，杜绝基准测到空数据（对标 C# BfTreeOperations
  // GlobalSetup 的 Debug.Assert 三连预校验口径）
  {
    let probe_key = make_num_key::<DEFAULT_KEY_SIZE>(b"wkv:regress:k:", 0);
    let mut probe_buf = [0u8; 512];
    assert_eq!(
      harness.get_sync(&probe_key, &mut probe_buf),
      Some(DEFAULT_VALUE_SIZE),
      "wkv_get_immutable 工况失真: 封区后键不可读"
    );
  }
  let mut imm_idx = 0usize;
  group.bench_function("wkv_get_immutable", |b| {
    b.iter(|| {
      imm_idx += 1;
      let key = make_num_key::<DEFAULT_KEY_SIZE>(b"wkv:regress:k:", imm_idx % 10_000);
      black_box(harness.get_sync(&key, black_box(&mut read_buf)));
    });
  });

  group.finish();
}

fn bench_wbftree_regression(c: &mut Criterion) {
  let mut group = c.benchmark_group("wbftree_regression");
  group.significance_level(0.05);
  group.noise_threshold(0.02);

  let harness = WbftreeHarness::default_bench().expect("初始化 wbftree 失败");
  let sample_val = [b'w'; DEFAULT_VALUE_SIZE];

  // 1. 点写性能回归基准
  let mut write_idx = 0usize;
  group.bench_function("wbftree_insert", |b| {
    b.iter(|| {
      write_idx += 1;
      let key = make_num_key::<DEFAULT_KEY_SIZE>(b"wbf:regress:k:", write_idx % 50_000);
      black_box(harness.insert(&key, black_box(&sample_val)));
    });
  });

  // 预热预插数据
  for i in 0..10_000 {
    let key = make_num_key::<DEFAULT_KEY_SIZE>(b"wbf:regress:k:", i);
    harness.insert(&key, &sample_val);
  }

  // 2. 点读性能回归基准
  // 读缓冲取 READ_BUF_LEN(cb_max_record_size=4097)：引擎要求 out_buf 容量 >=
  // cb_max_record_size 才走 read_into 直读快路径，低于门槛（4096/512 均触发）
  // 走 fallback 双拷贝中转；对标 C# BfTreeService.ReadByPtr 的 stackalloc 4096 中转口径
  let mut read_buf = [0u8; READ_BUF_LEN];
  let mut read_idx = 0usize;
  group.bench_function("wbftree_read", |b| {
    b.iter(|| {
      read_idx += 1;
      let key = make_num_key::<DEFAULT_KEY_SIZE>(b"wbf:regress:k:", read_idx % 10_000);
      black_box(harness.read(&key, black_box(&mut read_buf)));
    });
  });

  // 3. 范围扫描性能回归基准 (10 项，对齐 bench/ 默认 scan_len)
  let mut scan_idx = 0usize;
  group.bench_function("wbftree_scan_10", |b| {
    b.iter(|| {
      scan_idx += 1;
      let key = make_num_key::<DEFAULT_KEY_SIZE>(b"wbf:regress:k:", (scan_idx * 7) % 9_000);
      black_box(harness.scan(&key, DEFAULT_SCAN_LEN));
    });
  });

  // 4. 范围扫描性能回归基准 (50 项)
  group.bench_function("wbftree_scan_50", |b| {
    b.iter(|| {
      scan_idx += 1;
      let key = make_num_key::<DEFAULT_KEY_SIZE>(b"wbf:regress:k:", (scan_idx * 11) % 8_000);
      black_box(harness.scan(&key, 50));
    });
  });

  // 5. 删除性能回归基准
  let mut del_idx = 0usize;
  group.bench_function("wbftree_delete", |b| {
    b.iter_batched(
      || {
        del_idx += 1;
        let key = make_num_key::<DEFAULT_KEY_SIZE>(b"wbf:regress:k:", del_idx % 10_000);
        harness.insert(&key, &sample_val);
        key
      },
      |key| black_box(harness.delete(&key)),
      BatchSize::SmallInput,
    );
  });

  group.finish();
}

fn bench_ycsb_wkv_regression(c: &mut Criterion) {
  let mut group = c.benchmark_group("ycsb_wkv_regression");
  group.significance_level(0.05);
  group.noise_threshold(0.02);

  // YCSB core workload A：50/50 读写混合、zipf θ=0.99（对标 KV.benchmark RUMD+zipf 默认档）
  let params = WorkloadParams::ycsb_a(50_000);
  let mut harness = YcsbHarness::new(params).expect("初始化 ycsb 失败");
  harness.load();
  // 工况预校验：装载后全量回读校验，杜绝基准测到空/错数据（对标 C# Validate 口径）
  let (mismatches, misses) = harness.validate();
  assert_eq!(
    (mismatches, misses),
    (0, 0),
    "ycsb_wkv_rud50_50 工况失真: 装载回读校验未过"
  );
  // 预热窗丢弃后再进 criterion 采样（对标 KV.benchmark --warmup-sec 结果剔除语义）
  harness.warmup(50_000);

  group.bench_function("ycsb_wkv_rud50_50", |b| {
    b.iter(|| {
      let cnt = harness.run_ops(1_000);
      black_box(cnt.reads + cnt.writes + cnt.deletes);
    });
  });

  // 运行后再校验：删除回插保证键常驻，混合跑完仍须全命中
  let (m2, s2) = harness.validate();
  assert_eq!((m2, s2), (0, 0), "ycsb_wkv_rud50_50 运行后回读失真");

  group.finish();
}

fn bench_wepoch_micro(c: &mut Criterion) {
  let mut group = c.benchmark_group("wepoch_micro");
  group.significance_level(0.05);
  group.noise_threshold(0.02);

  let harness = EpochHarness::bench();
  // 工况预校验：作用域进出保护态翻转、纪元推进单调（杜绝测到未生效的空操作）
  harness.validate();

  // 受保护域进出（对标 C# LightEpoch Protect/Unprotect 热路径）
  group.bench_function("wepoch_protect_scope", |b| {
    b.iter(|| black_box(harness.protect_once()));
  });
  // 推进当前纪元（对标回收水位推进）
  group.bench_function("wepoch_bump_epoch", |b| {
    b.iter(|| black_box(harness.bump_once()));
  });

  group.finish();
}

fn bench_whlog_micro(c: &mut Criterion) {
  let mut group = c.benchmark_group("whlog_micro");
  group.significance_level(0.05);
  group.noise_threshold(0.02);

  let harness = PageHarness::bench().expect("初始化 whlog 页环失败");
  let page_data = vec![b'p'; harness.page_size];

  // 工况预校验：装载后回读该页前缀须逐字节命中（杜绝测到脏/空页）
  harness.validate_roundtrip(0);

  // 页装载：拷数据入环形槽（对标 AllocatorBase 页刷入缓冲）
  let mut pid = 0u64;
  group.bench_function("whlog_page_load", |b| {
    b.iter(|| {
      pid = (pid + 1) % harness.num_pages as u64;
      harness.load(black_box(pid), &page_data);
      black_box(());
    });
  });

  // 页直读命中：取页首字节
  let mut rid = 0u64;
  group.bench_function("whlog_page_read", |b| {
    b.iter(|| {
      rid = (rid + 1) % harness.num_pages as u64;
      black_box(harness.read_head(black_box(rid)));
    });
  });

  group.finish();
}

fn bench_resp_regression(c: &mut Criterion) {
  use regress::harness::resp;
  let mut group = c.benchmark_group("resp_regression");
  group.significance_level(0.05);
  group.noise_threshold(0.02);

  const PIPE: usize = 128;
  const KEY_LEN: usize = 16;
  const VAL_LEN: usize = 32;
  // 工况预校验：解析还原命令名/参数数、编码结构长度精确（杜绝测到空解析/错编码）
  resp::validate(PIPE, KEY_LEN, VAL_LEN);

  let pipeline = resp::build_set_pipeline(PIPE, KEY_LEN, VAL_LEN);
  // 端到端解析：一次迭代解析整段流水线命令
  group.bench_function("resp_parse_pipeline", |b| {
    b.iter(|| {
      let (cmds, bytes) = resp::parse_pipeline(black_box(&pipeline));
      black_box(cmds + bytes)
    });
  });
  // 端到端编码：一次迭代编码 PIPE 条 bulk 应答
  group.bench_function("resp_encode_bulks", |b| {
    b.iter(|| black_box(resp::encode_bulks(PIPE, VAL_LEN)));
  });

  group.finish();
}

fn bench_wdev_regression(c: &mut Criterion) {
  use regress::harness::wdev::WdevHarness;
  let mut group = c.benchmark_group("wdev_regression");
  group.significance_level(0.05);
  group.noise_threshold(0.02);

  let harness = WdevHarness::bench().expect("初始化 wdev 失败");
  // 工况预校验：直写后回读逐字节命中、传输计数写满整页（杜绝测到短写/空读）
  harness.validate();

  // 设备层扇区对齐直写吞吐
  let mut wid = 0u64;
  group.bench_function("wdev_write_page", |b| {
    b.iter(|| {
      wid = (wid + 1) % WDEV_PAGE_SLOTS;
      black_box(harness.write_page(black_box(wid)));
    });
  });

  // 设备层池化直读吞吐
  let mut rid = 0u64;
  group.bench_function("wdev_read_page", |b| {
    b.iter(|| {
      rid = (rid + 1) % WDEV_PAGE_SLOTS;
      black_box(harness.read_page(black_box(rid)));
    });
  });

  group.finish();
}

criterion_group!(
  benches,
  bench_wkv_regression,
  bench_wbftree_regression,
  bench_ycsb_wkv_regression,
  bench_wepoch_micro,
  bench_whlog_micro,
  bench_resp_regression,
  bench_wdev_regression
);
criterion_main!(benches);
