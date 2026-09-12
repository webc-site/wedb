use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use regress::harness::{
  DEFAULT_KEY_SIZE, DEFAULT_SCAN_LEN, DEFAULT_VALUE_SIZE, WbftreeHarness, WkvHarness, make_num_key,
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

  // 3. 删除性能回归基准
  let mut del_idx = 0usize;
  group.bench_function("wkv_delete", |b| {
    b.iter_batched(
      || {
        del_idx += 1;
        let key = make_num_key::<DEFAULT_KEY_SIZE>(b"wkv:regress:k:", del_idx % 10_000);
        harness.upsert_sync(&key, &sample_val);
        key
      },
      |key| black_box(harness.delete_sync(&key)),
      criterion::BatchSize::SmallInput,
    );
  });

  // 4. 不可变区（只读区）点查性能回归基准：flush + 封区后数据驻留内存但已封存，
  //    点查走纯指针直读路径（生产 flush 后稳态主工况，与可变区 get_hot 互补）
  harness.seal_immutable();
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
  // 读缓冲对齐引擎直读门槛 cb_max_record_size(4096)：对标 C# BfTreeService.ReadByPtr
  // 的 stackalloc 4096 中转口径，走 read_into 直读快路径（512 会触发双拷贝中转）
  let mut read_buf = [0u8; 4096];
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
      criterion::BatchSize::SmallInput,
    );
  });

  group.finish();
}

criterion_group!(benches, bench_wkv_regression, bench_wbftree_regression);
criterion_main!(benches);
