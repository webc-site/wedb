//! 多线程随机读扩展性探针：复刻 harness 第 8-11 段的调用路径
//! （per-thread `connect` → `read_transaction` → `get_reader` → `get`），
//! 每线程固定时长循环读本分片键，输出各线程档吞吐。
//!
//! 用途：多线程读不扩展问题的采样归因与修复 A/B——不进对基口径，
//! 键序列与 harness 同源（fastrand 同种子快进分片），装载后分片键预生成，
//! 计时环内零 RNG 成本，读到的就是纯引擎读路径。

use std::{
  env,
  fs::create_dir_all,
  path::PathBuf,
  sync::{
    Arc, Barrier,
    atomic::{AtomicU64, Ordering},
  },
  thread,
  time::{Duration, Instant},
};

use fastrand::Rng;
use wedb_bench::{Workload, config::REDB_CACHE_SIZE, engines::hash_engine::HashEngine, traits::*};

fn random_pair(rng: &mut Rng, key_size: usize, value_size: usize) -> (Vec<u8>, Vec<u8>) {
  let mut key = vec![0u8; key_size];
  rng.fill(&mut key);
  let mut value = vec![0u8; value_size];
  rng.fill(&mut value);
  (key, value)
}

/// bench harness 同款装载与前置段序复刻：bulk(sync) → individual(sync) →
/// batch(sync) → nosync → 1t 读×3 → range×3，随后逐档单轮多线程读。
/// 用于定位 bench 4t/8t 相对裸装载（sweep 模式）的塌陷来自哪个前置环节
fn run_precise(db: &HashEngine, w: &Workload, threads_list: &[usize], skip_preheat: bool) {
  use std::sync::Barrier;
  let elements = w.loaded_elements();
  let mut rng = Rng::with_seed(w.rng_seed);
  let mut conn = db.connect();
  let mut kb = vec![0u8; w.key_size];
  let mut vb = vec![0u8; w.value_size];
  let t = "precise";

  // bulk
  let s = Instant::now();
  {
    let mut txn = conn.write_transaction();
    {
      let mut ins = txn.get_inserter();
      for _ in 0..w.bulk_elements {
        rng.fill(&mut kb);
        rng.fill(&mut vb);
        ins.insert(&kb, &vb).unwrap();
      }
    }
    txn.commit().unwrap();
  }
  println!(
    "[{t}] bulk {} in {}ms",
    w.bulk_elements,
    s.elapsed().as_millis()
  );

  // individual（每笔 sync 提交）
  let s = Instant::now();
  for _ in 0..w.individual_writes {
    let mut txn = conn.write_transaction();
    {
      let mut ins = txn.get_inserter();
      rng.fill(&mut kb);
      rng.fill(&mut vb);
      ins.insert(&kb, &vb).unwrap();
    }
    txn.commit().unwrap();
  }
  println!(
    "[{t}] individual {} in {}ms",
    w.individual_writes,
    s.elapsed().as_millis()
  );

  // batch
  let s = Instant::now();
  for _ in 0..w.batch_writes {
    let mut txn = conn.write_transaction();
    {
      let mut ins = txn.get_inserter();
      for _ in 0..w.batch_size {
        rng.fill(&mut kb);
        rng.fill(&mut vb);
        ins.insert(&kb, &vb).unwrap();
      }
    }
    txn.commit().unwrap();
  }
  println!(
    "[{t}] batch {}x{} in {}ms",
    w.batch_writes,
    w.batch_size,
    s.elapsed().as_millis()
  );

  // nosync
  conn.set_sync(false);
  let s = Instant::now();
  for _ in 0..w.nosync_writes {
    let mut txn = conn.write_transaction();
    {
      let mut ins = txn.get_inserter();
      rng.fill(&mut kb);
      rng.fill(&mut vb);
      ins.insert(&kb, &vb).unwrap();
    }
    txn.commit().unwrap();
  }
  println!(
    "[{t}] nosync {} in {}ms",
    w.nosync_writes,
    s.elapsed().as_millis()
  );
  drop(conn);

  // 前置 1：单线程读 ×3 轮（median of 3，与 harness 读段同形）
  for r in 0..if skip_preheat { 0 } else { w.read_iterations } {
    let mut rng = Rng::with_seed(w.rng_seed);
    let conn = db.connect();
    let txn = conn.read_transaction();
    let mut reader = txn.get_reader();
    let mut got = vec![0u8; w.value_size];
    let s = Instant::now();
    let mut checksum = 0u64;
    for _ in 0..w.num_reads {
      rng.fill(&mut kb);
      rng.fill(&mut vb);
      reader.get_into(&kb, &mut got).unwrap();
      checksum += got[0] as u64;
    }
    assert!(checksum > 0);
    println!(
      "[{t}] 1t read round {}：{} in {}ms",
      r + 1,
      w.num_reads,
      s.elapsed().as_millis()
    );
  }

  // 前置 2：range reads ×3 轮（RANGE_SCAN_WINDOW 现状形态）
  for r in 0..if skip_preheat { 0 } else { w.scan_iterations } {
    let mut rng = Rng::with_seed(w.rng_seed);
    let conn = db.connect();
    let txn = conn.read_transaction();
    let mut reader = txn.get_reader();
    let s = Instant::now();
    let mut value_sum = 0u64;
    for _ in 0..w.num_scans {
      rng.fill(&mut kb);
      rng.fill(&mut vb);
      let mut iter = reader.range_from(&kb);
      for _ in 0..w.scan_len {
        if let Some((_, value)) = iter.next() {
          value_sum += value[0] as u64;
        } else {
          break;
        }
      }
    }
    assert!(value_sum > 0);
    println!(
      "[{t}] range round {}：{} scans in {}ms",
      r + 1,
      w.num_scans,
      s.elapsed().as_millis()
    );
  }

  // 多线程读：每档单轮（bench 同形：分片快进在计时外主线程完成，
  // 计时含 spawn/connect/barrier）
  for &num_threads in threads_list {
    let barrier = Arc::new(Barrier::new(num_threads));
    let eps = elements / num_threads;
    // harness make_rng_shards 同形：主线程顺序快进，绝不占计时窗
    let mut rngs: Vec<Rng> = Vec::with_capacity(num_threads);
    for i in 0..num_threads {
      let mut rng = Rng::with_seed(w.rng_seed);
      let mut kb = vec![0u8; w.key_size];
      let mut vb = vec![0u8; w.value_size];
      for _ in 0..(i * eps) {
        rng.fill(&mut kb);
        rng.fill(&mut vb);
      }
      rngs.push(rng);
    }
    let start = Instant::now();
    thread::scope(|s| {
      for shard_rng in rngs.iter() {
        let barrier = barrier.clone();
        let conn = db.connect();
        let mut rng = shard_rng.clone();
        s.spawn(move || {
          barrier.wait();
          let txn = conn.read_transaction();
          let mut reader = txn.get_reader();
          let mut kb = vec![0u8; w.key_size];
          let mut vb = vec![0u8; w.value_size];
          let mut got = vec![0u8; w.value_size];
          let mut checksum = 0u64;
          for _ in 0..eps {
            rng.fill(&mut kb);
            rng.fill(&mut vb);
            reader.get_into(&kb, &mut got).unwrap();
            checksum += got[0] as u64;
          }
          assert!(checksum > 0);
        });
      }
    });
    let elapsed = start.elapsed();
    println!(
      "[{t}] threads={num_threads:2} items={} in {}ms ({:.2}M key/s)",
      elements,
      elapsed.as_millis(),
      elements as f64 / elapsed.as_secs_f64() / 1e6
    );
  }
}

fn main() {
  let mut elements = 1_000_000usize;
  let mut secs = 3f64;
  let mut threads: Vec<usize> = vec![1, 2, 4, 8, 16, 32];
  let mut data_path = PathBuf::from("/tmp/mt_probe_data");
  let mut cache = REDB_CACHE_SIZE;
  let mut key_size = 24usize;
  let mut value_size = 150usize;
  // loop：每线程循环读本分片热集（SLC 内，稳态上限）；sweep：每轮按 harness
  // 读段形态把本分片全部键顺序扫一遍再换轮（冷 miss 工况，贴近对基口径）
  let mut mode = "loop".to_string();
  let mut skip_preheat = false;
  // 装载后补一次 sync 提交（触发 flush_all 刷盘封页），二分装载形态差异
  let mut flush_after_load = false;
  // sweep 读循环内改为在线 fill 键值（与 harness 读段同形），量化 fill 税
  let mut fill_timer = false;
  // 装载后冲掉 SLC/LLC（写 3GB 无关内存），让读段从冷缓存起步
  let mut evict_cache = false;
  let mut args = env::args().skip(1);
  while let Some(arg) = args.next() {
    match arg.as_str() {
      "--elements" => elements = args.next().unwrap().parse().unwrap(),
      "--secs" => secs = args.next().unwrap().parse().unwrap(),
      "--threads" => {
        threads = args
          .next()
          .unwrap()
          .split(',')
          .map(|t| t.parse().unwrap())
          .collect();
      }
      "--data-path" => data_path = PathBuf::from(args.next().unwrap()),
      "--cache-mb" => cache = args.next().unwrap().parse::<usize>().unwrap() * 1024 * 1024,
      "--key-size" => key_size = args.next().unwrap().parse().unwrap(),
      "--value-size" => value_size = args.next().unwrap().parse().unwrap(),
      "--mode" => mode = args.next().unwrap(),
      "--no-preheat" => skip_preheat = true,
      "--flush-after-load" => flush_after_load = true,
      "--fill-timer" => fill_timer = true,
      "--evict" => evict_cache = true,
      other => panic!("未知参数 {other}"),
    }
  }
  create_dir_all(&data_path).unwrap();

  let mut w = Workload::redb_standard();
  w.bulk_elements = elements;
  w.key_size = key_size;
  w.value_size = value_size;
  w.cache_size = cache;

  let db = HashEngine::open(&data_path, &w).expect("打开引擎失败");

  if mode == "precise" {
    // 读段量与 0.2 档 bench 对齐（--elements 视作 bulk 数，比例折算）
    let scale = elements as f64 / 5_000_000.0;
    w.num_reads = (1_000_000.0 * scale).max(1.0) as usize;
    w.num_scans = (500_000.0 * scale).max(1.0) as usize;
    run_precise(&db, &w, &threads, skip_preheat);
    return;
  }

  // 装载：同种子序列从头生成 elements 对（与 harness bulk 段同源）
  {
    let mut conn = db.connect();
    conn.set_sync(false);
    let mut rng = Rng::with_seed(w.rng_seed);
    let mut txn = conn.write_transaction();
    let mut ins = txn.get_inserter();
    for _ in 0..elements {
      let (k, v) = random_pair(&mut rng, key_size, value_size);
      ins.insert(&k, &v).unwrap();
    }
    txn.commit().unwrap();
  }

  if evict_cache {
    let t0 = Instant::now();
    let junk = vec![1u8; 3 * 1024 * 1024 * 1024];
    let mut acc = 0u64;
    for c in junk.iter() {
      acc = acc.wrapping_add(*c as u64);
    }
    println!("缓存冲刷：{}ms（acc={acc}）", t0.elapsed().as_millis());
  }

  if flush_after_load {
    let conn = db.connect();
    let mut txn = conn.write_transaction();
    {
      let mut ins = txn.get_inserter();
      ins.insert(b"__flush_probe__", b"v").unwrap();
    }
    let t0 = Instant::now();
    txn.commit().unwrap();
    println!("flush_all 补一发：{}ms", t0.elapsed().as_millis());
  }

  for &num_threads in &threads {
    let barrier = Arc::new(Barrier::new(num_threads));
    // 分片键预生成（harness make_rng_shards 同快进逻辑：第 i 片快进 i*eps 对）
    let eps = elements / num_threads;
    let mut shards: Vec<Vec<(Vec<u8>, Vec<u8>)>> = Vec::with_capacity(num_threads);
    for i in 0..num_threads {
      let mut rng = Rng::with_seed(w.rng_seed);
      for _ in 0..(i * eps) {
        let _ = random_pair(&mut rng, key_size, value_size);
      }
      let mut shard = Vec::with_capacity(eps);
      for _ in 0..eps {
        shard.push(random_pair(&mut rng, key_size, value_size));
      }
      shards.push(shard);
    }

    let deadline = Duration::from_secs_f64(secs);
    let total_ops = Arc::new(AtomicU64::new(0));
    // harness / fill-timer 形态的分片 rng 在主线程顺序快进（bench harness 的
    // make_rng_shards 在计时外），绝不占计时窗
    let mut pre_rngs: Vec<Rng> = Vec::new();
    if mode == "harness" || fill_timer {
      for i in 0..num_threads {
        let mut rng = Rng::with_seed(w.rng_seed);
        for _ in 0..(i * eps) {
          let _ = random_pair(&mut rng, key_size, value_size);
        }
        pre_rngs.push(rng);
      }
    }
    let start = Instant::now();
    let mode = mode.as_str();
    thread::scope(|s| {
      for (ti, shard) in shards.iter().enumerate() {
        let barrier = barrier.clone();
        let total_ops = total_ops.clone();
        let conn = db.connect();
        let shard_idx = ti;
        // 按分片索引取快进好的 rng（clone 拷贝状态，避免 pop 逆序错位）
        let mut harness_rng = pre_rngs.get(ti).cloned();
        s.spawn(move || {
          let i = shard_idx;
          let eps = eps;
          barrier.wait();
          let txn = conn.read_transaction();
          let mut reader = txn.get_reader();
          let mut checksum = 0u64;
          let mut expect = 0u64;
          let mut local = 0u64;
          match mode {
            "loop" => {
              let mut idx = 0usize;
              while start.elapsed() < deadline {
                let (k, v) = &shard[idx];
                let got = reader.get(k).unwrap();
                checksum += AsRef::<[u8]>::as_ref(&got)[0] as u64;
                expect += v[0] as u64;
                local += 1;
                idx += 1;
                if idx == shard.len() {
                  idx = 0;
                }
              }
            }
            "sweep" => {
              if fill_timer {
                // 在线 fill 形态：从本分片序列位置在线生成键值（与 harness
                // 读段同形），fill 成本进计时；键序列与 shard 预生成严格一致
                let mut rng = harness_rng
                  .take()
                  .expect("fill-timer 需要 harness rng（快进已有）");
                let mut kb = vec![0u8; w.key_size];
                let mut vb = vec![0u8; w.value_size];
                let mut got = vec![0u8; w.value_size];
                // 多轮：每轮从分片起点重放（rng 回绕，键集恒在装载区内）
                let round_start = rng.clone();
                let mut first_round = true;
                while start.elapsed() < deadline {
                  rng = round_start.clone();
                  for _ in 0..eps {
                    rng.fill(&mut kb);
                    rng.fill(&mut vb);
                    if first_round {
                      // 首对与预生成分片逐字节对齐自检（fill 后比对）
                      assert_eq!(kb, shard[0].0, "fill-timer 分片序列发散");
                      first_round = false;
                    }
                    reader.get_into(&kb, &mut got).unwrap();
                    checksum += got[0] as u64;
                    expect += vb[0] as u64;
                    local += 1;
                  }
                }
              } else {
                while start.elapsed() < deadline {
                  for (k, v) in shard.iter() {
                    let got = reader.get(k).unwrap();
                    checksum += AsRef::<[u8]>::as_ref(&got)[0] as u64;
                    expect += v[0] as u64;
                    local += 1;
                  }
                }
              }
            }
            // harness 形态：random_pair（两次 Vec 分配 + fill）进计时，复刻
            // harness.rs 读段的每 op 驱动侧成本，A/B 量化其占比
            "harness" => {
              let mut rng = harness_rng.take().expect("harness rng 未初始化");
              // 自检：快进后首对必须与预生成分片首对逐字节一致；该对计入首轮读，
              // 其余 eps-1 对在线生成，保持每 op random_pair 成本在计时内
              let first = random_pair(&mut rng, key_size, value_size);
              assert_eq!(first.0, shard[0].0, "shard {i} 快进后序列发散");
              assert_eq!(first.1, shard[0].1, "shard {i} 快进后序列发散");
              let mut next = Some(first);
              // 单轮：rng 只覆盖装载区一遍，多轮会越过装载区生成库外键
              for _ in 0..eps {
                let (key, value) = next
                  .take()
                  .unwrap_or_else(|| random_pair(&mut rng, key_size, value_size));
                let got = reader.get(&key).unwrap_or_else(|| {
                  panic!(
                    "shard {i} 读到 None：key={:02x?}（装载应为命中）",
                    &key[..8.min(key.len())]
                  )
                });
                checksum += AsRef::<[u8]>::as_ref(&got)[0] as u64;
                expect += value[0] as u64;
                local += 1;
              }
            }
            other => panic!("未知模式 {other}"),
          }
          assert_eq!(checksum, expect, "读值与装载值不一致");
          total_ops.fetch_add(local, Ordering::Relaxed);
        });
      }
    });
    let elapsed = start.elapsed();
    let ops = total_ops.load(Ordering::Relaxed);
    println!(
      "mode={mode} threads={num_threads:2} ops={ops:>12} elapsed={:7.3}s rate={:8.2}M key/s",
      elapsed.as_secs_f64(),
      ops as f64 / elapsed.as_secs_f64() / 1e6
    );
  }
}
