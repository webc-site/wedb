use std::{
  sync::{
    Arc, Barrier,
    atomic::{AtomicBool, AtomicU64, Ordering},
  },
  thread::{self, yield_now},
};

use aok::{OK, Void};
use log::info;
use windex::{DATA_ENTRIES, HashBucketEntry, HashIndex};

use super::support::{HashIndexTestOps, make_address, make_key, make_keys};

/// 验证高并发 RCU CAS 内存原子替换与读者无锁一致性
/// 对标 Tsavorite `BasicLockTests.FunctionsLockTest`
///
/// libs/storage/Tsavorite/cs/test/BasicLockTests.cs:FunctionsLockTest
#[test]
fn test_concurrent_rcu_atomic_update() -> Void {
  info!("高并发 RCU CAS 内存原子替换与读者无锁一致性验证");

  let index = Arc::new(HashIndex::new(32)?);
  let num_keys = 50;
  let keys = Arc::new(make_keys("rcu_record", num_keys));

  for (key_id, key) in keys.iter().enumerate() {
    let initial_addr = (key_id as u64) * 10000 + 1;
    index.insert(key, initial_addr)?;
  }

  let running = Arc::new(AtomicBool::new(true));
  let update_success_count = Arc::new(AtomicU64::new(0));

  let writer_threads = 8;
  let updates_per_thread = 500;
  let mut writer_handles = Vec::new();

  for wid in 0..writer_threads {
    let idx = Arc::clone(&index);
    let success_counter = Arc::clone(&update_success_count);
    let keys = Arc::clone(&keys);
    writer_handles.push(thread::spawn(move || {
      for step in 1..=updates_per_thread {
        for (key_id, key) in keys.iter().enumerate() {
          let base = (key_id as u64) * 10000;
          let new_addr = base + (wid as u64 * 1000) + (step as u64);

          loop {
            let addrs = idx.lookup_vec(key);
            if let Some(&curr_addr) = addrs.first() {
              if idx.update_address(key, curr_addr, new_addr) {
                success_counter.fetch_add(1, Ordering::Relaxed);
                break;
              }
            } else {
              break;
            }
            yield_now();
          }
        }
      }
    }));
  }

  let reader_threads = 8;
  let read_count = Arc::new(AtomicU64::new(0));
  let mut reader_handles = Vec::new();

  for _ in 0..reader_threads {
    let idx = Arc::clone(&index);
    let r = Arc::clone(&running);
    let rcount = Arc::clone(&read_count);
    let keys = Arc::clone(&keys);
    reader_handles.push(thread::spawn(move || {
      while r.load(Ordering::Relaxed) {
        for (key_id, key) in keys.iter().enumerate() {
          let base = (key_id as u64) * 10000;
          let addrs = idx.lookup_vec(key);

          assert!(
            !addrs.is_empty(),
            "并发 RCU 读取中条目丢失: key_id={key_id}"
          );
          for &addr in &addrs {
            assert!(
              addr > 0 && addr <= HashBucketEntry::ADDRESS_MASK,
              "读到非法损坏地址: {addr:#x}"
            );
            assert!(
              addr >= base && addr < base + 10000,
              "读到跨 Key 的篡改地址: addr={addr}, expected_base={base}"
            );
          }
          rcount.fetch_add(1, Ordering::Relaxed);
        }
      }
    }));
  }

  for h in writer_handles {
    h.join().unwrap();
  }

  running.store(false, Ordering::Release);
  for rh in reader_handles {
    rh.join().unwrap();
  }

  info!(
    "RCU 测试完成: 共完成 {} 次原子 CAS 地址更迭，读者并发校验了 {} 次读取",
    update_success_count.load(Ordering::Acquire),
    read_count.load(Ordering::Acquire)
  );

  for (key_id, key) in keys.iter().enumerate() {
    let addrs = index.lookup_vec(key);
    assert_eq!(addrs.len(), 1);
    let final_addr = addrs[0];
    let base = (key_id as u64) * 10000;
    assert!(final_addr >= base && final_addr < base + 10000);
  }

  OK
}

/// 验证 HashIndex.find_tag 快速单槽位探针查找
/// 对标 Tsavorite `TsavoriteBase.FindTag`
#[test]
fn test_find_tag_fast_probe() -> Void {
  info!("严格对标 TsavoriteBase.FindTag 首项快速探针验证");

  let index = HashIndex::new(1)?;
  let tag = 0x1234_u16;

  // 1. 初始空表：find_tag 返回 None
  assert_eq!(
    index.find_tag_by_hash((tag as u64) << HashBucketEntry::HASH_TAG_SHIFT),
    None
  );

  // 2. 槽位 0 设置试探性 Tentative 条目，槽位 1 设置有效条目
  let bucket = &index.buckets[0];
  let addr_tentative = 0x1111;
  let addr_valid = 0x2222;

  let tent_entry = HashBucketEntry::new(addr_tentative, tag, true);
  bucket.entries[0].store(tent_entry.as_raw(), Ordering::Release);

  let valid_entry = HashBucketEntry::new(addr_valid, tag, false);
  bucket.entries[1].store(valid_entry.as_raw(), Ordering::Release);

  // find_tag 必须跳过 tentative 槽位 0，精准命中槽位 1 的有效地址
  assert_eq!(
    index.find_tag_by_hash((tag as u64) << HashBucketEntry::HASH_TAG_SHIFT),
    Some(addr_valid)
  );

  // 3. 将槽位 0 转为有效条目（模拟 CAS 提交）
  let committed_entry_0 = HashBucketEntry::new(addr_tentative, tag, false);
  bucket.entries[0].store(committed_entry_0.as_raw(), Ordering::Release);

  // 此时槽位 0 已有效，find_tag 必须在槽位 0 立即返回 addr_tentative
  assert_eq!(
    index.find_tag_by_hash((tag as u64) << HashBucketEntry::HASH_TAG_SHIFT),
    Some(addr_tentative)
  );

  // 4. 清空主桶，在溢出桶中写入数据
  bucket.entries[0].store(0, Ordering::Release);
  bucket.entries[1].store(0, Ordering::Release);
  assert_eq!(
    index.find_tag_by_hash((tag as u64) << HashBucketEntry::HASH_TAG_SHIFT),
    None
  );

  let ov_idx = index.overflow_pool.allocate()?;
  bucket.set_overflow_index(ov_idx);
  let ov_bucket = index.overflow_pool.get(ov_idx).unwrap();
  let ov_addr = 0x9999;
  let ov_entry = HashBucketEntry::new(ov_addr, tag, false);
  ov_bucket.entries[0].store(ov_entry.as_raw(), Ordering::Release);

  assert_eq!(
    index.find_tag_by_hash((tag as u64) << HashBucketEntry::HASH_TAG_SHIFT),
    Some(ov_addr)
  );

  // 5. 回环判环安全退出
  ov_bucket.set_overflow_index(ov_idx);
  assert_eq!(index.find_tag(b"non_exist_cycle_key"), None);

  OK
}

/// 验证高并发 find_or_create_tag 溢出挂载竞争与 winner 桶深入遍历
/// 对标 Tsavorite `FindOrCreateTag`
///
/// 结束态校验三条不变式（禁止断言空闲栈清空——C# 同类池为机会主义复用，
/// 静止态残留空闲桶属正常语义，见 TsavoriteBase.cs:368-381 败者 Free 归还）：
/// 1. 挂载数精确：160 键 → 主桶 7 槽 + 22 个溢出桶（各 7 槽）
/// 2. 守恒：allocated == 挂载 + 空闲（无泄漏、无重复回收）
/// 3. 竞争残留上界：空闲 ≤ 线程数（末次挂载竞争败者的合法留存）
#[test]
fn test_find_or_create_tag_concurrent_winner_cascade() -> Void {
  info!("高并发 find_or_create_tag 溢出挂载竞争与 winner 深入遍历");

  let index = Arc::new(HashIndex::new(1)?);
  let thread_count = 8;
  let items_per_thread = 20;
  let barrier = Arc::new(Barrier::new(thread_count));
  let mut handles = Vec::new();

  for t in 0..thread_count {
    let idx = Arc::clone(&index);
    let bar = Arc::clone(&barrier);
    handles.push(thread::spawn(move || -> aok::Result<()> {
      bar.wait();
      for i in 0..items_per_thread {
        let key = format!("winner_t{}_i{}", t, i).into_bytes();
        let target_addr = make_address(t as u64 + 1, i as u64 + 1);

        let mut retry = 0;
        loop {
          let mut hei = idx.find_or_create_tag(&key)?;
          if hei.is_found() {
            panic!("不应命中重复 Key: {:?}", String::from_utf8_lossy(&key));
          }
          if hei.try_cas(target_addr) {
            break;
          }
          retry += 1;
          if retry > 1000 {
            panic!("重试次数超过上限，可能发生活锁");
          }
          yield_now();
        }
      }
      OK
    }));
  }

  for h in handles {
    h.join().expect("线程异常退出")?;
  }

  for t in 0..thread_count {
    for i in 0..items_per_thread {
      let key = format!("winner_t{}_i{}", t, i).into_bytes();
      let expected_addr = ((t as u64 + 1) << 32) | (i as u64 + 1);
      assert_eq!(
        index.find_tag(&key),
        Some(expected_addr),
        "Key {:?} 地址未匹配",
        String::from_utf8_lossy(&key)
      );
    }
  }

  // 不变式一：沿桶 0 溢出链统计挂载数，160 键恰好占满 7 + 21×7 = 154 槽、
  // 第 22 个溢出桶余 6 空槽——链长必为 22
  let mut mounted = 0u64;
  let mut next = index.buckets[0].overflow_index();
  while next != 0 {
    mounted += 1;
    assert!(mounted <= 22, "溢出链长度异常：{mounted}");
    let bucket = index
      .overflow_pool
      .get(next)
      .expect("已挂载溢出桶必须可解析");
    let cur = next;
    next = bucket.overflow_index();
    assert_ne!(next, cur, "溢出链自环");
  }
  assert_eq!(mounted, 22, "160 键应恰好挂载 22 个溢出桶");

  // 不变式二（守恒）：分配总数 == 链上挂载数 + 空闲栈数。
  // 竞争败者经 free 全额归还（对标 TsavoriteBase.cs:368-381 Install 失败即
  // overflowBucketsAllocator.Free），既无泄漏也无重复回收
  assert_eq!(
    index.overflow_pool.allocated_count(),
    mounted + index.overflow_pool.free_count(),
    "溢出桶必须严格守恒：allocated == 挂载 + 空闲"
  );

  // 不变式三（竞争残留上界）：末次挂载竞争的败者桶留在空闲栈等待复用
  // （C# MallocFixedPageSize.Free 同语义：池残留为常态，机会主义复用），
  // 每线程同时至多持有一个未挂载桶，残留至多 thread_count 个
  assert!(
    index.overflow_pool.free_count() <= thread_count as u64,
    "空闲残留 {} 超过线程数 {thread_count}",
    index.overflow_pool.free_count()
  );

  OK
}

/// 验证单趟 find_tag_or_insert 与溢出链穿透防重复插入
/// 对标 Tsavorite `find_tag_or_insert`
#[test]
fn test_find_tag_or_insert_single_pass_and_overflow_penetration() -> Void {
  info!("验证单趟 find_tag_or_insert 与溢出链穿透防重复插入");

  let index = HashIndex::new(8)?;
  let key = b"user_balance_key";
  let addr1 = 0x1000;

  // 首次调用：不存在该条目，执行 CAS 插入，返回 (None, true)
  let (existing, inserted) = index.find_tag_or_insert(key, addr1)?;
  assert_eq!(existing, None);
  assert!(inserted);
  assert_eq!(index.find_tag(key), Some(addr1));

  // 第二次调用相同 Key：已存在该条目，返回已有地址 (Some(addr1), false)，不重复插入
  let addr2 = 0x2000;
  let (existing2, inserted2) = index.find_tag_or_insert(key, addr2)?;
  assert_eq!(existing2, Some(addr1));
  assert!(!inserted2);
  assert_eq!(index.find_tag(key), Some(addr1));

  // 溢出链穿透防重复插入测试：容量为 1，目标在溢出桶，主桶槽位被删除后
  let single_index = HashIndex::new(1)?;
  let tag = 0x5555_u16;

  // 填满主桶 7 个槽位
  for (slot, entry_slot) in single_index.buckets[0]
    .entries
    .iter()
    .enumerate()
    .take(DATA_ENTRIES)
  {
    let dummy_addr = (slot as u64) + 1;
    let entry = HashBucketEntry::new(dummy_addr, 0x1000 + slot as u16, false);
    entry_slot.store(entry.as_raw(), Ordering::Release);
  }

  // 在溢出桶中插入目标条目
  let ov_idx = single_index.overflow_pool.allocate()?;
  single_index.buckets[0].set_overflow_index(ov_idx);
  let ov_bucket = single_index.overflow_pool.get(ov_idx).unwrap();
  let target_addr = 0x9999;
  let target_entry = HashBucketEntry::new(target_addr, tag, false);
  ov_bucket.entries[0].store(target_entry.as_raw(), Ordering::Release);

  let hash = (tag as u64) << HashBucketEntry::HASH_TAG_SHIFT;

  // 删除主桶槽位 0 的条目，腾出空槽位
  single_index.buckets[0].entries[0].store(0, Ordering::Release);

  // 对 target 调用 find_tag_or_insert_by_hash：必须穿透并返回已有 target_addr，杜绝在主桶槽位 0 重复插入
  let (existing_pen, inserted_pen) = single_index.find_tag_or_insert_by_hash(hash, 0xAAAA)?;
  assert_eq!(existing_pen, Some(target_addr));
  assert!(
    !inserted_pen,
    "已存在于溢出桶的条目绝对不可被判定为插入成功"
  );
  assert_eq!(
    single_index.buckets[0].entries[0].load(Ordering::Acquire),
    0
  );

  OK
}

/// 验证截断死槽位单趟探针无锁实时清退与就地复用
/// 对标 Tsavorite `TsavoriteBase.cs:338-352`
#[test]
fn test_find_or_create_tag_active_reclamation() -> Void {
  info!("验证截断死槽位单趟探针无锁实时清退与就地复用");

  let index = HashIndex::new(1)?;

  // 填入 7 个旧记录（地址均为 0x100 ~ 0x700）
  for i in 1..=7u64 {
    let key = format!("old_key_{}", i);
    let mut hei = index.find_or_create_tag(key.as_bytes())?;
    assert!(!hei.is_found());
    assert!(hei.try_cas(i * 0x100));
  }

  assert_eq!(index.overflow_pool.allocated_count(), 0);

  // 模拟底层 HybridLog 日志截断推进至 0x500 (0x100 ~ 0x400 槽位截断死亡)
  let min_valid_addr = 0x500;

  // 验证对已截断的旧键重新查询：判定为 NOT FOUND 并原位置零清退
  let old_key_1 = b"old_key_1";
  let hei_old = index.find_or_create_tag_with_min_addr(old_key_1, min_valid_addr)?;
  assert!(!hei_old.is_found());
  assert_eq!(hei_old.address(), 0);

  // 写入全新 Key，带有 min_valid_addr 探针：必须在单趟扫描中原地复用清退槽位，不触发溢出桶分配
  let new_key = b"brand_new_key_1";
  let mut hei = index.find_or_create_tag_with_min_addr(new_key, min_valid_addr)?;
  assert!(!hei.is_found());
  assert!(hei.try_cas(0x9000));

  assert_eq!(
    index.overflow_pool.allocated_count(),
    0,
    "截断死槽位已被原地清退并复用，绝不分配溢出桶"
  );

  let hei_read = index.find_or_create_tag(new_key)?;
  assert!(hei_read.is_found());
  assert_eq!(hei_read.address(), 0x9000);

  // 验证未截断的存活键（old_key_5, 6, 7）依然完好
  for i in 5..=7u64 {
    let key = format!("old_key_{}", i);
    let hei = index.find_or_create_tag_with_min_addr(key.as_bytes(), min_valid_addr)?;
    assert!(hei.is_found());
    assert_eq!(hei.address(), i * 0x100);
  }

  // 验证 ReadCache 防误清保护：带有 READ_CACHE_BIT 标记的条目不被清退
  let rc_addr = 0x100 | HashBucketEntry::READ_CACHE_BIT;
  let rc_key = b"rc_cached_key";
  let mut hei_rc = index.find_or_create_tag_with_min_addr(rc_key, min_valid_addr)?;
  assert!(!hei_rc.is_found());
  assert!(hei_rc.try_cas(rc_addr));

  let hei_rc_check = index.find_or_create_tag_with_min_addr(rc_key, min_valid_addr)?;
  assert!(
    hei_rc_check.is_found(),
    "ReadCache 条目绝不可被当作截断死槽误清！"
  );
  assert_eq!(hei_rc_check.address(), rc_addr);

  OK
}

/// 验证并发 RCU 混合负载（查找、插入、更新、删除）
/// 对标 Tsavorite `FindOrCreateTag` 并发混合工作负载
#[test]
fn test_concurrent_rcu_mixed_workload() -> Void {
  info!("严格对标 Tsavorite FindOrCreateTag 并发 RCU 混合负载测试");

  let index = Arc::new(HashIndex::new(4)?);
  let thread_count = 8;
  let ops_per_thread = 50;
  let barrier = Arc::new(Barrier::new(thread_count));
  let mixed_keys = Arc::new(make_keys("mixed_rcu_key", 20));
  let mut handles = Vec::new();

  for t in 0..thread_count {
    let idx = Arc::clone(&index);
    let bar = Arc::clone(&barrier);
    let keys = Arc::clone(&mixed_keys);
    handles.push(thread::spawn(move || -> aok::Result<()> {
      bar.wait();
      for i in 0..ops_per_thread {
        let key = &keys[(t * 7 + i) % 20];
        let addr1 = make_address(t as u64 + 1, i as u64 * 2 + 1);
        let addr2 = make_address(t as u64 + 1, i as u64 * 2 + 2);

        let (existing, _inserted) = idx.find_tag_or_insert(key, addr1)?;
        let cur_addr = existing.unwrap_or(addr1);

        let _ = idx.update_address(key, cur_addr, addr2);

        let candidates = idx.lookup_candidates(key);
        assert!(!candidates.is_empty(), "必须存在有效候选逻辑地址");
      }
      OK
    }));
  }

  for h in handles {
    h.join().expect("线程异常退出")?;
  }

  OK
}

/// 验证多线程高并发插入与并发 Lookup 无锁一致性
/// 对标 Tsavorite 索引高并发插入读取测试
#[test]
fn test_multithread_concurrent_insert_and_lookup() -> Void {
  info!("多线程高并发插入与并发 Lookup 无锁一致性压力测试");

  let thread_count = 8;
  let items_per_thread = 1000;
  let index = Arc::new(HashIndex::new(64)?);

  let running = Arc::new(AtomicBool::new(true));
  let mut handles = Vec::new();

  for t in 0..thread_count {
    let idx = Arc::clone(&index);
    handles.push(thread::spawn(move || {
      for i in 0..items_per_thread {
        let key = format!("t_{}_item_{}", t, i).into_bytes();
        let addr = ((t as u64 + 1) * 1_000_000 + i as u64 + 1) & HashBucketEntry::ADDRESS_MASK;
        idx.insert(&key, addr).expect("并发插入成功");
      }
    }));
  }

  let reader_count = 4;
  let lookup_count = Arc::new(AtomicU64::new(0));
  let mut reader_handles = Vec::new();
  let t0_keys = Arc::new(make_keys("t_0_item", items_per_thread));

  for _ in 0..reader_count {
    let idx = Arc::clone(&index);
    let r = Arc::clone(&running);
    let counter = Arc::clone(&lookup_count);
    let keys = Arc::clone(&t0_keys);
    reader_handles.push(thread::spawn(move || {
      let mut i = 0;
      while r.load(Ordering::Relaxed) {
        let _ = idx.lookup_vec(&keys[i]);
        counter.fetch_add(1, Ordering::Relaxed);
        i = (i + 1) % items_per_thread;
      }
    }));
  }

  for h in handles {
    h.join().unwrap();
  }

  running.store(false, Ordering::Release);
  for rh in reader_handles {
    rh.join().unwrap();
  }

  info!(
    "写者完成 8000 次插入，读者并发执行了 {} 次 lookup",
    lookup_count.load(Ordering::Acquire)
  );

  for t in 0..thread_count {
    for i in 0..items_per_thread {
      let key = format!("t_{}_item_{}", t, i).into_bytes();
      let expected_addr =
        ((t as u64 + 1) * 1_000_000 + i as u64 + 1) & HashBucketEntry::ADDRESS_MASK;
      let res = index.lookup_vec(&key);
      assert!(
        res.contains(&expected_addr),
        "并发插入丢失条目: t={t}, i={i}, 期待地址: {expected_addr}"
      );
    }
  }

  OK
}

/// 挑选 Tag 互异的测试键
///
/// `hash_key` 走 gxhash 进程级随机种子，Tag 关系不可写死字面量，须运行时求定：
/// 单桶索引内 Tag 互异即保证各键探针各自落在不同槽位，用例零时序依赖、零线程。
fn distinct_tag_keys(prefix: &str, count: usize) -> Vec<Vec<u8>> {
  let mut keys = Vec::with_capacity(count);
  let mut seen: Vec<u16> = Vec::with_capacity(count);
  for id in 0.. {
    let key = make_key(prefix, id);
    let tag = HashBucketEntry::tag_from_hash(HashIndex::hash_key(&key));
    if seen.contains(&tag) {
      continue;
    }
    seen.push(tag);
    keys.push(key);
    if keys.len() == count {
      break;
    }
  }
  keys
}

/// 空闲槽句柄被并发写者抢占后，刷新与定点 CAS 绝不采纳、更绝不覆写他键条目
///
/// 缺陷复现形态（对标 C# TsavoriteBase 中 FindOrCreateTag 的 `InsertInBucketInternal`
/// Tentative 占位前提）：本 crate 探针返回的空闲槽在末尾单次 CAS 落笔前未被占位，
/// `HashEntryInfo::set_to_current` 若无条件采纳槽位当前字，就把他键刚发布的条目当成
/// 「本键旧值」，随后的 `try_cas` 以该 foreign 字为期望值必然成功，把他键条目从索引上
/// 静默抹去——他键记录完好躺在日志里，却再无可命中其 Tag 的槽位，永久 NOTFOUND。
/// wkv 会话高频并发写入实测丢键即此（单线程构造等价复现，无 sleep 无重试）。
#[test]
fn test_free_slot_handle_never_clobbers_foreign_entry() -> Void {
  // 单桶索引 + Tag 互异键：全部条目挤同一桶，空闲槽抢占可确定复现
  let index = HashIndex::new(1)?;
  let keys = distinct_tag_keys("slot_claim", 3);
  let (key_a, key_b, key_c) = (&keys[0], &keys[1], &keys[2]);

  index.insert(key_a, 100)?;

  // B 的探针落在空闲槽（句柄 raw == 0，未占位）
  let mut hei_b = index.find_or_create_tag_by_hash_with_min_addr(HashIndex::hash_key(key_b), 0)?;
  assert!(!hei_b.is_found(), "新键探针须返回空闲槽句柄");

  // 并发写者 C 在同一窗口抢占该空闲槽（单线程等价构造：紧随其后完成落槽）
  let mut hei_c = index.find_or_create_tag_by_hash_with_min_addr(HashIndex::hash_key(key_c), 0)?;
  assert!(!hei_c.is_found(), "C 的探针须与 B 落在同一空闲槽");
  assert!(hei_c.try_cas(300), "C 首次落槽须成功");

  // B 刷新句柄后 CAS：槽位此刻承载 C 的条目（Tag 非 B），须判为抢占失败
  hei_b.set_to_current();
  assert!(
    !hei_b.try_cas(200),
    "空闲槽被他键抢占后本句柄 CAS 必须失败，绝不覆写他键条目"
  );
  assert_eq!(index.lookup_vec(key_c), vec![300], "C 的条目必须完好在册");

  // B 走既有 CAS 败者重试收敛：重新探针即得另一空闲槽
  let mut hei_b = index.find_or_create_tag_by_hash_with_min_addr(HashIndex::hash_key(key_b), 0)?;
  assert!(hei_b.try_cas(200), "重试须落进另一空闲槽");
  assert_eq!(index.lookup_vec(key_a), vec![100], "A 不受影响");
  assert_eq!(index.lookup_vec(key_b), vec![200]);
  assert_eq!(index.lookup_vec(key_c), vec![300]);

  OK
}

/// 命中槽易主后，定点脱钩绝不置零他键条目
///
/// 与 [`test_free_slot_handle_never_clobbers_foreign_entry`] 同源的删除侧形态：
/// 命中句柄取得与刷新之间，本键条目可被并发删除且该槽被他键抢占，
/// 无条件刷新会让 `try_elide` 以他键条目为期望值 CAS 置零，等价于删掉一个从未
/// 被删过的键（对标 C# Helpers 辅助实现中 FindTagAndTryEphemeralXLock 仅在持锁且槽位
/// 为已占位条目前提下才 SetToCurrent）。
#[test]
fn test_refreshed_handle_never_elides_foreign_entry() -> Void {
  let index = HashIndex::new(1)?;
  let keys = distinct_tag_keys("elide_claim", 2);
  let (key_a, key_e) = (&keys[0], &keys[1]);

  index.insert(key_a, 100)?;
  let mut hei_a = index
    .find_tag_entry_by_hash_with_min_addr(HashIndex::hash_key(key_a), 0)
    .expect("A 的命中句柄");
  assert!(hei_a.is_found(), "命中句柄 raw 须为 A 的条目");

  // 句柄取得与刷新之间：A 被脱钩、同一槽位被他键 E 抢占（单线程等价构造）
  assert!(index.delete(key_a, 100), "A 脱钩须成功");
  let mut hei_e = index.find_or_create_tag_by_hash_with_min_addr(HashIndex::hash_key(key_e), 0)?;
  assert!(hei_e.try_cas(500), "E 须落进 A 腾出的槽位");

  hei_a.set_to_current();
  assert!(
    !hei_a.try_elide(),
    "槽位已易主，脱钩必须失败，绝不置零他键条目"
  );
  assert_eq!(index.lookup_vec(key_e), vec![500], "E 的条目必须完好在册");

  OK
}
