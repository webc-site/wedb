//! EXEC 重放窗换代重展开 WATCH 键臂锁表用例
//! （对应 task/ing/wtxn-exec-replay-generation-swap-reexpand-missing-watch-key-arm.md）
//!
//! 验证点：
//! 1. 旧代入锁后 reexpand 至新前缀，新代桶上他连接 try_lock_exclusive 失败（确已持闩）；
//! 2. WATCH 键与排队键在换代后均被增量取闩，旧代持桶完好保持；
//! 3. 换代增量取闩争用时，新代已取桶回滚，旧代持桶依然保持，释放争用后重试成功；
//! 4. 事务提交后，新代桶与旧代桶均被正确释放。

use std::sync::Arc;

use wtxn::{
  ExecRun, LockType, TransactionManager, TxnKeyEntries, TxnKeyEntryComparison, TxnLockTable,
  TxnState, WatchVersionMap,
};

fn create_manager(table: &TxnLockTable) -> (TransactionManager, Arc<WatchVersionMap>) {
  let map = Arc::new(WatchVersionMap::new(64));
  let mgr = TransactionManager::new(table.clone(), Arc::clone(&map), None);
  (mgr, map)
}

/// 选择一对使 key 映射到不同桶的前缀
fn find_distinct_bucket_prefixes(
  table: &TxnLockTable,
  key: &[u8],
) -> (&'static [u8], &'static [u8], usize, usize) {
  let index = table.pin();
  let prefixes: [&'static [u8]; 4] = [b"gen_pfx_0", b"gen_pfx_1", b"gen_pfx_2", b"gen_pfx_3"];
  for i in 0..prefixes.len() {
    for j in (i + 1)..prefixes.len() {
      let h1 = TxnKeyEntryComparison::scoped_key_hash(prefixes[i], key);
      let h2 = TxnKeyEntryComparison::scoped_key_hash(prefixes[j], key);
      let b1 = index.bucket_index_for_hash(h1 as u64);
      let b2 = index.bucket_index_for_hash(h2 as u64);
      if b1 != b2 {
        return (prefixes[i], prefixes[j], b1, b2);
      }
    }
  }
  panic!("未找到离散到不同桶的前缀对");
}

/// 验证点 1：旧代入锁后 reexpand 至新前缀，新代桶上他连接 try_lock_exclusive 失败（确已持闩）
#[test]
fn test_watch_key_reexpand_on_generation_swap_holds_new_bucket_latch() {
  let table = TxnLockTable::new();
  let (mut mgr, _) = create_manager(&table);
  let watch_key = b"watch_reexpand_key";

  let (old_prefix, new_prefix, old_bucket, new_bucket) =
    find_distinct_bucket_prefixes(&table, watch_key);
  assert_ne!(old_bucket, new_bucket, "旧代桶与新代桶必须不同");

  let index = table.pin();
  // 事务前两桶均未被锁
  assert!(index.get_bucket(old_bucket).try_lock_exclusive());
  index.get_bucket(old_bucket).unlock_exclusive();
  assert!(index.get_bucket(new_bucket).try_lock_exclusive());
  index.get_bucket(new_bucket).unlock_exclusive();

  // 1. WATCH 键并进入 EXEC 运行态（旧代入锁）
  mgr.watch(old_prefix, watch_key);
  assert_eq!(mgr.run_exec(old_prefix), ExecRun::Started);
  assert_eq!(mgr.state, TxnState::Running);

  // 旧代桶已被持有 Shared 闩（排他取闩失败）
  assert!(
    !index.get_bucket(old_bucket).try_lock_exclusive(),
    "旧代桶必须已持闩"
  );
  // 新代桶此时尚未持闩
  assert!(
    index.get_bucket(new_bucket).try_lock_exclusive(),
    "换代前新代桶不应被持闩"
  );
  index.get_bucket(new_bucket).unlock_exclusive();

  // 2. 发生换代：调用 reexpand_for_generation_swap 至新前缀
  assert!(
    mgr.reexpand_for_generation_swap(new_prefix),
    "重展开增量取闩应成功"
  );

  // 3. 核心断言：新代桶上他连接 try_lock_exclusive 失败（确已持闩）
  assert!(
    !index.get_bucket(new_bucket).try_lock_exclusive(),
    "新代桶上他连接 try_lock_exclusive 失败（确已持闩）"
  );

  // 他连接事务尝试以排他锁加锁新代桶，也必须争用失败
  let new_hash = TxnKeyEntryComparison::scoped_key_hash(new_prefix, watch_key);
  let mut other = TxnKeyEntries::new(2, table.clone());
  other.add_key(
    new_hash,
    whasher::fast_hash_i64(watch_key),
    LockType::Exclusive,
  );
  assert!(
    !other.try_lock_all_keys_once(),
    "他连接事务对新代桶加排他锁必须争用失败"
  );

  // 旧代桶上的闩依然保持
  assert!(
    !index.get_bucket(old_bucket).try_lock_exclusive(),
    "旧代桶闩在重展开后依然保持"
  );

  // 4. 提交事务，新旧代桶上的闩均被释放
  mgr.commit(false).unwrap();
  assert_eq!(mgr.state, TxnState::None);

  assert!(
    index.get_bucket(new_bucket).try_lock_exclusive(),
    "提交后新代桶闩释放"
  );
  index.get_bucket(new_bucket).unlock_exclusive();

  assert!(
    index.get_bucket(old_bucket).try_lock_exclusive(),
    "提交后旧代桶闩释放"
  );
  index.get_bucket(old_bucket).unlock_exclusive();
}

/// 验证点 2：WATCH 键（Shared）与排队命令键（Exclusive）换代后均增量取闩
#[test]
fn test_watch_and_txn_keys_both_reexpanded_on_generation_swap() {
  let table = TxnLockTable::new();
  let (mut mgr, _) = create_manager(&table);
  let watch_key = b"combined_watch_key";
  let cmd_key = b"combined_cmd_key";

  let (old_prefix, new_prefix, watch_old_b, watch_new_b) =
    find_distinct_bucket_prefixes(&table, watch_key);
  assert_ne!(watch_old_b, watch_new_b);

  let index = table.pin();

  // WATCH 键 + 排队独占键
  mgr.watch(old_prefix, watch_key);
  mgr.save_key_entry_to_lock(cmd_key, LockType::Exclusive);

  assert_eq!(mgr.run_exec(old_prefix), ExecRun::Started);

  // 换代重展开
  assert!(mgr.reexpand_for_generation_swap(new_prefix));

  // WATCH 键新代桶持有 Shared 闩：他连接 try_lock_exclusive 失败，但 try_lock_shared 成功
  assert!(
    !index.get_bucket(watch_new_b).try_lock_exclusive(),
    "WATCH 键新代桶他连接排他取闩必须失败"
  );
  assert!(
    index.get_bucket(watch_new_b).try_lock_shared(),
    "WATCH 键新代桶他连接共享取闩应成功"
  );
  index.get_bucket(watch_new_b).unlock_shared();

  // 排队命令键新代桶持有 Exclusive 闩：他连接 try_lock_shared 与 try_lock_exclusive 均失败
  let cmd_new_hash = TxnKeyEntryComparison::scoped_key_hash(new_prefix, cmd_key);
  let cmd_new_b = index.bucket_index_for_hash(cmd_new_hash as u64);
  assert!(
    !index.get_bucket(cmd_new_b).try_lock_shared(),
    "Exclusive 排队键新代桶他连接共享取闩必须失败"
  );
  assert!(
    !index.get_bucket(cmd_new_b).try_lock_exclusive(),
    "Exclusive 排队键新代桶他连接排他取闩必须失败"
  );

  mgr.commit(false).unwrap();
}

/// 验证点 3：换代增量取闩争用时，新代已取桶回滚，旧代持桶依然保持，释放争用后重试成功
#[test]
fn test_watch_key_reexpand_contention_rolls_back_and_preserves_old_locks() {
  let table = TxnLockTable::new();
  let (mut mgr, _) = create_manager(&table);
  let watch_key = b"contention_watch_key";

  let (old_prefix, new_prefix, old_bucket, new_bucket) =
    find_distinct_bucket_prefixes(&table, watch_key);
  assert_ne!(old_bucket, new_bucket);

  let index = table.pin();

  mgr.watch(old_prefix, watch_key);
  assert_eq!(mgr.run_exec(old_prefix), ExecRun::Started);

  // 外部他连接预先锁住 new_bucket，制造换代重展开争用
  assert!(index.get_bucket(new_bucket).try_lock_exclusive());

  // 重展开因争用返回 false
  assert!(
    !mgr.reexpand_for_generation_swap(new_prefix),
    "争用时重展开必须返回 false"
  );

  // 旧代桶上的闩仍应被 mgr 完好持有
  assert!(
    !index.get_bucket(old_bucket).try_lock_exclusive(),
    "争用失败后旧代桶闩不得被丢弃"
  );

  // 外部释放争用
  index.get_bucket(new_bucket).unlock_exclusive();

  // 重试重展开成功
  assert!(
    mgr.reexpand_for_generation_swap(new_prefix),
    "争用解除后重展开必须成功"
  );
  assert!(
    !index.get_bucket(new_bucket).try_lock_exclusive(),
    "重展开成功后新代桶确已持闩"
  );

  mgr.commit(false).unwrap();
}

struct RelockCollision {
  old_prefix: Vec<u8>,
  new_prefix: Vec<u8>,
  watch_key: Vec<u8>,
  write_key: Vec<u8>,
  batch_key: Vec<u8>,
}

/// 搜索「新代写键桶 == 旧代 WATCH Shared 持桶」的同桶碰撞代际组：
/// 返回 RelockCollision，满足
/// 1. bucket_old(watch_key) = B，bucket_new(write_key) = B（升闩碰撞桶）；
/// 2. bucket_old(write_key) = B1 != B（旧代不预先并桶，保住「先持弱后来要强」形态）；
/// 3. bucket_new(watch_key) = Bw 不落入旧代持桶集（保持新增共享槽独立可断言）；
/// 4. bucket_new(batch_write_key) = B2 不落入任何持桶集且 B2 > B、bucket_old(batch_write_key)
///    亦不落入持桶集（用于「升闩已完成、他连接先占新桶排他闩令整批失败」的回滚臂）。
fn find_relock_collision(table: &TxnLockTable) -> RelockCollision {
  let index = table.pin();
  let bucket_of = |prefix: &[u8], key: &[u8]| -> usize {
    index.bucket_index_for_hash(TxnKeyEntryComparison::scoped_key_hash(prefix, key) as u64)
  };
  for i in 0..32u32 {
    for j in 0..32u32 {
      let old_prefix = format!("rl_old_{i}").into_bytes();
      let new_prefix = format!("rl_new_{j}").into_bytes();
      for a in 0..8u32 {
        let watch_key = format!("rl_watch_{a}").into_bytes();
        let b_up = bucket_of(&old_prefix, &watch_key);
        for b in 0..4096u32 {
          let write_key = format!("rl_write_{b}").into_bytes();
          let b1 = bucket_of(&old_prefix, &write_key);
          if b1 == b_up || bucket_of(&new_prefix, &write_key) != b_up {
            continue;
          }
          let bw = bucket_of(&new_prefix, &watch_key);
          if bw == b_up || bw == b1 {
            continue;
          }
          for c in 0..8192u32 {
            let batch_key = format!("rl_batch_{c}").into_bytes();
            let b2 = bucket_of(&new_prefix, &batch_key);
            let b2_old = bucket_of(&old_prefix, &batch_key);
            if b2 > b_up
              && b2 != b_up
              && b2 != b1
              && b2 != bw
              && b2_old != b_up
              && b2_old != b1
              && b2_old != bw
              && b2_old != b2
            {
              return RelockCollision {
                old_prefix,
                new_prefix,
                watch_key,
                write_key,
                batch_key,
              };
            }
          }
        }
      }
    }
  }
  panic!("未找到同桶碰撞代际组");
}

/// 锁测 (a) 成功臂：旧代 Shared 持桶 + 新代同桶 Exclusive 条目的增量重展开，
/// 该桶升闩为排他（is_latched_exclusive 真、他连接共享取闩亦败），终局 commit 放闩且幂等
#[test]
fn test_incremental_relock_upgrades_shared_bucket_to_exclusive() {
  let table = TxnLockTable::new();
  let (mut mgr, _) = create_manager(&table);
  let RelockCollision {
    old_prefix,
    new_prefix,
    watch_key,
    write_key,
    ..
  } = find_relock_collision(&table);
  let index = table.pin();
  let b_up = index
    .bucket_index_for_hash(TxnKeyEntryComparison::scoped_key_hash(&old_prefix, &watch_key) as u64);

  mgr.watch(old_prefix.as_slice(), &watch_key);
  mgr.save_key_entry_to_lock(&write_key, LockType::Exclusive);
  assert_eq!(mgr.run_exec(old_prefix.as_slice()), ExecRun::Started);

  // 旧代形态：碰撞桶仅持 Shared（他连接共享可入、排他被拒）
  assert!(
    !index.get_bucket(b_up).try_lock_exclusive(),
    "旧代该桶已被 WATCH 持闩"
  );
  assert!(
    index.get_bucket(b_up).try_lock_shared(),
    "旧代该桶仅持 Shared"
  );
  index.get_bucket(b_up).unlock_shared();

  assert!(
    mgr.reexpand_for_generation_swap(new_prefix.as_slice()),
    "同桶碰撞的换代重展开必须走升闩并成功"
  );

  // 核心断言：升闩后该桶排他闩在位、自家旧 Shared 已释出
  assert!(
    index.get_bucket(b_up).is_latched_exclusive(),
    "升闩后该桶必持排他闩"
  );
  assert!(
    !index.get_bucket(b_up).is_latched_shared(),
    "升闩后自家旧 Shared 必已释放"
  );
  assert!(
    !index.get_bucket(b_up).try_lock_shared(),
    "他连接共享取闩亦败（held 排他槽落真的可观测形态）"
  );
  // held 槽强度收口：写键按排他要求判覆盖；WATCH 键新代桶仅 Shared，按排他要求不覆盖
  let wk: &[u8] = &write_key;
  let wa: &[u8] = &watch_key;
  assert!(
    mgr
      .key_entries
      .covers_user_keys(new_prefix.as_slice(), [wk], true),
    "升闩成功槽按新强度落槽：排他要求必判覆盖"
  );
  assert!(
    !mgr
      .key_entries
      .covers_user_keys(new_prefix.as_slice(), [wa], true),
    "仅 Shared 持桶对排他要求必判不覆盖"
  );
  assert!(
    mgr
      .key_entries
      .covers_user_keys(new_prefix.as_slice(), [wa], false),
    "仅 Shared 持桶对共享要求照常覆盖"
  );
  assert_eq!(
    mgr.key_entries.held_bucket_exclusive(b_up),
    Some(true),
    "held 内该槽 exclusive 落真"
  );

  mgr.commit(false).unwrap();
  // 终局 commit 后该桶闩全释放且放闩幂等
  assert!(
    index.get_bucket(b_up).try_lock_exclusive(),
    "提交后该桶排他闩已释放"
  );
  index.get_bucket(b_up).unlock_exclusive();
  mgr.key_entries.unlock_all_keys();
  mgr.key_entries.unlock_all_keys();
  assert!(
    index.get_bucket(b_up).try_lock_exclusive(),
    "重复放闩不得撕裂闩态"
  );
  index.get_bucket(b_up).unlock_exclusive();
}

/// 锁测 (a) 争用臂一：升闩步争用——他连接持该桶共享闩令排他升闩必败，
/// 整批判 false 且旧 Shared 原样回补；他连接释闩后重驱成功
#[test]
fn test_incremental_relock_upgrade_step_contended_restores_shared() {
  let table = TxnLockTable::new();
  let (mut mgr, _) = create_manager(&table);
  let RelockCollision {
    old_prefix,
    new_prefix,
    watch_key,
    write_key,
    ..
  } = find_relock_collision(&table);
  let index = table.pin();
  let b_up = index
    .bucket_index_for_hash(TxnKeyEntryComparison::scoped_key_hash(&old_prefix, &watch_key) as u64);

  mgr.watch(old_prefix.as_slice(), &watch_key);
  mgr.save_key_entry_to_lock(&write_key, LockType::Exclusive);
  assert_eq!(mgr.run_exec(old_prefix.as_slice()), ExecRun::Started);

  {
    // 他连接先占该碰撞桶（与自家旧 Shared 兼容入册），排他读者排空必败
    let mut other = TxnKeyEntries::new(1, table.clone());
    other.add_key(
      TxnKeyEntryComparison::scoped_key_hash(&new_prefix, &write_key),
      whasher::fast_hash_i64(&write_key),
      LockType::Shared,
    );
    assert!(
      other.try_lock_all_keys_once(),
      "他连接共享条目与自家旧 Shared 兼容入册"
    );

    assert!(
      !mgr.reexpand_for_generation_swap(new_prefix.as_slice()),
      "升闩步争用时增量臂必回 false"
    );
    // 旧 Shared 原样回补：该桶自家共享闩在位、无排他闩，他连接再取排他必败
    assert!(
      index.get_bucket(b_up).is_latched_shared(),
      "失败臂后旧 Shared 必已原样回补"
    );
    assert!(
      !index.get_bucket(b_up).is_latched_exclusive(),
      "升闩失败不得留排他闩"
    );
    assert!(
      !index.get_bucket(b_up).try_lock_exclusive(),
      "回补的旧 Shared 仍挡他连接排他取闩（覆盖未被吞）"
    );

    // 他连接释闩后重驱：升闩成功
    other.unlock_all_keys();
  }
  assert!(
    mgr.reexpand_for_generation_swap(new_prefix.as_slice()),
    "争用解除后重展开（升闩）必须成功"
  );
  assert!(
    index.get_bucket(b_up).is_latched_exclusive(),
    "重驱后该桶排他闩在位"
  );
  mgr.commit(false).unwrap();
}

/// 锁测 (a) 争用臂二：他连接先占新增桶排他闩令整批失败时，已完成升闩桶
/// 降回并原样回补旧 Shared、已取新增桶逆序放闩（整批对称回滚），重驱终全持
#[test]
fn test_incremental_relock_batch_rollback_restores_upgraded_and_new_slots() {
  let table = TxnLockTable::new();
  let (mut mgr, _) = create_manager(&table);
  let RelockCollision {
    old_prefix,
    new_prefix,
    watch_key,
    write_key,
    batch_key,
  } = find_relock_collision(&table);
  let index = table.pin();
  let bucket_of = |prefix: &[u8], key: &[u8]| -> usize {
    index.bucket_index_for_hash(TxnKeyEntryComparison::scoped_key_hash(prefix, key) as u64)
  };
  let b_up = bucket_of(&old_prefix, &watch_key);
  let b_new = bucket_of(&new_prefix, &batch_key);

  mgr.watch(old_prefix.as_slice(), &watch_key);
  mgr.save_key_entry_to_lock(&write_key, LockType::Exclusive);
  mgr.save_key_entry_to_lock(&batch_key, LockType::Exclusive);
  assert_eq!(mgr.run_exec(old_prefix.as_slice()), ExecRun::Started);

  // 他连接先占新增桶 b_new 的排他闩（升闩桶 b_up < b_new 先完成，随后本桶败）
  assert!(
    index.get_bucket(b_new).try_lock_exclusive(),
    "他连接预占新增桶排他闩"
  );

  assert!(
    !mgr.reexpand_for_generation_swap(new_prefix.as_slice()),
    "他连接先占该桶排他闩时增量臂整批必回 false"
  );
  // 已完成升闩桶的旧 Shared 原样回补；他连接对该新增桶的共享取闩亦败（排他仍在其手）
  assert!(
    index.get_bucket(b_up).is_latched_shared(),
    "回滚臂必把升闩桶旧 Shared 原样回补"
  );
  assert!(
    !index.get_bucket(b_up).is_latched_exclusive(),
    "回滚臂必降回升闩桶的排他闩"
  );
  assert!(
    !index.get_bucket(b_new).try_lock_shared(),
    "该新增桶他连接排他闩在位，共享取闩亦败"
  );

  index.get_bucket(b_new).unlock_exclusive();
  assert!(
    mgr.reexpand_for_generation_swap(new_prefix.as_slice()),
    "争用解除后重展开必须成功"
  );
  assert!(
    index.get_bucket(b_up).is_latched_exclusive(),
    "重驱后升闩桶排他闩在位"
  );
  assert!(
    !index.get_bucket(b_new).try_lock_exclusive(),
    "重驱后新增桶排他闩在位"
  );
  mgr.commit(false).unwrap();
  assert!(
    index.get_bucket(b_up).try_lock_exclusive(),
    "提交后升闩桶放闩"
  );
  index.get_bucket(b_up).unlock_exclusive();
  assert!(
    index.get_bucket(b_new).try_lock_exclusive(),
    "提交后新增桶放闩"
  );
  index.get_bucket(b_new).unlock_exclusive();
}
