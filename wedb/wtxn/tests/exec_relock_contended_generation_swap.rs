#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! EXEC 取锁争用重驱轮物理前缀复验用例
//! （对应 task/ing/wtxn-exec-relock-retry-stale-generation-prefix.md）
//!
//! 缺陷形：run_exec 的 armed 门只让首轮消费 lock_prefix，争用重驱轮整段
//! 跳过、入参前缀被忽略；争用让步窗内会话域换代（FLUSHDB/SWAPDB 物理前缀
//! 变）后，重驱轮仍按旧代桶计划完成取锁——旧代桶闩对新一代写入无防护，
//! 取锁成功到重放首条命令补锁点之间存在无保护写窗。C# 锁身份是 store 裸键
//! 哈希无代际维度，结构性免疫；rust 物理前缀编码引入代际维度，重驱轮须自证
//! 前缀新鲜度。
//!
//! 修复口径：run_exec 复入轮（armed 已真）先比对入参 lock_prefix 与锚定
//! lock_prefix：同源直入单次取闩（行为不变）；异源重走首轮同款门序——注销
//! 旧票据 → 重取屏障 → 放尽旧代锁集 → register_run_preamble 重展开
//! （watch_container/txn_keys 跨轮保全）→ 单次取闩。garnet_api::exec 补锁点
//! 保持不动（Running 重放窗纵深兜底）。
//!
//! 判据验证：
//! 1. 首轮 Contended → 换号 → 重驱：重驱轮持新代桶闩（写键排他、WATCH 键
//!    共享），他连接对新代排队键写入被阻至事务提交，旧代桶无跨代幽灵闩；
//! 2. 异源复入遇扩容屏障占用（PREPARE_GROW）：先注销旧票据再让步，复入
//!    幂等（注销不重不漏），锁集与锚前缀在让步轮原样保留；
//! 3. 票据注册/注销跨换代成对（无扩容排空挂死）。

use std::sync::{
  Arc,
  atomic::{AtomicBool, AtomicUsize, Ordering},
};

use wtxn::{
  ExecRun, LockType, TransactionManager, TxnBarrier, TxnBarrierTicket, TxnKeyEntryComparison,
  TxnLockTable, TxnState, WatchVersionMap,
};

fn create_manager(table: &TxnLockTable) -> (TransactionManager, Arc<WatchVersionMap>) {
  let map = Arc::new(WatchVersionMap::new(64));
  let mgr = TransactionManager::new(table.clone(), Arc::clone(&map), None);
  (mgr, map)
}

/// 搜索使全部 keys 在新旧前缀下离散落桶、且各 key 新代桶互异的前缀对
/// （新旧桶不同令「新代持闩/旧代无闩」断言独立可分，新代互异令
/// 排他/共享强度断言互不污染）
fn find_swap_prefixes(table: &TxnLockTable, keys: &[&[u8]]) -> (&'static [u8], &'static [u8]) {
  let index = table.pin();
  let bucket_of = |prefix: &[u8], key: &[u8]| {
    index.bucket_index_for_hash(TxnKeyEntryComparison::scoped_key_hash(prefix, key) as u64)
  };
  let prefixes: [&'static [u8]; 8] = [
    b"relock_pfx_0",
    b"relock_pfx_1",
    b"relock_pfx_2",
    b"relock_pfx_3",
    b"relock_pfx_4",
    b"relock_pfx_5",
    b"relock_pfx_6",
    b"relock_pfx_7",
  ];
  for i in 0..prefixes.len() {
    for j in (i + 1)..prefixes.len() {
      let old_buckets: Vec<_> = keys.iter().map(|k| bucket_of(prefixes[i], k)).collect();
      let new_buckets: Vec<_> = keys.iter().map(|k| bucket_of(prefixes[j], k)).collect();
      if old_buckets.iter().zip(&new_buckets).any(|(a, b)| a == b) {
        continue;
      }
      let distinct = new_buckets.iter().enumerate().all(|(a, ba)| {
        new_buckets
          .iter()
          .enumerate()
          .all(|(b, bb)| a == b || ba != bb)
      });
      if distinct {
        return (prefixes[i], prefixes[j]);
      }
    }
  }
  panic!("未找到全部键新旧代离散且新代互异的前缀对");
}

/// 验证点 1：首轮 Contended → 换号 → 重驱，重驱轮持新代桶闩且旧代无幽灵闩
#[test]
fn run_exec_contended_then_generation_swap_relocks_new_generation_buckets() {
  let table = TxnLockTable::new();
  let (mut mgr, _) = create_manager(&table);
  let watch_key = b"relock_swap_watch_key";
  let write_key = b"relock_swap_write_key";
  let (old_prefix, new_prefix) = find_swap_prefixes(&table, &[watch_key, write_key]);
  let index = table.pin();
  let bucket_of = |prefix: &[u8], key: &[u8]| {
    index.bucket_index_for_hash(TxnKeyEntryComparison::scoped_key_hash(prefix, key) as u64)
  };
  let old_write_b = bucket_of(old_prefix, write_key);
  let new_write_b = bucket_of(new_prefix, write_key);
  let new_watch_b = bucket_of(new_prefix, watch_key);

  mgr.watch(old_prefix, watch_key);
  mgr.save_key_entry_to_lock(write_key, LockType::Exclusive);
  mgr.state = TxnState::Started;

  // 首轮：他连接持旧代写键桶排他闩 → Contended（armed 置位、锚定旧前缀、键集保留）
  assert!(index.get_bucket(old_write_b).try_lock_exclusive());
  assert_eq!(mgr.run_exec(old_prefix), ExecRun::Contended);
  assert!(mgr.exec_lock_armed, "首轮争用后门控已置位");
  assert_eq!(
    mgr.lock_prefix.as_deref(),
    Some(old_prefix),
    "首轮锚定旧物理前缀"
  );
  assert_eq!(mgr.key_entries.count(), 2, "争用轮键集保留（WATCH+排队键）");

  // 争用让步窗内换代：他连接释放旧代桶，重驱轮传新前缀（异源）
  index.get_bucket(old_write_b).unlock_exclusive();
  assert_eq!(mgr.run_exec(new_prefix), ExecRun::Started);
  assert_eq!(mgr.state, TxnState::Running);

  // 核心断言：写键新代桶持排他闩，他连接写入被阻至提交
  assert_eq!(
    mgr.key_entries.held_bucket_exclusive(new_write_b),
    Some(true),
    "重驱轮按新代桶计划取锁：写键桶排他在闩"
  );
  assert!(
    !index.get_bucket(new_write_b).try_lock_shared(),
    "他连接对新代排队键共享取闩必败（写入被阻）"
  );
  assert!(
    !index.get_bucket(new_write_b).try_lock_exclusive(),
    "他连接对新代排队键排他取闩必败（写入被阻）"
  );
  // WATCH 键新代桶持共享闩（并入重展开面，非只排队键）
  assert_eq!(
    mgr.key_entries.held_bucket_exclusive(new_watch_b),
    Some(false),
    "WATCH 键新代桶持共享闩"
  );
  assert!(
    !index.get_bucket(new_watch_b).try_lock_exclusive(),
    "WATCH 键新代桶他连接排他取闩必败"
  );
  assert!(
    index.get_bucket(new_watch_b).try_lock_shared(),
    "WATCH 键新代桶共享可入（Shared 形态）"
  );
  index.get_bucket(new_watch_b).unlock_shared();
  // 旧代桶无跨代幽灵闩（旧计划已随重展开废弃）
  assert_eq!(
    mgr.key_entries.held_bucket_exclusive(old_write_b),
    None,
    "旧代桶不在持锁记录"
  );
  assert!(
    index.get_bucket(old_write_b).try_lock_exclusive(),
    "旧代桶自由可取（无幽灵闩）"
  );
  index.get_bucket(old_write_b).unlock_exclusive();

  // 提交收尾：新代桶放闩
  mgr.commit(false).unwrap();
  assert_eq!(mgr.state, TxnState::None);
  assert!(
    index.get_bucket(new_write_b).try_lock_exclusive(),
    "提交后新代写键桶放闩"
  );
  index.get_bucket(new_write_b).unlock_exclusive();
  assert!(
    index.get_bucket(new_watch_b).try_lock_exclusive(),
    "提交后新代 WATCH 桶放闩"
  );
  index.get_bucket(new_watch_b).unlock_exclusive();
}

/// 验证点 2：异源复入遇扩容屏障占用，先注销旧票据再让步，复入幂等、锁集保留
#[test]
fn run_exec_swap_reentry_barrier_contended_deregisters_ticket_and_is_idempotent() {
  let gate = Arc::new(AtomicBool::new(true));
  let acquire_cnt = Arc::new(AtomicUsize::new(0));
  let end_cnt = Arc::new(AtomicUsize::new(0));

  // 生产同构 gated 锁表：gate=false 模拟扩容 PrepareGrow 占用，票据带注销计数
  let fallback = TxnLockTable::new();
  let index = fallback.pin();
  let (g, a, e) = (
    Arc::clone(&gate),
    Arc::clone(&acquire_cnt),
    Arc::clone(&end_cnt),
  );
  let table = TxnLockTable::from_loader_gated(
    move || Arc::clone(&index),
    move || {
      if !g.load(Ordering::SeqCst) {
        return None;
      }
      a.fetch_add(1, Ordering::SeqCst);
      Some(Arc::new(CountingEndTxn(Arc::clone(&e))) as TxnBarrierTicket)
    },
  );

  let (mut mgr, _) = create_manager(&table);
  let write_key = b"relock_barrier_swap_key";
  let (old_prefix, new_prefix) = find_swap_prefixes(&table, &[write_key]);
  let index = table.pin();
  let bucket_of = |prefix: &[u8], key: &[u8]| {
    index.bucket_index_for_hash(TxnKeyEntryComparison::scoped_key_hash(prefix, key) as u64)
  };
  let old_write_b = bucket_of(old_prefix, write_key);
  let new_write_b = bucket_of(new_prefix, write_key);

  mgr.save_key_entry_to_lock(write_key, LockType::Exclusive);
  mgr.state = TxnState::Started;

  // 首轮：他连接占旧代桶 → Contended（注册一次、未注销）
  assert!(index.get_bucket(old_write_b).try_lock_exclusive());
  assert_eq!(mgr.run_exec(old_prefix), ExecRun::Contended);
  assert_eq!(acquire_cnt.load(Ordering::SeqCst), 1);
  assert_eq!(end_cnt.load(Ordering::SeqCst), 0);

  // 换号重驱时扩容屏障占用：旧票据先注销（不阻塞排空）→ 注册失败 → Contended
  gate.store(false, Ordering::SeqCst);
  assert_eq!(mgr.run_exec(new_prefix), ExecRun::Contended);
  assert_eq!(end_cnt.load(Ordering::SeqCst), 1, "旧票据已注销");
  assert_eq!(acquire_cnt.load(Ordering::SeqCst), 1, "屏障占用未再注册");
  assert_eq!(mgr.state, TxnState::Started, "让步轮事务态保持 Started");
  assert_eq!(
    mgr.key_entries.count(),
    1,
    "让步轮键集保留（重展开待屏障就绪）"
  );
  assert_eq!(
    mgr.lock_prefix.as_deref(),
    Some(old_prefix),
    "让步轮锚前缀未动（复入再判异源）"
  );

  // 让步复入幂等：注销不重不漏（票据已空）
  assert_eq!(mgr.run_exec(new_prefix), ExecRun::Contended);
  assert_eq!(end_cnt.load(Ordering::SeqCst), 1, "复入注销幂等");

  // 他连接释放旧代桶；扩容完成 → 复入注册成功、重展开新代 → Started
  index.get_bucket(old_write_b).unlock_exclusive();
  gate.store(true, Ordering::SeqCst);
  assert_eq!(mgr.run_exec(new_prefix), ExecRun::Started);
  assert_eq!(acquire_cnt.load(Ordering::SeqCst), 2);
  assert_eq!(
    mgr.key_entries.held_bucket_exclusive(new_write_b),
    Some(true),
    "重展开后按新代桶取锁"
  );

  // 提交收尾：注销成对，无扩容排空挂死
  mgr.commit(false).unwrap();
  assert_eq!(acquire_cnt.load(Ordering::SeqCst), 2);
  assert_eq!(end_cnt.load(Ordering::SeqCst), 2, "注册/注销成对");
}

/// 验证点 3：同源重驱行为不变（锚前缀与键集跨轮恒定，直入单次取闩）
#[test]
fn run_exec_same_prefix_reentry_keeps_anchor_and_keyset_stable() {
  let table = TxnLockTable::new();
  let (mut mgr, _) = create_manager(&table);
  let write_key = b"relock_same_prefix_key";
  let (old_prefix, new_prefix) = find_swap_prefixes(&table, &[write_key]);
  assert_ne!(old_prefix, new_prefix);
  let index = table.pin();
  let bucket_of = |prefix: &[u8], key: &[u8]| {
    index.bucket_index_for_hash(TxnKeyEntryComparison::scoped_key_hash(prefix, key) as u64)
  };
  let write_b = bucket_of(old_prefix, write_key);

  mgr.save_key_entry_to_lock(write_key, LockType::Exclusive);
  mgr.state = TxnState::Started;

  // 首轮争用（他连接占本代桶）
  assert!(index.get_bucket(write_b).try_lock_exclusive());
  assert_eq!(mgr.run_exec(old_prefix), ExecRun::Contended);
  let version = mgr.txn_version;
  assert_ne!(version, 0);

  // 同源复入争用：锚前缀、键集、版本恒定（零重注册开销）
  assert_eq!(mgr.run_exec(old_prefix), ExecRun::Contended);
  assert_eq!(mgr.lock_prefix.as_deref(), Some(old_prefix));
  assert_eq!(mgr.key_entries.count(), 1);
  assert_eq!(mgr.txn_version, version);

  // 释放后同源重驱成功，持本代桶
  index.get_bucket(write_b).unlock_exclusive();
  assert_eq!(mgr.run_exec(old_prefix), ExecRun::Started);
  assert_eq!(mgr.key_entries.held_bucket_exclusive(write_b), Some(true));
  mgr.commit(false).unwrap();
}

/// 注销计数票据（验证注册/注销跨换代成对）
struct CountingEndTxn(Arc<AtomicUsize>);

impl TxnBarrier for CountingEndTxn {
  fn end_txn(&self) {
    self.0.fetch_add(1, Ordering::SeqCst);
  }
}
