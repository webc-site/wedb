//! 复活池入池水位回归：槽位归还门槛必须与出池 `take` 同取
//! `min_revivifiable_address`（对标 C# Helpers.cs:GetMinRevivifiableAddress、
//! FreeRecordPool.cs:TryAdd/TryAddToBin 两端统一水位），杜绝缓冲窗死槽污染。
//!
//! 与 `reviv.rs`（默认 `revivifiable_fraction = 1.0`，`min == read_only`，
//! 水位差异不显形）互补：本文件把 `revivifiable_fraction` 收紧为 0.5，令缓冲窗
//! `[read_only, min_revivifiable)` 具化为非空区间，用同一删除 elide 归池路径、仅改
//! 槽位地址相对水位的位置，做正反双断言锁定入池门槛。
//!
//! 在 garnet 中的相对路径: libs/storage/Tsavorite/cs/test/test.recordops/RevivificationTests.cs（自研复活水位改良）

use std::sync::atomic::Ordering;

use aok::{OK, Void};
use wbase::align::DEFAULT_SECTOR_SIZE;

use crate::support::{config, open_store, slot_in_pool};

/// 负断言：落在缓冲窗 `[read_only, min_revivifiable)` 内的死槽，删除 elide 归池时
/// 必须被入池门槛拒收——put 被调用（put_count 递增）但门槛丢弃（drop_count 递增）、
/// 槽位绝不进入分桶、也绝不会被 `take` 交出。修复前该处误传 `read_only_address`，
/// 死槽被塞入分桶挤占容量，出池再按 `min_revivifiable` 淘汰清零，即本用例锁定之回归。
#[compio::test]
async fn test_elide_below_min_watermark_not_pooled() -> Void {
  let page_size = DEFAULT_SECTOR_SIZE;
  let env = open_store(
    "reviv_wm_below.db",
    config(1024, page_size, 16)?
      .with_revivification(true)
      .with_revivifiable_fraction(0.5)?,
  )?;
  let store = env.store;
  let session = store.new_session()?;

  // 目标键先落最低位，其后追加足量填充抬高 tail，使 read_only=0 下
  // min_revivifiable = tail/2 恒高于该槽位，令其落入缓冲窗
  let k = b"wm_below_key";
  let v = b"wm_val_payload_0001";
  let addr_k = session.upsert(k, v).await?;
  let pad = vec![b'P'; 300];
  for i in 0..6 {
    session
      .upsert(format!("wm_below_pad_{i}").as_bytes(), &pad)
      .await?;
  }

  let min_addr = store.min_revivifiable_address();
  assert!(
    addr_k >= store.hlog.read_only_address() && addr_k < min_addr,
    "前置：目标槽位须落在缓冲窗 [read_only, min_revivifiable) 内"
  );

  let put_before = store.reviv_pool.put_count.load(Ordering::Relaxed);
  let drop_before = store.reviv_pool.drop_count.load(Ordering::Relaxed);

  // 删除单版本可变区链首 → 触发 elide 归池路径（同 test_revivification 步 1）
  assert!(session.delete(k).await?, "无前驱可变区记录删除应脱钩成功");
  assert_eq!(session.read(k).await?, None);

  // 门槛确被调用且判拒：put 计数递增、死槽计入 drop，且未占据分桶
  assert!(
    store.reviv_pool.put_count.load(Ordering::Relaxed) > put_before,
    "elide 归池路径应触达入池门槛"
  );
  assert!(
    store.reviv_pool.drop_count.load(Ordering::Relaxed) > drop_before,
    "低于 min_revivifiable 的槽位应在门槛被丢弃并计入 drop_count"
  );
  assert!(
    !slot_in_pool(&store, addr_k),
    "缓冲窗死槽绝不得进入分桶（修复前 read_only 门槛会误纳）"
  );
  OK
}

/// 正断言：位于复活窗内（地址 >= min_revivifiable）的可复活槽位，删除 elide 归池后
/// 必须正常入桶并可被同水位 `take` 取出复用——证明门槛收紧只挡缓冲窗死槽，
/// 不误伤合法复活。
#[compio::test]
async fn test_elide_within_window_admitted_and_revivable() -> Void {
  let page_size = DEFAULT_SECTOR_SIZE;
  let env = open_store(
    "reviv_wm_within.db",
    config(1024, page_size, 16)?
      .with_revivification(true)
      .with_revivifiable_fraction(0.5)?,
  )?;
  let store = env.store;
  let session = store.new_session()?;

  // 先追加填充抬高 tail，再落目标键于尾部高位，使其地址 >= min_revivifiable = tail/2
  let pad = vec![b'P'; 300];
  for i in 0..6 {
    session
      .upsert(format!("wm_within_pad_{i}").as_bytes(), &pad)
      .await?;
  }
  let k = b"wm_within_key";
  let v = b"wm_val_payload_0001";
  let addr_k = session.upsert(k, v).await?;

  let min_addr = store.min_revivifiable_address();
  assert!(
    addr_k >= min_addr,
    "前置：目标槽位须落在复活窗内（>= min_revivifiable）"
  );

  assert!(session.delete(k).await?, "无前驱可变区记录删除应脱钩成功");
  assert_eq!(session.read(k).await?, None);

  // 合法槽位应正常入桶
  assert!(
    slot_in_pool(&store, addr_k),
    "复活窗内槽位必须成功归还入分桶"
  );

  // 并可被同水位 take 取出（缓冲窗内槽位可正常复活）
  let frame = wrecord::record_size(session.session_string_key(k).len(), v.len()) as u32;
  let taken = store.reviv_pool.take(
    frame,
    store.min_revivifiable_address(),
    store.min_revivifiable_address(),
  );
  assert_eq!(
    taken.map(|(addr, _)| addr),
    Some(addr_k),
    "取槽须以 min_revivifiable 水位命中刚归还的复活窗内槽位"
  );
  OK
}
