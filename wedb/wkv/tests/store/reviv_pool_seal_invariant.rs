//! 复活池「池中槽位恒处 Closed 态」不变式（票 wkv-reviv-pool-transfer-seal-discard-unsealed-pooling）
//!
//! 在 garnet 中的相对路径: libs/storage/Tsavorite/cs/test/test.recordops/RevivificationTests.cs
//! （C# 对位：Helpers.cs:TryTransferToFreeList :128 前置断言 `logRecord.Info.IsClosed`
//! + InternalUpsert.cs:366-372 elide 转移臂 SealAndInvalidate 先行、仅达复活下界方
//!   TryTransferToFreeList，低于下界 OnDispose(Elided) 遗弃）
//!
//! rust 归池单点 [`WedbStore::transfer_to_reviv_pool`] 以 try_seal_record 三态结果
//! 门控：Sealed / AlreadySealed（槽位确认 Closed）方入池；NotSealable（页滑窗驱逐
//! 竞态 / 头部残片）弃归池留痕、交由截断清退。本文件锁 elide 归池正形与「池中无
//! 未闭合槽位」全池复核；三态可分辨与不可密封注入形在 whlog
//! tests/hlog/seal_outcome.rs 锁定。

use std::sync::Arc;

use aok::{OK, Void};
use tempfile::tempdir;
use wdev::SegmentedDevice;
use whlog::{HybridLog, SealOutcome};
use wkv::{StoreConfig, WedbStore};
use wrecord::{HEADER_SIZE, RecordHeader};
use wreviv::FreeRecord;

/// 指定地址当前是否作为空闲槽位存在于复活池分桶（与顶层测试支撑 slot_in_pool 同口，本二进制内联）
fn slot_in_pool(store: &WedbStore<SegmentedDevice>, addr: u64) -> bool {
  store
    .reviv_pool
    .bins
    .iter()
    .flat_map(|bin| bin.slots.iter())
    .any(|slot| !slot.is_empty() && FreeRecord::unpack(slot.raw()).0 == addr)
}

/// 池内全量槽位头复核 SEAL（「池中无未闭合槽位」不变式内省口）
fn assert_pool_all_sealed(store: &WedbStore<SegmentedDevice>) {
  let hlog: &Arc<HybridLog<SegmentedDevice>> = store.hlog();
  let mut checked = 0;
  for bin in &store.reviv_pool.bins {
    for slot in &bin.slots {
      if slot.is_empty() {
        continue;
      }
      let addr = FreeRecord::unpack(slot.raw()).0;
      let page_id = hlog.config.page_id(addr);
      let offset = hlog.config.page_offset(addr);
      assert!(
        hlog.buffer.is_page_loaded(page_id),
        "池内槽位 {addr:#x} 所在页必须驻留内存"
      );
      let guard = hlog.buffer.read_page(page_id);
      let header = RecordHeader::decode_opt(&guard[offset..offset + HEADER_SIZE])
        .expect("池内槽位头必须可解码");
      assert!(
        header.is_closed(),
        "池内槽位 {addr:#x} 必须恒处 Closed 密封态（未密封槽位入池即撕裂险）"
      );
      checked += 1;
    }
  }
  assert!(checked > 0, "前置条件：池内必须有槽位可复核");
}

/// elide 删除整帧归池后，全池槽位头必带 SEALED 位；多键连续删除零回归
#[compio::test]
async fn elided_pool_slots_are_sealed() -> Void {
  let dir = tempdir()?;
  let config = StoreConfig::new(1024, 4 * 1024, 16, 0.5)?.with_revivification(true);
  let device = Arc::new(SegmentedDevice::single_file(
    dir.path().join("reviv_seal.db"),
  )?);
  let store = Arc::new(WedbStore::open(config, device)?);
  let session = store.new_session()?;

  // 垫高尾部：后继键槽位稳居复活水位之上（用例结构前提，与 reviv_window_floor 同形）
  session.upsert(b"reviv:seal:pad", &[b'P'; 1200]).await?;

  // 多键写入后逐一 elide 删除：每条单记录链首走 Seal → 三态门控 → 入池
  let mut addrs = Vec::new();
  for i in 0..4 {
    let key = format!("reviv:seal:k{i}").into_bytes();
    addrs.push(session.upsert(&key, &[b'v'; 256]).await?);
  }
  for (i, addr) in addrs.iter().enumerate() {
    let key = format!("reviv:seal:k{i}").into_bytes();
    assert!(
      session.delete(&key).await?,
      "单记录删除必须触发 Record Elision"
    );
    assert_eq!(session.read(&key).await?, None, "删除后键不存活");
    assert!(slot_in_pool(&store, *addr), "elide 删除槽位必须整帧归池");
  }

  // 全池复核：池中无未闭合槽位（密封先行 + 三态门控的共同承诺）
  assert_pool_all_sealed(&store);

  OK
}

/// 不可密封槽位绝不入池（页未就绪 / 头部解码失败），既有已密封槽位正常入池
#[compio::test]
async fn unsealable_slot_never_pooled_and_already_sealed_normally_pooled() -> Void {
  let dir = tempdir()?;
  let config = StoreConfig::new(1024, 4 * 1024, 16, 0.5)?.with_revivification(true);
  let device = Arc::new(SegmentedDevice::single_file(
    dir.path().join("reviv_seal_unsealable.db"),
  )?);
  let store = Arc::new(WedbStore::open(config, device)?);
  let session = store.new_session()?;

  // 垫高尾部：后继键槽位稳居复活水位之上
  session.upsert(b"reviv:seal:pad", &[b'P'; 1200]).await?;

  // 1. 页未就绪场景：环形页缓冲未装载页面（例如未装载的后继页），不可密封槽位绝不入池
  let unready_addr = 100 * 4096;
  assert!(
    !store
      .hlog()
      .buffer
      .is_page_loaded(store.hlog().config.page_id(unready_addr)),
    "前置条件：目标页未就绪"
  );
  store.transfer_to_reviv_pool(unready_addr, 256);
  assert!(
    !slot_in_pool(&store, unready_addr),
    "页未就绪导致不可密封的槽位绝不入池"
  );

  // 2. 解码失败场景：驻留页内头部残片/非法尺寸导致解码失败，不可密封槽位绝不入池
  let garbage_addr = store.hlog().tail_address();
  let garbage = RecordHeader::new(0, 8, u32::MAX, false)?.to_bytes();
  {
    let page_id = store.hlog().config.page_id(garbage_addr);
    let offset = store.hlog().config.page_offset(garbage_addr);
    let mut guard = store.hlog().buffer.write_page(page_id);
    guard[offset..offset + HEADER_SIZE].copy_from_slice(&garbage);
  }
  store.transfer_to_reviv_pool(garbage_addr, 256);
  assert!(
    !slot_in_pool(&store, garbage_addr),
    "解码失败导致不可密封的槽位绝不入池"
  );

  // 3. 既有已密封场景：槽位先前已处于密封态，调用 transfer_to_reviv_pool 正常归池
  let key = b"reviv:seal:already";
  let addr = session.upsert(key, &[b'v'; 256]).await?;
  let slot_size = wrecord::record_size(key.len(), 256) as u32;

  // 先对槽位单点密封，使其跃迁至密封态
  assert_eq!(store.hlog().try_seal_record(addr), SealOutcome::Sealed);

  // 移交归池：try_seal_record 幂等返回 AlreadySealed，确认 Closed 后正常入池
  store.transfer_to_reviv_pool(addr, slot_size);
  assert!(slot_in_pool(&store, addr), "既有已密封槽位必须正常入池");

  // 全池复核：池中无未闭合槽位
  assert_pool_all_sealed(&store);

  OK
}
