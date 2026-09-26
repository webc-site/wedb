//! 紧缩搬迁的复活池取收编回归（对标 C# CompactionConditionalCopyToTail 经
//! ConditionalCopyToTail → TryCopyToTail 的 `AllocateOptions{recycle:true}` 统一
//! TryAllocateRecord 契约，BlockAllocate.cs:57-82）：预置池槽位后触发紧缩，
//! 首个存活记录的搬迁帧必须落于池槽位——索引 CAS 改指槽位、值完整、槽位脱池；
//! 修复前紧缩搬迁直落纯尾部追加，池取消费面缺紧缩臂，日志空洞收敛失效。

use std::sync::Arc;

use aok::{OK, Void};
use log::info;
use wcompact::LogCompactor;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wrecord::{HEADER_SIZE, record_size};

/// 槽位是否在复活池（与顶层测试支撑 slot_in_pool 同一内省口，本二进制内联）
fn slot_in_pool(store: &WedbStore<SegmentedDevice>, addr: u64) -> bool {
  store
    .reviv_pool
    .bins
    .iter()
    .flat_map(|bin| bin.slots.iter())
    .any(|slot| !slot.is_empty() && slot.address() == addr)
}

#[compio::test]
async fn compact_copy_reuses_pool_slot() -> Void {
  let dir = tempfile::tempdir()?;
  let device = Arc::new(SegmentedDevice::single_file(
    dir.path().join("compact_reviv.db"),
  )?);
  let config = StoreConfig::new(4096, 4 * 1024, 16, 0.5)?.with_revivification(true);
  let store = Arc::new(WedbStore::open(config, device)?);
  let session = store.new_session()?;

  // 4 存活 + 2 删：小窗口保证首个存活记录的搬迁帧由池取承接
  let keys = ["cpk0", "cpk1", "cpk2", "cpk3", "cpk4", "cpk5"];
  for k in keys {
    session
      .upsert(k.as_bytes(), format!("val-{k}").as_bytes())
      .await?;
  }
  assert!(session.delete(b"cpk4").await?);
  assert!(session.delete(b"cpk5").await?);

  // 跨入页 1 后冷却：预置记录全部入磁盘（紧缩窗口内）
  let pad = vec![b'P'; 3000];
  session.upsert(b"cppadk1", &pad).await?;
  session.upsert(b"cppadk2", &pad).await?;
  store.flush_all().await?;
  store.shift_head_address(4 * 1024);

  // 供体：冷却后写尾部（可复活窗口内，键长与值长同搬迁键完全一致、帧同档），
  // upsert+delete 整帧归池
  let phys_d = session.session_string_key(b"cpkd");
  let frame = record_size(phys_d.len(), 8);
  let vd = vec![b'D'; frame - HEADER_SIZE - phys_d.len()];
  assert_eq!(record_size(phys_d.len(), vd.len()), frame);
  let addr_d = session.upsert(b"cpkd", &vd).await?;
  assert!(session.delete(b"cpkd").await?);
  assert!(slot_in_pool(&store, addr_d), "前置条件：供体整帧已归池");

  let hit_before = store.reviv_pool.stats().hit_count;
  let compactor = LogCompactor::new(Arc::clone(&store));
  let stats = compactor.compact_lazy(u64::MAX).await?;
  assert!(!stats.is_empty(), "存在垃圾区间时必须执行紧缩");
  assert!(
    stats.live_copied >= 4,
    "4 条存活记录必须全部搬迁（实搬 {}）",
    stats.live_copied
  );
  assert!(stats.dead_dropped >= 2, "2 条墓碑必须判死丢弃");

  // 首个存活记录 cpk0 的搬迁帧必须复活在供体槽位
  assert!(!slot_in_pool(&store, addr_d), "供体槽位必须被搬迁取走");
  assert_eq!(
    store.reviv_pool.stats().hit_count,
    hit_before + 1,
    "紧缩搬迁必须命中池取"
  );
  let phys_k0 = session.session_string_key(b"cpk0");
  assert_eq!(
    store.index.load().find_tag(phys_k0.as_slice()),
    Some(addr_d),
    "首个存活记录的索引必须 CAS 改指复活槽位"
  );
  assert_eq!(
    session.read(b"cpk0").await?,
    Some(b"val-cpk0".to_vec()),
    "搬迁后值必须完整"
  );

  info!("对照 C# CompactionConditionalCopyToTail 池取臂验证通过");
  OK
}
