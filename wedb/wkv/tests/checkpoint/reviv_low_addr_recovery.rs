//! 复活池低地址指向 × 恢复结算差异用例：判活唯一取「被索引引用」
//!
//! 任务票 wkv-cpr-host-recovery-comment-divergence（4564030 RI 存根恢复落笔票
//! 票面语境）。C# 对位：Recovery/Recovery.cs:ClearBitsOnPage →
//! GarnetRecordTriggers.cs:OnRecoverySnapshotRead →
//! RangeIndexManager.Index.cs:MarkRecoveredFromCheckpoint——恢复期逐记录对
//! 快照区 RI 桩置位，恢复后一切路由与 RIPROMOTE 一律按哈希桶条目解析，
//! 索引槽位实际指向的存活记录必带恢复位。
//!
//! 复活池使新写复用更低地址槽位后，索引可指向低地址复活记录、高位残留同键
//! 旧桩死帧（SEALED 位纯易失，盘上帧无从判别死活）；旧「按键排序保最高
//! 地址」判据在此形态上会让存活帧漏落 mark_recovered、死帧反被改写重挂。
//! 本用例直构该持久形态（与 recovery_single_pass.rs 素材③「直写日志、从不
//! CAS」同一构造法），恢复结算后三项判据：
//! 1. 索引槽位指向的帧（低地址存活帧）句柄清零 + 标记恢复；
//! 2. 高位同键陈旧死帧字节零改写（恢复位仍 0、句柄原样保留）；
//! 3. 恢复后经索引路由读历史字段照常（pending 注册与惰性开树不受影响）。
//!
//! 自研依据: reviv 低地址地板随快照恢复（复活下界语义对标 libs/storage/Tsavorite/cs/test/test.recordops/RevivificationTests.cs）

use std::{fs::create_dir_all, sync::Arc};

use aok::{OK, Void};
use tempfile::tempdir;
use wbftree::{RANGE_INDEX_STUB_SIZE, RangeIndexStub, StorageBackendType, TreeTuning};
use wcpr::CheckpointType;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wval::META_VALUE_SIZE;

/// 从 RI 元记录值体取存根视图（Meta 定长段后随 35B 存根窗口）
fn stub_of(val: &[u8]) -> RangeIndexStub {
  RangeIndexStub::decode(&val[META_VALUE_SIZE..META_VALUE_SIZE + RANGE_INDEX_STUB_SIZE])
    .expect("RI 元记录存根窗口必须可解码")
}

/// 复活池低地址指向形态下，恢复结算只改写被索引引用的存活帧
#[compio::test]
async fn recovery_settle_marks_index_referenced_stub_not_highest_addr() -> Void {
  let dir = tempdir()?;
  let db_path = dir.path().join("reviv_low_addr.db");
  let ckpt_dir = dir.path().join("checkpoints");
  let ri_dir = dir.path().join("ri_data");
  create_dir_all(&ri_dir)?;
  create_dir_all(&ckpt_dir)?;

  const TUNE: TreeTuning = TreeTuning {
    cache_size: 65536,
    min_record_size: 8,
    max_record_size: 1024,
    max_key_len: 128,
    leaf_page_size: 0,
  };

  // 复活池启用位与恢复装配后的持久化 StoreMeta 同源（恢复侧回灌），此处
  // 显式开启以贴合票面场景的进程形态
  let config = StoreConfig::new(1024, 64 * 1024, 16, 0.5)?
    .with_range_index_dir(&ri_dir)
    .with_revivification(true);

  let token;
  let stale_addr;
  let stale_handle_before;
  {
    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
    let store = Arc::new(WedbStore::open(config.clone(), Arc::clone(&device))?);
    let session = store.new_session()?;

    session
      .range_index_create(b"orders", StorageBackendType::Memory, TUNE)
      .await?;
    session
      .range_index_set(b"orders", b"user:1", b"balance_100")
      .await?;

    // 索引槽位当前指向的存活帧（复活池场景中的低地址复活记录）
    let meta_k = session.session_meta_key(b"orders");
    let live_addr = store
      .index
      .load()
      .find_tag(&meta_k)
      .expect("RI 建索引后槽位必挂载");
    let live_val = session.read_raw(&meta_k).await?.expect("存活帧必可读");
    let live_stub = stub_of(&live_val);
    assert!(
      live_stub.tree_handle != 0 && !live_stub.is_recovered(),
      "检查点前存活帧须为活树形态: handle={}",
      live_stub.tree_handle
    );

    // 高位陈旧死帧持久形态：直写日志、从不 CAS——槽位仍指低地址存活帧，
    // 盘上该帧与活帧无从判别（SEALED 纯易失不留痕），恢复扫描必访之
    stale_handle_before = live_stub.tree_handle;
    {
      let _guard = session.participant().enter();
      stale_addr = store.hlog.append(&meta_k, &live_val, live_addr, false)?.0
    }
    assert!(
      stale_addr > live_addr,
      "构造形态要求死帧地址高于被引用存活帧: live={live_addr:#x} stale={stale_addr:#x}"
    );

    let meta = store
      .create_checkpoint(&ckpt_dir, CheckpointType::FoldOver)
      .await?;
    token = meta.token;

    drop(session);
    drop(store);
    drop(device);
  } // 模拟断电宕机

  // 重启恢复：结算段唯一判活「被索引引用」
  let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
  let recovered = Arc::new(WedbStore::recover(&ckpt_dir, token, device).await?);
  let session = recovered.new_session()?;
  let meta_k = session.session_meta_key(b"orders");

  // 判据①：索引槽位指向的帧（低地址存活帧）已落恢复自愈形态
  let healed_addr = recovered
    .index
    .load()
    .find_tag(&meta_k)
    .expect("恢复后槽位须仍指向存活帧（或其治愈重挂帧）");
  let healed = recovered.hlog.read_record(healed_addr).await?;
  let healed_stub = stub_of(healed.value()?);
  assert!(
    healed_stub.is_recovered() && healed_stub.tree_handle == 0,
    "被索引引用的存活帧必落 mark_recovered（handle=0 + recovered）: \
     handle={}, recovered={}",
    healed_stub.tree_handle,
    healed_stub.is_recovered()
  );

  // 判据②：高位陈旧死帧零改写——恢复位仍 0、句柄原样（旧「保最高地址」
  // 判据在此必反中：改写死帧且漏落存活帧）
  assert_ne!(healed_addr, stale_addr, "死帧不可能被结算重挂为槽位指向");
  let stale = recovered.hlog.read_record(stale_addr).await?;
  let stale_stub = stub_of(stale.value()?);
  assert!(
    !stale_stub.is_recovered() && stale_stub.tree_handle == stale_handle_before,
    "未被索引引用的陈旧死帧必保持原字节（handle={}, recovered={}）",
    stale_stub.tree_handle,
    stale_stub.is_recovered()
  );

  // 判据③：恢复后经索引路由惰性开树回读历史字段照常
  assert_eq!(
    session.range_index_get(b"orders", b"user:1").await?,
    Some(b"balance_100".to_vec()),
    "恢复结算后首访须惰性开树回读"
  );

  drop(session);
  drop(recovered);
  OK
}
