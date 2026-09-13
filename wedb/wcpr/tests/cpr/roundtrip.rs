//! CprStore/CprRecover 往返：检查点后销毁重建，地址状态机、桶内容、ReadCache 顺链
//! 回写、超尾截断与 hlog 尾部崩溃残留（前缀规则）逐项校验

use std::{
  fs,
  sync::{Arc, atomic},
};

use aok::{OK, Void};
use compio::runtime::Runtime;
use tempfile::tempdir;
use wcpr::{
  CheckpointManager, CheckpointMeta, CheckpointType, read_index_checkpoint_truncated,
  write_index_checkpoint,
};
use wdev::SegmentedDevice;
use whlog::HybridLogConfig;
use windex::{HashBucket, HashBucketEntry, HashIndex};

use super::support::MiniStore;

/// 构造含墓碑、TTL 候选、ReadCache 位条目的最小数据集
async fn seed_store(store: &MiniStore, p: &wepoch::Participant) -> Void {
  // 覆盖版本链：两代记录均在日志中，索引指向 v2
  store.put(p, b"user:1001", b"alice_v1").await?;
  store.put(p, b"user:1001", b"alice_v2").await?;
  // 墓碑：记录与墓碑均在日志中，索引指向墓碑
  store.put(p, b"user:1002", b"bob").await?;
  store.del(p, b"user:1002").await?;
  // TTL 候选：8 字节大端到期戳载荷，检查点视角为普通记录，逐字节保真
  store
    .put(p, b"ttl:cand", &1234567890123u64.to_be_bytes())
    .await?;
  // ReadCache 位条目：索引槽位指向易失读缓存虚拟地址
  store.put(p, b"user:rc", b"hot_value").await?;
  store.install_read_cache_entry(p, b"user:rc")?;
  Ok(())
}

/// 校验恢复后的桶内容与索引形态
async fn verify_recovered(store: &MiniStore, p: &wepoch::Participant, tail: u64) -> Void {
  assert_eq!(
    store.get(p, b"user:1001").await?.as_deref(),
    Some(b"alice_v2".as_slice()),
    "覆盖键必须恢复到最新版本"
  );
  assert_eq!(
    store.get(p, b"user:1002").await?.as_deref(),
    None,
    "墓碑键恢复后必须不可见"
  );
  assert_eq!(
    store.get(p, b"ttl:cand").await?.as_deref(),
    Some(1234567890123u64.to_be_bytes().as_slice()),
    "TTL 候选记录必须逐字节保真"
  );
  assert_eq!(
    store.get(p, b"user:rc").await?.as_deref(),
    Some(b"hot_value".as_slice()),
    "ReadCache 位条目必须顺链回写主日志地址后恢复可见"
  );

  // ReadCache 位条目恢复后必须是干净的主日志地址（无虚拟指示位）
  let rc_slot = store
    .index
    .find_tag(b"user:rc")
    .expect("恢复索引必须含 user:rc");
  assert!(
    rc_slot & HashBucketEntry::READ_CACHE_BIT == 0,
    "恢复后槽位不得残留 ReadCache 指示位: {rc_slot:#x}"
  );
  assert!(rc_slot < tail, "恢复槽位地址必须位于截断点之内");

  // 超尾幽灵条目必须被截断清零
  assert_eq!(
    store.index.find_tag(b"ghost:entry"),
    None,
    "超出一致性截断点的索引条目必须清零"
  );

  // 全桶扫描：不得残留 ReadCache 位与试探态标记
  for bucket in store.index.buckets.iter() {
    for slot in &bucket.entries[..HashBucket::DATA_ENTRIES] {
      let raw = slot.load(atomic::Ordering::Acquire);
      assert!(
        raw & (HashBucketEntry::READ_CACHE_BIT | HashBucketEntry::TENTATIVE_MASK) == 0,
        "恢复索引槽位残留瞬态标记: {raw:#x}"
      );
    }
  }
  Ok(())
}

/// FoldOver 检查点往返：地址状态机逐项比对 + 桶内容一致 + hlog 尾部崩溃残留前缀规则
#[test]
fn fold_over_checkpoint_roundtrip() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let ckpt_dir = dir.path().join("checkpoints");
    let db_path = dir.path().join("roundtrip.db");
    let store = MiniStore::open(&db_path)?;
    let p = store.session()?;

    seed_store(&store, &p).await?;
    // 超尾幽灵条目：直接插索引指向检查点 tail 之后的地址，模拟截断点之后的
    // 索引写入，恢复期必须按一致性截断清零
    let ghost_addr = store.hlog.tail_address() + 0x100;
    store.index.insert(b"ghost:entry", ghost_addr)?;

    let mgr = CheckpointManager::<SegmentedDevice>::new();
    let meta: CheckpointMeta = mgr
      .create_checkpoint(&store, &ckpt_dir, CheckpointType::FoldOver)
      .await?;
    let token = meta.token;
    assert_eq!(meta.cp_type, CheckpointType::FoldOver);
    assert_eq!(
      meta.hlog_meta.tail_address,
      store.hlog.tail_address(),
      "PREPARE 捕获的截断点必须等于创建时 tail"
    );
    assert_eq!(meta.index_meta.entry_count, store.entry_count());

    // 模拟崩溃残留：绕过日志协议直接污染设备文件 tail 起始区域，
    // 恢复按前缀规则信任 [*, tail) 并从 tail 续写覆盖残留
    store.scorch_device_beyond_tail(256)?;

    // 销毁重建：全新设备句柄 + 全新引擎实例
    drop(p);
    drop(store);
    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
    let restored = CheckpointManager::recover::<MiniStore>(&ckpt_dir, token, device).await?;
    assert_eq!(
      restored.meta.as_ref().expect("恢复实例必含 meta"),
      &meta,
      "恢复出的 meta 必须与发布时逐字段一致"
    );

    // 地址状态机：FoldOver 恢复 read_only 对齐 tail，flushed 钳制至截断点
    let hlog_meta = &restored.meta.as_ref().expect("恢复实例必含 meta").hlog_meta;
    assert_eq!(restored.hlog.tail_address(), hlog_meta.tail_address);
    assert_eq!(
      restored.hlog.read_only_address(),
      hlog_meta.tail_address,
      "FoldOver 恢复必须将 read_only 封印至 tail"
    );
    assert_eq!(
      restored.hlog.flushed_until_address(),
      hlog_meta.tail_address.min(hlog_meta.flushed_until_address),
      "flushed 必须钳制在截断点之内"
    );
    assert_eq!(restored.hlog.begin_address(), hlog_meta.begin_address);
    assert!(
      restored.hlog.begin_address() <= restored.hlog.head_address()
        && restored.hlog.head_address() <= restored.hlog.flushed_until_address()
        && restored.hlog.flushed_until_address() <= restored.hlog.tail_address(),
      "恢复地址必须满足 begin <= head <= flushed <= tail 不变式"
    );

    // 前缀规则：残留区不影响既有记录可见性，恢复后 [begin, tail) 记录数不变
    let p_restored = restored.session()?;
    let live_records = restored
      .scan_count(
        &p_restored,
        restored.hlog.begin_address(),
        hlog_meta.tail_address,
      )
      .await?;
    assert_eq!(
      live_records, 6,
      "检查点区间记录数必须精确（2 覆盖链 + 2 墓碑链 + 1 TTL + 1 ReadCache）"
    );

    // 残留区之上续写：新追加不受污染、可正常读写
    restored
      .put(&p_restored, b"post:recovery", b"fresh")
      .await?;
    assert_eq!(
      restored
        .get(&p_restored, b"post:recovery")
        .await?
        .as_deref(),
      Some(b"fresh".as_slice()),
      "恢复后追加必须正常工作"
    );

    verify_recovered(&restored, &p_restored, hlog_meta.tail_address).await?;

    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// Snapshot 检查点往返：恢复时按 mutable_fraction 重建内存可变区（非 tail 封印）
#[test]
fn snapshot_checkpoint_rebuilds_mutable_region() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let ckpt_dir = dir.path().join("checkpoints");
    let db_path = dir.path().join("snapshot.db");
    let store = MiniStore::open(&db_path)?;
    let p = store.session()?;

    seed_store(&store, &p).await?;
    let ghost_addr = store.hlog.tail_address() + 0x100;
    store.index.insert(b"ghost:entry", ghost_addr)?;

    let mgr = CheckpointManager::<SegmentedDevice>::new();
    let meta = mgr
      .create_checkpoint(&store, &ckpt_dir, CheckpointType::Snapshot)
      .await?;
    let token = meta.token;

    drop(p);
    drop(store);
    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
    let restored = CheckpointManager::recover::<MiniStore>(&ckpt_dir, token, device).await?;

    // Snapshot 语义：read_only = calculate_read_only_address(head, tail).max(head)
    let snapshot_meta = restored.meta.as_ref().expect("恢复实例必含 meta");
    let hlog_meta = &snapshot_meta.hlog_meta;
    let config = HybridLogConfig::new(
      snapshot_meta.store_meta.page_size,
      snapshot_meta.store_meta.num_pages,
      snapshot_meta.store_meta.mutable_fraction,
    )?;
    let expected_ro = config
      .calculate_read_only_address(restored.hlog.head_address(), hlog_meta.tail_address)
      .max(restored.hlog.head_address());
    assert_eq!(
      restored.hlog.read_only_address(),
      expected_ro,
      "Snapshot 恢复必须按 mutable_fraction 重建只读边界"
    );
    assert!(
      restored.hlog.read_only_address() <= hlog_meta.tail_address,
      "Snapshot 只读边界不得越过截断点"
    );

    let p_restored = restored.session()?;
    verify_recovered(&restored, &p_restored, hlog_meta.tail_address).await?;

    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// 索引快照纯往返：桶级模糊快照的截断、净化与溢出桶保真（含 CRC/token 门控）
#[test]
fn index_checkpoint_roundtrip_truncates_and_sanitizes() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let ckpt_dir = dir.path();
    let token = 0xDEAD_BEEF_u128;

    // 200 键注入 64 桶索引：迫使溢出桶链参与快照
    let index = HashIndex::new(64)?;
    let total_keys = 200usize;
    let base_addr = 64u64;
    for i in 0..total_keys {
      let key = format!("bucket:stress:{i:04}");
      let addr = base_addr + (i as u64) * 0x20;
      index.insert(key.as_bytes(), addr)?;
    }
    // 双条目键：低位版本存活、高位版本超尾
    index.insert(b"dual:version", base_addr + 0x10)?;
    let high_addr = base_addr + 0x2000_0000;
    index.insert(b"dual:version", high_addr)?;
    let overflow_before = index.overflow_bucket_count();
    let entry_count = total_keys + 2;

    write_index_checkpoint(&index, entry_count, ckpt_dir, token, &|addr| addr).await?;

    // 无截断读回：候选集保真（同 tag 碰撞候选共存，逐键包含性校验）
    let path = ckpt_dir.join(wcpr::index_filename(token));
    let (restored, meta) = read_index_checkpoint_truncated(&path, token, None).await?;
    assert_eq!(meta.size, 64);
    assert_eq!(meta.overflow_count, overflow_before, "溢出桶数必须保真");
    assert_eq!(meta.entry_count, entry_count);
    for i in 0..total_keys {
      let key = format!("bucket:stress:{i:04}");
      let addr = base_addr + (i as u64) * 0x20;
      assert!(
        restored.lookup_candidates(key.as_bytes()).contains(addr),
        "恢复索引必须包含键 {key} 的候选地址"
      );
    }
    assert!(
      restored
        .lookup_candidates(b"dual:version")
        .contains(high_addr),
      "无截断读回时超尾条目仍在"
    );

    // tail 截断读回：超尾条目净化清零，低位条目无恙
    let tail = base_addr + 0x1000;
    let (truncated, _) = read_index_checkpoint_truncated(&path, token, Some(tail)).await?;
    let dual = truncated.lookup_candidates(b"dual:version");
    assert!(
      dual.contains(base_addr + 0x10) && !dual.contains(high_addr),
      "截断读回必须清零超尾条目并保留低位条目: {dual:?}"
    );

    // token 门控：异名 token 读取必须拒绝
    let err = read_index_checkpoint_truncated(&path, token + 1, None)
      .await
      .err()
      .expect("异名 token 必须拒绝");
    assert!(matches!(err, wcpr::Error::TokenMismatch { .. }), "{err}");

    // CRC 门控：桶数据区单字节翻转必须拒绝
    let mut bytes = fs::read(&path)?;
    bytes[64 + 16] ^= 0x01;
    fs::write(&path, &bytes)?;
    let err = read_index_checkpoint_truncated(&path, token, None)
      .await
      .err()
      .expect("数据位翻转必须拒绝");
    assert!(
      matches!(err, wcpr::Error::ChecksumMismatch { .. }),
      "数据区位翻转必须被 CRC 拦截: {err}"
    );

    aok::Result::<()>::Ok(())
  })?;
  OK
}
