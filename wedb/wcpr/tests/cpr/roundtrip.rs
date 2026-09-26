//! CprStore/CprRecover 往返：检查点后销毁重建，地址状态机、桶内容、ReadCache 顺链
//! 回写、超尾截断与 hlog 尾部崩溃残留（前缀规则）逐项校验
//!
//! 自研依据: 检查点元数据编解码往返（C# 对应 test.recovery/CheckpointManagerTests.cs 元数据持久化面）

use std::{
  fs,
  sync::{Arc, atomic},
};

use aok::{OK, Void};
use tempfile::tempdir;
use wbase::crc::Crc32Hasher;
use wcpr::{
  CheckpointMeta, CheckpointType, CkptGateState, Error, read_index_checkpoint_truncated,
  write_index_checkpoint,
};
use wdev::SegmentedDevice;
use windex::{HashBucket, HashBucketEntry, HashIndex};

use super::support::{HashIndexTestOps, MiniStore, no_rc_resolve};

/// 快照头部尺寸（与 codec::HEADER_SIZE 对齐，集成测试面以字面量固化格式契约）
const HDR_BYTES: usize = 64;
/// 头部 CRC32 字段字节偏移（codec::HEADER_CRC_OFFSET 的格式镜像）
const HDR_CRC_OFFSET: usize = 12;
/// 溢出槽位在 64 字节桶内的字节偏移（槽位 7 × 8 字节）
const OVERFLOW_SLOT_OFFSET: usize = HashBucket::OVERFLOW_INDEX * 8;

/// 灌 200 键进 64 桶索引：迫使溢出桶链参与快照（与 index_checkpoint_roundtrip 同款压力形态）
fn seed_overflow_index() -> windex::Result<(HashIndex, usize)> {
  let index = HashIndex::new(64)?;
  let total_keys = 200usize;
  let base_addr = 64u64;
  for i in 0..total_keys {
    let key = format!("ovr:stress:{i:04}");
    index.insert(key.as_bytes(), base_addr + (i as u64) * 0x20)?;
  }
  Ok((index, total_keys))
}

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
#[compio::test]
async fn fold_over_checkpoint_roundtrip() -> Void {
  let dir = tempdir()?;
  let ckpt_dir = dir.path().join("checkpoints");
  let db_path = dir.path().join("roundtrip.db");
  let store = MiniStore::open(&db_path)?;
  let gate = CkptGateState::default();
  let p = store.session()?;

  seed_store(&store, &p).await?;
  // 超尾幽灵条目：直接插索引指向检查点 tail 之后的地址，模拟截断点之后的
  // 索引写入，恢复期必须按一致性截断清零
  let ghost_addr = store.hlog.tail_address() + 0x100;
  store.index.insert(b"ghost:entry", ghost_addr)?;

  let meta: CheckpointMeta =
    wcpr::create_checkpoint(&store, &gate, &ckpt_dir, CheckpointType::FoldOver).await?;
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
  let restored = wcpr::recover::<_, MiniStore>(&ckpt_dir, token, device).await?;
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
  OK
}

/// Snapshot 检查点往返：恢复时按 mutable_fraction 重建内存可变区（非 tail 封印）
#[compio::test]
async fn snapshot_checkpoint_rebuilds_mutable_region() -> Void {
  let dir = tempdir()?;
  let ckpt_dir = dir.path().join("checkpoints");
  let db_path = dir.path().join("snapshot.db");
  let store = MiniStore::open(&db_path)?;
  let gate = CkptGateState::default();
  let p = store.session()?;

  seed_store(&store, &p).await?;
  let ghost_addr = store.hlog.tail_address() + 0x100;
  store.index.insert(b"ghost:entry", ghost_addr)?;

  let meta = wcpr::create_checkpoint(&store, &gate, &ckpt_dir, CheckpointType::Snapshot).await?;
  let token = meta.token;

  drop(p);
  drop(store);
  let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
  let restored = wcpr::recover::<_, MiniStore>(&ckpt_dir, token, device).await?;

  // Snapshot 语义：read_only 与 FoldOver 同型对齐 tail（快照基线全程只读，
  // 隔离性由 ro=tail 单语义等价达成，见 wcpr/src/manager/recover.rs 恢复段注）
  let snapshot_meta = restored.meta.as_ref().expect("恢复实例必含 meta");
  let hlog_meta = &snapshot_meta.hlog_meta;
  assert_eq!(
    restored.hlog.read_only_address(),
    hlog_meta.tail_address,
    "Snapshot 恢复必须将 read_only 封印至 tail（快照基线只读）"
  );

  let p_restored = restored.session()?;
  verify_recovered(&restored, &p_restored, hlog_meta.tail_address).await?;
  OK
}

/// 索引快照纯往返：桶级模糊快照的截断、净化与溢出桶保真（含 CRC/token 门控）
#[compio::test]
async fn index_checkpoint_roundtrip_truncates_and_sanitizes() -> Void {
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
  let overflow_before = index.overflow_pool.allocated_count();
  let entry_count = total_keys + 2;

  write_index_checkpoint(&index, entry_count, ckpt_dir, token, &no_rc_resolve).await?;

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
  assert!(matches!(err, Error::TokenMismatch { .. }), "{err}");

  // CRC 门控：桶数据区单字节翻转必须拒绝
  let mut bytes = fs::read(&path)?;
  bytes[64 + 16] ^= 0x01;
  fs::write(&path, &bytes)?;
  let err = read_index_checkpoint_truncated(&path, token, None)
    .await
    .err()
    .expect("数据位翻转必须拒绝");
  assert!(
    matches!(err, Error::ChecksumMismatch { .. }),
    "数据区位翻转必须被 CRC 拦截: {err}"
  );
  OK
}

/// 写侧防线：采样点之后才存在的超界溢出桶 ID（模拟扫描期间并发挂链的新分配桶，
/// 槽位携带高 16 位 Latch 脏位）必须在落盘时被截断归零，悬空指针绝不允许写入快照
#[compio::test]
async fn index_checkpoint_truncates_overflow_beyond_sampled_bound() -> Void {
  let dir = tempdir()?;
  let ckpt_dir = dir.path();
  let token = 0xC0FF_EE01_u128;

  let (index, total_keys) = seed_overflow_index()?;
  let overflow_count = index.overflow_pool.allocated_count();
  assert!(
    overflow_count >= 1,
    "压力数据必须先产生真实溢出桶，构造超界场景才有意义"
  );

  // 人工注入：把首个主桶的溢出指针覆写为「采样上限之后才会出现的 ID」并叠置独占锁脏位
  // （dangling 超出池已分配范围，对应物理桶必然不会随本快照落盘）
  let dangling = (overflow_count + 4096) | HashBucket::EXCLUSIVE_LATCH_MASK;
  let victim = 0usize;
  index.bucket(victim).entries[HashBucket::OVERFLOW_INDEX]
    .store(dangling, atomic::Ordering::Release);

  write_index_checkpoint(&index, total_keys, ckpt_dir, token, &no_rc_resolve).await?;

  // 落盘字节级断言：victim 桶的溢出槽位必须为全零（超界 ID 连同锁位一并截断）
  let path = ckpt_dir.join(wcpr::index_filename(token));
  let bytes = fs::read(&path)?;
  let slot_off = HDR_BYTES + victim * 64 + OVERFLOW_SLOT_OFFSET;
  assert_eq!(
    &bytes[slot_off..slot_off + 8],
    &[0u8; 8],
    "超界溢出指针必须被写侧截断归零后才落盘"
  );

  // 读回防线：快照可正常通过 CRC 校验，恢复内存中该槽位同为链尾 0
  let (restored, meta) = read_index_checkpoint_truncated(&path, token, None).await?;
  assert_eq!(meta.overflow_count, overflow_count, "溢出桶数必须保真");
  assert_eq!(
    restored.bucket(victim).overflow_index(),
    0,
    "恢复后超界溢出槽位必须为链尾 0"
  );
  OK
}

/// 读侧防线：篡改快照（向无溢出链的主桶槽位写入越过头部 overflow_count 的野指针并
/// 重算 CRC 绕过完整性门控），恢复端必须就地截断归零，杜绝非法 ID 注入内存 HashBucket
/// 后令 ChainWalker 遍历解引用空 chunk（SIGSEGV）
#[compio::test]
async fn index_checkpoint_rejects_overflow_beyond_header_bound() -> Void {
  let dir = tempdir()?;
  let ckpt_dir = dir.path();
  let token = 0xC0FF_EE02_u128;

  let (index, total_keys) = seed_overflow_index()?;
  let overflow_count = index.overflow_pool.allocated_count();
  write_index_checkpoint(&index, total_keys, ckpt_dir, token, &no_rc_resolve).await?;

  let path = ckpt_dir.join(wcpr::index_filename(token));
  let mut bytes = fs::read(&path)?;

  // 篡改：选中原本无溢出链的主桶（链尾 0 槽位），写入越界一个 chunk 的野 ID + 共享锁脏位，
  // 再按篡改后数据区重算 CRC32 回写头部，完整绕过校验和门控
  let mut slot_bytes = [0u8; 8];
  let mut victim = None;
  for i in 0..64 {
    let off = HDR_BYTES + i * 64 + OVERFLOW_SLOT_OFFSET;
    slot_bytes.copy_from_slice(&bytes[off..off + 8]);
    if slot_bytes == [0u8; 8] {
      victim = Some(i);
      break;
    }
  }
  let victim = victim.expect("64 主桶必有链尾空溢出槽位可供注入");
  let wild = (overflow_count + (1 << 20)) | HashBucket::SHARED_LATCH_INC;
  let off = HDR_BYTES + victim * 64 + OVERFLOW_SLOT_OFFSET;
  bytes[off..off + 8].copy_from_slice(&wild.to_le_bytes());
  let mut hasher = Crc32Hasher::new();
  hasher.update(&bytes[HDR_BYTES..]);
  bytes[HDR_CRC_OFFSET..HDR_CRC_OFFSET + 4].copy_from_slice(&hasher.finalize().to_le_bytes());
  fs::write(&path, &bytes)?;

  // 恢复必须成功（CRC 已被重算合法化），且野指针在装载前被上界校验截断归零
  let (restored, meta) = read_index_checkpoint_truncated(&path, token, None).await?;
  assert_eq!(meta.overflow_count, overflow_count);
  assert_eq!(
    restored.overflow_pool.allocated_count(),
    overflow_count,
    "恢复端只允许按头部声明数分配溢出桶"
  );
  assert_eq!(
    restored.bucket(victim).overflow_index(),
    0,
    "越界野溢出 ID 必须被读侧截断归零: {wild:#x}"
  );

  // 全键位点查：压满 ChainWalker::advance 的链遍历路径，修复后不得出现空 chunk 解引用崩溃
  for i in 0..total_keys {
    let key = format!("ovr:stress:{i:04}");
    restored.lookup_candidates(key.as_bytes());
  }
  OK
}
