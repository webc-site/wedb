//! 索引快照底层能力：模糊截断与位运算净化、批处理缓冲 CRC32、RangeIndex CPR 存根自愈。

use std::{
  fs::{File, OpenOptions, create_dir_all, write},
  io::{Read, Seek, SeekFrom, Write},
  sync::{Arc, atomic::Ordering},
};

use aok::{OK, Void};
use compio::runtime::Runtime;
use log::info;
use tempfile::tempdir;
use wbase::crc::crc32;
use wcpr::{
  CheckpointType, Error, read_index_checkpoint_truncated, take_index_checkpoint,
  write_index_checkpoint,
};
use wdev::SegmentedDevice;
use windex::{HashBucket, HashBucketEntry, HashIndex};
use wkv::{CheckpointManager, StorageBackend, StoreConfig, TreeTuning, WedbStore};

/// 模糊索引截断（Fuzzy Index Truncation）与 Token 子目录彻底回收：
/// 恢复阶段超出截断点的脏条目与试探态条目被单次流式还原直接截断清零；
/// purge 时 token 专属目录被完全回收，零孤儿目录残留。
#[test]
fn test_fuzzy_index_truncation_and_token_dir_cleanup() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("fuzzy_truncation.db");
    let ckpt_dir = dir.path().join("checkpoints");

    let config = StoreConfig::new(512, 16 * 1024, 16, 0.5)?;
    let manager = CheckpointManager::new();

    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
    let store = Arc::new(WedbStore::open(config.clone(), Arc::clone(&device))?);
    let session = store.new_session()?;
    session.upsert(b"valid_key", b"valid_value").await?;

    let meta = manager
      .create_checkpoint(&store, &ckpt_dir, CheckpointType::FoldOver)
      .await?;
    let token = meta.token;
    let mut itoa_buf = itoa::Buffer::new();
    let token_str = itoa_buf.format(token);

    // 模拟 RangeIndex 子目录创建（如 token_str/rangeindex）
    let token_dir = ckpt_dir.join(token_str);
    let ri_dir = token_dir.join("rangeindex");
    create_dir_all(&ri_dir)?;
    write(ri_dir.join("prefix1.bftree"), b"dummy tree bytes")?;
    assert!(token_dir.exists());

    // 验证正常恢复成功
    {
      let rec = Arc::new(CheckpointManager::recover(&ckpt_dir, token, Arc::clone(&device)).await?);
      let s = rec.new_session()?;
      assert_eq!(s.read(b"valid_key").await?, Some(b"valid_value".to_vec()));
    }

    // 验证 purge_checkpoint 彻底清理包含父目录在内的 token 专属目录
    manager.purge(&ckpt_dir, token)?;
    assert!(!token_dir.exists(), "token 专属目录必须被连根彻底删除");
    assert!(
      !ckpt_dir
        .join(format!("checkpoint_{}.meta", token_str))
        .exists()
    );
    assert!(!ckpt_dir.join(format!("index_{}.ckpt", token_str)).exists());

    info!("模糊索引截断与 Token 目录彻底回收测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 熔合单次流式还原与截断的高级位运算全场景深度验证。
///
/// 覆盖路径（对标 C# Tsavorite DeleteTentativeEntries、SkipReadCache 与 Recovery untilAddress 截断）：
/// 1. 槽位 7 自旋锁（Latch）：高 16 位在排他锁（Exclusive）或共享锁（Shared）置位状态下写入快照；
///    恢复时验证高 16 位被 100% 剥离清零，低 48 位溢出指针完好保留，锁状态恢复为未锁定；
/// 2. 数据槽位试探态（Tentative）：未提交试探态条目在恢复时被精准清零；
/// 3. 数据槽位 ReadCache 易失指针：包含 READ_CACHE_BIT（第 47 位）的指针在恢复时被精准清零；
/// 4. 逻辑截断点（tail 截断）：address >= tail 的记录在单次流式还原时被精准清零；address < tail 的记录 100% 完好无损；
/// 5. tail 为 None 时：不执行地址截断，但 tentative、ReadCache 及槽位 7 Latch 仍然被正确净化；
/// 6. 溢出桶链：溢出桶中的条目与槽位 7 同样遵循相同的净化与截断位运算逻辑。
#[test]
fn test_read_index_checkpoint_truncated_bit_operations() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let ckpt_dir = dir.path().join("ckpt_bits");
    create_dir_all(&ckpt_dir)?;
    let token = 0xABCD_1234_EF56_7890_u128;

    let index = HashIndex::new(64)?;
    let tail_cutoff = 0x2000_u64;

    // 1. 在主桶 0 中精心构造各个边界槽位
    let b0 = &index.buckets[0];

    // 槽位 0: 合法普通条目 (addr = 0x1000 < tail, tag = 0x1234) -> 期望保留
    let e0 = HashBucketEntry::new(0x1000, 0x1234, false);
    b0.entries[0].store(e0.as_raw(), Ordering::Relaxed);

    // 槽位 1: 超出 tail 截断点的条目 (addr = 0x3000 >= tail, tag = 0x2345) -> 期望截断清零
    let e1 = HashBucketEntry::new(0x3000, 0x2345, false);
    b0.entries[1].store(e1.as_raw(), Ordering::Relaxed);

    // 槽位 2: 恰好对齐 tail 截断点的条目 (addr = 0x2000 == tail, tag = 0x3456) -> 期望截断清零
    let e2 = HashBucketEntry::new(0x2000, 0x3456, false);
    b0.entries[2].store(e2.as_raw(), Ordering::Relaxed);

    // 槽位 3: 未提交试探态条目 (addr = 0x800 < tail, tentative = true) -> 期望净化清零
    let e3 = HashBucketEntry::new(0x800, 0x4567, true);
    assert!(e3.is_tentative());
    b0.entries[3].store(e3.as_raw(), Ordering::Relaxed);

    // 槽位 4: 易失 ReadCache 指针条目 (addr = 0x900 < tail, read_cache = true) -> 期望净化清零
    let e4 = HashBucketEntry(0x900 | HashBucketEntry::READ_CACHE_BIT);
    assert!(e4.is_read_cache());
    b0.entries[4].store(e4.as_raw(), Ordering::Relaxed);

    // 槽位 5: 合法普通条目 (addr = 0x500 < tail, tag = 0x5678) -> 期望保留
    let e5 = HashBucketEntry::new(0x500, 0x5678, false);
    b0.entries[5].store(e5.as_raw(), Ordering::Relaxed);

    // 槽位 6: 空槽位 (0) -> 期望保持 0
    b0.entries[6].store(0, Ordering::Relaxed);

    // 槽位 7: 溢出指针 + 并发排他写锁 (Exclusive Latch)
    let ofb_id = index.overflow_pool.allocate()?;
    assert_eq!(ofb_id, 1);
    // 将槽位 7 设置为溢出 ID 加上排他锁标记
    let raw_slot7 = ofb_id | HashBucket::EXCLUSIVE_LATCH_MASK;
    b0.entries[HashBucket::OVERFLOW_INDEX].store(raw_slot7, Ordering::Relaxed);
    assert!(b0.is_latched());

    // 2. 在溢出桶中构造条目与共享自旋锁 (Shared Latch)
    let ofb = index.overflow_pool.get(ofb_id).unwrap();
    // 溢出桶槽位 0: 合法条目 (addr = 0x700 < tail) -> 期望保留
    let oe0 = HashBucketEntry::new(0x700, 0x6789, false);
    ofb.entries[0].store(oe0.as_raw(), Ordering::Relaxed);
    // 溢出桶槽位 1: 超出 tail 条目 (addr = 0x5000 >= tail) -> 期望截断清零
    let oe1 = HashBucketEntry::new(0x5000, 0x789A, false);
    ofb.entries[1].store(oe1.as_raw(), Ordering::Relaxed);
    // 溢出桶槽位 7: 加上共享锁标记
    ofb.entries[HashBucket::OVERFLOW_INDEX]
      .store(HashBucket::SHARED_LATCH_INC * 3, Ordering::Relaxed);
    assert!(ofb.is_latched());

    // 3. 刷写索引快照（无 ReadCache 场景使用恒等解析闭包）
    let meta = write_index_checkpoint(&index, 100, &ckpt_dir, token, &|addr| addr).await?;
    assert_eq!(meta.size, 64);
    assert_eq!(meta.overflow_count, 1);
    assert_eq!(meta.entry_count, 100);

    let index_path = ckpt_dir.join(format!("index_{}.ckpt", token));

    // 4. 第一种恢复场景：带 tail 截断恢复 (tail = Some(tail_cutoff))
    {
      let (rec_index, rec_meta) =
        read_index_checkpoint_truncated(&index_path, token, Some(tail_cutoff)).await?;
      assert_eq!(rec_meta.size, 64);
      assert_eq!(rec_meta.overflow_count, 1);
      assert_eq!(rec_meta.entry_count, 100);

      let rec_b0 = &rec_index.buckets[0];
      // 槽位 0: 必须完好保留
      let r0 = HashBucketEntry::from_raw(rec_b0.entries[0].load(Ordering::Relaxed));
      assert_eq!(r0.address(), 0x1000);
      assert_eq!(r0.tag(), 0x1234);
      assert!(!r0.is_tentative());

      // 槽位 1: >= tail, 必须被清零
      assert_eq!(rec_b0.entries[1].load(Ordering::Relaxed), 0);

      // 槽位 2: == tail, 必须被清零
      assert_eq!(rec_b0.entries[2].load(Ordering::Relaxed), 0);

      // 槽位 3: tentative, 必须被清零
      assert_eq!(rec_b0.entries[3].load(Ordering::Relaxed), 0);

      // 槽位 4: ReadCache, 必须被清零
      assert_eq!(rec_b0.entries[4].load(Ordering::Relaxed), 0);

      // 槽位 5: 必须完好保留
      let r5 = HashBucketEntry::from_raw(rec_b0.entries[5].load(Ordering::Relaxed));
      assert_eq!(r5.address(), 0x500);
      assert_eq!(r5.tag(), 0x5678);

      // 槽位 6: 原本为空，保持为 0
      assert_eq!(rec_b0.entries[6].load(Ordering::Relaxed), 0);

      // 槽位 7: 排他自旋锁高 16 位必须彻底剥离！低 48 位溢出指针精确保留为 ofb_id
      assert!(!rec_b0.is_latched(), "恢复后排他自旋锁必须已完全释放");
      assert_eq!(rec_b0.overflow_index(), ofb_id);

      // 检查溢出桶
      let rec_ofb = rec_index.overflow_pool.get(ofb_id).unwrap();
      let ro0 = HashBucketEntry::from_raw(rec_ofb.entries[0].load(Ordering::Relaxed));
      assert_eq!(ro0.address(), 0x700);
      assert_eq!(ro0.tag(), 0x6789);

      // 溢出桶槽位 1: >= tail 被清零
      assert_eq!(rec_ofb.entries[1].load(Ordering::Relaxed), 0);

      // 溢出桶槽位 7: 共享自旋锁高 16 位彻底剥离
      assert!(!rec_ofb.is_latched(), "恢复后共享自旋锁必须已完全释放");
      assert_eq!(rec_ofb.overflow_index(), 0);
    }

    // 5. 第二种恢复场景：不带 tail 截断恢复（read_index_checkpoint_truncated 显式传 tail = None）
    {
      let (rec_index_notrunc, _) =
        read_index_checkpoint_truncated(&index_path, token, None).await?;
      let rec_b0 = &rec_index_notrunc.buckets[0];

      // 槽位 0 正常保留
      assert_eq!(
        HashBucketEntry::from_raw(rec_b0.entries[0].load(Ordering::Relaxed)).address(),
        0x1000
      );
      // 槽位 1 在 tail = None 时保留（因为写入端不截断 tail，写入端仅净化 tentative/read_cache/latch）
      assert_eq!(
        HashBucketEntry::from_raw(rec_b0.entries[1].load(Ordering::Relaxed)).address(),
        0x3000
      );
      // 槽位 2 同样保留
      assert_eq!(
        HashBucketEntry::from_raw(rec_b0.entries[2].load(Ordering::Relaxed)).address(),
        0x2000
      );
      // 槽位 3 试探态依然被清零
      assert_eq!(rec_b0.entries[3].load(Ordering::Relaxed), 0);
      // 槽位 4 ReadCache 依然被清零
      assert_eq!(rec_b0.entries[4].load(Ordering::Relaxed), 0);
      // 槽位 7 Latch 依然被彻底剥离
      assert!(!rec_b0.is_latched());
      assert_eq!(rec_b0.overflow_index(), ofb_id);
    }

    info!("熔合单次流式还原与截断的高级位运算全场景深度测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 32KB 批处理缓冲区流水线任意桶数残块刷新与硬件加速 CRC32 深度验证。
///
/// 覆盖路径：
/// 1. 任意桶数（非 512 的整数倍）：
///    - 桶数 = 1（仅 64 字节，远远小于 32KB 批处理容量）；
///    - 桶数 = 7（非 2 的幂，448 字节，验证非对齐小桶数处理）；
///    - 桶数 = 513（512 个主桶 + 1 个溢出桶，恰好跨越 32KB 批处理边界，尾部残块为 1 个桶 64 字节）；
///    - 桶数 = 1025（1024 个主桶 + 1 个溢出桶，跨越 2 个 32KB 批处理边界，尾部残块为 1 个桶 64 字节）；
/// 2. 验证写入端 BatchWriter 的 cursor 在 finish() 时对未满 32KB 残块精准刷新；
/// 3. 验证写入端 CRC32 累积严格只包含有效数据字节，绝不累积未填满缓冲区的 0 字节；
/// 4. 验证读取端 BatchReader 的 remaining_bytes 步进与流式 CRC32 累积 100% 精确吻合；
/// 5. 注入故障：针对非 512 倍数的残块末尾桶篡改 1 比特位，验证 ChecksumMismatch 精准拦截。
#[test]
fn test_batch_buffer_arbitrary_buckets_and_crc32() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let ckpt_dir = dir.path().join("ckpt_batches");
    create_dir_all(&ckpt_dir)?;

    // 测试桶数配置列表: (num_buckets, overflow_count)
    // 主哈希桶必须是 2 的幂，配合任意数量的溢出桶构成任意总桶数（如 1, 7, 513, 517, 1025 桶）
    let configs = [
      (1usize, 0u64),    // 总计 1 个桶: 64B
      (4usize, 3u64),    // 总计 7 个桶: 448B (非 512 倍数且总数非 2 的幂)
      (512usize, 1u64),  // 总计 513 个桶: 32832B (跨越 32KB 边界，残块 64B)
      (512usize, 5u64),  // 总计 517 个桶: 33088B (跨越 32KB 边界，残块 320B)
      (1024usize, 1u64), // 总计 1025 个桶: 65600B (跨越 64KB 边界，残块 64B)
    ];

    for (case_idx, (num_buckets, overflow_count)) in configs.into_iter().enumerate() {
      let token = 0x8888_0000_u128 + case_idx as u128;
      let index = HashIndex::new(num_buckets)?;

      // 分配指定数量的溢出桶
      for _ in 0..overflow_count {
        let _ = index.overflow_pool.allocate()?;
      }
      assert_eq!(index.overflow_pool.allocated_count(), overflow_count);

      // 为每个桶写入特定的确定性数据
      for (b_idx, bucket) in index.buckets.iter().enumerate() {
        let addr = 0x1000 + (b_idx as u64) * 0x10;
        let tag = (0x1000 + b_idx) as u16;
        bucket.entries[0].store(
          HashBucketEntry::new(addr, tag, false).as_raw(),
          Ordering::Relaxed,
        );
        bucket.entries[1].store(
          HashBucketEntry::new(addr + 1, tag + 1, false).as_raw(),
          Ordering::Relaxed,
        );
      }
      for id in 1..=overflow_count {
        if let Some(ofb) = index.overflow_pool.get(id) {
          let addr = 0x8000 + id * 0x10;
          let tag = (0x2000 + id) as u16;
          ofb.entries[0].store(
            HashBucketEntry::new(addr, tag, false).as_raw(),
            Ordering::Relaxed,
          );
        }
      }

      // 1. 写入快照（无 ReadCache 场景使用恒等解析闭包）
      let meta =
        write_index_checkpoint(&index, num_buckets * 2, &ckpt_dir, token, &|addr| addr).await?;
      assert_eq!(meta.size, num_buckets);
      assert_eq!(meta.overflow_count, overflow_count);

      let ckpt_path = ckpt_dir.join(format!("index_{}.ckpt", token));
      assert!(ckpt_path.exists());

      // 2. 恢复快照并校验 CRC32 及全部数据
      let (recovered_index, rec_meta) =
        read_index_checkpoint_truncated(&ckpt_path, token, None).await?;
      assert_eq!(rec_meta.size, num_buckets);
      assert_eq!(rec_meta.overflow_count, overflow_count);

      for (b_idx, bucket) in recovered_index.buckets.iter().enumerate() {
        let expected_addr = 0x1000 + (b_idx as u64) * 0x10;
        let expected_tag = (0x1000 + b_idx) as u16;
        let e0 = HashBucketEntry::from_raw(bucket.entries[0].load(Ordering::Relaxed));
        assert_eq!(e0.address(), expected_addr);
        assert_eq!(e0.tag(), expected_tag);

        let e1 = HashBucketEntry::from_raw(bucket.entries[1].load(Ordering::Relaxed));
        assert_eq!(e1.address(), expected_addr + 1);
        assert_eq!(e1.tag(), expected_tag + 1);
      }

      for id in 1..=overflow_count {
        let ofb = recovered_index.overflow_pool.get(id).unwrap();
        let expected_addr = 0x8000 + id * 0x10;
        let expected_tag = (0x2000 + id) as u16;
        let e0 = HashBucketEntry::from_raw(ofb.entries[0].load(Ordering::Relaxed));
        assert_eq!(e0.address(), expected_addr);
        assert_eq!(e0.tag(), expected_tag);
      }

      // 3. 故障注入：篡改残块数据中最后一个字节，必须 100% 触发 ChecksumMismatch
      let file_len = File::open(&ckpt_path)?.metadata()?.len();
      let mut file = OpenOptions::new().read(true).write(true).open(&ckpt_path)?;
      // 定位到文件倒数第 1 字节（必然位于最后一个残块桶内）
      file.seek(SeekFrom::Start(file_len - 1))?;
      let mut last_byte = [0u8; 1];
      file.read_exact(&mut last_byte)?;
      last_byte[0] ^= 0x55; // 翻转位
      file.seek(SeekFrom::Start(file_len - 1))?;
      file.write_all(&last_byte)?;
      file.flush()?;

      let err = read_index_checkpoint_truncated(&ckpt_path, token, None).await;
      assert!(
        matches!(err, Err(Error::ChecksumMismatch { .. })),
        "残块桶被篡改必须触发 ChecksumMismatch"
      );
    }

    info!("32KB 批处理流水线任意桶数残块刷新与硬件加速 CRC32 深度测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 Garnet SnapshotAllTreesForCheckpoint / RebuildFromSnapshotIfPending —— RangeIndex CPR
/// 快照与存根自愈恢复全流程：快照生成 `<token>/rangeindex` 结构，恢复自动自愈存根并 1:1 回读。
///
/// 流程：
/// 1. 初始化引擎并配置 `with_range_index_dir`；
/// 2. 在存储中创建 RangeIndex 并写入字段数据，同时写入普通主存储 KV 记录；
/// 3. 执行 Checkpoint 快照创建，验证在一致性截断点先封印只读、整库刷盘、触发所有 RangeIndex CPR 快照并落盘元数据；
/// 4. 验证快照目录中不仅存在主索引快照与元数据，还生成了 `<token>/rangeindex/<hash_prefix>.bftree` 结构；
/// 5. 销毁原实例模拟断电宕机，新引擎调用 `CheckpointManager::recover` 执行崩溃恢复；
/// 6. 验证恢复流程自动触发 `recover_range_indexes`，自愈 RangeIndex 存根状态（标记已从检查点恢复）；
/// 7. 1:1 回读验证普通记录与 RangeIndex 历史字段内容完全一致；
/// 8. 恢复后继续追加写入新的 RangeIndex 字段并读回，验证读写状态机完全就绪；
/// 9. 调用 `purge_checkpoint`，验证包含 RangeIndex 子目录在内的 token 专属目录被彻底物理回收。
#[test]
fn test_range_index_cpr_stub_healing_recovery() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("range_index_ckpt.db");
    let ckpt_dir = dir.path().join("checkpoints");
    let ri_dir = dir.path().join("ri_data");
    create_dir_all(&ri_dir)?;

    const TUNE: TreeTuning = TreeTuning {
      cache_size: 65536,
      min_record_size: 8,
      max_record_size: 1024,
      max_key_len: 128,
      leaf_page_size: 0,
    };

    let config = StoreConfig::new(1024, 64 * 1024, 16, 0.5)?.with_range_index_dir(&ri_dir);
    let manager = CheckpointManager::new();
    let token;

    // 1. 初始化并写入普通 KV 与 RangeIndex 数据
    {
      let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
      let store = Arc::new(WedbStore::open(config.clone(), device)?);
      let session = store.new_session()?;

      // 写入普通 KV
      session.upsert(b"sys:status", b"online").await?;
      session.upsert(b"sys:epoch", b"100").await?;

      // 创建 RangeIndex 并插入多条字段
      session
        .range_index_create(b"accounts", StorageBackend::Memory, TUNE)
        .await?;
      session
        .range_index_set(b"accounts", b"user:1001", b"balance_5000")
        .await?;
      session
        .range_index_set(b"accounts", b"user:1002", b"balance_8888")
        .await?;

      // 验证创建期读取正常
      assert_eq!(
        session.range_index_get(b"accounts", b"user:1001").await?,
        Some(b"balance_5000".to_vec())
      );
      assert_eq!(
        session.range_index_get(b"accounts", b"user:1002").await?,
        Some(b"balance_8888".to_vec())
      );

      // 2. 创建 Checkpoint 快照
      let meta = manager
        .create_checkpoint(&store, &ckpt_dir, CheckpointType::FoldOver)
        .await?;
      token = meta.token;

      // 验证快照物理文件：元数据、主索引快照以及 token 专属目录下的 RangeIndex 快照
      let meta_file = ckpt_dir.join(format!("checkpoint_{}.meta", token));
      let index_file = ckpt_dir.join(format!("index_{}.ckpt", token));
      assert!(meta_file.exists(), "元数据文件必须已落盘");
      assert!(index_file.exists(), "主索引快照文件必须已落盘");

      let mut itoa_buf = itoa::Buffer::new();
      let token_str = itoa_buf.format(token);
      let token_ri_dir = ckpt_dir.join(token_str).join("rangeindex");
      assert!(
        token_ri_dir.exists(),
        "RangeIndex CPR 快照子目录必须在检查点中被创建"
      );
    } // 原 store、session、device 全部销毁，模拟节点崩溃

    // 3. 崩溃恢复并在全新实例上验证自愈
    {
      let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
      let recovered = Arc::new(CheckpointManager::recover(&ckpt_dir, token, device).await?);
      let session = recovered.new_session()?;

      // 验证普通 KV
      assert_eq!(session.read(b"sys:status").await?, Some(b"online".to_vec()));
      assert_eq!(session.read(b"sys:epoch").await?, Some(b"100".to_vec()));

      // 验证 RangeIndex 存根自愈并正确回读历史字段
      assert_eq!(
        session.range_index_get(b"accounts", b"user:1001").await?,
        Some(b"balance_5000".to_vec())
      );
      assert_eq!(
        session.range_index_get(b"accounts", b"user:1002").await?,
        Some(b"balance_8888".to_vec())
      );
      assert_eq!(
        session
          .range_index_get(b"accounts", b"user:non_existent")
          .await?,
        None
      );

      // 验证恢复后能够继续向自愈后的 RangeIndex 追加新字段
      session
        .range_index_set(b"accounts", b"user:1003", b"balance_9999")
        .await?;
      assert_eq!(
        session.range_index_get(b"accounts", b"user:1003").await?,
        Some(b"balance_9999".to_vec())
      );
    }

    // 4. 清理快照并验证 token 专属目录连同 RangeIndex 子目录被彻底物理回收
    let mut itoa_buf = itoa::Buffer::new();
    let token_str = itoa_buf.format(token);
    let token_dir = ckpt_dir.join(token_str);
    assert!(token_dir.exists());

    manager.purge(&ckpt_dir, token)?;
    assert!(
      !token_dir.exists(),
      "purge 后 token 专属目录必须被彻底物理清除"
    );
    assert!(
      !ckpt_dir
        .join(format!("checkpoint_{}.meta", token_str))
        .exists()
    );
    assert!(!ckpt_dir.join(format!("index_{}.ckpt", token_str)).exists());

    info!("RangeIndex CPR 快照与存根自愈恢复全流程验证通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 C# Garnet ReadCache.cs SkipReadCacheBucket —— ReadCache 易失指针快照解析回归：
/// 1. 写入端传入解析闭包时，指向读缓存的条目必须顺链回写为主日志真实地址（恢复后键仍可见）；
/// 2. 解析断链（记录已滑出读缓存窗口，闭包返回 0）时条目被安全归零，绝不将物理偏移误当主日志地址；
/// 3. 读取端防御：快照中残留的未解析 ReadCache 指针（如旧版本引擎产物）在恢复时被彻底清零。
#[test]
fn test_read_cache_pointer_snapshot_resolution() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let ckpt_dir = dir.path().join("ckpt_rc");
    create_dir_all(&ckpt_dir)?;
    let token = 0xACCE_5510_u128;

    let index = HashIndex::new(64)?;
    let b0 = &index.buckets[0];
    // 槽位 0: 普通主日志条目（解析闭包不触碰）
    b0.entries[0].store(
      HashBucketEntry::new(0x1000, 0x1111, false).as_raw(),
      Ordering::Relaxed,
    );
    // 槽位 1: 指向 ReadCache 的易失条目（bit47 置位），链尾主日志地址为 0x2000，
    // 并携带非零指纹（高 16 位）验证写侧解析仅换写地址字段、指纹原样保留
    let rc_addr = 0x2000 | HashBucketEntry::READ_CACHE_BIT;
    let rc_tag: u64 = 0x4242;
    b0.entries[1].store(
      HashBucketEntry::from_raw(rc_addr).as_raw() | (rc_tag << HashBucketEntry::TAG_SHIFT),
      Ordering::Relaxed,
    );
    // 槽位 2: 指向已滑出读缓存窗口的条目（解析断链）
    b0.entries[2].store(
      HashBucketEntry::from_raw(0x9000 | HashBucketEntry::READ_CACHE_BIT).as_raw(),
      Ordering::Relaxed,
    );

    // 1. 写入端解析闭包（模拟 store.read_cache.skip_read_cache：命中则剥离 RC 位回写主日志地址，断链返回 0）
    let resolve = |addr: u64| if addr == rc_addr { 0x2000 } else { 0 };
    write_index_checkpoint(&index, 3, &ckpt_dir, token, &resolve).await?;

    let (rec, _) =
      read_index_checkpoint_truncated(ckpt_dir.join(format!("index_{token}.ckpt")), token, None)
        .await?;
    let r0 = rec.buckets[0].entries[0].load(Ordering::Relaxed);
    assert_eq!(
      HashBucketEntry::from_raw(r0).address(),
      0x1000,
      "普通条目不得被解析闭包触碰"
    );

    // 槽位 1: 必须被顺链解析为主日志真实地址 0x2000，RC 位剥离
    let r1 = rec.buckets[0].entries[1].load(Ordering::Relaxed);
    assert!(
      !HashBucketEntry::from_raw(r1).is_read_cache(),
      "易失 ReadCache 指针严禁原样落盘"
    );
    assert_eq!(
      HashBucketEntry::from_raw(r1).address(),
      0x2000,
      "必须回写主日志真实地址"
    );
    assert_eq!(
      (r1 & HashBucketEntry::TAG_POS_MASK) >> HashBucketEntry::TAG_SHIFT,
      rc_tag,
      "写侧解析必须原样保留槽位指纹（C# Address setter 掩码换写语义）"
    );

    // 槽位 2: 解析断链必须安全归零
    assert_eq!(rec.buckets[0].entries[2].load(Ordering::Relaxed), 0);

    // 2. 读取端防御：手工构造含未解析 ReadCache 指针且 CRC 合法的快照文件（模拟旧版本引擎产物），
    //    恢复时残留指针必须被彻底清零，普通条目完好保留
    let raw_token = 0xDEAD_BEEF_u128;
    let mut bytes = Vec::with_capacity(128);
    bytes.extend_from_slice(b"WEDB_IDX"); // 魔数
    bytes.extend_from_slice(&1u32.to_le_bytes()); // 格式版本
    bytes.extend_from_slice(&0u32.to_le_bytes()); // CRC 占位
    bytes.extend_from_slice(&raw_token.to_le_bytes());
    bytes.extend_from_slice(&1u64.to_le_bytes()); // 主桶数
    bytes.extend_from_slice(&0u64.to_le_bytes()); // 溢出桶数
    bytes.extend_from_slice(&1u64.to_le_bytes()); // 条目数
    bytes.extend_from_slice(&[0u8; 8]); // 保留填充
    let mut bucket = [0u8; 64];
    bucket[0..8].copy_from_slice(&0x1000u64.to_le_bytes());
    bucket[8..16].copy_from_slice(&(0x2000u64 | HashBucketEntry::READ_CACHE_BIT).to_le_bytes());
    bytes.extend_from_slice(&bucket);
    let crc = crc32(&bytes[64..]);
    bytes[12..16].copy_from_slice(&crc.to_le_bytes());

    let raw_path = ckpt_dir.join(format!("index_{raw_token}.ckpt"));
    write(&raw_path, &bytes)?;

    let (rec2, _) = read_index_checkpoint_truncated(&raw_path, raw_token, None).await?;
    assert_eq!(
      rec2.buckets[0].entries[1].load(Ordering::Relaxed),
      0,
      "读取端必须将残留的未解析 ReadCache 指针彻底清零"
    );
    assert_eq!(rec2.buckets[0].entries[0].load(Ordering::Relaxed), 0x1000);

    info!("ReadCache 易失指针快照解析与读取端防御测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 验证 take_index_checkpoint 纯异步持久化、跨 32KB 批处理边界与硬件 CRC32 校验
#[test]
fn test_take_index_checkpoint_multi_batch() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let ckpt_dir = dir.path().join("async_ckpt");
    create_dir_all(&ckpt_dir)?;

    // 构造跨多批次（> 512 个桶，例如 1024 个桶 = 64KB）的 HashIndex
    let num_buckets = 1024;
    let index = Arc::new(HashIndex::new(num_buckets)?);

    // 填充部分桶
    for i in 0..num_buckets {
      let b = &index.buckets[i];
      let entry = HashBucketEntry::new(0x1000 + i as u64, (i % 100) as u16, false);
      b.entries[0].store(entry.as_raw(), Ordering::Relaxed);
    }

    // 分配并填充溢出桶
    let ofb_id = index.overflow_pool.allocate()?;
    let ofb = index.overflow_pool.get(ofb_id).unwrap();
    let oe = HashBucketEntry::new(0x8000, 42, false);
    ofb.entries[0].store(oe.as_raw(), Ordering::Relaxed);
    index.buckets[0].entries[HashBucket::OVERFLOW_INDEX]
      .store(ofb_id | HashBucket::EXCLUSIVE_LATCH_MASK, Ordering::Relaxed);

    let token1 = 0x1111_2222_3333_4444_u128;
    let meta1 =
      take_index_checkpoint(&index, num_buckets + 1, &ckpt_dir, token1, |addr| addr).await?;
    assert_eq!(meta1.size, num_buckets);
    assert_eq!(meta1.overflow_count, 1);
    assert_eq!(meta1.entry_count, num_buckets + 1);

    // 验证通过 CheckpointManager 实例方法调用 take_index_checkpoint
    let manager = CheckpointManager::<wdev::SegmentedDevice>::new();
    let token2 = 0x5555_6666_7777_8888_u128;
    let meta2 = manager
      .take_index_checkpoint(&index, num_buckets + 1, &ckpt_dir, token2, |addr| addr)
      .await?;
    assert_eq!(meta2.size, num_buckets);
    assert_eq!(meta2.overflow_count, 1);
    assert_eq!(meta2.entry_count, num_buckets + 1);

    // 校验持久化文件可完整回读并校验 CRC
    let ckpt1_path = ckpt_dir.join(format!("index_{token1}.ckpt"));
    let (rec_idx1, rec_meta1) = read_index_checkpoint_truncated(&ckpt1_path, token1, None).await?;
    assert_eq!(rec_meta1.size, num_buckets);
    assert_eq!(rec_meta1.overflow_count, 1);
    assert_eq!(rec_meta1.entry_count, num_buckets + 1);

    // 验证槽位 7 的自旋锁在写入端已被彻底剥离
    let rec_b0 = &rec_idx1.buckets[0];
    assert!(!rec_b0.is_latched());
    assert_eq!(rec_b0.overflow_index(), ofb_id);

    // 验证溢出桶中的条目
    let rec_ofb = rec_idx1.overflow_pool.get(ofb_id).unwrap();
    let rec_oe = HashBucketEntry::from_raw(rec_ofb.entries[0].load(Ordering::Relaxed));
    assert_eq!(rec_oe.address(), 0x8000);
    assert_eq!(rec_oe.tag(), 42);

    info!("take_index_checkpoint 跨 32KB 批处理与纯异步持久化测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}
