//! 快照文件故障注入与防御：损坏文件、CRC 校验失败、截断、魔数与元数据篡改拦截。

use std::{
  fs::{File, OpenOptions, read_dir, write},
  io::{Read, Seek, SeekFrom, Write},
  sync::Arc,
};

use aok::{OK, Void};
use compio::runtime::Runtime;
use log::info;
use tempfile::tempdir;
use wbftree::{StorageBackendType, TreeTuning};
use wcpr::{self, CheckpointType, Error};
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};

/// 快照文件损坏防御：元数据文件缺失、Token 不匹配、魔数篡改、CRC32 位反转、
/// 文件长度截断与断电残留 `.tmp` 临时文件，全部被精准拦截且不污染正常恢复。
#[test]
fn test_corrupted_files_defense() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("fault_injection.db");
    let ckpt_dir = dir.path().join("checkpoints");

    let config = StoreConfig::new(512, 16 * 1024, 16, 0.5)?;

    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
    let store = Arc::new(WedbStore::open(config.clone(), Arc::clone(&device))?);
    let session = store.new_session()?;
    session.upsert(b"foo", b"bar").await?;

    let meta = store
      .create_checkpoint(&ckpt_dir, CheckpointType::FoldOver)
      .await?;
    let token = meta.token;

    let meta_file = ckpt_dir.join(wcpr::meta_filename(token));
    let index_file = ckpt_dir.join(wcpr::index_filename(token));

    // 1. 测试 TokenMismatch：篡改元数据中的 token
    {
      let mut meta_corrupted = meta.clone();
      meta_corrupted.token = token + 1;
      let bin = meta_corrupted.encode();
      write(&meta_file, bin)?;

      let err = WedbStore::recover(&ckpt_dir, token, Arc::clone(&device)).await;
      assert!(
        matches!(err, Err(Error::TokenMismatch { .. })),
        "期望 TokenMismatch 错误"
      );

      // 恢复正确的元数据
      let bin_valid = meta.encode();
      write(&meta_file, bin_valid)?;
    }

    // 2. 测试魔数篡改
    {
      let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&index_file)?;
      file.seek(SeekFrom::Start(0))?;
      file.write_all(b"CORRUPT_")?; // 破坏 WEDB_IDX 魔数
      file.flush()?;

      let err = WedbStore::recover(&ckpt_dir, token, Arc::clone(&device)).await;
      assert!(
        matches!(err, Err(Error::InvalidIndexCkpt(msg)) if msg.contains("魔数不匹配")),
        "期望魔数不匹配错误"
      );

      // 恢复正确魔数
      file.seek(SeekFrom::Start(0))?;
      file.write_all(b"WEDB_IDX")?;
      file.flush()?;
    }

    // 3. 测试桶数据被篡改引发 CRC32 校验失败（ChecksumMismatch）
    {
      let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&index_file)?;
      file.seek(SeekFrom::Start(64))?; // 跳过 64 字节头部，篡改第一个数据桶
      let mut b = [0u8; 8];
      file.read_exact(&mut b)?;
      b[0] ^= 0xFF; // 反转比特位
      file.seek(SeekFrom::Start(64))?;
      file.write_all(&b)?;
      file.flush()?;

      let err = WedbStore::recover(&ckpt_dir, token, Arc::clone(&device)).await;
      assert!(
        matches!(err, Err(Error::ChecksumMismatch { .. })),
        "期望 ChecksumMismatch 错误"
      );

      // 修复数据位
      b[0] ^= 0xFF;
      file.seek(SeekFrom::Start(64))?;
      file.write_all(&b)?;
      file.flush()?;
    }

    // 4. 测试索引文件被非正常截断
    {
      let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&index_file)?;
      let cur_len = file.metadata()?.len();
      file.set_len(cur_len - 16)?; // 截断 16 字节

      let err = WedbStore::recover(&ckpt_dir, token, Arc::clone(&device)).await;
      assert!(
        matches!(err, Err(Error::InvalidIndexCkpt(msg)) if msg.contains("文件长度异常")),
        "期望文件长度异常错误"
      );

      // 恢复文件长度
      file.set_len(cur_len)?;
    }

    // 5. 模拟中途断电遗留的 .tmp 文件不影响 list_checkpoints 与正常恢复
    {
      let b32 = wcpr::token_to_base32(token);
      let tmp_meta = ckpt_dir.join(format!("checkpoint_{}_broken.meta.tmp", b32.as_str()));
      let tmp_index = ckpt_dir.join(format!("index_{}_broken.ckpt.tmp", b32.as_str()));
      File::create(&tmp_meta)?.write_all(b"half written meta")?;
      File::create(&tmp_index)?.write_all(b"half written index")?;

      // list_checkpoints 应当只列出已完成 rename 的正规文件
      let list = wcpr::list_checkpoints(&ckpt_dir)?;
      assert_eq!(list, vec![token]);

      // 恢复应正常成功
      let recovered = WedbStore::recover(&ckpt_dir, token, Arc::clone(&device)).await?;
      assert_eq!(recovered.entry_count(), 1);
    }

    info!("故障注入与边界防御测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 深度对抗式异常注入防御：头部 CRC 置零、日志三区地址越界（tail 低于起始基准、
/// begin/head 超出 tail）与索引快照大小不一致，全部被恢复流程逐一拦截。
#[test]
fn test_adversarial_fault_injection() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("adv_fault.db");
    let ckpt_dir = dir.path().join("checkpoints");

    let config = StoreConfig::new(256, 16 * 1024, 16, 0.5)?;

    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
    let store = Arc::new(WedbStore::open(config.clone(), Arc::clone(&device))?);
    let session = store.new_session()?;
    session.upsert(b"alpha", b"omega").await?;

    let meta = store
      .create_checkpoint(&ckpt_dir, CheckpointType::FoldOver)
      .await?;
    let token = meta.token;

    let meta_file = ckpt_dir.join(wcpr::meta_filename(token));
    let index_file = ckpt_dir.join(wcpr::index_filename(token));

    // 1. 防御：header 中 CRC 被恶意填 0 无法逃逸校验
    {
      let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&index_file)?;
      let mut orig_crc = [0u8; 4];
      file.seek(SeekFrom::Start(12))?;
      file.read_exact(&mut orig_crc)?;

      // 将头部 CRC 置零
      file.seek(SeekFrom::Start(12))?;
      file.write_all(&0u32.to_le_bytes())?;
      file.flush()?;

      let err = WedbStore::recover(&ckpt_dir, token, Arc::clone(&device)).await;
      assert!(
        matches!(err, Err(Error::ChecksumMismatch { expected: 0, .. })),
        "期望 CRC 置零时触发 ChecksumMismatch 校验失败"
      );

      // 还原正确 CRC
      file.seek(SeekFrom::Start(12))?;
      file.write_all(&orig_crc)?;
      file.flush()?;
    }

    // 2. 防御：元数据中 tail_address 小于 initial_address
    //    （语义字段篡改后重新封签，聚焦地址语义校验而非封签比对）
    {
      let mut corrupt_meta = meta.clone();
      corrupt_meta.hlog_meta.tail_address = 0x10; // 小于 0x40
      corrupt_meta.seal();
      write(&meta_file, corrupt_meta.encode())?;

      let err = WedbStore::recover(&ckpt_dir, token, Arc::clone(&device)).await;
      assert!(
        matches!(err, Err(Error::InvalidRecoveryAddress(msg)) if msg.contains("小于日志起始基准地址")),
        "期望 tail_address 异常被拦截"
      );
    }

    // 3. 防御：元数据中 begin_address > tail_address
    {
      let mut corrupt_meta = meta.clone();
      corrupt_meta.hlog_meta.begin_address = corrupt_meta.hlog_meta.tail_address + 100;
      corrupt_meta.seal();
      write(&meta_file, corrupt_meta.encode())?;

      let err = WedbStore::recover(&ckpt_dir, token, Arc::clone(&device)).await;
      assert!(
        matches!(err, Err(Error::InvalidRecoveryAddress(msg)) if msg.contains("超出 TailAddress")),
        "期望 begin_address > tail_address 被拦截"
      );
    }

    // 4. 防御：元数据中 head_address > tail_address
    {
      let mut corrupt_meta = meta.clone();
      corrupt_meta.hlog_meta.head_address = corrupt_meta.hlog_meta.tail_address + 500;
      corrupt_meta.seal();
      write(&meta_file, corrupt_meta.encode())?;

      let err = WedbStore::recover(&ckpt_dir, token, Arc::clone(&device)).await;
      assert!(
        matches!(err, Err(Error::InvalidRecoveryAddress(msg)) if msg.contains("超出 TailAddress")),
        "期望 head_address > tail_address 被拦截"
      );
    }

    // 5. 防御：元数据中 index_meta.size 与 store_meta.index_size 不一致
    {
      let mut corrupt_meta = meta.clone();
      corrupt_meta.index_meta.size = 1024;
      corrupt_meta.seal();
      write(&meta_file, corrupt_meta.encode())?;

      let err = WedbStore::recover(&ckpt_dir, token, Arc::clone(&device)).await;
      assert!(
        matches!(err, Err(Error::InvalidIndexCkpt(msg)) if msg.contains("大小不匹配")),
        "期望 index_meta.size 不一致被拦截"
      );
    }

    // 还原正确元数据文件并验证恢复成功
    write(&meta_file, meta.encode())?;
    let rec = WedbStore::recover(&ckpt_dir, token, Arc::clone(&device)).await?;
    assert_eq!(rec.entry_count(), 1);

    info!("深度对抗式异常注入防御测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 元数据完整性封签防御（v2 起强制校验）：
/// 1. 语义字段被篡改而封签未同步（JSON 文本级篡改/介质位翻转）→ MetaChecksumMismatch；
/// 2. 封签字段自身被翻转（内容合法、封签不符）→ MetaChecksumMismatch；
/// 3. 最新检查点封签损坏时 recover_latest 自动回退至更早的有效检查点；
/// 4. 篡改后重新封签（合法的发布时变更）恢复成功，封签闭环无误伤。
#[test]
fn test_meta_integrity_seal_defense() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("integrity_seal.db");
    let ckpt_dir = dir.path().join("checkpoints");

    let config = StoreConfig::new(256, 16 * 1024, 16, 0.5)?;

    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
    let store = Arc::new(WedbStore::open(config.clone(), Arc::clone(&device))?);
    let session = store.new_session()?;
    session.upsert(b"old", b"v1").await?;

    let meta_old = store
      .create_checkpoint(&ckpt_dir, CheckpointType::FoldOver)
      .await?;

    session.upsert(b"new", b"v2").await?;
    let meta_new = store
      .create_checkpoint(&ckpt_dir, CheckpointType::FoldOver)
      .await?;
    assert!(meta_new.token > meta_old.token);

    let old_file = ckpt_dir.join(wcpr::meta_filename(meta_old.token));
    let new_file = ckpt_dir.join(wcpr::meta_filename(meta_new.token));

    // 1. 篡改语义字段而未重封签：静默错误恢复类损坏被拦截（期望值/实际值互换可辨）
    {
      let mut tampered = meta_new.clone();
      tampered.store_meta.page_size = 32 * 1024; // 合法但错误的取值
      write(&new_file, tampered.encode())?;

      let err = WedbStore::recover(&ckpt_dir, meta_new.token, Arc::clone(&device)).await;
      match err {
        Err(Error::MetaChecksumMismatch { expected, actual }) => {
          assert_eq!(expected, tampered.integrity_crc32, "期望值为落盘封签");
          assert_eq!(
            actual,
            tampered.integrity_digest(),
            "实际值为对篡改内容的重算摘要"
          );
          assert_ne!(
            tampered.integrity_digest(),
            meta_new.integrity_crc32,
            "重算摘要必须异于原封签"
          );
        }
        Err(e) => panic!("期望 MetaChecksumMismatch，实际 {e}"),
        Ok(_) => panic!("期望 MetaChecksumMismatch，实际恢复成功"),
      }
    }

    // 2. 封签字段自身被翻转：内容合法但封签不符，同样拦截
    {
      let mut flipped = meta_new.clone();
      flipped.integrity_crc32 ^= 0xffff;
      write(&new_file, flipped.encode())?;

      let err = WedbStore::recover(&ckpt_dir, meta_new.token, Arc::clone(&device)).await;
      assert!(
        matches!(err, Err(Error::MetaChecksumMismatch { .. })),
        "期望封签翻转被拦截"
      );
    }

    // 3. recover_latest 回退：最新检查点封签损坏，自动落回更早的有效检查点
    {
      let recovered = WedbStore::recover_latest(&ckpt_dir, Arc::clone(&device)).await?;
      assert_eq!(recovered.entry_count(), 1, "回退至旧检查点仅含旧时点写入");
      assert!(old_file.exists(), "旧检查点文件集保持完好");
    }

    // 4. 篡改后重新封签（信息性字段变更，属发布时合法操作）：恢复成功且数据完整
    {
      let mut benign = meta_new.clone();
      benign.created_at += 1000;
      benign.seal();
      write(&new_file, benign.encode())?;

      let recovered = WedbStore::recover(&ckpt_dir, meta_new.token, Arc::clone(&device)).await?;
      assert_eq!(recovered.entry_count(), 2);
      let s = Arc::new(recovered).new_session()?;
      assert_eq!(s.read(b"old").await?, Some(b"v1".to_vec()));
      assert_eq!(s.read(b"new").await?, Some(b"v2".to_vec()));
    }

    info!("元数据完整性封签防御测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 回退清场防御（恢复回退链问题 2 + 问题 3 + 问题 4 联动）：
/// 1. 最新检查点的 RI 快照魔数损坏 → recover_all_trees_from_dir 判该 Token
///    失败（warn 留痕 + Err），recover_latest 回退至更早有效检查点；
/// 2. 失败轮已预置拷贝的 RI 工作文件被清场钩子摘除——无清场时残留新代
///    工件会在回退轮惰性开树/重放首访时混出代际混杂视图；
/// 3. 恢复句柄携带实际恢复 Token（回退后 find_latest 已不代表恢复版本）。
#[test]
fn test_fallback_cleanup_defense() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    const TUNE: TreeTuning = TreeTuning {
      cache_size: 65536,
      min_record_size: 8,
      max_record_size: 1024,
      max_key_len: 128,
      leaf_page_size: 0,
    };

    let dir = tempdir()?;
    let db_path = dir.path().join("fallback_cleanup.db");
    let ckpt_dir = dir.path().join("checkpoints");
    let ri_dir = dir.path().join("ri");

    let config = StoreConfig::new(512, 16 * 1024, 16, 0.5)?.with_range_index_dir(&ri_dir);
    let (token_old, token_new, stale_stem);
    {
      let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
      let store = Arc::new(WedbStore::open(config.clone(), Arc::clone(&device))?);
      let session = store.new_session()?;

      // 旧代：orders 一棵树一个成员 → T_old 快照（仅含 orders 树）
      session
        .range_index_create(b"orders", StorageBackendType::Memory, TUNE)
        .await?;
      session.range_index_set(b"orders", b"user:1", b"v1").await?;
      let meta_old = store
        .create_checkpoint(&ckpt_dir, CheckpointType::FoldOver)
        .await?;
      token_old = meta_old.token;

      // 新代：新增 cart 树 + orders 新成员 → T_new 快照（orders + cart）
      session
        .range_index_create(b"cart", StorageBackendType::Memory, TUNE)
        .await?;
      session.range_index_set(b"orders", b"user:2", b"v2").await?;
      let meta_new = store
        .create_checkpoint(&ckpt_dir, CheckpointType::FoldOver)
        .await?;
      token_new = meta_new.token;

      // 定位新代独有树快照（cart）并破坏其魔数（介质损坏形态）
      let snapshot_stems = |token: u128| -> Vec<String> {
        let snap_dir = ckpt_dir
          .join(wcpr::token_to_base32(token))
          .join("rangeindex");
        read_dir(snap_dir)
          .expect("快照目录必在")
          .flatten()
          .filter_map(|e| e.file_name().into_string().ok())
          .filter(|n| n.ends_with(".bftree"))
          .collect()
      };
      let old_stems = snapshot_stems(token_old);
      assert_eq!(old_stems.len(), 1, "旧代仅 orders 一棵树");
      let cart_name = snapshot_stems(token_new)
        .into_iter()
        .find(|n| !old_stems.contains(n))
        .expect("新代必有 cart 树快照");
      stale_stem = cart_name.trim_end_matches(".bftree").to_string();

      let cart_path = ckpt_dir
        .join(wcpr::token_to_base32(token_new))
        .join("rangeindex")
        .join(&cart_name);
      let mut file = OpenOptions::new().write(true).open(&cart_path)?;
      file.seek(SeekFrom::Start(0))?;
      file.write_all(&[0xFFu8; 16])?;
      file.flush()?;

      drop(session);
      drop(store);
      drop(device);
    } // 模拟断电宕机

    // 预置失败轮残留：cart 工作文件（模拟失败轮已拷贝的派生工件——真实
    // 生产失败源为后段 IO 故障等不可注入项，此处以残留直证清场摘除）
    let stale_work = ri_dir
      .join("rangeindex")
      .join(format!("{stale_stem}.data.bftree"));
    write(&stale_work, b"stale")?;

    // 重启恢复：T_new 因 cart 快照魔数损坏判失败（本轮 orders 工作文件已
    // 预置拷贝），清场钩子摘除预置工件后回退 T_old
    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
    let recovered = WedbStore::recover_latest(&ckpt_dir, Arc::clone(&device)).await?;

    // 判据①：实际恢复 Token = 旧代（回退轮 Token 出口）
    assert_eq!(
      recovered.recovered_checkpoint_token(),
      Some(token_old),
      "回退轮必须携带实际恢复的旧代 Token"
    );
    // 判据②：失败轮预置工件已被清场摘除（清场失效则残留新代文件）
    assert!(
      !stale_work.exists(),
      "回退清场必须摘除失败轮预置的 RI 工作文件: {}",
      stale_work.display()
    );
    // 判据③：数据态 = 旧代，新代成员不得混入
    let session = Arc::new(recovered).new_session()?;
    assert_eq!(
      session.range_index_get(b"orders", b"user:1").await?,
      Some(b"v1".to_vec()),
      "回退轮恢复旧代成员（惰性开树回读）"
    );
    assert_eq!(
      session.range_index_get(b"orders", b"user:2").await?,
      None,
      "新代成员不得混入回退轮"
    );

    info!("回退清场防御测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}
