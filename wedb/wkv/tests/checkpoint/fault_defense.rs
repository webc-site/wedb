//! 快照文件故障注入与防御：损坏文件、CRC 校验失败、截断、魔数与元数据篡改拦截。

use std::{
  fs::{File, OpenOptions, write},
  io::{Read, Seek, SeekFrom, Write},
  sync::Arc,
};

use aok::{OK, Void};
use compio::runtime::Runtime;
use log::info;
use tempfile::tempdir;
use wcpr::{CheckpointType, Error};
use wdev::SegmentedDevice;
use wkv::{CheckpointManager, StoreConfig, WedbStore};

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
    let manager = CheckpointManager::new();

    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
    let store = Arc::new(WedbStore::open(config.clone(), Arc::clone(&device))?);
    let session = store.new_session()?;
    session.upsert(b"foo", b"bar").await?;

    let meta = manager
      .create_checkpoint(&store, &ckpt_dir, CheckpointType::FoldOver)
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

      let err = CheckpointManager::recover(&ckpt_dir, token, Arc::clone(&device)).await;
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

      let err = CheckpointManager::recover(&ckpt_dir, token, Arc::clone(&device)).await;
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

      let err = CheckpointManager::recover(&ckpt_dir, token, Arc::clone(&device)).await;
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

      let err = CheckpointManager::recover(&ckpt_dir, token, Arc::clone(&device)).await;
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
      let list = CheckpointManager::<SegmentedDevice>::list_checkpoints(&ckpt_dir)?;
      assert_eq!(list, vec![token]);

      // 恢复应正常成功
      let recovered = CheckpointManager::recover(&ckpt_dir, token, Arc::clone(&device)).await?;
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
    let manager = CheckpointManager::new();

    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
    let store = Arc::new(WedbStore::open(config.clone(), Arc::clone(&device))?);
    let session = store.new_session()?;
    session.upsert(b"alpha", b"omega").await?;

    let meta = manager
      .create_checkpoint(&store, &ckpt_dir, CheckpointType::FoldOver)
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

      let err = CheckpointManager::recover(&ckpt_dir, token, Arc::clone(&device)).await;
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

      let err = CheckpointManager::recover(&ckpt_dir, token, Arc::clone(&device)).await;
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

      let err = CheckpointManager::recover(&ckpt_dir, token, Arc::clone(&device)).await;
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

      let err = CheckpointManager::recover(&ckpt_dir, token, Arc::clone(&device)).await;
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

      let err = CheckpointManager::recover(&ckpt_dir, token, Arc::clone(&device)).await;
      assert!(
        matches!(err, Err(Error::InvalidIndexCkpt(msg)) if msg.contains("大小不匹配")),
        "期望 index_meta.size 不一致被拦截"
      );
    }

    // 还原正确元数据文件并验证恢复成功
    write(&meta_file, meta.encode())?;
    let rec = CheckpointManager::recover(&ckpt_dir, token, Arc::clone(&device)).await?;
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
    let manager = CheckpointManager::new();

    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
    let store = Arc::new(WedbStore::open(config.clone(), Arc::clone(&device))?);
    let session = store.new_session()?;
    session.upsert(b"old", b"v1").await?;

    let meta_old = manager
      .create_checkpoint(&store, &ckpt_dir, CheckpointType::FoldOver)
      .await?;

    session.upsert(b"new", b"v2").await?;
    let meta_new = manager
      .create_checkpoint(&store, &ckpt_dir, CheckpointType::FoldOver)
      .await?;
    assert!(meta_new.token > meta_old.token);

    let old_file = ckpt_dir.join(wcpr::meta_filename(meta_old.token));
    let new_file = ckpt_dir.join(wcpr::meta_filename(meta_new.token));

    // 1. 篡改语义字段而未重封签：静默错误恢复类损坏被拦截（期望值/实际值互换可辨）
    {
      let mut tampered = meta_new.clone();
      tampered.store_meta.page_size = 32 * 1024; // 合法但错误的取值
      write(&new_file, tampered.encode())?;

      let err = CheckpointManager::recover(&ckpt_dir, meta_new.token, Arc::clone(&device)).await;
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

      let err = CheckpointManager::recover(&ckpt_dir, meta_new.token, Arc::clone(&device)).await;
      assert!(
        matches!(err, Err(Error::MetaChecksumMismatch { .. })),
        "期望封签翻转被拦截"
      );
    }

    // 3. recover_latest 回退：最新检查点封签损坏，自动落回更早的有效检查点
    {
      let recovered = CheckpointManager::recover_latest(&ckpt_dir, Arc::clone(&device)).await?;
      assert_eq!(recovered.entry_count(), 1, "回退至旧检查点仅含旧时点写入");
      assert!(old_file.exists(), "旧检查点文件集保持完好");
    }

    // 4. 篡改后重新封签（信息性字段变更，属发布时合法操作）：恢复成功且数据完整
    {
      let mut benign = meta_new.clone();
      benign.created_at += 1000;
      benign.seal();
      write(&new_file, benign.encode())?;

      let recovered =
        CheckpointManager::recover(&ckpt_dir, meta_new.token, Arc::clone(&device)).await?;
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
