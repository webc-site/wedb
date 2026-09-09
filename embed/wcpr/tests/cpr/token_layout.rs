//! token 流转与目录布局：文件集生成、恢复选最新 token、目录 floor 回拨防御

use std::{fs, sync::Arc};

use aok::{OK, Void};
use compio::runtime::Runtime;
use tempfile::tempdir;
use wcpr::{CheckpointManager, CheckpointType, Error, index_filename, meta_filename, next_token};
use wdev::SegmentedDevice;

use super::support::MiniStore;

/// 对标 Garnet CheckpointManagerTests：检查点文件集生成正确、list/latest 检索准确、
/// 恢复可选定任意历史版本或最新版本，purge_outdated 精确回收旧版本
#[test]
fn checkpoint_files_layout_and_latest_recovery() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let ckpt_dir = dir.path().join("checkpoints");
    let store = MiniStore::open(dir.path().join("layout.db"))?;
    let p = store.session()?;
    let mgr = CheckpointManager::<SegmentedDevice>::new();

    store.put(&p, b"key:a", b"v1").await?;
    let meta1 = mgr
      .create_checkpoint(&store, &ckpt_dir, CheckpointType::FoldOver)
      .await?;
    let t1 = meta1.token;

    // 文件集布局：meta 与 index 均以 token 命名，且发布后无 .tmp 残留
    assert!(
      ckpt_dir.join(meta_filename(t1)).is_file(),
      "checkpoint_{t1}.meta 必须存在"
    );
    assert!(
      ckpt_dir.join(index_filename(t1)).is_file(),
      "index_{t1}.ckpt 必须存在"
    );
    assert!(
      !ckpt_dir.join(wcpr::meta_tmp_filename(t1)).exists()
        && !ckpt_dir.join(wcpr::index_tmp_filename(t1)).exists(),
      "成功发布后不得残留 .tmp 临时文件"
    );

    // 第二次检查点：token 严格递增（进程签发闸门单调）
    store.put(&p, b"key:a", b"v2").await?;
    store.put(&p, b"key:b", b"vb").await?;
    let meta2 = mgr
      .create_checkpoint(&store, &ckpt_dir, CheckpointType::Snapshot)
      .await?;
    let t2 = meta2.token;
    assert!(t2 > t1, "token 必须严格单调递增: {t1} -> {t2}");

    // 目录检索：升序全列与最新定位
    assert_eq!(
      CheckpointManager::<SegmentedDevice>::list_checkpoints(&ckpt_dir)?,
      vec![t1, t2]
    );
    assert_eq!(
      CheckpointManager::<SegmentedDevice>::find_latest_checkpoint(&ckpt_dir)?,
      Some(t2)
    );

    // 指定 token 恢复到历史版本
    let device = Arc::new(SegmentedDevice::single_file(dir.path().join("layout.db"))?);
    let restored_old =
      CheckpointManager::recover::<MiniStore>(&ckpt_dir, t1, Arc::clone(&device)).await?;
    let p_old = restored_old.session()?;
    assert_eq!(
      restored_old.get(&p_old, b"key:a").await?.as_deref(),
      Some(b"v1".as_slice()),
      "恢复 t1 必须回到历史版本 v1"
    );

    // recover_latest 恢复到最新版本
    let restored_latest =
      CheckpointManager::recover_latest::<MiniStore>(&ckpt_dir, Arc::clone(&device)).await?;
    let p_latest = restored_latest.session()?;
    assert_eq!(
      restored_latest.get(&p_latest, b"key:a").await?.as_deref(),
      Some(b"v2".as_slice()),
      "recover_latest 必须恢复最新版本 v2"
    );
    assert_eq!(
      restored_latest.get(&p_latest, b"key:b").await?.as_deref(),
      Some(b"vb".as_slice())
    );

    // 回收旧版本：仅保留最新，且最新版本恢复能力不受影响
    let removed = CheckpointManager::<SegmentedDevice>::purge_outdated(&ckpt_dir, 1)?;
    assert_eq!(removed, vec![t1], "purge_outdated 必须由旧到新回收 t1");
    assert_eq!(
      CheckpointManager::<SegmentedDevice>::list_checkpoints(&ckpt_dir)?,
      vec![t2]
    );
    let after_purge =
      CheckpointManager::recover_latest::<MiniStore>(&ckpt_dir, Arc::clone(&device)).await?;
    let p_after = after_purge.session()?;
    assert_eq!(
      after_purge.get(&p_after, b"key:a").await?.as_deref(),
      Some(b"v2".as_slice())
    );

    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// 跨进程墙钟回拨防御：目录内现存最大 token 作为签发下界（floor），
/// 签发值绝不低于历史版本；目录 token 大小序即版本新旧序，与创建时序无关
#[test]
fn token_floor_defends_against_directory_regression() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let ckpt_dir = dir.path().join("checkpoints");
    let store = MiniStore::open(dir.path().join("floor.db"))?;
    let p = store.session()?;
    let mgr = CheckpointManager::<SegmentedDevice>::new();

    // 模拟「另一进程」曾发布过大 token 的检查点（如墙钟超前的旧实例）
    let big_token = 1u128 << 127;
    store.put(&p, b"floor:key", b"v_big").await?;
    mgr
      .create_checkpoint_with_token(&store, &ckpt_dir, CheckpointType::FoldOver, big_token)
      .await?;

    // 本进程自动签发：墙钟候选（约 2^124）低于目录 floor，必须被钳制至 floor + 1
    let auto = mgr
      .create_checkpoint(&store, &ckpt_dir, CheckpointType::FoldOver)
      .await?;
    assert_eq!(
      auto.token,
      big_token + 1,
      "自动签发 token 必须被目录 floor 钳制至 floor + 1"
    );
    assert!(next_token() > auto.token, "进程闸门签发须继续严格递增");

    // token 大小序即版本新旧序：后创建的小 token 绝不反超为「最新」
    store.put(&p, b"floor:key", b"v_300").await?;
    mgr
      .create_checkpoint_with_token(&store, &ckpt_dir, CheckpointType::FoldOver, 300)
      .await?;
    store.put(&p, b"floor:key", b"v_900").await?;
    mgr
      .create_checkpoint_with_token(&store, &ckpt_dir, CheckpointType::FoldOver, 900)
      .await?;

    // token 大小序即版本新旧序：后创建的小 token（300/900）绝不变为「最新」，
    // 最大 token（floor 钳制出的 big+1，其时数据为 v_big）恒为恢复入口
    assert_eq!(
      CheckpointManager::<SegmentedDevice>::find_latest_checkpoint(&ckpt_dir)?,
      Some(big_token + 1),
      "最新 token 恒为目录最大值（big+1），与创建时序无关"
    );
    let device = Arc::new(SegmentedDevice::single_file(dir.path().join("floor.db"))?);
    let latest = CheckpointManager::recover_latest::<MiniStore>(&ckpt_dir, device).await?;
    let p_latest = latest.session()?;
    assert_eq!(
      latest.get(&p_latest, b"floor:key").await?.as_deref(),
      Some(b"v_big".as_slice()),
      "recover_latest 必须选中目录内最大 token 的版本"
    );

    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// purge_all 全量清扫：meta/ckpt/tmp 与孤儿 token 子目录一并回收，
/// 清空后 recover_latest 报 NoValidCheckpoint
#[test]
fn purge_all_sweeps_all_residue() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let ckpt_dir = dir.path().join("checkpoints");
    let store = MiniStore::open(dir.path().join("purge.db"))?;
    let p = store.session()?;
    let mgr = CheckpointManager::<SegmentedDevice>::new();

    store.put(&p, b"purge:key", b"v").await?;
    mgr
      .create_checkpoint(&store, &ckpt_dir, CheckpointType::FoldOver)
      .await?;

    // 注入残留物：.tmp 临时文件与孤儿 token 子目录
    fs::write(ckpt_dir.join(wcpr::meta_tmp_filename(7)), b"garbage")?;
    fs::create_dir_all(ckpt_dir.join("7/rangeindex"))?;
    fs::write(ckpt_dir.join("7/rangeindex/tree.bftree"), b"stale")?;

    mgr.purge_all_checkpoints(&ckpt_dir)?;
    let entries: Vec<_> = fs::read_dir(&ckpt_dir)?
      .flatten()
      .map(|e| e.file_name())
      .collect();
    assert!(
      entries.is_empty(),
      "purge_all 后目录必须全空，残留: {entries:?}"
    );
    assert!(CheckpointManager::<SegmentedDevice>::list_checkpoints(&ckpt_dir)?.is_empty());

    let device = Arc::new(SegmentedDevice::single_file(dir.path().join("purge.db"))?);
    let err = CheckpointManager::recover_latest::<MiniStore>(&ckpt_dir, device)
      .await
      .err()
      .expect("空目录恢复必须失败");
    assert!(
      matches!(err, Error::NoValidCheckpoint(_)),
      "空目录恢复必须报 NoValidCheckpoint: {err}"
    );

    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// IndexCkptHeader、IndexMeta 与 HlogMeta 定长二进制编解码测试
#[test]
fn test_binary_headers_and_meta() -> Void {
  use wcpr::{HlogMeta, IndexCkptHeader, IndexMeta};

  let hdr = IndexCkptHeader {
    version: 1,
    crc: 0x1234_5678,
    token: 0xfeed_cafe_dead_beef_0123_4567_89ab_cdef,
    num_buckets: 1024,
    overflow_count: 16,
    entry_count: 5000,
  };
  let bytes = hdr.encode();
  assert_eq!(bytes.len(), IndexCkptHeader::SIZE);
  let decoded = IndexCkptHeader::decode_opt(&bytes).expect("IndexCkptHeader 解码失败");
  assert_eq!(decoded, hdr);

  // 校验魔数错误分支
  let mut bad_magic = bytes;
  bad_magic[0] ^= 0xff;
  assert!(IndexCkptHeader::decode_opt(&bad_magic).is_none());

  let im = IndexMeta {
    size: 2048,
    overflow_count: 32,
    entry_count: 8888,
  };
  let im_bytes = im.to_bytes();
  assert_eq!(im_bytes.len(), IndexMeta::META_SIZE);
  let im_decoded = IndexMeta::from_bytes(im_bytes);
  assert_eq!(im_decoded, im);
  assert_eq!(IndexMeta::decode_opt(&im_bytes), Some(im));
  assert_eq!(IndexMeta::decode_opt(&im_bytes[..23]), None);

  let hm = HlogMeta {
    begin_address: 64,
    head_address: 4096,
    flushed_until_address: 8192,
    tail_address: 16384,
  };
  let hm_bytes = hm.to_bytes();
  assert_eq!(hm_bytes.len(), HlogMeta::META_SIZE);
  let hm_decoded = HlogMeta::from_bytes(hm_bytes);
  assert_eq!(hm_decoded, hm);
  assert_eq!(HlogMeta::decode_opt(&hm_bytes), Some(hm));
  assert_eq!(HlogMeta::decode_opt(&hm_bytes[..31]), None);

  OK
}
