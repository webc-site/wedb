//! token 流转与目录布局：文件集生成、恢复选最新 token、目录 floor 高位钳制回拨防御

use std::{fs, sync::Arc};

use aok::{OK, Void};
use compio::runtime::Runtime;
use tempfile::tempdir;
use wcpr::{CheckpointType, CkptGateState, CprStore, Error, index_filename, meta_filename};
use wdev::SegmentedDevice;

use super::support::MiniStore;

/// 对标 Garnet CheckpointManagerTests：检查点文件集生成正确、list/latest 检索准确、
/// 恢复可选定任意历史版本或最新版本，purge_outdated 精确回收旧版本
#[compio::test]
async fn checkpoint_files_layout_and_latest_recovery() -> Void {
  let dir = tempdir()?;
  let ckpt_dir = dir.path().join("checkpoints");
  let store = MiniStore::open(dir.path().join("layout.db"))?;
  let gate = CkptGateState::default();
  let p = store.session()?;

  store.put(&p, b"key:a", b"v1").await?;
  let meta1 = wcpr::create_checkpoint(&store, &gate, &ckpt_dir, CheckpointType::FoldOver).await?;
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
  let meta2 = wcpr::create_checkpoint(&store, &gate, &ckpt_dir, CheckpointType::Snapshot).await?;
  let t2 = meta2.token;
  assert!(t2 > t1, "token 必须严格单调递增: {t1} -> {t2}");

  // 目录检索：升序全列与最新定位
  assert_eq!(wcpr::list_checkpoints(&ckpt_dir)?, vec![t1, t2]);
  assert_eq!(wcpr::find_latest_checkpoint(&ckpt_dir)?, Some(t2));

  // 指定 token 恢复到历史版本
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("layout.db"))?);
  let restored_old = wcpr::recover::<_, MiniStore>(&ckpt_dir, t1, Arc::clone(&device)).await?;
  let p_old = restored_old.session()?;
  assert_eq!(
    restored_old.get(&p_old, b"key:a").await?.as_deref(),
    Some(b"v1".as_slice()),
    "恢复 t1 必须回到历史版本 v1"
  );

  // recover_latest 恢复到最新版本
  let restored_latest =
    wcpr::recover_latest::<_, MiniStore>(&ckpt_dir, Arc::clone(&device)).await?;
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
  let removed = wcpr::purge_outdated(&ckpt_dir, 1)?;
  assert_eq!(removed, vec![t1], "purge_outdated 必须由旧到新回收 t1");
  assert_eq!(wcpr::list_checkpoints(&ckpt_dir)?, vec![t2]);
  let after_purge = wcpr::recover_latest::<_, MiniStore>(&ckpt_dir, Arc::clone(&device)).await?;
  let p_after = after_purge.session()?;
  assert_eq!(
    after_purge.get(&p_after, b"key:a").await?.as_deref(),
    Some(b"v2".as_slice())
  );
  OK
}

/// 跨进程墙钟回拨防御：目录内现存最大 token 作为签发下界（floor），
/// 签发值绝不低于历史版本且高 64 位（版本投影域）跨代严格递增；
/// 目录 token 大小序即版本新旧序，与创建时序无关
#[compio::test]
async fn token_floor_defends_against_directory_regression() -> Void {
  let dir = tempdir()?;
  let ckpt_dir = dir.path().join("checkpoints");
  let store = MiniStore::open(dir.path().join("floor.db"))?;
  let gate = CkptGateState::default();
  let p = store.session()?;

  // 模拟「另一进程」曾发布过大 token 的检查点（如墙钟超前的旧实例）
  let big_token = 1u128 << 127;
  store.put(&p, b"floor:key", b"v_big").await?;
  let index_start = store.tail_address();
  wcpr::create_checkpoint_with_token(
    &store,
    &gate,
    &ckpt_dir,
    CheckpointType::FoldOver,
    big_token,
    index_start,
  )
  .await?;

  // 本进程自动签发：墙钟候选（约 2^124）低于目录 floor，必须被高位钳制抬升：
  // 版本投影域（高 64 位）恰抬升一代（钳制等式），全序绝不回拨
  let auto = wcpr::create_checkpoint(&store, &gate, &ckpt_dir, CheckpointType::FoldOver).await?;
  assert_eq!(
    auto.token >> 64,
    (big_token >> 64) + 1,
    "自动签发高 64 位必须恰为目录 floor 高位 + 1（钳制等式）"
  );
  assert!(auto.token > big_token, "签发值全序严格大于目录 floor");

  // token 大小序即版本新旧序：后创建的小 token 绝不反超为「最新」
  store.put(&p, b"floor:key", b"v_300").await?;
  let index_start = store.tail_address();
  wcpr::create_checkpoint_with_token(
    &store,
    &gate,
    &ckpt_dir,
    CheckpointType::FoldOver,
    300,
    index_start,
  )
  .await?;
  store.put(&p, b"floor:key", b"v_900").await?;
  let index_start = store.tail_address();
  wcpr::create_checkpoint_with_token(
    &store,
    &gate,
    &ckpt_dir,
    CheckpointType::FoldOver,
    900,
    index_start,
  )
  .await?;

  // token 大小序即版本新旧序：后创建的小 token（300/900）绝不变为「最新」，
  // 最大 token（高位钳制抬升签发的 auto，其时数据为 v_big）恒为恢复入口
  assert_eq!(
    wcpr::find_latest_checkpoint(&ckpt_dir)?,
    Some(auto.token),
    "最新 token 恒为目录最大值（auto 签发值），与创建时序无关"
  );
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("floor.db"))?);
  let latest = wcpr::recover_latest::<_, MiniStore>(&ckpt_dir, device).await?;
  let p_latest = latest.session()?;
  assert_eq!(
    latest.get(&p_latest, b"floor:key").await?.as_deref(),
    Some(b"v_big".as_slice()),
    "recover_latest 必须选中目录内最大 token 的版本"
  );

  // 进程闸门签发须继续严格递增：高位钳制签发已抬升进程内历史签发最大值，
  // 其后的自动签发绝不再回落至目录已有 token（黑盒等价原 next_token 断言）
  let auto_next =
    wcpr::create_checkpoint(&store, &gate, &ckpt_dir, CheckpointType::FoldOver).await?;
  assert!(
    auto_next.token > auto.token,
    "floor 钳制后自动签发须继续严格递增: {} -> {}",
    auto.token,
    auto_next.token
  );
  OK
}

/// 跨进程低位判别：C# 各 Take*Checkpoint 的 token 为 `Guid.NewGuid()`（128 位
/// 随机全局唯一，不依赖时钟与进程内序列）；rust「墙钟 + 序列」替代形制的低位由
/// 进程判别盐 + 自增序列铸型（[`wcpr::mint_low`]）。两签发主体各自 LAST_TOKEN
/// 为空、同目录同 floor、TOKEN_SEQ 同起点（重启重叠/双实例共目录）时，旧裸序列
/// 低位必同值撞号——文件集按 token 命名互相覆写、失败清场按 token 回收互毁；
/// 盐判别形制使两次签发 token 不等、文件集互不覆写
#[test]
fn token_low_bits_discriminate_concurrent_issuers() -> Void {
  // 同机异 pid 为主威胁面（同一检查点目录仅同机文件系统可达）；
  // 同 pid 异熵覆盖 pid 回收复用（重启重叠时新旧进程 pid 同值）
  let salt_a = wcpr::salt_bits(4711, 0xA5A5_5A5A_DEAD_BEEF);
  let salt_b = wcpr::salt_bits(4712, 0xA5A5_5A5A_DEAD_BEEF);
  let salt_reused = wcpr::salt_bits(4711, 0x5A5A_A5A5_BEEF_DEAD);

  // 两主体同 tick 候选高位同值、目录 floor 同值、TOKEN_SEQ 同起点 1
  let hi = 0x0123_4567_89AB_CDEFu128;
  let issue = |salt: u64| (hi << 64) | wcpr::mint_low(salt, 1) as u128;
  let token_a = issue(salt_a);
  let token_b = issue(salt_b);
  let token_reused = issue(salt_reused);

  assert_ne!(token_a, token_b, "异 pid 同起点同 floor 签发必须相异");
  assert_ne!(
    token_a, token_reused,
    "pid 回收（同 pid 异启动熵）签发必须相异"
  );
  assert_ne!(token_b, token_reused);

  // 文件集互不覆写：token 相异 ⇒ meta/index/token 子目录名全异，
  // 失败清场（purge 按 token 回收）不再互毁
  assert_ne!(
    wcpr::meta_filename(token_a),
    wcpr::meta_filename(token_b),
    "meta 文件名必须互不相覆"
  );
  assert_ne!(
    wcpr::index_filename(token_a),
    wcpr::index_filename(token_b),
    "index 文件名必须互不相覆"
  );
  assert_ne!(
    wcpr::token_to_base32(token_a),
    wcpr::token_to_base32(token_b),
    "token 子目录名必须互不相覆"
  );

  // 生产闸门端到端：自动签发低位走同一铸型（盐取自本进程），连续两次签发恒相异
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let ckpt_dir = dir.path().join("checkpoints");
    let store = MiniStore::open(dir.path().join("lowbits.db"))?;
    let gate = CkptGateState::default();
    let p = store.session()?;

    store.put(&p, b"low:key", b"v1").await?;
    let meta1 = wcpr::create_checkpoint(&store, &gate, &ckpt_dir, CheckpointType::FoldOver).await?;
    store.put(&p, b"low:key", b"v2").await?;
    let meta2 = wcpr::create_checkpoint(&store, &gate, &ckpt_dir, CheckpointType::FoldOver).await?;
    assert_ne!(meta1.token, meta2.token, "生产闸门连续签发必须恒相异");
    assert_ne!(
      wcpr::meta_filename(meta1.token),
      wcpr::meta_filename(meta2.token),
      "生产签发的文件集必须互不覆写"
    );

    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// purge_all 全量清扫：meta/ckpt/tmp 与孤儿 token 子目录一并回收，
/// 清空后 recover_latest 报 NoValidCheckpoint
///
/// libs/storage/Tsavorite/cs/test/test.recovery/CheckpointManagerTests.cs:CheckpointManagerPurgeCheck
/// （C# 走 DeviceLogCommitCheckpointManager.PurgeAll 并断言目录清空；rust 由
/// wcpr purge_all 承接同一清扫契约，本地与 Azure 双形态收敛为单实现）
#[compio::test]
async fn purge_all_sweeps_all_residue() -> Void {
  let dir = tempdir()?;
  let ckpt_dir = dir.path().join("checkpoints");
  let store = MiniStore::open(dir.path().join("purge.db"))?;
  let gate = CkptGateState::default();
  let p = store.session()?;

  store.put(&p, b"purge:key", b"v").await?;
  wcpr::create_checkpoint(&store, &gate, &ckpt_dir, CheckpointType::FoldOver).await?;

  // 注入残留物：.tmp 临时文件与孤儿 token 子目录（Base32 命名）
  let b32_7 = wcpr::token_to_base32(7);
  fs::write(ckpt_dir.join(wcpr::meta_tmp_filename(7)), b"garbage")?;
  fs::create_dir_all(ckpt_dir.join(format!("{b32_7}/rangeindex")))?;
  fs::write(
    ckpt_dir.join(format!("{b32_7}/rangeindex/tree.bftree")),
    b"stale",
  )?;

  wcpr::purge_all(&ckpt_dir)?;
  let entries: Vec<_> = fs::read_dir(&ckpt_dir)?
    .flatten()
    .map(|e| e.file_name())
    .collect();
  assert!(
    entries.is_empty(),
    "purge_all 后目录必须全空，残留: {entries:?}"
  );
  assert!(wcpr::list_checkpoints(&ckpt_dir)?.is_empty());

  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("purge.db"))?);
  let err = wcpr::recover_latest::<_, MiniStore>(&ckpt_dir, device)
    .await
    .err()
    .expect("空目录恢复必须失败");
  assert!(
    matches!(err, Error::NoValidCheckpoint(_)),
    "空目录恢复必须报 NoValidCheckpoint: {err}"
  );
  OK
}
