//! 单机启动恢复「清未用」物理枚举回归（孤儿快照泄漏）
//!
//! 对位修复：`DatabaseManagerBase::purge_unrecovered_checkpoints` 的枚举口由
//! `wcpr::list_checkpoints`（仅认含完整 `.meta` 的已提交快照）改为
//! `wcpr::list_all_checkpoint_tokens`（物理实体全量，含未提交/损坏孤儿），
//! 精确对标 C# `DeviceLogCommitCheckpointManager.OnRecovery`（:337-365）经
//! `GetLogCheckpointTokens`/`GetIndexCheckpointTokens`（:235/:285
//! `deviceFactory.ListContents` 物理列举、不以元数据完整性过滤）删除一切未被
//! 本次恢复选中快照的语义。
//!
//! 孤儿现场来源（真实崩溃形态，非人造）：检查点写出第 2 步 index 快照 rename
//! 发布（`wcpr/src/index_ckpt/mod.rs:136`）先于第 9 步 `.meta` rename 提交
//!（`wcpr/src/manager/create.rs:377`），两步之间被 kill -9 / OOM 即沉淀
//! 「index_<b32>.ckpt 与 <b32>/ RangeIndex 子目录在、meta 缺」的半截快照；
//! 单机轨若按 meta 过滤枚举，本口对其永不调用 `purge_checkpoint`，每轮崩溃
//! 累积一份 index 量级的残留直至 ENOSPC 令检查点写出 fail-closed。

use std::{fs, path::Path, sync::Arc};

use compio::runtime::Runtime;
use wcpr::{
  index_filename, list_all_checkpoint_tokens, list_checkpoints, meta_filename, token_to_base32,
};
use wnode::database::{GarnetDatabase, SingleDatabaseManager};
use wtest_base::open_test_store;

/// 孤儿快照现场：index 文件 + Base32 RangeIndex 子目录已落盘，`.meta` 提交标记始终未到
fn seed_meta_less_orphan(dir: &Path, token: u128) {
  fs::write(dir.join(index_filename(token)), b"index-half").expect("seed orphan index");
  let b32 = token_to_base32(token);
  let ri_sub = dir.join(b32.as_str()).join("rangeindex");
  fs::create_dir_all(&ri_sub).expect("seed orphan ri dir");
  fs::write(ri_sub.join("tree.bftree"), b"tree-half").expect("seed orphan tree");
  // 缺 meta 正是本票的引爆点：恢复选点面看不见它，清理面必须兜住它
  assert!(
    !dir.join(meta_filename(token)).exists(),
    "孤儿现场必须无 meta 提交标记"
  );
}

/// 断言孤儿物理痕迹彻底移除（index 文件与 Base32 子目录双双消失，任何逃逸即红）
fn assert_orphan_gone(dir: &Path, token: u128) {
  assert!(
    !dir.join(index_filename(token)).exists(),
    "孤儿 index 快照文件必须被物理删除"
  );
  assert!(
    !dir.join(token_to_base32(token).as_str()).exists(),
    "孤儿 Base32 RangeIndex 子目录必须被递归删除"
  );
}

/// 以恢复 Token 的版本投影域为基线派生孤儿 Token：`delta` 为高位偏移（±1 代），
/// 低位取固定判别值（与真实签发值同形制，绝不与在用快照的 Token 相同）
fn orphan_token_near(recovered: u128, delta: i64) -> u128 {
  let hi = (recovered >> 64) as i64;
  assert_ne!(hi, 0, "真实签发的 Token 高 64 位为版本投影域，必非零");
  let shifted = (hi + delta) as u64;
  (shifted as u128) << 64 | 0x0a1b_2c3d_4e5f_6071_u128
}

/// 启动恢复「清未用」回收旧代孤儿快照：预置一份「比恢复基线更旧」的无 meta 孤儿
///（index 文件 + RangeIndex 子目录），启动恢复后孤儿被物理回收、有效版原样保留、
/// 恢复选点不回退、目录零残留；无关同形文件不受波及
///
/// 反向注入判据：把枚举口回装为 `wcpr::list_checkpoints`（meta 过滤），孤儿 Token
/// 无从进入候选 → `purge_checkpoint` 永不调用 → 本用例的 `assert_orphan_gone` 与
/// 「目录零残留」断言必红。
#[test]
fn startup_recovery_purge_reclaims_older_orphan_snapshot() -> aok::Result<()> {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (dir, store) = open_test_store("purge_orphan_older")?;
    let cp_dir = dir.path().join("checkpoints");
    let db = Arc::new(GarnetDatabase::new(
      0,
      Arc::clone(&store),
      Arc::clone(&store.device),
      cp_dir.clone(),
      None,
    ));
    let single = SingleDatabaseManager::new(cp_dir.clone(), Arc::clone(&db));

    let session = store.new_session()?;
    session.upsert(b"ck_key", b"ck_val").await?;
    assert!(single.take_checkpoint(true).await?, "快照须发布");
    let recovered =
      wcpr::find_latest_checkpoint(&cp_dir)?.expect("已提交快照须可被恢复选点枚举（meta 过滤轨）");

    // 旧代孤儿（版本投影严格低于基线，走 debug_assert 次析取臂）
    let orphan = orphan_token_near(recovered, -1);
    seed_meta_less_orphan(&cp_dir, orphan);
    // 非快照名（Base32 解码必失败）：物理枚举与删除臂都不得误命中
    fs::write(cp_dir.join("readme.txt"), b"foreign")?;
    fs::create_dir_all(cp_dir.join("not_a_token_dir"))?;

    // 枚举口径分工（清理前的现场事实，锁死不可互换）：
    // 恢复选点轨看不见孤儿，清理候选轨必须看见
    assert_eq!(
      list_checkpoints(&cp_dir)?,
      vec![recovered],
      "缺 meta 的孤儿不得进入恢复选点候选"
    );
    assert!(
      list_all_checkpoint_tokens(&cp_dir)?.contains(&orphan),
      "全量物理枚举须把孤儿纳入清理候选"
    );

    // 启动恢复（生产入口 recover_database_checkpoint_async → 清未用尾段）
    let recovered_store = single
      .base
      .recover_database_checkpoint_async(&db, None)
      .await?
      .expect("最新有效快照须可恢复");

    assert_orphan_gone(&cp_dir, orphan);
    // 有效版原样保留
    assert!(
      cp_dir.join(meta_filename(recovered)).is_file()
        && cp_dir.join(index_filename(recovered)).is_file(),
      "本次恢复选中那一版的文件集须原样保留"
    );
    // 磁盘零残留：物理枚举只剩在用那一版
    assert_eq!(
      list_all_checkpoint_tokens(&cp_dir)?,
      vec![recovered],
      "恢复后检查点目录不得残留任何孤儿物理痕迹"
    );
    assert_eq!(
      list_checkpoints(&cp_dir)?,
      vec![recovered],
      "recover_latest 的选中不回退（meta 过滤轨语义不变）"
    );
    // 无关文件不受波及（物理枚举按 Base32 命名门分派，绝不误吞）
    assert!(cp_dir.join("readme.txt").is_file() && cp_dir.join("not_a_token_dir").is_dir());
    // 数据面：恢复视图恰为该版内容
    let rs = recovered_store.new_session()?;
    assert_eq!(
      rs.read(b"ck_key").await?,
      Some(b"ck_val".to_vec()),
      "恢复视图须读回选中那一版的写入"
    );
    aok::OK
  })
}

/// 启动恢复「清未用」回收「新于恢复基线」的孤儿快照：删除生效且 debug_assert
/// 臂不触发（首析取 `stale > recovered` 直接放行）
///
/// 场景对标真实形态：崩溃前一轮的 index 快照已 rename 发布（Token 高于本次恢复
/// 选中的那一版），meta 未及提交即被杀；显式恢复旧 Token 后更新的孤儿同样属
/// 「未被本次恢复选中」，必须回收
#[test]
fn startup_recovery_purge_reclaims_newer_orphan_snapshot() -> aok::Result<()> {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (dir, store) = open_test_store("purge_orphan_newer")?;
    let cp_dir = dir.path().join("checkpoints");
    let db = Arc::new(GarnetDatabase::new(
      0,
      Arc::clone(&store),
      Arc::clone(&store.device),
      cp_dir.clone(),
      None,
    ));
    let single = SingleDatabaseManager::new(cp_dir.clone(), Arc::clone(&db));

    let session = store.new_session()?;
    session.upsert(b"k_old", b"v_old").await?;
    assert!(single.take_checkpoint(true).await?, "第一代快照须发布");
    let older = wcpr::find_latest_checkpoint(&cp_dir)?.expect("第一代快照");

    session.upsert(b"k_new", b"v_new").await?;
    assert!(single.take_checkpoint(true).await?, "第二代快照须发布");
    let newest = wcpr::find_latest_checkpoint(&cp_dir)?.expect("第二代快照");
    assert!(newest > older, "第二代 Token 须严格更新");

    // 新于在用基线的孤儿（版本投影严格高于基线，走 debug_assert 首析取臂）
    let orphan = orphan_token_near(newest, 1);
    seed_meta_less_orphan(&cp_dir, orphan);

    // 显式恢复较旧一代：更新的代与更新的孤儿同属未选中集，须一并回收
    let recovered_store = single
      .base
      .recover_database_checkpoint_async(&db, Some(older))
      .await?
      .expect("历史快照须可恢复");

    assert_orphan_gone(&cp_dir, orphan);
    assert_eq!(
      list_all_checkpoint_tokens(&cp_dir)?,
      vec![older],
      "未选中的快照与孤儿须全数物理回收，只余被选中的那一版"
    );
    let rs = recovered_store.new_session()?;
    assert_eq!(rs.read(b"k_old").await?, Some(b"v_old".to_vec()));
    assert_eq!(
      rs.read(b"k_new").await?,
      None,
      "恢复视图须恰为所选那一版，不得含其后代写入"
    );
    aok::OK
  })
}
