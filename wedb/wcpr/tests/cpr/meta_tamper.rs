//! meta 完整性防线：封签逐字段篡改拒绝、介质位翻转拒绝、tmp 残留不进恢复视图、
//! 最新检查点损坏时回退更早版本、token 不匹配与超前版本拒绝

use std::{fs, path::Path, sync::Arc};

use aok::{OK, Void};
use compio::runtime::Runtime;
use tempfile::tempdir;
use wcpr::{
  CheckpointManager, CheckpointMeta, CheckpointType, Error, FORMAT_VERSION, HlogMeta, IndexMeta,
  StoreMeta, meta_filename, meta_tmp_filename,
};
use wdev::SegmentedDevice;

use super::support::MiniStore;

/// 构造一个封签完备的样例元数据（覆盖全部载荷字段的非平凡取值）
fn sample_sealed_meta() -> CheckpointMeta {
  let mut meta = CheckpointMeta {
    token: 0x0123_4567_89ab_cdef_u128,
    cp_type: CheckpointType::Snapshot,
    index_meta: IndexMeta {
      size: 64,
      overflow_count: 2,
      entry_count: 100,
    },
    hlog_meta: HlogMeta {
      begin_address: 64,
      head_address: 4096,
      flushed_until_address: 8192,
      tail_address: 16384,
    },
    store_meta: StoreMeta {
      index_size: 64,
      page_size: 16384,
      num_pages: 8,
      mutable_fraction: 0.5,
      max_sessions: 64,
      enable_revivification: false,
      enable_read_cache: true,
      read_cache_num_pages: 8,
      range_index_dir: Some("/data/ri".into()),
      next_key_id: 7,
    },
    created_at: 1_700_000_000_000,
    format_version: FORMAT_VERSION,
    integrity_crc32: 0,
  };
  meta.seal();
  meta
}

/// 改写指定检查点 meta 的任一字段后按 bitcode 原样写回（封签不重算）
fn tamper_meta(ckpt_dir: &Path, token: u128, mutate: impl FnOnce(&mut CheckpointMeta)) -> Void {
  let path = ckpt_dir.join(meta_filename(token));
  let mut meta = CheckpointMeta::decode(&fs::read(&path)?)?;
  mutate(&mut meta);
  fs::write(&path, meta.encode())?;
  Ok(())
}

/// 封签逐字段校验：篡改任意载荷字段（created_at）后恢复必须报 MetaChecksumMismatch，
/// 结构合法的 bitcode 篡改无法逃逸逐字段摘要比对
#[test]
fn tampered_meta_field_rejected_by_integrity_seal() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let ckpt_dir = dir.path().join("checkpoints");
    let store = MiniStore::open(dir.path().join("seal.db"))?;
    let p = store.session()?;
    let mgr = CheckpointManager::<SegmentedDevice>::new();

    store.put(&p, b"seal:key", b"v").await?;
    let meta = mgr
      .create_checkpoint(&store, &ckpt_dir, CheckpointType::FoldOver)
      .await?;

    tamper_meta(&ckpt_dir, meta.token, |m| m.created_at += 1)?;

    let device = Arc::new(SegmentedDevice::single_file(dir.path().join("seal.db"))?);
    let err = CheckpointManager::recover::<MiniStore>(&ckpt_dir, meta.token, device)
      .await
      .err()
      .expect("字段篡改必须导致恢复失败");
    assert!(
      matches!(err, Error::MetaChecksumMismatch { .. }),
      "字段篡改必须被封签拦截: {err}"
    );

    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// 介质位翻转防线：meta 任意单字节位翻转与 index ckpt 数据区位翻转均拒绝恢复
#[test]
fn single_bit_flip_rejects_recovery() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let ckpt_dir = dir.path().join("checkpoints");
    let store = MiniStore::open(dir.path().join("flip.db"))?;
    let p = store.session()?;
    let mgr = CheckpointManager::<SegmentedDevice>::new();

    store.put(&p, b"flip:key", b"payload").await?;
    let meta = mgr
      .create_checkpoint(&store, &ckpt_dir, CheckpointType::FoldOver)
      .await?;
    let token = meta.token;

    // 1. meta 中部字节位翻转：恢复必须失败（结构破坏报解析错误，数值破坏报封签错误）
    let meta_path = ckpt_dir.join(meta_filename(token));
    let pristine = fs::read(&meta_path)?;
    let mut bytes = pristine.clone();
    let mid = bytes.len() / 2;
    bytes[mid] ^= 0x01;
    fs::write(&meta_path, &bytes)?;
    let device = Arc::new(SegmentedDevice::single_file(dir.path().join("flip.db"))?);
    let err = CheckpointManager::recover::<MiniStore>(&ckpt_dir, token, device).await;
    assert!(err.is_err(), "meta 位翻转必须拒绝恢复");
    drop(err);

    // 还原 meta 后重封签，翻转 index ckpt 桶数据区单字节：CRC 校验拦截
    let mut meta = CheckpointMeta::decode(&pristine)?;
    meta.seal();
    fs::write(&meta_path, meta.encode())?;
    let index_path = ckpt_dir.join(wcpr::index_filename(token));
    let mut ckpt_bytes = fs::read(&index_path)?;
    // 64 字节头部之后为桶数据区，CRC 覆盖全部桶数据
    ckpt_bytes[64 + 8] ^= 0x80;
    fs::write(&index_path, &ckpt_bytes)?;
    let device = Arc::new(SegmentedDevice::single_file(dir.path().join("flip.db"))?);
    let err = CheckpointManager::recover::<MiniStore>(&ckpt_dir, token, device).await;
    let err = err.err().expect("index 位翻转必须导致恢复失败");
    assert!(
      matches!(err, Error::ChecksumMismatch { .. }),
      "index 快照数据位翻转必须被 CRC 拦截: {err}"
    );

    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// tmp 残留（未完成 rename 的半截检查点）绝不参与恢复视图；meta 缺失的孤儿 ckpt 同样被无视
#[test]
fn tmp_residue_never_enters_recovery_view() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let ckpt_dir = dir.path().join("checkpoints");
    let store = MiniStore::open(dir.path().join("residue.db"))?;
    let p = store.session()?;

    // 仅存在 .tmp 半截文件：目录无任何可用检查点
    fs::create_dir_all(&ckpt_dir)?;
    fs::write(ckpt_dir.join(meta_tmp_filename(42)), b"{\"half\":")?;
    fs::write(ckpt_dir.join(wcpr::index_tmp_filename(42)), b"half-written")?;
    assert!(
      CheckpointManager::<SegmentedDevice>::list_checkpoints(&ckpt_dir)?.is_empty(),
      ".tmp 残留不得进入 token 列表"
    );
    let device = Arc::new(SegmentedDevice::single_file(dir.path().join("residue.db"))?);
    let err = CheckpointManager::recover_latest::<MiniStore>(&ckpt_dir, Arc::clone(&device))
      .await
      .err()
      .expect("纯 tmp 残留目录必须恢复失败");
    assert!(
      matches!(err, Error::NoValidCheckpoint(_)),
      "纯 tmp 残留目录必须报 NoValidCheckpoint: {err}"
    );

    // 正式检查点发布后：tmp 残留被无视，恢复直达有效版本
    store.put(&p, b"residue:key", b"v").await?;
    let meta = CheckpointManager::<SegmentedDevice>::new()
      .create_checkpoint(&store, &ckpt_dir, CheckpointType::FoldOver)
      .await?;
    let restored = CheckpointManager::recover_latest::<MiniStore>(&ckpt_dir, device).await?;
    let p_restored = restored.session()?;
    assert_eq!(
      restored.get(&p_restored, b"residue:key").await?.as_deref(),
      Some(b"v".as_slice())
    );

    // 孤儿 index ckpt（meta 缺失）：同样不进恢复视图
    fs::write(ckpt_dir.join(wcpr::index_filename(42)), b"orphan-ckpt")?;
    assert_eq!(
      CheckpointManager::<SegmentedDevice>::list_checkpoints(&ckpt_dir)?,
      vec![meta.token],
      "孤儿 ckpt 不得进入 token 列表"
    );

    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// 最新检查点损坏时 recover_latest 由新到旧容错回退，恢复到更早的有效版本
#[test]
fn corrupted_latest_falls_back_to_older_checkpoint() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let ckpt_dir = dir.path().join("checkpoints");
    let store = MiniStore::open(dir.path().join("fallback.db"))?;
    let p = store.session()?;
    let mgr = CheckpointManager::<SegmentedDevice>::new();

    store.put(&p, b"fb:key", b"v1").await?;
    mgr
      .create_checkpoint(&store, &ckpt_dir, CheckpointType::FoldOver)
      .await?;
    store.put(&p, b"fb:key", b"v2").await?;
    let meta2 = mgr
      .create_checkpoint(&store, &ckpt_dir, CheckpointType::FoldOver)
      .await?;

    // 篡改最新检查点（跳过封签校验的干净字段改写同样触发拦截）
    tamper_meta(&ckpt_dir, meta2.token, |m| {
      m.hlog_meta.tail_address += 8;
    })?;

    let device = Arc::new(SegmentedDevice::single_file(
      dir.path().join("fallback.db"),
    )?);
    let restored = CheckpointManager::recover_latest::<MiniStore>(&ckpt_dir, device).await?;
    let p_restored = restored.session()?;
    assert_eq!(
      restored.get(&p_restored, b"fb:key").await?.as_deref(),
      Some(b"v1".as_slice()),
      "最新版本损坏后必须回退至 meta1 历史版本"
    );

    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// meta bitcode 编码防线：bitcode 编解码往返一致、截断与损坏字节显式报错
#[test]
fn meta_bitcode_roundtrip_and_tamper_detect() -> Void {
  let meta = sample_sealed_meta();

  // bitcode 编码往返一致
  let bc = meta.encode();
  assert_eq!(CheckpointMeta::decode(&bc)?, meta);

  // 截断的 bitcode：必须显式报错
  let mut truncated = bc.clone();
  truncated.truncate(truncated.len() / 2);
  assert!(
    CheckpointMeta::decode(&truncated).is_err(),
    "截断 bitcode 必须显式报错"
  );

  // 损坏的垃圾数据：必须显式报错
  assert!(CheckpointMeta::decode(b"\x00garbage").is_err());
  OK
}

/// token 不匹配与超前格式版本均被显式拒绝
#[test]
fn token_mismatch_and_future_version_rejected() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let ckpt_dir = dir.path().join("checkpoints");
    let store = MiniStore::open(dir.path().join("reject.db"))?;
    let p = store.session()?;
    let mgr = CheckpointManager::<SegmentedDevice>::new();

    store.put(&p, b"reject:key", b"v").await?;
    let meta = mgr
      .create_checkpoint(&store, &ckpt_dir, CheckpointType::FoldOver)
      .await?;
    let token = meta.token;

    // meta 复制到异名 token：恢复时 token 须严格比对
    let src = ckpt_dir.join(meta_filename(token));
    let alien_token = token.wrapping_add(1);
    fs::copy(&src, ckpt_dir.join(meta_filename(alien_token)))?;
    let device = Arc::new(SegmentedDevice::single_file(dir.path().join("reject.db"))?);
    let err = CheckpointManager::recover::<MiniStore>(&ckpt_dir, alien_token, Arc::clone(&device))
      .await
      .err()
      .expect("异名 token 恢复必须失败");
    assert!(
      matches!(err, Error::TokenMismatch { .. }),
      "异名 token 恢复必须报 TokenMismatch: {err}"
    );

    // 超前格式版本：新版引擎写入的 meta 被旧引擎拒绝
    tamper_meta(&ckpt_dir, token, |m| {
      m.format_version = FORMAT_VERSION + 1;
      // 重封签使封签自洽，确保拒绝来自版本门控而非封签校验
      m.seal();
    })?;
    let err = CheckpointManager::recover::<MiniStore>(&ckpt_dir, token, device)
      .await
      .err()
      .expect("超前版本恢复必须失败");
    assert!(
      matches!(err, Error::UnsupportedMetaVersion { .. }),
      "超前版本必须被版本门控拒绝: {err}"
    );

    aok::Result::<()>::Ok(())
  })?;
  OK
}
