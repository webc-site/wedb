//! 检查点版本号统计（CurrentVersion 与 LastCheckpointedVersion）回归测试
//!
//! 契约（对标 C# TsavoriteKV.lastVersion 与 SystemState.Version）：
//! 1. 检查点失败轮：版本推进窗口已开启，CurrentVersion 停留在新版本；
//!    快照段失败未发布，LastCheckpointedVersion 保持上一成功版本，双字段分列。
//! 2. 检查点成功轮：快照成功落盘发布，LastCheckpointedVersion 单点登记新版本，
//!    与 CurrentVersion 重新相等。
//! 3. 恢复轮：从检查点恢复，set_current_version 将版本基线与成功检查点版本同点对齐，
//!    两字段再次归同。

use std::{fs, str::from_utf8, sync::Arc};

use aok::{OK, Void};
use tempfile::tempdir;
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  RespSessionConsumer,
  database::{GarnetDatabase, SingleDatabaseManager},
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
};
use wnode_test::pump;
use wtest_base::test_store_config;

fn sync_roundtrip(c: &mut RespSessionConsumer, frame: &[u8]) -> Vec<u8> {
  let (consumed, out) = pump(c, frame);
  assert_eq!(consumed, Some(0));
  out
}

fn parse_versions(info_text: &str) -> (i64, i64) {
  let mut cur = None;
  let mut last = None;
  for line in info_text.lines() {
    let line = line.trim();
    if let Some(v) = line.strip_prefix("CurrentVersion:") {
      cur = Some(v.trim().parse::<i64>().unwrap());
    } else if let Some(v) = line.strip_prefix("LastCheckpointedVersion:") {
      last = Some(v.trim().parse::<i64>().unwrap());
    }
  }
  (
    cur.expect("CurrentVersion 字段缺失"),
    last.expect("LastCheckpointedVersion 字段缺失"),
  )
}

#[compio::test]
async fn checkpoint_failure_splits_versions_and_recovery_restores_parity() -> Void {
  let dir = tempdir()?;
  let cp_dir = dir.path().join("cp");
  let db_path = dir.path().join("ckpt_version_test.db");
  let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
  let config = test_store_config();
  let store = Arc::new(WedbStore::open(config, Arc::clone(&device))?);
  let session = store.new_session()?;
  let db = Arc::new(GarnetDatabase::<SegmentedDevice>::new(
    0,
    Arc::clone(&store),
    Arc::clone(&device),
    cp_dir.clone(),
    None,
  ));
  let mgr = Arc::new(SingleDatabaseManager::new(cp_dir.clone(), Arc::clone(&db)));
  let api = StoreGarnetApi::new(session).with_database_manager(Arc::clone(&mgr));
  let mut consumer =
    RespSessionConsumer::new(1, RespServerSessionOptions::default(), Arc::new(api));

  // 0. 初始状态：无 checkpoint 历史，两字段恒 0
  let out = sync_roundtrip(&mut consumer, b"*2\r\n$4\r\nINFO\r\n$5\r\nSTORE\r\n");
  let text = from_utf8(&out)?;
  let (cur, last) = parse_versions(text);
  assert_eq!(cur, 0);
  assert_eq!(last, 0);

  // 1. 故障注入：将 checkpoint_dir 占位为普通文件，令 create_checkpoint_with_token 内部 create_dir_all 报错
  fs::write(&cp_dir, b"blocked_regular_file")?;

  // 触发检查点，快照段必失败
  let fail_res = mgr.take_checkpoint(false).await;
  assert!(fail_res.is_err(), "检查点应因目录被文件阻挡而失败");

  // 验证：失败轮中 CurrentVersion 已随 begin_version_shift 推进，但 LastCheckpointedVersion 仍保持上一成功版本 0
  let out = sync_roundtrip(&mut consumer, b"*2\r\n$4\r\nINFO\r\n$5\r\nSTORE\r\n");
  let text = from_utf8(&out)?;
  let (cur, last) = parse_versions(text);
  assert!(cur > 0, "CurrentVersion 须推进至新版本号: {cur}");
  assert_eq!(
    last, 0,
    "失败轮 LastCheckpointedVersion 必须保持上一成功版本 0"
  );
  assert_ne!(cur, last, "失败轮双字段必须分列");

  // 2. 成功轮：移除阻挡文件，创建合法目录
  fs::remove_file(&cp_dir)?;
  fs::create_dir_all(&cp_dir)?;

  let ok_res = mgr.take_checkpoint(false).await?;
  assert!(ok_res, "检查点应成功发布");

  // 验证：成功轮后 LastCheckpointedVersion 追平 CurrentVersion
  let out = sync_roundtrip(&mut consumer, b"*2\r\n$4\r\nINFO\r\n$5\r\nSTORE\r\n");
  let text = from_utf8(&out)?;
  let (cur, last) = parse_versions(text);
  assert!(cur > 0);
  assert_eq!(cur, last, "成功轮两字段必须重新相等");

  // 3. 恢复轮：从检查点执行恢复
  let recovered = mgr
    .base
    .recover_database_checkpoint_async(&db, None)
    .await?;
  assert!(recovered.is_some(), "恢复应成功找到快照");

  // 验证：恢复轮两字段同点对齐，再次归同
  let out = sync_roundtrip(&mut consumer, b"*2\r\n$4\r\nINFO\r\n$5\r\nSTORE\r\n");
  let text = from_utf8(&out)?;
  let (cur_rec, last_rec) = parse_versions(text);
  assert_eq!(cur_rec, last_rec, "恢复轮两字段必须归同");
  assert_eq!(cur_rec, cur, "恢复版本须等于先前成功发布的版本");

  OK
}
