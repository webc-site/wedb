//! RENAME 补偿臂复验待删内容回归测试（票 zcode-r37-lockfix 发现 B）
//!
//! 验证当旧键被并发删除导致 RENAME 失败进入补偿臂时：
//! 1. 若新键被并发盲写修改，补偿臂不得盲删新键，杜绝吞掉并发已确认写；
//! 2. 若新键无并发盲写，补偿臂将其正常删除复原。

use std::sync::{
  Arc,
  atomic::{AtomicBool, Ordering},
};

use tempfile::tempdir;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, StoreEvent, StoreEventSink, WedbStore};
use wnode::resp::{
  garnet_api::{GarnetApi, StoreGarnetApi},
  resp_server_session::{RespServerSession, RespServerSessionOptions},
};
use wresp::command::RespCommand;

struct RenameRaceContext {
  store: Arc<WedbStore<SegmentedDevice>>,
  overwrite_new: bool,
  triggered: AtomicBool,
}

fn race_rename_sink(
  ctx: &RenameRaceContext,
  _ver: i64,
  _aof_session_id: i32,
  event: StoreEvent<'_>,
) -> wkv::Result<()> {
  match event {
    StoreEvent::Write {
      key,
      tombstone: false,
      ..
    } if key.ends_with(b"new") => {
      if ctx.triggered.swap(true, Ordering::AcqRel) {
        return Ok(());
      }
      // 模拟并发删除旧键：令 RENAME 尾部 try_delete_sync(old) 返回 NotFound 触发补偿
      let sess = ctx.store.new_session()?;
      let _ = sess.try_delete_sync(b"old")?;

      // 若启用并发覆写，模拟并发盲写修改 new 键
      if ctx.overwrite_new {
        let _ = sess.try_upsert_sync(b"new", b"concurrent_val")?;
      }
      Ok(())
    }
    _ => Ok(()),
  }
}

fn open_store_with_sink(path: &str, overwrite_new: bool) -> (GarnetApi, tempfile::TempDir) {
  let dir = tempdir().unwrap();
  let config = StoreConfig::new(1024, 64 * 1024, 16, 0.5).unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(path)).unwrap());
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  assert!(store.set_event_sink(StoreEventSink::new(
    Arc::new(RenameRaceContext {
      store: Arc::clone(&store),
      overwrite_new,
      triggered: AtomicBool::new(false),
    }),
    race_rename_sink,
  )));
  let api: GarnetApi = Arc::new(StoreGarnetApi::new(store.new_session().unwrap()));
  (api, dir)
}

fn session_with(api: &GarnetApi) -> RespServerSession {
  let mut s = RespServerSession::new(1, RespServerSessionOptions::default());
  s.set_garnet_api(api.clone());
  s
}

/// RENAME 补偿臂复验待删内容：
/// 新键出现并发盲写时，补偿臂不得吞掉并发写入值
#[test]
fn rename_compensation_preserves_concurrent_blind_write() {
  let (api, _dir) = open_store_with_sink("rename-race-preserve.db", true);
  let mut s = session_with(&api);

  // 预置旧键
  api.exec(&mut s, RespCommand::Set, &[b"old", b"v_old"]);
  assert_eq!(s.output, b"+OK\r\n");
  s.output.clear();

  // 执行 RENAME old new：
  // 写 new 后 sink 并发删除 old 并盲写 new 为 concurrent_val，
  // RENAME 删 old 失败进入补偿臂，因 new 已被并发改写，补偿臂跳过删除
  api.exec(&mut s, RespCommand::Rename, &[b"old", b"new"]);
  assert_eq!(s.output, b"-ERR no such key\r\n");
  s.output.clear();

  // new 上的并发写得到保全
  api.exec(&mut s, RespCommand::Get, &[b"new"]);
  assert_eq!(s.output, b"$14\r\nconcurrent_val\r\n");
}

/// RENAME 补偿臂正常清除：
/// 无并发盲写时，补偿臂将 new 正常清除
#[test]
fn rename_compensation_cleans_new_key_when_no_concurrent_write() {
  let (api, _dir) = open_store_with_sink("rename-clean-comp.db", false);
  let mut s = session_with(&api);

  // 预置旧键
  api.exec(&mut s, RespCommand::Set, &[b"old", b"v_old"]);
  assert_eq!(s.output, b"+OK\r\n");
  s.output.clear();

  // 执行 RENAME old new：
  // 写 new 后 sink 并发删除 old，无并发改写 new。补偿臂正常删除 new
  api.exec(&mut s, RespCommand::Rename, &[b"old", b"new"]);
  assert_eq!(s.output, b"-ERR no such key\r\n");
  s.output.clear();

  // new 已被补偿臂清除
  api.exec(&mut s, RespCommand::Get, &[b"new"]);
  assert_eq!(s.output, b"$-1\r\n");
}
