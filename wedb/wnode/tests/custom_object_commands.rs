//! 自定义对象命令执行面集成测试（对标 libs/server/Custom/CustomRespCommands.cs）

#![cfg(any(feature = "roaring", feature = "json"))]

use std::{
  sync::{Arc, OnceLock, mpsc},
  thread,
};

use parking_lot::Mutex;
use tempfile::tempdir;
use wcol::object_payload::ObjLoad;
use wcustom::{CustomObjectFns, RespVersion};
use wdev::SegmentedDevice;
use wkv::{BatchStoreSession, StoreConfig, WedbStore};
use wnode::resp::objects::{
  custom_object_commands::{
    CustomObjCtx, CustomObjMutation, CustomObjOutcome, CustomObjStep, dispatch_custom_object_read,
    dispatch_custom_object_rmw, try_custom_object_rmw_sync,
  },
  object_store_utils::obj_load_custom_sync,
};
use wresp::ext::RespVecExt;
use wval::CustomObjectType;

fn dummy_fns() -> CustomObjectFns {
  CustomObjectFns {
    need_initial_update: |_args, output, _resp_version| {
      if _args.first().copied() == Some(b"bad") {
        output.extend_from_slice(b"-ERR bad\r\n");
        false
      } else {
        true
      }
    },
    updater: |payload, args, output, _resp_version| {
      if args.first().copied() == Some(b"fail") {
        output.extend_from_slice(b"-ERR fail\r\n");
        false
      } else {
        payload.extend_from_slice(args.first().copied().unwrap_or(&[]));
        true
      }
    },
    reader: |payload, _args, output, _resp_version| {
      output.extend_from_slice(payload);
      true
    },
    not_found: |_args, output, resp_version| {
      output.write_resp_null_ver(resp_version);
    },
    is_empty: |payload| payload.is_empty(),
  }
}

#[test]
fn test_dispatch_read() {
  let fns = dummy_fns();
  let mut out = Vec::new();

  // Missing -> NotFound
  dispatch_custom_object_read(&fns, None, &[], &mut out, 2);
  assert_eq!(out, b"$-1\r\n");

  // Present -> Reader (借用切片零拷贝)
  out.clear();
  dispatch_custom_object_read(&fns, Some(b"hello"), &[], &mut out, 2);
  assert_eq!(out, b"hello");
}

#[test]
fn test_dispatch_rmw() {
  let fns = dummy_fns();
  let mut out = Vec::new();

  // Degrade
  let step = dispatch_custom_object_rmw(&fns, ObjLoad::Degrade, &[], &mut out, 2);
  assert_eq!(step, CustomObjStep::Degrade);

  // WrongType
  let step = dispatch_custom_object_rmw(&fns, ObjLoad::WrongType, &[], &mut out, 2);
  assert_eq!(step, CustomObjStep::Done);

  // Missing + need_initial_update returns false
  out.clear();
  let step = dispatch_custom_object_rmw(&fns, ObjLoad::Missing, &[b"bad"], &mut out, 2);
  assert_eq!(step, CustomObjStep::Done);
  assert_eq!(out, b"-ERR bad\r\n");

  // Missing + need_initial_update ok + updater returns false
  out.clear();
  let step = dispatch_custom_object_rmw(&fns, ObjLoad::Missing, &[b"fail"], &mut out, 2);
  assert_eq!(step, CustomObjStep::Done);
  assert_eq!(out, b"-ERR fail\r\n");

  // Missing + need_initial_update ok + updater ok + empty payload -> Done (no ghost tombstone)
  out.clear();
  let step = dispatch_custom_object_rmw(&fns, ObjLoad::Missing, &[], &mut out, 2);
  assert_eq!(step, CustomObjStep::Done);

  // Missing + need_initial_update ok + updater ok + non-empty payload -> Mutate(Save)
  out.clear();
  let step = dispatch_custom_object_rmw(&fns, ObjLoad::Missing, &[b"v1"], &mut out, 2);
  assert_eq!(
    step,
    CustomObjStep::Mutate(CustomObjMutation::Save(b"v1".to_vec()))
  );

  // Present + updater ok + non-empty payload -> Mutate(Save)
  out.clear();
  let step = dispatch_custom_object_rmw(
    &fns,
    ObjLoad::Present(b"p0".to_vec()),
    &[b"v1"],
    &mut out,
    2,
  );
  assert_eq!(
    step,
    CustomObjStep::Mutate(CustomObjMutation::Save(b"p0v1".to_vec()))
  );

  // Present + emptied payload -> Mutate(Delete) (strict empty deletion)
  let mut empty_fns = dummy_fns();
  empty_fns.updater = |payload, _args, _out, _resp_version| {
    payload.clear();
    true
  };
  let step = dispatch_custom_object_rmw(
    &empty_fns,
    ObjLoad::Present(b"old".to_vec()),
    &[],
    &mut out,
    2,
  );
  assert_eq!(step, CustomObjStep::Mutate(CustomObjMutation::Delete));
}

/// 小预算独立测试库
fn open_store(tag: &str) -> (tempfile::TempDir, Arc<WedbStore<SegmentedDevice>>) {
  let dir = tempdir().expect("tempdir");
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag)).expect("测试设备打开"));
  let store = Arc::new(
    WedbStore::open(StoreConfig::auto_with_budget(16 << 20), device).expect("测试存储打开"),
  );
  (dir, store)
}

/// 确定性会合闸门
struct Gate {
  held_tx: mpsc::SyncSender<()>,
  held_rx: Mutex<mpsc::Receiver<()>>,
  acked_tx: mpsc::SyncSender<()>,
  acked_rx: Mutex<mpsc::Receiver<()>>,
}

static GATE: OnceLock<Gate> = OnceLock::new();

fn gate() -> &'static Gate {
  GATE.get_or_init(|| {
    let (held_tx, held_rx) = mpsc::sync_channel(0);
    let (acked_tx, acked_rx) = mpsc::sync_channel(0);
    Gate {
      held_tx,
      held_rx: Mutex::new(held_rx),
      acked_tx,
      acked_rx: Mutex::new(acked_rx),
    }
  })
}

fn gate_updater(payload: &mut Vec<u8>, args: &[&[u8]], _out: &mut Vec<u8>, _rv: u8) -> bool {
  payload.extend_from_slice(args.first().copied().unwrap_or(&[]));
  let g = gate();
  g.held_tx.send(()).expect("持窗宣告送达");
  g.acked_rx.lock().recv().expect("对面命令已回执");
  true
}

type CustomUpdater = fn(&mut Vec<u8>, &[&[u8]], &mut Vec<u8>, RespVersion) -> bool;

fn stub_fns(updater: CustomUpdater) -> CustomObjectFns {
  CustomObjectFns {
    need_initial_update: |_, _, _| true,
    updater,
    reader: |_, _, _, _| true,
    not_found: |_, _, _| (),
    is_empty: |p| p.is_empty(),
  }
}

fn rmw_sync(
  batch: &BatchStoreSession<'_, SegmentedDevice>,
  fns: &CustomObjectFns,
  key: &[u8],
  arg: &[u8],
) -> CustomObjOutcome {
  let ctx = CustomObjCtx {
    tag: CustomObjectType::Json,
    fns,
    resp_version: 2,
  };
  let args = [arg];
  try_custom_object_rmw_sync(batch, &ctx, key, &args, &mut Vec::new())
}

fn plain_fns() -> CustomObjectFns {
  stub_fns(|payload, args, _out, _rv| {
    payload.extend_from_slice(args.first().copied().unwrap_or(&[]));
    true
  })
}

fn load_probe(batch: &BatchStoreSession<'_, SegmentedDevice>, key: &[u8]) -> ObjLoad<()> {
  obj_load_custom_sync(
    batch,
    key,
    CustomObjectType::Json.as_u8(),
    &mut Vec::new(),
    |_| Some(()),
  )
}

fn load_value(batch: &BatchStoreSession<'_, SegmentedDevice>, key: &[u8]) -> Option<Vec<u8>> {
  match obj_load_custom_sync(
    batch,
    key,
    CustomObjectType::Json.as_u8(),
    &mut Vec::new(),
    |p| Some(p.to_vec()),
  ) {
    ObjLoad::Present(p) => Some(p),
    other => panic!("预期信封存活，实得 {other:?}"),
  }
}

#[test]
fn uncontended_rmw_writes_back() {
  let (_dir, store) = open_store("custom-rmw-guard-ctl.db");
  let sess = store.new_session().unwrap();
  let batch = sess.enter_batch();
  let key = b"custom:guard:ctl";
  let outcome = rmw_sync(&batch, &plain_fns(), key, b"v1");
  assert_eq!(outcome, CustomObjOutcome::Done);
  assert_eq!(load_value(&batch, key).as_deref(), Some(b"v1".as_slice()));
}

#[test]
fn window_contention_degrades_without_evaluating() {
  let (_dir, store) = open_store("custom-rmw-guard-window.db");
  let sess = store.new_session().unwrap();
  let batch = sess.enter_batch();
  let key = b"custom:guard:window";
  assert_eq!(
    rmw_sync(&batch, &plain_fns(), key, b"v0"),
    CustomObjOutcome::Done
  );
  let rival = batch.try_rmw_window(key).expect("他者窗应可取");
  assert_eq!(
    rmw_sync(&batch, &plain_fns(), key, b"v1"),
    CustomObjOutcome::Degrade
  );
  assert_eq!(
    load_value(&batch, key).as_deref(),
    Some(b"v0".as_slice()),
    "不得闩即降级，求值与写回绝不执行"
  );
  drop(rival);
  assert_eq!(
    rmw_sync(&batch, &plain_fns(), key, b"v1"),
    CustomObjOutcome::Done
  );
  assert_eq!(load_value(&batch, key).as_deref(), Some(b"v0v1".as_slice()));
}

#[test]
fn concurrent_delete_recheck_discards_stale_write() {
  let (_dir, store) = open_store("custom-rmw-guard-del.db");
  let sess = store.new_session().unwrap();
  let batch = sess.enter_batch();
  let key: &[u8] = b"custom:guard:del";
  assert_eq!(
    rmw_sync(&batch, &plain_fns(), key, b"v0"),
    CustomObjOutcome::Done
  );

  let store2 = store.clone();
  let key2 = key.to_vec();
  let watcher = thread::spawn(move || {
    let g = gate();
    g.held_rx.lock().recv().expect("RMW 持窗装载宣告");
    let s = store2.new_session().expect("对面会话");
    let b = s.enter_batch();
    let deleted = b
      .try_delete_sync(&key2)
      .expect("DEL 存储面")
      .expect("DEL 判定面");
    assert!(deleted, "对面 DEL 应真实删除信封键");
    g.acked_tx.send(()).expect("DEL 回执送达");
  });
  let outcome = rmw_sync(&batch, &stub_fns(gate_updater), key, b"v1");
  watcher.join().expect("对面线程无 panic");
  assert_eq!(outcome, CustomObjOutcome::Degrade, "复验判异应弃写降级");
  assert!(
    matches!(load_probe(&batch, key), ObjLoad::Missing),
    "键复活或 String/信封双域并存（盲写已 ACK 删除的键）"
  );
}
