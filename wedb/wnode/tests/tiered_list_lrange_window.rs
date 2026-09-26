//! 分层列表 LRANGE 有界序号窗口回归（task/done/fix-tiered-lrange-bounded-scan.md）
//!
//! 树内臂原为无条件全树扫（`scan_with_count_callback(&[0u8], usize::MAX)` 汇成
//! `Vec<Vec<u8>>` 再裁剪），现按 `meta.size` 先折算起止、再以 `(头 + start)` 键起点
//! 与 `count = stop - start + 1` 上界做有界区间扫，帧头先落、成员逐条直出。
//!
//! 参照模型即 C# `ListObjectImpl.cs:ListRange`（libs/server/Objects/List/
//! ListObjectImpl.cs:129-168）：`Count` 折负数、下限钳 0、上限钳 `Count-1`、
//! `start > stop` 回空数组，否则 `WriteArrayLength(count)` 后逐条 `WriteBulkString`。
//! 本用例把该算法在测试侧独立复写一份（不经被测码），对分层态与内存态同一成员集
//! 逐窗口断言逐字节全等，即 collection.md 第 5 节「分层态与内存态回包逐字节全等」。

use std::sync::Arc;

use compio::runtime::Runtime;
use tempfile::tempdir;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::resp::{
  garnet_api::{GarnetApi, StoreGarnetApi},
  resp_server_session::{RespServerSession, RespServerSessionOptions},
};
use wnode_test::auto_exec;
use wresp::command::RespCommand;

type TestStore = WedbStore<SegmentedDevice>;

fn open_env(tag: &str) -> (Runtime, GarnetApi, Arc<TestStore>, tempfile::TempDir) {
  let dir = tempdir().unwrap();
  let config = StoreConfig::new(2048, 1024 * 1024, 16, 0.5).unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag)).unwrap());
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  (
    Runtime::new().unwrap(),
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
    store,
    dir,
  )
}

fn session_with(api: &GarnetApi) -> RespServerSession {
  let mut s = RespServerSession::new(1, RespServerSessionOptions::default());
  s.set_garnet_api(api.clone());
  s
}

/// RPUSH 成批推入（1000 条一批，兼作跨升阶门槛的驱动面）
fn push_all(
  api: &GarnetApi,
  rt: &Runtime,
  s: &mut RespServerSession,
  key: &[u8],
  values: &[Vec<u8>],
) {
  for chunk in values.chunks(1000) {
    let mut args: Vec<Vec<u8>> = vec![key.to_vec()];
    args.extend(chunk.iter().cloned());
    let slices: Vec<&[u8]> = args.iter().map(|v| v.as_slice()).collect();
    auto_exec(api, rt, s, RespCommand::Rpush, &slices);
  }
}

/// C# ListRange 的逐行对位参照模型（独立于被测实现）
fn expect_range(model: &[Vec<u8>], start: i32, stop: i32) -> Vec<u8> {
  let count = model.len() as i32;
  let mut out = Vec::new();
  if count == 0 {
    out.extend_from_slice(b"*0\r\n");
    return out;
  }
  let mut start = if start < 0 { count + start } else { start };
  if start < 0 {
    start = 0;
  }
  let mut stop = if stop < 0 { count + stop } else { stop };
  if stop >= count {
    stop = count - 1;
  }
  if start > stop {
    out.extend_from_slice(b"*0\r\n");
    return out;
  }
  let n = (stop - start + 1) as usize;
  out.extend_from_slice(format!("*{n}\r\n").as_bytes());
  for item in &model[start as usize..=stop as usize] {
    out.extend_from_slice(format!("${}\r\n", item.len()).as_bytes());
    out.extend_from_slice(item);
    out.extend_from_slice(b"\r\n");
  }
  out
}

/// 十进制参数字节（LRANGE 的 start / stop 实参）
fn arg(v: i32) -> Vec<u8> {
  v.to_string().into_bytes()
}

/// 窗口矩阵：同一成员集在内存态与分层态逐窗口应答逐字节全等，且两侧均等于
/// C# 参照模型（负数起止、越界钳制、start > stop、单元素窗、全量窗全覆盖）
#[test]
fn test_tiered_list_lrange_bounded_window() {
  let (rt, api, store, _dir) = open_env("tiered-lrange-window.db");
  let mut s = session_with(&api);

  // 内存态对照键（同一成员集的前 12 条，未跨升阶门槛）
  let small: Vec<Vec<u8>> = (1..=12).map(|i| format!("v{i}").into_bytes()).collect();
  push_all(&api, &rt, &mut s, b"m", &small);
  let sess = store.new_session().unwrap();
  assert!(
    rt.block_on(sess.load_collection_stub(b"m"))
      .unwrap()
      .is_none(),
    "对照键须保持内存态"
  );

  // 分层态键：推过升阶门槛（条目数维 65536）
  let total = wcol::TIERED_PROMOTE_THRESHOLD + 10;
  let model: Vec<Vec<u8>> = (1..=total).map(|i| format!("v{i}").into_bytes()).collect();
  push_all(&api, &rt, &mut s, b"l", &model);
  let sess = store.new_session().unwrap();
  assert!(
    rt.block_on(sess.load_collection_stub(b"l"))
      .unwrap()
      .is_some(),
    "目标键应已升阶为分层态"
  );
  let size = total as i32;

  // 分层态窗口矩阵：折算法与钳制口径若与本模块实现分叉，这里逐条露相
  let windows: [(i32, i32); 13] = [
    (0, 0),
    (0, 10),
    (5, 4),
    (0, -1),
    (-3, -1),
    (-1, -1),
    (-2, -5),
    (size - 1, i32::MAX),
    (size, size + 5),
    (i32::MIN + 1, 1),
    (1000, 1010),
    (-size, -size),
    (size - 12, -size + 3),
  ];
  for (start, stop) in windows {
    let tiered = auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Lrange,
      &[b"l", &arg(start), &arg(stop)],
    );
    assert_eq!(
      tiered,
      expect_range(&model, start, stop),
      "分层态 LRANGE l {start} {stop} 与 C# 参照模型分叉"
    );
  }

  // 单元素窗应答字节有界（旧形虽应答等价、但堆峰值随全树基数增长）
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Lrange, &[b"l", b"0", b"0"]),
    b"*1\r\n$2\r\nv1\r\n"
  );
  // 尾端窗（起点键 = 头 + size - 3，最右页一次定位）
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Lrange,
      &[b"l", b"-3", b"-1"]
    ),
    b"*3\r\n$6\r\nv65544\r\n$6\r\nv65545\r\n$6\r\nv65546\r\n"
  );

  // 两态逐字节全等：对照键成员集为分层态集的前缀，故完全落在前缀内的非负窗
  // 两态应答必等（collection.md 第 5 节透明口径），且仍等于 C# 参照模型
  for (start, stop) in [(0, 0), (0, 11), (2, 5), (11, 11), (5, 4), (0, 5)] {
    let mem = auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Lrange,
      &[b"m", &arg(start), &arg(stop)],
    );
    let tiered = auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Lrange,
      &[b"l", &arg(start), &arg(stop)],
    );
    let expect = expect_range(&small, start, stop);
    assert_eq!(
      mem, expect,
      "内存态 LRANGE m {start} {stop} 与 C# 参照模型分叉"
    );
    assert_eq!(
      tiered, expect,
      "分层态 LRANGE l {start} {stop} 与内存态分叉"
    );
    assert_eq!(mem, tiered, "两态前缀窗 {start} {stop} 字节不等");
  }

  // 两端推入后窗口随头/尾移动：头端序号须重定位，尾端由 size 派生
  auto_exec(&api, &rt, &mut s, RespCommand::Lpush, &[b"l", b"h1", b"h2"]);
  let mut grown = vec![b"h2".to_vec(), b"h1".to_vec()];
  grown.extend(model.iter().cloned());
  auto_exec(&api, &rt, &mut s, RespCommand::Rpush, &[b"l", b"t1"]);
  grown.push(b"t1".to_vec());
  let grown_size = grown.len() as i32;
  for (start, stop) in [
    (0, 2),
    (-2, -1),
    (0, grown_size - 1),
    (2, 3),
    (grown_size - 1, grown_size - 1),
    (-grown_size, -grown_size),
  ] {
    let out = auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Lrange,
      &[b"l", &arg(start), &arg(stop)],
    );
    assert_eq!(
      out,
      expect_range(&grown, start, stop),
      "两端推入后 LRANGE l {start} {stop} 窗口错位"
    );
  }
}
