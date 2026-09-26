//! MSET 快臂「窗内预检、窗内裁决」零落库回归（票 zcode-r141c-msetbig 案二）
//!
//! 修复形态：`network_mset` 快臂先 `try_rmw_window_sorted` 取全键读改写窗口、
//! 再窗内逐键 `ri_write_gate` 裁决——Blocked 出 WRONGTYPE 时保证零键落笔、
//! WATCH 版本零推进；Deferred 弃窗沿既有出口降级慢臂（慢臂窗内异步对偶门
//! 复裁决，本文件不重复覆盖）。修复前窗外预检与他核取窗交叠可致批内核 Meta
//! 降级臂已写前序键、慢臂窗内门改答 WRONGTYPE 的半提交假拒（单线程静态态
//! 不复现该交错，本文件按修后形态钉死「拒绝=零落库」契约面）。
//!
//! 对标 C#：`ArrayCommands.cs:NetworkMSET` 逐键 SET 无整命令拒绝形态，rust
//! 将 RI 键拒收口为写门整命令裁决（`set.rs:ri_write_gate` 成文偏离，deviations
//! 既定口径不重报），该裁决成立的前提即「拒绝恒零落库」；`MainStoreOps.cs:
//! MSET_Conditional` 全键排他锁内判定与写一体的折叠先例对位本窗。
//!
//! WATCH 栅栏观测：引擎级写面钩子（`version_map_watch_hook`）挂 `wtxn::
//! WatchVersionMap`，与 EXEC 校验同一哈希面（先例 range_index_watch_fence.rs），
//! 并以成功写 +OK 的恰一次推进证明钩子在场、使「零推进」断言非虚断。

use std::sync::Arc;

use compio::runtime::Runtime;
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
  storage::session::storage_session::version_map_watch_hook,
};
use wnode_test::err_frame;
use wresp::cmd_strings::RESP_ERR_WRONG_TYPE;
use wtest_base::{resp_frame_str, test_store_config};
use wtxn::{TxnKeyEntryComparison, WatchVersionMap};
use wval::SessionPrefixBuf;

/// 装配带 RangeIndex 目录与 WATCH 版本表的会话消费者（每测试独立临时目录）
fn env(tag: &str) -> (tempfile::TempDir, RespSessionConsumer, Arc<WatchVersionMap>) {
  let dir = tempfile::tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag)).unwrap());
  let mut config = test_store_config();
  config.range_index_dir = Some(dir.path().join("ri"));
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  let map = Arc::new(WatchVersionMap::new(1 << 10));
  assert!(
    store.set_watch_hook(version_map_watch_hook(Arc::clone(&map))),
    "引擎级写面钩子应首次挂载"
  );
  let session = store.new_session().unwrap();
  let consumer = RespSessionConsumer::new(
    1,
    RespServerSessionOptions::default(),
    Arc::new(StoreGarnetApi::new(session)),
  );
  (dir, consumer, map)
}

/// 单命令往返：同步段消费 + 慢路径闭环（RI 族与降级命令在异步段应答）
fn cmd(rt: &Runtime, c: &mut RespSessionConsumer, args: &[&str]) -> Vec<u8> {
  let mut scratch = c.take_recv_scratch();
  scratch.extend_from_slice(&resp_frame_str(args));
  c.return_recv_scratch(scratch);
  let mut out = Vec::new();
  let remaining = c.try_consume_messages_into(&mut out);
  assert_eq!(remaining, Some(0), "帧应被完整消费: {args:?}");
  if let Some(slow) = c.take_slow_wait() {
    rt.block_on(async {
      out.extend_from_slice(&slow.resolve().await);
    });
  }
  out
}

/// 版本表读点（与 wtxn EXEC 校验同一哈希面：根域 scoped，写会话默认 (0,0)）
fn ver(map: &WatchVersionMap, key: &[u8]) -> u64 {
  map.read_version(
    TxnKeyEntryComparison::scoped_key_hash(SessionPrefixBuf::ROOT.as_slice(), key) as u64,
  )
}

/// 案二修后形态主锁：混列存活 RI 键的 MSET 整命令拒 WRONGTYPE——单帧应答、
/// 前序普通键旧值逐字节保留、WATCH 版本零推进；反向对照（RI 键排字节先）
/// 同口径；成功写臂恰一次推进证明观测面非虚断
#[test]
fn mset_window_gate_blocked_writes_zero_keys_and_advances_no_watch() {
  let rt = Runtime::new().unwrap();
  let (_dir, mut c, map) = env("mset_window_gate.db");

  // 建存活 RangeIndex 键 idx（KeyTag::Meta 存活元记录在场）与普通键 a=old
  assert_eq!(
    cmd(
      &rt,
      &mut c,
      &["RI.CREATE", "idx", "MEMORY", "MINRECORD", "8"]
    ),
    b"+OK\r\n"
  );
  assert_eq!(
    cmd(&rt, &mut c, &["RI.SET", "idx", "field1", "value1"]),
    b"+OK\r\n"
  );
  assert_eq!(cmd(&rt, &mut c, &["SET", "a", "old"]), b"+OK\r\n");
  let a_v0 = ver(&map, b"a");

  // 主案形：普通键 a 排命令序先（批内核字节键序首笔即 a）——窗内门于任何
  // 落笔前裁决，应答恰一枚 WRONGTYPE 帧（无 +OK 尾随、无第二帧）
  let expected_err = err_frame(RESP_ERR_WRONG_TYPE);
  assert_eq!(
    cmd(&rt, &mut c, &["MSET", "a", "new", "idx", "v"]),
    expected_err,
    "MSET 混列 RI 键须单帧 WRONGTYPE 拒绝"
  );
  // 拒绝=零落库契约：前序键旧值逐字节保留、WATCH 版本未推进
  assert_eq!(cmd(&rt, &mut c, &["GET", "a"]), b"$3\r\nold\r\n");
  assert_eq!(ver(&map, b"a"), a_v0, "被拒 MSET 不得推进任何键版本");

  // 反向对照钉：RI 键排命令序先（修复前后皆净的静态序），同口径单帧零落库
  assert_eq!(
    cmd(&rt, &mut c, &["MSET", "idx", "v", "a", "new2"]),
    expected_err
  );
  assert_eq!(cmd(&rt, &mut c, &["GET", "a"]), b"$3\r\nold\r\n");
  assert_eq!(ver(&map, b"a"), a_v0);

  // RI 键身份完好未受染
  assert_eq!(cmd(&rt, &mut c, &["TYPE", "idx"]), b"+rangeindex\r\n");
  assert_eq!(
    cmd(&rt, &mut c, &["RI.GET", "idx", "field1"]),
    b"$6\r\nvalue1\r\n"
  );

  // 成功写对照：无 RI 键的 MSET 照常 +OK 且恰一次推进版本——证明钩子在场，
  // 上方「零推进」断言非虚断
  assert_eq!(
    cmd(&rt, &mut c, &["MSET", "a", "new2", "b1", "v1"]),
    b"+OK\r\n"
  );
  assert_eq!(cmd(&rt, &mut c, &["GET", "a"]), b"$4\r\nnew2\r\n");
  assert_eq!(ver(&map, b"a"), a_v0 + 1, "实写 a 应恰一次推进版本");
}
