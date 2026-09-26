//! 自定义对象命令写回面保护与多键读应答契约回归（票 zcode-r22-wcustom）
//!
//! 发现一（P0，custom RMW 臂裸装载→求值→裸写回）：同步臂窗口互斥与并发 DEL
//! 交错复验弃写的确定性会合回归在 `custom_object_commands.rs::tests::rmw_guard`
//! （私有执行臂直调，注入式会合点）；本文件补冷键慢路径重放正向闭环（异步臂
//! 取窗 → 装载 → 求值 → 复验 → 写回）与双连接同键交错终态自洽。C# 对位
//! libs/server/Storage/Functions/ObjectStore/RMWMethods.cs 四钩子全在记录 X 锁
//! 内求值，「装载 → 写回」间隙不存在。
//!
//! 发现二（P1，custom 槽校验硬编码单键 spec）：JSON.MGET 的 MultiRead 键作用
//! 域接入槽校验——键区全量交切面逐键裁决（迁移窗口跨槽键不再本地直读），跨槽
//! 命令回 MOVED；单键命令维持 C# CustomCommandSingleKeySpec 形态。
//!
//! 发现三（P2，MGET 逐键错误帧入数组元素位）：坏路径 reader 错误帧改写 nil
//! 元素位，批量应答恒为合法值帧；单键 GET 顶层错误契约不变。

use std::sync::{
  Arc,
  atomic::{AtomicBool, Ordering},
};

use parking_lot::Mutex;
use wnode::{
  ClusterSessionFace,
  cluster_session::{ClusterSlotVerificationInput, SlotVerifyGate},
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSession},
};
use wnode_test::test_env;
use wresp::command::RespCommand;
use wtest_base::resp_frame;

/// 驱动一轮消费并回线面应答字节（慢路径经产线冲出口 `resolve_slow_wait_into`
/// 并入，与真实网络泵同一写出面；同 mget_slow_path_storage_error 装配形态）
async fn pump(session: &mut RespServerSession, input: &[u8]) -> Vec<u8> {
  session.recv_buffer.clear();
  session.recv_buffer.extend_from_slice(input);
  session.bytes_read = input.len();
  session.read_head = 0;
  session.end_read_head = 0;
  session.output.clear();
  assert_eq!(
    session.try_consume_messages(),
    Some(0),
    "输入命令应整段消费完毕"
  );
  let mut wire = Vec::new();
  if let Some(slow) = session.take_slow_wait() {
    let reply = slow.resolve().await;
    session.resolve_slow_wait_into(&reply, &mut wire);
  } else {
    session.take_output_into(&mut wire);
  }
  wire
}

/// 桩切面：按键规格真提取键区间并记录（提取复用 wresp 单点
/// `get_key_search_args_slice`），槽归属按开关注写 MOVED
struct RecordingCluster {
  redirect: AtomicBool,
  /// 每次有应答校验记录到的键区间 (first, last, step)
  seen: Mutex<Vec<(usize, usize, usize)>>,
}

impl RecordingCluster {
  fn new(redirect: bool) -> Self {
    Self {
      redirect: AtomicBool::new(redirect),
      seen: Mutex::new(Vec::new()),
    }
  }
}

impl ClusterSessionFace for RecordingCluster {
  fn set_read_only_session(&self) {}
  fn set_read_write_session(&self) {}
  fn local_current_epoch(&self) -> i64 {
    0
  }
  fn acquire_current_epoch(&self) {}
  fn release_current_epoch(&self) {}
  fn network_multi_key_slot_verify(
    &self,
    input: &ClusterSlotVerificationInput<'_>,
    args: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> SlotVerifyGate {
    for spec in input.key_specs {
      if let Some(range) = spec.get_key_search_args_slice(args, input.is_sub_command) {
        self.seen.lock().push(range);
      }
    }
    if self.redirect.load(Ordering::SeqCst) {
      output.extend_from_slice(b"-MOVED 0 127.0.0.1:7001\r\n");
      SlotVerifyGate::Redirected
    } else {
      SlotVerifyGate::Serve
    }
  }
  fn network_multi_key_slot_verify_no_response(
    &self,
    _input: &ClusterSlotVerificationInput<'_>,
    _args: &[&[u8]],
  ) -> bool {
    self.redirect.load(Ordering::SeqCst)
  }
  fn process_cluster_commands(
    &self,
    _cmd: RespCommand,
    _args: &[&[u8]],
    _output: &mut Vec<u8>,
    _slot: u16,
  ) -> bool {
    false
  }
  fn dispose(&self) {}
}

/// 发现二：多键读命令键区全量过槽门（修复前单键 spec 只提取首键
/// (0,0,1)，其余键脱离逐键裁决），跨槽命令回 MOVED 不再本地直读
#[compio::test]
async fn custom_multi_read_multi_key_spec_reaches_slot_gate() {
  let (_dir, session, mut s) = test_env(false);
  s.set_garnet_api(Arc::new(StoreGarnetApi::new(session)));
  let cluster = Arc::new(RecordingCluster::new(true));
  s.attach_cluster_session(cluster.clone());

  // JSON.MGET k1 k2 $.a：parse_state 不含命令名，args = [k1,k2,$.a]，
  // MultiRead{tail:1} spec 键区 = 首键至倒数第 2 参 → (0,1,1)
  let wire = pump(&mut s, &resp_frame(&[b"JSON.MGET", b"k1", b"k2", b"$.a"])).await;
  assert_eq!(wire, b"-MOVED 0 127.0.0.1:7001\r\n");
  assert_eq!(*cluster.seen.lock(), vec![(0, 1, 1)]);
}

/// 发现二对照：单键命令维持 C# CustomCommandSingleKeySpec（首键一处），
/// 校验放行后命令照常执行
#[compio::test]
async fn custom_single_key_spec_unchanged() {
  let (_dir, session, mut s) = test_env(false);
  s.set_garnet_api(Arc::new(StoreGarnetApi::new(session)));
  let cluster = Arc::new(RecordingCluster::new(false));
  s.attach_cluster_session(cluster.clone());

  // JSON.GET k1 $.a：单键 spec 提取 (0,0,1)，放行后缺键读应答 nil
  let wire = pump(&mut s, &resp_frame(&[b"JSON.GET", b"k1", b"$.a"])).await;
  assert_eq!(wire, b"$-1\r\n");
  assert_eq!(*cluster.seen.lock(), vec![(0, 0, 1)]);
}

/// 发现三：JSON.MGET 坏路径逐字节断言——命中键 reader 的路径错误帧禁入元素
/// 位（改写协议 nil），批量应答恒为「数组头 + 值帧」合法形态；单键 GET 坏路径
/// 顶层错误契约不受影响
#[compio::test]
async fn mget_bad_path_element_is_nil_never_error_frame() {
  let (_dir, session, mut s) = test_env(false);
  s.set_garnet_api(Arc::new(StoreGarnetApi::new(session)));

  assert_eq!(
    pump(
      &mut s,
      &resp_frame(&[b"JSON.SET", b"mk", b"$", b"{\"a\":1}"])
    )
    .await,
    b"+OK\r\n"
  );

  // $[x] 路径解析失败（Array index expected）：mk 命中 reader 出错帧 → 改
  // nil；nokey 走 not_found → nil。RESP2 逐字节断言元素位全为值帧（修复前
  // 此处为 `*2\r\n-Array index expected...\r\n-ERR ...` 畸形拼接）
  let wire = pump(
    &mut s,
    &resp_frame(&[b"JSON.MGET", b"mk", b"nokey", b"$[x]"]),
  )
  .await;
  assert_eq!(wire, b"*2\r\n$-1\r\n$-1\r\n");

  // 单键 JSON.GET 坏路径：错误帧即整条命令应答（顶层，不在任何元素位）
  let wire = pump(&mut s, &resp_frame(&[b"JSON.GET", b"mk", b"$[x]"])).await;
  assert!(wire.starts_with(b"-"), "单键顶层错误契约被误改：{wire:?}");
}

/// 发现一（异步臂正向）：磁盘候选上的 JSON.SET 同步臂降级 → 慢路径重放
/// （异步 RMW 臂取窗装载求值复验写回），复验正向放行，写回与热路径同构
#[compio::test]
async fn cold_key_json_set_slow_replay_writes_back() {
  let (_dir, session, mut s) = test_env(false);
  let store = session.store.clone();
  s.set_garnet_api(Arc::new(StoreGarnetApi::new(session)));

  // 热写并落盘淘汰：键成磁盘候选
  let wire = pump(&mut s, &resp_frame(&[b"JSON.SET", b"ck", b"$", b"1"])).await;
  assert_eq!(wire, b"+OK\r\n");
  store.flush_and_evict_all().await.unwrap();

  // 磁盘候选上的 JSON.SET：整体降级 → 慢路径重放闭环（复验不得误弃写）
  let wire = pump(&mut s, &resp_frame(&[b"JSON.SET", b"ck", b"$", b"2"])).await;
  assert_eq!(wire, b"+OK\r\n");

  // 终值对照：与同值热键的读响应逐字节一致（冷键重放写回与热路径同构）
  let wire = pump(&mut s, &resp_frame(&[b"JSON.SET", b"ref", b"$", b"2"])).await;
  assert_eq!(wire, b"+OK\r\n");
  let got = pump(&mut s, &resp_frame(&[b"JSON.GET", b"ck", b"$"])).await;
  let want = pump(&mut s, &resp_frame(&[b"JSON.GET", b"ref", b"$"])).await;
  assert_eq!(got, want, "冷键重放写回与热路径写回应答分叉");
}

/// 票 4.1：双连接 JSON.SET / R.SETBIT 同键交错——类型面自洽（跨类型同键
/// WRONGTYPE、同类型后写生效无丢更新、无双域并存），配合 src rmw_guard 的
/// 窗口互斥与复验弃写负向回归
#[compio::test]
async fn two_connection_interleaved_same_key_terminal_consistency() {
  let (_dir, session, mut s1) = test_env(false);
  let store = session.store.clone();
  s1.set_garnet_api(Arc::new(StoreGarnetApi::new(session)));
  let session2 = store.new_session().unwrap();
  let mut s2 = RespServerSession::default();
  s2.set_garnet_api(Arc::new(StoreGarnetApi::new(session2)));

  // 交错一：JSON 先定域，R.SETBIT 同键 → WRONGTYPE（跨类型不静默穿透）
  let wire = pump(&mut s1, &resp_frame(&[b"JSON.SET", b"ik", b"$", b"1"])).await;
  assert_eq!(wire, b"+OK\r\n");
  let wire = pump(&mut s2, &resp_frame(&[b"R.SETBIT", b"ik", b"0", b"1"])).await;
  assert!(
    wire.starts_with(b"-WRONGTYPE"),
    "跨类型同键应 WRONGTYPE：{wire:?}"
  );

  // 交错二：bitmap 先定域，JSON.SET 同键 → WRONGTYPE
  let wire = pump(&mut s2, &resp_frame(&[b"R.SETBIT", b"bk", b"3", b"1"])).await;
  assert_eq!(wire, b":0\r\n");
  let wire = pump(&mut s1, &resp_frame(&[b"JSON.SET", b"bk", b"$", b"1"])).await;
  assert!(
    wire.starts_with(b"-WRONGTYPE"),
    "跨类型同键应 WRONGTYPE：{wire:?}"
  );

  // 同键两连接交替写（JSON）：后写生效，无丢更新
  for v in ["2", "3", "4"] {
    let conn = if v == "3" { &mut s2 } else { &mut s1 };
    let wire = pump(conn, &resp_frame(&[b"JSON.SET", b"ik", b"$", v.as_bytes()])).await;
    assert_eq!(wire, b"+OK\r\n");
  }
  // 双连接读响应一致且等于最后成功写（对照键同值热写）
  let got = pump(&mut s2, &resp_frame(&[b"JSON.GET", b"ik", b"$"])).await;
  let want = pump(&mut s1, &resp_frame(&[b"JSON.GET", b"ik", b"$"])).await;
  assert_eq!(got, want, "双连接读响应分叉");
  assert_eq!(
    pump(&mut s1, &resp_frame(&[b"JSON.SET", b"ref", b"$", b"4"])).await,
    b"+OK\r\n"
  );
  let want = pump(&mut s1, &resp_frame(&[b"JSON.GET", b"ref", b"$"])).await;
  assert_eq!(got, want, "交错后终值应与最后成功写一致");

  // bitmap 域未被 JSON 写触碰（无双域并存 / 键复活）
  let wire = pump(&mut s2, &resp_frame(&[b"R.GETBIT", b"bk", b"3"])).await;
  assert_eq!(wire, b":1\r\n");
}
