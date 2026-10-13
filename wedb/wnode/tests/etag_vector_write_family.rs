#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! ETag 写族撞存活向量登记键的强制覆写收口
//! （票 wnode-etag-write-family-vector-gate-blocks-promote-overwrite，P2）
//!
//! 缺陷形态：SETWITHETAG/SETIFMATCH/SETIFGREATER/DELIFGREATER 既不在
//! set_vector_guard 覆写族也不在 is_vector_gate_exempt 豁免清单，被
//! vector_registry_gate 通用值域门拦成 -WRONGTYPE 错误帧、向量集保留——
//! 与 basic_etag_commands.rs 自身的对象键 WrongType promote 臂（C#
//! ExecuteETagSetCommand 的 DELETE+SET_Conditional 强制覆写终态，
//! BasicEtagCommands.cs:296-307）直接矛盾，终态契约分叉。
//!
//! 修复形态（本组锁钉）：豁免清单登记四命令 + 写族三命令快臂窗内第四态
//! 探针（vector_live_in_window 单源）命中诚实降级 + 慢臂持窗内折叠
//! （registry_alive 单源）摘登记清退（clear_vector_registry 全仓单源）后
//! 按初写口径覆写——对位 C# RMW 撞向量记录回 WRONGTYPE
//! （RMWMethods.cs RecordType 判定）后 promote DELETE+SET 强制覆写；
//! DELIFGREATER 无覆写臂，三域 Missing 折 :0 天然对齐 C# status != OK
//! （:100-110）。读族 GETWITHETAG/GETIFNOTMATCH 留在门内对齐 C# 读侧
//! CheckRecordTypeMismatch。
//!
//! 测试全真存储真协议帧，无 mock。降级宽窗用占窗会话夹逼 try_rmw_window
//! 失闩构造（先例 set_family_vector_window_guard.rs），宽窗内注入 VADD 的
//! 确定性序无 sleep（先例 msetnx_vector_registry_fold.rs）。
//! 对标 C# 测试面：Garnet.test.vectorset/VectorSetWrongTypeTests.cs（值域
//! 门矩阵）与 Garnet.test/RespEtagTests.cs（etag 族契约）。

use std::sync::Arc;

use compio::runtime::Runtime;
use itoa::Buffer;
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  MessageConsumerFace, RespSessionConsumer, resp::vector::vector_manager::VectorManager,
};
use wnode_test::{pump_frame, roundtrip, vector_env as env};
use wval::{KeyTag, SessionPrefixBuf};

const REPLY_WRONGTYPE: &[u8] =
  b"-WRONGTYPE Operation against a key holding the wrong kind of value.\r\n";

type TestStore = WedbStore<SegmentedDevice>;

/// RESP2 请求帧编码（任意字节参数）
fn frame(args: &[&[u8]]) -> Vec<u8> {
  let mut ibuf = Buffer::new();
  let mut out = Vec::new();
  out.push(b'*');
  out.extend_from_slice(ibuf.format(args.len()).as_bytes());
  out.extend_from_slice(b"\r\n");
  for arg in args {
    out.push(b'$');
    out.extend_from_slice(ibuf.format(arg.len()).as_bytes());
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(arg);
    out.extend_from_slice(b"\r\n");
  }
  out
}

/// 建存活向量登记键（VADD 三参形态，先例 set_family_vector_window_guard.rs）
fn vadd(rt: &Runtime, c: &mut RespSessionConsumer, key: &[u8], member: &[u8]) {
  let vbytes: Vec<u8> = [1.0f32, 2.0, 3.0]
    .iter()
    .flat_map(|v| v.to_le_bytes())
    .collect();
  let out = roundtrip(rt, c, &[b"VADD", key, b"FP32", &vbytes, member]);
  assert_eq!(out, b":1\r\n", "VADD 建登记应回 :1: {out:?}");
}

/// 登记表存活判据（第四态源单点读取口）
fn registry_present(vm: &VectorManager, key: &[u8]) -> bool {
  vm.read_stored_index(SessionPrefixBuf::ROOT.as_slice(), key)
    .is_some()
}

/// 物理域记录在场取证（String 域零写 / 覆写落笔判据）
fn string_record_present(rt: &Runtime, store: &Arc<TestStore>, key: &[u8]) -> bool {
  let sess = store.new_session().expect("取证会话");
  let rec_k = sess.session_tag_key(KeyTag::String, key);
  rt.block_on(sess.read_raw(&rec_k))
    .expect("物理记录读取不得报存储错误")
    .is_some()
}

/// `[etag, nil]` 二元数组帧（RESP2）
fn etag_nil_array(etag: i64) -> Vec<u8> {
  let mut ibuf = Buffer::new();
  let mut out = Vec::new();
  out.extend_from_slice(b"*2\r\n:");
  out.extend_from_slice(ibuf.format(etag).as_bytes());
  out.extend_from_slice(b"\r\n$-1\r\n");
  out
}

/// 主锁：存活向量登记键上 etag 写族全形态对位 C# promote 强制覆写终态——
/// SETWITHETAG / SETIFMATCH / SETIFGREATER 覆写成功且登记窗内清退（无幽灵
/// 双域键，值可 GET 读出）、DELIFGREATER 折 :0 且登记保留、读族
/// GETWITHETAG / GETIFNOTMATCH 仍 -WRONGTYPE（C# 读侧照拦的反钉）。
/// 修复前四命令全被派发值域门拦成 -WRONGTYPE 错误帧、向量集保留（红灯取证）
#[test]
fn etag_write_family_fourth_state_promote_terminal() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (_api, mut c, _inj, vm, store) = env("etag_vec_terminal.db");

    // SETWITHETAG：覆写终态 = 初写口径新 etag 1（C# promote DELETE 后
    // HandleSetWithEtagInitialUpdate 的 NoETag + 1），登记清退、值可读
    vadd(&rt, &mut c, b"swe", b"m1");
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"SETWITHETAG", b"swe", b"v1"]),
      b":1\r\n",
      "向量键 SETWITHETAG 须 promote 覆写回新 etag 整数"
    );
    assert!(!registry_present(&vm, b"swe"), "覆写后登记须窗内清退");
    assert!(
      string_record_present(&rt, &store, b"swe"),
      "覆写须落 String 域"
    );
    assert_eq!(roundtrip(&rt, &mut c, &[b"GET", b"swe"]), b"$2\r\nv1\r\n");

    // SETIFMATCH：无条件初写（条件判定不在初写路径），新 etag = given + 1
    vadd(&rt, &mut c, b"sim", b"m1");
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"SETIFMATCH", b"sim", b"v2", b"5"]),
      etag_nil_array(6).as_slice(),
      "向量键 SETIFMATCH 须 promote 覆写回 [given+1, nil]"
    );
    assert!(!registry_present(&vm, b"sim"));
    assert_eq!(roundtrip(&rt, &mut c, &[b"GET", b"sim"]), b"$2\r\nv2\r\n");

    // SETIFGREATER：无条件初写，新 etag = given
    vadd(&rt, &mut c, b"sig", b"m1");
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"SETIFGREATER", b"sig", b"v3", b"7"]),
      etag_nil_array(7).as_slice(),
      "向量键 SETIFGREATER 须 promote 覆写回 [given, nil]"
    );
    assert!(!registry_present(&vm, b"sig"));
    assert_eq!(roundtrip(&rt, &mut c, &[b"GET", b"sig"]), b"$2\r\nv3\r\n");

    // DELIFGREATER：无覆写臂，折 :0（C# status != OK，:100-110）且键保留
    vadd(&rt, &mut c, b"dig", b"m1");
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"DELIFGREATER", b"dig", b"99"]),
      b":0\r\n",
      "向量键 DELIFGREATER 须折 :0"
    );
    assert!(registry_present(&vm, b"dig"), "DELIFGREATER 不得动登记");
    assert!(
      !string_record_present(&rt, &store, b"dig"),
      "DELIFGREATER 须零落笔"
    );

    // 读族反钉：C# CheckRecordTypeMismatch 照拦，派发门 -WRONGTYPE 保留
    vadd(&rt, &mut c, b"gwe", b"m1");
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"GETWITHETAG", b"gwe"]),
      REPLY_WRONGTYPE
    );
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"GETIFNOTMATCH", b"gwe", b"0"]),
      REPLY_WRONGTYPE
    );
    assert!(registry_present(&vm, b"gwe"), "读族拒绝须零副作用");
  })
}

/// 宽窗 TOCTOU 锁（执行方案第三项）：victim `SETWITHETAG` 快臂取窗遭占窗
/// 会话失闩整体降级挂 SlowWait，降级快照与慢臂持窗之间，他会话 VADD 提交
/// 登记（VADD 零触键闩，可达序如实构造）；慢臂窗内折叠须判第四态摘登记
/// 清退后覆写——终态无双域并存。修复前慢臂三域折叠判 Missing 直接初写，
/// String 值落库而登记残留，值被值域门永久遮蔽（幽灵双域键红灯取证）
#[test]
fn setwithetag_wide_window_interleave_vadd_no_dual_domain() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (_api, mut c, mut inj, vm, store) = env("etag_vec_widewin.db");

    let holder = store.new_session().expect("占窗会话");
    let batch_h = holder.enter_batch();
    let window = batch_h.try_rmw_window(b"wk").expect("占窗会话取本键排他闩");

    let out = pump_frame(&mut c, &frame(&[b"SETWITHETAG", b"wk", b"v1"]));
    assert!(out.is_empty(), "失闩应整体降级零应答: {out:?}");
    let slow = c.take_slow_wait().expect("失闩降级应挂起慢路径");

    // 宽窗内注入：他会话 VADD 提交登记（派发值域门早已放行）
    vadd(&rt, &mut inj, b"wk", b"m1");
    assert!(registry_present(&vm, b"wk"));

    drop(window);
    let verdict = rt.block_on(slow.resolve());
    assert_eq!(verdict, b":1\r\n", "慢臂窗内折叠须清退后覆写回新 etag 1");
    assert!(
      !registry_present(&vm, b"wk"),
      "覆写落毕登记须摘除，杜绝双域并存"
    );
    assert!(string_record_present(&rt, &store, b"wk"));
    assert_eq!(roundtrip(&rt, &mut c, &[b"GET", b"wk"]), b"$2\r\nv1\r\n");
  })
}

/// 宽窗 TOCTOU 对偶（条件写臂）：`SETIFMATCH` 失闩降级宽窗内并发 VADD 提交
/// 登记，慢臂窗内折叠摘登记清退后按初写口径覆写（条件判定不在初写路径），
/// 应答 [given+1, nil] 而非条件失配帧；DELIFGREATER 同宽窗对拍折 :0 保留登记
#[test]
fn setifmatch_and_delifgreater_wide_window_fold() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (_api, mut c, mut inj, vm, store) = env("etag_vec_widewin2.db");

    // SETIFMATCH 宽窗：promote 初写终态
    let holder = store.new_session().expect("占窗会话");
    let batch_h = holder.enter_batch();
    let window = batch_h.try_rmw_window(b"wm").expect("占窗会话取本键排他闩");
    let out = pump_frame(&mut c, &frame(&[b"SETIFMATCH", b"wm", b"v9", b"3"]));
    assert!(out.is_empty(), "失闩应整体降级零应答: {out:?}");
    let slow = c.take_slow_wait().expect("失闩降级应挂起慢路径");
    vadd(&rt, &mut inj, b"wm", b"m1");
    drop(window);
    let verdict = rt.block_on(slow.resolve());
    assert_eq!(
      verdict,
      etag_nil_array(4).as_slice(),
      "慢臂窗内折叠须清退后初写回 [given+1, nil]"
    );
    assert!(!registry_present(&vm, b"wm"), "覆写落毕登记须摘除");
    assert_eq!(roundtrip(&rt, &mut c, &[b"GET", b"wm"]), b"$2\r\nv9\r\n");

    // DELIFGREATER 宽窗：折 :0 保留登记（无覆写臂，键存活零副作用）
    let holder2 = store.new_session().expect("占窗会话");
    let batch_h2 = holder2.enter_batch();
    let window2 = batch_h2
      .try_rmw_window(b"wd")
      .expect("占窗会话取本键排他闩");
    let out = pump_frame(&mut c, &frame(&[b"DELIFGREATER", b"wd", b"9"]));
    assert!(out.is_empty(), "失闩应整体降级零应答: {out:?}");
    let slow2 = c.take_slow_wait().expect("失闩降级应挂起慢路径");
    vadd(&rt, &mut inj, b"wd", b"m1");
    drop(window2);
    let verdict2 = rt.block_on(slow2.resolve());
    assert_eq!(verdict2, b":0\r\n", "慢臂向量键 DELIFGREATER 须折 :0");
    assert!(registry_present(&vm, b"wd"), "DELIFGREATER 不得动登记");
    assert!(
      !string_record_present(&rt, &store, b"wd"),
      "DELIFGREATER 须零落笔"
    );
  })
}

/// 反向对拍钉：普通字符串键上 ETag 写族契约逐字节零漂移（第四态折叠不误伤
/// 三域命中态与缺席态——SETWITHETAG etag 递进、SETIFMATCH 条件命中与失配、
/// DELIFGREATER 条件删除，与修前同形）
#[test]
fn etag_write_family_zero_drift_for_plain_keys() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (_api, mut c, _inj, _vm, _store) = env("etag_vec_drift.db");

    // 缺失键初写：SETWITHETAG → :1；SETIFMATCH → [given+1, nil]
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"SETWITHETAG", b"pk", b"a"]),
      b":1\r\n"
    );
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"SETIFMATCH", b"pk", b"b", b"1"]),
      etag_nil_array(2).as_slice()
    );

    // 存活键条件失配：SETIFMATCH etag=0 对现存 etag=2 失配回 [2, 旧值]
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"SETIFMATCH", b"pk", b"c", b"0"]),
      b"*2\r\n:2\r\n$1\r\nb\r\n"
    );

    // DELIFGREATER 条件命中删除：etag 2 < 5 → :1，键删净
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"DELIFGREATER", b"pk", b"5"]),
      b":1\r\n"
    );
    assert_eq!(roundtrip(&rt, &mut c, &[b"GET", b"pk"]), b"$-1\r\n");
  })
}
