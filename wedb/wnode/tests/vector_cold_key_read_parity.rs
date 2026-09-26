//! 向量冷态键只读命令 NOTFOUND 族应答对账与 FILTER 编译失败文案一致性
//! （工单 zcode-r16-vector 发现一 / 发现二）
//!
//! 发现一：C# 全部向量命令经 Read_MainStore 真读落盘裁决
//! （VectorManager.Locking.cs:ReadVectorIndexCore），冷态墓碑键回 NOTFOUND
//! 族应答，仅存活非向量记录才 WRONGTYPE（RespServerSessionVectors.cs 各
//! NetworkV* 的 res 三态分派）。rust 快路径同步探针读不准磁盘候选（DEL 后
//! 检查点淘汰、索引槽指盘），修复前一律折 WRONGTYPE 错误帧；修复后只读命令
//! 降级慢路径异步真读裁决（network_vector_read_slow）。写命令保守拒取舍见
//! doc/zh/deviations.md §22。
//!
//! 发现二：VSIM ELE 与 FP32 对同一非法 FILTER 表达式回同一条
//! "ERR Compiling filter failed"（C# ElementSimilarity 未设 errorMsg 回落
//! 量化 mismatch 误导文案，rust 双臂统一收敛，doc/zh/deviations.md §23）。

use std::{mem::forget, sync::Arc, thread::sleep, time::Duration};

use compio::runtime::Runtime;
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  MessageConsumerFace,
  resp::{
    RespSessionConsumer,
    garnet_api::StoreGarnetApi,
    resp_server_session::RespServerSessionOptions,
    vector::{
      vector_manager::{VectorManager, VectorManagerOptions},
      vector_store_callbacks::{OwnedActiveVectorSession, WedbVectorStoreCallbacks},
    },
  },
};
use wtest_base::{resp_frame as encode_frame, test_store_config};
use wval::SessionPrefixBuf;

type ReadTestCase<'a> = (&'a [u8], Vec<&'a [u8]>, &'a [u8]);
use wnode_test::pump;
use wvector::Callbacks;

/// 守卫 WRONGTYPE 帧（C# AbortVectorSetWrongType 无句点文案，RESP2 错误帧）
const WRONGTYPE_FRAME: &[u8] =
  b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";

/// 装配会话消费者（真存储 + 向量集合管理器；返回 store 句柄供冷化驱逐）。
/// 装配形态与 resp_vector_set_wrong_type.rs 同源（每测试独立临时目录）
fn consumer() -> (
  Runtime,
  RespSessionConsumer,
  Arc<WedbStore<SegmentedDevice>>,
  Arc<VectorManager>,
) {
  let dir = tempfile::tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("wt.db")).unwrap());
  let config = test_store_config();
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  let session = store.new_session().unwrap();

  // 回调无状态：向量会话按执行域绑定（测试为单任务同步段，专用会话持至用例结束）
  let _vector_domain = OwnedActiveVectorSession::new(store.new_session().unwrap());
  let callbacks = Callbacks::new(Arc::new(WedbVectorStoreCallbacks::<SegmentedDevice>::new()));
  let vm = Arc::new(VectorManager::new(
    VectorManagerOptions {
      is_enabled: true,
      ..Default::default()
    },
    callbacks,
  ));

  let api = StoreGarnetApi::new(session).with_vector_manager(Arc::clone(&vm));
  let consumer = RespSessionConsumer::new(1, RespServerSessionOptions::default(), Arc::new(api));
  forget(dir);
  (Runtime::new().unwrap(), consumer, store, vm)
}

/// 热路径命令往返（建键 / 删除等准备步；慢路径自动闭环）
fn roundtrip(rt: &Runtime, c: &mut RespSessionConsumer, req: &[u8]) -> Vec<u8> {
  let (consumed, mut out) = pump(c, req);
  assert_eq!(consumed, Some(0), "帧应被完整消费: {req:?}");
  if let Some(slow) = c.take_slow_wait() {
    rt.block_on(async {
      out.extend_from_slice(&slow.resolve().await);
    });
  }
  out
}

/// 冷态命令往返：快路径必须降级（零应答挂起 SlowWait），慢路径异步真读裁决
/// 后交付应答——降级本身即断言面（同步探针读不准磁盘候选）
fn cold_roundtrip(rt: &Runtime, c: &mut RespSessionConsumer, req: &[u8]) -> Vec<u8> {
  let (consumed, out) = pump(c, req);
  assert_eq!(consumed, Some(0), "帧应被完整消费: {req:?}");
  assert!(
    out.is_empty(),
    "冷态键命令必须降级慢路径而非就地应答: {req:?} → {:?}",
    String::from_utf8_lossy(&out)
  );
  let slow = c
    .take_slow_wait()
    .expect("冷态键命令必须挂起 SlowWait 异步闭环");
  rt.block_on(slow.resolve())
}

/// 3 维 FP32 向量字节（对齐 resp_vector_set_wrong_type.rs 建键形态）
fn vec3() -> Vec<u8> {
  [1.0f32, 2.0, 3.0]
    .iter()
    .flat_map(|v| v.to_le_bytes())
    .collect()
}

/// 冷态墓碑键上只读命令全臂 NOTFOUND 族应答（发现一 · 快路径确证缺失臂）：
/// SET → DEL 后墓碑同步探针可直接确证缺失（wkv 摘槽式删除）→ 放行快路径
/// 登记表 NOTFOUND 族应答（VSIM/VEMB 空数组、VCARD/VISMEMBER 0、VDIM
/// "ERR Key not found"、VGETATTR/VLINKS/VRANDMEMBER null、VINFO null 数组），
/// 绝非 WRONGTYPE 错误帧
#[test]
fn cold_tombstone_read_commands_notfound_parity() {
  let (rt, mut c, _store, vm) = consumer();

  assert_eq!(
    roundtrip(&rt, &mut c, &encode_frame(&[b"SET", b"ck", b"v"])),
    b"+OK\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &encode_frame(&[b"DEL", b"ck"])),
    b":1\r\n"
  );

  // 只读九命令全臂：C# NOTFOUND 族应答逐字节断言
  let cases: Vec<ReadTestCase<'_>> = vec![
    (b"VSIM", vec![b"ELE", b"e"], b"*0\r\n"),
    (b"VEMB", vec![b"e"], b"*0\r\n"),
    (b"VCARD", vec![], b":0\r\n"),
    (b"VDIM", vec![], b"-ERR Key not found\r\n"),
    (b"VGETATTR", vec![b"e"], b"$-1\r\n"),
    (b"VINFO", vec![], b"*-1\r\n"),
    (b"VISMEMBER", vec![b"e"], b":0\r\n"),
    (b"VLINKS", vec![b"e"], b"$-1\r\n"),
    (b"VRANDMEMBER", vec![], b"$-1\r\n"),
    (b"VRANDMEMBER", vec![b"3"], b"*0\r\n"),
  ];
  for (cmd, args, expected) in cases {
    let mut parts: Vec<&[u8]> = vec![cmd, b"ck"];
    parts.extend(args);
    let req = encode_frame(&parts);
    let out = roundtrip(&rt, &mut c, &req);
    assert_eq!(
      out,
      expected,
      "{} 冷态墓碑键应回 NOTFOUND 族应答: {:?}",
      String::from_utf8_lossy(cmd),
      String::from_utf8_lossy(&out)
    );
  }

  // 冷态键零登记：NOTFOUND 应答不产生向量登记副作用
  assert!(
    vm.read_stored_index(SessionPrefixBuf::ROOT.as_slice(), b"ck")
      .is_none(),
    "冷态读命令不得残留向量登记"
  );
}

/// 冷态过期键上只读命令经慢路径真读裁决回 NOTFOUND 族应答（发现一 · 读不准
/// 降级臂）：SET + EXPIRE 后检查点冷化淘汰，同步探针对数据记录只能判
/// RecordOnDisk（读不准）→ 修复前一律折 WRONGTYPE，修复后降级慢路径异步真读
/// 裁决——TTL 已过期视同缺失（对标 C# Reader 内 CheckExpiry 与冷记录
/// CompletePending 落盘裁决），回 NOTFOUND 族应答而非 WRONGTYPE 错误帧。
/// 每命令独立键：首命令真读即触发过期键惰性物理清除，同键后续命令探针
/// 可直接确证缺失（放行快路径，应答语义不变）
#[test]
fn cold_expired_read_commands_notfound_via_true_read() {
  let (rt, mut c, store, vm) = consumer();

  let cases: Vec<ReadTestCase<'_>> = vec![
    (b"VSIM", vec![b"ELE", b"e"], b"*0\r\n"),
    (b"VEMB", vec![b"e"], b"*0\r\n"),
    (b"VCARD", vec![], b":0\r\n"),
    (b"VDIM", vec![], b"-ERR Key not found\r\n"),
    (b"VGETATTR", vec![b"e"], b"$-1\r\n"),
    (b"VINFO", vec![], b"*-1\r\n"),
    (b"VISMEMBER", vec![b"e"], b":0\r\n"),
    (b"VLINKS", vec![b"e"], b"$-1\r\n"),
    (b"VRANDMEMBER", vec![], b"$-1\r\n"),
    (b"VRANDMEMBER", vec![b"3"], b"*0\r\n"),
  ];

  // 逐命令独立键统一构造：SET + EXPIRE 后检查点冷化淘汰（数据与 TTL 记录
  // 落盘，同步探针只余磁盘候选待裁决），再统一等 TTL 到期（1s 窗口 + 300ms
  // 余量）——慢路径真读落盘裁决时键已过期
  let keys: Vec<String> = (0..cases.len()).map(|ix| format!("ek{ix}")).collect();
  for key in &keys {
    let kb = key.as_bytes();
    assert_eq!(
      roundtrip(&rt, &mut c, &encode_frame(&[b"SET", kb, b"v"])),
      b"+OK\r\n"
    );
    assert_eq!(
      roundtrip(&rt, &mut c, &encode_frame(&[b"EXPIRE", kb, b"1"])),
      b":1\r\n"
    );
  }
  rt.block_on(store.flush_and_evict_all()).unwrap();
  sleep(Duration::from_millis(1300));

  for ((cmd, args, expected), key) in cases.iter().zip(&keys) {
    let mut parts: Vec<&[u8]> = vec![cmd, key.as_bytes()];
    parts.extend(args);
    let req = encode_frame(&parts);
    let out = cold_roundtrip(&rt, &mut c, &req);
    assert_eq!(
      out.as_slice(),
      *expected,
      "{} 冷态过期键应经真读裁决回 NOTFOUND 族应答: {:?}",
      String::from_utf8_lossy(cmd),
      String::from_utf8_lossy(&out)
    );
    assert!(
      vm.read_stored_index(SessionPrefixBuf::ROOT.as_slice(), key.as_bytes())
        .is_none(),
      "冷态读命令不得残留向量登记: {key}"
    );
  }
}

/// 冷态存活 string 锥上读写分臂（发现一断言另一半）：
/// 真读裁决存活非向量记录 → 只读命令慢路径回 WRONGTYPE（C# res==WRONGTYPE
/// 同文案）；写命令快路径守卫保守拒（deviations.md §22），不降级
#[test]
fn cold_alive_string_read_slow_wrongtype_write_guard_reject() {
  let (rt, mut c, store, _vm) = consumer();
  let vec_bytes = vec3();

  assert_eq!(
    roundtrip(&rt, &mut c, &encode_frame(&[b"SET", b"sk", b"v"])),
    b"+OK\r\n"
  );
  rt.block_on(store.flush_and_evict_all()).unwrap();

  // 只读：降级慢路径真读裁决 → WRONGTYPE（C# 无句点文案）
  let out = cold_roundtrip(
    &rt,
    &mut c,
    &encode_frame(&[b"VSIM", b"sk", b"FP32", &vec_bytes]),
  );
  assert_eq!(
    out, WRONGTYPE_FRAME,
    "冷态存活 string 锥上 VSIM 应回 WRONGTYPE"
  );

  // 写命令三臂：快路径守卫保守拒（不降级、无 SlowWait），同一文案
  for (cmd, args) in [
    (
      "VADD",
      vec![b"FP32".to_vec(), vec_bytes.clone(), b"e".to_vec()],
    ),
    ("VREM", vec![b"e".to_vec()]),
    ("VSETATTR", vec![b"e".to_vec(), b"a".to_vec()]),
  ] {
    let mut parts: Vec<&[u8]> = vec![cmd.as_bytes(), b"sk"];
    parts.extend(args.iter().map(|v| v.as_slice()));
    let (consumed, out) = pump(&mut c, &encode_frame(&parts));
    assert_eq!(consumed, Some(0), "帧应被完整消费: {cmd}");
    assert_eq!(
      out,
      WRONGTYPE_FRAME,
      "{cmd} 冷态不确定键应保守拒 WRONGTYPE（不降级）: {:?}",
      String::from_utf8_lossy(&out)
    );
    assert!(
      c.take_slow_wait().is_none(),
      "{cmd} 写命令保守拒不得挂起慢路径"
    );
  }
}

/// VSIM ELE 与 FP32 对同一非法 FILTER 回同一条编译失败文案（发现二，
/// deviations.md §23 语义锁）
#[test]
fn vsim_filter_compile_failure_parity() {
  // VADD 消费链 insert 需当前线程挂 compio runtime（thread-per-core），
  // 全体命令驱动置于 runtime 上下文内（roundtrip 内层 block_on 同线程复用）
  let (rt, mut c, _store, _vm) = consumer();
  rt.block_on(async {
    let vec_bytes = vec3();

    assert_eq!(
      roundtrip(
        &rt,
        &mut c,
        &encode_frame(&[b"VADD", b"fk", b"FP32", &vec_bytes, b"e1"])
      ),
      b":1\r\n",
      "VADD 建键应成功"
    );

    // 非法过滤表达式（未闭合括号，wvector filter compiler compile_errors 同款）
    let bad_filter = b"(1 + 2";
    let ele_out = roundtrip(
      &rt,
      &mut c,
      &encode_frame(&[b"VSIM", b"fk", b"ELE", b"e1", b"FILTER", bad_filter]),
    );
    let fp32_out = roundtrip(
      &rt,
      &mut c,
      &encode_frame(&[b"VSIM", b"fk", b"FP32", &vec_bytes, b"FILTER", bad_filter]),
    );
    assert!(
      ele_out.starts_with(b"-ERR Compiling filter failed\r\n"),
      "VSIM ELE 非法 FILTER 应回编译失败文案: {:?}",
      String::from_utf8_lossy(&ele_out)
    );
    assert_eq!(
      ele_out,
      fp32_out,
      "VSIM ELE 与 FP32 对同一非法 FILTER 必须回同一条错误文案: ELE={:?} FP32={:?}",
      String::from_utf8_lossy(&ele_out),
      String::from_utf8_lossy(&fp32_out)
    );
  })
}
