//! 向量九读锁臂收口回归（工单 zcode-r137c-vecrd2）
//!
//! 修复点：VCARD / VDIM / VGETATTR / VINFO / VISMEMBER / VLINKS / VRANDMEMBER /
//! VREM / VSETATTR 九臂自会话裸读单点 `read_index` 迁至锁定读面
//! `VectorManager::read_vector_index`（wkv 条带锁 + ptr=0 独占重建 + 原子降级
//! 共享，对标 C# VectorStoreOps.cs 十四锁点 `using(ReadVectorIndex)` 全程
//! 罩住命令体的形态）；裸读 pub 口 `read_index` 已随批删除，任何回退
//! 编译期即破（本测试面为「read_index 零消费端」正锁）。VCARD/VDIM/VINFO
//! 三臂同步段挂起化循既有 SlowWait 通道，无新机制。
//!
//! 断言面（工单 §4）：
//!   1. 重启回建 / 迁移导入完成臂同款窗（`vector_registry_recovery.rs:245`
//!      与 `vector_manager_migration.rs:308` 均置 `index_ptr = 0` 并弃内存索引）
//!      首触 VCARD/VREM 与先触 VSIM 后同命令应答逐字节全等；
//!   2. DEL×VREM 竞速注入锁：VREM 持共享守卫期间 `delete_vector_set` 独占
//!      臂必阻塞至守卫排空（对标 C# VectorStoreOps.cs:VectorSetRemove :227-234
//!      "That lock prevents deletion"）；
//!   3. VCARD/VDIM/VINFO 三臂同步段无就地应答形态（改锁定读后含重建挂起
//!      面，全量走 SlowWait）。
//!
//! 迁移导入目的一端与重启回建共用同一登记形态（ptr=0 + 内存索引缺失），
//! 首触行为同源——测试以 `inject_cold_rebuild_state` 桩模拟，两态一锁。

use std::{
  mem::forget,
  str::from_utf8,
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
  thread,
  time::{Duration, Instant},
};

use compio::runtime::Runtime;
use itoa::Buffer;
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  MessageConsumerFace,
  resp::{
    RespSessionConsumer,
    garnet_api::StoreGarnetApi,
    resp_server_session::RespServerSessionOptions,
    vector::{
      vector_manager::{VectorAddArgs, VectorManager, VectorManagerOptions, VectorManagerResult},
      vector_manager_index::Index,
      vector_manager_locking::CreateIndexParams,
      vector_store_callbacks::{OwnedActiveVectorSession, WedbVectorStoreCallbacks},
    },
  },
};
use wnode_test::pump;
use wtest_base::{resp_frame as encode_frame, test_store_config};
use wval::SessionPrefixBuf;
use wvector::{
  Callbacks, VectorDistanceMetricType, VectorQuantType, VectorValueType, store::StoreCallbacks,
};

/// 装配会话消费者（真存储 + 向量集合管理器）——形态与
/// `vector_cold_key_read_parity.rs` 一致（每测试独立临时目录，真 wkv 承
/// 载元素/邻接表落盘；drop 内存索引后锁定读面重建依赖盘数据召回，
/// 与 `vector_set_recall_smoke.rs:wkv_persistence_and_recall_after_recreate`
/// 同物性；严禁假 mock）
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

/// 命令往返（同步段可就地应答或挂起 SlowWait 皆闭环）
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

/// 挂起往返：同步段必须零应答（否则即读锁臂回退到 inline 就地应答形态），
/// SlowWait 必须挂起（十二臂全量挂起化断言面）
fn roundtrip_suspend(rt: &Runtime, c: &mut RespSessionConsumer, req: &[u8]) -> Vec<u8> {
  let (consumed, out) = pump(c, req);
  assert_eq!(consumed, Some(0), "帧应被完整消费: {req:?}");
  assert!(
    out.is_empty(),
    "向量命令同步段不得就地应答（须挂起 SlowWait）: {req:?} → {:?}",
    String::from_utf8_lossy(&out)
  );
  let slow = c
    .take_slow_wait()
    .expect("向量命令必须挂起 SlowWait 异步闭环");
  rt.block_on(slow.resolve())
}

fn fp32(vals: &[f32]) -> Vec<u8> {
  vals.iter().flat_map(|v| v.to_le_bytes()).collect()
}

/// 解析 RESP 数组-of-bulk 应答的成员字节集（VRANDMEMBER 乱序等值判定用）
fn bulk_array_members(frame: &[u8]) -> Vec<Vec<u8>> {
  let text = from_utf8(frame).expect("ASCII 帧");
  let mut rest = text.strip_prefix('*').expect("*N 数组帧");
  let (n_str, tail) = rest.split_once("\r\n").expect("*N 行尾");
  let n: usize = n_str.parse().expect("*N 计数");
  rest = tail;
  let mut out = Vec::with_capacity(n);
  for _ in 0..n {
    let bulk = rest.strip_prefix('$').expect("$bulk 项");
    let (len_str, tail) = bulk.split_once("\r\n").expect("$len 行尾");
    let len: usize = len_str.parse().expect("$len 计数");
    let bytes = tail.as_bytes()[..len].to_vec();
    rest = &tail[len + 2..];
    out.push(bytes);
  }
  assert!(rest.is_empty(), "帧应被完整解析");
  out
}

/// 建集：`VADD key FP32 <2dim> elem [SETATTR attr]` × 1
fn seed_one(rt: &Runtime, c: &mut RespSessionConsumer, key: &[u8], elem: &[u8], attr: &[u8]) {
  let q = fp32(&[1.0, 2.0]);
  let mut parts: Vec<&[u8]> = vec![b"VADD", key, b"FP32", &q, elem];
  if !attr.is_empty() {
    parts.push(b"SETATTR");
    parts.push(attr);
  }
  assert_eq!(
    roundtrip(rt, c, &encode_frame(&parts)),
    b":1\r\n",
    "VADD 建集应成功"
  );
}

/// 建集：三元素 e1/e2/e3（无属性），供基数/成员判据
fn seed_three(rt: &Runtime, c: &mut RespSessionConsumer, key: &[u8]) {
  for (i, e) in [b"e1".as_slice(), b"e2".as_slice(), b"e3".as_slice()]
    .iter()
    .enumerate()
  {
    let base = i as f32;
    assert_eq!(
      roundtrip(
        rt,
        c,
        &encode_frame(&[b"VADD", key, b"FP32", &fp32(&[base, base + 0.5]), e])
      ),
      b":1\r\n",
      "VADD 预插 {i} 应成功"
    );
  }
}

/// 注入重启回建 / 迁移导入完成端同款窗：登记记录 `index_ptr = 0` 且
/// 内存索引丢弃（`vector_registry_recovery.rs:245` 与
/// `vector_manager_migration.rs:308` 落地态的确定性桩，触发首笔锁定读
/// 走独占重建降级共享；裸读臂在窗内结构性零答）
fn inject_cold_rebuild_state<S: StoreCallbacks>(
  rt: &Runtime,
  vm: &Arc<VectorManager<S>>,
  key: &[u8],
) {
  let prefix = SessionPrefixBuf::ROOT.as_slice();
  let bytes = vm.read_stored_index(prefix, key).expect("登记记录应存在");
  let index = Index::from_bytes(&bytes).expect("登记记录应可解");
  // 丢内存索引（模拟新进程 service.indexes 缺该 context）
  vm.drop_in_memory_index(&bytes);
  assert_eq!(vm.service.card(index.context), 0, "内存索引应已丢弃");
  // 清指针（重启/迁移臂同款形态）
  let mut cleared = bytes;
  VectorManager::<S>::clear_index_pointer(&mut cleared);
  rt.block_on(vm.write_stored_index(prefix, key, &cleared));
  let cur = vm.read_stored_index(prefix, key).expect("桩应写入");
  assert_eq!(
    Index::from_bytes(&cur).expect("可解").index_ptr,
    0,
    "指针应清零"
  );
}

/// 读锁臂首触锁定重建：注入重启回建窗后，首触任一读臂即锁内独占重建 →
/// 降级共享命中 → 应答与暖态形（同命令不经注入路径）逐字节全等。
/// 修复前该臂沿 `read_index` papaya 直读，登记记录与原生索引生命周期
/// 分叉——登记在、内存索引缺，`wvector service.card / links_of / sample /
/// check_external_id_valid / get_attribute / get_full_vector` 静默零答，
/// 与暖态形应答分叉即工单 §2 案一常态可达面
#[test]
fn nine_read_arms_first_touch_matches_warm_byte_equality() {
  let (rt, mut c, _store, vm) = consumer();
  rt.block_on(async {
    // 每个命令位一对键：暖态形 (k_w) / 冷桩首触 (k_c)，内容完全一致。
    // prefix_args[0] 为命令名，其余为键后置参数（对标 C# parseState 线形
    // `CMD key element|count|FP32 <vec>`，RespServerSessionVectors.cs:1515/
    // 1633/1683：key 恒为 args[0]，元素/计数紧随键后）
    let cases: Vec<(&str, Vec<&[u8]>)> = vec![
      ("VCARD", vec![b"VCARD"]),
      ("VDIM", vec![b"VDIM"]),
      ("VGETATTR", vec![b"VGETATTR", b"e1"]),
      ("VINFO", vec![b"VINFO"]),
      ("VISMEMBER", vec![b"VISMEMBER", b"e1"]),
      ("VLINKS", vec![b"VLINKS", b"e1"]),
      ("VRANDMEMBER", vec![b"VRANDMEMBER", b"3"]),
      ("VEMB", vec![b"VEMB", b"e1"]),
      // VSIM 是既有锁定读臂（本工单不新增），保留以证明同臂族一致：
      // 冷桩首触 VSIM 与暖态形 VSIM 应答全等
      ("VSIM", vec![b"VSIM", b"FP32"]),
    ];
    // FP32 查询向量的字节随 vec3() 现场构造，不入 parts（借用生命周期）
    let q = fp32(&[1.0, 2.0]);

    for (name, prefix_args) in cases {
      let warm_key = format!("w_{name}").into_bytes();
      let cold_key = format!("c_{name}").into_bytes();
      seed_three(&rt, &mut c, &warm_key);
      seed_three(&rt, &mut c, &cold_key);

      // 构造完整帧：命令名 + 键 + 后置参数（键恒在 args[0]，与产品
      // read_vector_index(prefix, args[0]) 及 C# GetArgSliceByRef(0) 同形；
      // VSIM 尾追 FP32 查询字节）
      let mut warm_parts: Vec<&[u8]> = Vec::with_capacity(prefix_args.len() + 2);
      warm_parts.push(prefix_args[0]);
      warm_parts.push(&warm_key);
      warm_parts.extend_from_slice(&prefix_args[1..]);
      if name == "VSIM" {
        warm_parts.push(&q);
      }
      let warm = roundtrip(&rt, &mut c, &encode_frame(&warm_parts));

      // 冷桩首触：注入 index_ptr=0 + 内存索引丢弃 → 首触命令锁定臂内
      // 走独占重建 → 降级共享 → 应答与暖态形全等（工单 §4 案一锁）
      inject_cold_rebuild_state(&rt, &vm, &cold_key);
      let mut cold_parts: Vec<&[u8]> = Vec::with_capacity(prefix_args.len() + 2);
      cold_parts.push(prefix_args[0]);
      cold_parts.push(&cold_key);
      cold_parts.extend_from_slice(&prefix_args[1..]);
      if name == "VSIM" {
        cold_parts.push(&q);
      }
      let cold = roundtrip(&rt, &mut c, &encode_frame(&cold_parts));

      // VRANDMEMBER 系随机采样臂（C# VectorStoreOps.SampleElements 同源 rng
      // 乱序）：等值关系按「成员多重集全等」钉——count=3 抽 3 元集恒为全成员
      // 的随机排列，逐字节序不作确定性锁；其余臂维持逐字节全等
      if name == "VRANDMEMBER" {
        let (mut c_mem, mut w_mem) = (bulk_array_members(&cold), bulk_array_members(&warm));
        c_mem.sort();
        w_mem.sort();
        assert_eq!(
          c_mem,
          w_mem,
          "{name} 冷桩首触成员应与暖态形多重集全等: cold={:?} warm={:?}",
          String::from_utf8_lossy(&cold),
          String::from_utf8_lossy(&warm),
        );
      } else {
        assert_eq!(
          cold,
          warm,
          "{name} 冷桩首触应答应与暖态形逐字节全等（锁定读面独占重建降级共享 vs 已在位）: \
           cold={:?} warm={:?}",
          String::from_utf8_lossy(&cold),
          String::from_utf8_lossy(&warm),
        );
      }

      // 附加锁：冷桩首触后登记记录应回到 ptr!=0（重建写回），内存索引
      // 应在位——修复前静默零答不触发重建，此断言直接翻面
      let bytes = vm
        .read_stored_index(SessionPrefixBuf::ROOT.as_slice(), &cold_key)
        .expect("登记应在");
      assert_ne!(
        Index::from_bytes(&bytes).expect("可解").index_ptr,
        0,
        "{name} 冷桩首触后指针应由锁定读面重建写回"
      );
    }
  });
}

/// 工单 §4 案一核心锁：重启回建 / 迁移导入完成端注入态首触 VCARD 与
/// 「先触 VSIM 触发锁定重建 → 再触 VCARD」两序的应答逐字节全等——
/// 修复前 VCARD 走裸读，同数据集在窗内首触回 `:0` 而 VSIM 后回 `:3`，
/// 违反 review.md 板块 5.1「内部多态存储对同一数据集返回逐字节全等应答」
#[test]
fn vcard_first_touch_equals_vsim_then_vcard() {
  let (rt, mut c, _store, vm) = consumer();
  rt.block_on(async {
    // A 序：注入 → 首触 VCARD
    let ka = b"parity_a".as_slice();
    seed_three(&rt, &mut c, ka);
    inject_cold_rebuild_state(&rt, &vm, ka);
    let first = roundtrip(&rt, &mut c, &encode_frame(&[b"VCARD", ka]));

    // B 序：注入 → 先触 VSIM（既有锁定臂，触发重建）→ 再触 VCARD
    let kb = b"parity_b".as_slice();
    seed_three(&rt, &mut c, kb);
    inject_cold_rebuild_state(&rt, &vm, kb);
    let q = fp32(&[1.0, 2.0]);
    let _ = roundtrip(&rt, &mut c, &encode_frame(&[b"VSIM", kb, b"FP32", &q]));
    let second = roundtrip(&rt, &mut c, &encode_frame(&[b"VCARD", kb]));

    assert_eq!(
      first,
      second,
      "VCARD 首触应与「VSIM 先触重建后 VCARD」逐字节全等: first={:?} second={:?}",
      String::from_utf8_lossy(&first),
      String::from_utf8_lossy(&second),
    );
    assert_eq!(first, b":3\r\n", "冷桩首触 VCARD 应回三元素基数");
  });
}

/// 工单 §4 案二核心锁：VREM 首触（锁定重建 → 命中删除）应答与
/// 「VSIM 先触重建 → VREM」逐字节全等，且删除后暖态基数应扣 1
#[test]
fn vrem_first_touch_equals_vsim_then_vrem() {
  let (rt, mut c, _store, vm) = consumer();
  rt.block_on(async {
    // A 序：注入 → 首触 VREM
    let ka = b"vrem_a".as_slice();
    seed_three(&rt, &mut c, ka);
    inject_cold_rebuild_state(&rt, &vm, ka);
    let first = roundtrip(&rt, &mut c, &encode_frame(&[b"VREM", ka, b"e1"]));

    // B 序：注入 → VSIM 先触 → VREM
    let kb = b"vrem_b".as_slice();
    seed_three(&rt, &mut c, kb);
    inject_cold_rebuild_state(&rt, &vm, kb);
    let q = fp32(&[1.0, 2.0]);
    let _ = roundtrip(&rt, &mut c, &encode_frame(&[b"VSIM", kb, b"FP32", &q]));
    let second = roundtrip(&rt, &mut c, &encode_frame(&[b"VREM", kb, b"e1"]));

    assert_eq!(
      first,
      second,
      "VREM 首触应与 VSIM 先触后 VREM 逐字节全等: first={:?} second={:?}",
      String::from_utf8_lossy(&first),
      String::from_utf8_lossy(&second),
    );
    assert_eq!(
      first, b":1\r\n",
      "VREM 首触应命中重建后的原生索引，回 1（非修复前静默零答 :0）"
    );
    // 后续 VCARD 应扣一（写体在守卫下真的落到了原生索引）
    assert_eq!(
      roundtrip(&rt, &mut c, &encode_frame(&[b"VCARD", ka])),
      b":2\r\n"
    );
    assert_eq!(
      roundtrip(&rt, &mut c, &encode_frame(&[b"VCARD", kb])),
      b":2\r\n"
    );
  });
}

/// VSETATTR 首触写体（同案二族）：锁定重建后属性应写入；VGETATTR 应读回；
/// 与「VSIM 先触重建 → VSETATTR」逐字节全等
#[test]
fn vsetattr_first_touch_equals_vsim_then_vsetattr() {
  let (rt, mut c, _store, vm) = consumer();
  rt.block_on(async {
    let ka = b"sa_a".as_slice();
    let kb = b"sa_b".as_slice();
    let attr = b"{\"k\":1}".as_slice();

    seed_one(&rt, &mut c, ka, b"e1", b"");
    seed_one(&rt, &mut c, kb, b"e1", b"");

    inject_cold_rebuild_state(&rt, &vm, ka);
    let first = roundtrip(&rt, &mut c, &encode_frame(&[b"VSETATTR", ka, b"e1", attr]));

    inject_cold_rebuild_state(&rt, &vm, kb);
    let q = fp32(&[1.0, 2.0]);
    let _ = roundtrip(&rt, &mut c, &encode_frame(&[b"VSIM", kb, b"FP32", &q]));
    let second = roundtrip(&rt, &mut c, &encode_frame(&[b"VSETATTR", kb, b"e1", attr]));

    assert_eq!(
      first,
      second,
      "VSETATTR 首触应与 VSIM 先触后 VSETATTR 逐字节全等: first={:?} second={:?}",
      String::from_utf8_lossy(&first),
      String::from_utf8_lossy(&second),
    );
    assert_eq!(
      first, b":1\r\n",
      "VSETATTR 首触应回 1（非修复前静默零答 0）"
    );
    // 属性应真的写入
    let got = roundtrip(&rt, &mut c, &encode_frame(&[b"VGETATTR", ka, b"e1"]));
    assert_eq!(
      got,
      encode_frame_single_bulk(attr),
      "首触后 VGETATTR 应读回新属性: got={:?}",
      String::from_utf8_lossy(&got)
    );
  });
}

/// 单 bulk 编码（VGETATTR 应答形态）
fn encode_frame_single_bulk(bytes: &[u8]) -> Vec<u8> {
  let mut ibuf = Buffer::new();
  let mut out = Vec::with_capacity(bytes.len() + 16);
  out.push(b'$');
  out.extend_from_slice(ibuf.format(bytes.len()).as_bytes());
  out.extend_from_slice(b"\r\n");
  out.extend_from_slice(bytes);
  out.extend_from_slice(b"\r\n");
  out
}

/// 工单 §4 案二锁面：VREM 持共享守卫期间 delete_vector_set 独占臂必阻塞
/// 至守卫排空——C# `VectorStoreOps.cs:VectorSetRemove :227-234` "That lock
/// prevents deletion" 契约的 rust 承接。修复前 VREM 走裸读，`try_remove`
/// 无守卫跨 await，DEL 独占臂在 VREM 挂起窗口内直接摘登记 + 弃内存索引，
/// `try_remove` 苏醒后对已弃 context 回 0/false 或先落 remove 再被 discard
/// 覆盖，主从经 AOF 复制发散
#[test]
fn vrem_guard_blocks_delete_vector_set_until_body_drained() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (_dir, store) = fresh_store();
    let _bound = OwnedActiveVectorSession::new(store.new_session().unwrap());
    let callbacks = Callbacks::new(Arc::new(WedbVectorStoreCallbacks::<SegmentedDevice>::new()));
    let vm: Arc<VectorManager> = Arc::new(VectorManager::new(
      VectorManagerOptions {
        is_enabled: true,
        ..Default::default()
      },
      callbacks,
    ));

    let root = SessionPrefixBuf::ROOT.as_slice();
    let key = b"vrem-del-block".as_slice();
    // 建集（read_or_create_vector_index 锁面，与生产 VADD 语义一致）
    let (index, create_guard) = vm
      .read_or_create_vector_index(root, key, Some(&create_params_2d()))
      .await
      .unwrap();
    let bytes = index.to_bytes();
    for i in 0..2u32 {
      let e = format!("e{i}").into_bytes();
      let mut v = [0u8; 8];
      v[..4].copy_from_slice(&(i as f32).to_le_bytes());
      v[4..].copy_from_slice(&1.0f32.to_le_bytes());
      let add = VectorAddArgs::new(&e, VectorValueType::FP32, &v, b"");
      assert_eq!(
        vm.try_add(root, key, &bytes, &add).await,
        Ok(VectorManagerResult::OK),
        "预插 {i} 应成功"
      );
    }
    drop(create_guard);

    // 取共享守卫——与九臂命令体内 `read_vector_index(prefix, key)` 同物；
    // 本测试面锁定守卫语义等价于 VREM 命令体持守卫窗口
    let (_hit_index, shared) = vm.read_vector_index(root, key).await;
    let shared = shared.expect("锁定读命中应交付共享守卫");

    // 删除线程：独占臂应被共享守卫挡在门外
    let deleted = Arc::new(AtomicBool::new(false));
    let d_mgr = Arc::clone(&vm);
    let d_flag = Arc::clone(&deleted);
    let d_key = key.to_vec();
    let deleter = thread::spawn(move || {
      let rt = Runtime::new().unwrap();
      if rt.block_on(d_mgr.delete_vector_set(root, &d_key)) {
        d_flag.store(true, Ordering::Release);
      }
    });

    // 200ms 观察窗（对标 vector_delete_exclusive_lock_race.rs 同款确定性
    // 时序窗）：旧实现裸读臂零守卫，此窗内删除即穿透
    let deadline = Instant::now() + Duration::from_millis(200);
    while Instant::now() < deadline {
      assert!(
        !deleted.load(Ordering::Acquire),
        "共享守卫存续期间 delete_vector_set 独占臂不得穿透条带锁"
      );
      assert!(
        vm.read_stored_index(root, key).is_some(),
        "守卫存续期间登记不得被摘除"
      );
      thread::sleep(Duration::from_millis(10));
    }

    // 排空守卫：删除立即推进
    drop(shared);
    deleter.join().unwrap();
    assert!(deleted.load(Ordering::Acquire), "守卫排空后删除必须完成");
    assert!(
      vm.read_stored_index(root, key).is_none(),
      "删除后登记原子消失"
    );
  });
}

/// VCARD / VDIM / VINFO 同步段挂起化：修复前三臂走同步段 inline 应答，
/// 收口为 `read_vector_index` 锁定读面后含独占重建挂起面，全量转
/// SlowWait 通道（本工单 §1.1 循既有 SlowWait 无新机制）。断言面：
/// 暖态与冷桩两态下，同步段就地输出必空、SlowWait 必挂起、resolve 后
/// 应答与锁定读语义一致
#[test]
fn vcard_vdim_vinfo_suspend_via_slowwait() {
  let (rt, mut c, _store, vm) = consumer();
  rt.block_on(async {
    let k = b"susp".as_slice();
    seed_three(&rt, &mut c, k);

    // 暖态：三臂同步段应零输出并挂 SlowWait（锁定读臂的 await 面）
    for args in [
      vec![b"VCARD".as_slice(), k],
      vec![b"VDIM".as_slice(), k],
      vec![b"VINFO".as_slice(), k],
    ] {
      let out = roundtrip_suspend(&rt, &mut c, &encode_frame(&args));
      assert!(!out.is_empty(), "resolve 应交付应答: {args:?}");
    }

    // 冷桩态：注入后首触三臂（重建触发路径），同步段亦零输出挂 SlowWait，
    // resolve 交付与三臂锁定读一致的应答
    inject_cold_rebuild_state(&rt, &vm, k);
    let card = roundtrip_suspend(&rt, &mut c, &encode_frame(&[b"VCARD", k]));
    assert_eq!(card, b":3\r\n");
    inject_cold_rebuild_state(&rt, &vm, k);
    let dim = roundtrip_suspend(&rt, &mut c, &encode_frame(&[b"VDIM", k]));
    assert_eq!(dim, b":2\r\n");
    inject_cold_rebuild_state(&rt, &vm, k);
    let info = roundtrip_suspend(&rt, &mut c, &encode_frame(&[b"VINFO", k]));
    assert!(
      info.starts_with(b"*14\r\n"),
      "VINFO 应回 14 项数组（修复前冷桩态回 NullArray `*-1`）: {:?}",
      String::from_utf8_lossy(&info)
    );
  });
}

/// 迁移导入目的一端窗锁：`vector_manager_migration.rs:308` 直写
/// `index_ptr = 0` 与重启回建形态等价，本测试通过注入相同形态覆盖
/// （`inject_cold_rebuild_state` 桩）；九臂收口后共享守卫路径与暖态
/// 逐字节全等——已由 `nine_read_arms_first_touch_matches_warm_byte_equality`
/// 全臂锁面统一。本用例补 VREM 一条目的一端形态（案二写臂迁移窗）
#[test]
fn vrem_migration_target_state_matches_warm() {
  let (rt, mut c, _store, vm) = consumer();
  rt.block_on(async {
    let ka = b"mig_warm".as_slice();
    let kb = b"mig_cold".as_slice();
    seed_three(&rt, &mut c, ka);
    seed_three(&rt, &mut c, kb);

    let warm = roundtrip(&rt, &mut c, &encode_frame(&[b"VREM", ka, b"e2"]));
    // 迁移目的一端形态：ptr=0 + 内存索引丢弃（同 inject_cold_rebuild_state）
    inject_cold_rebuild_state(&rt, &vm, kb);
    let cold = roundtrip(&rt, &mut c, &encode_frame(&[b"VREM", kb, b"e2"]));

    assert_eq!(
      cold,
      warm,
      "VREM 迁移态首触与暖态应答应逐字节全等: cold={:?} warm={:?}",
      String::from_utf8_lossy(&cold),
      String::from_utf8_lossy(&warm)
    );
    assert_eq!(warm, b":1\r\n");
  });
}

// ========================= 夹具（本文件独立） =========================

/// 独立临时 wkv 存储（守卫阻塞用例专用；不共享 consumer 内会话）
fn fresh_store() -> (tempfile::TempDir, Arc<WedbStore<SegmentedDevice>>) {
  let dir = tempfile::tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("race.db")).unwrap());
  let store = Arc::new(WedbStore::open(test_store_config(), device).unwrap());
  (dir, store)
}

fn create_params_2d() -> CreateIndexParams {
  CreateIndexParams {
    hash_slot: 0,
    dims: 2,
    reduce_dims: 0,
    quant: VectorQuantType::NoQuant,
    build_exploration_factor: 64,
    num_links: 8,
    distance_metric: VectorDistanceMetricType::L2,
  }
}
