use std::{str::from_utf8, sync::Arc, thread};

use compio::runtime::Runtime;
use parking_lot::Mutex;
use wbase::{hash_slot::slot_of, map::HashMap};
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::resp::vector::{
  resp_server_session_vectors::{RespServerSessionVectors, VectorGuardVerdict, VectorReply},
  vector_manager::{
    CONTEXT_METADATA_SIZE, VectorAddArgs, VectorManager, VectorManagerOptions, VectorManagerResult,
    VectorSearchOptions,
  },
  vector_manager_filter::{
    evaluate_candidate_filter, get_selector_ranges, with_inline_filter_state,
  },
  vector_manager_index::Index,
  vector_manager_locking::registry_key,
  vector_registry_recovery::{
    RegistryRecord, decode_registry_physical, index_registry_physical_key,
    metadata_registry_physical_key,
  },
  vector_store_callbacks::OwnedActiveVectorSession,
};
use wval::{KeyTag, NamespaceDbCodec, SessionPrefixBuf, VectorRegistrySubTag};
use wvector::{
  Callbacks, DiskAnnInsertResult, IndexConfig, VectorDistanceMetricType, VectorIdFormat,
  VectorQuantType, VectorSetFlags, VectorValueType,
  filter::{
    attribute_extractor::extract_fields,
    compiler::try_compile,
    expression::ExprToken,
    runner::{default_stack, run},
  },
  store::{StoreCallbacks, Term, namespace_bytes},
  unpack_length_prefixed,
};

type TestMap = HashMap<(u64, Vec<u8>), Vec<u8>>;

/// 物理键拼装：`[命名空间字节][键字节]`（生产键编码构件 namespace_bytes 的测试装配）
#[inline]
fn physical_key(context: u64, key: &[u8]) -> Vec<u8> {
  let (ns_len, ns_buf) = namespace_bytes(context);
  let mut out = Vec::with_capacity(ns_len + key.len());
  out.extend_from_slice(&ns_buf[..ns_len]);
  out.extend_from_slice(key);
  out
}

/// 内存存储桩（统一物理键读写；生产路径由 wkv 磁盘会话承接）。
struct TestVectorStore {
  data: Mutex<TestMap>,
}

impl TestVectorStore {
  fn new() -> Self {
    Self {
      data: Mutex::new(HashMap::default()),
    }
  }
}

impl StoreCallbacks for TestVectorStore {
  async fn read_multi<F>(&self, context: u64, keys: &[u8], _length_hint: usize, mut f: F) -> bool
  where
    F: FnMut(u32, &[u8]) + Send,
  {
    let mut index = 0u32;
    let mut rest = keys;
    let guard = self.data.lock();
    while rest.len() >= 4 {
      let len = u32::from_le_bytes(rest[..4].try_into().unwrap_or([0; 4])) as usize;
      let total = 4 + len;
      if rest.len() < total {
        break;
      }
      let key = &rest[4..total];
      if let Some(value) = guard.get(&(context, physical_key(context, key).to_vec())) {
        f(index, value);
      }
      index += 1;
      rest = &rest[total..];
    }
    true
  }

  async fn read<F>(&self, context: u64, key: &[u8], mut f: F) -> bool
  where
    F: FnMut(&[u8]),
  {
    let phys_key = physical_key(context, key);
    let guard = self.data.lock();
    match guard.get(&(context, phys_key.as_slice().to_vec())) {
      Some(value) => {
        f(value);
        true
      }
      None => false,
    }
  }

  async fn write(&self, context: u64, key: &[u8], value: &[u8]) -> bool {
    let phys_key = physical_key(context, key);
    self
      .data
      .lock()
      .insert((context, phys_key.as_slice().to_vec()), value.to_vec());
    true
  }

  async fn delete(&self, context: u64, key: &[u8]) -> bool {
    let phys_key = physical_key(context, key);
    self
      .data
      .lock()
      .remove(&(context, phys_key.as_slice().to_vec()))
      .is_some()
  }

  async fn rmw<F>(&self, context: u64, key: &[u8], write_len: usize, mut f: F) -> bool
  where
    F: FnMut(&mut [u8]) + Send,
  {
    // 纯读短路（对齐生产回调与 C# 谓词 WriteDesiredSize == 0 判假）：不建不写
    if write_len == 0 {
      return true;
    }
    let phys_key = physical_key(context, key);
    // 对齐生产内核口径（WedbVectorStoreCallbacks::rmw）：write_len 即目标
    // 记录尺寸，旧值截短/补零后闭包改写、整值写回
    let mut buf = self
      .data
      .lock()
      .get(&(context, phys_key.as_slice().to_vec()))
      .cloned()
      .unwrap_or_default();
    buf.resize(write_len, 0);
    f(&mut buf);
    self
      .data
      .lock()
      .insert((context, phys_key.as_slice().to_vec()), buf);
    true
  }

  /// 内联过滤（对标 FilterCallbackUnmanaged → EvaluateCandidateFilter；
  /// 与生产 `WedbVectorStoreCallbacks::filter` 同语义：属性记录按
  /// internal_id 单读（本桩存储即物理键内存表），缺失/失败/不满足一律
  /// 排除；无内联过滤上下文回落放行）
  async fn filter(&self, context: u64, internal_id: u32) -> bool {
    // 属性单读先异步闭环，谓词求值再入线程槽同步段（守卫绝不跨 await）
    let mut attr = Vec::new();
    let hit = self
      .read(
        context | (Term::Attributes as u64),
        &internal_id.to_le_bytes(),
        |value| attr.extend_from_slice(value),
      )
      .await;
    with_inline_filter_state(|state| hit && evaluate_candidate_filter(state, &attr)).unwrap_or(true)
  }

  async fn purge_context(&self, context: u64) -> bool {
    self
      .data
      .lock()
      .retain(|&(ctx, _), _| ctx & !0b111 != context);
    true
  }

  fn log(&self, _context: u64, _msg: &str) {}
}

fn test_callbacks() -> Callbacks<TestVectorStore> {
  Callbacks::new(Arc::new(TestVectorStore::new()))
}

fn session() -> RespServerSessionVectors<TestVectorStore> {
  let manager = Arc::new(VectorManager::new(
    VectorManagerOptions {
      is_enabled: true,
      ..Default::default()
    },
    test_callbacks(),
  ));
  RespServerSessionVectors::new(manager)
}

fn manager() -> VectorManager<TestVectorStore> {
  VectorManager::new(
    VectorManagerOptions {
      is_enabled: true,
      ..Default::default()
    },
    test_callbacks(),
  )
}

/// 执行域绑定测试装配（新契约：进入 manager 命令臂前本执行域须已绑定向量
/// 存储会话，生产命令面由 `StoreGarnetApi::exec` 每命令包同步段守卫兜底）。
/// 直调用例按仓内唯一机制自持：真 wkv 会话经 [`OwnedActiveVectorSession`]
/// 后台自持形态绑定（同 `sync_transport::export_vector_set_elements` 导出臂
/// 口径），数据面仍由内存桩承接，守卫仅落契约绑定位。
fn bound_domain() -> (
  tempfile::TempDir,
  Arc<WedbStore<SegmentedDevice>>,
  OwnedActiveVectorSession<SegmentedDevice>,
) {
  let dir = tempfile::tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("bind.db")).unwrap());
  let store = Arc::new(WedbStore::open(wtest_base::test_store_config(), device).unwrap());
  let bound = OwnedActiveVectorSession::new(store.new_session().unwrap());
  (dir, store, bound)
}

fn fresh_index(context: u64, dims: u32) -> Index {
  Index {
    context,
    index_ptr: 1,
    dimensions: dims,
    reduce_dims: 0,
    num_links: 8,
    build_exploration_factor: 64,
    quant_type: VectorQuantType::NoQuant,
    distance_metric: VectorDistanceMetricType::L2,
    flags: VectorSetFlags::NONE,
  }
}

fn s(bytes: &[u8]) -> &str {
  from_utf8(bytes).unwrap()
}

/// 锁定读面取样（裸读口收口删除后的测试承接：断言面经 read_vector_index
/// 锁定读单点取登记记录，与生产命令臂同源）
async fn locked_index<S: StoreCallbacks>(sess: &RespServerSessionVectors<S>, key: &[u8]) -> Index {
  sess
    .manager
    .read_vector_index(SessionPrefixBuf::ROOT.as_slice(), key)
    .await
    .0
    .expect("登记记录应存在")
}

fn err_text(r: VectorReply) -> String {
  match r {
    VectorReply::Error(e) => s(&e).to_owned(),
    other => panic!("期望错误应答，实际 {other:?}"),
  }
}

fn f32_bytes(vals: &[f32]) -> Vec<u8> {
  vals.iter().flat_map(|v| v.to_le_bytes()).collect()
}

async fn seed_index<S: StoreCallbacks>(
  sess: &RespServerSessionVectors<S>,
  key: &[u8],
  dims: u32,
) -> [u8; 56] {
  let context = sess.manager.next_vector_set_context(0).await.unwrap();
  let _ = sess
    .manager
    .service
    .create_index(
      context,
      IndexConfig::new(
        dims,
        0,
        VectorQuantType::NoQuant,
        VectorDistanceMetricType::L2,
        64,
        8,
      ),
      sess.manager.callbacks.clone(),
    )
    .await;
  let index = Index {
    context,
    index_ptr: 1,
    dimensions: dims,
    reduce_dims: 0,
    num_links: 8,
    build_exploration_factor: 64,
    quant_type: VectorQuantType::NoQuant,
    distance_metric: VectorDistanceMetricType::L2,
    ..Index::default()
  };
  let bytes = index.to_bytes();
  sess
    .manager
    .write_stored_index(SessionPrefixBuf::ROOT.as_slice(), key, &bytes)
    .await;
  bytes
}

/// 默认会话库槽（测试键不参与定槽，统一取 (0,0) 库槽位）
const SLOT0: u16 = slot_of(0, 0);

/// test/standalone/Garnet.test.vectorset/RespVectorSetTests.cs:VADD
#[test]
fn vadd_parse_and_defaults() {
  // VADD/VSIM 命令面 insert 链 webc-diskann Handle::block_on 需当前线程挂 compio runtime
  Runtime::new().unwrap().block_on(async {
    // 新契约：直调命令臂须先绑定执行域向量会话（见 bound_domain 注释）
    let (_dir, _store, _bound) = bound_domain();
    let sess = session();

    // 参数不足
    let r = sess
      .network_vadd(
        SessionPrefixBuf::ROOT.as_slice(),
        &[b"k", b"FP32"],
        SLOT0,
        false,
      )
      .await;
    assert!(err_text(r).contains("wrong number of arguments"));

    // VALUES 形式 + 默认 Q8/200/16/L2（成功应答为整数 1）
    let r = sess
      .network_vadd(
        SessionPrefixBuf::ROOT.as_slice(),
        &[b"k", b"VALUES", b"2", b"1.5", b"-2.5", b"elem1", b"CAS"],
        SLOT0,
        false,
      )
      .await;
    assert_eq!(r, VectorReply::Integer(1));
    let index = locked_index(&sess, b"k").await;
    assert_eq!(index.dimensions, 2);
    assert_eq!(index.quant_type, VectorQuantType::Q8);
    assert_eq!(index.num_links, 16);
    assert_eq!(index.build_exploration_factor, 200);
    assert_eq!(index.distance_metric, VectorDistanceMetricType::L2);
    assert!(sess.manager.is_member(&index.to_bytes(), b"elem1").await);

    // 重复元素 → 整数 0（非错误，对齐 C# Duplicate 分支）
    let r = sess
      .network_vadd(
        SessionPrefixBuf::ROOT.as_slice(),
        &[b"k", b"VALUES", b"2", b"1.5", b"-2.5", b"elem1"],
        SLOT0,
        false,
      )
      .await;
    assert_eq!(r, VectorReply::Integer(0));

    // FP32 形式 + 全选项（新键）
    let r = sess
      .network_vadd(
        SessionPrefixBuf::ROOT.as_slice(),
        &[
          b"k2",
          b"FP32",
          &f32_bytes(&[3.0, 4.0]),
          b"elem2",
          b"NOQUANT",
          b"EF",
          b"64",
          b"SETATTR",
          b"{\"a\":1}",
          b"M",
          b"8",
          b"XDISTANCE_METRIC",
          b"COSINE",
        ],
        SLOT0,
        false,
      )
      .await;
    assert_eq!(r, VectorReply::Integer(1));
    let index = locked_index(&sess, b"k2").await;
    assert_eq!(index.quant_type, VectorQuantType::NoQuant);
    assert_eq!(index.num_links, 8);
    assert_eq!(index.build_exploration_factor, 64);
    assert_eq!(index.distance_metric, VectorDistanceMetricType::Cosine);
    assert_eq!(
      sess
        .manager
        .service
        .get_attribute(index.context, b"elem2")
        .await,
      Some(b"{\"a\":1}".to_vec())
    );

    // REDUCE + FP32：降维集合
    let r = sess
      .network_vadd(
        SessionPrefixBuf::ROOT.as_slice(),
        &[
          b"kr",
          b"REDUCE",
          b"1",
          b"FP32",
          &f32_bytes(&[3.0, 4.0]),
          b"e0",
        ],
        SLOT0,
        false,
      )
      .await;
    assert_eq!(r, VectorReply::Integer(1));
    let index = locked_index(&sess, b"kr").await;
    assert_eq!(index.reduce_dims, 1);

    // REDUCE 超过向量维度
    let r = sess
      .network_vadd(
        SessionPrefixBuf::ROOT.as_slice(),
        &[
          b"kr2",
          b"REDUCE",
          b"5",
          b"FP32",
          &f32_bytes(&[1.0, 2.0]),
          b"e",
        ],
        SLOT0,
        false,
      )
      .await;
    assert_eq!(
      err_text(r),
      "ERR REDUCE dimension must be <= vector dimensions"
    );

    // X 系量化 + REDUCE → quantization mismatch（对齐 C# BadParams 空文案回落）
    let r = sess
      .network_vadd(
        SessionPrefixBuf::ROOT.as_slice(),
        &[
          b"kq",
          b"REDUCE",
          b"1",
          b"FP32",
          &f32_bytes(&[1.0, 2.0]),
          b"e",
          b"XNOQUANT_U8",
        ],
        SLOT0,
        false,
      )
      .await;
    assert_eq!(
      err_text(r),
      "ERR asked quantization mismatch with existing vector set"
    );

    // 重复选项报错（量化 / EF / M / SETATTR / CAS / XDISTANCE_METRIC）
    let v1 = f32_bytes(&[1.0]);
    let dup_sets: Vec<Vec<&[u8]>> = vec![
      vec![b"k", b"FP32", &v1, b"x", b"NOQUANT", b"Q8"],
      vec![b"k", b"FP32", &v1, b"x", b"EF", b"8", b"EF", b"9"],
      vec![b"k", b"FP32", &v1, b"x", b"M", b"8", b"M", b"9"],
      vec![b"k", b"FP32", &v1, b"x", b"SETATTR", b"a", b"SETATTR", b"b"],
      vec![b"k", b"FP32", &v1, b"x", b"CAS", b"CAS"],
      vec![
        b"k",
        b"FP32",
        &v1,
        b"x",
        b"XDISTANCE_METRIC",
        b"L2",
        b"XDISTANCE_METRIC",
        b"COSINE",
      ],
    ];
    for dup in dup_sets {
      let r = sess
        .network_vadd(SessionPrefixBuf::ROOT.as_slice(), &dup, SLOT0, false)
        .await;
      assert!(matches!(r, VectorReply::Error(_)), "重复选项应报错: {r:?}");
    }
    assert_eq!(
      err_text(
        sess
          .network_vadd(
            SessionPrefixBuf::ROOT.as_slice(),
            &[b"k", b"FP32", &v1, b"x", b"NOQUANT", b"Q8"],
            SLOT0,
            false,
          )
          .await
      ),
      "Quantization specified multiple times"
    );

    // M 越界
    let r = sess
      .network_vadd(
        SessionPrefixBuf::ROOT.as_slice(),
        &[b"k", b"FP32", &f32_bytes(&[1.0]), b"x", b"M", b"2"],
        SLOT0,
        false,
      )
      .await;
    assert_eq!(err_text(r), "ERR M must be an integer between 4 and 4096");

    // EF 越界 + 非法度量 + 未知选项 + 元素后 REDUCE
    assert_eq!(
      err_text(
        sess
          .network_vadd(
            SessionPrefixBuf::ROOT.as_slice(),
            &[b"k", b"FP32", &v1, b"x", b"EF", b"0"],
            SLOT0,
            false,
          )
          .await
      ),
      "ERR EF must be an integer between 1 and 1000000"
    );
    assert_eq!(
      err_text(
        sess
          .network_vadd(
            SessionPrefixBuf::ROOT.as_slice(),
            &[b"k", b"FP32", &v1, b"x", b"XDISTANCE_METRIC", b"DOT"],
            SLOT0,
            false,
          )
          .await
      ),
      "ERR invalid XDISTANCE_METRIC"
    );
    assert_eq!(
      err_text(
        sess
          .network_vadd(
            SessionPrefixBuf::ROOT.as_slice(),
            &[b"k", b"FP32", &v1, b"x", b"WHAT"],
            SLOT0,
            false,
          )
          .await
      ),
      "ERR invalid option after element"
    );
    assert_eq!(
      err_text(
        sess
          .network_vadd(
            SessionPrefixBuf::ROOT.as_slice(),
            &[b"k", b"FP32", &v1, b"x", b"REDUCE", b"2"],
            SLOT0,
            false,
          )
          .await
      ),
      "ERR invalid option after element"
    );

    // 向量格式非法 + 空键
    assert_eq!(
      err_text(
        sess
          .network_vadd(
            SessionPrefixBuf::ROOT.as_slice(),
            &[b"k", b"FP32", b"123", b"x"],
            SLOT0,
            false,
          )
          .await
      ),
      "ERR invalid vector specification"
    );
    assert_eq!(
      err_text(
        sess
          .network_vadd(
            SessionPrefixBuf::ROOT.as_slice(),
            &[b"", b"FP32", &v1, b"x"],
            SLOT0,
            false,
          )
          .await
      ),
      "ERR Vector Set key cannot be empty"
    );

    // 严格整数：前导零拒绝（rust 严格收口，C# TryReadInt32Safe 因死参放行 007，见 doc/zh/deviations.md §32）
    let r = sess
      .network_vadd(
        SessionPrefixBuf::ROOT.as_slice(),
        &[b"k", b"VALUES", b"02", b"1.0", b"2.0", b"e"],
        SLOT0,
        false,
      )
      .await;
    assert_eq!(err_text(r), "ERR invalid vector specification");
  })
}

/// test/standalone/Garnet.test.vectorset/RespVectorSetTests.cs:VSIM
#[test]
fn vsim_options_and_output() {
  // try_add/insert async 化后整体包 compio runtime 收割（同步测试体,单层无重入）
  Runtime::new().unwrap().block_on(async {
    // 新契约：直调命令臂须先绑定执行域向量会话（见 bound_domain 注释）
    let (_dir, _store, _bound) = bound_domain();
    let sess = session();

    sess
      .manager
      .try_add(
        SessionPrefixBuf::ROOT.as_slice(),
        b"vs",
        &seed_index(&sess, b"vs", 2).await,
        &VectorAddArgs::new(
          b"near",
          VectorValueType::FP32,
          &f32_bytes(&[1.0, 0.0]),
          b"{\"n\":1}",
        ),
      )
      .await
      .unwrap();
    sess
      .manager
      .try_add(
        SessionPrefixBuf::ROOT.as_slice(),
        b"vs",
        &sess
          .manager
          .read_stored_index(SessionPrefixBuf::ROOT.as_slice(), b"vs")
          .unwrap(),
        &VectorAddArgs::new(
          b"far",
          VectorValueType::FP32,
          &f32_bytes(&[1.5, 0.5]),
          b"{\"n\":2}",
        ),
      )
      .await
      .unwrap();

    // RESP2 + WITHSCORES（id/score 交错扁平数组）
    let r = sess
      .network_vsim(
        SessionPrefixBuf::ROOT.as_slice(),
        &[
          b"vs",
          b"FP32",
          &f32_bytes(&[1.0, 0.0]),
          b"WITHSCORES",
          b"COUNT",
          b"2",
        ],
        false,
      )
      .await;
    let mut encoded = Vec::new();
    r.encode_resp2(&mut encoded);
    let text = s(&encoded);
    assert!(text.starts_with("*4\r\n"), "扁平数组 id/score 成对: {text}");

    // RESP3 + WITHSCORES → map（id → distance）
    let r3 = sess
      .network_vsim(
        SessionPrefixBuf::ROOT.as_slice(),
        &[
          b"vs",
          b"FP32",
          &f32_bytes(&[1.0, 0.0]),
          b"WITHSCORES",
          b"COUNT",
          b"2",
        ],
        true,
      )
      .await;
    let mut encoded3 = Vec::new();
    r3.encode_resp3(&mut encoded3);
    let text3 = s(&encoded3);
    assert!(text3.starts_with("%2\r\n"), "RESP3 map 头: {text3}");

    // RESP3 无选项 → 普通数组
    let r3 = sess
      .network_vsim(
        SessionPrefixBuf::ROOT.as_slice(),
        &[b"vs", b"ELE", b"near", b"COUNT", b"1"],
        true,
      )
      .await;
    let mut encoded3 = Vec::new();
    r3.encode_resp3(&mut encoded3);
    assert!(
      s(&encoded3).starts_with("*1\r\n"),
      "RESP3 普通数组: {}",
      s(&encoded3)
    );

    // FILTER 过滤 far
    let r = sess
      .network_vsim(
        SessionPrefixBuf::ROOT.as_slice(),
        &[
          b"vs",
          b"FP32",
          &f32_bytes(&[1.0, 0.0]),
          b"FILTER",
          b".n > 1",
          b"COUNT",
          b"2",
        ],
        false,
      )
      .await;
    let mut encoded = Vec::new();
    r.encode_resp2(&mut encoded);
    let text = s(&encoded);
    assert!(text.contains("far") && !text.contains("near"));

    // ELE 形式
    let r = sess
      .network_vsim(
        SessionPrefixBuf::ROOT.as_slice(),
        &[b"vs", b"ELE", b"near", b"COUNT", b"1"],
        false,
      )
      .await;
    let mut encoded = Vec::new();
    r.encode_resp2(&mut encoded);
    assert!(s(&encoded).contains("near"));

    // COUNT 重复
    let r = sess
      .network_vsim(
        SessionPrefixBuf::ROOT.as_slice(),
        &[
          b"vs",
          b"FP32",
          &f32_bytes(&[1.0, 0.0]),
          b"COUNT",
          b"1",
          b"COUNT",
          b"2",
        ],
        false,
      )
      .await;
    assert_eq!(err_text(r), "COUNT specified multiple times");

    // 未知选项（对齐 C# "Unknown option"）
    assert_eq!(
      err_text(
        sess
          .network_vsim(
            SessionPrefixBuf::ROOT.as_slice(),
            &[b"vs", b"ELE", b"near", b"WHAT"],
            false,
          )
          .await
      ),
      "Unknown option"
    );

    // 非法 kind（对齐 C# "VSIM expected ELE, FP32, or VALUES"）
    assert_eq!(
      err_text(
        sess
          .network_vsim(
            SessionPrefixBuf::ROOT.as_slice(),
            &[b"vs", b"WHAT", b"x"],
            false
          )
          .await
      ),
      "VSIM expected ELE, FP32, or VALUES"
    );

    // VALUES 分量非法（对齐 C# "VALUES value must be valid float"）
    assert_eq!(
      err_text(
        sess
          .network_vsim(
            SessionPrefixBuf::ROOT.as_slice(),
            &[b"vs", b"VALUES", b"2", b"1.0", b"abc"],
            false,
          )
          .await
      ),
      "VALUES value must be valid float"
    );

    // COUNT 选项缺值 → 参数数量错误（对齐 C# AbortWithWrongNumberOfArguments）
    assert!(
      err_text(
        sess
          .network_vsim(
            SessionPrefixBuf::ROOT.as_slice(),
            &[b"vs", b"ELE", b"near", b"COUNT"],
            false,
          )
          .await
      )
      .contains("wrong number of arguments")
    );

    // 元素不在集合中 → "Element not in Vector Set"（刻意偏差见 deviations.md §79）
    assert_eq!(
      err_text(
        sess
          .network_vsim(
            SessionPrefixBuf::ROOT.as_slice(),
            &[b"vs", b"ELE", b"ghost"],
            false,
          )
          .await
      ),
      "Element not in Vector Set"
    );

    // 组合用例：元素不在集合中 × FILTER 非法表达式 → 优先报 "Element not in Vector Set"（刻意偏差见 deviations.md §79）
    assert_eq!(
      err_text(
        sess
          .network_vsim(
            SessionPrefixBuf::ROOT.as_slice(),
            &[b"vs", b"ELE", b"ghost", b"FILTER", b">"],
            false,
          )
          .await
      ),
      "Element not in Vector Set"
    );

    // 缺键 → 空数组（对齐 C# NOTFOUND → TryWriteEmptyArray）
    assert_eq!(
      sess
        .network_vsim(
          SessionPrefixBuf::ROOT.as_slice(),
          &[b"nope", b"ELE", b"x"],
          false
        )
        .await,
      VectorReply::Array(Vec::new())
    );

    // EPSILON / FILTER-EF 解析与越界
    assert_eq!(
      err_text(
        sess
          .network_vsim(
            SessionPrefixBuf::ROOT.as_slice(),
            &[b"vs", b"ELE", b"near", b"EPSILON", b"-1"],
            false,
          )
          .await
      ),
      "EPSILON must be float > 0"
    );
    assert_eq!(
      err_text(
        sess
          .network_vsim(
            SessionPrefixBuf::ROOT.as_slice(),
            &[b"vs", b"ELE", b"near", b"FILTER-EF", b"3"],
            false,
          )
          .await
      ),
      "ERR FILTER-EF must be an integer between 4 and 256"
    );
  })
}

/// test/standalone/Garnet.test.vectorset/RespVectorSetTests.cs:VEMB_FP32Storage
#[test]
fn auxiliary_commands() {
  // try_add/insert async 化后整体包 compio runtime 收割（同步测试体,单层无重入）
  Runtime::new().unwrap().block_on(async {
    // 新契约：直调命令臂须先绑定执行域向量会话（见 bound_domain 注释）
    let (_dir, _store, _bound) = bound_domain();
    let sess = session();
    sess
      .manager
      .try_add(
        SessionPrefixBuf::ROOT.as_slice(),
        b"aux",
        &seed_index(&sess, b"aux", 2).await,
        &VectorAddArgs::new(
          b"e1",
          VectorValueType::FP32,
          &f32_bytes(&[5.0, 6.0]),
          b"{\"tag\":\"x\"}",
        ),
      )
      .await
      .unwrap();
    sess
      .manager
      .try_add(
        SessionPrefixBuf::ROOT.as_slice(),
        b"aux",
        &sess
          .manager
          .read_stored_index(SessionPrefixBuf::ROOT.as_slice(), b"aux")
          .unwrap(),
        &VectorAddArgs::new(b"e2", VectorValueType::FP32, &f32_bytes(&[1.0, 1.0]), b""),
      )
      .await
      .unwrap();

    assert_eq!(
      sess
        .network_vcard(SessionPrefixBuf::ROOT.as_slice(), &[b"aux"])
        .await,
      VectorReply::Integer(2)
    );
    assert_eq!(
      sess
        .network_vdim(SessionPrefixBuf::ROOT.as_slice(), &[b"aux"])
        .await,
      VectorReply::Integer(2)
    );
    assert_eq!(
      sess
        .network_vismember(SessionPrefixBuf::ROOT.as_slice(), &[b"aux", b"e1"], false)
        .await,
      VectorReply::Integer(1)
    );
    assert_eq!(
      sess
        .network_vismember(SessionPrefixBuf::ROOT.as_slice(), &[b"aux", b"zz"], false)
        .await,
      VectorReply::Integer(0)
    );
    assert_eq!(
      sess
        .network_vismember(SessionPrefixBuf::ROOT.as_slice(), &[b"aux", b"e1"], true)
        .await,
      VectorReply::Boolean(true)
    );

    // VEMB / VEMB RAW（NoQuant → fp32 通道）
    let emb = sess
      .network_vemb(SessionPrefixBuf::ROOT.as_slice(), &[b"aux", b"e1"])
      .await;
    let mut encoded = Vec::new();
    emb.encode_resp2(&mut encoded);
    let text = s(&encoded);
    assert!(text.contains('5'), "嵌入值包含 5: {text}");

    // VEMB RAW：NoQuant → [fp32, 原始 f32 字节, 范数]（原始字节非 UTF-8，按结构断言）
    let raw = sess
      .network_vemb(SessionPrefixBuf::ROOT.as_slice(), &[b"aux", b"e1", b"RAW"])
      .await;
    let VectorReply::Array(items) = raw else {
      panic!("RAW 应为数组: {raw:?}");
    };
    assert_eq!(items.len(), 3);
    assert_eq!(items[0], VectorReply::Simple(b"fp32"));
    assert_eq!(
      items[1],
      VectorReply::Bulk(Some(f32_bytes(&[5.0, 6.0]).into()))
    );

    // VEMB 非法选项
    assert_eq!(
      err_text(
        sess
          .network_vemb(SessionPrefixBuf::ROOT.as_slice(), &[b"aux", b"e1", b"BAD"])
          .await
      ),
      "Unexpected option to VEMB"
    );
    // VEMB 缺失元素 → 空数组（对齐 C# NOTFOUND）
    assert_eq!(
      sess
        .network_vemb(SessionPrefixBuf::ROOT.as_slice(), &[b"aux", b"ghost"])
        .await,
      VectorReply::Array(Vec::new())
    );

    // VGETATTR / VSETATTR（成功 1 / 缺失 0，非错误）
    assert_eq!(
      sess
        .network_vgetattr(SessionPrefixBuf::ROOT.as_slice(), &[b"aux", b"e1"])
        .await,
      VectorReply::Bulk(Some(b"{\"tag\":\"x\"}".to_vec().into()))
    );
    assert_eq!(
      sess
        .network_vsetattr(
          SessionPrefixBuf::ROOT.as_slice(),
          &[b"aux", b"e1", b"{}"],
          false
        )
        .await,
      VectorReply::Integer(1)
    );
    assert_eq!(
      sess
        .network_vsetattr(
          SessionPrefixBuf::ROOT.as_slice(),
          &[b"aux", b"ghost", b"{}"],
          false,
        )
        .await,
      VectorReply::Integer(0)
    );
    assert_eq!(
      sess
        .network_vgetattr(SessionPrefixBuf::ROOT.as_slice(), &[b"aux", b"e1"])
        .await,
      VectorReply::Bulk(Some(b"{}".to_vec().into()))
    );

    // VLINKS（缺失元素 → null）
    assert!(matches!(
      sess
        .network_vlinks(SessionPrefixBuf::ROOT.as_slice(), &[b"aux", b"e1"])
        .await,
      VectorReply::Array(_)
    ));
    assert_eq!(
      sess
        .network_vlinks(SessionPrefixBuf::ROOT.as_slice(), &[b"aux", b"ghost"])
        .await,
      VectorReply::Bulk(None)
    );
    assert_eq!(
      err_text(
        sess
          .network_vlinks(SessionPrefixBuf::ROOT.as_slice(), &[b"aux", b"e1", b"BAD"])
          .await
      ),
      "ERR Unexpected option"
    );

    // VRANDMEMBER
    assert!(matches!(
      sess
        .network_vrandmember(SessionPrefixBuf::ROOT.as_slice(), &[b"aux", b"2"])
        .await,
      VectorReply::Array(_)
    ));
    assert_eq!(
      err_text(
        sess
          .network_vrandmember(SessionPrefixBuf::ROOT.as_slice(), &[b"aux", b"x"])
          .await
      ),
      "ERR expected integer count"
    );
    assert_eq!(
      sess
        .network_vrandmember(SessionPrefixBuf::ROOT.as_slice(), &[b"ghost", b"2"])
        .await,
      VectorReply::Array(Vec::new())
    );
    assert_eq!(
      sess
        .network_vrandmember(SessionPrefixBuf::ROOT.as_slice(), &[b"ghost"])
        .await,
      VectorReply::Bulk(None)
    );

    // VINFO（对齐 C# 布局：14 项 + 小写枚举名）
    let info = sess
      .network_vinfo(SessionPrefixBuf::ROOT.as_slice(), &[b"aux"])
      .await;
    let mut encoded = Vec::new();
    info.encode_resp2(&mut encoded);
    let text = s(&encoded);
    assert!(text.starts_with("*14\r\n"), "VINFO 14 项: {text}");
    assert!(text.contains("input-vector-dimensions"));
    assert!(text.contains("reduced-dimensions"));
    assert!(text.contains("f32"));
    assert!(text.contains("l2"));
    // 整型五字段（aux 索引：dimensions 2 / reduce_dims 0 / BEF 64 / num_links 8 /
    // 基数 2）收口到 VectorReply::BulkInt 后的逐位帧型。
    assert_eq!(
      text,
      "*14\r\n+quant-type\r\n+f32\r\n+distance-metric\r\n+l2\r\n\
     +input-vector-dimensions\r\n$1\r\n2\r\n+reduced-dimensions\r\n$1\r\n0\r\n\
     +build-exploration-factor\r\n$2\r\n64\r\n+num-links\r\n$1\r\n8\r\n\
     +size\r\n$1\r\n2\r\n"
    );
    // 与收口前的老写法（`to_string().into_bytes()` 落 Bulk）逐位等帧，RESP2/RESP3 双版本
    let legacy = VectorReply::Array(vec![
      VectorReply::Simple(b"quant-type"),
      VectorReply::Simple(b"f32"),
      VectorReply::Simple(b"distance-metric"),
      VectorReply::Simple(b"l2"),
      VectorReply::Simple(b"input-vector-dimensions"),
      VectorReply::Bulk(Some(b"2".to_vec().into())),
      VectorReply::Simple(b"reduced-dimensions"),
      VectorReply::Bulk(Some(b"0".to_vec().into())),
      VectorReply::Simple(b"build-exploration-factor"),
      VectorReply::Bulk(Some(b"64".to_vec().into())),
      VectorReply::Simple(b"num-links"),
      VectorReply::Bulk(Some(b"8".to_vec().into())),
      VectorReply::Simple(b"size"),
      VectorReply::Bulk(Some(b"2".to_vec().into())),
    ]);
    let mut legacy2 = Vec::new();
    legacy.encode_resp2(&mut legacy2);
    assert_eq!(encoded, legacy2, "VINFO RESP2 整型帧与旧写法不等价");
    let mut encoded3 = Vec::new();
    info.encode_resp3(&mut encoded3);
    let mut legacy3 = Vec::new();
    legacy.encode_resp3(&mut legacy3);
    assert_eq!(encoded3, legacy3, "VINFO RESP3 整型帧与旧写法不等价");
    assert_eq!(encoded3, encoded, "VINFO 整型 bulk 帧不随协议版本变体");
    assert_eq!(
      sess
        .network_vinfo(SessionPrefixBuf::ROOT.as_slice(), &[b"ghost"])
        .await,
      VectorReply::NullArray
    );

    // VREM
    assert_eq!(
      sess
        .network_vrem(SessionPrefixBuf::ROOT.as_slice(), &[b"aux", b"e2"])
        .await,
      VectorReply::Integer(1)
    );
    assert_eq!(
      sess
        .network_vrem(SessionPrefixBuf::ROOT.as_slice(), &[b"aux", b"e2"])
        .await,
      VectorReply::Integer(0)
    );
    assert_eq!(
      sess
        .network_vcard(SessionPrefixBuf::ROOT.as_slice(), &[b"aux"])
        .await,
      VectorReply::Integer(1)
    );

    // VDIM 缺键 → "ERR Key not found"
    assert_eq!(
      err_text(
        sess
          .network_vdim(SessionPrefixBuf::ROOT.as_slice(), &[b"ghost"])
          .await,
      ),
      "ERR Key not found"
    );
  })
}

/// test/standalone/Garnet.test.vectorset/RespVectorSetTests.cs:DisabledWithFeatureFlag
#[compio::test]
async fn disabled_and_reply_encoding() {
  let manager = Arc::new(VectorManager::new(
    VectorManagerOptions {
      is_enabled: false,
      ..Default::default()
    },
    test_callbacks(),
  ));
  let sess = RespServerSessionVectors::new(manager);

  for r in [
    sess
      .network_vadd(
        SessionPrefixBuf::ROOT.as_slice(),
        &[b"k", b"FP32", &[0; 4], b"e"],
        SLOT0,
        false,
      )
      .await,
    sess
      .network_vsim(
        SessionPrefixBuf::ROOT.as_slice(),
        &[b"k", b"ELE", b"e"],
        false,
      )
      .await,
    sess
      .network_vcard(SessionPrefixBuf::ROOT.as_slice(), &[b"k"])
      .await,
    sess
      .network_vrem(SessionPrefixBuf::ROOT.as_slice(), &[b"k", b"e"])
      .await,
  ] {
    assert!(matches!(r, VectorReply::Error(_)), "未启用应拒绝");
  }

  // WRONGTYPE 守卫（键不存在 → Allow 放行。探针为 wkv 同步面，真存储会话承接）
  let dir = tempfile::tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("wt.db")).unwrap());
  let store = Arc::new(WedbStore::open(wtest_base::test_store_config(), device).unwrap());
  let session = store.new_session().unwrap();
  let batch = session.enter_batch();
  assert!(matches!(
    sess.vector_key_guard(b"somekey", &batch),
    VectorGuardVerdict::Allow
  ));

  // RESP2/RESP3 编码差异（含 map / null array）
  let reply = VectorReply::Array(vec![
    VectorReply::Bulk(Some(b"a".to_vec().into())),
    VectorReply::Double(2.0),
    VectorReply::Boolean(true),
  ]);
  let mut r2 = Vec::new();
  reply.encode_resp2(&mut r2);
  assert_eq!(s(&r2), "*3\r\n$1\r\na\r\n$1\r\n2\r\n$1\r\n1\r\n");
  let mut r3 = Vec::new();
  reply.encode_resp3(&mut r3);
  assert_eq!(s(&r3), "*3\r\n$1\r\na\r\n,2\r\n#t\r\n");

  let map = VectorReply::Map(vec![(
    VectorReply::Bulk(Some(b"id".to_vec().into())),
    VectorReply::Double(1.5),
  )]);
  let mut m2 = Vec::new();
  map.encode_resp2(&mut m2);
  assert_eq!(s(&m2), "*2\r\n$2\r\nid\r\n$3\r\n1.5\r\n");
  let mut m3 = Vec::new();
  map.encode_resp3(&mut m3);
  assert_eq!(s(&m3), "%1\r\n$2\r\nid\r\n,1.5\r\n");

  let mut na = Vec::new();
  VectorReply::NullArray.encode_resp2(&mut na);
  assert_eq!(s(&na), "*-1\r\n");
}

/// test/standalone/Garnet.test.vectorset/RespVectorSetTests.cs:VSIMWithAttributeFiltering
#[test]
fn result_writers_honor_bitmap_and_count() {
  let ids = vec![b"a".as_slice(), b"b".as_slice(), b"c".as_slice()];
  let distances = [0.1f32, 0.2, 0.3];
  // 位图：a、c 通过
  let bitmap = [0b101u8];

  // RESP2：输出上限 = min(count, popcount)
  let out =
    RespServerSessionVectors::write_resp2_result(10, &ids, &distances, &bitmap, None, false);
  let mut encoded = Vec::new();
  out.encode_resp2(&mut encoded);
  assert_eq!(s(&encoded), "*2\r\n$1\r\na\r\n$1\r\nc\r\n");

  // RESP3 无 score/attr → 普通数组
  let out =
    RespServerSessionVectors::write_resp3_result(10, &ids, &distances, &bitmap, None, false);
  let mut encoded = Vec::new();
  out.encode_resp3(&mut encoded);
  assert_eq!(s(&encoded), "*2\r\n$1\r\na\r\n$1\r\nc\r\n");

  // RESP3 + WITHSCORES + WITHATTRIBS → map，值为 [score, attr] 数组
  let attrs = vec![b"x".as_slice(), b"".as_slice(), b"z".as_slice()];
  let out =
    RespServerSessionVectors::write_resp3_result(10, &ids, &distances, &bitmap, Some(&attrs), true);
  let mut encoded = Vec::new();
  out.encode_resp3(&mut encoded);
  let text = s(&encoded);
  assert!(text.starts_with("%2\r\n"), "{text}");
  assert!(
    text.contains("$1\r\na\r\n*2\r\n,0.10000000149011612\r\n$1\r\nx\r\n"),
    "{text}"
  );
  assert!(
    text.contains("$1\r\nc\r\n*2\r\n,0.30000001192092896\r\n$1\r\nz\r\n"),
    "{text}"
  );

  // 空属性写 null：C# RespServerSessionVectors.cs:1098 `self.WriteNull()` →
  // RespServerSessionOutput.cs:193-205 按会话版本分派，RESP3 恒为 `_\r\n`，
  // 与「嵌在 map 值数组内」无关（C# 无按层位改形之臂）
  let attrs_one = vec![b"".as_slice()];
  let out = RespServerSessionVectors::write_resp3_result(
    10,
    &[b"e".as_slice()],
    &[0.5],
    &[],
    Some(&attrs_one),
    true,
  );
  let mut encoded = Vec::new();
  out.encode_resp3(&mut encoded);
  assert_eq!(s(&encoded), "%1\r\n$1\r\ne\r\n*2\r\n,0.5\r\n_\r\n");

  // count 截断
  let out = RespServerSessionVectors::write_resp2_result(1, &ids, &distances, &[], None, false);
  let mut encoded = Vec::new();
  out.encode_resp2(&mut encoded);
  assert_eq!(s(&encoded), "*1\r\n$1\r\na\r\n");
}

/// test/standalone/Garnet.test.vectorset/RespVectorSetTests.cs:VADDVariableLengthElementIds
#[test]
fn vadd_variable_length_element_ids() {
  // VADD/VSIM 命令面 insert 链 webc-diskann Handle::block_on 需当前线程挂 compio runtime
  Runtime::new().unwrap().block_on(async {
    // 新契约：直调命令臂须先绑定执行域向量会话（见 bound_domain 注释）
    let (_dir, _store, _bound) = bound_domain();
    let sess = session();
    let mut ids: Vec<Vec<u8>> = vec![Vec::new()];
    let mut len = 1usize;
    while len <= 1024 {
      ids.push(vec![len as u8; len]);
      len *= 2;
    }

    for id in &ids {
      let val = (id.len() as u8) as f32;
      let val_str = val.to_string();
      let r = sess
        .network_vadd(
          SessionPrefixBuf::ROOT.as_slice(),
          &[
            b"foo",
            b"VALUES",
            b"1",
            val_str.as_bytes(),
            id.as_slice(),
            b"XPREQ8",
          ],
          SLOT0,
          false,
        )
        .await;
      assert_eq!(r, VectorReply::Integer(1));
    }

    for id in &ids {
      let emb = sess
        .network_vemb(SessionPrefixBuf::ROOT.as_slice(), &[b"foo", id.as_slice()])
        .await;
      let VectorReply::Array(items) = emb else {
        panic!("应返回数组: {emb:?}");
      };
      assert_eq!(items.len(), 1);
      let VectorReply::Double(val) = items[0] else {
        panic!("元素应为 Double: {:?}", items[0]);
      };
      let expected = f64::from((id.len() as u8) as f32);
      assert!(
        (val - expected).abs() < 1e-2,
        "val={val}, expected={expected}"
      );
    }
  })
}

/// test/standalone/Garnet.test.vectorset/RespVectorSetTests.cs:VREM
///
/// 重复 VADD（Duplicate 臂）不覆盖既有属性/嵌入：无独立 C# 用例，行为原型为
/// C# VectorManager.TryAdd → DiskANNService.Insert 调用面（Duplicate 直接
/// return false，不触碰 attributes/vector；实现对位锚点由
/// wnode::resp::vector::vector_manager 的 try_add 单点持有）
#[test]
fn add_remove_and_query_lifecycle() {
  // try_add/insert async 化后整体包 compio runtime 收割（同步测试体,单层无重入）
  Runtime::new().unwrap().block_on(async {
    // 新契约：直调命令臂须先绑定执行域向量会话（见 bound_domain 注释）
    let (_dir, _store, _bound) = bound_domain();
    let manager = manager();
    let _ = manager
      .service
      .create_index(
        16,
        IndexConfig::new(
          2,
          0,
          VectorQuantType::NoQuant,
          VectorDistanceMetricType::L2,
          64,
          8,
        ),
        test_callbacks(),
      )
      .await;
    let index = fresh_index(16, 2).to_bytes();

    // VADD
    let res = manager
      .try_add(
        SessionPrefixBuf::ROOT.as_slice(),
        b"set",
        &index,
        &VectorAddArgs::new(
          b"elem-a",
          VectorValueType::FP32,
          &f32_bytes(&[1.0, 1.0]),
          b"{\"k\":1}",
        ),
      )
      .await;
    assert_eq!(res.unwrap(), VectorManagerResult::OK);

    // 重复 VADD → Duplicate
    let res = manager
      .try_add(
        SessionPrefixBuf::ROOT.as_slice(),
        b"set",
        &index,
        &VectorAddArgs::new(
          b"elem-a",
          VectorValueType::FP32,
          &f32_bytes(&[2.0, 2.0]),
          b"",
        ),
      )
      .await;
    assert_eq!(res.unwrap(), VectorManagerResult::Duplicate);

    // 维度不匹配
    let res = manager
      .try_add(
        SessionPrefixBuf::ROOT.as_slice(),
        b"set",
        &index,
        &VectorAddArgs::new(b"elem-b", VectorValueType::FP32, &f32_bytes(&[1.0]), b""),
      )
      .await;
    assert_eq!(res.unwrap_err().result, VectorManagerResult::BadParams);

    // M 不匹配
    let bad_m_bytes = f32_bytes(&[1.0, 2.0]);
    let mut bad_m = VectorAddArgs::new(b"elem-b", VectorValueType::FP32, &bad_m_bytes, b"");
    bad_m.num_links = 4;
    let res = manager
      .try_add(SessionPrefixBuf::ROOT.as_slice(), b"set", &index, &bad_m)
      .await;
    assert_eq!(
      res.unwrap_err().message.as_slice(),
      b"ERR asked M value mismatch with existing vector set"
    );

    // 成员判定 / 属性 / 嵌入（元素读为存储异步回调，逐点 `.await` 闭环）
    assert!(manager.is_member(&index, b"elem-a").await);
    assert!(!manager.is_member(&index, b"elem-z").await);
    assert_eq!(
      manager
        .fetch_single_vector_element_attributes(&index, b"elem-a")
        .await
        .as_deref(),
      Some(&b"{\"k\":1}"[..])
    );
    assert_eq!(
      manager
        .fetch_single_vector_element_attributes(&index, b"elem-z")
        .await,
      None
    );

    let emb = manager.try_get_embedding(&index, b"elem-a").await.unwrap();
    assert_eq!(emb, vec![1.0, 1.0]);

    let (raw, quant, norm, range) = manager
      .try_get_raw_embedding(&index, b"elem-a")
      .await
      .unwrap();
    assert_eq!(quant, VectorQuantType::NoQuant);
    assert!((norm - 1.0).abs() < f64::EPSILON);
    assert!(range.is_none());
    assert_eq!(raw, f32_bytes(&[1.0, 1.0]));

    // VREM（键身份带会话物理前缀，与 VADD 登记域同源）
    assert_eq!(
      manager
        .try_remove(SessionPrefixBuf::ROOT.as_slice(), b"set", &index, b"elem-a")
        .await,
      VectorManagerResult::OK
    );
    assert_eq!(
      manager
        .try_remove(SessionPrefixBuf::ROOT.as_slice(), b"set", &index, b"elem-a")
        .await,
      VectorManagerResult::MissingElement
    );
    assert!(!manager.is_member(&index, b"elem-a").await);
    assert_eq!(manager.try_get_embedding(&index, b"elem-a").await, None);
  })
}

/// test/standalone/Garnet.test.vectorset/RespVectorSetTests.cs:VSIMWithAdvancedFiltering
#[test]
fn value_similarity_with_post_filter() {
  // try_add/insert async 化后整体包 compio runtime 收割（同步测试体,单层无重入）
  Runtime::new().unwrap().block_on(async {
    // 新契约：直调命令臂须先绑定执行域向量会话（见 bound_domain 注释）
    let (_dir, _store, _bound) = bound_domain();
    let manager = manager();
    let _ = manager
      .service
      .create_index(
        32,
        IndexConfig::new(
          2,
          0,
          VectorQuantType::NoQuant,
          VectorDistanceMetricType::L2,
          64,
          8,
        ),
        test_callbacks(),
      )
      .await;
    let index = fresh_index(32, 2).to_bytes();

    for (name, v, attr) in [
      ("near", vec![1.0, 0.0], "{\"n\": 1}"),
      ("far", vec![5.0, 5.0], "{\"n\": 100}"),
      ("mid", vec![2.0, 0.0], "{\"n\": 5}"),
    ] {
      manager
        .try_add(
          SessionPrefixBuf::ROOT.as_slice(),
          b"set",
          &index,
          &VectorAddArgs::new(
            name.as_bytes(),
            VectorValueType::FP32,
            &f32_bytes(&v),
            attr.as_bytes(),
          ),
        )
        .await
        .unwrap();
    }

    // 无过滤：返回按距离排序的全部
    let out = manager
      .value_similarity(
        &index,
        VectorValueType::FP32,
        &f32_bytes(&[1.0, 0.0]),
        &VectorSearchOptions {
          count: 3,
          search_exploration_factor: 32,
          include_attributes: true,
          ..Default::default()
        },
      )
      .await
      .unwrap();
    assert_eq!(out.found, 3);
    assert_eq!(out.id_format, VectorIdFormat::I32LengthPrefixed);
    assert!(out.output_distances[0] < out.output_distances[2]);

    // EPSILON：距离上限截断（near 距离 0，mid 距离 1，far 距离 32）
    let out = manager
      .value_similarity(
        &index,
        VectorValueType::FP32,
        &f32_bytes(&[1.0, 0.0]),
        &VectorSearchOptions {
          count: 3,
          search_exploration_factor: 32,
          delta: 2.0,
          ..Default::default()
        },
      )
      .await
      .unwrap();
    assert_eq!(out.found, 2);
    let ids: Vec<&[u8]> = unpack_length_prefixed(&out.output_ids);
    assert!(ids.contains(&b"near".as_slice()) && ids.contains(&b"mid".as_slice()));

    // 过滤 .n > 1：near（n=1）在收集前即被内联过滤剔除——C# ValueSimilarity
    // 内联过滤路径先绑定 InlineFilterState，服务逐候选回调求值，仅通过候选
    // 写入输出缓冲（libs/server/Resp/Vector/VectorManager.cs:826-829 "Only
    // passing candidates are written to the output buffer"；Filter.cs:222-226
    // 内联回调"avoiding the need to over-fetch candidates and filter them
    // afterwards"），故 found 即通过数（=2）；ApplyPostFilter 为同款表达式
    // 复核，位图对入选项全置位（零压实契约：无就地压实、返回值弃用不回写
    // found，序列化端仍按位跳过未过项）
    let out = manager
      .value_similarity(
        &index,
        VectorValueType::FP32,
        &f32_bytes(&[1.0, 0.0]),
        &VectorSearchOptions {
          count: 3,
          search_exploration_factor: 32,
          filter: b".n > 1",
          max_filtering_effort: 16,
          include_attributes: true,
          ..Default::default()
        },
      )
      .await
      .unwrap();
    assert_eq!(out.found, 2, "内联过滤收集前剔除 near，found 即通过数");
    let ids: Vec<&[u8]> = unpack_length_prefixed(&out.output_ids);
    assert_eq!(
      ids,
      vec![b"mid".as_slice(), b"far".as_slice()],
      "入选项恰为满足谓词的 mid、far（按距离升序），near 不得滞留结果"
    );
    let passed = out
      .filter_bitmap
      .iter()
      .map(|b| b.count_ones())
      .sum::<u32>();
    assert_eq!(passed, 2, "后置复核位图对入选项全置位");

    // 过滤编译失败
    let out = manager
      .value_similarity(
        &index,
        VectorValueType::FP32,
        &f32_bytes(&[1.0, 0.0]),
        &VectorSearchOptions {
          count: 3,
          search_exploration_factor: 32,
          filter: b".n > >",
          max_filtering_effort: 16,
          ..Default::default()
        },
      )
      .await;
    assert_eq!(out.unwrap_err().message, b"ERR Compiling filter failed");
  })
}

/// test/standalone/Garnet.test.vectorset/RespVectorSetTests.cs:VSIMWithAttribs
#[test]
fn element_similarity_and_attributes_batch() {
  // try_add/insert async 化后整体包 compio runtime 收割（同步测试体,单层无重入）
  Runtime::new().unwrap().block_on(async {
    // 新契约：直调命令臂须先绑定执行域向量会话（见 bound_domain 注释）
    let (_dir, _store, _bound) = bound_domain();
    let manager = manager();
    let _ = manager
      .service
      .create_index(
        48,
        IndexConfig::new(
          1,
          0,
          VectorQuantType::NoQuant,
          VectorDistanceMetricType::L2,
          64,
          8,
        ),
        test_callbacks(),
      )
      .await;
    let index = fresh_index(48, 1).to_bytes();

    manager
      .try_add(
        SessionPrefixBuf::ROOT.as_slice(),
        b"set",
        &index,
        &VectorAddArgs::new(
          b"x",
          VectorValueType::FP32,
          &f32_bytes(&[0.0]),
          b"{\"a\":1}",
        ),
      )
      .await
      .unwrap();
    manager
      .try_add(
        SessionPrefixBuf::ROOT.as_slice(),
        b"set",
        &index,
        &VectorAddArgs::new(b"y", VectorValueType::FP32, &f32_bytes(&[10.0]), b""),
      )
      .await
      .unwrap();

    let out = manager
      .element_similarity(
        &index,
        b"x",
        &VectorSearchOptions {
          count: 2,
          search_exploration_factor: 32,
          ..Default::default()
        },
      )
      .await
      .unwrap();
    assert_eq!(out.found, 2);

    // 缺失元素 → MissingElement + "Element not in Vector Set"（刻意偏差见 deviations.md §79）
    let err = manager
      .element_similarity(
        &index,
        b"ghost",
        &VectorSearchOptions {
          count: 2,
          search_exploration_factor: 32,
          ..Default::default()
        },
      )
      .await
      .unwrap_err();
    assert_eq!(err.result, VectorManagerResult::MissingElement);
    assert_eq!(err.message, b"Element not in Vector Set".to_vec());

    // 批量属性
    let mut ids = Vec::new();
    ids.extend_from_slice(&1i32.to_le_bytes());
    ids.extend_from_slice(b"x");
    ids.extend_from_slice(&1i32.to_le_bytes());
    ids.extend_from_slice(b"y");
    let attrs = manager.fetch_vector_element_attributes(48, &ids).await;
    let parsed = unpack_length_prefixed(&attrs);
    assert_eq!(parsed[0], b"{\"a\":1}".as_slice());
    assert_eq!(parsed[1], b"".as_slice());

    // VSETATTR（键身份带会话物理前缀，与 VADD 登记域同源）
    assert!(
      manager
        .try_set_attribute(
          SessionPrefixBuf::ROOT.as_slice(),
          b"set",
          &index,
          b"y",
          b"{\"b\":2}"
        )
        .await
    );
    assert!(
      !manager
        .try_set_attribute(
          SessionPrefixBuf::ROOT.as_slice(),
          b"set",
          &index,
          b"none",
          b"{}"
        )
        .await
    );
  })
}

/// test/standalone/Garnet.test.vectorset/RespVectorSetTests.cs:DeleteVectorSet
#[test]
fn request_deletion_respects_suppress_cleanup() {
  // try_add/insert async 化后整体包 compio runtime 收割（同步测试体,单层无重入）
  Runtime::new().unwrap().block_on(async {
    let manager = manager();
    let mut index = fresh_index(64, 2);
    let _ = manager
      .service
      .create_index(
        64,
        IndexConfig::new(
          2,
          0,
          VectorQuantType::NoQuant,
          VectorDistanceMetricType::L2,
          16,
          4,
        ),
        test_callbacks(),
      )
      .await;
    assert_eq!(
      manager
        .service
        .insert(64, b"e", &f32_bytes(&[0.0, 0.0]), b"")
        .await,
      DiskAnnInsertResult::True
    );

    // 正常删除：发布清理请求 + 丢弃索引
    manager.request_deletion(&index.to_bytes());
    assert!(!manager.request_cleanup_task_channel.is_empty());
    assert_eq!(manager.request_cleanup_task_channel.try_pop(), Some(64));
    assert_eq!(manager.service.card(64), 0);

    // SuppressCleanup：删除被忽略
    let _ = manager
      .service
      .create_index(
        66,
        IndexConfig::new(
          2,
          0,
          VectorQuantType::NoQuant,
          VectorDistanceMetricType::L2,
          16,
          4,
        ),
        test_callbacks(),
      )
      .await;
    assert_eq!(
      manager
        .service
        .insert(66, b"e", &f32_bytes(&[0.0, 0.0]), b"")
        .await,
      DiskAnnInsertResult::True
    );
    index.context = 66;
    index.flags = VectorSetFlags::SUPPRESS_CLEANUP;
    manager.request_deletion(&index.to_bytes());
    assert!(manager.request_cleanup_task_channel.is_empty());
    assert_eq!(manager.service.card(66), 1);

    // 尺寸不符：忽略
    manager.request_deletion(&[0u8; 10]);
  })
}

/// test/standalone/Garnet.test.vectorset/RespVectorSetTests.cs:RepeatedVectorSetDeletes
#[test]
fn drop_in_memory_index_flow() {
  // try_add/insert async 化后整体包 compio runtime 收割（同步测试体,单层无重入）
  Runtime::new().unwrap().block_on(async {
    let manager = manager();
    let _ = manager
      .service
      .create_index(
        80,
        IndexConfig::new(
          2,
          0,
          VectorQuantType::NoQuant,
          VectorDistanceMetricType::L2,
          16,
          4,
        ),
        test_callbacks(),
      )
      .await;
    assert_eq!(
      manager
        .service
        .insert(80, b"e", &f32_bytes(&[0.0, 0.0]), b"")
        .await,
      DiskAnnInsertResult::True
    );

    let index = fresh_index(80, 2);
    // 直接丢弃：索引服务侧基数归零
    assert_eq!(manager.service.card(80), 1);
    manager.drop_in_memory_index(&index.to_bytes());
    assert_eq!(manager.service.card(80), 0);
  })
}

/// test/standalone/Garnet.test.vectorset/RespVectorSetTests.cs:RecreateIndexesOnRestoreAsync
#[compio::test]
async fn recovery_reconciliation_flow() {
  let manager = manager();

  let ctx = manager.next_vector_set_context(1).await.unwrap();

  let record = fresh_index(ctx, 4).to_bytes();
  manager.recovered_vector_set_index_key(&record);

  let meta_bytes = manager.context_metadatas.lock()[0].to_bytes();
  assert!(manager.recovered_context_metadata(&0i32.to_le_bytes(), &meta_bytes));
  assert!(!manager.recovered_context_metadata(&0i32.to_le_bytes(), &meta_bytes));
  assert!(manager.recovered_context_metadata(&0i32.to_le_bytes(), &[0u8; 8]));
  assert!(manager.recovered_context_metadata(&[0u8; 8], &meta_bytes));

  assert!(manager.reconcile_recovered_state(false).await);

  manager.request_deletion(&[0u8; 3]);
  manager.recovered_vector_set_index_key(&[0u8; 3]);

  let manager2 = VectorManager::new(
    VectorManagerOptions {
      is_enabled: true,
      ..Default::default()
    },
    test_callbacks(),
  );
  let _ = manager2.next_vector_set_context(0).await;
  assert!(!manager2.reconcile_recovered_state(true).await);
}

/// test/standalone/Garnet.test.vectorset/RespVectorSetTests.cs:HideInternalRecordsAsync
#[test]
fn rebuild_ingested_record_arms() {
  let manager = manager();

  // 元数据臂（rebuild 直调 recovered_context_metadata）：空记录剪枝放行
  let meta = [0u8; CONTEXT_METADATA_SIZE];
  assert!(manager.recovered_context_metadata(&0i32.to_le_bytes(), &meta));

  // 索引臂（rebuild 直调 clear_index_pointer + recovered_vector_set_index_key）：
  // 句柄属于写入进程——先清指针再暂存恢复上下文
  let mut value = fresh_index(96, 2).to_bytes();
  assert_ne!(u64::from_le_bytes(value[8..16].try_into().unwrap()), 0);
  VectorManager::<TestVectorStore>::clear_index_pointer(&mut value);
  manager.recovered_vector_set_index_key(&value);
  assert!(manager.recovered_indexes.pin().contains_key(&96));
  assert_eq!(u64::from_le_bytes(value[8..16].try_into().unwrap()), 0);
}

/// 登记旁路子标签枚举与物理键编解码回环（全链路一套强类型机制的编码单点校验）
#[test]
fn registry_subtag_physical_key_roundtrip() {
  // 1 字节码单点真值：from_u8 ↔ as_u8 回环，未定义值显式拒绝
  assert_eq!(VectorRegistrySubTag::Index.as_u8(), 0x01);
  assert_eq!(VectorRegistrySubTag::Metadata.as_u8(), 0x02);
  assert_eq!(
    VectorRegistrySubTag::from_u8(0x01),
    Some(VectorRegistrySubTag::Index)
  );
  assert_eq!(
    VectorRegistrySubTag::from_u8(0x02),
    Some(VectorRegistrySubTag::Metadata)
  );
  for raw in [0x00u8, 0x03, 0xff] {
    assert_eq!(
      VectorRegistrySubTag::from_u8(raw),
      None,
      "未定义子标签 {raw:#04x} 必须拒绝"
    );
  }

  // 索引登记：物理键编码 → 解码回环，域前缀随复合键原样拼回
  let prefix = SessionPrefixBuf::new(3, 5);
  let rk = registry_key(prefix.as_slice(), b"vec-set");
  let physical = index_registry_physical_key(rk.as_slice());
  assert_eq!(
    decode_registry_physical(physical.as_slice()),
    Some(RegistryRecord::Index(rk))
  );

  // 元数据登记：恒落根域前缀，下标回环
  assert_eq!(
    decode_registry_physical(metadata_registry_physical_key(7).as_slice()),
    Some(RegistryRecord::Metadata(7))
  );

  // 未定义子标签与残缺负载一律拒收（扫描侧跳过）
  let bogus = NamespaceDbCodec::encode_with_session_prefix(
    SessionPrefixBuf::ROOT.as_slice(),
    KeyTag::VectorRegistry,
    &[0x03, b'x'],
  );
  assert_eq!(decode_registry_physical(bogus.as_slice()), None);
  let truncated = NamespaceDbCodec::encode_with_session_prefix(
    SessionPrefixBuf::ROOT.as_slice(),
    KeyTag::VectorRegistry,
    &[VectorRegistrySubTag::Metadata.as_u8(), 0, 0],
  );
  assert_eq!(decode_registry_physical(truncated.as_slice()), None);
}

/// test/standalone/Garnet.test.vectorset/RespVectorSetTests.cs:VectorSetOpacity
#[test]
fn vector_set_opacity() {
  // VADD/VSIM 命令面 insert 链 webc-diskann Handle::block_on 需当前线程挂 compio runtime
  Runtime::new().unwrap().block_on(async {
    // 新契约：直调命令臂须先绑定执行域向量会话（见 bound_domain 注释）
    let (_dir, _store, _bound) = bound_domain();
    let sess = session();
    // C# 形态：VADD foo REDUCE 50 VALUES 75 <75 x "1.0"> <4 字节元素 id>
    // CAS NOQUANT EF 16 M 32 → 新建向量集返回 1；键为向量集（GET 报
    // WRONGTYPE），元素可经 VEMB 读回
    let mut args: Vec<&[u8]> = vec![b"foo", b"REDUCE", b"50", b"VALUES", b"75"];
    let vals = vec![b"1.0"; 75];
    for &v in &vals {
      args.push(v);
    }
    let elem: &[u8] = &[0, 0, 0, 0];
    args.extend_from_slice(&[elem, b"CAS", b"NOQUANT", b"EF", b"16", b"M", b"32"]);
    assert_eq!(
      sess
        .network_vadd(SessionPrefixBuf::ROOT.as_slice(), &args, SLOT0, false)
        .await,
      VectorReply::Integer(1)
    );

    // 向量集键在位（GET foo → WRONGTYPE 的不透明性由键类型承载）
    let index = locked_index(&sess, b"foo").await;
    assert!(sess.manager.is_member(&index.to_bytes(), elem).await);
  })
}

/// VADD REDUCE 的物理几何与 VINFO 回显同源：降维量化记录落盘、
/// 全精度记录维持全维（对标 C# SetActiveReadGeometry quantizedDims 判据；
/// 旧实现量化记录按全维落盘，本测试尺寸断言必红）。
#[test]
fn vadd_reduce_dims_physical_matches_vinfo() {
  // VADD/VSIM 命令面 insert 链 webc-diskann Handle::block_on 需当前线程挂 compio runtime
  Runtime::new().unwrap().block_on(async {
    // 新契约：直调命令臂须先绑定执行域向量会话（见 bound_domain 注释）
    let (_dir, _store, _bound) = bound_domain();
    const DIMS: usize = 64;
    const REDUCE: u32 = 8;

    let store = Arc::new(TestVectorStore::new());
    let manager = Arc::new(VectorManager::new(
      VectorManagerOptions {
        is_enabled: true,
        ..Default::default()
      },
      Callbacks::new(Arc::clone(&store)),
    ));
    let sess = RespServerSessionVectors::new(Arc::clone(&manager));

    // 默认 Q8 + REDUCE 8：64 维 FP32 首元素建集并入图
    let vec_bytes = f32_bytes(&(1..=DIMS).map(|i| i as f32).collect::<Vec<f32>>());
    assert_eq!(
      sess
        .network_vadd(
          SessionPrefixBuf::ROOT.as_slice(),
          &[b"kr", b"REDUCE", b"8", b"FP32", &vec_bytes, b"e0"],
          SLOT0,
          false,
        )
        .await,
      VectorReply::Integer(1)
    );
    let index = locked_index(&sess, b"kr").await;
    assert_eq!(index.reduce_dims, REDUCE);

    // 物理面：起点（iid 0）与元素（iid 1）的 Term::Quantized 记录均为
    // 降维规格（Q8 = 降维字节 + 20B minmax 元数据）；Term::Vector 维持全维
    let iid = sess
      .manager
      .service
      .internal_id_of(index.context, b"e0")
      .await
      .expect("e0 应已入图");
    let quant_ctx = index.context | Term::Quantized as u64;
    let full_ctx = index.context | Term::Vector as u64;
    for id in [0u32, iid] {
      let key = physical_key(quant_ctx, &id.to_le_bytes());
      let q = store.data.lock().get(&(quant_ctx, key)).map(Vec::len);
      assert_eq!(
        q,
        Some(REDUCE as usize + 20),
        "iid {id} 量化记录应为降维规格"
      );
      let key = physical_key(full_ctx, &id.to_le_bytes());
      let f = store.data.lock().get(&(full_ctx, key)).map(Vec::len);
      assert_eq!(f, Some(DIMS * 4), "iid {id} 全精度记录维持全维");
    }

    // 协议面：VINFO reduced-dimensions 回显与底层真实规格同值（非虚报）
    let mut encoded = Vec::new();
    sess
      .network_vinfo(SessionPrefixBuf::ROOT.as_slice(), &[b"kr"])
      .await
      .encode_resp2(&mut encoded);
    let text = s(&encoded);
    assert!(
      text.contains("+reduced-dimensions\r\n$1\r\n8\r\n"),
      "VINFO 应回显降维 8: {text}"
    );
  })
}

// ======================== 内联过滤(InlineFilterSearch)召回 ========================

/// 高选择性数据集装配：248 近邻（n=1，半径 0.01..2.48 步进 0.01）+ 8 命中
///（n=10，半径 0.061..0.096 交错）+ 8 无属性（半径介于命中项之间）；黄金角
/// 离散方位，到原点（查询点）距离恰为半径，全体两两异向。全图最相近的
/// top-5 均为不满足谓词的近邻——恒放行桩退化为普通 KNN + 纯后置过滤必返
/// 回 0 项，唯内联过滤可动态排除近邻召回命中元素（回归锁）。
async fn seed_high_selectivity(sess: &RespServerSessionVectors<TestVectorStore>) -> [u8; 56] {
  const GOLDEN_ANGLE: f64 = 2.399_963_229_728_653;
  let index = seed_index(sess, b"hs", 2).await;
  let mut seq = 0usize;
  // async 闭包驱动逐元素插入（edition 2024,调用处在 async 块内 .await）
  let mut add = async |element: &[u8], radius: f64, attr: &[u8]| {
    let angle = seq as f64 * GOLDEN_ANGLE;
    seq += 1;
    let point = f32_bytes(&[(radius * angle.cos()) as f32, (radius * angle.sin()) as f32]);
    sess
      .manager
      .try_add(
        SessionPrefixBuf::ROOT.as_slice(),
        b"hs",
        &index,
        &VectorAddArgs::new(element, VectorValueType::FP32, &point, attr),
      )
      .await
      .unwrap();
  };
  for i in 0..248u32 {
    add(
      format!("near{i}").as_bytes(),
      0.01 * (i as f64 + 1.0),
      b"{\"n\":1}",
    )
    .await;
  }
  for j in 0..8u32 {
    add(
      format!("hit{j}").as_bytes(),
      0.061 + 0.005 * j as f64,
      b"{\"n\":10}",
    )
    .await;
  }
  for j in 0..8u32 {
    add(
      format!("none{j}").as_bytes(),
      0.0635 + 0.005 * j as f64,
      b"",
    )
    .await;
  }
  index
}

/// 高选择性召回（本票主证，期望值由召回语义推导）：`FILTER ".n > 9"` 时
/// 图探索须内联排除近邻、召回 COUNT 个满足谓词的命中元素；负例面：无属性
/// 元素与不满足谓词元素均不得入选。协议面（RESP2）与元素臂（ELE 形式）同证。
#[test]
fn vsim_filter_high_selectivity_inline_recall() {
  // VSIM 检索链 webc-diskann Handle::block_on 需当前线程挂 compio runtime
  Runtime::new().unwrap().block_on(async {
    let (_dir, _store, _bound) = bound_domain();
    let sess = session();
    let index = seed_high_selectivity(&sess).await;

    let opts = VectorSearchOptions {
      count: 5,
      search_exploration_factor: 32,
      filter: b".n > 9",
      // 对齐 VSIM 协议面 FILTER-EF 默认值（DEFAULT_VSIM_FILTER_EF = 16）
      max_filtering_effort: 16,
      ..Default::default()
    };
    let out = sess
      .manager
      .value_similarity(
        &index,
        VectorValueType::FP32,
        &f32_bytes(&[0.0, 0.0]),
        &opts,
      )
      .await
      .unwrap();
    let ids: Vec<&[u8]> = unpack_length_prefixed(&out.output_ids)
      .into_iter()
      .collect();
    assert_eq!(ids.len(), 5, "须召回满 COUNT 的命中项");
    assert!(
      ids.iter().all(|id| id.starts_with(b"hit")),
      "全部满足谓词: {ids:?}"
    );
    assert!(
      !ids.iter().any(|id| id.starts_with(b"none")),
      "无属性元素不得入选"
    );
    assert!(
      !ids.iter().any(|id| id.starts_with(b"near")),
      "不满足谓词元素不得入选"
    );
    // 过滤位图与入选一致（零压实契约：按原结果下标置位）
    for i in 0..out.found {
      assert_eq!(
        (out.filter_bitmap[i >> 3] >> (i & 7)) & 1,
        1,
        "bit {i} 应置位"
      );
    }

    // 协议面：RESP2 应答只含命中元素
    let r = sess
      .network_vsim(
        SessionPrefixBuf::ROOT.as_slice(),
        &[
          b"hs",
          b"FP32",
          &f32_bytes(&[0.0, 0.0]),
          b"FILTER",
          b".n > 9",
          b"COUNT",
          b"5",
        ],
        false,
      )
      .await;
    let mut encoded = Vec::new();
    r.encode_resp2(&mut encoded);
    let text = s(&encoded);
    assert!(text.contains("hit"), "协议应答含命中项: {text}");
    assert!(
      !text.contains("near") && !text.contains("none"),
      "协议应答不得含被排除项: {text}"
    );

    // 元素臂同构：以命中元素为查询中心，ELE 形式同样只出满足谓词项
    let eopts = VectorSearchOptions {
      count: 3,
      search_exploration_factor: 32,
      filter: b".n > 9",
      max_filtering_effort: 16,
      ..Default::default()
    };
    let eout = sess
      .manager
      .element_similarity(&index, b"hit0", &eopts)
      .await
      .unwrap();
    assert!(eout.found >= 1, "元素臂至少命中一项");
    for id in unpack_length_prefixed(&eout.output_ids) {
      assert!(id.starts_with(b"hit"), "元素臂全部满足谓词: {id:?}");
    }
  })
}

/// 负例（缺失即排除）：请求超过命中总数的项数，应答恰为全部满足谓词的
/// 8 个命中元素；比部分命中元素更近的无属性元素一律被内联排除
///（对齐 `StoreCallbacks::filter` 契约与 C# 读失败 return 0 口径）。
#[test]
fn vsim_filter_missing_attributes_excluded() {
  // VSIM 检索链 webc-diskann Handle::block_on 需当前线程挂 compio runtime
  Runtime::new().unwrap().block_on(async {
    let (_dir, _store, _bound) = bound_domain();
    let sess = session();
    let index = seed_high_selectivity(&sess).await;

    let opts = VectorSearchOptions {
      count: 16,
      search_exploration_factor: 32,
      filter: b".n > 9",
      max_filtering_effort: 16,
      ..Default::default()
    };
    let out = sess
      .manager
      .value_similarity(
        &index,
        VectorValueType::FP32,
        &f32_bytes(&[0.0, 0.0]),
        &opts,
      )
      .await
      .unwrap();
    assert_eq!(out.found, 8, "应答恰为 8 个命中元素");
    for id in unpack_length_prefixed(&out.output_ids) {
      assert!(id.starts_with(b"hit"), "全部满足谓词: {id:?}");
    }
  })
}

/// 谓词正确性对齐：内联入选集合与 apply_post_filter 同款机制逐元素求值
/// 的位图在谓词上必一致（AdaptiveL 只影响召回量，不得影响谓词合法性）。
#[test]
fn vsim_filter_predicate_matches_post_filter_evaluation() {
  // VADD/VSIM 命令面 insert 链 webc-diskann Handle::block_on 需当前线程挂 compio runtime
  Runtime::new().unwrap().block_on(async {
    let (_dir, _store, _bound) = bound_domain();
    let sess = session();
    let index = seed_high_selectivity(&sess).await;

    let opts = VectorSearchOptions {
      count: 5,
      search_exploration_factor: 32,
      filter: b".n > 9",
      max_filtering_effort: 16,
      include_attributes: true,
      ..Default::default()
    };
    let out = sess
      .manager
      .value_similarity(
        &index,
        VectorValueType::FP32,
        &f32_bytes(&[0.0, 0.0]),
        &opts,
      )
      .await
      .unwrap();
    assert_eq!(out.found, 5);

    let filter = b".n > 9";
    let mut program = try_compile(filter).unwrap();
    let mut ranges = Vec::new();
    assert_eq!(
      get_selector_ranges(&program.instructions, filter, &mut ranges),
      1
    );
    let mut fields = vec![ExprToken::default(); ranges.len().max(1)];
    let mut stack = default_stack();
    for (i, attr) in unpack_length_prefixed(&out.output_attributes)
      .into_iter()
      .enumerate()
    {
      program.reset_runtime_pool();
      extract_fields(attr, filter, &ranges, &mut fields, &mut program);
      let pass = run(&program, attr, filter, &ranges, &fields, &mut stack);
      let bit = (out.filter_bitmap[i >> 3] >> (i & 7)) & 1;
      assert_eq!(bit, pass as u8, "结果 {i} 内联入选与后置位图必一致");
      assert!(pass, "结果 {i} 属性 {attr:?} 必满足谓词");
    }
  })
}

/// 线程/任务交错安全：两线程同管理器并发携带不同 FILTER 检索（各自绑定
/// 本线程执行域会话），断言各自结果只含各自谓词命中项，证明线程槽守卫
/// 无串味。
#[test]
fn vsim_filter_concurrent_queries_no_cross_talk() {
  // VADD/VSIM 命令面 insert 链 webc-diskann Handle::block_on 需当前线程挂 compio runtime
  Runtime::new().unwrap().block_on(async {
    async fn filtered_probe(
      mgr: &Arc<VectorManager<TestVectorStore>>,
      index: &[u8],
      predicate: &[u8],
      expect: &[u8],
    ) {
      for _ in 0..8 {
        let opts = VectorSearchOptions {
          count: 5,
          search_exploration_factor: 32,
          filter: predicate,
          max_filtering_effort: 16,
          ..Default::default()
        };
        let out = mgr
          .value_similarity(index, VectorValueType::FP32, &f32_bytes(&[0.0, 0.0]), &opts)
          .await
          .unwrap();
        assert!(out.found >= 1, "谓词 {predicate:?} 至少命中一项");
        for id in unpack_length_prefixed(&out.output_ids) {
          assert!(
            id.starts_with(expect),
            "谓词 {predicate:?} 结果只含 {expect:?} 前缀，实际 {id:?}"
          );
        }
      }
    }

    let (_dir, store, _bound) = bound_domain();
    let sess = session();
    let index = seed_high_selectivity(&sess).await;
    let manager = Arc::clone(&sess.manager);

    // 检索链 webc-diskann Handle::block_on 需线程挂 compio runtime（thread-per-core）,
    // 各查询线程自持 runtime 上下文（与生产一线程一运行时同形态）
    let h1 = {
      let manager = Arc::clone(&manager);
      let store = Arc::clone(&store);
      thread::spawn(move || {
        let _bound = OwnedActiveVectorSession::new(store.new_session().unwrap());
        Runtime::new().unwrap().block_on(async {
          filtered_probe(&manager, &index, b".n > 9", b"hit").await;
        })
      })
    };
    let h2 = thread::spawn(move || {
      let _bound = OwnedActiveVectorSession::new(store.new_session().unwrap());
      Runtime::new().unwrap().block_on(async {
        filtered_probe(&manager, &index, b".n < 5", b"near").await;
      })
    });
    h1.join().unwrap();
    h2.join().unwrap();
  })
}

/// 编译失败口径不变：非法 FILTER 仍报 ERR Compiling filter failed
///（检索入口编译校验臂不改道）。
#[compio::test]
async fn vsim_filter_compile_failure_error_unchanged() {
  let (_dir, _store, _bound) = bound_domain();
  let sess = session();
  seed_high_selectivity(&sess).await;

  let r = sess
    .network_vsim(
      SessionPrefixBuf::ROOT.as_slice(),
      &[
        b"hs",
        b"FP32",
        &f32_bytes(&[0.0, 0.0]),
        b"FILTER",
        b">",
        b"COUNT",
        b"5",
      ],
      false,
    )
    .await;
  assert_eq!(err_text(r), "ERR Compiling filter failed");
}

/// 刻意偏差参数优先级锁（见 deviations.md §79）：
/// VSIM key ELE 缺失元素 FILTER 非法表达式，断言优先回缺失元素文案（先于 FILTER 编译）。
#[compio::test]
async fn vsim_missing_element_priority_over_filter_compile() {
  let (_dir, _store, _bound) = bound_domain();
  let sess = session();
  let index = seed_index(&sess, b"vs_priority", 2).await;
  sess
    .manager
    .try_add(
      SessionPrefixBuf::ROOT.as_slice(),
      b"vs_priority",
      &index,
      &VectorAddArgs::new(
        b"existing",
        VectorValueType::FP32,
        &f32_bytes(&[1.0, 0.0]),
        b"{\"n\":1}",
      ),
    )
    .await
    .unwrap();

  // 会话面：ELE 缺失元素 + FILTER 非法表达式（">" 编译失败）
  // Rust 前置检查元素存在性，优先返回 "Element not in Vector Set" 而非 "ERR Compiling filter failed"
  let r = sess
    .network_vsim(
      SessionPrefixBuf::ROOT.as_slice(),
      &[
        b"vs_priority",
        b"ELE",
        b"ghost",
        b"FILTER",
        b">",
        b"COUNT",
        b"2",
      ],
      false,
    )
    .await;
  assert_eq!(err_text(r), "Element not in Vector Set");

  // 管理面：element_similarity 直接入参同样断言 MissingElement 先于 FILTER 编译失败
  let err = sess
    .manager
    .element_similarity(
      &index,
      b"ghost",
      &VectorSearchOptions {
        count: 2,
        filter: b">",
        ..Default::default()
      },
    )
    .await
    .unwrap_err();
  assert_eq!(err.result, VectorManagerResult::MissingElement);
  assert_eq!(err.message, b"Element not in Vector Set".to_vec());
}
