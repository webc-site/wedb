//! VADD 属性写失败回归（票：wvector-service-insert-attribute-write-failure-mapped-duplicate）
//!
//! 缺陷形态：DiskANNService::insert 中「元素已插入、属性单独写失败」被折叠成
//! DiskAnnInsertResult::False ⇒ try_add 误报 Duplicate ⇒ 客户端收 0/false 而
//! 元素已提交图结构（应答与存储分叉、重试永久锁死、AOF 漏发致主从发散）。
//! 修复契约（对标 C# 单次原生 insert 原子落盘的可观察终态，
//! garnet/libs/server/Resp/Vector/DiskANNService.cs:90 Insert、
//! garnet/libs/server/Resp/Vector/VectorManager.cs:548 TryAdd）：
//!   * 属性写失败 ⇒ 回滚摘除（图连接/五族记录/fsm 槽位清干净）⇒ 回 ERR 错误帧、
//!     不写 AOF；
//!   * 无 SETATTR 的 VADD 不发起属性写，不受属性写故障影响；
//!   * 故障恢复后重试成功且复制侧恰一条不漏发。
//!
//! 故障注入为真实失败路径：生产回调 WedbVectorStoreCallbacks（真 wkv 会话 +
//! 真盘设备）全包透传，仅在属性项（Term::Attributes）写落盘口按生产写失败
//! 同形返回 false（对照 WedbVectorStoreCallbacks::write 的
//! `blocking_wait(upsert_raw).is_ok()` 为假的失败形态）。

use std::{
  fs::remove_dir_all,
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
};

use waof::AofHeader;
use wbase::hash_slot::slot_of;
use wconf::RuntimeServerOptions;
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  GarnetAppendOnlyFile, GarnetLog, ReplayInput,
  resp::vector::{
    resp_server_session_vectors::{RespServerSessionVectors, VectorReply},
    vector_manager::{
      ERR_VECTOR_SERVICE_RESPONSE, VADD_APPEND_LOG_ARG, VectorManager, VectorManagerOptions,
    },
    vector_manager_replication::VectorAofSink,
    vector_store_callbacks::{OwnedActiveVectorSession, WedbVectorStoreCallbacks},
  },
};
use wresp::command::RespCommand;
use wtest_base::test_store_config;
use wval::SessionPrefixBuf;
use wvector::{
  Callbacks,
  store::{StoreCallbacks, TERM_BITMASK, term},
};

/// 默认会话库槽（测试键不参与定槽，统一取 (0,0) 库槽位）
const SLOT0: u16 = slot_of(0, 0);

/// 属性项写落盘口失败注入：其余读写删/RMW 全部透传生产回调（真 wkv 会话 +
/// 真设备）；armed 时对 `context 低位 == Attributes` 的写返回 false——即生产
/// [`WedbVectorStoreCallbacks::write`] IO 失败的同形返回，驱动 provider 的
/// `StoreError::Write` 真实传播链（非空 mock 假应答）。
struct AttributeWriteFault {
  inner: WedbVectorStoreCallbacks<SegmentedDevice>,
  armed: AtomicBool,
}

impl AttributeWriteFault {
  #[inline]
  fn attr_write_failed(&self, context: u64) -> bool {
    self.armed.load(Ordering::Acquire) && context & TERM_BITMASK == term::ATTRIBUTES
  }
}

impl StoreCallbacks for AttributeWriteFault {
  async fn read_multi<F>(&self, context: u64, keys: &[u8], length_hint: usize, f: F) -> bool
  where
    F: FnMut(u32, &[u8]) + Send,
  {
    self.inner.read_multi(context, keys, length_hint, f).await
  }

  async fn read<F>(&self, context: u64, key: &[u8], f: F) -> bool
  where
    F: FnMut(&[u8]) + Send,
  {
    self.inner.read(context, key, f).await
  }

  async fn write(&self, context: u64, key: &[u8], value: &[u8]) -> bool {
    if self.attr_write_failed(context) {
      return false;
    }
    self.inner.write(context, key, value).await
  }

  async fn delete(&self, context: u64, key: &[u8]) -> bool {
    self.inner.delete(context, key).await
  }

  async fn rmw<F>(&self, context: u64, key: &[u8], write_len: usize, f: F) -> bool
  where
    F: FnMut(&mut [u8]) + Send,
  {
    self.inner.rmw(context, key, write_len, f).await
  }

  async fn filter(&self, context: u64, internal_id: u32) -> bool {
    self.inner.filter(context, internal_id).await
  }

  async fn purge_context(&self, context: u64) -> bool {
    self.inner.purge_context(context).await
  }

  fn log(&self, context: u64, msg: &str) {
    self.inner.log(context, msg);
  }
}

/// 内存 AOF（无盘拓扑；与磁盘拓扑同生产入队路径）
fn memory_aof() -> Arc<GarnetAppendOnlyFile> {
  let options = RuntimeServerOptions::default();
  Arc::new(GarnetAppendOnlyFile::new(
    Arc::new(
      GarnetLog::new(
        &options,
        {
          let (_dirs, backends) = wnode_test::test_sublogs("vadd_attr_fault", 1);
          backends
        },
        None,
      )
      .expect("构造 GarnetLog"),
    ),
    &options,
    None,
  ))
}

/// FP32 4 维向量参数
fn fp32_bytes(vals: [f32; 4]) -> Vec<u8> {
  vals.iter().flat_map(|f| f.to_le_bytes()).collect()
}

/// RESP 错误帧推导公式：`-` + 载荷 + CRLF（RESP2/RESP3 错误帧同形）
fn err_frame(payload: &[u8]) -> Vec<u8> {
  let mut out = Vec::with_capacity(1 + payload.len() + 2);
  out.push(b'-');
  out.extend_from_slice(payload);
  out.extend_from_slice(b"\r\n");
  out
}

/// RESP 整数帧推导公式：`:` + 十进制 + CRLF
fn int_frame(v: i64) -> Vec<u8> {
  [b":", v.to_string().as_bytes(), b"\r\n"].concat().to_vec()
}

/// RESP3 布尔帧推导公式：`#t`/`#f` + CRLF
fn bool_frame(v: bool) -> Vec<u8> {
  if v {
    b"#t\r\n".to_vec()
  } else {
    b"#f\r\n".to_vec()
  }
}

/// 编码应答为 RESP3 帧字节
fn frame3(reply: &VectorReply) -> Vec<u8> {
  let mut out = Vec::new();
  reply.encode_resp3(&mut out);
  out
}

fn arg_refs(v: &[Vec<u8>]) -> Vec<&[u8]> {
  v.iter().map(Vec::as_slice).collect()
}

/// 编码应答为 RESP2 帧字节
fn frame2(reply: &VectorReply) -> Vec<u8> {
  let mut out = Vec::new();
  reply.encode_resp2(&mut out);
  out
}

type TestHarness = (
  tempfile::TempDir,
  Arc<WedbStore<SegmentedDevice>>,
  Arc<AttributeWriteFault>,
  Arc<GarnetAppendOnlyFile>,
  RespServerSessionVectors<AttributeWriteFault>,
);

/// 装配：真盘 wkv + 属性写故障回调 + AOF 注入端口的完整会话链路
fn harness() -> TestHarness {
  let dir = tempfile::tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("v.db")).unwrap());
  let store = Arc::new(WedbStore::open(test_store_config(), device).unwrap());
  // 回调不持会话（执行域私有化）：真会话由测试体经 OwnedActiveVectorSession
  // 绑定到当前执行域，透传臂据此取用
  let fault = Arc::new(AttributeWriteFault {
    inner: WedbVectorStoreCallbacks::new(),
    armed: AtomicBool::new(false),
  });
  let vm = Arc::new(VectorManager::new(
    VectorManagerOptions {
      is_enabled: true,
      ..Default::default()
    },
    Callbacks::new(Arc::clone(&fault)),
  ));
  let aof = memory_aof();
  vm.set_aof_sink(Arc::new(VectorAofSink::new(
    &aof,
    Arc::clone(store.current_version_atomic()),
  )));
  (dir, store, fault, aof, RespServerSessionVectors::new(vm))
}

/// 非提交帧条目计数（AOF 面观测口，同 vector_replication_replay 判据）
fn aof_records(aof: &Arc<GarnetAppendOnlyFile>) -> Vec<waof::WalRecord> {
  aof.log().commit();
  let mut records = Vec::new();
  aof.log().scan_single_with(0, 0, i64::MAX, |rec| {
    if !waof::is_commit_frame(&rec.payload) {
      records.push(rec.clone());
    }
    true
  });
  records
}

/// 解析条目的 ReplayInput（payload = 头 + key 长度前缀 + key + input）
fn parse_input(record: &waof::WalRecord) -> ReplayInput {
  let header = AofHeader::TOTAL_SIZE;
  let key_len = u32::from_le_bytes(record.payload[header..header + 4].try_into().unwrap()) as usize;
  ReplayInput::deserialize(&record.payload[header + 4 + key_len..]).expect("ReplayInput 解析")
}

/// 属性写失败 ⇒ ERR 错误帧 + 元素回滚无残留 + AOF 零入队；恢复后重试成功且
/// 复制恰一条；重复添加仍走 Duplicate（无第二分叉）。
#[compio::test]
async fn attribute_write_failure_rolls_back_and_maps_store_error() {
  let (dir, store, fault, aof, sess) = harness();
  // 本执行域绑定专用会话（回调透传臂取用面），持有至测试结束
  let _domain = OwnedActiveVectorSession::new(store.new_session().unwrap());
  let root = SessionPrefixBuf::ROOT.as_slice();

  let v1 = fp32_bytes([1.0, 0.0, 0.0, 0.0]);
  let vadd: Vec<Vec<u8>> = vec![
    b"vk".to_vec(),
    b"FP32".to_vec(),
    v1.clone(),
    b"e1".to_vec(),
    b"NOQUANT".to_vec(),
    b"SETATTR".to_vec(),
    b"attr-1".to_vec(),
  ];

  // ── 注入：属性写落盘口失败 ──
  fault.armed.store(true, Ordering::Release);
  let reply = sess
    .network_vadd(root, &arg_refs(&vadd), SLOT0, false)
    .await;
  // VADD 回 ERR 错误帧（期望字节由 RESP 错误帧公式推导）
  assert_eq!(
    frame2(&reply),
    err_frame(ERR_VECTOR_SERVICE_RESPONSE),
    "属性写失败必须回存储错误帧，不得误报 Duplicate"
  );

  // ── 回滚摘除无残留：VISMEMBER 0 / VCARD 0 / VGETATTR null / VEMB 空 ──
  assert_eq!(
    frame2(&sess.network_vismember(root, &[b"vk", b"e1"], false).await),
    int_frame(0),
    "回滚后元素不得存活"
  );
  assert_eq!(
    frame2(&sess.network_vcard(root, &[b"vk"]).await),
    int_frame(0),
    "回滚后计数不得计入已摘除元素"
  );
  assert!(
    matches!(
      sess.network_vgetattr(root, &[b"vk", b"e1"]).await,
      VectorReply::Bulk(None)
    ),
    "回滚后属性不可读（映射记录已清）"
  );
  assert!(
    matches!(
      sess.network_vemb(root, &[b"vk", b"e1"]).await,
      VectorReply::Array(items) if items.is_empty()
    ),
    "回滚后嵌入不可读（向量记录已清）"
  );

  // ── 复制面：失败应答零入队（应答与存储收敛一致，无主从发散窗口）──
  assert!(aof_records(&aof).is_empty(), "属性写失败严禁入队 VADD 条目");

  // ── 恢复故障：重试同元素 VADD 成功（未被 Duplicate 永久锁死）──
  fault.armed.store(false, Ordering::Release);
  assert_eq!(
    frame2(
      &sess
        .network_vadd(root, &arg_refs(&vadd), SLOT0, false)
        .await
    ),
    int_frame(1),
    "恢复后重试必须成功插入"
  );
  assert_eq!(
    frame2(&sess.network_vismember(root, &[b"vk", b"e1"], false).await),
    int_frame(1),
    "重试后元素存活"
  );
  match sess.network_vgetattr(root, &[b"vk", b"e1"]).await {
    VectorReply::Bulk(Some(attr)) => assert_eq!(&attr[..], b"attr-1", "重试后属性随插入落盘"),
    other => panic!("重试后属性应可读，实际 {other:?}"),
  }

  // ── 复制面恰一条 VADD 条目（失败尝试零入队 + 成功重试入队一条，不漏发）──
  let records = aof_records(&aof);
  assert_eq!(records.len(), 1, "AOF 应恰含成功重试的 1 条 VADD 条目");
  let input = parse_input(&records[0]);
  assert_eq!(input.cmd, RespCommand::Vadd);
  assert_eq!(input.arg1, VADD_APPEND_LOG_ARG);
  assert_eq!(&input.args[3], &v1[..], "条目向量参应为重试向量");
  assert_eq!(&input.args[4], b"e1", "条目元素参应为重试元素");

  // ── Duplicate 通道原样保留（一处定义：错误态未挤占既有重复应答）──
  assert_eq!(
    frame2(
      &sess
        .network_vadd(root, &arg_refs(&vadd), SLOT0, false)
        .await
    ),
    int_frame(0),
    "既有重复元素仍回 Duplicate 应答 0"
  );
  assert_eq!(aof_records(&aof).len(), 1, "重复添加幂等不入队");

  let _ = remove_dir_all(dir.path());
}

/// 无 SETATTR 的 VADD 不发起属性写（执行方案第 1 条）：属性写故障存续期间
/// 纯向量插入照常成功，证明 set_attributes 未被空属性无谓调用。
#[compio::test]
async fn vadd_without_setattr_skips_attribute_write() {
  let (dir, store, fault, _aof, sess) = harness();
  // 本执行域绑定专用会话（回调透传臂取用面），持有至测试结束
  let _domain = OwnedActiveVectorSession::new(store.new_session().unwrap());
  let root = SessionPrefixBuf::ROOT.as_slice();

  let v1 = fp32_bytes([0.0, 1.0, 0.0, 0.0]);
  let vadd: Vec<Vec<u8>> = vec![
    b"vk".to_vec(),
    b"FP32".to_vec(),
    v1,
    b"e1".to_vec(),
    b"NOQUANT".to_vec(),
  ];

  fault.armed.store(true, Ordering::Release);
  assert_eq!(
    frame2(
      &sess
        .network_vadd(root, &arg_refs(&vadd), SLOT0, false)
        .await
    ),
    int_frame(1),
    "无 SETATTR 的 VADD 不得触碰属性写落盘口"
  );
  assert_eq!(
    frame3(&sess.network_vismember(root, &[b"vk", b"e1"], true).await),
    bool_frame(true),
    "纯向量插入应存活（RESP3 布尔真帧）"
  );

  let _ = remove_dir_all(dir.path());
}
