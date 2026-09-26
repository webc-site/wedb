//! VADD 键锁域 × DEL 交错回归（票：wnode-vadd-lock-drop-before-try-add）
//!
//! 缺陷形态：`network_vadd_slow` 在 `read_or_create_vector_index` 返回后、
//! `try_add` 之前显式 `drop(lock)`，共享守卫被截短——并发 DEL 的条带独占
//! 锁（C# VectorManager.Locking.cs:ReadForDeleteVectorIndex 对偶
//! `delete_vector_set`）可在 try_add 窗口内穿透完成全链删除（drop_index
//! 摘 service 注册表 + 摘登记表），VADD 的 insert 打在已弃 context 上：
//! miss 即 service.rs `index(context)` 回 False 被 try_add 折成 Duplicate
//! （客户端收伪 :0），交错在 Arc 保活窗口则续写孤儿记录且 AOF 条目序与
//! 主端登记表终态发散。
//! 修复契约（对标 garnet/libs/server/Storage/Session/MainStore/
//! VectorStoreOps.cs:192 `using (ReadOrCreateVectorIndex)` 罩 TryAdd 与
//! OK 后 ReplicateVectorSetAdd 全程）：守卫随 VADD 的 async 栈帧存活至
//! 函数返回，删除被排挡至写体与 AOF 注入完成之后，终态恒为闭合结局
//! 「VADD :1 后 DEL 生效」。
//!
//! 交错构造为真实竞态而非假桩：存储回调在 try_add 写落盘口让位挂起
//!（async_lock 门闸，VADD 此刻已持条带共享守卫），删除线程此刻发起
//! `delete_vector_set`——旧实现该窗口内删除穿透完成、VADD 回伪
//! Duplicate；新实现删除必须恒被排挡至 VADD 函数返回。

use std::{
  fs::remove_dir_all,
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
  thread,
  time::{Duration, Instant},
};

use async_lock::Mutex as AsyncMutex;
use compio::runtime::Runtime;
use waof::AofHeader;
use wbase::hash_slot::slot_of;
use wconf::RuntimeServerOptions;
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  GarnetAppendOnlyFile, GarnetLog, ReplayInput,
  resp::vector::{
    resp_server_session_vectors::{RespServerSessionVectors, VectorReply},
    vector_manager::{VADD_APPEND_LOG_ARG, VectorManager, VectorManagerOptions},
    vector_manager_index::Index,
    vector_manager_replication::VectorAofSink,
    vector_store_callbacks::OwnedActiveVectorSession,
  },
};
use wresp::command::RespCommand;
use wtest_base::test_store_config;
use wval::SessionPrefixBuf;
use wvector::{Callbacks, store::StoreCallbacks};
use wvector_test::MemStore;

/// 默认会话库槽（测试键不参与定槽，统一取 (0,0) 库槽位）
const SLOT0: u16 = slot_of(0, 0);

/// 真内存 KV 存储桩（基座 [`MemStore`] 转发 + filter 钩子臂；生产路径由
/// wkv 磁盘会话承接——起点/元素/属性数据必须可读回，空桩会使插入链路
/// 失真）
struct MemKvStore {
  base: MemStore,
}

impl MemKvStore {
  fn new() -> Self {
    Self {
      base: MemStore::new(),
    }
  }
}

impl StoreCallbacks for MemKvStore {
  /// 钩子臂：本夹具检索不携 FILTER 表达式，按生产回调「无过滤上下文回落
  /// 放行 true」臂恒真（基座为恒假）。
  async fn filter(&self, _context: u64, _internal_id: u32) -> bool {
    true
  }

  async fn read_multi<F>(&self, context: u64, keys: &[u8], length_hint: usize, f: F) -> bool
  where
    F: FnMut(u32, &[u8]) + Send,
  {
    self.base.read_multi(context, keys, length_hint, f).await
  }

  async fn read<F>(&self, context: u64, key: &[u8], f: F) -> bool
  where
    F: FnMut(&[u8]) + Send,
  {
    self.base.read(context, key, f).await
  }

  async fn write(&self, context: u64, key: &[u8], value: &[u8]) -> bool {
    self.base.write(context, key, value).await
  }

  async fn delete(&self, context: u64, key: &[u8]) -> bool {
    self.base.delete(context, key).await
  }

  async fn rmw<F>(&self, context: u64, key: &[u8], write_len: usize, f: F) -> bool
  where
    F: FnMut(&mut [u8]) + Send,
  {
    self.base.rmw(context, key, write_len, f).await
  }

  async fn purge_context(&self, context: u64) -> bool {
    self.base.purge_context(context).await
  }

  fn log(&self, context: u64, msg: &str) {
    self.base.log(context, msg);
  }
}

/// try_add 写落盘口让位门闸：armed 期间首个写调用（元素记录落盘）置位
/// parked 后挂起在异步门闩上——真实存储 await 让位点，VADD 此刻已持
/// 条带共享守卫（read_or_create_vector_index 交还），旧实现则已在
/// try_add 前 drop 守卫。门闸取放均在 map 锁之外，观测线程零阻塞。
struct GateStore {
  inner: MemKvStore,
  gate: AsyncMutex<()>,
  armed: AtomicBool,
  consumed: AtomicBool,
  parked: AtomicBool,
}

impl GateStore {
  fn new() -> Self {
    Self {
      inner: MemKvStore::new(),
      gate: AsyncMutex::new(()),
      armed: AtomicBool::new(false),
      consumed: AtomicBool::new(false),
      parked: AtomicBool::new(false),
    }
  }

  /// 门闸命中判定：armed 且未被先前写消费 ⇒ 本次写为交错让位点
  async fn park_once(&self) {
    if !self.armed.load(Ordering::Acquire) || self.consumed.swap(true, Ordering::AcqRel) {
      return;
    }
    self.parked.store(true, Ordering::Release);
    self.gate.lock().await;
  }
}

impl StoreCallbacks for GateStore {
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
    self.park_once().await;
    self.inner.write(context, key, value).await
  }

  async fn delete(&self, context: u64, key: &[u8]) -> bool {
    self.inner.delete(context, key).await
  }

  async fn rmw<F>(&self, context: u64, key: &[u8], write_len: usize, f: F) -> bool
  where
    F: FnMut(&mut [u8]) + Send,
  {
    if write_len > 0 {
      self.park_once().await;
    }
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
          let (_dirs, backends) = wnode_test::test_sublogs("vadd_guard_interleave", 1);
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

/// FP32 2 维向量参数
fn fp32(x: f32, y: f32) -> Vec<u8> {
  [x, y].iter().flat_map(|f| f.to_le_bytes()).collect()
}

fn vadd_args(element: &[u8], values: &[u8]) -> Vec<Vec<u8>> {
  vec![
    b"vk".to_vec(),
    b"FP32".to_vec(),
    values.to_vec(),
    element.to_vec(),
    b"NOQUANT".to_vec(),
  ]
}

fn arg_refs(v: &[Vec<u8>]) -> Vec<&[u8]> {
  v.iter().map(Vec::as_slice).collect()
}

/// 非提交帧条目计数（AOF 面观测口，同 vadd_attribute_write_failure 判据）
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

/// 本 context 域（低位 term 剥除）物理记录计数（孤儿观测口）
fn context_record_count(gate: &GateStore, base: u64) -> usize {
  gate
    .inner
    .base
    .data
    .lock()
    .iter()
    .filter(|((ctx, _), _)| *ctx & !0b111 == base)
    .count()
}

/// VADD 写体挂起窗口 × DEL 交叠：断言删除被 VADD 持守卫排挡至写体与 AOF
/// 注入完成之后，终态闭合为「VADD :1 后 DEL 生效」——VADD 恒回真 1（杜绝
/// service.insert miss 折 Duplicate 伪 :0）、AOF 恰两条 VADD 条目且交错条目
/// 先于删除生效入队（副本重放无 DEL、VADD 倒序发散）、删除完成后登记表
/// 原子消失、context 物理记录经清理链全数 purge 无孤儿。
#[test]
fn vadd_guard_spans_try_add_blocks_delete_interleave() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    // ── 装配：真盘 wkv 会话绑定域 + 门闸存储回调 + AOF 注入端口 ──
    let dir = tempfile::tempdir().unwrap();
    let device = Arc::new(SegmentedDevice::single_file(dir.path().join("vadd_del.db")).unwrap());
    let store = Arc::new(WedbStore::open(test_store_config(), device).unwrap());
    let _bound = OwnedActiveVectorSession::new(store.new_session().unwrap());
    let gate = Arc::new(GateStore::new());
    let vm = Arc::new(VectorManager::new(
      VectorManagerOptions {
        is_enabled: true,
        ..Default::default()
      },
      Callbacks::new(Arc::clone(&gate)),
    ));
    let aof = memory_aof();
    vm.set_aof_sink(Arc::new(VectorAofSink::new(
      &aof,
      Arc::clone(store.current_version_atomic()),
    )));
    let sess = RespServerSessionVectors::new(Arc::clone(&vm));
    let root = SessionPrefixBuf::ROOT.as_slice();
    let key = b"vk";

    // ── 种子：常规 VADD 建集并入 e1（AOF 面恰一条）──
    let seed = vadd_args(b"e1", &fp32(1.0, 0.0));
    assert!(
      matches!(
        sess
          .network_vadd(root, &arg_refs(&seed), SLOT0, false)
          .await,
        VectorReply::Integer(1)
      ),
      "种子 VADD 必须成功"
    );
    assert_eq!(aof_records(&aof).len(), 1, "种子 VADD 入队一条");
    let context = Index::from_bytes(&vm.read_stored_index(root, key).unwrap())
      .unwrap()
      .context;
    let base = context & !0b111;

    // 门闸武装：VADD e2 的 try_add 首个写落盘调用即让位挂起
    gate.armed.store(true, Ordering::Release);
    let hold = gate.gate.lock().await;

    // ── VADD 线程：持守卫进入 try_add，写落盘口 park 在门闸上 ──
    let v_mgr = Arc::clone(&vm);
    let v_gate = Arc::clone(&gate);
    let v_store = Arc::clone(&store);
    let vadd_thread = thread::spawn(move || {
      let _domain = OwnedActiveVectorSession::new(v_store.new_session().unwrap());
      let sess = RespServerSessionVectors::new(v_mgr);
      let args = vadd_args(b"e2", &fp32(0.0, 1.0));
      let rt = Runtime::new().unwrap();
      rt.block_on(async move {
        match sess
          .network_vadd(root, &arg_refs(&args), SLOT0, false)
          .await
        {
          VectorReply::Integer(v) => v,
          other => panic!("VADD 应答形态异常: {other:?}"),
        }
      })
    });

    // 等待让位点命中（try_add 写落盘口；VADD 此刻已持条带共享守卫）
    let deadline = Instant::now() + Duration::from_secs(5);
    while !v_gate.parked.load(Ordering::Acquire) {
      assert!(Instant::now() < deadline, "VADD 未在 try_add 写落盘口让位");
      thread::sleep(Duration::from_millis(2));
    }

    // ── 删除线程：DEL 插队（delete_vector_set = ReadForDeleteVectorIndex
    // 排他删除单点，同 vector_delete_exclusive_lock_race 销毁者形态）──
    let d_mgr = Arc::clone(&vm);
    let del_done = Arc::new(AtomicBool::new(false));
    let d_flag = Arc::clone(&del_done);
    let del_thread = thread::spawn(move || {
      let rt = Runtime::new().unwrap();
      if rt.block_on(d_mgr.delete_vector_set(root, key)) {
        d_flag.store(true, Ordering::Release);
      }
    });

    // ── 交错窗口观测：删除必须被排挡（旧实现此窗内已穿透完成，随后
    // VADD insert miss 回伪 Duplicate :0）──
    let window = Instant::now() + Duration::from_millis(200);
    while Instant::now() < window {
      assert!(
        !del_done.load(Ordering::Acquire),
        "VADD 写体存续期间删除不得穿透条带独占锁（锁域分叉回归）"
      );
      assert!(
        vm.read_stored_index(root, key).is_some(),
        "VADD 写体存续期间登记表不得被摘除"
      );
      thread::sleep(Duration::from_millis(5));
    }

    // ── 放行门闸：VADD 完成写体与 AOF 注入后释放守卫，删除随即落位 ──
    drop(hold);
    let reply = vadd_thread.join().unwrap();
    assert_eq!(reply, 1, "VADD 必须回真 1，杜绝 miss 折 Duplicate 伪 :0");
    del_thread.join().unwrap();
    assert!(
      del_done.load(Ordering::Acquire),
      "VADD 收口后删除必须完成（闭合结局一：VADD :1 后 DEL 生效）"
    );

    // ── 终态一致性：登记表原子消失；AOF 恰两条 VADD 条目（种子 + 交错），
    // 交错条目先于删除生效入队 ⇒ 副本重放序 VADD、VADD、DEL 与主端键空间
    // 终态（键已删）一致 ──
    assert!(
      vm.read_stored_index(root, key).is_none(),
      "删除后登记原子消失"
    );
    let records = aof_records(&aof);
    assert_eq!(records.len(), 2, "AOF 应恰有种子与交错两条 VADD 条目");
    for record in &records {
      let input = parse_input(record);
      assert_eq!(input.cmd, RespCommand::Vadd);
      assert_eq!(input.arg1, VADD_APPEND_LOG_ARG);
    }
    assert_eq!(
      &parse_input(&records[1]).args[4],
      b"e2",
      "后入队条目应为交错 VADD（先于删除生效）"
    );

    // ── 清理链闭合：context 物理记录全数 purge，无孤儿写 ──
    assert_eq!(vm.service.card(context), 0, "删除后内存索引不得存活");
    let leaked = context_record_count(&gate, base);
    assert!(leaked > 0, "删除前该 context 域应留有元素物理记录");
    vm.process_request_cleanup(context).await;
    vm.process_cleanup(context).await;
    assert_eq!(
      context_record_count(&gate, base),
      0,
      "清理链后该 context 物理记录必须全数 purge（孤儿={leaked}）"
    );

    let _ = remove_dir_all(dir.path());
  });
}

/// 删除先行 × VADD 闭合结局二观测：DEL 先行完成后 VADD 走
/// read_or_create_vector_index 缺失臂重建，恒回真 1（重建非伪 Duplicate
/// 通道），登记表与 AOF 序一致收敛。
#[test]
fn delete_first_then_vadd_recreates_closed() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let dir = tempfile::tempdir().unwrap();
    let device = Arc::new(SegmentedDevice::single_file(dir.path().join("vadd_del2.db")).unwrap());
    let store = Arc::new(WedbStore::open(test_store_config(), device).unwrap());
    let _bound = OwnedActiveVectorSession::new(store.new_session().unwrap());
    let gate = Arc::new(GateStore::new());
    let vm = Arc::new(VectorManager::new(
      VectorManagerOptions {
        is_enabled: true,
        ..Default::default()
      },
      Callbacks::new(Arc::clone(&gate)),
    ));
    let aof = memory_aof();
    vm.set_aof_sink(Arc::new(VectorAofSink::new(
      &aof,
      Arc::clone(store.current_version_atomic()),
    )));
    let sess = RespServerSessionVectors::new(Arc::clone(&vm));
    let root = SessionPrefixBuf::ROOT.as_slice();
    let key = b"vk";

    let seed = vadd_args(b"e1", &fp32(1.0, 0.0));
    assert!(
      matches!(
        sess
          .network_vadd(root, &arg_refs(&seed), SLOT0, false)
          .await,
        VectorReply::Integer(1)
      ),
      "种子 VADD 必须成功"
    );
    // DEL 先行完整收口（无交错窗口）
    assert!(vm.delete_vector_set(root, key).await, "种子集删除必须命中");
    assert!(vm.read_stored_index(root, key).is_none());

    // VADD 重建臂：缺失 → read_or_create 新建 + 插入，恒回真 1
    let rebuild = vadd_args(b"e2", &fp32(0.0, 1.0));
    assert!(
      matches!(
        sess
          .network_vadd(root, &arg_refs(&rebuild), SLOT0, false)
          .await,
        VectorReply::Integer(1)
      ),
      "DEL 先行后 VADD 必须走重建臂回真 1（重建非伪 Duplicate 通道）"
    );
    let records = aof_records(&aof);
    assert_eq!(records.len(), 2, "AOF 面种子与重建各入队一条 VADD");
    assert_eq!(&parse_input(&records[1]).args[4], b"e2");
    assert!(
      vm.read_stored_index(root, key).is_some(),
      "重建后键空间存活"
    );

    let _ = remove_dir_all(dir.path());
  });
}
