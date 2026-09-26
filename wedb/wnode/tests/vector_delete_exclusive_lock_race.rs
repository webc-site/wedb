//! 向量集删除条带独占锁并发回归（对标
//! libs/server/Resp/Vector/VectorManager.Locking.cs:ReadForDeleteVectorIndex
//! 的删除锁协议 + libs/server/Resp/Vector/VectorManager.Cleanup.cs:
//! RunRequestCleanupTaskAsync 的「仅标记、不丢弃」契约；并发锤击原型
//! test/standalone/Garnet.test.extensions/ReadOptimizedLockTests.cs）
//!
//! 修复点：delete_vector_set / reclaim_registry_domain /
//! delete_migrated_vector_set_of 旧实现无锁摘登记表并同步丢弃索引，并发
//! VSIM/VADD 持条带共享读锁被穿透——检索打在已销毁索引上；后台
//! process_request_cleanup 无锁二次 drop_index 与 C# 契约分叉。断言：
//!   * 共享读者存续期间 delete_vector_set 必须被条带独占锁挡住，读者排空
//!     后删除才完成（确定性锁协议）；
//!   * 删除主链路单点收敛索引丢弃；后台请求清理仅标记清理中 + 投递，
//!     不动内存索引；
//!   * VSIM/VADD 与 DEL 交叠锤击：守卫存续期间检索恒成功、删除后登记
//!     原子消失，全程无撕裂、无 Panic。

use std::{
  hint::spin_loop,
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
  thread,
  time::{Duration, Instant},
};

use compio::runtime::Runtime;
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::resp::vector::{
  vector_manager::{
    VectorAddArgs, VectorManager, VectorManagerOptions, VectorManagerResult, VectorSearchOptions,
  },
  vector_manager_index::Index,
  vector_manager_locking::{CreateIndexParams, ReadIndexOutcome, registry_key},
  vector_store_callbacks::OwnedActiveVectorSession,
};
use wval::SessionPrefixBuf;
use wvector::{
  Callbacks, VectorDistanceMetricType, VectorQuantType, VectorValueType, store::StoreCallbacks,
};
use wvector_test::MemStore;

/// 真内存 KV 存储桩（基座 [`MemStore`] 转发 + filter 钩子臂；生产路径由
/// wkv 磁盘会话承接——WedbProvider 起点/元素/属性数据必须可读回，空桩会
/// 使插入链路失真）
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

fn manager() -> Arc<VectorManager<MemKvStore>> {
  Arc::new(VectorManager::new(
    VectorManagerOptions {
      is_enabled: true,
      ..Default::default()
    },
    Callbacks::new(Arc::new(MemKvStore::new())),
  ))
}

/// 执行域绑定测试装配（新契约：try_add/value_similarity 等命令臂入口
/// `assert_have_storage_session`，生产由 `StoreGarnetApi::exec` 每命令包
/// 同步段守卫兜底）。直调用例按仓内唯一机制自持：真 wkv 会话经
/// [`OwnedActiveVectorSession`] 后台自持形态绑定（同 resp_vector_set 夹具
/// 与 `sync_transport::export_vector_set_elements` 口径）。绑定是线程局部
/// （对标 C# `[ThreadStatic]`），多线程锤击用例每个触达断言臂的线程各自
/// 经共享 store 自持一份。
fn race_store() -> (tempfile::TempDir, Arc<WedbStore<SegmentedDevice>>) {
  let dir = tempfile::tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("bind.db")).unwrap());
  let store = Arc::new(WedbStore::open(wtest_base::test_store_config(), device).unwrap());
  (dir, store)
}

fn bind_domain(
  store: &Arc<WedbStore<SegmentedDevice>>,
) -> OwnedActiveVectorSession<SegmentedDevice> {
  OwnedActiveVectorSession::new(store.new_session().unwrap())
}

fn create_params() -> CreateIndexParams {
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

/// 建集 + 预插 `count` 个 2 维 FP32 元素，返回登记记录字节。
///
/// 全程持共享守卫（生产 VADD 语义：锁协议读命中后持守卫插入），插入
/// 原子完成，销毁者被条带独占锁挡在门外。
async fn seed(mgr: &VectorManager<MemKvStore>, root: &[u8], key: &[u8], count: u32) -> [u8; 56] {
  let (index, guard) = mgr
    .read_or_create_vector_index(root, key, Some(&create_params()))
    .await
    .unwrap();
  let bytes = index.to_bytes();
  for e in 0..count {
    let mut values = [0u8; 8];
    values[..4].copy_from_slice(&(e as f32).to_le_bytes());
    values[4..].copy_from_slice(&1.0f32.to_le_bytes());
    let id = e.to_le_bytes();
    let args = VectorAddArgs::new(&id, VectorValueType::FP32, &values, b"");
    // 直调 `.await` 闭环（调用方自持运行时）；守卫存续窗口是读命中持锁
    // 插入的被测锁协议语义，异步锁守卫跨 await 合法
    let res = mgr.try_add(root, key, &bytes, &args).await;
    assert_eq!(
      res,
      Ok(VectorManagerResult::OK),
      "预插元素 {e} 必须成功: res={res:?}, context={}",
      index.context
    );
  }
  // 预插完成直接返回 index 字节（创建时已采样且全程持守卫不变）；
  // 落守卫后再从 mgr 读与高频并发删除竞争，无谓增加 None unwrap 风险
  drop(guard);
  bytes
}

/// 检索探针参数（count=4，无过滤）与探针向量
fn probe_opts() -> VectorSearchOptions<'static> {
  VectorSearchOptions {
    count: 4,
    ..Default::default()
  }
}

fn probe_vec() -> Vec<u8> {
  [0f32, 1.0].iter().flat_map(|f| f.to_le_bytes()).collect()
}

/// 确定性锁协议：共享读者存续期间 delete_vector_set 必须被条带独占锁
/// 挡住（登记不得被摘、索引不得被丢弃），读者排空后删除才完成。
/// 旧实现无锁直删，此窗口内即穿透。
#[test]
fn shared_reader_blocks_delete_until_drained() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (_dir, store) = race_store();
    let _bound = bind_domain(&store);
    let mgr = manager();
    let root = SessionPrefixBuf::ROOT.as_slice();
    let key = b"del-lock-block";

    let index_bytes = seed(&mgr, root, key, 1).await;
    let context = Index::from_bytes(&index_bytes).unwrap().context;
    assert_ne!(mgr.service.card(context), 0);

    // 持条带共享读锁（模拟在途 VSIM/VADD；异步锁阻塞获取在竞争时挂起本任务）
    let rk = registry_key(root, key);
    let shared = mgr.vector_set_locks.acquire_shared(rk.as_slice()).await;

    // 删除线程：应被共享读者挡在独占锁外（自持运行时逐次 block_on，
    // 异步锁等待由事件监听器跨线程唤醒）
    let deleted = Arc::new(AtomicBool::new(false));
    let d_mgr = Arc::clone(&mgr);
    let d_flag = Arc::clone(&deleted);
    let deleter = thread::spawn(move || {
      let rt = Runtime::new().unwrap();
      if rt.block_on(d_mgr.delete_vector_set(root, key)) {
        d_flag.store(true, Ordering::Release);
      }
    });

    // 200ms 观察窗：旧实现微秒级穿透，此窗内删除必须零进展
    let deadline = Instant::now() + Duration::from_millis(200);
    while Instant::now() < deadline {
      assert!(
        !deleted.load(Ordering::Acquire),
        "共享读者存续期间删除不得穿透条带独占锁"
      );
      assert!(
        mgr.read_stored_index(root, key).is_some(),
        "读者存续期间登记表不得被摘除"
      );
      thread::sleep(Duration::from_millis(10));
    }
    assert_ne!(
      mgr.service.card(context),
      0,
      "读者存续期间内存索引不得被丢弃"
    );

    // 排空读者：删除立即推进完成，登记原子消失
    drop(shared);
    deleter.join().unwrap();
    assert!(deleted.load(Ordering::Acquire), "读者排空后删除必须完成");
    assert!(
      mgr.read_stored_index(root, key).is_none(),
      "删除后登记原子消失"
    );
  });
}

/// 丢弃单点收敛：ptr=0 桩记录删除时主链路依约不丢弃（DropIndex 的
/// 「从未拉起，无可丢弃」分支），后台 process_request_cleanup 仅标记
/// 清理中 + 投递主清理通道，不得无锁二次丢弃内存索引（C#
/// RunRequestCleanupTaskAsync 契约；旧实现此处无条件强丢，存在砸中
/// 复用上下文的竞态）。
#[compio::test]
async fn request_cleanup_marks_without_dropping_index() {
  let (_dir, store) = race_store();
  let _bound = bind_domain(&store);
  let mgr = manager();
  let root = SessionPrefixBuf::ROOT.as_slice();
  let key = b"del-lock-cleanup";

  let index_bytes = seed(&mgr, root, key, 1).await;
  let context = Index::from_bytes(&index_bytes).unwrap().context;

  // ptr=0 桩（清指针语义：管理器不再拥有该索引实例）
  let mut stub = Index::from_bytes(&index_bytes).unwrap();
  stub.index_ptr = 0;
  mgr.write_stored_index(root, key, &stub.to_bytes()).await;

  assert!(
    mgr.delete_vector_set(root, key).await,
    "ptr=0 桩删除照常摘表"
  );
  assert!(mgr.read_stored_index(root, key).is_none());
  let card_before = mgr.service.card(context);
  assert!(card_before > 0, "ptr=0 记录主链路依约不丢弃，内存索引存活");

  // 后台请求清理：仅标记 + 投递，内存索引不动
  mgr.process_request_cleanup(context).await;
  assert_eq!(
    mgr.service.card(context),
    card_before,
    "后台请求清理不得丢弃内存索引（丢弃由持独占锁的主删除链路单点收敛）"
  );
  assert!(is_cleaning_up(&mgr, context), "清理中标记照常收敛");
}

/// VSIM/VADD 与 DEL 交叠锤击：worker 经锁协议读命中持共享守卫，守卫
/// 存续期间检索必须成功（删除被条带独占锁挡住，索引不可能被销毁）；
/// 删除线程走内置独占锁的 delete_vector_set，删除后登记原子消失。
/// 旧实现删除穿透读锁即在此显形（守卫窗口内检索报错 = 索引被并发销毁）。
#[test]
fn search_add_overlap_delete_never_tears() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (_dir, store) = race_store();
    let _bound = bind_domain(&store);
    let mgr = manager();
    let root = SessionPrefixBuf::ROOT.as_slice();
    let key = b"del-lock-race";
    let stop = Arc::new(AtomicBool::new(false));

    // 销毁者：高频删除（delete_vector_set 自带条带独占锁；自持运行时
    // 逐次 block_on，异步锁等待由事件监听器跨线程唤醒）
    let d_mgr = Arc::clone(&mgr);
    let d_stop = Arc::clone(&stop);
    let destroyer = thread::spawn(move || {
      let rt = Runtime::new().unwrap();
      while !d_stop.load(Ordering::Acquire) {
        let _ = rt.block_on(d_mgr.delete_vector_set(root, key));
        spin_loop();
      }
    });

    let probe = probe_vec();
    let opts = probe_opts();
    for _ in 0..24u32 {
      // 确保本轮重建前上一轮若未被销毁者摘除则清理干净，杜绝残留旧集合导致预插 Duplicate
      let _ = mgr.delete_vector_set(root, key).await;
      // 重建集合（销毁者已摘除登记 → 缺失臂新建 context）+ 预插 8 元素
      seed(&mgr, root, key, 8).await;
      // 交叠 worker：守卫存续期间高频检索 + 插入，任何 Err 即撕裂证据。
      // 各 worker 重插不同的既有元素（同 id 并发插入在 diskann 图层非原子，
      // 不属本票锁协议面）
      let mut workers = Vec::new();
      for w in 0..2u32 {
        let w_mgr = Arc::clone(&mgr);
        let w_probe = probe.clone();
        let w_opts = opts;
        let dup_element = w.to_le_bytes();
        // worker 线程各持一份自持绑定（线程局域执行域，跨线程不复用）
        let w_store = Arc::clone(&store);
        workers.push(thread::spawn(move || {
          let _w_bound = bind_domain(&w_store);
          let rt = Runtime::new().unwrap();
          rt.block_on(async {
            for _ in 0..32 {
              match w_mgr.read_vector_index_core(root, key, false).await {
                ReadIndexOutcome::Hit(index, guard) => {
                  let ctx = index.context;
                  if let Err(e) = w_mgr
                    .value_similarity(
                      index.to_bytes().as_slice(),
                      VectorValueType::FP32,
                      &w_probe,
                      &w_opts,
                    )
                    .await
                  {
                    panic!(
                      "守卫存续期间检索撕裂: context={ctx} card={} 登记存在={} err={e:?}",
                      w_mgr.service.card(ctx),
                      w_mgr.read_stored_index(root, key).is_some()
                    );
                  }
                  // 持守卫插入既有元素：Duplicate 幂等（真实写路径并发面）；
                  // 异步锁守卫跨 await 合法（compio 任务不迁线程）
                  let dup_res = w_mgr
                    .try_add(
                      root,
                      key,
                      index.to_bytes().as_slice(),
                      &VectorAddArgs::new(&dup_element, VectorValueType::FP32, &w_probe, b""),
                    )
                    .await;
                  assert!(matches!(dup_res, Ok(VectorManagerResult::Duplicate)));
                  drop(guard);
                }
                // 登记被删（本轮销毁者抢先把集合删除）：跳过，等待下一轮重建
                ReadIndexOutcome::NotFound | ReadIndexOutcome::Failed => {}
                ReadIndexOutcome::WouldBlock => spin_loop(),
              }
            }
          });
        }));
      }
      for w in workers {
        w.join().unwrap();
      }
    }

    stop.store(true, Ordering::Release);
    destroyer.join().unwrap();
  });
}

/// 上下文清理中标记断言单点
fn is_cleaning_up(mgr: &VectorManager<MemKvStore>, context: u64) -> bool {
  let (ci, cv) = VectorManager::<MemKvStore>::decompose_context(context);
  let metas = mgr.context_metadatas.lock();
  metas[ci].is_cleaning_up(ci != 0, cv)
}
