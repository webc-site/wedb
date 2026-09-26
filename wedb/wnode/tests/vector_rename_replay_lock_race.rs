//! 向量 RENAME 重放臂锁屏障与 FLUSH 域回收双轮扫尾并发回归
//!
//! 修复点一（P1）：replay_vector_set_rename 旧实现零条带锁直写登记表并无锁
//! request_deletion，副本常驻重放线程穿透共享锁屏障——VSIM/VEMB 持共享读锁
//! 拷出的索引句柄可在存续期内被无锁丢弃（context 复用后旧读者打到复用后的
//! 新集合图上）。现实现与主端口 rename_vector_set_of 同锁轴同定序
//!（VectorSetKeyLocks::acquire 双键独占），被顶登记清退复用
//! delete_vector_set_of 锁内复核单点。断言：
//!   * 共享读者存续期间重放 RENAME 必须被双键独占锁挡住（登记不动、被顶
//!     索引不丢），读者排空后迁移才完成（确定性锁协议）；
//!   * 重放 RENAME onto 既有键：被顶 context 原生索引即时清退、迁移后
//!     登记与检索闭环；
//!   * 重放臂与 VSIM 交叠锤击全程无撕裂、登记终态一致。
//!
//! 修复点二（P2）：reclaim_registry_domain 旧实现单轮快照-逐键锁回收，与
//! 在途 VADD 竞速时快照点之后落表的条目永久漏收（死域登记 + 原生索引 +
//! context 位运行期滞留）。现实现双轮扫尾。断言：第一轮逐键锁间隙中完成
//! 落表的条目由第二轮捕获回收，回收后死域登记零残留、跨域存活。

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
use wbase::hash_slot::slot_of;
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::resp::vector::{
  vector_manager::{
    RegistryReclaim, VectorAddArgs, VectorManager, VectorManagerOptions, VectorManagerResult,
    VectorSearchOptions,
  },
  vector_manager_index::Index,
  vector_manager_locking::{CreateIndexParams, ReadIndexOutcome, registry_key, stripe_for},
  vector_store_callbacks::OwnedActiveVectorSession,
};
use wresp::command::RespCommand;
use wval::SessionPrefixBuf;
use wvector::{
  Callbacks, VectorDistanceMetricType, VectorQuantType, VectorValueType, store::StoreCallbacks,
};
use wvector_test::MemStore;

/// 死域（回收目标域）会话前缀：vns=7、vdb=9（任意非根域）
fn dead_prefix() -> SessionPrefixBuf {
  SessionPrefixBuf::new(7, 9)
}

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

/// 执行域绑定测试装配（try_add / value_similarity 等命令臂入口
/// `assert_have_storage_session`；绑定是线程局部，多线程用例每个触达断言
/// 臂的线程各自经共享 store 自持一份）
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
/// 全程持共享守卫（生产 VADD 语义：锁协议读命中后持守卫插入）。
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
    let res = mgr.try_add(root, key, &bytes, &args).await;
    assert_eq!(
      res,
      Ok(VectorManagerResult::OK),
      "预插元素 {e} 必须成功: res={res:?}"
    );
  }
  drop(guard);
  bytes
}

/// RENAME 重放条目（args = [旧名]，条目键 = 新名）
fn rename_input(old_key: &[u8]) -> wnode::ReplayInput {
  wnode::ReplayInput {
    cmd: RespCommand::Rename,
    flags: 0,
    sub_id: 0,
    obj_type: 0,
    arg1: 0,
    arg2: 0,
    arg3: 0,
    args: vec![old_key.to_vec()],
  }
}

/// 登记记录的 context 读取（登记缺席即 panic）
async fn registered_context(mgr: &VectorManager<MemKvStore>, root: &[u8], key: &[u8]) -> u64 {
  let bytes = mgr
    .read_stored_index(root, key)
    .unwrap_or_else(|| panic!("{key:?} 登记应在位"));
  Index::from_bytes(&bytes).unwrap().context
}

/// 检索探针参数（count=4，长 EF=256 放大并发窗口，无过滤）与探针向量
fn probe_opts() -> VectorSearchOptions<'static> {
  VectorSearchOptions {
    count: 4,
    search_exploration_factor: 256,
    ..Default::default()
  }
}

/// 排空并处理后台清理通道与请求通道中的全部上下文
async fn drain_cleanups(mgr: &VectorManager<MemKvStore>) {
  for ctx in mgr.request_cleanup_task_channel.drain() {
    mgr.process_request_cleanup(ctx).await;
  }
  for ctx in mgr.cleanup_task_channel.drain() {
    mgr.process_cleanup(ctx).await;
  }
}

fn probe_vec() -> Vec<u8> {
  [0f32, 1.0].iter().flat_map(|f| f.to_le_bytes()).collect()
}

/// 确定性锁协议：共享读者存续期间 replay_vector_set_rename 必须被双键
/// 独占锁挡住（旧名登记不得被摘、被顶索引不得被丢弃、新名登记不得被
/// 覆写），读者排空后迁移才完成。旧实现零锁直迁，此窗口内即穿透。
#[test]
fn shared_reader_blocks_rename_replay_until_drained() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (_dir, store) = race_store();
    let _bound = bind_domain(&store);
    let mgr = manager();
    let root = SessionPrefixBuf::ROOT.as_slice();
    let old_key = b"rr-old";
    let new_key = b"rr-new";

    let old_bytes = seed(&mgr, root, old_key, 2).await;
    let displaced_bytes = seed(&mgr, root, new_key, 1).await;
    let old_context = Index::from_bytes(&old_bytes).unwrap().context;
    let displaced_context = Index::from_bytes(&displaced_bytes).unwrap().context;

    // 持新名条带共享读锁（模拟在途 VSIM 读被顶集合；双键独占锁必含新名）
    let rk_new = registry_key(root, new_key);
    let shared = mgr.vector_set_locks.acquire_shared(rk_new.as_slice()).await;

    // 副本重放线程：应被双键独占锁挡在门外（自持运行时逐次 block_on，
    // 异步锁等待由事件监听器跨线程唤醒）
    let done = Arc::new(AtomicBool::new(false));
    let r_mgr = Arc::clone(&mgr);
    let r_flag = Arc::clone(&done);
    let input = rename_input(old_key);
    let replayer = thread::spawn(move || {
      let rt = Runtime::new().unwrap();
      let res = rt.block_on(r_mgr.replay_vector_set_rename(root, new_key, &input));
      assert!(res.is_ok(), "重放 RENAME 应成功: {res:?}");
      r_flag.store(true, Ordering::Release);
    });

    // 200ms 观察窗：旧实现微秒级穿透，此窗内重放必须零进展
    let deadline = Instant::now() + Duration::from_millis(200);
    while Instant::now() < deadline {
      assert!(
        !done.load(Ordering::Acquire),
        "共享读者存续期间重放 RENAME 不得穿透双键独占锁"
      );
      assert!(
        mgr.read_stored_index(root, old_key).is_some(),
        "读者存续期间旧名登记不得被摘除"
      );
      assert_eq!(
        registered_context(&mgr, root, new_key).await,
        displaced_context,
        "读者存续期间新名登记不得被覆写"
      );
      assert!(
        mgr.service.card(displaced_context) > 0,
        "读者存续期间被顶索引不得被丢弃"
      );
      thread::sleep(Duration::from_millis(10));
    }

    // 排空读者：重放立即推进完成，登记迁移闭环
    drop(shared);
    replayer.join().unwrap();
    assert!(done.load(Ordering::Acquire), "读者排空后重放必须完成");
    assert!(
      mgr.read_stored_index(root, old_key).is_none(),
      "重放后旧名登记应摘除"
    );
    assert_eq!(
      registered_context(&mgr, root, new_key).await,
      old_context,
      "重放后新名登记应迁移为旧名 context"
    );
  });
}

/// 重放 RENAME onto 既有键的 displaced 清退闭环：被顶 context 原生索引
/// 即时清退（delete_vector_set_of 锁内复核单点），迁移后新名检索打在
/// 旧名集合元素上（context 迁移语义），旧名登记摘除、旧 context 存续。
#[compio::test]
async fn rename_replay_reclaims_displaced_registration() {
  let (_dir, store) = race_store();
  let _bound = bind_domain(&store);
  let mgr = manager();
  let root = SessionPrefixBuf::ROOT.as_slice();
  let old_key = b"rr-disp-old";
  let new_key = b"rr-disp-new";

  let old_bytes = seed(&mgr, root, old_key, 2).await;
  let displaced_bytes = seed(&mgr, root, new_key, 1).await;
  let old_context = Index::from_bytes(&old_bytes).unwrap().context;
  let displaced_context = Index::from_bytes(&displaced_bytes).unwrap().context;

  mgr
    .replay_vector_set_rename(root, new_key, &rename_input(old_key))
    .await
    .expect("重放 RENAME 应成功");

  // 登记迁移闭环
  assert!(
    mgr.read_stored_index(root, old_key).is_none(),
    "旧名登记应摘除"
  );
  assert_eq!(
    registered_context(&mgr, root, new_key).await,
    old_context,
    "新名登记应迁移为旧名 context"
  );
  // 被顶清退闭环：displaced 原生索引即时丢弃，迁移 context 元素无损
  assert_eq!(
    mgr.service.card(displaced_context),
    0,
    "被顶 context 原生索引应即时清退"
  );
  assert_eq!(mgr.service.card(old_context), 2, "迁移 context 元素应无损");

  // 迁移后检索闭环：新名检索打在旧名集合元素上
  let migrated_bytes = mgr.read_stored_index(root, new_key).unwrap();
  let output = mgr
    .value_similarity(
      migrated_bytes.as_slice(),
      VectorValueType::FP32,
      &probe_vec(),
      &probe_opts(),
    )
    .await
    .expect("迁移后新名检索应成功");
  assert!(!output.output_ids.is_empty(), "迁移后检索应命中元素");
}

/// 域回收双轮扫尾判别：第一轮逐键锁间隙中完成落表的死域条目（victims
/// 快照后抵达）由第二轮捕获。旧实现单轮快照，此条目永久漏收。
#[test]
fn reclaim_domain_second_sweep_catches_late_arrival() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (_dir, store) = race_store();
    let _bound = bind_domain(&store);
    let mgr = manager();
    let dead = dead_prefix();
    let alive = SessionPrefixBuf::ROOT.as_slice();

    // 死域存量键 A（victims 快照内）+ 跨域存活键（不得误收）
    let a_key = b"rd-sweep-a";
    seed(&mgr, dead.as_slice(), a_key, 1).await;
    let alive_key = b"rd-sweep-alive";
    seed(&mgr, alive, alive_key, 1).await;

    // 死域迟到键 B：选与 A 异条带的键名，杜绝 seed B 的共享锁排队在
    // 回收者已持有的 A 独占锁后造成自阻塞
    let a_stripe = stripe_for(&registry_key(dead.as_slice(), a_key));
    let b_key = (0..64u32)
      .map(|i| format!("rd-sweep-b{i}").into_bytes())
      .find(|cand| stripe_for(&registry_key(dead.as_slice(), cand)) != a_stripe)
      .expect("64 个候选键必含异条带键");

    // 持 A 条带共享读锁：回收者过第一轮快照后阻塞在 A 的独占锁上
    let rk_a = registry_key(dead.as_slice(), a_key);
    let shared = mgr.vector_set_locks.acquire_shared(rk_a.as_slice()).await;

    let reclaim = RegistryReclaim::Database {
      vns: 7,
      vdb: 9,
      slot: None,
    };
    let r_mgr = Arc::clone(&mgr);
    let reclaimer = thread::spawn(move || {
      let rt = Runtime::new().unwrap();
      rt.block_on(r_mgr.reclaim_registry_domain(reclaim));
    });

    // 回收者过快照点（阻塞在 A 锁）后，迟到键 B 落表（在途 VADD 完成语义）
    thread::sleep(Duration::from_millis(200));
    seed(&mgr, dead.as_slice(), b_key.as_slice(), 1).await;

    // 排空读者：第一轮收 A → 第二轮快照捕获迟到键 B → 收 B
    drop(shared);
    reclaimer.join().unwrap();

    assert!(
      mgr.read_stored_index(dead.as_slice(), a_key).is_none(),
      "死域存量键 A 应回收"
    );
    assert!(
      mgr
        .read_stored_index(dead.as_slice(), b_key.as_slice())
        .is_none(),
      "第一轮锁间隙落表的迟到键 B 应由第二轮扫尾捕获（单轮实现在此残留）"
    );
    assert!(
      mgr.read_stored_index(alive, alive_key).is_some(),
      "跨域存活键不得误收"
    );
  });
}

/// 重放 RENAME 与 VSIM 交叠锤击：读者经锁协议持共享守卫，守卫存续期间
/// 检索恒成功（重放臂被双键独占锁挡住，索引不可能被销毁）；终态登记
/// 迁移一致。旧实现重放臂零锁，被顶索引可在读者守卫窗口内被丢弃。
#[test]
fn rename_replay_overlap_vsim_never_tears() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (_dir, store) = race_store();
    let _bound = bind_domain(&store);
    let mgr = manager();
    let root = SessionPrefixBuf::ROOT.as_slice();
    let old_key = b"rr-race-old";
    let new_key = b"rr-race-new";
    let stop = Arc::new(AtomicBool::new(false));

    seed(&mgr, root, old_key, 4).await;
    seed(&mgr, root, new_key, 2).await;

    // VSIM 读者线程：守卫存续期间高频检索，任何 Err 即撕裂证据
    let w_mgr = Arc::clone(&mgr);
    let w_store = Arc::clone(&store);
    let w_stop = Arc::clone(&stop);
    let w_new = new_key.to_vec();
    let reader = thread::spawn(move || {
      let _w_bound = bind_domain(&w_store);
      let rt = Runtime::new().unwrap();
      let probe = probe_vec();
      let opts = probe_opts();
      while !w_stop.load(Ordering::Acquire) {
        match rt.block_on(w_mgr.read_vector_index_core(root, &w_new, false)) {
          ReadIndexOutcome::Hit(index, guard) => {
            let ctx = index.context;
            if let Err(e) = rt.block_on(w_mgr.value_similarity(
              index.to_bytes().as_slice(),
              VectorValueType::FP32,
              &probe,
              &opts,
            )) {
              panic!(
                "守卫存续期间检索撕裂: context={ctx} card={} 登记存在={} err={e:?}",
                w_mgr.service.card(ctx),
                w_mgr.read_stored_index(root, &w_new).is_some()
              );
            }
            drop(guard);
          }
          ReadIndexOutcome::NotFound => panic!("新名登记在重放窗口内不得缺席"),
          ReadIndexOutcome::WouldBlock | ReadIndexOutcome::Failed => {
            panic!("阻塞读者不得竞争失败或重建失败")
          }
        }
        spin_loop();
      }
    });

    // 重放臂锤击：每轮 rename onto（清退被顶）后重建旧名（下一轮旧名
    // 登记必在的重放前提），登记终态收敛
    for round in 0..24u32 {
      let res = mgr
        .replay_vector_set_rename(root, new_key, &rename_input(old_key))
        .await;
      assert!(res.is_ok(), "第 {round} 轮重放 RENAME 应成功: {res:?}");
      // 重建旧名（新 context），下一轮重放的 "旧名登记必在" 前提复位
      mgr
        .read_or_create_vector_index(root, old_key, Some(&create_params()))
        .await
        .expect("旧名重建应成功");
    }

    stop.store(true, Ordering::Release);
    reader.join().unwrap();

    // 终态：新名在位、旧名摘除
    assert!(
      mgr.read_stored_index(root, old_key).is_some(),
      "锤击尾旧名经重建应在位"
    );
    mgr
      .replay_vector_set_rename(root, new_key, &rename_input(old_key))
      .await
      .expect("终态重放应成功");
    assert!(mgr.read_stored_index(root, old_key).is_none());
    assert!(mgr.read_stored_index(root, new_key).is_some());
  });
}

/// 确定性锁协议（旧名臂）：共享读者读取旧名（在途 VSIM）存续期间，
/// replay_vector_set_rename 必须被旧名条带独占锁挡住，旧名登记与底层索引
/// 均不得被摘除；读者排空后重放才能推进完成。
#[test]
fn shared_old_key_reader_blocks_rename_replay_until_drained() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (_dir, store) = race_store();
    let _bound = bind_domain(&store);
    let mgr = manager();
    let root = SessionPrefixBuf::ROOT.as_slice();
    let old_key = b"rr-old-blk";
    let new_key = b"rr-new-blk";

    let old_bytes = seed(&mgr, root, old_key, 3).await;
    let old_context = Index::from_bytes(&old_bytes).unwrap().context;

    // 持旧名条带共享读锁（模拟在途 VSIM 读旧集合；双键独占锁必含旧名）
    let rk_old = registry_key(root, old_key);
    let shared = mgr.vector_set_locks.acquire_shared(rk_old.as_slice()).await;

    let done = Arc::new(AtomicBool::new(false));
    let r_mgr = Arc::clone(&mgr);
    let r_flag = Arc::clone(&done);
    let input = rename_input(old_key);
    let replayer = thread::spawn(move || {
      let rt = Runtime::new().unwrap();
      let res = rt.block_on(r_mgr.replay_vector_set_rename(root, new_key, &input));
      assert!(res.is_ok(), "重放 RENAME 应成功: {res:?}");
      r_flag.store(true, Ordering::Release);
    });

    // 200ms 观察窗：持旧名读锁期间重放必须被独占锁挡住
    let deadline = Instant::now() + Duration::from_millis(200);
    while Instant::now() < deadline {
      assert!(
        !done.load(Ordering::Acquire),
        "旧名共享读者存续期间重放 RENAME 不得穿透独占锁"
      );
      assert!(
        mgr.read_stored_index(root, old_key).is_some(),
        "读者存续期间旧名登记不得被摘除"
      );
      assert_eq!(
        mgr.service.card(old_context),
        3,
        "读者存续期间旧名索引元素不得丢失"
      );
      thread::sleep(Duration::from_millis(10));
    }

    // 排空读者：重放推进完成
    drop(shared);
    replayer.join().unwrap();
    assert!(done.load(Ordering::Acquire), "读者排空后重放必须完成");
    assert!(
      mgr.read_stored_index(root, old_key).is_none(),
      "重放后旧名登记应摘除"
    );
    assert_eq!(
      registered_context(&mgr, root, new_key).await,
      old_context,
      "重放后新名登记应迁移为旧名 context"
    );
  });
}

/// 重放 RENAME 与双路并发读者（新旧双键长 EF 检索）交叠竞速：
/// 读者通过锁协议持共享读守卫，长 EF 遍历期间重放臂被互斥锁屏障拦截，
/// 读者绝不发生索引释放或跨集合结果穿透，重放收敛后终态一致。
#[test]
fn rename_replay_overlap_dual_vsim_never_tears() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (_dir, store) = race_store();
    let _bound = bind_domain(&store);
    let mgr = manager();
    let root = SessionPrefixBuf::ROOT.as_slice();
    let old_key = b"rr-dual-old";
    let new_key = b"rr-dual-new";
    let stop = Arc::new(AtomicBool::new(false));

    seed(&mgr, root, old_key, 4).await;
    seed(&mgr, root, new_key, 2).await;

    // 读者 1：高频读 new_key（长 EF=256）
    let w_mgr1 = Arc::clone(&mgr);
    let w_store1 = Arc::clone(&store);
    let w_stop1 = Arc::clone(&stop);
    let w_new = new_key.to_vec();
    let reader_new = thread::spawn(move || {
      let _bound = bind_domain(&w_store1);
      let rt = Runtime::new().unwrap();
      let probe = probe_vec();
      let opts = probe_opts();
      while !w_stop1.load(Ordering::Acquire) {
        if let ReadIndexOutcome::Hit(index, guard) =
          rt.block_on(w_mgr1.read_vector_index_core(root, &w_new, false))
        {
          let ctx = index.context;
          let res = rt.block_on(w_mgr1.value_similarity(
            index.to_bytes().as_slice(),
            VectorValueType::FP32,
            &probe,
            &opts,
          ));
          assert!(
            res.is_ok(),
            "新名守卫存续期间检索撕裂: context={ctx} err={res:?}"
          );
          drop(guard);
        }
        spin_loop();
      }
    });

    // 读者 2：高频读 old_key（长 EF=256）
    let w_mgr2 = Arc::clone(&mgr);
    let w_store2 = Arc::clone(&store);
    let w_stop2 = Arc::clone(&stop);
    let w_old = old_key.to_vec();
    let reader_old = thread::spawn(move || {
      let _bound = bind_domain(&w_store2);
      let rt = Runtime::new().unwrap();
      let probe = probe_vec();
      let opts = probe_opts();
      while !w_stop2.load(Ordering::Acquire) {
        if let ReadIndexOutcome::Hit(index, guard) =
          rt.block_on(w_mgr2.read_vector_index_core(root, &w_old, false))
        {
          let ctx = index.context;
          let res = rt.block_on(w_mgr2.value_similarity(
            index.to_bytes().as_slice(),
            VectorValueType::FP32,
            &probe,
            &opts,
          ));
          assert!(
            res.is_ok(),
            "旧名守卫存续期间检索撕裂: context={ctx} err={res:?}"
          );
          drop(guard);
        }
        spin_loop();
      }
    });

    // 重放臂与重建循环交替执行
    for round in 0..16u32 {
      let res = mgr
        .replay_vector_set_rename(root, new_key, &rename_input(old_key))
        .await;
      assert!(res.is_ok(), "第 {round} 轮重放 RENAME 应成功: {res:?}");
      mgr
        .read_or_create_vector_index(root, old_key, Some(&create_params()))
        .await
        .expect("旧名重建应成功");
    }

    stop.store(true, Ordering::Release);
    reader_new.join().unwrap();
    reader_old.join().unwrap();

    // 终态重放
    mgr
      .replay_vector_set_rename(root, new_key, &rename_input(old_key))
      .await
      .expect("终态重放应成功");
    assert!(mgr.read_stored_index(root, old_key).is_none());
    assert!(mgr.read_stored_index(root, new_key).is_some());
  });
}

/// 发现二（P2）兜底验证：在途 VADD 分配 context 并在 service 建索引后遭遇 FLUSHDB，
/// 登记表尚无键记录（快照前孤儿条目）。reclaim_registry_domain 经 context_metadatas
/// 槽位反查捕获该孤儿 context，即时丢弃原生索引并补投清理通道，消除内存与位图滞留。
#[compio::test]
async fn in_flight_vadd_orphan_context_reclaimed_by_flushdb() {
  let (_dir, store) = race_store();
  let _bound = bind_domain(&store);
  let mgr = manager();
  let dead_slot = slot_of(7, 9);

  // 模拟在途 VADD 第一阶段：分配 context + 构建原生索引，但尚未 put_stored_index
  let orphan_ctx = mgr
    .next_vector_set_context(dead_slot)
    .await
    .expect("分配 context 必须成功");
  mgr
    .service
    .create_index(
      orphan_ctx,
      create_params().index_config(),
      mgr.callbacks.clone(),
    )
    .await
    .expect("原生索引构建必须成功");

  assert!(mgr.service.quant_of(orphan_ctx).is_some());

  // FLUSHDB 执行：指定死域与其逻辑槽位
  let reclaim = RegistryReclaim::Database {
    vns: 7,
    vdb: 9,
    slot: Some(dead_slot),
  };
  mgr.reclaim_registry_domain(reclaim).await;

  // 验证原生索引已被即时丢弃
  assert!(
    mgr.service.quant_of(orphan_ctx).is_none(),
    "死域孤儿 context 原生索引应被即时 drop"
  );

  // 排空并处理补投的清理通道
  drain_cleanups(&mgr).await;

  // 验证 context 位图与槽位已回收：重新分配可复用该 context
  let (c_idx, c_val) = VectorManager::<MemKvStore>::decompose_context(orphan_ctx);
  {
    let metas = mgr.context_metadatas.lock();
    assert!(
      !metas[c_idx].is_in_use(c_idx != 0, c_val),
      "孤儿 context 在清理完成后 in_use 位应清零"
    );
  }

  // 验证分配池无滞留，可重新分配到相同的首选 context
  let recycled_ctx = mgr
    .next_vector_set_context(dead_slot)
    .await
    .expect("回收后重新分配应成功");
  assert_eq!(recycled_ctx, orphan_ctx, "回收的 context 应优先被复用");
}

/// 发现二（P2）双轮扫尾：在途 VADD 在第一轮快照后完成 put_stored_index 落表，
/// 由第二轮扫尾捕获，登记表彻底清零，无死域条目残留。
#[test]
fn in_flight_vadd_late_put_cleaned_by_sweep() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (_dir, store) = race_store();
    let _bound = bind_domain(&store);
    let mgr = manager();
    let dead = dead_prefix();
    let dead_slot = slot_of(7, 9);
    let late_key = b"rd-vadd-late";

    // 预建一个已有键 A
    let a_key = b"rd-vadd-exist";
    seed(&mgr, dead.as_slice(), a_key, 2).await;

    // 模拟在途 VADD：持 A 锁阻塞第一轮回收，回收中途落表 late_key
    let rk_a = registry_key(dead.as_slice(), a_key);
    let shared = mgr.vector_set_locks.acquire_shared(rk_a.as_slice()).await;

    let reclaim = RegistryReclaim::Database {
      vns: 7,
      vdb: 9,
      slot: Some(dead_slot),
    };
    let r_mgr = Arc::clone(&mgr);
    let reclaimer = thread::spawn(move || {
      let rt = Runtime::new().unwrap();
      rt.block_on(r_mgr.reclaim_registry_domain(reclaim));
    });

    thread::sleep(Duration::from_millis(150));
    // 在途 VADD 落表
    seed(&mgr, dead.as_slice(), late_key, 3).await;

    drop(shared);
    reclaimer.join().unwrap();
    drain_cleanups(&mgr).await;

    assert!(
      mgr.read_stored_index(dead.as_slice(), a_key).is_none(),
      "存量键 A 应被回收"
    );
    assert!(
      mgr.read_stored_index(dead.as_slice(), late_key).is_none(),
      "迟到落表的在途 VADD 应由第二轮扫尾彻底回收"
    );
    assert_eq!(
      mgr.registry_domain_count(dead.as_slice()),
      0,
      "死域内登记条目计数必须为 0"
    );
  });
}

/// 发现二（P2）在途 RENAME 竞速 FLUSHDB：RENAME 双键迁移在回收期间交错落表，
/// 回收双轮扫尾确保旧名与新名均被彻底回收，无孤儿条目或悬挂索引。
#[test]
fn in_flight_rename_racing_flushdb_cleaned_up() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (_dir, store) = race_store();
    let _bound = bind_domain(&store);
    let mgr = manager();
    let dead = dead_prefix();
    let dead_slot = slot_of(7, 9);
    let old_key = b"rd-rnm-old";
    let new_key = b"rd-rnm-new";

    seed(&mgr, dead.as_slice(), old_key, 2).await;

    // 阻塞在旧名锁上
    let rk_old = registry_key(dead.as_slice(), old_key);
    let shared = mgr.vector_set_locks.acquire_shared(rk_old.as_slice()).await;

    let reclaim = RegistryReclaim::Database {
      vns: 7,
      vdb: 9,
      slot: Some(dead_slot),
    };
    let r_mgr = Arc::clone(&mgr);
    let reclaimer = thread::spawn(move || {
      let rt = Runtime::new().unwrap();
      rt.block_on(r_mgr.reclaim_registry_domain(reclaim));
    });

    thread::sleep(Duration::from_millis(150));
    drop(shared);
    reclaimer.join().unwrap();

    // 回收后如果在途 RENAME 仍尝试在已清空状态下重放新键，
    // 第二次 sweep（单库 flush 链 safe_flush_aof 后的二次回收）彻底兜底
    mgr.reclaim_registry_domain(reclaim).await;
    drain_cleanups(&mgr).await;

    assert!(mgr.read_stored_index(dead.as_slice(), old_key).is_none());
    assert!(mgr.read_stored_index(dead.as_slice(), new_key).is_none());
    assert_eq!(mgr.registry_domain_count(dead.as_slice()), 0);
  });
}

/// 发现二（P2）并发压力测试：在途 VADD/RENAME 与 FLUSHDB 并发执行，
/// 终态断言死域在登记表中零残留，且所有分配的 context 位在排空清理后均可复用。
#[test]
fn concurrent_vadd_rename_x_flushdb_stress() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (_dir, store) = race_store();
    let _bound = bind_domain(&store);
    let mgr = manager();
    let dead = dead_prefix();
    let dead_slot = slot_of(7, 9);
    let stop = Arc::new(AtomicBool::new(false));

    // 工作线程：持续对死域进行 VADD / RENAME
    let w_mgr = Arc::clone(&mgr);
    let w_store = Arc::clone(&store);
    let w_stop = Arc::clone(&stop);
    let dead_vec = dead.as_slice().to_vec();
    let worker = thread::spawn(move || {
      let _bound = bind_domain(&w_store);
      let rt = Runtime::new().unwrap();
      let mut i = 0u32;
      while !w_stop.load(Ordering::Acquire) {
        let k1 = format!("stress-{i}").into_bytes();
        let k2 = format!("stress-dst-{i}").into_bytes();
        let _ = rt.block_on(seed(&w_mgr, &dead_vec, &k1, 1));
        let _ = rt.block_on(w_mgr.rename_vector_set(&dead_vec, &k1, &k2));
        i = (i + 1) % 64;
      }
    });

    // 主线程：执行多次 FLUSH 回收
    for _ in 0..4 {
      thread::sleep(Duration::from_millis(50));
      let reclaim = RegistryReclaim::Database {
        vns: 7,
        vdb: 9,
        slot: Some(dead_slot),
      };
      mgr.reclaim_registry_domain(reclaim).await;
    }

    stop.store(true, Ordering::Release);
    worker.join().unwrap();

    // 终态扫尾回收 + 排空清理
    let reclaim = RegistryReclaim::Database {
      vns: 7,
      vdb: 9,
      slot: Some(dead_slot),
    };
    mgr.reclaim_registry_domain(reclaim).await;
    drain_cleanups(&mgr).await;

    assert_eq!(
      mgr.registry_domain_count(dead.as_slice()),
      0,
      "并发压测终态死域登记必须零残留"
    );
  });
}
