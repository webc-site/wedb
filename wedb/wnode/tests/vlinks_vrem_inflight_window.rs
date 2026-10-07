#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! VREM 在飞窗 × VLINKS 交错回归（票：wvector-vlinks-dangling-adjacency-poisons-echo）
//!
//! 缺陷形态：VREM 与 VLINKS 同持条带共享读守卫可交错（vector_manager.rs
//! 锁域自陈，对标 C# VectorStoreOps 的 VectorSetRemove 锁点 :225 "holding a
//! shared lock ... everything else can proceed in parallel"）；删除体先摘
//! 数据并释放 fsm 槽位（delete_element 的 mark_free 在前），图边回收
//! （inplace_delete 的 add_edge_and_prune）在其后——交错窗内存活成员的
//! 邻接表必现悬空 id。旧 neighbors 遍历臂对该 id 以 `?` 整链上抛、links_of
//! `.ok()?` 折 None、network_vlinks 与「元素缺席」同形写 null：单个已删
//! 邻居毒化整包，存活成员 VISMEMBER 真、VLINKS null 命令面自相矛盾。
//!
//! 修复契约：neighbors 迭代臂对每个邻居先 vector_iid_exists（fsm 占用位）
//! 前置过滤——窗内 VLINKS 恒 Array 非 null，悬空邻居跳过回显。
//!
//! 交错构造为真实竞态而非假桩：存储回调在 armed 期间对 **Neighbors 域**
//! rmw 落盘口让位挂起（async_lock 门闸，VREM 此刻已持共享守卫、删除体已
//! 完成）——删除体五记录删除走 direct delete、fsm 落盘走 Metadata 域 rmw，
//! 唯有图阶段边回收（set_neighbors）走 Neighbors 域 rmw，故挂起点恰为
//! 「数据已删 + 悬空边未回收」窗（形态同 vector_vadd_guard_blocks_delete.rs
//! 门闸族）。

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
use wbase::hash_slot::slot_of;
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::resp::vector::{
  resp_server_session_vectors::{RespServerSessionVectors, VectorReply},
  vector_manager::{VectorManager, VectorManagerOptions},
  vector_manager_index::Index,
  vector_store_callbacks::OwnedActiveVectorSession,
};
use wtest_base::test_store_config;
use wval::SessionPrefixBuf;
use wvector::{
  Callbacks,
  store::{StoreCallbacks, TERM_BITMASK, Term},
};
use wvector_test::MemKvStore;

/// 默认会话库槽（测试键不参与定槽，统一取 (0,0) 库槽位）
const SLOT0: u16 = slot_of(0, 0);

/// Neighbors 域 rmw 落盘口让位门闸：armed 期间首个 Neighbors 域 rmw（VREM
/// 图阶段边回收写，删除体已完成）置位 parked 后挂起在异步门闩上。门闸取放
/// 均在 map 锁之外，观测线程零阻塞。
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
    self.inner.write(context, key, value).await
  }

  async fn delete(&self, context: u64, key: &[u8]) -> bool {
    self.inner.delete(context, key).await
  }

  async fn rmw<F>(&self, context: u64, key: &[u8], write_len: usize, f: F) -> bool
  where
    F: FnMut(&mut [u8]) + Send,
  {
    if write_len > 0 && context & TERM_BITMASK == Term::Neighbors as u64 {
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

/// VREM 让位窗内 VLINKS 交错：断言存活成员 e1 的 VLINKS 恒 Array 非 null
/// 且悬空邻居 e2 跳过回显（修复前 null 毒化整包），门闸放行后 VREM 完整
/// 收口闭合结局 Integer(1)。
#[test]
fn vlinks_survivor_array_during_vrem_inflight_window() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    // ── 装配：真盘 wkv 会话绑定域 + 门闸存储回调 ──
    let dir = tempfile::tempdir().unwrap();
    let device = Arc::new(SegmentedDevice::single_file(dir.path().join("vlinks_vrem.db")).unwrap());
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
    let sess = RespServerSessionVectors::new(Arc::clone(&vm));
    let root = SessionPrefixBuf::ROOT.as_slice();
    let key = b"vk";

    // ── 种子：常规 VADD 建集并入 e1 / e2（近邻互连）──
    for (element, values) in [
      (b"e1".as_slice(), fp32(1.0, 0.0)),
      (b"e2".as_slice(), fp32(0.0, 1.0)),
    ] {
      let args = vadd_args(element, &values);
      assert!(
        matches!(
          sess
            .network_vadd(root, &arg_refs(&args), SLOT0, false)
            .await,
          VectorReply::Integer(1)
        ),
        "种子 VADD {element:?} 必须成功"
      );
    }
    let context = Index::from_bytes(&vm.read_stored_index(root, key).unwrap())
      .unwrap()
      .context;

    // 图连边基线：e1 的层 0 邻接含 e2（交错窗悬空面前提）
    let links = vm.service.links_of(context, b"e1").await.unwrap();
    assert!(
      links.iter().any(|(id, _)| id.as_slice() == b"e2"),
      "e1 应邻接 e2: {links:?}"
    );

    // 门闸武装：VREM 的图阶段首条 Neighbors 域 rmw（边回收写）让位挂起
    gate.armed.store(true, Ordering::Release);
    let hold = gate.gate.lock().await;

    // ── VREM 线程：持共享守卫进入删除，边回收 rmw 口 park 在门闸上 ──
    let v_mgr = Arc::clone(&vm);
    let v_gate = Arc::clone(&gate);
    let v_store = Arc::clone(&store);
    let vrem_thread = thread::spawn(move || {
      let _domain = OwnedActiveVectorSession::new(v_store.new_session().unwrap());
      let sess = RespServerSessionVectors::new(v_mgr);
      let rt = Runtime::new().unwrap();
      rt.block_on(async move {
        match sess.network_vrem(root, &[b"vk", b"e2"]).await {
          VectorReply::Integer(v) => v,
          other => panic!("VREM 应答形态异常: {other:?}"),
        }
      })
    });

    // 等待让位点命中（VREM 此刻已持共享守卫、删除体已完成）
    let deadline = Instant::now() + Duration::from_secs(5);
    while !v_gate.parked.load(Ordering::Acquire) {
      assert!(
        Instant::now() < deadline,
        "VREM 未在图阶段 Neighbors rmw 让位"
      );
      thread::sleep(Duration::from_millis(2));
    }

    // ── 交错窗观测 ──
    // 删除体已生效：e2 映射摘除、fsm 槽位归还
    assert!(
      !vm
        .service
        .check_external_id_valid(context, b"e2")
        .await
        .unwrap(),
      "删除体应在窗内已完成"
    );
    // 核心：存活成员 e1 VLINKS 恒 Array 非 null（同刻共享锁读态交错，修复前
    // 悬空 iid 经 `?` 上抛折 null 毒化整包），悬空邻居 e2 跳过回显
    let reply = sess.network_vlinks(root, &[b"vk", b"e1"]).await;
    let VectorReply::Array(items) = reply else {
      panic!("VREM 在飞窗存活成员 VLINKS 必为 Array 非 null: {reply:?}");
    };
    let e2_reply = VectorReply::Bulk(Some(b"e2".to_vec().into()));
    assert!(
      !items.contains(&e2_reply),
      "窗内悬空邻居必须跳过回显: {items:?}"
    );

    // ── 放行门闸：VREM 边回收完成，闭合结局 Integer(1) ──
    drop(hold);
    let removed = vrem_thread.join().unwrap();
    assert_eq!(removed, 1, "门闸放行后 VREM 必须完整收口");

    // 收口后 e1 回显仍健康（无 e2、非 null），计数收敛
    let reply = sess.network_vlinks(root, &[b"vk", b"e1"]).await;
    let VectorReply::Array(items) = reply else {
      panic!("收口后 VLINKS 必为 Array: {reply:?}");
    };
    assert!(!items.contains(&e2_reply), "收口后 e2 不得回显: {items:?}");
    assert_eq!(vm.service.card(context), 1);

    let _ = remove_dir_all(dir.path());
  });
}
