//! 向量索引锁降级竞态窗口回归（对标
//! libs/server/Resp/Vector/VectorManager.Locking.cs 的共享/独占升降级协议；
//! 并发锤击原型 test/standalone/Garnet.test.extensions/ReadOptimizedLockTests.cs）
//!
//! 修复点：独占写回完成 → 读守卫交还的过渡以 parking_lot
//! [`parking_lot::RwLockWriteGuard::downgrade`] 原子降级承接（旧实现 drop
//! 独占后重取共享，存在并发 DEL/UNLINK/FLUSHDB/RENAME 抢独占销毁索引、
//! 摘除登记记录的时序穿透窗口，返回句柄沦为悬挂引用）。断言：
//!   * 创建/重建返回的守卫即原子降级的读锁：存续期间同条带独占不可得，
//!     drop 后即刻可得（确定性）；
//!   * 重建命中直接降级返回，无需释放重读（登记记录与句柄一致）；
//!   * 创建/重建与并发删除交叠锤击下，句柄交还时刻登记记录必存续且
//!     与句柄同 context（压测，悬挂句柄即登记记录缺席/失配）。

use std::{
  hint::spin_loop,
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
  thread,
};

use compio::runtime::Runtime;
use wnode::resp::vector::{
  vector_manager::{VectorManager, VectorManagerOptions},
  vector_manager_index::Index,
  vector_manager_locking::{CreateIndexParams, ReadIndexOutcome, registry_key},
};
use wval::SessionPrefixBuf;
use wvector::{Callbacks, VectorDistanceMetricType, VectorQuantType, store::StoreCallbacks};

/// 内存存储桩（仅作回调句柄装配；本文件用例的失败注入与原生索引构建
/// 均在 service 层，无落盘面）
struct MemVectorStore;

impl StoreCallbacks for MemVectorStore {
  async fn read_multi<F>(&self, _context: u64, _keys: &[u8], _length_hint: usize, _f: F) -> bool
  where
    F: FnMut(u32, &[u8]) + Send,
  {
    true
  }

  async fn read<F>(&self, _context: u64, _key: &[u8], _f: F) -> bool
  where
    F: FnMut(&[u8]) + Send,
  {
    false
  }

  async fn write(&self, _context: u64, _key: &[u8], _value: &[u8]) -> bool {
    true
  }

  async fn delete(&self, _context: u64, _key: &[u8]) -> bool {
    false
  }

  async fn rmw<F>(&self, _context: u64, _key: &[u8], _write_len: usize, _f: F) -> bool
  where
    F: FnMut(&mut [u8]) + Send,
  {
    true
  }

  async fn filter(&self, _context: u64, _internal_id: u32) -> bool {
    true
  }

  async fn purge_context(&self, _context: u64) -> bool {
    true
  }

  fn log(&self, _context: u64, _msg: &str) {}
}

fn manager() -> Arc<VectorManager<MemVectorStore>> {
  Arc::new(VectorManager::new(
    VectorManagerOptions {
      is_enabled: true,
      ..Default::default()
    },
    Callbacks::new(Arc::new(MemVectorStore)),
  ))
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

/// 锁轴键（复合登记键域）：条带哈希以 prefix+user_key 复合键为轴，
/// 裸用户键不在同一锁域，探锁/持锁必须走 [`registry_key`] 单点
fn lock_axis(root: &[u8], key: &[u8]) -> wval::TaggedKeyBuf {
  registry_key(root, key)
}

/// 句柄有效性断言单点：守卫存续期间登记记录必须存续且与句柄同 context、
/// 同为已初始化态（悬挂句柄 = 登记记录缺席或失配，即降级窗口被穿透）
fn assert_handle_anchored(
  mgr: &VectorManager<MemVectorStore>,
  root: &[u8],
  key: &[u8],
  index: &Index,
  tag: &str,
) {
  assert_ne!(index.index_ptr, 0, "{tag}: 句柄必须已初始化");
  let bytes = mgr
    .read_stored_index(root, key)
    .unwrap_or_else(|| panic!("{tag}: 守卫存续期间登记记录被摘除——写读转换窗口被并发删除穿透"));
  let rec = Index::from_bytes(&bytes).unwrap();
  assert_eq!(rec.index_ptr, 1, "{tag}: 登记记录必须为已初始化态");
  assert_eq!(
    rec.context, index.context,
    "{tag}: 句柄与登记记录失配（悬挂索引）"
  );
}

/// 确定性语义：create/重建返回的守卫即原子降级的读锁——存续期间同条带
/// 独占不可得，drop 后即刻可得（旧实现重取共享同样持锁，此断言与并发
/// 压测共同闭环；单测钉死"降级而非重取"的锁性质）
#[compio::test]
async fn downgraded_guard_blocks_exclusive_until_drop() {
  let mgr = manager();
  let root = SessionPrefixBuf::ROOT.as_slice();
  let key = b"downgrade-guard";
  let lock_key = lock_axis(root, key);

  // 创建路径：独占内建索引 → 原子降级交还
  let (index, guard) = mgr
    .read_or_create_vector_index(root, key, Some(&create_params()))
    .await
    .unwrap();
  assert_handle_anchored(&mgr, root, key, &index, "create");
  assert!(
    mgr
      .vector_set_locks
      .try_acquire_exclusive(&lock_key)
      .is_none(),
    "降级读守卫存续期间同条带独占必须不可得"
  );
  drop(guard);
  let exclusive = mgr
    .vector_set_locks
    .try_acquire_exclusive(&lock_key)
    .expect("守卫 drop 后独占必须即刻可得");
  drop(exclusive);

  // 重建路径：ptr=0 触发独占重建，重建完成降级返回的守卫同性质
  let ctx = index.context;
  let stale = Index {
    context: ctx,
    index_ptr: 0,
    ..index
  };
  mgr.write_stored_index(root, key, &stale.to_bytes()).await;
  let (rebuilt, guard) = mgr
    .read_or_create_vector_index(root, key, Some(&create_params()))
    .await
    .unwrap();
  assert_eq!(rebuilt.context, ctx, "重建保持原 context");
  assert_handle_anchored(&mgr, root, key, &rebuilt, "recreate");
  assert!(
    mgr
      .vector_set_locks
      .try_acquire_exclusive(&lock_key)
      .is_none(),
    "重建降级守卫存续期间同条带独占必须不可得"
  );
  drop(guard);
}

/// 确定性语义：read_vector_index_core 重建命中直接降级返回（不释放重读），
/// 登记记录与句柄一致、读锁即刻生效
#[compio::test]
async fn recreate_direct_hit_downgrades_in_place() {
  let mgr = manager();
  let root = SessionPrefixBuf::ROOT.as_slice();
  let key = b"recreate-direct-hit";
  let lock_key = lock_axis(root, key);

  // 全几何 ptr=0 残录（对标迁移/恢复后 index_ptr 失效的重建前提）
  let p = create_params();
  let stale = Index {
    context: 42,
    dimensions: p.dims,
    num_links: p.num_links,
    build_exploration_factor: p.build_exploration_factor,
    quant_type: p.quant,
    distance_metric: p.distance_metric,
    ..Index::default()
  };
  mgr.write_stored_index(root, key, &stale.to_bytes()).await;

  match mgr.read_vector_index_core(root, key, false).await {
    ReadIndexOutcome::Hit(index, guard) => {
      assert_eq!(index.context, 42, "重建保持原 context");
      assert_handle_anchored(&mgr, root, key, &index, "recreate-hit");
      assert!(
        mgr
          .vector_set_locks
          .try_acquire_exclusive(&lock_key)
          .is_none(),
        "重建降级守卫存续期间同条带独占必须不可得"
      );
      drop(guard);
    }
    other => panic!("ptr=0 记录须以重建命中返回，实测 {other:?}"),
  }
}

/// 创建/重建与并发删除交叠压测（C# ReadForDeleteVectorIndex 协议对偶：
/// 销毁者在条带独占内删集）。锤击下句柄交还时刻登记记录必须存续且
/// 与句柄一致——旧实现的 drop 独占 → 重取共享空隙被销毁者抢占即在此
/// 显形（登记记录缺席 / 句柄悬挂）。
#[test]
fn create_and_recreate_overlap_delete_never_returns_dangling_handle() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let mgr = manager();
    let root = SessionPrefixBuf::ROOT.as_slice();
    let key = b"downgrade-race";

    let stop = Arc::new(AtomicBool::new(false));
    let lock_key = lock_axis(root, key);
    let destroyer_stop = Arc::clone(&stop);
    let destroyer_mgr = Arc::clone(&mgr);
    let destroyer = thread::spawn(move || {
      // 条带锁为异步锁：销毁者自持运行时，逐次 block_on 闭环（等待方在
      // await 点让出，唤醒由事件监听器跨线程投递）
      let rt = Runtime::new().unwrap();
      while !destroyer_stop.load(Ordering::Acquire) {
        // delete_vector_set 自带条带独占锁（C# ReadForDeleteVectorIndex 协议
        // 已内置于删除入口）；非命中自旋高频与创建者争用同一条带
        let _ = rt.block_on(destroyer_mgr.delete_vector_set(root, key));
        spin_loop();
      }
    });

    let rounds = 64;
    for round in 0..rounds {
      // 创建路径：独占内建索引 → 原子降级交还
      let (index, guard) = mgr
        .read_or_create_vector_index(root, key, Some(&create_params()))
        .await
        .unwrap();
      assert_handle_anchored(&mgr, root, key, &index, &format!("create round {round}"));
      assert!(
        mgr
          .vector_set_locks
          .try_acquire_exclusive(&lock_key)
          .is_none(),
        "round {round}: 守卫存续期间独占不可得"
      );
      drop(guard);

      // 重建路径：ptr=0 触发独占重建 → 降级直返；销毁者抢先摘除记录则
      // NotFound，跳过该轮
      let stale = Index {
        index_ptr: 0,
        ..index
      };
      mgr.write_stored_index(root, key, &stale.to_bytes()).await;
      if let ReadIndexOutcome::Hit(rebuilt, guard) =
        mgr.read_vector_index_core(root, key, false).await
      {
        assert_handle_anchored(
          &mgr,
          root,
          key,
          &rebuilt,
          &format!("recreate round {round}"),
        );
        drop(guard);
      }
    }

    stop.store(true, Ordering::Release);
    destroyer.join().unwrap();
  });
}
