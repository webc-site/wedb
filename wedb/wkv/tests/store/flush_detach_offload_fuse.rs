//! 清库回收臂同核停车档保险丝回归（票 wkv-flush-replay-detach-tree-async-stripe-park-core-deadlock，P1）
//!
//! 缺陷形态：FLUSHDB 换号联动树回收（`reclaim_bftree_keys` → `detach_tree`）、
//! RI.CREATE/升阶回滚臂（`delete_index`）与副本 FlushDb/FlushNs 回放臂
//! （`retire_dead_domain`/`retire_dead_namespace`）曾在 compio 异步任务内同步
//! 直调 `detach_tree`/`delete_index`——wbftree 条带锁无界 `locks.write` 停车档
//! （manager/lifecycle.rs）被同核任务直取。若该条带恰被同核分层写臂的
//! `TreeWriteGuard` 持跨 await（真实挂起点：锁内 refresh/save 元记录落盘），
//! 写臂续算只能由本核 reactor 驱动，而本核线程正停车等锁——互候永挂，整核
//! 全部连接会话停摆（C# 线程池任务可迁移 + TryWriteLock 失败 Thread.Yield
//! 重试，无同核耦合；本形态系 rust thread-per-core 新增危害面，数据面分层
//! DEL 排空臂 drain.rs 早已按纪律卸载，生命周期臂系漏接）。
//!
//! 修法落点（本回归锁定判据）：全部异步调用臂统一卸载既有
//! `range_index_blocking` 通道（spawn_blocking，与 drain.rs 排空臂既有正确形
//! 同款，零新机制零新锁型）——停车发生在阻塞线程，本核 reactor 永不停摆；
//! 取锁等待与锁内 I/O 一并离核。
//!
//! 夹具形态：确定性 poll_fn 交错驱动（沿 tree_stripe_latch_fuse 同款）：T1
//! 持 `TreeWriteGuard` 正 await 于锁内让出点，同核 T2 发起 FLUSHDB——修复前
//! T2 首 poll 直抵 `detach_tree` 即 park OS 线程、本用例整核悬挂天然红；修复
//! 后回收臂卸载阻塞线程（泊于条带等待），T2 钉在 join Pending，T1 完成释锁
//! 后回收照常收口，FLUSHDB 有界完成（绝不整核挂死）。副本端 FlushDb 回放臂
//! 经同一 `reclaim_bftree_keys` 单点承接（retire 链异步化后与本用例共收口），
//! 回放语义面由既有 aof_flush_replay 套件覆盖，不另设栅栏夹具。

use std::{
  future::poll_fn,
  pin::pin,
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
  task::Poll,
  time::{Duration, Instant},
};

use aok::Result;
use compio::runtime::Runtime;
use tempfile::tempdir;
use wbftree::{StorageBackendType, TreeTuning};
use whasher::fast_hash;
use wkv::StoreConfig;

use crate::support::{open_store_in, tree_id_key};

/// 与 C# 测试一致的默认树调优（同 store/range_index.rs）
const TUNE: TreeTuning = TreeTuning {
  cache_size: 65536,
  min_record_size: 8,
  max_record_size: 1024,
  max_key_len: 128,
  leaf_page_size: 0,
};

/// 守卫窗探针轮数（有界忙询：修复后 T2 在窗内恒钉回收 join Pending；轮数
/// 只放大「误收口即夹具前提破坏」的探针密度，不涉时长）
const PROBE_ROUNDS: usize = 64;

/// T1 锁内让出点 future：首个 poll 挂起并自唤（外层驱动器承接让核），复 poll
/// 即续行——TreeWriteGuard 跨 await 持锁窗口的确定性定格点
async fn guard_held_yield() {
  let mut first = true;
  poll_fn(|cx| {
    if first {
      first = false;
      cx.waker().wake_by_ref();
      Poll::Pending
    } else {
      Poll::Ready(())
    }
  })
  .await;
}

/// T1 持 TreeWriteGuard 跨 await（锁内让出点）× 同核 T2 FLUSHDB 换号树回收：
/// 断言回收臂离核（核心存活、FLUSHDB 有界完成）、旧域树摘注册、同名重建
/// 不被 IndexExists 拦截（换号清库语义零变化）
#[test]
fn flushdb_reclaim_same_core_guard_window_fuse() -> Result<()> {
  let dir = tempdir()?;
  let rt = Runtime::new()?;
  rt.block_on(async {
    let config = StoreConfig::new(1024, 64 * 1024, 16, 0.5)?
      .with_range_index_dir(dir.path().join("range_indexes"));
    let store = open_store_in(&dir, "flush_detach_fuse.db", config)?;
    let s1 = store.new_session()?;

    let key = b"fuse_flush_idx";
    s1.range_index_create(key, StorageBackendType::Disk, TUNE)
      .await?;
    s1.range_index_set(key, b"field1", b"value1").await?;
    let (_, mut stub1) = s1
      .load_collection_stub(key)
      .await?
      .expect("既存索引必可装载");

    // 条带锁在手判据（同 tree_stripe_latch_fuse）：树身份键 = 物理 Meta 键，
    // FLUSHDB 回收臂 detach 的 stripe 与 T1 守卫同条带（键在册旧域）
    let stripe = fast_hash(&s1.session_meta_key(key));
    let held = || {
      store
        .range_index
        .try_read_range_index_lock(stripe)
        .is_none()
    };

    // T1：分层写臂取写锁 → 锁内让出点（跨 await 持 guard 定格窗）→ 释锁
    let released = Arc::new(AtomicBool::new(false));
    let released_t1 = Arc::clone(&released);
    let t1 = async {
      let guard = s1
        .acquire_tree_write(key, &mut stub1, None)
        .await
        .expect("无争用下 T1 写臂取锁必成");
      guard_held_yield().await;
      drop(guard);
      released_t1.store(true, Ordering::Release);
    };
    let mut t1 = pin!(t1);
    // T2：同核 FLUSHDB（0 号库换号清库，回收臂必触同条带）
    let t2 = store.flush_database(0, 0);
    let mut t2 = pin!(t2);

    let deadline = Instant::now() + Duration::from_secs(30);
    let mut guard_parked = false;
    let mut probe_rounds = 0usize;
    let mut t1_done = false;

    // 状态机交错驱动（同 tree_stripe_latch_fuse 次序纪律）：T1 定格持锁窗后
    // 询 T2——修复前首触 detach_tree 即 park 本线程、本循环永不推进（天然
    // 红）；修复后 T2 钉在回收 join Pending，探针轮内绝不收口，随后放行 T1
    // 释锁、回收阻塞线程照常收口
    poll_fn(|cx| {
      assert!(
        Instant::now() < deadline,
        "同核清库×分层写臂交叠驱动超时悬挂"
      );
      loop {
        if !guard_parked {
          // 阶段一：推进 T1 至持锁挂起点（让出点必 Pending 且锁在手）
          match t1.as_mut().poll(cx) {
            Poll::Pending if held() => guard_parked = true,
            // 让出在取锁前的装载段：唤源已挂（IO/让核），待唤复询即可
            Poll::Pending => return Poll::Pending,
            Poll::Ready(()) => panic!("T1 未在持锁跨 await 窗口定格（夹具前提破坏）"),
          }
          continue;
        }
        if probe_rounds < PROBE_ROUNDS {
          // 阶段二：守卫窗探针——T2 在窗内绝不收口（回收臂必经同条带）
          match t2.as_mut().poll(cx) {
            Poll::Ready(_) => {
              panic!("FLUSHDB 在分层写臂持守卫窗内即收口（回收臂未触同条带，夹具前提破坏）")
            }
            Poll::Pending => {}
          }
          assert!(held(), "持守卫窗内条带锁不得易手");
          probe_rounds += 1;
          cx.waker().wake_by_ref();
          return Poll::Pending;
        }
        if !t1_done {
          // 阶段三：放行 T1 走完让出点并释锁
          match t1.as_mut().poll(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(()) => t1_done = true,
          }
          continue;
        }
        // 阶段四：释锁后回收阻塞线程照常收口，FLUSHDB 有界完成
        return match t2.as_mut().poll(cx) {
          Poll::Ready(res) => {
            let (vns, old_vdb) = res.expect("FLUSHDB 必有界完成（回收离核后整核不悬挂）");
            assert_eq!(vns, 0, "根租户换号 vns 不变");
            assert_eq!(old_vdb, Some(0), "既有映射换号必退役旧域 0");
            Poll::Ready(())
          }
          Poll::Pending => Poll::Pending,
        };
      }
    })
    .await;

    assert!(released.load(Ordering::Acquire), "T1 写臂必已收口");
    assert!(!held(), "T1 释锁后条带锁必须已回收");

    // 换号清库语义零变化：旧域树已摘注册、旁表登记已取走、同名重建成功
    // （回收臂若未触条带即收口，此处 get_tree 必仍命中旧域在册树）
    assert!(
      store
        .range_index
        .get_tree(&tree_id_key(0, 0, key))
        .is_none(),
      "旧域树必须已从注册表摘除"
    );
    assert!(
      store.snapshot_bftree_domains().is_empty(),
      "旧域旁表登记必须已取走"
    );
    s1.range_index_create(key, StorageBackendType::Disk, TUNE)
      .await
      .expect("清库后同名重建索引必成功");
    Ok(())
  })
}
