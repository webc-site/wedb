//! 树条带锁同核保险丝回归（票 zcode-r135c-lockorder 案二，P1）
//!
//! 缺陷形态：wkv `acquire_tree_read`/`acquire_tree_write` 曾在异步任务内直取
//! parking_lot 无界 `read()`/`write()`（wbase/striped.rs），而分层臂 TreeGuard
//! （wnode `tiered_collection_ops/common.rs:tiered_guard`）持该条带锁跨 await
//! 树 IO（refresh/save/sweep）——compio thread-per-core 下同核任务 T2 对同
//! 条带键无界 park 即停摆本核 io_uring reactor，T1 的 IO 完成永不被收割、
//! T1 永不释锁，双方永久互候，整核全部任务连带悬挂（C# 线程池抢占式停车
//! 无同核耦合，本形态系 rust 同核形态新增危害面）。
//!
//! 修法落点（本回归锁定判据）：两取锁点改有界 try+让核环
//! （`try_read_range_index_lock`/`try_acquire_exclusive_for_delete` +
//! `TREE_LATCH_YIELD_BUDGET=1024`），预算耗尽沿既有 MigrationBusy 存储忙漏斗
//! 上抛——写臂 fail-closed 撤窗即重试，杜绝异步上下文无界停车。
//!
//! 夹具形态：确定性 poll_fn 交错驱动（沿 set_store_cold_window_ttl_selfheal
//! 风格，无 sleep）：T1 持 TreeGuard::Write 正 await 于锁内让出点（save 元
//! 记录落盘前窗），同核注入 T2 写臂与 T3 读臂对同条带键取锁——修复前 T2 首
//! 个 poll 即 park OS 线程、本用例整核悬挂天然红；修复后有界轮次内让核回忙
//! （MigrationBusy），T1 完成释锁后复取得即成。驱动器每处让出必由被询 future
//! 自挂唤源（yield 自唤 / reactor 完成唤），阶段就绪切换在同呼内同步续行，
//! 杜绝「无人唤而永挂」。

use std::{
  future::poll_fn,
  pin::pin,
  result,
  task::Poll,
  time::{Duration, Instant},
};

use aok::Result;
use compio::runtime::Runtime;
use tempfile::tempdir;
use wbftree::{StorageBackendType, TreeTuning};
use whasher::fast_hash;
use wkv::{Error, RangeIndexError, StoreConfig};

use crate::support::open_store_in;

/// 与 C# 测试一致的默认树调优（同 store/range_index.rs）
const TUNE: TreeTuning = TreeTuning {
  cache_size: 65536,
  min_record_size: 8,
  max_record_size: 1024,
  max_key_len: 128,
  leaf_page_size: 0,
};

/// T1 锁内让出点 future：首个 poll 挂起并自唤（外层驱动器承接让核），
/// 复 poll 即续行——TreeGuard 跨 await 持锁窗口的确定性定格点
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

/// T1 持 TreeGuard::Write 跨 await（锁内让出点 + 真实元记录落盘）× 同核
/// T2 写臂 / T3 读臂取锁：断言取锁臂有界让核回忙、T1 释锁后复取得即成、
/// T1 落盘生效（保险丝不改语义，仅杜绝无界停车）
#[test]
fn tree_stripe_latch_fuse_same_core() -> Result<()> {
  const SENTINEL: u64 = 42_424_242;
  let dir = tempdir()?;
  let rt = Runtime::new()?;
  rt.block_on(async {
    let config = StoreConfig::new(1024, 64 * 1024, 16, 0.5)?
      .with_range_index_dir(dir.path().join("range_indexes"));
    let store = open_store_in(&dir, "tree_stripe_fuse.db", config)?;
    let s1 = store.new_session()?;
    let s2 = store.new_session()?;
    let s3 = store.new_session()?;

    let key = b"fuse_idx";
    s1.range_index_create(key, StorageBackendType::Disk, TUNE)
      .await?;
    s1.range_index_set(key, b"field1", b"value1").await?;
    let (mut meta1, mut stub1) = s1
      .load_collection_stub(key)
      .await?
      .expect("既存索引必可装载");
    meta1.size = SENTINEL;
    let (_, mut stub2) = s2.load_collection_stub(key).await?.unwrap();
    let (_, mut stub3) = s3.load_collection_stub(key).await?.unwrap();
    let (_, mut stub4) = s3.load_collection_stub(key).await?.unwrap();

    // 条带锁在手判据：树身份键 = 物理 Meta 键（与 acquire 两臂同一判据）；
    // T1 持写锁期间 try_read 必失手，锁空时必得手（即取即放）
    let stripe = fast_hash(&s1.session_meta_key(key));
    let held = || {
      store
        .range_index
        .try_read_range_index_lock(stripe)
        .is_none()
    };

    // T1：取写锁 → 锁内让出点（跨 await 持 guard）→ save 元记录 → 释锁
    let t1 = async {
      let guard = s1
        .acquire_tree_write(key, &mut stub1)
        .await
        .expect("无争用下 T1 写臂取锁必成");
      guard_held_yield().await;
      s1.save_bftree_meta_stub(key, &meta1, &stub1)
        .await
        .expect("T1 锁内元记录落盘必成");
      drop(guard);
    };
    let mut t1 = pin!(t1);
    // T2：同核同条带键（同键）写臂注入；T3：读臂注入（写锁在手读臂同被挡）
    let t2 = s2.acquire_tree_write(key, &mut stub2);
    let mut t2 = pin!(t2);
    let t3 = s3.acquire_tree_read(key, &mut stub3);
    let mut t3 = pin!(t3);
    // T4：T1 释锁后的复取收敛臂（撤窗即重试的 fail-closed 语义面）
    let t4 = s2.acquire_tree_write(key, &mut stub4);
    let mut t4 = pin!(t4);

    let deadline = Instant::now() + Duration::from_secs(30);
    let mut guard_parked = false;
    let mut t2_res: Option<result::Result<(), RangeIndexError>> = None;
    let mut t2_pending_polls = 0usize;
    let mut t3_res: Option<result::Result<(), RangeIndexError>> = None;
    let mut t3_pending_polls = 0usize;
    let mut t1_done = false;

    // 状态机交错驱动：每阶段只 poll 该动的 future——被询 future 让出时必自
    // 挂唤源（让出点 yield 自唤、取锁环 yield 自唤、落盘 IO 由 reactor 唤），
    // 就绪切换同呼内同步续行，故驱动器每次返回 Pending 都必有待唤，绝不永挂。
    // T1 挂起在锁内让出点期间绝不 poll T1，T2/T3 的有界让核环必须自行收口回忙
    // （修复前该环首个 poll 即 parking_lot 无界 park 停摆本线程，本循环永不
    // 推进即超时红）
    poll_fn(|cx| {
      assert!(Instant::now() < deadline, "同核取锁保险丝交叠驱动超时悬挂");
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
        if t2_res.is_none() {
          // 阶段二：T2 写臂同核取锁——只 poll T2，有界环内必回忙
          match t2.as_mut().poll(cx) {
            Poll::Pending => t2_pending_polls += 1,
            Poll::Ready(res) => t2_res = Some(res.map(|_| ())),
          }
          continue;
        }
        if t3_res.is_none() {
          // 阶段三：T3 读臂同核取锁——写锁在手 try_read 同被挡，有界环回忙
          match t3.as_mut().poll(cx) {
            Poll::Pending => t3_pending_polls += 1,
            Poll::Ready(res) => t3_res = Some(res.map(|_| ())),
          }
          continue;
        }
        if !t1_done {
          // 阶段四：放行 T1 走完锁内落盘并释锁（真实 IO 由本核驱动器收割）
          match t1.as_mut().poll(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(()) => t1_done = true,
          }
          continue;
        }
        // 阶段五：释锁后同会话复取即成（存储忙漏斗的客户端重试收敛面）
        return match t4.as_mut().poll(cx) {
          Poll::Ready(res) => {
            assert!(res.is_ok(), "释锁后 T4 复取必成，实得 {:?}", res.err());
            Poll::Ready(())
          }
          Poll::Pending => Poll::Pending,
        };
      }
    })
    .await;

    // 忙拒因由 = 既有 MigrationBusy 存储忙漏斗（案二裁决：不新增第二套通道）
    let t2_err = t2_res.expect("T2 已收口").expect_err("T2 必忙拒");
    assert!(
      matches!(&t2_err, RangeIndexError::Store(e) if matches!(**e, Error::MigrationBusy)),
      "T2 失手因由须为 MigrationBusy 存储忙，实得 {t2_err:?}"
    );
    let t3_err = t3_res.expect("T3 已收口").expect_err("T3 必忙拒");
    assert!(
      matches!(&t3_err, RangeIndexError::Store(e) if matches!(**e, Error::MigrationBusy)),
      "T3 失手因由须为 MigrationBusy 存储忙，实得 {t3_err:?}"
    );
    // 有界性：让核轮次应落在预算 1024 量级（远小于「无界停车=永不 Ready」，
    // 又足以证明逐轮让核推进而非首轮空判即失手）
    assert!(
      (256..4096).contains(&t2_pending_polls),
      "T2 让核轮次 {t2_pending_polls} 脱离预算 1024 量级（无界停车或空判即机制破坏）"
    );
    assert!(
      (256..4096).contains(&t3_pending_polls),
      "T3 让核轮次 {t3_pending_polls} 脱离预算 1024 量级"
    );
    assert!(!held(), "T1 释锁后条带锁必须已回收");

    // T1 锁内落盘生效：哨兵 size 经 save_bftree_meta_stub 主存可读
    let (meta_chk, _) = s3
      .load_collection_stub(key)
      .await?
      .expect("T1 落盘后索引必仍存活");
    assert_eq!(meta_chk.size, SENTINEL, "持锁窗口内的元记录落盘须生效");
    Ok(())
  })
}

/// 无争用稳态零变化：两臂首试即取得直放（保险丝不给稳态路径引入让核开销）
#[test]
fn tree_stripe_latch_uncontended_fast_path() -> Result<()> {
  let dir = tempdir()?;
  let rt = Runtime::new()?;
  rt.block_on(async {
    let config = StoreConfig::new(1024, 64 * 1024, 16, 0.5)?
      .with_range_index_dir(dir.path().join("range_indexes"));
    let store = open_store_in(&dir, "tree_stripe_fast.db", config)?;
    let session = store.new_session()?;
    let key = b"fuse_fast";
    session
      .range_index_create(key, StorageBackendType::Disk, TUNE)
      .await?;
    session.range_index_set(key, b"field1", b"value1").await?;
    let (_, mut stub) = session
      .load_collection_stub(key)
      .await?
      .expect("既存索引必可装载");
    {
      let _w = session
        .acquire_tree_write(key, &mut stub)
        .await
        .expect("无争用写臂直取");
    }
    {
      let _r = session
        .acquire_tree_read(key, &mut stub)
        .await
        .expect("无争用读臂直取");
    }
    Ok(())
  })
}
