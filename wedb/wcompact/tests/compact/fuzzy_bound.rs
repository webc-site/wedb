//! 紧缩上界 SafeReadOnlyAddress 回归用例
//!
//! 在人为放大的模糊区窗口（纪元保护驻留的原位写者 + read_only 推进）下验证：
//! - 越过 safe_ro 的紧缩被内核硬拒（对标 TsavoriteCompaction.cs:35-36/:72-73
//!   "Can compact only until Log.SafeReadOnlyAddress"）——旧实现以 read_only 为界，
//!   会把模糊区旧值副本 CAS 顶掉索引并随 begin 推进物理截断在途原位新值（丢更新），
//!   对称时序下原位墓碑被架空（已删键复活）；
//! - 以 safe_ro 为界的紧缩绝不触达 [safe_ro, read_only)：原位新值与原位墓碑
//!   紧缩后按原位结果可见，索引槽位不被搬迁副本改写。

use std::{
  sync::{Arc, mpsc},
  thread,
  time::{Duration, Instant},
};

use aok::{OK, Void};
use compio::runtime::Runtime;
use tempfile::tempdir;
use wcompact::{CompactSession, CompactionType, Error, LogCompactor};

use super::support::{FixtureStore, str_key};

/// 驻留窗口的扫描进展等待预算（毫秒）：紧缩扫描/尾部追加为微秒级，超时兜底放行
const RELEASE_BUDGET: Duration = Duration::from_millis(500);

/// 等待紧缩扫描阶段可见进展（尾部增长）或超时后放行驻留写者
///
/// 独立 OS 线程执行：value 用例的驻留写者持有页写锁（try_modify_record_in_place
/// 的用户闭包在 write_page 守卫内执行），紧缩扫描以 parking_lot 同步读锁等该页
/// 会挂起 reactor 线程——放行协议若以 async sleep 驻留同一 reactor，定时器永
/// 不被调度，「扫描等页锁 ← 放行等 reactor ← reactor 等页锁」成闭环死锁；独立
/// 线程的同步轮询不受 reactor 挂起影响，保证放行必达。shift_begin 的补刷与
/// 截断屏障需写者退出纪元保护才能排空，放行点即测试汇聚点；无论放行落在扫描
/// 前还是扫描后，A >= safe_ro 恒在紧缩区间之外，断言不受时序影响
fn release_after_progress(
  store: Arc<FixtureStore>,
  baseline_tail: u64,
  go: mpsc::Sender<()>,
) -> thread::JoinHandle<()> {
  thread::spawn(move || {
    let deadline = Instant::now() + RELEASE_BUDGET;
    while store.hlog.tail_address() <= baseline_tail && Instant::now() < deadline {
      thread::sleep(Duration::from_millis(5));
    }
    let _ = go.send(());
  })
}

/// 模糊区在途原位值覆写：safe_ro 界紧缩后原位新值可见，索引不被旧值副本顶掉
#[test]
fn fuzzy_inplace_value_write_survives_safe_bound() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let store = FixtureStore::open(dir.path().join("fuzzy_value.db"))?;
    let s = store.session()?;

    // 填充记录（[begin, safe_ro) 可紧缩区间）+ 目标键 K@A + 尾锚（保证 A < tail）
    for i in 0..4 {
      store
        .put(&s, &str_key(format!("fill:{i}").as_bytes()), b"filler")
        .await?;
    }
    let k = str_key(b"fuzzy");
    let addr = store.put(&s, &k, b"old_value").await?;
    store.put(&s, &str_key(b"anchor"), b"tail_anchor").await?;
    let tail = store.hlog.tail_address();

    // 封印至 A：ro = safe_ro = A，K 仍在可变区（A >= ro，原位写准入成立）
    store.seal_read_only(addr);
    assert_eq!(store.hlog.safe_read_only_address(), addr);

    // 写者驻留：纪元保护内进入原位改写闭包（页写锁 + read_only 双检已过），
    // 值字节落笔前停摆——即生产 RMW 在途原位写的确定性放大形态
    let (park_tx, park_rx) = mpsc::channel::<()>();
    let (go_tx, go_rx) = mpsc::channel::<()>();
    let wstore = Arc::clone(&store);
    let wk = k.clone();
    let writer = thread::spawn(move || {
      let p = wstore.epoch.register().unwrap_or_else(|e| panic!("{e}"));
      let _guard = p.enter();
      let modified = wstore.hlog.try_modify_record_in_place(addr, &wk, |val| {
        // 页写锁内宣告驻留（park 信号后置进锁，保证主线程见信号时写锁已持有）
        let _ = park_tx.send(());
        let _ = go_rx.recv();
        val.copy_from_slice(b"new_value");
        Some(())
      });
      assert!(
        matches!(modified, Ok(Some(()))),
        "原位改写必须成功: {modified:?}"
      );
    });
    park_rx.recv().unwrap();

    // 推进 read_only 越过 A：写者保护钉住纪元 → safe_ro 停在 A，模糊区窗口成立
    store.hlog.shift_read_only_address(tail);
    let safe_ro = store.hlog.safe_read_only_address();
    let ro = store.hlog.read_only_address();
    assert!(
      safe_ro <= addr && addr < ro,
      "模糊区窗口必须成立: safe_ro={safe_ro:#x}, A={addr:#x}, ro={ro:#x}"
    );

    // 越过 safe_ro 的紧缩必须被内核硬拒（双模式）：旧实现在此接受 until=ro 并丢失在途新值
    let compactor = LogCompactor::new(Arc::clone(&store));
    for comp_type in [CompactionType::Lookup, CompactionType::Scan] {
      let err = compactor.compact(ro, comp_type).await;
      assert!(
        matches!(err, Err(Error::UntilAddressOutOfRange { .. })),
        "越过 safe_ro 的紧缩必须硬拒: {comp_type:?} -> {err:?}"
      );
    }

    // 以 safe_ro 为界紧缩：A 不在区间内，索引槽位与原位字节均不被触碰
    let task_store = Arc::clone(&store);
    let task = rt.spawn(async move {
      LogCompactor::new(Arc::clone(&task_store))
        .compact(safe_ro, CompactionType::Lookup)
        .await
    });
    let releaser = release_after_progress(Arc::clone(&store), tail, go_tx);
    let stats = task
      .await
      .map_err(|e| aok::anyhow!("紧缩任务异常结束: {e}"))??;
    writer.join().unwrap();
    releaser.join().unwrap();

    assert_eq!(stats.scanned_records, 4, "仅填充记录参与紧缩: {stats:?}");
    assert_eq!(stats.live_copied, 4);
    assert_eq!(stats.new_begin_address, safe_ro);
    assert_eq!(
      store.hlog.begin_address(),
      safe_ro,
      "begin 推进至 safe_ro，绝不越过 A"
    );

    // K 的索引槽位仍是原位地址（未被旧值副本顶掉），读取得到原位新值
    let slot = {
      let _guard = s.enter_epoch();
      store.index.find_tag(&k)
    };
    assert_eq!(
      store.resolve_main(slot.expect("K 槽位必须在")),
      addr,
      "索引槽位不得被搬迁副本改写"
    );
    assert_eq!(
      store.get(&s, &k).await?.as_deref(),
      Some(b"new_value".as_slice()),
      "紧缩后必须读到原位新值而非搬回的旧值副本"
    );

    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// 模糊区原位墓碑：safe_ro 界紧缩后已删键不复活（墓碑不被旧值副本架空）
#[test]
fn fuzzy_inplace_tombstone_not_superseded_by_safe_bound() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let store = FixtureStore::open(dir.path().join("fuzzy_tomb.db"))?;
    let s = store.session()?;

    for i in 0..4 {
      store
        .put(&s, &str_key(format!("fill:{i}").as_bytes()), b"filler")
        .await?;
    }
    let k = str_key(b"fuzzy");
    let addr = store.put(&s, &k, b"old_value").await?;
    store.put(&s, &str_key(b"anchor"), b"tail_anchor").await?;
    let tail = store.hlog.tail_address();

    store.seal_read_only(addr);
    assert_eq!(store.hlog.safe_read_only_address(), addr);

    // 写者：纪元保护内先落原位墓碑（A >= ro 双检过），随后驻留钉住纪元
    let (park_tx, park_rx) = mpsc::channel::<()>();
    let (go_tx, go_rx) = mpsc::channel::<()>();
    let wstore = Arc::clone(&store);
    let wk = k.clone();
    let writer = thread::spawn(move || {
      let p = wstore.epoch.register().unwrap_or_else(|e| panic!("{e}"));
      let _guard = p.enter();
      assert!(
        wstore
          .hlog
          .try_set_tombstone_in_place(addr, &wk)
          .is_ok_and(|done| done),
        "原位墓碑必须落笔成功"
      );
      let _ = park_tx.send(());
      let _ = go_rx.recv();
    });
    park_rx.recv().unwrap();

    store.hlog.shift_read_only_address(tail);
    let safe_ro = store.hlog.safe_read_only_address();
    let ro = store.hlog.read_only_address();
    assert!(
      safe_ro <= addr && addr < ro,
      "模糊区窗口必须成立: safe_ro={safe_ro:#x}, A={addr:#x}, ro={ro:#x}"
    );

    let compactor = LogCompactor::new(Arc::clone(&store));
    for comp_type in [CompactionType::Lookup, CompactionType::Scan] {
      let err = compactor.compact(ro, comp_type).await;
      assert!(
        matches!(err, Err(Error::UntilAddressOutOfRange { .. })),
        "越过 safe_ro 的紧缩必须硬拒: {comp_type:?} -> {err:?}"
      );
    }

    let task_store = Arc::clone(&store);
    let task = rt.spawn(async move {
      LogCompactor::new(Arc::clone(&task_store))
        .compact(safe_ro, CompactionType::Scan)
        .await
    });
    let releaser = release_after_progress(Arc::clone(&store), tail, go_tx);
    let stats = task
      .await
      .map_err(|e| aok::anyhow!("紧缩任务异常结束: {e}"))??;
    writer.join().unwrap();
    releaser.join().unwrap();

    assert_eq!(stats.scanned_records, 4, "仅填充记录参与紧缩: {stats:?}");
    assert_eq!(stats.new_begin_address, safe_ro);
    assert_eq!(store.hlog.begin_address(), safe_ro);

    // 原位墓碑仍生效：读取为 None（已删键不复活），索引槽位仍指向原位地址
    let slot = {
      let _guard = s.enter_epoch();
      store.index.find_tag(&k)
    };
    assert_eq!(
      store.resolve_main(slot.expect("K 槽位必须在")),
      addr,
      "索引槽位不得被搬迁副本改写"
    );
    assert!(
      store.get(&s, &k).await?.is_none(),
      "原位墓碑不得被旧值副本架空（已删键不复活）"
    );

    aok::Result::<()>::Ok(())
  })?;
  OK
}
