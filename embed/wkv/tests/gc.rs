//! 内置 GC 后台管理器集成测试
//!
//! 覆盖：主动过期扫描的物理删除（数据 + raw 层 TTL 记录墓碑化）、
//! max_batch_deletes 批预算截断与游标推进收敛、紧缩阈值触发算法
//! （真 LogCompactor 端口适配，未超阈值不触发）、open_shared 自动启动与 stop 退出。

use std::{sync::Arc, time::Duration};

use aok::{OK, Void};
use compio::{runtime::Runtime, time::sleep};
use tempfile::{TempDir, tempdir};
use wbase::time::now_ms;
use wdev::SegmentedDevice;
use wkv::{GcConfig, GcManager, StoreConfig, TtlOpt, WedbStore};

#[ctor::ctor(unsafe)]
fn _log_init() {
  log_init::init();
}

/// 构造独立临时库（page 4KB / 16 页，GC 配置注入但强制 enabled=false 由测试手动驱动）
async fn open_manual(
  tag: &str,
  gc: GcConfig,
) -> aok::Result<(TempDir, Arc<WedbStore<SegmentedDevice>>)> {
  let dir = tempdir()?;
  let device = Arc::new(SegmentedDevice::single_file(
    dir.path().join(format!("gc_{tag}.db")),
  )?);
  let mut config = StoreConfig::new(1024, 4096, 16, 0.5)?;
  config.gc = gc;
  config.gc.enabled = false;
  let store = Arc::new(WedbStore::open(config, device)?);
  Ok((dir, store))
}

/// 测试 1: run_once 过期扫描物理删除短 TTL 键（数据 + raw 层 TTL 记录墓碑化），
/// 无 TTL 键与未到期键不受影响
#[test]
fn test_run_once_purges_expired_keys() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store) = open_manual("purge", GcConfig::default()).await?;
    assert!(
      store.gc_handle().is_none(),
      "enabled=false 时不得自动启动 GC"
    );
    let session = store.new_session()?;
    let mgr = GcManager::new(&store);

    let (dead, live, later) = (
      b"gc:dead".as_slice(),
      b"gc:live".as_slice(),
      b"gc:later".as_slice(),
    );
    for k in [dead, live, later] {
      session.upsert(k, b"v").await?;
    }
    assert_eq!(
      session.expire_at(dead, now_ms() + 50, TtlOpt::NONE).await?,
      1
    );
    assert_eq!(
      session
        .expire_at(later, now_ms() + 60_000, TtlOpt::NONE)
        .await?,
      1
    );
    sleep(Duration::from_millis(120)).await;

    mgr.run_once().await?;

    // 过期键物理删除：数据读空 + raw 层 TTL 记录墓碑化
    assert!(!session.contains_key(dead).await?);
    assert_eq!(session.read(dead).await?, None);
    assert_eq!(session.read_raw(&session.ttl_key(dead)).await?, None);
    // 无 TTL 键不受影响
    assert_eq!(session.read(live).await?, Some(b"v".to_vec()));
    assert_eq!(session.read_raw(&session.ttl_key(live)).await?, None);
    // 未到期键不受影响
    assert_eq!(session.read(later).await?, Some(b"v".to_vec()));
    assert!(session.pttl_ms(later).await? > 0);

    let st = mgr.stats();
    assert_eq!(st.last_scan_deleted, 1);
    assert_eq!(st.expired_deleted, 1);
    assert_eq!(st.compactions, 0, "未注入紧缩端口时不得计数紧缩");
    // 扫描观测对标 Garnet ExpiredKeyDeletionScan 的 (numExpiredKeysFound,
    // totalRecordsScanned) 双口径：扫描数覆盖删除数，首轮累计即最近一轮
    assert!(
      st.last_scan_scanned >= st.last_scan_deleted,
      "扫描记录数不得小于同轮物理删除数"
    );
    assert_eq!(st.total_scanned, st.last_scan_scanned, "首轮累计即最近一轮");

    // 再跑一轮：空窗零删除（陈旧日志版本被双检放行）；累计扫描数为各轮求和
    mgr.run_once().await?;
    assert_eq!(mgr.stats().last_scan_deleted, 0);
    assert!(
      mgr.stats().total_scanned >= st.total_scanned,
      "累计扫描记录数必须跨轮单调不减"
    );

    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// 测试 2: max_batch_deletes 批预算截断——单轮恰删上限个键，多轮推进游标收敛删完
#[test]
fn test_max_batch_deletes_truncation() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let gc = GcConfig {
      max_batch_deletes: 2,
      ..GcConfig::default()
    };
    let (_dir, store) = open_manual("cap", gc).await?;
    let session = store.new_session()?;
    let mgr = GcManager::new(&store);

    const N: usize = 6;
    for i in 0..N {
      let key = format!("gc:cap:{i}");
      session.upsert(key.as_bytes(), b"v").await?;
      assert_eq!(
        session
          .expire_at(key.as_bytes(), now_ms() + 50, TtlOpt::NONE)
          .await?,
        1
      );
    }
    sleep(Duration::from_millis(120)).await;

    // 三轮各删 2 个（内存探针双检剔除已删键的陈旧日志版本，不饿死批预算）
    for round in 1..=3 {
      mgr.run_once().await?;
      assert_eq!(
        mgr.stats().last_scan_deleted,
        2,
        "第 {round} 轮应恰删批上限个键"
      );
    }
    assert_eq!(mgr.stats().expired_deleted, N as u64);
    for i in 0..N {
      assert!(
        !session
          .contains_key(format!("gc:cap:{i}").as_bytes())
          .await?
      );
    }

    // 第四轮：窗口内只剩陈旧版本，零删除
    mgr.run_once().await?;
    assert_eq!(mgr.stats().last_scan_deleted, 0);

    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// 测试 3: 紧缩阈值触发算法（真 LogCompactor 端口）——未超阈值不触发；
/// 超阈值后单轮触发、begin_address 推进、存活数据完整保留；
/// 落入紧缩区间的过期 TTL 记录经迁移至尾部后由下一轮 sweep 闭环删除（不死键回归测试）
#[test]
fn test_compaction_threshold_trigger() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let gc = GcConfig {
      compaction_interval_ms: 0,
      compaction_max_segments: 1,
      compaction_num_segments: 1,
      ..GcConfig::default()
    };
    let (_dir, store) = open_manual("compact", gc).await?;
    let session = store.new_session()?;
    let mgr = GcManager::new(&store);

    // 短 TTL 键与墓碑键先写入：记录位于日志低位，将落入紧缩区间
    let dead = b"gc:cp:dead";
    session.upsert(dead, b"v").await?;
    assert_eq!(
      session.expire_at(dead, now_ms() + 50, TtlOpt::NONE).await?,
      1
    );
    let gone = b"gc:cp:gone";
    session.upsert(gone, b"v").await?;
    assert!(session.delete(gone).await?);

    // 预热：单条小写入未超阈值（segment_size 回退 page_size=4KB，max_segments=1）
    session.upsert(b"gc:cp:warm", &[b'x'; 100]).await?;
    mgr.run_once().await?;
    assert_eq!(mgr.stats().compactions, 0, "未超阈值不得触发紧缩");

    sleep(Duration::from_millis(120)).await;
    // 写入超过阈值跨度（124KB ≫ 4KB），只读线随换页推进，dead/gone 落入只读区
    for i in 0..120u32 {
      let key = format!("gc:cp:bulk:{i}");
      session.upsert(key.as_bytes(), &vec![b'v'; 1024]).await?;
    }
    let begin = store.begin_address();
    let read_only = store.read_only_address();
    assert!(
      read_only - begin > 4096,
      "前置：日志跨度应已超过阈值，实际 read_only={read_only:#x} begin={begin:#x}"
    );

    mgr.run_once().await?;
    let st = mgr.stats();
    assert_eq!(
      st.expired_deleted, 1,
      "只读线以下的冷区 TTL 同样必须主动回收"
    );
    assert_eq!(st.compactions, 1, "超阈值后应恰好触发一轮紧缩");
    assert!(
      st.last_compact_dropped >= 2,
      "紧缩应丢弃 gone 的数据记录与墓碑"
    );
    assert!(
      store.begin_address() > begin,
      "紧缩后 begin_address 必须推进: {} -> {}",
      begin,
      store.begin_address()
    );

    // 存活数据完整保留（迁移后仍可读），墓碑键保持删除态
    assert_eq!(session.read(b"gc:cp:warm").await?, Some(vec![b'x'; 100]));
    for i in 0..120u32 {
      assert!(
        session
          .contains_key(format!("gc:cp:bulk:{i}").as_bytes())
          .await?,
        "紧缩后 bulk:{i} 不得丢失"
      );
    }
    assert!(!session.contains_key(gone).await?);

    // 过期 TTL 记录与已过期数据记录在紧缩时已被直接判死丢弃（防不死键与永久孤儿），
    // 严禁迁移至尾部；下一轮 sweep 无需重复删除，且键已彻底不存在
    mgr.run_once().await?;
    assert!(!session.contains_key(dead).await?);
    assert_eq!(session.read(dead).await?, None);
    assert_eq!(session.read_raw(&session.ttl_key(dead)).await?, None);

    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// 测试 4: open_shared 显式配置 GC（enabled: true），后台循环物理删除过期键；
/// stop 后循环退出
#[test]
fn test_open_shared_auto_spawn_and_stop() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let device = Arc::new(SegmentedDevice::single_file(dir.path().join("gc_auto.db"))?);
    let mut config = StoreConfig::new(1024, 4096, 16, 0.5)?;
    config.gc = GcConfig {
      enabled: true,
      scan_interval_ms: 30,
      ..GcConfig::default()
    };
    let store = WedbStore::open_shared(config, device)?;
    assert!(
      store.gc_handle().is_some(),
      "open_shared 必须自动启动内置 GC"
    );

    let session = store.new_session()?;
    let key = b"gc:auto:key";
    session.upsert(key, b"v").await?;
    assert_eq!(
      session.expire_at(key, now_ms() + 40, TtlOpt::NONE).await?,
      1
    );

    // 等待后台循环 tick（30ms 间隔，预留 3s 上界）
    for _ in 0..150 {
      if store
        .gc_handle()
        .is_some_and(|h| h.stats().expired_deleted >= 1)
      {
        break;
      }
      sleep(Duration::from_millis(20)).await;
    }
    let handle = store.gc_handle().expect("GC 句柄必须存在");
    assert!(
      handle.stats().expired_deleted >= 1,
      "后台循环应在数个间隔内物理删除过期键"
    );
    assert!(!session.contains_key(key).await?);
    assert_eq!(session.read_raw(&session.ttl_key(key)).await?, None);

    // stop 后循环至多一个间隔内退出
    handle.stop();
    for _ in 0..200 {
      if handle.is_finished() {
        break;
      }
      sleep(Duration::from_millis(10)).await;
    }
    assert!(handle.is_finished(), "stop 后后台循环必须退出");

    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// 冷区过期回收不依赖紧缩端口；记录预算限制全为非 TTL 的扫描批次。
#[test]
fn test_cold_expiration_with_scan_budget() -> Void {
  Runtime::new()?.block_on(async {
    let gc = GcConfig {
      max_scan_records: 1,
      compaction_max_segments: 0,
      ..GcConfig::default()
    };
    let (_dir, store) = open_manual("cold_budget", gc).await?;
    let session = store.new_session()?;
    for i in 0..5 {
      session
        .upsert(format!("prefix:{i}").as_bytes(), b"v")
        .await?;
    }
    let key = b"cold:expired";
    session.upsert(key, b"v").await?;
    session.expire_at(key, now_ms() + 50, TtlOpt::NONE).await?;
    let ttl_addr_limit = store.tail_address();
    for i in 0..120 {
      session
        .upsert(format!("bulk:{i}").as_bytes(), &[b'v'; 1024])
        .await?;
    }
    assert!(store.read_only_address() > ttl_addr_limit);
    sleep(Duration::from_millis(120)).await;
    let mgr = GcManager::new(&store);
    // 冷区预算 max_scan_records=1：冷区积压远超 5 轮，每轮 scanned 至少含冷区
    // 预算的 1 条；total_scanned 必须恰为各轮 scanned 求和（对标 Garnet
    // totalRecordsScanned 的两段累加口径）
    let mut scanned_sum = 0u64;
    for _ in 0..5 {
      mgr.run_once().await?;
      assert_eq!(mgr.stats().expired_deleted, 0, "记录预算应让扫描分轮执行");
      assert!(
        mgr.stats().last_scan_scanned >= 1,
        "冷区预算为 1 且积压未清时单轮扫描不得为空"
      );
      scanned_sum += mgr.stats().last_scan_scanned;
    }
    assert_eq!(
      mgr.stats().total_scanned,
      scanned_sum,
      "累计扫描数必须为各轮求和"
    );
    for _ in 0..32 {
      mgr.run_once().await?;
      if mgr.stats().expired_deleted == 1 {
        break;
      }
    }
    assert_eq!(mgr.stats().expired_deleted, 1, "游标必须覆盖磁盘冷区");
    assert!(
      mgr.stats().total_scanned > scanned_sum,
      "删除轮的扫描数必须继续计入累计值"
    );
    assert_eq!(session.read_raw(&session.ttl_key(key)).await?, None);
    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// 热区窗口优先语义（对标 Garnet 滑动窗口低延迟）：冷区存在积压（冷区预算
/// max_scan_records=1 逐条推进）时，位于只读线之上热区窗口内的过期键至多
/// 一轮即被物理清除，不受冷区积压制约
#[test]
fn test_hot_window_priority_over_cold_backlog() -> Void {
  Runtime::new()?.block_on(async {
    let gc = GcConfig {
      max_scan_records: 1,
      ..GcConfig::default()
    };
    let (_dir, store) = open_manual("hot_pri", gc).await?;
    let session = store.new_session()?;

    // 冷区积压：32KB 非 TTL 批量写入越过只读线（游标逐条推进需数十轮）
    for i in 0..32 {
      session
        .upsert(format!("bulk:{i}").as_bytes(), &[b'v'; 1024])
        .await?;
    }
    // 热区短 TTL 键：最后写入，位于只读线之上（窗口内），50ms 后过期
    let hot = b"hot:expired";
    session.upsert(hot, b"v").await?;
    assert_eq!(
      session.expire_at(hot, now_ms() + 50, TtlOpt::NONE).await?,
      1
    );
    assert!(
      store.read_only_address() < store.tail_address(),
      "前置：热区窗口必须非空"
    );
    sleep(Duration::from_millis(120)).await;

    let mgr = GcManager::new(&store);
    mgr.run_once().await?;
    assert_eq!(
      mgr.stats().last_scan_deleted,
      1,
      "热区过期键必须一轮内清除，不受冷区积压影响"
    );
    assert!(!session.contains_key(hot).await?);
    assert_eq!(session.read_raw(&session.ttl_key(hot)).await?, None);

    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// 运行态配置热更新（对标 Garnet CONFIG SET）：update_gc_config 在 GC 未启动时
/// 持久生效（start_gc 按其 enabled 判定），启动后修改每轮重读即时生效
#[test]
fn test_gc_config_hot_update() -> Void {
  Runtime::new()?.block_on(async {
    let (_dir, store) = open_manual("hotcfg", GcConfig::default()).await?;
    assert!(store.gc_handle().is_none(), "前置：GC 未启动");
    assert!(!store.gc_config().enabled);

    // 未启动时热更新：enabled 置位后 start_gc 直接生效
    store.update_gc_config(|c| {
      c.enabled = true;
      c.scan_interval_ms = 30;
    });
    assert_eq!(store.gc_config().scan_interval_ms, 30);
    assert!(store.start_gc(), "热更新 enabled 后 start_gc 必须成功");
    assert!(store.gc_handle().is_some());

    // 运行中热更新：GcManager 与 store 共享同一配置句柄，快照即时一致
    store.update_gc_config(|c| c.scan_interval_ms = 250);
    assert_eq!(store.gc_config().scan_interval_ms, 250);

    store.gc_handle().unwrap().stop();
    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// 执行闸保护与取消恢复测试：
/// 1. 当执行闸被持有时（例如上一轮未结束或中途挂起），并发调用 run_once 必须被跳过；
/// 2. 任务取消触发 RunGuard::drop 释放执行闸后，下一轮 run_once 必须能成功获取执行闸并完成紧缩。
#[test]
fn test_cancelled_round_releases_gate() -> Void {
  Runtime::new()?.block_on(async {
    let gc = GcConfig {
      compaction_interval_ms: 0,
      compaction_max_segments: 1,
      compaction_num_segments: 1,
      ..GcConfig::default()
    };
    let (_dir, store) = open_manual("cancel", gc).await?;
    let session = store.new_session()?;
    for i in 0..120 {
      session
        .upsert(format!("bulk:{i}").as_bytes(), &[b'v'; 1024])
        .await?;
    }
    let mgr = GcManager::new(&store);

    // 1. 模拟执行中被挂起并持有执行闸 (RunGuard)
    {
      let _guard = mgr.try_acquire_gate().expect("初始状态必须成功获取执行闸");
      assert!(mgr.is_inflight(), "持有守卫期间执行闸必须为 true");

      // 执行闸持有期间，并发 run_once 必须被跳过，紧缩次数为 0
      mgr.run_once().await?;
      assert_eq!(
        mgr.stats().compactions,
        0,
        "处于执行闸保护中时并发调用必须跳过"
      );
      // 作用域结束，_guard 自动 Drop，模拟任务取消/终止时的 RAII 清理
    }

    // 2. 验证执行闸已安全释放
    assert!(!mgr.is_inflight(), "RunGuard Drop 后执行闸必须已释放");

    // 3. 执行闸释放后，下一轮 run_once 必须能成功获取执行闸并完成紧缩
    mgr.run_once().await?;
    assert_eq!(
      mgr.stats().compactions,
      1,
      "执行闸释放后下一轮必须能成功执行紧缩"
    );
    aok::Result::<()>::Ok(())
  })?;
  OK
}
