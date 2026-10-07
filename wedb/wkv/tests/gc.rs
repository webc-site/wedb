#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! 内置 GC 后台管理器集成测试
//!
//! 覆盖：主动过期扫描的物理删除（数据 + raw 层 TTL 记录墓碑化）、
//! max_batch_deletes 批预算截断与游标推进收敛、热区候选 cap 封顶多轮收敛（含反证锁测案）、
//! 冷区删除失败游标滞留与重扫收敛、
//! 紧缩阈值触发算法（真 LogCompactor 端口适配，未超阈值不触发）、open_shared 自动启动与 stop 退出、
//! 「禁用即停」判定三处入口（drive / spawn / reconcile_gc_scan）同一结论。
//!
//! 在 garnet 中的相对路径: test/standalone/Garnet.test/ExpiredKeyDeletionTests.cs（后台过期物理删除）+ libs/server/StoreWrapper.cs 过期调度

use std::{
  io,
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
  },
  time::Duration,
};

use aok::{OK, Void};
use compio::{
  runtime::{Runtime, spawn},
  time::sleep,
};
use tempfile::{TempDir, tempdir};
use wbase::{
  cfg::LogCompactionType,
  convert::{TICKS_PER_MILLISECOND, TICKS_PER_SECOND},
  pool::{AlignedBuf, BufferPool},
  time::now_ticks,
};
use wdev::{Device, Error as WdevError, SegmentedDevice};
use wkv::{
  GcConfig, GcManager, StoreConfig, TtlOpt, WedbStore,
  vdb::{GcDeadEntry, GcDeadLog},
};

/// 构造独立临时库（page 4KB / 16 页，GC 配置注入但强制 enabled=false 由测试手动驱动）
async fn open_manual(
  tag: &str,
  gc: GcConfig,
) -> aok::Result<(TempDir, Arc<WedbStore<SegmentedDevice>>)> {
  let dir = tempdir()?;
  let device = Arc::new(SegmentedDevice::new(
    dir.path().join(format!("gc_{tag}.db")),
    4096,
    4096,
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
    assert!(!store.gc_running(), "enabled=false 时不得自动启动 GC");
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
      session
        .expire_at(dead, now_ticks() + TICKS_PER_MILLISECOND * 50, TtlOpt::NONE)
        .await?,
      1
    );
    assert_eq!(
      session
        .expire_at(later, now_ticks() + TICKS_PER_SECOND * 60, TtlOpt::NONE)
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

/// 测试 2: max_batch_deletes 批预算截断——冷区欠账单轮恰删上限个键，多轮推进游标收敛删完
#[test]
fn test_max_batch_deletes_truncation() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let gc = GcConfig {
      max_batch_deletes: 2,
      max_scan_records: 1000,
      compaction_max_segments: 0,
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
          .expire_at(
            key.as_bytes(),
            now_ticks() + TICKS_PER_MILLISECOND * 50,
            TtlOpt::NONE
          )
          .await?,
        1
      );
    }
    let ttl_addr_limit = store.tail_address();
    // 写入 bulk 记录推动只读线越过全部过期键，使其完整落入冷区欠账区间
    for i in 0..120 {
      session
        .upsert(format!("gc:cap:bulk:{i}").as_bytes(), &[b'v'; 1024])
        .await?;
    }
    assert!(
      store.read_only_address() > ttl_addr_limit,
      "前置：只读线必须越过全部过期键记录"
    );
    sleep(Duration::from_millis(120)).await;

    // 三轮各删 2 个（冷区受 max_batch_deletes 约束）
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

/// 测试 2b: 热区 cap 截断多轮收敛——热区过期键数超过 max_batch_deletes 时单轮物理
/// 删除恰为 cap（本票翻转旧语义锁测「热区全量清零单轮完成」），余量凭下轮无游标
/// 全窗重扫天然再入候选，各轮 ≤ cap 至删尽（双检幂等，热区优先语义留痕）
#[test]
fn test_hot_window_batch_cap_multi_round_convergence() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let gc = GcConfig {
      max_batch_deletes: 2,
      ..GcConfig::default()
    };
    let (_dir, store) = open_manual("hot_cap", gc).await?;
    let session = store.new_session()?;
    let mgr = GcManager::new(&store);

    const N: usize = 6;
    for i in 0..N {
      let key = format!("gc:hot:{i}");
      session.upsert(key.as_bytes(), b"v").await?;
      assert_eq!(
        session
          .expire_at(
            key.as_bytes(),
            now_ticks() + TICKS_PER_MILLISECOND * 50,
            TtlOpt::NONE
          )
          .await?,
        1
      );
    }
    // 未写入 bulk 记录，所有记录均位于内存热区 [read_only, tail) 内
    assert!(
      store.read_only_address() < store.tail_address(),
      "前置：热区窗口必须非空"
    );
    sleep(Duration::from_millis(120)).await;

    // 首轮恰删 cap：热区候选收集受 max_batch_deletes 封顶，不再单轮全清
    mgr.run_once().await?;
    assert_eq!(
      mgr.stats().last_scan_deleted,
      2,
      "热区单轮物理删除必须恰为批上限，不得穿透 max_batch_deletes"
    );

    // 后续轮各删 ≤ cap，全窗重扫天然续收余量直至删尽
    for _ in 0..8 {
      if mgr.stats().expired_deleted >= N as u64 {
        break;
      }
      mgr.run_once().await?;
      assert!(
        mgr.stats().last_scan_deleted <= 2,
        "后续轮单轮删除数不得超过批上限"
      );
    }
    assert_eq!(
      mgr.stats().expired_deleted,
      N as u64,
      "多轮收敛必须最终清空全部热区过期键"
    );
    for i in 0..N {
      assert!(
        !session
          .contains_key(format!("gc:hot:{i}").as_bytes())
          .await?
      );
    }

    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// 反证敏感锁测（本票新案）：热区预置过期键数（5）> cap（2）时单轮物理删除 ≤ cap
/// 且首轮恰为 cap；后续轮重扫各删 ≤ cap 至清零残留，收敛后空轮零删除。
/// 回退修复（热区 max_picks 恢复 usize::MAX）首轮即删 5 键转红
#[test]
fn test_hot_zone_deletes_capped_by_max_batch_deletes() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let gc = GcConfig {
      max_batch_deletes: 2,
      ..GcConfig::default()
    };
    let (_dir, store) = open_manual("hot_pierce", gc).await?;
    let session = store.new_session()?;
    let mgr = GcManager::new(&store);

    const N: usize = 5;
    for i in 0..N {
      let key = format!("gc:pierce:{i}");
      session.upsert(key.as_bytes(), b"v").await?;
      assert_eq!(
        session
          .expire_at(
            key.as_bytes(),
            now_ticks() + TICKS_PER_MILLISECOND * 50,
            TtlOpt::NONE
          )
          .await?,
        1
      );
    }
    assert!(
      store.read_only_address() < store.tail_address(),
      "前置：热区窗口必须非空"
    );
    sleep(Duration::from_millis(120)).await;

    // 首轮恰删 cap：现状穿透形（单轮删尽 5）在此断言转红
    mgr.run_once().await?;
    assert_eq!(
      mgr.stats().last_scan_deleted,
      2,
      "首轮物理删除数必须恰为 max_batch_deletes"
    );

    // 续轮各删 ≤ cap 至全清零残留
    let mut rounds = 0usize;
    while mgr.stats().expired_deleted < N as u64 {
      rounds += 1;
      assert!(rounds <= 8, "多轮收敛须有上界，不得空转");
      mgr.run_once().await?;
      assert!(
        mgr.stats().last_scan_deleted <= 2,
        "任一轮物理删除数均不得越过 max_batch_deletes"
      );
    }
    assert_eq!(mgr.stats().expired_deleted, N as u64);
    for i in 0..N {
      assert!(
        !session
          .contains_key(format!("gc:pierce:{i}").as_bytes())
          .await?
      );
    }

    // 收敛后空轮：陈旧版本被双检放行，零删除
    mgr.run_once().await?;
    assert_eq!(
      mgr.stats().last_scan_deleted,
      0,
      "清零后重扫必须零删除无残留"
    );

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
      compaction_max_segments: 1,
      compaction_type: LogCompactionType::Lookup,
      ..GcConfig::default()
    };
    let (_dir, store) = open_manual("compact", gc).await?;
    let session = store.new_session()?;
    let mgr = GcManager::new(&store);

    // 短 TTL 键与墓碑键先写入：记录位于日志低位，将落入紧缩区间
    let dead = b"gc:cp:dead";
    session.upsert(dead, b"v").await?;
    assert_eq!(
      session
        .expire_at(dead, now_ticks() + TICKS_PER_MILLISECOND * 50, TtlOpt::NONE)
        .await?,
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
      session.upsert(key.as_bytes(), &[b'v'; 1024]).await?;
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

/// None 档关闭常规阈值紧缩（对标 C# DoCompactionAsync 首行判 None 短路；
/// CONFIG SET compaction-type none 语义）：超阈值也不触发紧缩，过期扫描不受影响
#[test]
fn test_compaction_type_none_disables() -> Void {
  Runtime::new()?.block_on(async {
    // compaction_type 缺省即 None（对标 C# GarnetServerOptions 默认）
    let gc = GcConfig {
      compaction_max_segments: 1,
      ..GcConfig::default()
    };
    let (_dir, store) = open_manual("none_off", gc).await?;
    let session = store.new_session()?;
    let mgr = GcManager::new(&store);

    for i in 0..120u32 {
      session
        .upsert(format!("gc:none:bulk:{i}").as_bytes(), &[b'v'; 1024])
        .await?;
    }
    let begin = store.begin_address();
    assert!(
      store.read_only_address() - begin > 4096,
      "前置：日志跨度应已超过阈值"
    );

    mgr.run_once().await?;
    let st = mgr.stats();
    assert_eq!(st.compactions, 0, "None 档不得触发常规紧缩");
    assert_eq!(
      store.begin_address(),
      begin,
      "None 档 begin_address 不得推进"
    );
    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// None 档 + 熔断旁路：死亡虚拟 ID 账本积压超高水位时，None 关闭不了换号物理
/// 回收安全机制（doc/zh/db.md「高低水位熔断」承诺），以 Lookup 活性校验档全速
/// 紧缩推进 begin（共享单日志混有他库活记录，绝不可以 Shift 旁路）
#[test]
fn test_compaction_type_none_boost_bypass() -> Void {
  Runtime::new()?.block_on(async {
    let gc = GcConfig {
      compaction_max_segments: 2,
      gc_dead_high_watermark: 4,
      gc_dead_low_watermark: 2,
      ..GcConfig::default()
    };
    let (_dir, store) = open_manual("none_boost", gc).await?;
    let mgr = GcManager::new(&store);
    let dead = &store.vdb.gc_dead;
    // 假死亡条目：expired_at/tail_address 双保险取极值，防被 sweep_vdb 提前摘除
    for id in 0..5u64 {
      dead.insert(
        id,
        GcDeadEntry {
          expired_at: i64::MAX,
          tail_address: u64::MAX,
          vns: None,
        },
      );
    }
    hiwat_bulk(&store).await?;
    let ro = store.read_only_address();
    assert!(
      ro - store.begin_address() > 2 * 4096,
      "前置：日志跨度应超过段数阈值"
    );

    mgr.run_once().await?;
    let st = mgr.stats();
    assert!(st.compact_boosting, "积压超高水位必须置位熔断");
    assert_eq!(st.compactions, 1, "None 档熔断态必须旁路执行紧缩");
    assert!(
      store.begin_address() >= ro,
      "熔断全速回退须推进 begin 至只读线（until = read_only）"
    );
    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// Shift 档（对标 C# ShiftBeginAddress(untilAddress, true, …)，数据丢弃档）：
/// 不搬记录直接推进 begin，移位区间内活记录一并丢弃、只读线以上存活；
/// until 恒钳制在只读线以下至少一段（回退段数钳 max-1），绝不全移位
#[test]
fn test_compaction_type_shift_drops_below_until() -> Void {
  Runtime::new()?.block_on(async {
    let gc = GcConfig {
      compaction_max_segments: 2,
      compaction_type: LogCompactionType::Shift,
      ..GcConfig::default()
    };
    let (_dir, store) = open_manual("shift", gc).await?;
    let session = store.new_session()?;
    let mgr = GcManager::new(&store);

    // 低位标记键（将落入移位区间，Shift 档下即使活记录也被丢弃）
    let marker = b"gc:shift:marker";
    session.upsert(marker, b"v").await?;
    for i in 0..120u32 {
      session
        .upsert(format!("gc:shift:bulk:{i}").as_bytes(), &[b'v'; 1024])
        .await?;
    }
    // 高位键（位于只读线附近，必须在移位后存活）
    let tail_key = b"gc:shift:tail";
    session.upsert(tail_key, b"v").await?;

    let begin = store.begin_address();
    let read_only = store.read_only_address();
    assert!(read_only - begin > 2 * 4096, "前置：日志跨度应超过段数阈值");

    mgr.run_once().await?;
    let st = mgr.stats();
    assert_eq!(st.compactions, 1, "Shift 档超阈值应触发一轮移位");
    assert_eq!(st.last_compact_dropped, 0, "移位不搬记录，无紧缩丢弃统计");
    // until = read_only - segment_size × (max - min(n, max-1)) = read_only - 1 段
    assert_eq!(
      store.begin_address(),
      read_only - 4096,
      "Shift 档 begin 必须精确推进到只读线下一段（begin={}，只读线={read_only:#x}）",
      store.begin_address()
    );
    // 移位区间内：活记录一并丢失（数据丢弃档语义，与 Lookup 档的本质区别）
    assert_eq!(
      session.read(marker).await?,
      None,
      "移位越过的活记录必须被丢弃（Shift 数据丢弃语义）"
    );
    assert_eq!(
      session.read(b"gc:shift:bulk:0").await?,
      None,
      "低位批量记录同样被移位丢弃"
    );
    // 只读线附近存活
    assert_eq!(session.read(tail_key).await?, Some(b"v".to_vec()));
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
    assert!(store.gc_running(), "open_shared 必须自动启动内置 GC");

    let session = store.new_session()?;
    let key = b"gc:auto:key";
    session.upsert(key, b"v").await?;
    assert_eq!(
      session
        .expire_at(key, now_ticks() + TICKS_PER_MILLISECOND * 40, TtlOpt::NONE)
        .await?,
      1
    );

    // 等待后台循环 tick（30ms 间隔，预留 3s 上界）
    for _ in 0..150 {
      if store.gc_stats().is_some_and(|s| s.expired_deleted >= 1) {
        break;
      }
      sleep(Duration::from_millis(20)).await;
    }
    assert!(
      store.gc_stats().is_some_and(|s| s.expired_deleted >= 1),
      "后台循环应在数个间隔内物理删除过期键"
    );
    assert!(!session.contains_key(key).await?);
    assert_eq!(session.read_raw(&session.ttl_key(key)).await?, None);

    // stop 后循环至多一个间隔内退出
    store.stop_gc();
    for _ in 0..200 {
      if !store.gc_running() {
        break;
      }
      sleep(Duration::from_millis(10)).await;
    }
    assert!(!store.gc_running(), "stop 后后台循环必须退出");

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
    session
      .expire_at(key, now_ticks() + TICKS_PER_MILLISECOND * 50, TtlOpt::NONE)
      .await?;
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
      session
        .expire_at(hot, now_ticks() + TICKS_PER_MILLISECOND * 50, TtlOpt::NONE)
        .await?,
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
/// 持久生效（start_gc 按其 enabled 判定），启动后修改每轮重读即时生效；
/// 禁用（enabled=false / 间隔 0）循环退出后 start_gc 按新配置重拉
///（对标 C# ReconcilePrimaryTask 停任务 → 按新间隔 RegisterAndRun）
#[test]
fn test_gc_config_hot_update() -> Void {
  Runtime::new()?.block_on(async {
    let (_dir, store) = open_manual("hotcfg", GcConfig::default()).await?;
    assert!(!store.gc_running(), "前置：GC 未启动");
    assert!(!store.gc_config().enabled);

    // 未启动时热更新：enabled 置位后 start_gc 直接生效
    store.update_gc_config(|c| {
      c.enabled = true;
      c.scan_interval_ms = 30;
    });
    assert_eq!(store.gc_config().scan_interval_ms, 30);
    assert!(store.start_gc(), "热更新 enabled 后 start_gc 必须成功");
    assert!(store.gc_running());

    // 运行中热更新：GcManager 与 store 共享同一配置句柄，快照即时一致
    store.update_gc_config(|c| c.scan_interval_ms = 250);
    assert_eq!(store.gc_config().scan_interval_ms, 250);

    // 禁用热更新 → 循环下一轮检点退出（禁用即停，对标 CancelAsync）
    store.update_gc_config(|c| c.enabled = false);
    for _ in 0..200 {
      if !store.gc_running() {
        break;
      }
      sleep(Duration::from_millis(10)).await;
    }
    assert!(!store.gc_running(), "禁用后后台循环必须退出");

    // 停止后按新配置重拉
    store.update_gc_config(|c| {
      c.enabled = true;
      c.scan_interval_ms = 40;
    });
    assert!(store.start_gc(), "禁用退出后 start_gc 必须重拉循环");
    assert!(store.gc_running());

    store.stop_gc();
    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// 轮询等待条件成立（内置 GC 循环为协作式退出，至多再运行一个扫描间隔；2s 上界）
async fn wait_until(pred: impl Fn() -> bool) -> bool {
  for _ in 0..200 {
    if pred() {
      return true;
    }
    sleep(Duration::from_millis(10)).await;
  }
  pred()
}

/// 「禁用即停」判定真值表：仅开关开启且间隔 > 0 判「运行」，开关关闭与间隔清零
/// 两类禁用态同判「不运行」（默认配置对标 Garnet `ExpiredKeyDeletionScanFrequencySecs
/// = -1` 亦不运行）；判定与间隔口径同源，判「运行」的快照间隔恒不低于忙轮询下限
#[test]
fn test_gc_scan_predicate_disabled_states() {
  let zero_interval = GcConfig {
    enabled: true,
    scan_interval_ms: 0,
    ..GcConfig::default()
  };
  let switch_off = GcConfig {
    enabled: false,
    scan_interval_ms: 1_000,
    ..GcConfig::default()
  };
  let effective = GcConfig {
    enabled: true,
    scan_interval_ms: 1_000,
    ..GcConfig::default()
  };
  assert!(
    !wkv::enabled_by_config(&zero_interval),
    "间隔清零须判为不运行"
  );
  assert!(!wkv::enabled_by_config(&switch_off), "开关关闭须判为不运行");
  assert!(
    !wkv::enabled_by_config(&GcConfig::default()),
    "默认配置须判为不运行（惰性过期兜底）"
  );
  assert!(
    wkv::enabled_by_config(&effective),
    "开关开启且间隔 > 0 才运行"
  );
  assert!(
    wkv::scan_interval_ms(&effective) >= wkv::MIN_SCAN_INTERVAL_MS,
    "判「运行」的快照间隔不得低于忙轮询下限"
  );
}

/// 「禁用即停」判定三处入口同一结论（判定单点 `wkv/src/gc.rs:enabled_by_config`，
/// 对标 C# StoreWrapper.cs:ReconcilePrimaryTask + TryStartExpiredKeyDeletionTask
/// 的单点开关判定）：两类禁用配置（`enabled = true` 而 `scan_interval_ms = 0`、
/// `enabled = false` 而 `scan_interval_ms > 0`）下 [`GcManager::drive`] 强引用循环
/// 立即返回、[`GcManager::spawn`] 内置循环首轮即退出、
/// [`WedbStore::reconcile_gc_scan`] 返回 false 且循环不在跑；有效配置三处同判
/// 「运行」，且热更新置禁用后两条循环各在一个间隔内退出
#[test]
fn test_gc_scan_predicate_single_point() -> Void {
  Runtime::new()?.block_on(async {
    // (用例标签, 运行态配置, 期望：后台循环应否在跑)
    let cases = [
      (
        "zerointerval",
        GcConfig {
          enabled: true,
          scan_interval_ms: 0,
          ..GcConfig::default()
        },
        false,
      ),
      (
        "disabled",
        GcConfig {
          enabled: false,
          scan_interval_ms: 1_000,
          ..GcConfig::default()
        },
        false,
      ),
      (
        "effective",
        GcConfig {
          enabled: true,
          scan_interval_ms: 20,
          ..GcConfig::default()
        },
        true,
      ),
    ];
    for (label, gc, running) in cases {
      let (_dir, store) = open_manual(label, gc.clone()).await?;
      let (enabled, interval) = (gc.enabled, gc.scan_interval_ms);

      // 入口一：drive 强引用循环的循环内判定
      store.update_gc_config(|c| *c = gc.clone());
      let join = {
        let mgr = Arc::new(GcManager::new(&store));
        spawn(async move { mgr.drive().await })
      };
      if running {
        // 有效配置：循环刚拉起必未退出（存活方向即刻可判，无需等待窗）
        assert!(
          !join.is_finished(),
          "{label}：有效配置下 drive 循环必须在跑"
        );
      } else {
        // 禁用配置：协作式退出沿轮询预算等齐，固定 80ms 单发窗在慢机上会假红
        assert!(
          wait_until(|| join.is_finished()).await,
          "{label}：禁用配置下 drive 循环必须退出"
        );
      }
      store.update_gc_config(|c| c.enabled = false);
      assert!(
        wait_until(|| join.is_finished()).await,
        "{label}：热更新置禁用后 drive 循环必须退出"
      );

      // 入口二：spawn 内置弱引用循环（绕过 reconcile 直取循环内判定）
      store.update_gc_config(|c| *c = gc.clone());
      let handle = GcManager::spawn(Arc::clone(&store));
      if running {
        // 刚 spawn 未收敛且未请求退出，is_active 即刻为真（确定性判定）
        assert!(
          handle.is_active(),
          "{label}：有效配置下 spawn 内置循环必须在跑"
        );
      } else {
        assert!(
          wait_until(|| handle.is_finished()).await,
          "{label}：禁用配置下 spawn 内置循环必须退出"
        );
      }
      store.update_gc_config(|c| c.enabled = false);
      assert!(
        wait_until(|| handle.is_finished()).await,
        "{label}：热更新置禁用后 spawn 内置循环必须退出"
      );
      handle.stop();

      // 入口三：reconcile_gc_scan 外部拉起判定（含 start_gc 的同源转发）
      store.update_gc_config(|c| *c = gc.clone());
      assert_eq!(
        store.reconcile_gc_scan(enabled, Some(interval)),
        running,
        "{label}：reconcile_gc_scan 返回值须与判定一致"
      );
      assert_eq!(
        store.gc_running(),
        running,
        "{label}：reconcile_gc_scan 后循环在跑态须与判定一致"
      );
      store.stop_gc();
    }
    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// 写入 48KB 非 TTL 批量记录，推动只读线越过段数阈值（2×4KB）
async fn hiwat_bulk(store: &Arc<WedbStore<SegmentedDevice>>) -> aok::Result<()> {
  let session = store.new_session()?;
  for i in 0..48u32 {
    session
      .upsert(format!("gc:hiwat:bulk:{i}").as_bytes(), &[b'v'; 1024])
      .await?;
  }
  Ok(())
}

/// 高低水位熔断三态（落地 doc/zh/db.md「高低水位熔断」承诺）：gc_dead 积压在
/// 迟滞死区内不加速（低水位不触发，回退段数恒 1 不贴只读线）；超高水位置位熔断——
/// 全速回退（until 推进到只读线）；回落低水位以下自动退出加速态
#[test]
fn test_gc_dead_watermark_circuit_breaker() -> Void {
  Runtime::new()?.block_on(async {
    let gc = GcConfig {
      // 阈值 2 段（2×4KB）：常规回退留下 1 段积压，熔断全速回退
      compaction_max_segments: 2,
      compaction_type: LogCompactionType::Lookup,
      gc_dead_high_watermark: 4,
      gc_dead_low_watermark: 2,
      ..GcConfig::default()
    };
    let (_dir, store) = open_manual("hiwat", gc).await?;
    let mgr = GcManager::new(&store);
    let dead = &store.vdb.gc_dead;
    // 假死亡条目：expired_at/tail_address 双保险取极值，防被 sweep_vdb 提前摘除
    let inject = |dead: &GcDeadLog, id: u64| {
      dead.insert(
        id,
        GcDeadEntry {
          expired_at: i64::MAX,
          tail_address: u64::MAX,
          vns: None,
        },
      );
    };
    // 前置超限 + len=0：首轮常规判定，单步回退留下积压段
    hiwat_bulk(&store).await?;
    let begin0 = store.begin_address();
    let ro0 = store.read_only_address();
    mgr.run_once().await?;
    let st0 = mgr.stats();
    assert!(!st0.compact_boosting, "空队列不得触发熔断");
    assert_eq!(st0.compactions, 1, "超阈值首轮应常规触发紧缩");
    assert!(
      store.begin_address() < ro0,
      "常规回退须留下段数积压（until = read_only - 1 段）"
    );
    assert!(store.begin_address() > begin0);

    // 态 1（低水位/死区内）：len=0 仍在死区 → 不加速，每轮判定均按常规单步回退
    // 推进（判定无节流，节奏即轮次本身），begin 恒不贴只读线
    let ro1 = store.read_only_address();
    mgr.run_once().await?;
    let st1 = mgr.stats();
    assert!(!st1.compact_boosting, "死区内不得触发熔断");
    assert!(
      store.begin_address() < ro1,
      "常规态回退段数为 1，绝不全速贴至只读线"
    );

    // 态 2（高水位触发）：len=5 > hi=4 → 置位熔断：回退段数提升至 max，
    // 全速回退推进 begin 至只读线
    for id in 0..5u64 {
      inject(dead, id);
    }
    assert_eq!(dead.len(), 5);
    hiwat_bulk(&store).await?;
    let ro2 = store.read_only_address();
    assert!(
      ro2 - store.begin_address() > 2 * 4096,
      "前置：日志跨度应再次超过段数阈值"
    );
    mgr.run_once().await?;
    let st2 = mgr.stats();
    assert!(st2.compact_boosting, "积压超高水位必须置位熔断");
    assert_eq!(
      st2.compactions,
      st1.compactions + 1,
      "熔断态必须执行一轮紧缩"
    );
    assert!(
      store.begin_address() >= ro2,
      "熔断全速回退须推进 begin 至只读线（until = read_only）"
    );

    // 态 3（回落退出）：len=2 <= lo=2 → 清位熔断，退出加速态
    for id in 2..5u64 {
      dead.remove(&id);
    }
    assert_eq!(dead.len(), 2);
    mgr.run_once().await?;
    let st3 = mgr.stats();
    assert!(!st3.compact_boosting, "回落低水位必须退出熔断");

    // 态 4（迟滞死区维持原态）：len=3 落在 (lo, hi] 死区，熔断位不得抖动
    inject(dead, 5);
    assert_eq!(dead.len(), 3);
    mgr.run_once().await?;
    assert!(!mgr.stats().compact_boosting, "死区必须维持清零态不抖动");

    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// 熔断 + Shift 档归一锁测（三面文档承诺：门面头注 / 高水位文档 / try_compact
/// 方法文档）：compaction_type=Shift 且死亡账本积压超高水位置位熔断时，Shift
/// 移位臂必须经 `!boosting` 门让位，下沉归一臂以 Lookup 活性校验档全速回退——
/// 熔断态多出的那一段（移位终点由 safe_ro-(max-1)·seg 抬到 safe_ro-seg）内
/// 他活域预写键必须判活搬运存活，不得被 begin 无条件越过。
/// revert-proof：撤 Shift 分支的 `!boosting` 门后「他活域键仍在」断言必转红
#[test]
fn test_gc_shift_boost_normalizes_to_lookup() -> Void {
  Runtime::new()?.block_on(async {
    let gc = GcConfig {
      compaction_max_segments: 2,
      compaction_type: LogCompactionType::Shift,
      gc_dead_high_watermark: 4,
      gc_dead_low_watermark: 2,
      ..GcConfig::default()
    };
    let (_dir, store) = open_manual("shift_boost", gc).await?;
    let session = store.new_session()?;
    let mgr = GcManager::new(&store);
    let dead = &store.vdb.gc_dead;

    // 他活域预写键置于日志低位：落入熔断态移位终点（safe_ro-1 段）相对常规
    // 终点（safe_ro-(max-1) 段）多出的未判活区间——归一 Lookup 臂判活搬运，
    // 旁落移位臂则随段截断无条件出局
    let marker = b"gc:shift:boost:marker";
    session.upsert(marker, b"v").await?;
    hiwat_bulk(&store).await?;
    // 假死亡条目：tail_address 取 0（低于现 begin，注销门不拦）、expired_at
    // 取极值使账本只增不减，熔断位在本轮判定内稳定为真
    for id in 0..5u64 {
      dead.insert(
        id,
        GcDeadEntry {
          expired_at: i64::MAX,
          tail_address: 0,
          vns: None,
        },
      );
    }
    let ro = store.read_only_address();
    assert!(
      ro - store.begin_address() > 2 * 4096,
      "前置：日志跨度应超过段数阈值"
    );

    mgr.run_once().await?;
    let st = mgr.stats();
    assert!(st.compact_boosting, "积压超高水位必须置位熔断");
    assert_eq!(st.compactions, 1, "熔断态必须执行一轮紧缩");
    // 核心锁测断言：归一为 Lookup 活性校验档后他活域键必须存活
    assert_eq!(
      session.read(marker).await?,
      Some(b"v".to_vec()),
      "熔断态 Shift 移位臂必须让位归一 Lookup，他活域键不得被未判活移位越过（撤 !boosting 门即转红）"
    );
    assert!(
      store.begin_address() >= ro,
      "熔断归一须按全速回退推进 begin 至只读线（until = safe_ro）"
    );

    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// gc.enabled=false（默认，后台扫描循环缺席）时，换号物理回收四面仍由常驻回收
/// 兜底入口 [`GcManager::reclaim_when_scan_idle`] 无条件推进：到期且 begin 已越界的
/// 死亡账本条目被弹出注销、退役租户路由随之释放。证明物理回收不随扫描开关关停
/// （本单解耦验收）。扫描循环在跑时（enabled=true）其 tick 已含同一内核，本入口让位
#[test]
fn test_reclaim_when_scan_idle_reclaims_with_scan_disabled() -> Void {
  Runtime::new()?.block_on(async {
    // open_manual 强制 enabled=false 且间隔 0：后台扫描循环不存在
    let (_dir, store) = open_manual("idle_reclaim", GcConfig::default()).await?;
    assert!(!store.gc_running(), "前置：扫描循环未启动（禁用即停）");
    let mgr = GcManager::new(&store);
    let dead = &store.vdb.gc_dead;
    // 到期（expired_at 0 <= now）且 begin 已越界（tail_address 0 <= begin）的退役租户：
    // vns 为 None 走命名空间级回收，弹出时同步 pin_routing.remove 释放退役路由
    dead.insert(
      7,
      GcDeadEntry {
        expired_at: 0,
        tail_address: 0,
        vns: None,
      },
    );
    assert_eq!(dead.len(), 1, "前置：死亡账本含一条待回收项");
    mgr.reclaim_when_scan_idle().await?;
    assert_eq!(
      dead.len(),
      0,
      "gc.enabled=false 时常驻兜底仍须弹出到期墓碑并释放退役路由"
    );
    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// 单轮投递大于 cap (256) 的到期项，单轮弹出注销数恰为 cap，余量留在堆中下轮续收
#[test]
fn test_reclaim_when_scan_idle_respects_cap() -> Void {
  Runtime::new()?.block_on(async {
    let (_dir, store) = open_manual("idle_cap", GcConfig::default()).await?;
    let mgr = GcManager::new(&store);
    let dead = &store.vdb.gc_dead;
    // 单轮投递大于 cap (256) 的到期项（256 + 10 = 266 条）
    for id in 1..=266u64 {
      dead.insert(
        id,
        GcDeadEntry {
          expired_at: 0,
          tail_address: 0,
          vns: None,
        },
      );
    }
    assert_eq!(dead.len(), 266, "前置：死亡账本存入 266 条待回收项");

    // 单轮回收：单轮弹出注销数恰为 cap (256)，余量 10 条留在堆中
    mgr.reclaim_when_scan_idle().await?;
    assert_eq!(dead.len(), 10, "单轮弹出注销数恰为 cap (256)，余量留在堆中");

    // 下一轮续收：余量全部回收，账本最终清零
    mgr.reclaim_when_scan_idle().await?;
    assert_eq!(dead.len(), 0, "下轮续收后账本最终清零");

    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// 数据库级垃圾回收延时配置装配接线与热更联动
#[test]
fn test_gc_dead_grace_delay_wiring_and_hot_update() -> Void {
  Runtime::new()?.block_on(async {
    // 1. 装配期以短档 1 秒建实例，验证 max(60) 守卫生效（60 秒而非 86400 秒）
    let cfg = GcConfig {
      db_gc_reclaim_delay_secs: 1,
      ..Default::default()
    };
    let (_dir, store) = open_manual("gracedelay", cfg).await?;

    let entry = GcDeadEntry {
      expired_at: 100,
      tail_address: 0,
      vns: Some(10),
    };
    store.vdb.gc_dead.insert(42, entry);
    store.vdb.gc_dead.pop_reclaimable(100, 0, 256);

    let grace_60s_ticks = 60 * TICKS_PER_SECOND;
    // 宽限期内（<= 100 + 60s ticks）维持判死
    assert!(store.vdb.gc_dead.is_in_grace(10, 42, 100 + grace_60s_ticks));
    // 越过 60 秒宽限期（而非 86400 秒）：超出判死并 prune
    let past_60s = 100 + grace_60s_ticks + 1;
    assert!(!store.vdb.gc_dead.is_in_grace(10, 42, past_60s));
    store.vdb.gc_dead.pop_reclaimable(past_60s, 0, 256);
    assert!(!store.vdb.gc_dead.is_in_grace(10, 42, past_60s));

    // 2. 运行态热更联动验证
    store.update_gc_config(|c| {
      c.db_gc_reclaim_delay_secs = 300;
    });
    store.vdb.gc_dead.insert(43, entry);
    store.vdb.gc_dead.pop_reclaimable(200, 0, 256);

    let grace_300s_ticks = 300 * TICKS_PER_SECOND;
    assert!(
      store
        .vdb
        .gc_dead
        .is_in_grace(10, 43, 200 + grace_300s_ticks)
    );
    assert!(
      !store
        .vdb
        .gc_dead
        .is_in_grace(10, 43, 200 + grace_300s_ticks + 1)
    );

    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// 冷区点读故障注入设备：仅拦 `read_range` 在目标记录逻辑地址上的定点恒败
/// （armed 且 offset 精确命中 armed_addr 时返回 I/O 错误），页面级冷读（偏移
/// 恒为页起点）与其余全部 I/O 委托真实设备——复现「扫描成功、删除循环逐键
/// `ttl_of` 点读失败」形态（冷读为纯设备直读、单条记录探针读取以记录地址为
/// 偏移，见 whlog/hlog/io.rs:read_disk_record）
struct FailPointReadDevice {
  inner: SegmentedDevice,
  /// 注入开关：true 时对 armed_addr 的读恒败
  armed: AtomicBool,
  /// 注入目标：目标 TTL 记录的逻辑地址（= hlog 地址 = read_range 偏移）
  armed_addr: AtomicU64,
}

impl FailPointReadDevice {
  /// 拨入/解除定点读失败注入（offset 精确匹配目标记录地址）
  fn set_fail(&self, addr: u64) {
    self.armed_addr.store(addr, Ordering::Relaxed);
    self.armed.store(true, Ordering::Relaxed);
  }

  fn clear_fail(&self) {
    self.armed.store(false, Ordering::Relaxed);
  }
}

impl Device for FailPointReadDevice {
  fn sector_size(&self) -> usize {
    self.inner.sector_size()
  }

  fn segment_size(&self) -> u64 {
    self.inner.segment_size()
  }

  fn direct_io(&self) -> bool {
    self.inner.direct_io()
  }

  fn start_segment(&self) -> u32 {
    self.inner.start_segment()
  }

  fn end_segment(&self) -> Option<u32> {
    self.inner.end_segment()
  }

  fn capacity(&self) -> Option<u64> {
    self.inner.capacity()
  }

  fn pool(&self) -> &Arc<BufferPool> {
    self.inner.pool()
  }

  async fn write_aligned(&self, offset: u64, buf: AlignedBuf) -> (wdev::Result<usize>, AlignedBuf) {
    self.inner.write_aligned(offset, buf).await
  }

  async fn read_aligned(&self, offset: u64, buf: AlignedBuf) -> (wdev::Result<usize>, AlignedBuf) {
    self.inner.read_aligned(offset, buf).await
  }

  async fn read_raw(&self, offset: u64, buf: AlignedBuf) -> (wdev::Result<usize>, AlignedBuf) {
    self.inner.read_raw(offset, buf).await
  }

  async fn read_range(&self, offset: u64, len: usize) -> wdev::Result<AlignedBuf> {
    if self.armed.load(Ordering::Relaxed) && offset == self.armed_addr.load(Ordering::Relaxed) {
      return Err(WdevError::Io(io::Error::other(
        "injected ttl record point-read failure",
      )));
    }
    self.inner.read_range(offset, len).await
  }

  async fn sync(&self) -> wdev::Result<()> {
    self.inner.sync().await
  }

  fn get_file_size(&self, segment_id: u32) -> wdev::Result<u64> {
    self.inner.get_file_size(segment_id)
  }

  async fn remove_segment(&self, segment_id: u32) -> wdev::Result<()> {
    self.inner.remove_segment(segment_id).await
  }

  async fn truncate_until_segment(&self, segment_id: u32) -> wdev::Result<()> {
    self.inner.truncate_until_segment(segment_id).await
  }
}

/// 注入指定路径的故障设备引擎（GC enabled=false 手动驱动，配置同 open_manual）
fn open_fault_store(
  dir: &TempDir,
  gc: GcConfig,
) -> aok::Result<(
  Arc<WedbStore<FailPointReadDevice>>,
  Arc<FailPointReadDevice>,
)> {
  let device = Arc::new(FailPointReadDevice {
    inner: SegmentedDevice::new(dir.path().join("gc_fp.db"), 4096, 4096)?,
    armed: AtomicBool::new(false),
    armed_addr: AtomicU64::new(0),
  });
  let mut config = StoreConfig::new(1024, 4096, 16, 0.5)?;
  config.gc = gc;
  config.gc.enabled = false;
  let store = Arc::new(WedbStore::open(config, Arc::clone(&device))?);
  Ok((store, device))
}

/// 测试: 冷区删除失败不推进游标——任一键 `check_expired` Err 即本轮冷游标停在
/// 批起点，解除故障后下一轮重扫命中同批并收敛删除（幂等：已删键双检零副作用）；
/// 全成功路径游标照常推进（回归不变）。回装缺陷形（删除循环无条件 store）时
/// 「失败轮游标不动」断言转红
#[test]
fn test_cold_cursor_holds_on_delete_failure() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let gc = GcConfig {
      max_batch_deletes: 4,
      max_scan_records: 5000,
      compaction_max_segments: 0,
      ..GcConfig::default()
    };
    let dir = tempdir()?;
    let (store, device) = open_fault_store(&dir, gc)?;
    let session = store.new_session()?;
    let mgr = GcManager::new(&store);

    // 批 1：两过期键整批成功 → 游标提交推进（全成功路径回归不变臂）
    for k in [b"gc:fp:a1".as_slice(), b"gc:fp:b1".as_slice()] {
      session.upsert(k, b"v").await?;
      assert_eq!(
        session
          .expire_at(k, now_ticks() + TICKS_PER_MILLISECOND * 50, TtlOpt::NONE)
          .await?,
        1
      );
    }
    let limit1 = store.tail_address();
    for i in 0..120 {
      session
        .upsert(format!("gc:fp:bulk1:{i}").as_bytes(), &[b'v'; 1024])
        .await?;
    }
    assert!(
      store.read_only_address() > limit1,
      "前置：只读线必须越过批 1 过期键记录"
    );
    sleep(Duration::from_millis(120)).await;
    mgr.run_once().await?;
    assert_eq!(mgr.stats().last_scan_deleted, 2, "批 1 两键整批删除");
    let cursor1 = mgr.stats().cold_cursor;
    assert!(
      cursor1 > limit1,
      "全成功轮游标必须推进提交（cursor={cursor1} limit1={limit1}）"
    );

    // 批 2：一坏键一点读故障 + 一键正常
    for k in [b"gc:fp:bad".as_slice(), b"gc:fp:good".as_slice()] {
      session.upsert(k, b"v").await?;
      assert_eq!(
        session
          .expire_at(k, now_ticks() + TICKS_PER_MILLISECOND * 50, TtlOpt::NONE)
          .await?,
        1
      );
    }
    let bad_ttl_addr = store
      .index
      .load()
      .find_tag(&session.ttl_key(b"gc:fp:bad"))
      .expect("坏键 TTL 记录索引项在场");
    assert_ne!(
      bad_ttl_addr % 4096,
      0,
      "前置：坏键 TTL 记录不得落于页起点（定点读注入与页读偏移须可区分）"
    );
    let limit2 = store.tail_address();
    for i in 0..120 {
      session
        .upsert(format!("gc:fp:bulk2:{i}").as_bytes(), &[b'v'; 1024])
        .await?;
    }
    assert!(
      store.read_only_address() > limit2,
      "前置：只读线必须越过批 2 过期键记录"
    );
    sleep(Duration::from_millis(120)).await;

    // 故障轮：坏键删除 Err → 游标停在批起点（= cursor1），好键照常删除
    device.set_fail(bad_ttl_addr);
    mgr.run_once().await?;
    device.clear_fail();
    let st = mgr.stats();
    assert_eq!(st.last_scan_deleted, 1, "故障轮仅坏键失败，好键必须删除");
    assert_eq!(
      st.cold_cursor, cursor1,
      "任一 Err 即本轮游标不得越过失败批（停在批起点）"
    );
    assert!(
      session
        .read_raw(&session.ttl_key(b"gc:fp:bad"))
        .await?
        .is_some(),
      "坏键 TTL 记录必须物理滞留待重试"
    );

    // 解除故障轮：游标未动 → 重扫命中同批，坏键收敛删除并整批提交
    mgr.run_once().await?;
    assert_eq!(
      mgr.stats().last_scan_deleted,
      1,
      "下轮重扫必须命中同批坏键并物理删除（好键陈旧版本双检零副作用）"
    );
    assert!(
      mgr.stats().cold_cursor > limit2,
      "整批成功后游标提交越过批 2"
    );
    assert!(
      session
        .read_raw(&session.ttl_key(b"gc:fp:bad"))
        .await?
        .is_none(),
      "坏键 TTL 记录已随重试清除"
    );
    assert_eq!(mgr.stats().expired_deleted, 4, "四键累计删尽");

    aok::Result::<()>::Ok(())
  })?;
  OK
}
