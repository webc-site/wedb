//! grow 扩容迁移窗 × ReadCache 环形驱逐竞态回归
//! （wkv-growsplit-rc-evicted-dead-slot-doublewrite）
//!
//! 对标 C# 契约：扩容时序 IndexResizeSMTask.cs:52 先翻 resizeInfo.version、:76 后
//! SplitAllBuckets，迁移源是旧表、驱逐清洗恢复面是活跃新表
//! （ReadCache.cs:ReadCacheEvict 经 TsavoriteBase.cs:226-231 FindTag 只查活跃表）；
//! 滑出 RC 条目必须等待清洗恢复（ReadCache.cs:ReadCacheNeedToWaitForEviction），
//! 索引槽位绝不允许残留指向已清零页的死 RC 地址。rust 端口收口：泵关闭屏障注册时
//! 并捕 `resize.old_index` 迁移源表快照，`close_pending_page` 对双表各跑一遍
//! cleanse_page（同表判等跳过、evict_chain 幂等收敛），旧表槽位恢复为主日志地址后
//! 迁移读到的恒为活地址或主日志地址，死条目源头消除。
//!
//! 注入形态仿 wcompact grow_window.rs 协同窗先例（support::stage_resize 确定性装配
//! IN_PROGRESS_GROW 迁移窗，非真并发时序碰运气），并附加压级真 grow 并发回绕用例。

use std::{
  iter::once,
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicI64, Ordering},
    mpsc,
  },
  thread::{scope, sleep, spawn, yield_now},
  time::{Duration, Instant},
};

use aok::{OK, Void};
use compio::runtime::Runtime;
use wbase::{addr::is_read_cache, align::DEFAULT_SECTOR_SIZE};
use wdev::SegmentedDevice;
use windex::{HashIndex, SPLIT_UNSTARTED, chunk_count};
use wkv::store::ResizePhase;

use crate::support::{
  HashIndexTestOps, config, finish_resize_window, open_store, read_cache_holds_key, stage_resize,
};

/// 键数足够灌穿 2 页 RC 环形：victim 页被驱逐时其迁移分块尚未迁移（新表无此键）
const PADS: usize = 48;

fn pad_key(i: usize) -> Vec<u8> {
  format!("rc_grow_pad_{i:02}").into_bytes()
}

/// 断言单键在当前活跃表所有候选槽位无死 RC 地址：含 RC 位的地址必须可被
/// skip_read_cache 解析（滑出即死，落 None 违例）
fn assert_no_dead_rc_slot(store: &Arc<WedbStoreLike>, key: &[u8]) {
  for addr in store.index.load().lookup_vec(key) {
    if is_read_cache(addr) {
      assert!(
        store.read_cache.skip_read_cache(addr).is_some(),
        "新表槽位残留滑出死 RC 地址: {addr:#x}"
      );
    }
  }
}

type WedbStoreLike = wkv::WedbStore<SegmentedDevice>;

/// 确定性主用例：迁移窗内环形回绕驱逐，双表并洗恢复迁移源旧表槽位，
/// 迁移完成后新表无死 RC 地址、被驱逐页内键读正常（非 LockTimeout）
#[compio::test]
async fn grow_window_wraparound_restores_migration_source_slots() -> Void {
  let env = open_store(
    "rc_grow_window.db",
    config(16, DEFAULT_SECTOR_SIZE, 4)?
      .with_read_cache(true)
      .with_read_cache_pages(2)?,
  )?;
  let store = env.store;
  let session = store.new_session()?;

  // 1. 全量落键（setup 期允许自由自动扩容，与窗口危害无关）
  session.upsert(b"rc_grow_victim", b"payload-victim").await?;
  let mut pad_phys = Vec::new();
  for i in 0..PADS {
    session.upsert(&pad_key(i), b"padval").await?;
    pad_phys.push(session.session_string_key(&pad_key(i)));
  }
  let victim_phys = session.session_string_key(b"rc_grow_victim");

  // 2. 迁移窗装配前读晋升：victim 槽位挂上 RC 虚拟地址（落 RC 环形页 0），
  //    该记录即「被驱逐页内键」的受害者原型
  let a_index = store.active_index();
  let v_main = a_index
    .find_tag(victim_phys.as_slice())
    .expect("victim 主日志槽位在场");
  assert!(!is_read_cache(v_main), "挂载前槽位为主日志地址");
  let v_rc = store
    .read_cache
    .append(
      victim_phys.as_slice(),
      b"payload-victim",
      &a_index,
      store.begin_address(),
    )
    .expect("victim RC 挂载成功");
  assert!(is_read_cache(v_rc));
  let pad_mains: Vec<u64> = pad_phys
    .iter()
    .map(|p| a_index.find_tag(p.as_slice()).expect("pad 槽位在场"))
    .collect();

  // 3. 确定性装配 IN_PROGRESS_GROW 迁移窗：切上 2 倍新表 B、victim 条目仅存
  //    迁移源旧表 A（未迁分块形态）；pad 条目补挂进 B（他分块已迁移形态）
  stage_resize(&store, true, ResizePhase::InProgressGrow);
  let b_index = store.active_index();
  for (phys, main) in pad_phys.iter().zip(&pad_mains) {
    b_index.insert(phys.as_slice(), *main)?;
  }

  // 4. 迁移期间读晋升持续灌环形触发回绕驱逐 victim 页：泵注册捕获活跃表 +
  //    resize.old_index 双表快照（与 session/raw/read.rs 生产泵调用点同参形态）
  let mut wrapped = false;
  'wrap: for _ in 0..16 {
    for phys in &pad_phys {
      let idx = store.index.load_full();
      store
        .read_cache
        .append(phys.as_slice(), b"padval", &idx, store.begin_address());
      store
        .read_cache
        .pump_close_barrier(None, &idx, store.resize.old_index.load_full().as_ref());
      if store.read_cache.closed_until_address() > 0 {
        wrapped = true;
        break 'wrap;
      }
    }
  }
  assert!(
    wrapped,
    "连续灌入必触发环形回绕驱逐并发布 ClosedUntilAddress"
  );

  // 5. 修复核心断言：旧表 A 中 victim 死槽位经双表并洗恢复为主日志地址
  //    （清洗闭包仅持新表 B 时 find_tag(B) 落空即 return，此处恒残留死 RC 地址）
  assert_eq!(
    a_index.find_tag(victim_phys.as_slice()),
    Some(v_main),
    "迁移源旧表 victim 槽位必须被恢复为主日志地址（残留死 RC 即本票双写源头）"
  );

  // 6. 收窗全量迁移：迁移读旧表恒为活地址/主日志地址，死条目源头消除
  finish_resize_window(&store);
  let cands = store.index.load().lookup_vec(victim_phys.as_slice());
  assert!(
    cands.contains(&v_main),
    "victim 必须以主日志地址迁入新表: {cands:x?}"
  );
  assert!(
    cands.iter().all(|a| !is_read_cache(*a)),
    "victim 新表槽位不得残留任何 RC 位地址: {cands:x?}"
  );
  for phys in pad_phys.iter().chain(once(&victim_phys)) {
    assert_no_dead_rc_slot(&store, phys.as_slice());
  }

  // 7. 被驱逐页内键读正常：值精准返回，绝无 LockTimeout
  assert_eq!(
    session.read(b"rc_grow_victim").await?,
    Some(b"payload-victim".to_vec()),
    "修复后 victim 读必须正常返回，不得 LockTimeout"
  );
  for i in 0..PADS {
    assert_eq!(session.read(&pad_key(i)).await?, Some(b"padval".to_vec()));
  }
  OK
}

/// 危害对照形态固化：泵不捕旧表（`old_index` 传 None，即修复前形态）时，环形
/// 驱逐回绕后迁移源旧表 victim 槽位残留指向已清零页的死 RC 地址，且旧页清页复用
/// 后信息全失、无处可恢复——锁定「恢复面必须含迁移源表且必须当页关闭时并洗」的
/// 因果，杜绝未来把双表并洗改回单表
#[compio::test]
async fn single_table_cleanse_leaves_dead_slot_in_migration_source() -> Void {
  let env = open_store(
    "rc_grow_single.db",
    config(16, DEFAULT_SECTOR_SIZE, 4)?
      .with_read_cache(true)
      .with_read_cache_pages(2)?,
  )?;
  let store = env.store;
  let session = store.new_session()?;

  session.upsert(b"rc_grow_victim", b"payload-victim").await?;
  let mut pad_phys = Vec::new();
  for i in 0..PADS {
    session.upsert(&pad_key(i), b"padval").await?;
    pad_phys.push(session.session_string_key(&pad_key(i)));
  }
  let victim_phys = session.session_string_key(b"rc_grow_victim");

  let a_index = store.active_index();
  let v_main = a_index
    .find_tag(victim_phys.as_slice())
    .expect("victim 主日志槽位在场");
  store
    .read_cache
    .append(
      victim_phys.as_slice(),
      b"payload-victim",
      &a_index,
      store.begin_address(),
    )
    .expect("victim RC 挂载成功");
  let pad_mains: Vec<u64> = pad_phys
    .iter()
    .map(|p| a_index.find_tag(p.as_slice()).expect("pad 槽位在场"))
    .collect();

  stage_resize(&store, true, ResizePhase::InProgressGrow);
  let b_index = store.active_index();
  for (phys, main) in pad_phys.iter().zip(&pad_mains) {
    b_index.insert(phys.as_slice(), *main)?;
  }

  // 泵仅持新表（修复前形态）：回绕驱逐后新表 pad 槽位恢复，旧表 victim 槽位
  // 无人复位
  let mut wrapped = false;
  'wrap: for _ in 0..16 {
    for phys in &pad_phys {
      let idx = store.index.load_full();
      store
        .read_cache
        .append(phys.as_slice(), b"padval", &idx, store.begin_address());
      store.read_cache.pump_close_barrier(None, &idx, None);
      if store.read_cache.closed_until_address() > 0 {
        wrapped = true;
        break 'wrap;
      }
    }
  }
  assert!(wrapped, "连续灌入必触发环形回绕驱逐");
  let dead = a_index
    .find_tag(victim_phys.as_slice())
    .expect("旧表 victim 槽位仍在");
  assert!(
    is_read_cache(dead) && store.read_cache.skip_read_cache(dead).is_none(),
    "单表清洗下旧表 victim 槽位必残留滑出死 RC 地址（双写源头对照形）: {dead:#x}"
  );
  assert_ne!(dead, v_main);
  OK
}

/// 压级并发用例：真 grow_index 线程 × 多读晋升线程（票面「迁移期间并发读
/// 晋升灌满 2 页小环形触发回绕」形态）
///
/// 晋升一律走生产 `session.read` 事务域：不可变区命中臂 `promote_immutable_read_hit`
/// 内 append + pump（pump 与磁盘回填臂同签同参，见 read.rs 两调用点），清洗回主
/// 地址的槽位再读必再晋升，自维持循环灌穿 2 页环形；grow_index 步骤 1a 活跃事务
/// 排空与 1b 纪元屏障保证「武装未注册」不跨切表——裸 append+pump 的脱事务形态
/// 注册对可跨切表漂移，非生产形，本用例不予采用。
///
/// 口径说明：本用例刻意不做 `flush_and_evict_all` 全量冷化回放——键全量驻留内存
/// 不可变区即足以按票面形态驱动环形回绕；另有既存缺陷（全量冷化回放 × 2 页环形
/// 下个别键持久 LockTimeout，与 grow 完全无关：零 grow、零并发单线程亦确定性
/// 复现，且泵按修复前单表行为同样复现）越出本票迁移窗双写轴界，另行立案处置，
/// 本用例不予覆盖。终态断言全键槽位无死 RC 地址且读全部正常。
#[test]
fn grow_index_concurrent_read_promotion_no_dead_slots() -> Void {
  const KEYS: usize = 400;
  const READERS: usize = 3;
  const ROUNDS: usize = 8;

  let rt = Runtime::new()?;
  rt.block_on(async {
    let env = open_store(
      "rc_grow_stress.db",
      config(16, DEFAULT_SECTOR_SIZE, 32)?
        .with_read_cache(true)
        .with_read_cache_pages(2)?,
    )?;
    let store = env.store;
    let session = store.new_session()?;
    // 63 字节值放大晋升记录：400 键一轮灌入即超 2 页环形容量，首次回绕后
    // 清洗回主地址的槽位再读必再晋升，自维持回绕循环
    let payload: Arc<[u8]> = b"sval-64b-".repeat(7).into(); // 63 字节
    for i in 0..KEYS {
      session
        .upsert(format!("rc_grow_s{i:04}").as_bytes(), &payload[..])
        .await?;
    }

    // 扩容驱动线程：连发真 grow_index（相位被并发事务占用返回 false 即重试），
    // 直到索引翻倍两次收口 Rest——环形回绕驱逐与迁移窗交叠
    let grow_store = Arc::clone(&store);
    let grower = spawn(move || {
      let mut grown = 0;
      while grown < 2 {
        if grow_store.grow_index().expect("真扩容不得报错") {
          grown += 1;
        } else {
          sleep(Duration::from_micros(50));
        }
      }
    });

    let mut handles = Vec::new();
    for t in 0..READERS {
      let read_store = Arc::clone(&store);
      let payload = Arc::clone(&payload);
      handles.push(spawn(move || {
        let rt = Runtime::new().unwrap();
        rt.block_on(async move {
          let sess = read_store.new_session().unwrap();
          // 窗内偶发 Retry 预算尽属合法瞬态（驱逐等待×协同迁移×晋升复检多路
          // 让渡叠加），持续 LockTimeout 才是死槽位特征——窗内错误只计数，
          // 终态断言收口（无死槽位 + 全键读正常）
          let mut transient_errs = 0usize;
          for round in 0..ROUNDS {
            for i in 0..KEYS {
              let n = (i + round * 7 + t * 31) % KEYS;
              let key = format!("rc_grow_s{n:04}");
              match sess.read(key.as_bytes()).await {
                Ok(Some(v)) if v.as_slice() == &payload[..] => {}
                Ok(other) => panic!("键 {key} 读值失真: {other:?}"),
                Err(_) => transient_errs += 1,
              }
            }
          }
          transient_errs
        })
      }));
    }

    let mut errs = 0usize;
    for h in handles {
      errs += h.join().expect("线程零 panic");
    }
    grower.join().expect("扩容线程零 panic");
    log::info!("迁移窗并发瞬态 Retry 预算尽计数: {errs}");

    // 读晋升必已灌穿 2 页环形触发回绕驱逐（setup 期零晋升，此处 >0 即窗内回绕实证）
    assert!(
      store.read_cache.closed_until_address() > 0,
      "迁移期间并发读晋升必灌满环形触发回绕驱逐"
    );

    // 收口终态：全键活跃表槽位无死 RC 地址
    let phys: Vec<_> = (0..KEYS)
      .map(|i| {
        let k = format!("rc_grow_s{i:04}").into_bytes();
        session.session_string_key(&k).as_slice().to_vec()
      })
      .collect();
    for key in &phys {
      assert_no_dead_rc_slot(&store, key);
    }
    // 被驱逐页内键读全部正常（票面：非 LockTimeout、值精准）；收口后桶闩/
    // 协同让渡仍属瞬态「请重试」族，允许有限重试，终局必须取回精准值
    for i in 0..KEYS {
      let key = format!("rc_grow_s{i:04}").into_bytes();
      let mut val: Option<Option<Vec<u8>>> = None;
      let mut last_err = None;
      for _attempt in 0..1000 {
        match session.read(&key).await {
          Ok(v) => {
            val = Some(v);
            break;
          }
          Err(e) => last_err = Some(format!("{e:?}")),
        }
        sleep(Duration::from_micros(200));
      }
      assert_eq!(
        val.flatten().as_deref(),
        Some(&payload[..]),
        "键 {key:?} 终态读必须正常（持续报错即死槽位）last_err={last_err:?}"
      );
    }
    OK
  })
}

/// 装配中途 panic 的收口安全钉：放行闭包会合并解除滞留的 PrepareGrow，
/// 防止主线程断链时读者永久停等/自旋导致测试挂死（失败仍如实上抛）
struct AssemblySafetyPin {
  store: Arc<WedbStoreLike>,
  release: Arc<AtomicBool>,
  /// PrepareGrow 已置位且尚未发布相位（drop 时须解除）
  phase_armed: bool,
}

impl Drop for AssemblySafetyPin {
  fn drop(&mut self) {
    self.release.store(true, Ordering::Release);
    if self.phase_armed {
      let _ = self.store.resize.phase.compare_exchange(
        ResizePhase::PrepareGrow as u8,
        ResizePhase::Rest as u8,
        Ordering::SeqCst,
        Ordering::SeqCst,
      );
    }
  }
}

/// 磁盘冷读回填臂 × 整次扩容切表交错确定性锁测
/// （wkv-readcache-promote-sample-outside-epoch-guard）
///
/// 缺陷形态（修复前）：`read_from_disk` 回填臂在 enter_gated 屏障守卫之前
/// `store.index.load_full()` 采样索引句柄——磁盘 I/O 完成至重入守卫之间无纪元
/// 保护，一次完整扩容（resize 切表先于相位发布，1a/1b 排空 + 新表构建全程可
/// 插入）即令晋升条目 CAS 进已退役旧表：晋升丢失 + RC 环孤儿占位，纯性能面
/// 危害。修复形态：守卫与采样同层提升（对位 C# InternalRead/ContinuePending
/// 的 TryCopyToReadCache 在同一 ephemeral 保护窗内装载 hei，以及本文件
/// promote_immutable_read_hit 臂守卫内采样形态）——barrier_enter 的
/// PrepareGrow 自旋即天然门控：自旋挂起期不持纪元钉，扩容推进不受阻；采样
/// 恒在切表发布之后。
///
/// 门控手法（闭包会合 + 屏障/相位硬同步装配，非时序碰运气）：`read_with` 值
/// 闭包恰在生产回填臂「复检出口之后、索引采样之前」的唯一可观测点执行，读者
/// 停等其中；主线程置 PrepareGrow 后放行闭包，随即按 grow_index 合法次序执行
/// 1a/1b 排空、构建并切上新表 B、全量分块迁移先行、最后发布 InProgressGrow
/// 放行自旋。唯一以裕量兜底的边是「修复前形态采样(取旧表 A) 须先于切表」：
/// 放行后至切表之间留 50ms 裕量覆盖停等唤醒与相邻指令执行（其余全为硬同步，
/// 且切表前主线程先核验活跃表仍为 A、发布前先核验读者确被屏障扣停）。
/// 断言晋升条目落新表不落退役表：活跃新表 victim 槽位带 RC 位且环内可解析、
/// 退役旧表 victim 槽位恒保持主日志地址。回退修复即两断言皆红，实测取证。
#[test]
fn cold_read_backfill_promotes_into_new_table_across_resize() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let env = open_store(
      "rc_promote_grow_window.db",
      config(16, DEFAULT_SECTOR_SIZE, 4)?
        .with_read_cache(true)
        .with_read_cache_pages(2)?,
    )?;
    let store = env.store;
    let session = store.new_session()?;

    // setup：victim 冷化到磁盘（本用例唯一生产回填键）；seed 另键走常态冷读晋升
    // 作回填臂可用性金丝雀（两键链条零交集，杜绝 seed 干扰 victim 的守卫窗）
    session
      .upsert(b"rc_promote_victim", b"payload-victim")
      .await?;
    session.upsert(b"rc_promote_seed", b"seedval").await?;
    let victim_phys = session.session_string_key(b"rc_promote_victim");
    let seed_phys = session.session_string_key(b"rc_promote_seed");
    let a_index = store.active_index();
    let v_main = a_index
      .find_tag(victim_phys.as_slice())
      .expect("victim 主日志槽位在场");
    assert!(!is_read_cache(v_main), "挂载前槽位为主日志地址");
    store.flush_and_evict_all().await?;
    assert_eq!(
      a_index.find_tag(victim_phys.as_slice()),
      Some(v_main),
      "冷化后 victim 槽位保持主日志地址"
    );
    assert_eq!(
      session.read(b"rc_promote_seed").await?,
      Some(b"seedval".to_vec())
    );
    assert!(
      is_read_cache(
        a_index
          .find_tag(seed_phys.as_slice())
          .expect("seed 槽位在场")
      ),
      "seed 常态冷读经同一回填臂晋升挂载实证"
    );

    let reached = Arc::new(AtomicBool::new(false));
    let release = Arc::new(AtomicBool::new(false));
    let (tx, rx) = mpsc::channel();
    let mut pin = AssemblySafetyPin {
      store: Arc::clone(&store),
      release: Arc::clone(&release),
      phase_armed: false,
    };

    scope(|s| {
      let store2 = Arc::clone(&store);
      let (reached2, release2) = (Arc::clone(&reached), Arc::clone(&release));
      let h = s.spawn(move || {
        let rt = Runtime::new().unwrap();
        let out = rt.block_on(async move {
          let sess = store2.new_session().expect("reader session");
          sess
            .read_with(b"rc_promote_victim", move |v: &[u8]| {
              reached2.store(true, Ordering::Release);
              let t0 = Instant::now();
              while !release2.load(Ordering::Acquire) {
                sleep(Duration::from_millis(1));
                assert!(t0.elapsed() < Duration::from_secs(10), "闭包会合放行超时（装配缺陷）");
              }
              v.to_vec()
            })
            .await
            .expect("冷读不得报错")
        });
        let _ = tx.send(out);
      });

      // 1. 会合：读者停在生产回填臂命中闭包——复检出口与采样之间的唯一同步观测
      //    点，此刻磁盘 I/O 已毕、纪元无保护、活跃表仍为 A
      let t0 = Instant::now();
      while !reached.load(Ordering::Acquire) {
        sleep(Duration::from_millis(1));
        assert!(t0.elapsed() < Duration::from_secs(10), "读者未达回填臂闭包（装配缺陷）");
      }
      assert!(
        Arc::ptr_eq(&store.index.load_full(), &a_index),
        "会合点活跃表仍为 A"
      );
      assert_eq!(store.resize.phase(), ResizePhase::Rest, "会合点无并发扩容");

      // 2. PrepareGrow（对位 grow_index 步骤 1 的 Rest CAS，环境内必成功）
      store
        .resize
        .phase
        .compare_exchange(
          ResizePhase::Rest as u8,
          ResizePhase::PrepareGrow as u8,
          Ordering::SeqCst,
          Ordering::SeqCst,
        )
        .expect("Rest 稳态相位 CAS 必成功");
      pin.phase_armed = true;

      // 3. 放行闭包：修复前形态即刻以 load_full 取旧表 A 后进屏障自旋
      //    （PrepareGrow 挂起期不持纪元钉，扩容推进无阻）；修复后形态采样前
      //    即停旋。裕量覆盖停等唤醒延迟与相邻指令执行，保证采样先于切表
      release.store(true, Ordering::Release);
      sleep(Duration::from_millis(50));

      // 4. 步骤 1a/1b：全事务计数恒零；纪元排空对自旋瞬钉（钉于 current）照常收敛
      while store.resize.num_active_txns.load(Ordering::SeqCst) > 0 {
        yield_now();
      }
      let target = store.epoch.bump_current_epoch();
      store.epoch.bump_and_wait(target);

      // 5. 构建新表 B 并切表（对位步骤 2/步骤 3 前半）：读者仍扣停在屏障自旋
      let new_index = Arc::new(HashIndex::new(a_index.size * 2).expect("新表构建"));
      let count = chunk_count(a_index.size);
      store.resize.split_status.store(Arc::new(
        (0..count)
          .map(|_| AtomicI64::new(SPLIT_UNSTARTED))
          .collect(),
      ));
      store.resize.num_pending_chunks.store(count, Ordering::Release);
      store.resize.old_index.store(Some(Arc::clone(&a_index)));
      store.index.store(Arc::clone(&new_index));

      // 6. 全量分块迁移先行（严格对位「切表后、回填晋升前」——即便修复前形态
      //    把 RC 位残挂旧表槽位，也绝无经迁移旁路混入新表的侥幸通道）
      for i in 0..count {
        assert!(
          store
            .split_single_chunk(i, count, &a_index)
            .expect("迁移不得报错"),
          "分块 {i} 须为 UNSTARTED 抢占成功"
        );
      }
      while store.resize.num_pending_chunks.load(Ordering::Acquire) > 0 {
        yield_now();
      }
      assert!(
        rx.try_recv().is_err(),
        "读者必须被 PrepareGrow 屏障扣停（自旋门控失效即装配缺陷）"
      );

      // 7. 发布相位：屏障放行，两形态即刻收口回填臂——修复前手持退役旧表 A 做
      //    CAS 晋升；修复后守卫内采样新表 B 晋升
      store
        .resize
        .phase
        .store(ResizePhase::InProgressGrow as u8, Ordering::SeqCst);
      pin.phase_armed = false;

      // 8. 收割读者：读结果本身两形态皆合法（线性化与晋升去向无关）
      let out = rx.recv().expect("读者必收口");
      assert_eq!(out, Some(b"payload-victim".to_vec()), "冷读必须精准回实值");
      h.join().expect("读者线程零 panic");
    });
    drop(pin);

    // 9. 核心断言：晋升条目落新表、不落退役表
    let b_index = store.index.load_full();
    assert!(!Arc::ptr_eq(&b_index, &a_index), "活跃表已切换");
    let head = b_index
      .find_tag(victim_phys.as_slice())
      .expect("victim 槽位已迁入新表（发布前迁移先行）");
    assert!(
      is_read_cache(head),
      "晋升条目必须挂活跃新表：victim 新表槽位带 RC 位（回退修复即红——修复前采样先于守卫，CAS 落退役旧表）"
    );
    assert!(
      store.read_cache.skip_read_cache(head).is_some(),
      "新表 RC 地址必环内可解析"
    );
    assert_eq!(
      a_index.find_tag(victim_phys.as_slice()),
      Some(v_main),
      "退役旧表 victim 槽位不得被晋升挂载（回退修复即红——修复前此处残留 RC 前缀）"
    );
    assert_eq!(
      read_cache_holds_key(&store, victim_phys.as_slice()),
      Some(true),
      "晋升记录沿活跃表 RC 链可定位"
    );

    // 10. 扩容收口，终态读经 RC 命中精准回值
    finish_resize_window(&store);
    assert_eq!(store.resize.phase(), ResizePhase::Rest);
    assert_eq!(
      session.read(b"rc_promote_victim").await?,
      Some(b"payload-victim".to_vec())
    );
    assert_eq!(
      session.read(b"rc_promote_seed").await?,
      Some(b"seedval".to_vec())
    );
    OK
  })
}
