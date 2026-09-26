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
  sync::Arc,
  thread::{sleep, spawn},
  time::Duration,
};

use aok::{OK, Void};
use compio::runtime::Runtime;
use wbase::{addr::is_read_cache, align::DEFAULT_SECTOR_SIZE};
use wdev::SegmentedDevice;
use wkv::store::ResizePhase;

use crate::support::{HashIndexTestOps, config, finish_resize_window, open_store, stage_resize};

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
        .pump_close_barrier(&idx, store.resize.old_index.load_full().as_ref());
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
      store.read_cache.pump_close_barrier(&idx, None);
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
