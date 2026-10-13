//! 环形缓冲容量受限下并发写驱动换页驱逐的紧缩压力测试
//!
//! 独立写线程在紧缩全程持续追加记录令 8 页环形槽位不停翻转回收（页回收门禁：
//! head/flushed_until/safe_head 三水位 + 纪元排空），主线程并发执行多轮 Scan
//! 紧缩 + 物理截断。断言面（票据计划 3）：
//! 1. 紧缩截断后全部存活键可完整点查且值正确（漏迁键的槽位随截断被物理抹除，
//!    点查将落至 begin 之下的截断区而失败）；
//! 2. 已删键不可见（墓碑随截断退役，索引引用同步清理）；
//! 3. 哈希索引对本测试全部存活键不残留低于最终 begin 的失效槽位。
//!
//! 纪元素件与回收机制的共存面（本测试在修复后语义下的核心压点）：扫描器内联
//! TLS 守卫逐记录取放（whlog `ScanIterator::next_ref`）↔ 写线程 evict 路径的
//! bump/drain 排空 ↔ 紧缩迁移 `conditional_copy_to_tail` 的会话守卫——三者必须
//! 互不挂死且互不误伤（守卫缺失则红：回收窗口内裸读页被清零/复用致整页漏迁、
//! 截断后存活键点查失败；守卫滞留则轮次超预算，由 [`Watchdog`] 兜底诊断）。
//!
//! 自研依据: 紧缩扫描与驱逐对抗（C# 对应 SpanByteLogCompactionTests.cs 压力面）

use std::{
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
  thread::Builder,
  time::{Duration, Instant},
};

use aok::{OK, Result as AokResult, Void};
use compio::{runtime::Runtime, time::sleep};
use tempfile::tempdir;
use wcompact::{CompactSession, CompactStore, CompactionType, LogCompactor};

use super::support::{FixtureStore, Watchdog, str_key};

/// 每轮新键数
const KEYS_PER_ROUND: usize = 100;
/// 每轮内被二次覆盖（紧缩区间内产生被取代旧版本）的键数
const OVERWRITES_PER_ROUND: usize = 25;
/// 每轮内被墓碑删除的键数（取区间尾部，与覆盖段不交叠）
const DELETES_PER_ROUND: usize = 15;
/// 紧缩轮次
const ROUNDS: usize = 2;
/// 初始灌数键数（保证首轮紧缩区间横跨磁盘冷区与内存驻留区）
const SETUP_KEYS: usize = 500;
/// 初始灌数中被删除的键数（区间尾部）
const SETUP_DELETES: usize = 50;
/// 写线程单记录值体大小（4KB，16 条即翻转一页 64KB 槽位）
const WRITER_VAL_LEN: usize = 4096;
/// 写线程节流：每追加 4 页让出一次（限住 evict 自旋占核，不改变竞速结构）
const WRITER_YIELD_EVERY_APPENDS: u64 = 64;
/// 测试总预算（超出即看门狗报挂死）
const BUDGET: Duration = Duration::from_secs(60);

fn setup_key_name(i: usize) -> String {
  format!("live:setup:{i}")
}

fn round_key_name(round: usize, i: usize) -> String {
  format!("live:{round}:{i}")
}

/// 环形缓冲容量受限下并发写驱动换页驱逐的多轮 Scan 紧缩：截断后存活键全量点查存活
#[test]
fn scan_compact_survives_concurrent_ring_recycling() -> Void {
  let _watchdog = Watchdog::start("scan_compact_survives_concurrent_ring_recycling", BUDGET);
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let store = FixtureStore::open(dir.path().join("evict_stress.db"))?;
    let s = store.session()?;

    // 期望终值表（String 物理键 -> Some(值) / None=已删不可见）。键一律经
    // str_key 物理键入索引/点查，与 index.find_tag 的 physical-key 口径同源。
    let mut expect: Vec<(Vec<u8>, Option<Vec<u8>>)> = Vec::new();

    // ===== 初始灌数：1500 键，前 300 覆盖、末 150 删除（约 1MB，环形窗口 512KB，
    // 灌数过程本身已多轮翻转槽位，首轮紧缩区间天然横跨冷/驻留两区） =====
    let v1 = vec![0x01u8; 400];
    let ov = vec![0x02u8; 400];
    for i in 0..SETUP_KEYS {
      let pk = str_key(setup_key_name(i).as_bytes());
      store.put(&s, &pk, &v1).await?;
      expect.push((pk, Some(v1.clone())));
    }
    for i in 0..OVERWRITES_PER_ROUND * 5 {
      let pk = str_key(setup_key_name(i).as_bytes());
      store.put(&s, &pk, &ov).await?;
      let slot = expect.iter_mut().find(|(k, _)| *k == pk).unwrap();
      slot.1 = Some(ov.clone());
    }
    for i in (SETUP_KEYS - SETUP_DELETES)..SETUP_KEYS {
      let pk = str_key(setup_key_name(i).as_bytes());
      store.del(&s, &pk).await?;
      let slot = expect.iter_mut().find(|(k, _)| *k == pk).unwrap();
      slot.1 = None;
    }

    // ===== 并发驱逐写线程：紧缩全程持续追加，环形槽位不停回收复用 =====
    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
      let store_w = Arc::clone(&store);
      let stop_w = Arc::clone(&stop);
      Builder::new()
        .name("evict-writer".to_string())
        .spawn(move || -> AokResult<()> {
          let rt = Runtime::new()?;
          rt.block_on(async move {
            let session = store_w.new_session()?;
            let val = vec![0xa5u8; WRITER_VAL_LEN];
            let mut seq: u64 = 0;
            while !stop_w.load(Ordering::Relaxed) {
              let key = format!("wr{seq}");
              // append_pure 内建 PageNotReady → evict（补刷 + sync + 抬 ro/head +
              // bump）→ 重试：持续把 8 页环形槽位从头顶翻到脚下
              session.append_pure(key.as_bytes(), &val, 0, false).await?;
              seq += 1;
              if seq.is_multiple_of(WRITER_YIELD_EVERY_APPENDS) {
                sleep(Duration::from_micros(100)).await;
              }
            }
            AokResult::<()>::Ok(())
          })
        })?
    };

    let outcome: AokResult<()> = async {
      let compactor = LogCompactor::new(Arc::clone(&store));
      let deadline = Instant::now() + BUDGET;
      let mut total_scanned = 0usize;
      let mut total_copied = 0usize;

      // ===== 多轮：本轮新键灌数 → 封印至当下 tail → Scan 紧缩至该点（含截断） =====
      for round in 0..ROUNDS {
        for i in 0..KEYS_PER_ROUND {
          let pk = str_key(round_key_name(round, i).as_bytes());
          let mut v = vec![0x10u8 + round as u8; 400];
          v[1] = i as u8;
          store.put(&s, &pk, &v).await?;
          expect.push((pk, Some(v)));
        }
        for i in 0..OVERWRITES_PER_ROUND {
          let pk = str_key(round_key_name(round, i).as_bytes());
          let v = vec![0x20u8 + round as u8; 400];
          store.put(&s, &pk, &v).await?;
          let slot = expect.iter_mut().find(|(k, _)| *k == pk).unwrap();
          slot.1 = Some(v);
        }
        for i in (KEYS_PER_ROUND - DELETES_PER_ROUND)..KEYS_PER_ROUND {
          let pk = str_key(round_key_name(round, i).as_bytes());
          store.del(&s, &pk).await?;
          let slot = expect.iter_mut().find(|(k, _)| *k == pk).unwrap();
          slot.1 = None;
        }

        let tail = store.hlog.tail_address();
        store.seal_read_only(tail);
        let stats = compactor.compact(tail, CompactionType::Scan).await?;
        assert!(
          stats.scanned_records > 0,
          "第 {round} 轮紧缩区间为空，驱逐/紧缩并发面未覆盖: {stats:?}"
        );
        assert!(
          stats.bytes_freed > 0,
          "第 {round} 轮截断线未推进，测试未真正穿越物理回收: {stats:?}"
        );
        assert!(
          stats.retained == 0,
          "第 {round} 轮出现保守保留（截断线回退），并发穿越面未达预期: {stats:?}"
        );
        total_scanned += stats.scanned_records;
        total_copied += stats.live_copied;
        assert!(
          Instant::now() < deadline,
          "紧缩轮次超出预算，疑似扫描守卫与页回收排空互相挂死"
        );
      }
      assert!(
        total_copied
          >= (SETUP_KEYS - SETUP_DELETES) + ROUNDS * (KEYS_PER_ROUND - DELETES_PER_ROUND),
        "存活键迁移总量不足: scanned={total_scanned} copied={total_copied}"
      );

      // ===== 断言 1/2：全部存活键点查值正确、已删键不可见 =====
      for (pk, want) in &expect {
        let got = store.get(&s, pk).await?;
        assert_eq!(
          got.as_deref(),
          want.as_deref(),
          "截断后点查失配（漏迁随截断丢失/误删）: key={pk:?}"
        );
      }

      // ===== 断言 3：索引对全部存活键不残留低于最终 begin 的失效槽位 =====
      let final_begin = store.hlog.begin_address();
      let final_tail = store.hlog.tail_address();
      for (pk, want) in &expect {
        if want.is_none() {
          continue;
        }
        let slot = {
          let _guard = s.enter_epoch();
          store.index.find_tag(pk)
        };
        let Some(slot) = slot else {
          panic!("存活键索引槽位缺失（应已随迁移挂新）: key={pk:?}");
        };
        let main = store.resolve_main(slot);
        assert!(
          main >= final_begin && main < final_tail,
          "索引残留低于 begin 的失效槽位: key={pk:?} addr={main:#x} begin={final_begin:#x}"
        );
      }
      AokResult::<()>::Ok(())
    }
    .await;

    stop.store(true, Ordering::Relaxed);
    let join = writer.join();
    outcome?;
    join.expect("驱逐写线程 panic").expect("驱逐写线程异常退出");
    aok::Result::<()>::Ok(())
  })?;
  OK
}
