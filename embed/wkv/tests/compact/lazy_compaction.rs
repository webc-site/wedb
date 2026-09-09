//! compact_lazy 周期惰性紧缩入口测试（wedb 自有的单轮有界前推语义；
//! Garnet 式触发回退紧缩由 gc.rs 与 basic 套件覆盖）
//!
//! 覆盖场景：
//! 1. 大量写入 + 删除产生垃圾后单轮紧缩：释放字节 > 0、新起始地址推进、再次调用返回空统计；
//! 2. 单轮 max_seek 有界推进：多轮滚动消化积压垃圾并收敛至只读区边界；
//! 3. 短路边界：零写入、max_seek == 0 等场景直接返回空统计，起始地址保持不变。

use std::{fmt::Write, sync::Arc};

use aok::{OK, Void};
use compio::runtime::Runtime;
use log::info;
use wcompact::LogCompactor;

use super::support::create_test_store;

/// 惰性紧缩测试总写入记录数
const TOTAL_RECORDS: usize = 100;
/// 惰性紧缩删除记录数（前半部分）
const DELETE_COUNT: usize = 50;
/// 滚动紧缩轮次安全上限（防死循环兜底）
const MAX_ROUNDS: usize = 1000;
/// 滚动紧缩单轮推进字节上限（约数倍单条记录尺寸，强制多轮推进）
const SEEK_PER_ROUND: u64 = 1024;

/// 对标 Garnet 周期紧缩语义：删除垃圾经 compact_lazy 单轮回收后
/// bytes_freed > 0、新起始地址推进至只读区边界，再次调用返回空统计
#[test]
fn lazy_compaction_frees_garbage_and_advances_begin() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store, session) = create_test_store("lazy_gc.db")?;

    let mut k = String::with_capacity(16);
    let mut v = String::with_capacity(16);
    for i in 0..TOTAL_RECORDS {
      k.clear();
      let _ = write!(&mut k, "lazy_key:{i:05}");
      v.clear();
      let _ = write!(&mut v, "lazy_val:{i:05}");
      session.upsert(k.as_bytes(), v.as_bytes()).await?;
    }

    // 删除产生墓碑与过期版本垃圾，再统一截断只读区：紧缩窗口覆盖全部 150 条记录
    for i in 0..DELETE_COUNT {
      k.clear();
      let _ = write!(&mut k, "lazy_key:{i:05}");
      session.delete(k.as_bytes()).await?;
    }

    store.flush_and_evict_all().await?;
    let read_only_cut = store.hlog.read_only_address();
    let begin_before = store.begin_address();
    assert!(
      read_only_cut > begin_before,
      "前置条件：只读区必须有待紧缩区间"
    );

    let compactor = LogCompactor::new(Arc::clone(&store));
    let stats = compactor.compact_lazy(u64::MAX).await?;

    assert!(!stats.is_empty(), "存在垃圾区间时必须执行紧缩");
    // 窗口内 = 100 条初版 + 50 条墓碑；存活 50 条；50 条被删键的初版被区间内墓碑
    // 取代计为弃迁，50 条墓碑计为判死丢弃
    assert_eq!(stats.scanned_records, TOTAL_RECORDS + DELETE_COUNT);
    assert_eq!(stats.live_copied, TOTAL_RECORDS - DELETE_COUNT);
    assert_eq!(stats.superseded, DELETE_COUNT, "被墓碑取代的初版计为弃迁");
    assert_eq!(stats.dead_dropped, DELETE_COUNT, "墓碑计为判死丢弃");
    assert!(stats.bytes_freed > 0, "删除垃圾必须释放日志字节");
    assert_eq!(stats.new_begin_address, read_only_cut);
    assert_eq!(store.begin_address(), read_only_cut);

    // 数据完整性：已删除键读空，存活键内容不变
    let mut expected = String::with_capacity(16);
    for i in 0..TOTAL_RECORDS {
      k.clear();
      let _ = write!(&mut k, "lazy_key:{i:05}");
      let val = session.read(k.as_bytes()).await?;
      if i < DELETE_COUNT {
        assert!(val.is_none(), "已删除键必须返回 None: {k}");
      } else {
        expected.clear();
        let _ = write!(&mut expected, "lazy_val:{i:05}");
        assert_eq!(val.as_deref(), Some(expected.as_bytes()));
      }
    }

    // 再次调用：begin >= read_only（无可紧缩区间），返回空统计且起始地址不再推进
    let second = compactor.compact_lazy(u64::MAX).await?;
    assert!(second.is_empty());
    assert_eq!(second.bytes_freed, 0);
    assert_eq!(second.new_begin_address, read_only_cut);
    assert_eq!(store.begin_address(), read_only_cut);

    info!("compact_lazy 垃圾回收与起始地址推进验证通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 单轮 max_seek 有界推进：多轮滚动消化积压垃圾并收敛至只读区边界（滚动语义核心）
#[test]
fn lazy_compaction_bounded_seek_rolling_rounds() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store, session) = create_test_store("lazy_rolling.db")?;

    let mut k = String::with_capacity(16);
    let mut v = String::with_capacity(32);
    for i in 0..TOTAL_RECORDS {
      k.clear();
      let _ = write!(&mut k, "roll_key:{i:05}");
      v.clear();
      let _ = write!(&mut v, "v1_{i:05}");
      session.upsert(k.as_bytes(), v.as_bytes()).await?;
    }

    let read_only_cut = store.tail_address();
    store.shift_read_only_address(read_only_cut);

    // 只读区之后全部覆盖为更长的新版本：[begin, cut) 区间整体沦为过期垃圾
    for i in 0..TOTAL_RECORDS {
      k.clear();
      let _ = write!(&mut k, "roll_key:{i:05}");
      v.clear();
      let _ = write!(&mut v, "v2_{i:05}_overwritten_payload");
      session.upsert(k.as_bytes(), v.as_bytes()).await?;
    }

    let begin_before = store.begin_address();
    let compactor = LogCompactor::new(Arc::clone(&store));
    let mut rounds = 0usize;
    let mut total_freed = 0u64;
    loop {
      let stats = compactor.compact_lazy(SEEK_PER_ROUND).await?;
      if stats.is_empty() {
        break;
      }
      assert!(stats.bytes_freed > 0, "每轮滚动紧缩都必须释放字节");
      total_freed += stats.bytes_freed;
      rounds += 1;
      assert!(rounds < MAX_ROUNDS, "滚动紧缩必须收敛，不得死循环");
    }

    assert!(
      rounds > 1,
      "单轮推进上限必须使紧缩分多轮滚动完成, 实测轮次={rounds}"
    );
    assert_eq!(total_freed, read_only_cut - begin_before);
    assert_eq!(store.begin_address(), read_only_cut);

    // 回读校验全部键为覆盖后的新版本
    let mut expected = String::with_capacity(32);
    for i in 0..TOTAL_RECORDS {
      k.clear();
      let _ = write!(&mut k, "roll_key:{i:05}");
      expected.clear();
      let _ = write!(&mut expected, "v2_{i:05}_overwritten_payload");
      assert_eq!(
        session.read(k.as_bytes()).await?.as_deref(),
        Some(expected.as_bytes())
      );
    }

    info!("compact_lazy 单轮有界多轮滚动紧缩验证通过: 轮次={rounds}");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 短路边界：零写入与 max_seek == 0 均直接返回空统计，起始地址保持不变
#[test]
fn lazy_compaction_short_circuit_empty() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store, session) = create_test_store("lazy_empty.db")?;
    let compactor = LogCompactor::new(Arc::clone(&store));

    // 零写入：begin >= read_only，直接空统计
    let fresh = compactor.compact_lazy(u64::MAX).await?;
    assert!(fresh.is_empty());
    assert_eq!(fresh.bytes_freed, 0);
    assert_eq!(fresh.new_begin_address, store.begin_address());

    // max_seek == 0：即使存在可紧缩区间也直接空统计（等价紧缩关闭）
    session.upsert(b"sc_key", b"sc_value").await?;
    let cut = store.tail_address();
    store.shift_read_only_address(cut);
    let begin_before = store.begin_address();
    let zero_seek = compactor.compact_lazy(0).await?;
    assert!(zero_seek.is_empty());
    assert_eq!(zero_seek.bytes_freed, 0);
    assert_eq!(zero_seek.new_begin_address, begin_before);
    assert_eq!(
      store.begin_address(),
      begin_before,
      "短路路径不得推进起始地址"
    );

    info!("compact_lazy 短路边界验证通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}
