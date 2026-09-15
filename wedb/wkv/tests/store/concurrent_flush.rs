//! 并发刷盘回归：多线程 flush_all 下 Group Commit Leader/Follower 协商的
//! 唤醒完整性与持久化水位一致性（对标 Garnet TsavoriteLog Group Commit）。

use std::{sync::Arc, thread::spawn};

use aok::{OK, Void};
use compio::runtime::Runtime;
use log::info;
use wbase::align::DEFAULT_SECTOR_SIZE;

use crate::support::{config, open_store, pad};

/// 对标 Garnet Group Commit 合并语义：写线程持续追加的同时多个刷盘线程并发
/// flush_all，全部调用须成功返回，静默后硬件 sync 水位追平尾地址且数据可回读。
#[test]
fn test_flush_all_concurrent_waiters_site_consistency() -> Void {
  let page_size = DEFAULT_SECTOR_SIZE;
  let env = open_store(
    "concurrent_flush.db",
    config(2048, page_size, 32)?.with_max_sessions(32)?,
  )?;
  let store = env.store;

  let num_writers = 6;
  let num_flushers = 4;
  let mut handles = Vec::new();

  // 写线程：私有分区持续 upsert
  for worker in 0..num_writers {
    let store = Arc::clone(&store);
    handles.push(spawn(move || -> aok::Result<()> {
      let rt = Runtime::new()?;
      rt.block_on(async move {
        let session = store.new_session()?;
        for i in 0..60 {
          let key = format!("cw:w{worker}:k{}", pad(i, 3));
          let val = format!("concurrent_flush_payload_{worker}_{i}_len32_bytes!!");
          session.upsert(key.as_bytes(), val.as_bytes()).await?;
        }
        aok::Result::<()>::Ok(())
      })?;
      Ok(())
    }));
  }

  // 刷盘线程：反复 flush_all，与写线程及彼此竞争 Leader 身份
  for flusher in 0..num_flushers {
    let store = Arc::clone(&store);
    handles.push(spawn(move || -> aok::Result<()> {
      let rt = Runtime::new()?;
      rt.block_on(async move {
        for _ in 0..30 {
          store.flush_all().await?;
          let _ = flusher;
        }
        aok::Result::<()>::Ok(())
      })?;
      Ok(())
    }));
  }

  for handle in handles {
    handle.join().expect("并发刷盘线程正常结束")?;
  }

  // 静默终态：最后一次 flush_all 收尾后，持久化水位须追平尾地址
  let rt = Runtime::new()?;
  rt.block_on(store.flush_all())?;
  let tail = store.tail_address();
  assert_eq!(
    store.synced_until.load(std::sync::atomic::Ordering::Acquire),
    tail,
    "静默后硬件 sync 水位须追平尾地址"
  );

  // 抽样回读验证数据一致性
  rt.block_on(async {
    let session = store.new_session()?;
    for worker in 0..num_writers {
      for i in [0, 30, 59] {
        let key = format!("cw:w{worker}:k{}", pad(i, 3));
        let val = format!("concurrent_flush_payload_{worker}_{i}_len32_bytes!!");
        assert_eq!(session.read(key.as_bytes()).await?, Some(val.into_bytes()));
      }
    }
    aok::Result::<()>::Ok(())
  })?;

  info!("并发 flush_all 唤醒与水位一致性测试通过");
  OK
}
