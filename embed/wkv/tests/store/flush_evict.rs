//! 刷盘驱逐、磁盘冷读取、环形缓冲区翻转与极端并发驱逐对抗测试。

use std::{
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
  thread,
  time::Duration,
};

use aok::{OK, Void};
use compio::{runtime::Runtime, time::sleep};
use log::info;
use whlog::SECTOR_ALIGNMENT;

use crate::support::{config, open_store, pad};

/// 对标 Garnet BasicTests.cs: TestShiftHeadAddress / FlushAndEvict ——
/// 页面落盘驱逐后底层 Device 异步冷读取与 CopyUpdate 回流
///
/// 验证目标：
/// 1. 跨页面分配使记录分布在第 0 页与第 1 页。
/// 2. flush_all 将内存脏页持久化到底层块设备（SegmentedDevice）。
/// 3. shift_head_address 将第 0 页驱逐到磁盘区（is_on_disk == true && !is_in_memory）。
/// 4. session.read 对被驱逐的冷数据执行异步磁盘读取，验证数据一致性。
/// 5. 对磁盘上的冷数据发起更新，验证成功触发从磁盘到内存的 CopyUpdate 回流追加。
#[test]
fn test_flush_and_cold_read_evicted_pages() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    // 页面大小采用 4096 字节扇区最小尺寸
    let page_size = SECTOR_ALIGNMENT; // 4096
    let env = open_store("flush_cold_read.db", config(1024, page_size, 16)?)?;
    let store = env.store;
    let session = store.new_session()?;

    // 1. 在第 0 页写入冷记录
    let cold_k1 = b"archive:item:001";
    let cold_v1 = b"cold_payload_data_page_zero_alpha";
    let addr_cold1 = session.upsert(cold_k1, cold_v1).await?;
    assert!(addr_cold1 < page_size as u64);

    let cold_k2 = b"archive:item:002";
    let cold_v2 = b"cold_payload_data_page_zero_beta";
    let addr_cold2 = session.upsert(cold_k2, cold_v2).await?;
    assert!(addr_cold2 < page_size as u64);

    // 2. 写入大填充记录使分配指针跨越第 0 页边界进入第 1 页
    let pad = vec![b'P'; 3900];
    let addr_pad = session.upsert(b"padding_key", &pad).await?;
    assert!(addr_pad >= page_size as u64);

    // 3. 在第 1 页写入热记录
    let hot_k1 = b"hot:item:101";
    let hot_v1 = b"hot_payload_page_one_active";
    let addr_hot1 = session.upsert(hot_k1, hot_v1).await?;
    assert!(addr_hot1 >= page_size as u64);

    // 4. 将脏页全量刷盘落盘
    store.flush_all().await?;

    // 5. 推进 ReadOnlyAddress 与 HeadAddress 将第 0 页彻底逐出内存环形页
    store.shift_read_only_address(page_size as u64);
    store.shift_head_address(page_size as u64);

    // 校验第 0 页记录落在磁盘区且不在内存中
    assert!(store.hlog.is_on_disk(addr_cold1));
    assert!(!store.hlog.is_in_memory(addr_cold1));
    assert!(store.hlog.is_on_disk(addr_cold2));
    assert!(!store.hlog.is_in_memory(addr_cold2));

    // 校验第 1 页记录依然在内存驻送区
    assert!(store.hlog.is_in_memory(addr_hot1));

    // 6. 执行透明冷读取：底层自动从 Device 异步按扇区对齐加载磁盘数据
    let read_cold1 = session.read(cold_k1).await?;
    assert_eq!(
      read_cold1,
      Some(cold_v1.to_vec()),
      "从磁盘异步读取第 0 页冷数据 1 失败"
    );

    let read_cold2 = session.read(cold_k2).await?;
    assert_eq!(
      read_cold2,
      Some(cold_v2.to_vec()),
      "从磁盘异步读取第 0 页冷数据 2 失败"
    );

    // 7. 执行内存热数据读取
    let read_hot1 = session.read(hot_k1).await?;
    assert_eq!(read_hot1, Some(hot_v1.to_vec()), "读取内存热数据失败");

    // 8. 对磁盘冷数据发起更新，验证触发 CopyUpdate 从磁盘回流追加到内存尾部
    let cold_v1_new = b"cold_payload_modified_and_promoted_to_memory";
    let addr_cold1_new = session.upsert(cold_k1, cold_v1_new).await?;
    assert!(
      addr_cold1_new >= page_size as u64,
      "磁盘冷数据更新必须在内存尾部生成新记录"
    );
    assert!(
      store.hlog.is_in_memory(addr_cold1_new),
      "新记录必须驻留在内存可变区"
    );

    // 验证新记录的 prev_address 成功指向原本在磁盘上的 addr_cold1
    let rec_new = store.hlog.read_record(addr_cold1_new).await?;
    assert_eq!(rec_new.prev_address()?, addr_cold1);
    assert_eq!(rec_new.value()?, cold_v1_new);

    // 回读验证值为新值
    let read_cold1_updated = session.read(cold_k1).await?;
    assert_eq!(read_cold1_updated, Some(cold_v1_new.to_vec()));

    info!("页面落盘驱逐后底层 Device 异步冷读取与 CopyUpdate 回流验证通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 无直接 C# 原型 —— 极端并发对抗测试：高频对撞读写与并发换页驱逐交织
///
/// 12 个工作线程同时执行密集读写：
/// - 线程 0..6：执行私有分区密集 Upsert 与 Read
/// - 线程 6..12：高频对撞读写共享热点键（`hot:shared:0` .. `hot:shared:3`），交织执行 Upsert 与 Delete
/// - 1 个后台驱逐控制器线程：
///   - 在所有工作线程疯狂读写的同时，高频交织执行 `flush_all()` 与 `shift_head_address`，
///     强制把脏页持久化并从内存环形缓冲区逐出至底层块设备！
/// - 验证目标：
///   1. `LightEpoch` 并发保护确保任何读者不会在页面驱逐瞬间发生内存访问违规。
///   2. 被驱逐到磁盘的冷页面自动平滑转为底层 `SegmentedDevice` 异步读取。
///   3. 无锁哈希 CAS 在极端锁竞争与内存/磁盘滑动下维持全局一致性与 0 panic。
#[test]
fn test_adversarial_heavy_concurrency_with_eviction() -> Void {
  // 页面采用 4096 字节小尺寸，加速跨页换页与驱逐
  let page_size = SECTOR_ALIGNMENT; // 4096
  let env = open_store(
    "adversarial_eviction.db",
    config(2048, page_size, 32)?.with_max_sessions(64)?,
  )?;
  let store = env.store;

  let is_running = Arc::new(AtomicBool::new(true));
  let num_workers = 12;
  let items_per_worker = 40;
  let mut handles = Vec::new();

  // 1. 启动 12 个读写并发工作线程
  for worker_id in 0..num_workers {
    let store_clone = Arc::clone(&store);
    let handle = thread::spawn(move || -> aok::Result<()> {
      let rt = Runtime::new()?;
      rt.block_on(async move {
        let session = store_clone.new_session()?;

        if worker_id < 6 {
          // 私有分区工作线程：写入并反复回读
          for i in 0..items_per_worker {
            let key = format!("adv:w{}:k{}", worker_id, pad(i, 3));
            let val = format!("adv_payload_{}_{}_len32_bytes!!", worker_id, pad(i, 3));
            session.upsert(key.as_bytes(), val.as_bytes()).await?;

            // 间歇性回读校验
            if i % 5 == 0 {
              let r = session.read(key.as_bytes()).await?;
              assert_eq!(r, Some(val.into_bytes()));
            }
          }
        } else {
          // 热点碰撞工作线程：对共享键高频对撞更新与删除
          for round in 0..items_per_worker {
            let hot_k = format!("adv:hot:shared:{}", round % 4);
            let hot_v = format!("hot_v_w{}_r{}", worker_id, round);
            session.upsert(hot_k.as_bytes(), hot_v.as_bytes()).await?;

            if round % 3 == 0 {
              session.delete(hot_k.as_bytes()).await?;
            }
          }
        }

        aok::Result::<()>::Ok(())
      })?;
      Ok(())
    });
    handles.push(handle);
  }

  // 2. 启动 1 个并发换页驱逐控制线程
  let store_evictor = Arc::clone(&store);
  let is_running_evictor = Arc::clone(&is_running);
  let evictor_handle = thread::spawn(move || -> aok::Result<()> {
    let rt = Runtime::new()?;
    rt.block_on(async move {
      let mut current_evict_page = 0;
      while is_running_evictor.load(Ordering::Acquire) {
        // 尝试刷盘
        let _ = store_evictor.flush_all().await;

        // 推进 ReadOnlyAddress 与 HeadAddress 强制将旧页驱逐到磁盘
        let tail = store_evictor.tail_address();
        let max_page = tail / page_size as u64;
        if current_evict_page < max_page {
          let evict_addr = current_evict_page * page_size as u64;
          store_evictor.shift_read_only_address(evict_addr);
          store_evictor.shift_head_address(evict_addr);
          current_evict_page += 1;
        }

        // 异步任务内必须使用 compio 定时器让出，禁止阻塞 OS 线程
        sleep(Duration::from_millis(5)).await;
      }
      aok::Result::<()>::Ok(())
    })?;
    Ok(())
  });

  // 3. 等待所有工作线程完成
  for handle in handles {
    handle.join().unwrap()?;
  }

  // 通知并等待驱逐线程退出
  is_running.store(false, Ordering::Release);
  evictor_handle.join().unwrap()?;

  // 4. 最终全量落盘并验证私有分区数据一致性
  let verify_rt = Runtime::new()?;
  verify_rt.block_on(async {
    store.flush_all().await?;
    let check_session = store.new_session()?;

    for worker_id in 0..6 {
      for i in 0..items_per_worker {
        let key = format!("adv:w{}:k{}", worker_id, pad(i, 3));
        let expected_val = format!("adv_payload_{}_{}_len32_bytes!!", worker_id, pad(i, 3));
        let actual = check_session.read(key.as_bytes()).await?;
        assert_eq!(
          actual,
          Some(expected_val.into_bytes()),
          "极端并发与换页驱逐交织后私有键数据不一致: w={worker_id}, i={i}"
        );
      }
    }
    aok::Result::<()>::Ok(())
  })?;

  info!("极端并发对抗测试（高频对撞读写与并发换页驱逐交织）验证通过");
  OK
}

/// 对标 Garnet BasicTests.cs: TestShiftHeadAddress / FlushAndEvict ——
/// 环形缓冲区翻转覆盖后的全量刷盘驱逐与大批量冷读取验证
///
/// 验证目标：
/// 1. 写入超出环形缓冲区容量（16 个 4KB 页 = 64KB，写入 > 80KB 数据）的批量记录，
///    强制触发环形缓冲区槽位循环覆盖（Wrap-around）。
/// 2. 调用 `flush_and_evict_all()`，验证 `flush_all()` 能够正确识别内存驻留页面范围，
///    绝不将环形槽位覆写后的脏页错误地覆盖回已被逐出历史页面的磁盘偏移。
/// 3. 全量批量校验 `contains_key` 与 `read`：从磁盘冷读取被逐出内存的所有记录，
///    验证数据 100% 完整无损且未被后续页面内容污染破坏。
/// 4. 验证 `read_record(addr)`（ReadAtAddress）在纪元保护下直接读取物理记录的能力。
/// 5. 对已被驱逐至磁盘的大量冷记录发起更新，验证透明触发 CopyUpdate 回流至内存活跃区。
#[test]
fn test_flush_and_evict_large_dataset_cold_reads() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let page_size = SECTOR_ALIGNMENT; // 4096 字节
    let num_pages = 8; // 环形缓冲区 8 页，总容量 32KB
    let env = open_store("flush_evict_large.db", config(2048, page_size, num_pages)?)?;
    let store = env.store;
    let session = store.new_session()?;

    // 写入 300 条变长记录：每页约 30 条，300 条跨越 10+ 页，
    // 强制发生环形缓冲区多页循环复用覆盖（10 > 8）
    let count = 300;
    let mut addrs = Vec::with_capacity(count);

    for i in 0..count {
      let key = format!("cold_dataset:k{}", pad(i, 4));
      let val = format!(
        "cold_dataset_payload_{}_padding_to_exact_100_bytes_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
        pad(i, 4)
      );
      let addr = session.upsert(key.as_bytes(), val.as_bytes()).await?;
      addrs.push(addr);

      // 周期性刷盘与驱逐，加速环形页推进
      if i > 0 && i % 30 == 0 {
        let current_tail = store.tail_address();
        let safe_evict = (current_tail / page_size as u64).saturating_sub(3) * page_size as u64;
        store.flush_all().await?;
        store.shift_read_only_address(safe_evict);
        store.shift_head_address(safe_evict);
      }
    }

    // 验证确实写入并跨越了多个页面，且超过了环形缓冲区的 8 页
    let tail = store.tail_address();
    let total_pages = tail / page_size as u64;
    assert!(
      total_pages >= num_pages as u64,
      "必须跨越超过环形缓冲区总页数: total_pages={total_pages}, num_pages={num_pages}"
    );

    // 执行对标 Tsavorite 的全量落盘并逐出所有内存页面（FlushAndEvict）
    store.flush_and_evict_all().await?;
    assert_eq!(store.head_address(), tail);
    assert_eq!(store.read_only_address(), tail);

    // 此时所有写入的 400 条记录必须全部处于磁盘冷数据区
    for &addr in &addrs {
      assert!(store.hlog.is_on_disk(addr));
      assert!(!store.hlog.is_in_memory(addr));
    }

    // 1. 全量回读校验冷数据（验证未被环形槽位复用写坏）
    for (i, &addr) in addrs.iter().enumerate().take(count) {
      let key = format!("cold_dataset:k{}", pad(i, 4));
      let expected_val = format!(
        "cold_dataset_payload_{}_padding_to_exact_100_bytes_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
        pad(i, 4)
      );

      // 校验 contains_key
      assert!(
        session.contains_key(key.as_bytes()).await?,
        "冷数据键 {key} 必须存在"
      );

      // 校验 read 返回的值
      let actual = session.read(key.as_bytes()).await?;
      assert_eq!(
        actual,
        Some(expected_val.into_bytes()),
        "从磁盘读取已被驱逐的冷数据内容损坏: 索引 {i}"
      );

      // 校验磁盘区记录直读（ReadAtAddress）：记录已被全量驱逐，走设备级 read_disk_record；
      // 物理记录键为会话前缀物理键，比对须携带前缀
      let str_k = session.session_string_key(key.as_bytes());
      let rec_out = store.hlog.read_disk_record(addr).await?;
      assert_eq!(rec_out.key()?, str_k.as_slice());
      assert!(!rec_out.is_tombstone()?);
    }

    // 2. 校验不存在与已删除的键 contains_key 行为
    assert!(!session.contains_key(b"never_written_key").await?);

    let del_key = format!("cold_dataset:k{}", pad(123, 4));
    assert!(session.delete(del_key.as_bytes()).await?);
    assert!(!session.contains_key(del_key.as_bytes()).await?);
    assert_eq!(session.read(del_key.as_bytes()).await?, None);

    // 3. 对已被驱逐至磁盘的冷数据执行更新，验证 CopyUpdate 回流追加至内存活跃区
    let update_key = format!("cold_dataset:k{}", pad(250, 4));
    let new_val = b"updated_promoted_back_to_memory_active_region";
    let new_addr = session.upsert(update_key.as_bytes(), new_val).await?;
    assert!(new_addr >= tail, "更新必须在当前活跃尾部分配新记录");
    assert!(store.hlog.is_in_memory(new_addr), "新记录必须驻留在内存中");

    let read_back = session.read(update_key.as_bytes()).await?;
    assert_eq!(read_back, Some(new_val.to_vec()));

    info!("环形缓冲区翻转覆盖后的全量刷盘驱逐与大批量冷读取验证通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}
