//! BufferPool 跨线程 Origin-Return 路由测试
//!
//! 对标 C#：libs/storage/Tsavorite/cs/test/SectorAlignedBufferPoolTests.cs 的
//! `CrossThreadReturnRoutesBackToOriginAndReuses`、`CrossThreadDirtyReturnIsLazyClearedOnOwnerReuse`、
//! `LargeClassCrossThreadReturnSharesViaDepot`、`LargeClassOwnerReturnSharesViaDepot`。

use std::{sync::mpsc::channel, thread};

use aok::{OK, Void};
use log::info;
use wbase::{BufferPool, DEFAULT_SECTOR_SIZE, MIN_SECTOR_SIZE};

/// 跨线程归还必须路由回属主线程并在其复用同一底层内存
#[test]
fn cross_thread_return_routes_back_to_origin_and_reuses() -> Void {
  info!("对标 CrossThreadReturnRoutesBackToOriginAndReuses：异线程 Return 后属主 Get 复用同指针");

  let pool = BufferPool::new(MIN_SECTOR_SIZE)?;
  let p1 = pool.get_with_policy(4096, false)?;
  let ptr1 = p1.as_allocated_slice().as_ptr() as usize;

  // 异线程归还：经无锁 CAS 推入属主收件箱
  thread::spawn(move || drop(p1))
    .join()
    .expect("跨线程归还执行成功");

  // 属主线程重新租借：收割收件箱整链后复用
  let p2 = pool.get_with_policy(4096, false)?;
  assert_eq!(
    p2.as_allocated_slice().as_ptr() as usize,
    ptr1,
    "跨线程归还必须路由回属主线程并复用同一分配"
  );

  OK
}

/// 跨线程脏归还由属主惰性清零；反向极性下急切清零结果保持全零
#[test]
fn cross_thread_dirty_return_is_lazy_cleared_on_owner_reuse() -> Void {
  info!("对标 CrossThreadDirtyReturnIsLazyClearedOnOwnerReuse：脏归还惰性清零与反极性验证");

  let pool = BufferPool::new(DEFAULT_SECTOR_SIZE)?;

  // 正向：免清零租借填脏 -> 异线程脏归还 -> 属主默认 Get 惰性清零
  let mut p1 = pool.get_with_policy(4096, false)?;
  p1.as_allocated_slice_mut().fill(0xAB);
  let ptr1 = p1.as_allocated_slice().as_ptr() as usize;
  thread::spawn(move || drop(p1))
    .join()
    .expect("异线程归还成功");

  let p2 = pool.get(4096)?;
  assert_eq!(
    p2.as_allocated_slice().as_ptr() as usize,
    ptr1,
    "脏缓冲必须路由回属主复用"
  );
  assert!(
    p2.as_allocated_slice().iter().all(|&b| b == 0),
    "跨线程收割路径必须惰性清零脏缓冲"
  );
  drop(p2);

  // 反向极性：默认租借填脏 -> 异线程急切清零归还 -> 免清零策略复用仍为全零
  let mut p3 = pool.get_with_policy(4096, true)?;
  p3.as_allocated_slice_mut().fill(0xCD);
  thread::spawn(move || drop(p3))
    .join()
    .expect("异线程归还成功");

  let p4 = pool.get_with_policy(4096, false)?;
  assert_eq!(p4.as_allocated_slice().as_ptr() as usize, ptr1);
  assert!(
    p4.as_allocated_slice().iter().all(|&b| b == 0),
    "异线程急切清零后的缓冲必须保持全零"
  );

  OK
}

/// 大容量缓冲跨线程归还经全局条带仓库共享，第三个非属主线程可复用
#[test]
fn large_class_cross_thread_return_shares_via_depot() -> Void {
  info!("对标 LargeClassCrossThreadReturnSharesViaDepot：1MB 大缓冲经 Depot 跨线程共享");

  let pool = BufferPool::new(DEFAULT_SECTOR_SIZE)?;
  let large_size = 1024 * 1024; // 1 MB：高于 256KB 大容量分层阈值

  // 1. 属主线程创建大缓冲
  let p_clone1 = pool.clone();
  let (tx, rx) = channel();
  thread::spawn(move || {
    let p1 = p_clone1
      .get_with_policy(large_size, false)
      .expect("大缓冲租借成功");
    let ptr1 = p1.as_allocated_slice().as_ptr() as usize;
    tx.send((p1, ptr1)).expect("发送成功");
  })
  .join()
  .expect("属主线程执行成功");
  let (p1, ptr1) = rx.recv().expect("接收成功");

  // 2. 完成线程跨线程归还 -> 直接进入全局条带仓库 Depot
  thread::spawn(move || drop(p1))
    .join()
    .expect("完成线程执行成功");

  // 3. 第三个线程（非属主、非归还方）从 Depot 复用同一底层内存
  let p_clone2 = pool.clone();
  let ptr2 = thread::spawn(move || {
    p_clone2
      .get_with_policy(large_size, false)
      .expect("复用租借成功")
      .as_allocated_slice()
      .as_ptr() as usize
  })
  .join()
  .expect("复用线程执行成功");

  assert_eq!(ptr1, ptr2, "第三线程必须从 Depot 复用同一底层物理指针");

  OK
}

/// 大容量缓冲属主同线程归还亦直接进入全局条带仓库共享
#[test]
fn large_class_owner_return_shares_via_depot() -> Void {
  info!("对标 LargeClassOwnerReturnSharesViaDepot：属主归还的大缓冲不经本地栈，异源线程可命中");

  let pool = BufferPool::new(DEFAULT_SECTOR_SIZE)?;
  let large_size = 1024 * 1024; // 1 MB

  // 1. 属主线程创建并归还大缓冲（大容量 class 不进入线程私有栈，直接推入 Depot）
  let p_clone1 = pool.clone();
  let ptr1 = thread::spawn(move || {
    let p1 = p_clone1
      .get_with_policy(large_size, false)
      .expect("大缓冲租借成功");
    let ptr1 = p1.as_allocated_slice().as_ptr() as usize;
    drop(p1); // 属主同线程归还 -> Depot，而非本地栈
    ptr1
  })
  .join()
  .expect("属主线程执行成功");

  // 2. 异源线程申请相同规格缓冲，直接从 Depot 命中复用
  let p_clone2 = pool.clone();
  let ptr2 = thread::spawn(move || {
    p_clone2
      .get_with_policy(large_size, false)
      .expect("复用租借成功")
      .as_allocated_slice()
      .as_ptr() as usize
  })
  .join()
  .expect("复用线程执行成功");

  assert_eq!(ptr1, ptr2, "异源线程必须从 Depot 命中属主归还的大缓冲");

  OK
}

/// 属主线程退出后密封收件箱，跨线程归还回退 Depot 且首部 FreeNode 区域全零
#[test]
fn sealed_inbox_fallback_zeroing_preserves_free_node_region() -> Void {
  info!("验证属主退出后密封回退路径恢复 32B FreeNode 污染区，复用缓冲 100% 全零");

  let pool = BufferPool::new(DEFAULT_SECTOR_SIZE)?;

  // 1. 属主线程获取缓冲区并填充脏数据后退出（其收件箱随 TLS RAII 自动密封）
  let p1 = {
    let p_clone = pool.clone();
    let (tx, rx) = channel();
    thread::spawn(move || {
      let mut buf = p_clone.get_with_policy(4096, true).expect("租借成功");
      buf.as_allocated_slice_mut().fill(0x7F);
      tx.send(buf).expect("发送成功");
    })
    .join()
    .expect("属主线程退出成功");
    rx.recv().expect("接收成功")
  };

  // 2. 第二线程跨线程归还：属主收件箱已密封，回退进入 Depot（先恢复 FreeNode 污染区）
  thread::spawn(move || drop(p1))
    .join()
    .expect("归还线程执行成功");

  // 3. 第三线程从 Depot 复用，验证整个容量（含首部 32B FreeNode 区域）全零
  let p2 = pool.get(4096)?;
  assert!(
    p2.as_allocated_slice().iter().all(|&b| b == 0),
    "回退进入 Depot 的缓冲区复用时必须 100% 全零（不得残留 FreeNode 非零字节）"
  );

  OK
}

/// 属主线程不退出、不调 Get 时，pool.free() 仍通过 inbox_registry 强制排空异线程在途归还并释放预算
#[test]
fn cross_thread_inbox_drained_on_pool_free() -> Void {
  info!("验证 pool.free() 通过 inbox_registry 密封并排空存活线程的跨线程收件箱");

  let pool = BufferPool::new(DEFAULT_SECTOR_SIZE)?;
  let (tx_buf, rx_buf) = channel();
  let (tx_done, rx_done) = channel();

  let p_clone = pool.clone();
  // 属主线程创建缓冲区并注册收件箱，等待后续指令
  let owner_handle = thread::spawn(move || {
    let buf = p_clone.get_with_policy(4096, true).expect("租借成功");
    tx_buf.send(buf).expect("发送成功");
    // 保持存活，等待测试结束
    rx_done.recv().expect("等待结束");
  });

  let buf = rx_buf.recv().expect("接收缓冲区");
  let reserved_before_free = pool.reserved_bytes();
  assert!(reserved_before_free > 0, "在借缓冲必须占用预算");

  // 异线程归还：推入属主收件箱
  thread::spawn(move || drop(buf))
    .join()
    .expect("异线程归还成功");

  // 此时属主线程尚未收割且未退出，但 pool.free() 必须主动排空其收件箱
  pool.free();
  assert_eq!(
    pool.reserved_bytes(),
    0,
    "pool.free() 必须通过 inbox_registry 排空在途跨线程缓冲并归零预算"
  );

  // 通知属主线程退出
  tx_done.send(()).expect("通知属主退出");
  owner_handle.join().expect("属主线程正常退出");

  OK
}

/// 验证 set_thread_local_byte_cap 与 clean_depot 动态管理功能
#[test]
fn dynamic_thread_local_byte_cap_and_clean_depot() -> Void {
  info!("验证 set_thread_local_byte_cap 动态调整与 clean_depot 非关闭式排空");

  let pool = BufferPool::new(DEFAULT_SECTOR_SIZE)?;
  let initial_cap = pool.thread_local_byte_cap();
  assert!(initial_cap >= wbase::MIN_THREAD_LOCAL_BYTES);

  // 动态扩容与收缩
  pool.set_thread_local_byte_cap(initial_cap * 2);
  assert_eq!(pool.thread_local_byte_cap(), initial_cap * 2);

  pool.set_thread_local_byte_cap(wbase::MIN_THREAD_LOCAL_BYTES / 2);
  assert_eq!(
    pool.thread_local_byte_cap(),
    wbase::MIN_THREAD_LOCAL_BYTES,
    "下调必须受 MIN_THREAD_LOCAL_BYTES 保底"
  );

  // 溢出至 Depot 并通过 clean_depot 排空
  let large_size = 1024 * 1024; // 1MB 属于大容量 class，归还直接进 Depot
  let b = pool.get(large_size)?;
  drop(b);

  let cls = wbase::class_of_sectors(large_size / DEFAULT_SECTOR_SIZE).unwrap();
  assert!(pool.cached_len(cls) > 0, "大缓冲归还必须缓存在 Depot 中");

  // clean_depot 不关闭池，但清空 Depot 缓存
  pool.clean_depot();
  assert_eq!(pool.cached_len(cls), 0, "clean_depot 必须排空全局条带仓库");
  assert!(!pool.is_closed(), "clean_depot 不得关闭池");

  // 池依然可正常租借
  let b2 = pool.get(large_size)?;
  drop(b2);

  pool.free();
  assert_eq!(pool.reserved_bytes(), 0);

  OK
}
