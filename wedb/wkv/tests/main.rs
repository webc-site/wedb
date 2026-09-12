use std::{sync::Arc, thread};

use aok::{OK, Void};
use compio::runtime::Runtime;
use log::info;
use tempfile::tempdir;
use wdev::SegmentedDevice;
use whlog::SECTOR_ALIGNMENT;
use wkv::{StoreConfig, WedbStore};

use crate::support::pad;

mod store;
mod support;

#[ctor::ctor(unsafe)]
fn _log_init() {
  log_init::init();
}

/// 测试 1: 基本读写与查空测试
#[test]
fn test_basic_read_write() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("store_test1.db");
    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);

    let config = StoreConfig::new(1024, 64 * 1024, 16, 0.5)?;
    let store = Arc::new(WedbStore::open(config, device)?);
    let session = store.new_session()?;

    // 1. 读取不存在的 Key 应返回 None
    let missing = session.read(b"not_exist_key").await?;
    assert_eq!(missing, None, "未写入的 Key 读取应返回 None");

    // 2. 写入两条记录
    let k1 = b"user:1001";
    let v1 = b"alice_payload";
    let addr1 = session.upsert(k1, v1).await?;
    assert!(addr1 > 0);

    let k2 = b"user:1002";
    let v2 = b"bob_payload";
    let addr2 = session.upsert(k2, v2).await?;
    assert!(addr2 > addr1);

    // 3. 读取并验证记录内容
    let read_v1 = session.read(k1).await?;
    assert_eq!(read_v1, Some(v1.to_vec()));

    let read_v2 = session.read(k2).await?;
    assert_eq!(read_v2, Some(v2.to_vec()));

    // 4. 验证有效条目总数与刷盘
    assert_eq!(store.entry_count(), 2);
    store.flush_all().await?;

    info!("测试 1: 基本读写与查空测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 测试 2: 热点 Key 内存原位覆写（In-place update）验证
#[test]
fn test_in_place_update() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("store_test2.db");
    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);

    let config = StoreConfig::new(1024, 64 * 1024, 16, 0.5)?;
    let store = Arc::new(WedbStore::open(config, device)?);
    let session = store.new_session()?;

    let key = b"hot_counter";
    let v1 = b"cnt_0001"; // 8 字节
    let addr1 = session.upsert(key, v1).await?;

    // 原位覆写：相同长度在可变区必须实现原位更新，返回完全相同的逻辑地址
    let v2 = b"cnt_0002"; // 8 字节
    let addr2 = session.upsert(key, v2).await?;
    assert_eq!(addr1, addr2, "相同长度值原位覆写时逻辑地址必须保持不变");

    let v3 = b"cnt_9999"; // 8 字节
    let addr3 = session.upsert(key, v3).await?;
    assert_eq!(addr1, addr3, "多次原位覆写时逻辑地址必须保持不变");

    // 回读验证最新内容
    let val = session.read(key).await?;
    assert_eq!(val, Some(v3.to_vec()));

    // 索引条目总数依然为 1
    assert_eq!(store.entry_count(), 1);

    info!("测试 2: 热点 Key 内存原位覆写验证通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 测试 3: RCU 追加与版本链读取最新值验证
#[test]
fn test_rcu_append_and_version_chain() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("store_test3.db");
    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);

    let config = StoreConfig::new(1024, 64 * 1024, 16, 0.5)?;
    let store = Arc::new(WedbStore::open(config, device)?);
    let session = store.new_session()?;

    let key = b"profile:name";
    let v1 = b"tom"; // 3 字节
    let addr1 = session.upsert(key, v1).await?;

    // 长度发生改变（3 -> 18），不可原位更新，触发 RCU 追加
    let v2 = b"thomas_edward_18B";
    let addr2 = session.upsert(key, v2).await?;
    assert!(addr2 > addr1, "长度改变必须追加新版本记录");

    // 读取必须返回最新版本
    let current_val = session.read(key).await?;
    assert_eq!(current_val, Some(v2.to_vec()));

    // 追溯版本链：优化3（严格对标 InternalUpsert.cs:CreateNewRecordUpsert
    // elideSourceRecord + Helpers.cs:CanElide）——v1 为链首唯一记录且 prev=0 <
    // BeginAddress，v2 变长盲插时 v1 被脱钩 elide，新记录前驱接管旧记录前驱（0）
    let rec2 = store.hlog.read_record(addr2).await?;
    assert_eq!(rec2.prev_address()?, 0);

    let rec1 = store.hlog.read_record(addr1).await?;
    assert_eq!(rec1.prev_address()?, 0);
    assert_eq!(rec1.value()?, v1);

    // 再次追加第 3 个版本
    let v3 = b"thomas_edward_the_third";
    let addr3 = session.upsert(key, v3).await?;
    assert!(addr3 > addr2);

    let rec3 = store.hlog.read_record(addr3).await?;
    // 优化3：v2 为链首记录且其前驱 0 < BeginAddress，v3 盲插时 v2 被脱钩 elide，
    // v3 前驱接管 v2 的前驱（0）
    assert_eq!(rec3.prev_address()?, 0);
    assert_eq!(rec3.value()?, v3);

    // 索引条目总数依然为 1（旧地址被 CAS 替换）
    assert_eq!(store.entry_count(), 1);

    info!("测试 3: RCU 追加与版本链读取最新值验证通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 测试 4: 墓碑删除（Delete）与后续 Read 返回 None 验证
#[test]
fn test_tombstone_delete_and_resurrect() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("store_test4.db");
    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);

    let config = StoreConfig::new(1024, 64 * 1024, 16, 0.5)?;
    let store = Arc::new(WedbStore::open(config, device)?);
    let session = store.new_session()?;

    let key = b"session:token";
    let val = b"secret_token_12345";
    let addr_first = session.upsert(key, val).await?;
    assert_eq!(session.read(key).await?, Some(val.to_vec()));

    // 1. 首次删除，返回 true
    let deleted = session.delete(key).await?;
    assert!(deleted, "删除已存在的 Key 必须返回 true");

    // 2. 幂等删除：再次删除相同 Key，返回 false
    let deleted_again = session.delete(key).await?;
    assert!(!deleted_again, "重复删除已是墓碑的 Key 应返回 false");

    // 3. 读取已删除的 Key 必须返回 None
    let read_deleted = session.read(key).await?;
    assert_eq!(read_deleted, None, "已删除的 Key 读取必须返回 None");

    // 4. 复活已删除的 Key：重新写入新值
    let new_val = b"new_revived_token";
    let addr_revived = session.upsert(key, new_val).await?;
    let read_revived = session.read(key).await?;
    assert_eq!(read_revived, Some(new_val.to_vec()));

    // 5. 验证墓碑记录与版本链追溯：优化3（严格对标 InternalUpsert.cs
    // CreateNewRecordUpsert elideSourceRecord + Helpers.cs:CanElide）——复活写入时
    // 墓碑为链首唯一记录且 prev=0 < BeginAddress，被脱钩 elide，新记录前驱接管为 0；
    // 墓碑记录本身仍留存日志可按地址直读验证
    let rec_revived = store.hlog.read_record(addr_revived).await?;
    let tombstone_addr = rec_revived.prev_address()?;
    assert_eq!(tombstone_addr, 0);

    let rec_tombstone = store.hlog.read_record(addr_first).await?;
    assert!(rec_tombstone.is_tombstone()?);

    info!("测试 4: 墓碑删除与后续 Read 返回 None 及复活验证通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 测试 5: 多会话并发读写测试（多线程 + compio 异步并发）
#[test]
fn test_concurrent_sessions() -> Void {
  let dir = tempdir()?;
  let db_path = dir.path().join("store_test5.db");
  let device = Arc::new(SegmentedDevice::single_file(&db_path)?);

  let config = StoreConfig::new(2048, 64 * 1024, 16, 0.5)?;
  let store = Arc::new(WedbStore::open(config, device)?);

  let num_threads = 4;
  let items_per_thread = 50;
  let mut handles = Vec::new();

  for thread_id in 0..num_threads {
    let store_clone = Arc::clone(&store);
    let handle = thread::spawn(move || -> aok::Result<()> {
      let rt = Runtime::new()?;
      rt.block_on(async move {
        let session = store_clone.new_session()?;

        // 写入该线程负责的所有键值对
        for i in 0..items_per_thread {
          let mut key = String::from("t");
          key.push_str(itoa::Buffer::new().format(thread_id));
          key.push_str(":k");
          key.push_str(&pad(i, 4));
          let mut val = String::from("val_");
          val.push_str(itoa::Buffer::new().format(thread_id));
          val.push('_');
          val.push_str(&pad(i, 4));
          session.upsert(key.as_bytes(), val.as_bytes()).await?;
        }

        // 回读校验一致性
        for i in 0..items_per_thread {
          let mut key = String::from("t");
          key.push_str(itoa::Buffer::new().format(thread_id));
          key.push_str(":k");
          key.push_str(&pad(i, 4));
          let mut expected_val = String::from("val_");
          expected_val.push_str(itoa::Buffer::new().format(thread_id));
          expected_val.push('_');
          expected_val.push_str(&pad(i, 4));
          let actual_val = session.read(key.as_bytes()).await?;
          assert_eq!(
            actual_val,
            Some(expected_val.into_bytes()),
            "并发读取数据不一致"
          );
        }

        aok::Result::<()>::Ok(())
      })?;
      Ok(())
    });
    handles.push(handle);
  }

  for handle in handles {
    handle.join().unwrap()?;
  }

  assert_eq!(store.entry_count(), num_threads * items_per_thread);
  info!("测试 5: 多会话并发读写测试通过");

  OK
}

/// 测试 6: 换页刷盘并驱逐后从磁盘读取冷数据验证
#[test]
fn test_flush_evict_and_cold_read() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("store_test6.db");
    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);

    // 单页大小使用最小扇区尺寸 4096 字节
    let page_size = SECTOR_ALIGNMENT; // 4096
    let config = StoreConfig::new(1024, page_size, 16, 0.5)?;
    let store = Arc::new(WedbStore::open(config, device)?);
    let session = store.new_session()?;

    // 1. 在第 0 页写入冷数据记录
    let cold_key = b"archive:record:0";
    let cold_val = b"payload_stored_in_page_zero_before_eviction";
    let addr0 = session.upsert(cold_key, cold_val).await?;
    assert!(addr0 < page_size as u64);

    // 2. 写入大记录迫使发生换页（跨入第 1 页）
    let big_val = vec![b'X'; 3950];
    let addr_big = session.upsert(b"big_pad", &big_val).await?;
    assert!(addr_big >= page_size as u64);

    // 3. 在第 1 页写入热数据记录
    let hot_key = b"hot:record:1";
    let hot_val = b"payload_stored_in_page_one";
    let addr1 = session.upsert(hot_key, hot_val).await?;
    assert!(addr1 >= page_size as u64);

    // 4. 将所有脏页落盘
    store.flush_all().await?;

    // 5. 推进 HeadAddress 将第 0 页从内存驱逐到磁盘区
    store.hlog.shift_read_only_address(page_size as u64);
    store.hlog.shift_head_address(page_size as u64);

    assert!(store.hlog.is_on_disk(addr0), "第 0 页记录必须落在磁盘区");
    assert!(!store.hlog.is_in_memory(addr0), "第 0 页记录已被逐出内存");
    assert!(store.hlog.is_in_memory(addr1), "第 1 页记录仍在内存中");

    // 6. 异步透明读取磁盘冷数据
    let read_cold = session.read(cold_key).await?;
    assert_eq!(
      read_cold,
      Some(cold_val.to_vec()),
      "必须成功从磁盘读取已驱逐的冷数据"
    );

    // 7. 异步读取内存热数据
    let read_hot = session.read(hot_key).await?;
    assert_eq!(read_hot, Some(hot_val.to_vec()), "内存热数据必须可正常读取");

    info!("测试 6: 换页刷盘并驱逐后从磁盘读取冷数据验证通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}
