//! 基础 CRUD、原位更新与 CopyUpdate、并发会话、空键空值边界与会话生命周期测试。

use std::{sync::Arc, thread};

use aok::{OK, Void};
use compio::runtime::Runtime;
use log::info;

use crate::support::{config, open_store, pad};

/// 对标 Garnet BasicTests.cs: NativeInMemWriteRead / NativeInMemWriteReadDelete /
/// NativeInMemWriteReadDelete2 —— 基础 CRUD 流水线与有效条目数校验
///
/// 验证目标：
/// 1. 基本 Upsert 写入与 Read 校验。
/// 2. 未写入 Key 读取返回 None。
/// 3. Delete 墓碑标记删除与后续读取返回 None。
/// 4. 幂等删除（重复删除返回 false）。
/// 5. 复活已删除的 Key 并验证新值。
/// 6. 批量 100 条记录的 Upsert -> Delete -> NotFound -> Re-upsert 流水线。
/// 7. 索引有效条目数（EntryCount）在全生命周期内的一致性。
#[test]
fn test_basic_upsert_read_delete() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let env = open_store("basic_crud.db", config(2048, 64 * 1024, 16)?)?;
    let store = env.store;
    let session = store.new_session()?;

    // 初始状态：条目数为 0
    assert_eq!(store.entry_count(), 0);

    // 1. 读取不存在的 Key 返回 None
    let non_exist = session.read(b"missing_key_001").await?;
    assert_eq!(non_exist, None, "未写入的 Key 应返回 None");

    // 2. 对照 NativeInMemWriteRead: 写入单条记录并读取
    let key1 = b"user:account:1001";
    let val1 = b"value_payload_alpha";
    let addr1 = session.upsert(key1, val1).await?;
    assert!(addr1 > 0);
    assert_eq!(store.entry_count(), 1);

    let read_val1 = session.read(key1).await?;
    assert_eq!(read_val1, Some(val1.to_vec()));

    // 3. 对照 NativeInMemWriteReadDelete: 首次删除与重复删除
    let del1 = session.delete(key1).await?;
    assert!(del1, "首次删除已存在的 Key 必须返回 true");

    let del1_again = session.delete(key1).await?;
    assert!(!del1_again, "重复删除已是墓碑的 Key 必须返回 false（幂等）");

    let read_deleted1 = session.read(key1).await?;
    assert_eq!(read_deleted1, None, "已删除的 Key 读取必须返回 None");

    // 4. 复活已删除的 Key
    let val1_revived = b"value_payload_alpha_v2_revived";
    let addr1_revived = session.upsert(key1, val1_revived).await?;
    assert!(addr1_revived > addr1);
    let read_revived = session.read(key1).await?;
    assert_eq!(read_revived, Some(val1_revived.to_vec()));
    assert_eq!(store.entry_count(), 1);

    // 5. 对照 NativeInMemWriteReadDelete2: 批量 100 条记录 CRUD 流水线
    const COUNT: usize = 100;
    for i in 0..COUNT {
      let k = format!("batch_key_{}", pad(i, 4));
      let v = format!("batch_val_{}", pad(i, 4));
      session.upsert(k.as_bytes(), v.as_bytes()).await?;
    }
    assert_eq!(store.entry_count(), 1 + COUNT);

    // 批量回读校验
    for i in 0..COUNT {
      let k = format!("batch_key_{}", pad(i, 4));
      let expected = format!("batch_val_{}", pad(i, 4));
      let actual = session.read(k.as_bytes()).await?;
      assert_eq!(actual, Some(expected.into_bytes()));
    }

    // 批量全部删除
    for i in 0..COUNT {
      let k = format!("batch_key_{}", pad(i, 4));
      let deleted = session.delete(k.as_bytes()).await?;
      assert!(deleted, "批次键 {k} 删除必须成功");
    }

    // 批量验证均已 NotFound (None)
    for i in 0..COUNT {
      let k = format!("batch_key_{}", pad(i, 4));
      let val = session.read(k.as_bytes()).await?;
      assert_eq!(val, None, "批次键 {k} 已经删除，读取必须为 None");
    }

    // 批量重新 Upsert 新值
    for i in 0..COUNT {
      let k = format!("batch_key_{}", pad(i, 4));
      let new_v = format!("batch_new_val_{}", pad(i, 4));
      session.upsert(k.as_bytes(), new_v.as_bytes()).await?;
    }

    // 再次回读验证新值
    for i in 0..COUNT {
      let k = format!("batch_key_{}", pad(i, 4));
      let expected = format!("batch_new_val_{}", pad(i, 4));
      let actual = session.read(k.as_bytes()).await?;
      assert_eq!(actual, Some(expected.into_bytes()));
    }

    info!("基础 CRUD 流水线与有效条目数校验通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 Garnet NeedCopyUpdateTests.cs: TryAddTest / CopyUpdateFromHeadReadOnlyPageTest ——
/// 热点内存原位修改 vs 只读/长度变更触发 CopyUpdate
///
/// 验证目标：
/// 1. 内存可变区（Mutable Region）相同长度值更新：必须原位覆写（In-Place Update），
///    返回完全一致的逻辑地址（零 I/O、零日志追加分配）。
/// 2. 推进 ReadOnlyAddress 将记录划入只读区（ReadOnly Region）：
///    后续更新必须触发 CopyUpdate（RCU 追加写入新版本），分配新逻辑地址（addr > old_addr）。
/// 3. 新版本记录的前驱地址（prev_address）必须精准指向旧版本逻辑地址。
/// 4. 内存可变区内长度变化更新（如变长）：不可原位覆写，同样触发 CopyUpdate。
#[test]
fn test_in_place_update_vs_copy_update() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let env = open_store("in_place_vs_copy.db", config(1024, 64 * 1024, 16)?)?;
    let store = env.store;
    let session = store.new_session()?;

    let key = b"hot_metric:qps";
    let val_v1 = b"val_1000"; // 8 字节
    let addr_v1 = session.upsert(key, val_v1).await?;
    assert!(addr_v1 > 0);

    // 阶段 1: 内存可变区同长度原位覆写（In-Place Update）
    let val_v2 = b"val_2000"; // 8 字节
    let addr_v2 = session.upsert(key, val_v2).await?;
    assert_eq!(
      addr_v1, addr_v2,
      "内存可变区且相同长度值原位更新时逻辑地址必须保持不变"
    );

    let read_v2 = session.read(key).await?;
    assert_eq!(read_v2, Some(val_v2.to_vec()));

    let val_v3 = b"val_3000"; // 8 字节
    let addr_v3 = session.upsert(key, val_v3).await?;
    assert_eq!(addr_v1, addr_v3, "多次同长度覆写逻辑地址保持恒定");

    // 阶段 2: 推进 ReadOnlyAddress 将记录置为只读，触发 CopyUpdate
    // 对照 Tsavorite: store.Log.ShiftReadOnlyAddress(store.Log.TailAddress)
    let tail = store.tail_address();
    store.shift_read_only_address(tail);
    assert_eq!(store.read_only_address(), tail);

    // 记录现在处于只读区，尝试更新相同长度的 val_v4
    let val_v4 = b"val_4000"; // 8 字节
    let addr_v4 = session.upsert(key, val_v4).await?;
    assert!(
      addr_v4 > addr_v3,
      "记录处于只读区时更新必须触发 CopyUpdate 追加新版本记录: addr_v4={addr_v4}, addr_v3={addr_v3}"
    );

    // 回读验证最新值
    let read_v4 = session.read(key).await?;
    assert_eq!(read_v4, Some(val_v4.to_vec()));

    // 验证版本链追溯：addr_v4 的 prev_address 必须指向 addr_v3
    let rec_v4 = store.hlog.read_record(addr_v4).await?;
    assert_eq!(rec_v4.prev_address()?, addr_v3);
    assert_eq!(rec_v4.value()?, val_v4);

    // 验证旧版本记录依然保留在日志只读区未被破坏
    let rec_v3 = store.hlog.read_record(addr_v3).await?;
    assert_eq!(rec_v3.value()?, val_v3);

    // 阶段 3: 内存可变区内长度发生变化，触发 CopyUpdate
    let val_v5_larger = b"val_5000_expanded_payload_32B!!"; // 32 字节
    let addr_v5 = session.upsert(key, val_v5_larger).await?;
    assert!(addr_v5 > addr_v4, "长度扩容必须触发 RCU 追加");

    let rec_v5 = store.hlog.read_record(addr_v5).await?;
    assert_eq!(rec_v5.prev_address()?, addr_v4);
    assert_eq!(rec_v5.value()?, val_v5_larger);

    let read_v5 = session.read(key).await?;
    assert_eq!(read_v5, Some(val_v5_larger.to_vec()));

    // 整个过程中虽然生成了多个历史版本，但哈希索引只记录最新指针，entry_count 依然为 1
    assert_eq!(store.entry_count(), 1);

    info!("热点内存原位修改 vs 只读/长度变更触发 CopyUpdate 验证通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 Garnet Tsavorite 并发会话模型（每线程独立 ClientSession + LightEpoch 参与者）——
/// 多线程并发 ClientSession 压力读写与 Epoch 保护一致性
///
/// 验证目标：
/// 1. 多线程并发执行密集 Upsert 与 Read，包含私有分区键与共享热点键竞争。
/// 2. 验证 Epoch 保护、无锁哈希索引 CAS 竞争重试机制与最终一致性。
#[test]
fn test_concurrent_sessions_upsert_read() -> Void {
  let env = open_store("concurrent.db", config(4096, 64 * 1024, 16)?)?;
  let store = env.store;

  let num_threads = 8;
  let items_per_thread = 50;
  let shared_keys_count = 5;
  let mut handles = Vec::new();

  for thread_id in 0..num_threads {
    let store_clone = Arc::clone(&store);
    let handle = thread::spawn(move || -> aok::Result<()> {
      let rt = Runtime::new()?;
      rt.block_on(async move {
        let session = store_clone.new_session()?;

        // 1. 并发写入线程私有分区键
        for i in 0..items_per_thread {
          let key = format!("worker:{}:item:{}", thread_id, pad(i, 4));
          let val = format!("payload_w{}_{}", thread_id, pad(i, 4));
          session.upsert(key.as_bytes(), val.as_bytes()).await?;
        }

        // 2. 并发竞争写入共享热点键（模拟无锁 CAS 冲突并发更新）
        for round in 0..10 {
          for s in 0..shared_keys_count {
            let shared_key = format!("global:hot_key:{}", s);
            let shared_val = format!("thread_{}_round_{}", thread_id, round);
            session
              .upsert(shared_key.as_bytes(), shared_val.as_bytes())
              .await?;
          }
        }

        // 3. 回读私有分区键校验强一致性
        for i in 0..items_per_thread {
          let key = format!("worker:{}:item:{}", thread_id, pad(i, 4));
          let expected_val = format!("payload_w{}_{}", thread_id, pad(i, 4));
          let actual_val = session.read(key.as_bytes()).await?;
          assert_eq!(
            actual_val,
            Some(expected_val.into_bytes()),
            "并发私有键读取数据不一致: 线程 {thread_id}, 索引 {i}"
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

  // 校验全局条目总数：私有键总数 (num_threads * items_per_thread) + 共享热点键数
  let expected_entries = num_threads * items_per_thread + shared_keys_count;
  assert_eq!(
    store.entry_count(),
    expected_entries,
    "最终有效条目数不符合预期"
  );

  // 验证共享热点键可正常读取
  let verify_rt = Runtime::new()?;
  verify_rt.block_on(async {
    let check_session = store.new_session()?;
    for s in 0..shared_keys_count {
      let shared_key = format!("global:hot_key:{}", s);
      let val = check_session.read(shared_key.as_bytes()).await?;
      assert!(val.is_some(), "共享热点键 {shared_key} 必须存在");
    }
    aok::Result::<()>::Ok(())
  })?;

  info!("多线程并发 ClientSession 压力读写与 Epoch 保护一致性验证通过");
  OK
}

/// 无直接 C# 原型 —— 空键与空值（0 字节 Key / Value）极端边界 CRUD
///
/// 验证目标：
/// 1. 空键写入非空值：`upsert(b"", b"empty_key_val")` 正常分配、索引并可准确读取。
/// 2. 非空键写入空值：`upsert(b"empty_val_key", b"")` 正常返回 `Some(vec![])`，
///    区分空值存在与墓碑/键不存在（None）。
/// 3. 空值原位修改（`b""` -> `b""`）与长度变更扩容（`b""` -> `b"expanded"`）及缩容（`b"expanded"` -> `b""`）。
/// 4. 双空键值：`upsert(b"", b"")` 写入、读取、删除（幂等验证）与复活全生命周期。
/// 5. 过程中 EntryCount 有效条目数的准确性。
#[test]
fn test_edge_empty_key_value_crud() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let env = open_store("edge_empty.db", config(1024, 64 * 1024, 16)?)?;
    let store = env.store;
    let session = store.new_session()?;

    // 1. 0 字节 Key，非空 Value
    let val_k_empty = b"value_for_empty_key";
    let addr_empty_k = session.upsert(b"", val_k_empty).await?;
    assert!(addr_empty_k > 0);
    assert_eq!(store.entry_count(), 1);

    let read_empty_k = session.read(b"").await?;
    assert_eq!(read_empty_k, Some(val_k_empty.to_vec()));

    let del_empty_k = session.delete(b"").await?;
    assert!(del_empty_k);
    assert_eq!(session.read(b"").await?, None);
    assert!(!session.delete(b"").await?); // 幂等删除
    assert_eq!(
      store.entry_count(),
      1,
      "墓碑记录在哈希索引中仍保留槽位，与 Tsavorite 对齐"
    );

    // 2. 非空 Key，0 字节 Value
    let k_empty_v = b"key_with_empty_val";
    let addr_empty_v1 = session.upsert(k_empty_v, b"").await?;
    assert!(addr_empty_v1 > 0);
    assert_eq!(store.entry_count(), 2);

    let read_empty_v = session.read(k_empty_v).await?;
    assert_eq!(
      read_empty_v,
      Some(Vec::new()),
      "空值必须返回 Some(空切片)，不能与 None 混淆"
    );

    // 相同 0 字节原位修改保持地址不变
    let addr_empty_v2 = session.upsert(k_empty_v, b"").await?;
    assert_eq!(addr_empty_v1, addr_empty_v2, "0 字节原位修改地址保持恒定");

    // 扩容为非空值触发 RCU 追加
    let expanded = b"expanded_payload";
    let addr_expanded = session.upsert(k_empty_v, expanded).await?;
    assert!(addr_expanded > addr_empty_v2);
    assert_eq!(session.read(k_empty_v).await?, Some(expanded.to_vec()));

    // 缩容回 0 字节值：在动态松弛机制下原位覆写成功，保持地址恒定（消除写放大与 RCU 追加）
    let addr_shrunk = session.upsert(k_empty_v, b"").await?;
    assert_eq!(addr_shrunk, addr_expanded, "动态松弛原位覆写保持地址恒定");
    assert_eq!(session.read(k_empty_v).await?, Some(Vec::new()));

    session.delete(k_empty_v).await?;
    assert_eq!(session.read(k_empty_v).await?, None);
    assert_eq!(store.entry_count(), 2);

    // 3. 双 0 字节：0 字节 Key + 0 字节 Value（复用 b"" 的槽位）
    let addr_both_empty = session.upsert(b"", b"").await?;
    assert!(addr_both_empty > 0);
    assert_eq!(store.entry_count(), 2);
    assert_eq!(session.read(b"").await?, Some(Vec::new()));

    let del_both = session.delete(b"").await?;
    assert!(del_both);
    assert_eq!(session.read(b"").await?, None);

    // 复活双 0 字节
    let addr_revived = session.upsert(b"", b"").await?;
    assert!(addr_revived > addr_both_empty);
    assert_eq!(session.read(b"").await?, Some(Vec::new()));
    assert_eq!(store.entry_count(), 2);

    info!("空键与空值极端边界 CRUD 验证通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 无直接 C# 原型 —— 超大变长数据处理与跨页边界极端防御
///
/// 验证目标：
/// 1. 记录超限防御：单条记录总大小超过 `page_size` 时，必须安全拦截并返回 `RecordTooLarge` 错误，
///    不得导致存储引擎状态崩溃或内存越界。
/// 2. 页面容量极限贴边写入：写入刚好填满单页容量的记录（rec_size == page_size），
///    验证能正确分配、写入并回读。
/// 3. 跨页自动换页与数据连续性：在贴边记录后立即写入下一条记录，触发换页并在新页分配，
///    回读验证两条记录数据均完好。
/// 4. 小记录扩容为接近页面极限的大记录：触发跨页 RCU 追加与版本链正确指向。
#[test]
fn test_edge_oversized_records_and_boundary_handling() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let env = open_store("edge_oversized.db", config(1024, 4096, 16)?)?;
    let store = env.store;
    let session = store.new_session()?;

    let page_size = 4096; // 扇区大小 4096 字节

    // 1. 尝试写入超过单页大小的超大记录（例如 5000 字节 > 4096 字节）
    let oversized_val = vec![b'E'; 5000];
    let err = session.upsert(b"oversized_key", &oversized_val).await;
    assert!(err.is_err(), "超过单页尺寸的超大记录必须返回错误被安全拦截");

    // 验证存储引擎未受破坏，后续正常记录写入读出完全正常
    let normal_key = b"healthy_key";
    let normal_val = b"healthy_payload";
    session.upsert(normal_key, normal_val).await?;
    assert_eq!(session.read(normal_key).await?, Some(normal_val.to_vec()));

    // 2. 页面贴边极限记录：记录 = 16B 头 + 物理键（会话前缀 + KeyTag + 用户键）+ 值，
    //    按物理键长度构造恰好填满单页的记录
    let edge_key = b"edge_k01"; // 8 字节
    let edge_phys_len = session.session_string_key(edge_key).len();
    let edge_val_len = page_size - 16 - edge_phys_len;
    let edge_val = vec![b'Z'; edge_val_len];

    // 先换到新页以保证整页可用
    let pad = vec![b'P'; 3500];
    session.upsert(b"pad_to_turn", &pad).await?;

    let addr_edge = session.upsert(edge_key, &edge_val).await?;
    assert!(addr_edge > 0);
    let read_edge = session.read(edge_key).await?;
    assert_eq!(read_edge, Some(edge_val));

    // 3. 换页验证：在此之后继续写入记录，必然触发换页并写入新页
    let next_key = b"after_edge_key";
    let next_val = b"after_edge_value";
    let addr_next = session.upsert(next_key, next_val).await?;
    assert!(addr_next > addr_edge, "新记录必须分配在后续页面上");
    assert_eq!(session.read(next_key).await?, Some(next_val.to_vec()));

    // 4. 小记录扩容为大记录触发跨页 RCU 追加
    let dynamic_key = b"dynamic_expand_key";
    let small_val = b"small_val";
    let addr_dyn_small = session.upsert(dynamic_key, small_val).await?;

    let large_expanded_val = vec![b'W'; 3000];
    let addr_dyn_large = session.upsert(dynamic_key, &large_expanded_val).await?;
    assert!(addr_dyn_large > addr_dyn_small);

    let rec_dyn_large = store.hlog.read_record(addr_dyn_large).await?;
    // 优化3（严格对标 InternalUpsert.cs:CreateNewRecordUpsert elideSourceRecord +
    // Helpers.cs:CanElide）：前版本 addr_dyn_small 为链首唯一记录且 prev=0 <
    // BeginAddress，扩容盲插时旧记录被脱钩 elide，新记录前驱直接接管旧记录前驱（0），
    // 不再指向 addr_dyn_small
    assert_eq!(rec_dyn_large.prev_address()?, 0);
    assert_eq!(session.read(dynamic_key).await?, Some(large_expanded_val));

    info!("超大变长数据处理与跨页边界极端防御验证通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 无直接 C# 原型 —— 客户端会话高频创建与销毁（Rapid Churn）与 Epoch 槽位零泄漏
///
/// 验证目标：
/// 1. 多线程并发且极其高频地创建、使用并立刻销毁 `StoreSession`。
/// 2. 验证 `LightEpoch` 参与者槽位能否在 `Participant::drop` 中 100% 可靠归还与无缝复用。
/// 3. 验证在大并发高频 churn 场景下绝不出现槽位耗尽（`ExceededMaxThreads`）或悬空指针。
/// 4. 验证全部并发写入的数据在会话反复重置下依旧保持完整读写一致性。
#[test]
fn test_session_lifecycle_rapid_churn() -> Void {
  let env = open_store(
    "session_churn.db",
    config(2048, 64 * 1024, 16)?.with_max_sessions(32)?,
  )?;
  let store = env.store;

  let num_threads = 16;
  let iterations_per_thread = 50;
  let mut handles = Vec::new();

  for thread_id in 0..num_threads {
    let store_clone = Arc::clone(&store);
    let handle = thread::spawn(move || -> aok::Result<()> {
      let rt = Runtime::new()?;
      rt.block_on(async move {
        for i in 0..iterations_per_thread {
          // 每次操作均新建独立会话，并在块结束时立即销毁 Drop
          {
            let session = store_clone.new_session()?;
            let key = format!("churn:t{}:i{}", thread_id, pad(i, 4));
            let val = format!("payload_churn_{}_{}", thread_id, pad(i, 4));
            session.upsert(key.as_bytes(), val.as_bytes()).await?;

            let read_val = session.read(key.as_bytes()).await?;
            assert_eq!(read_val, Some(val.into_bytes()));
            // session 在此离开作用域自动调用 Drop，释放 LightEpoch 槽位
          }
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

  // 使用新会话校验全部写入数据均持久存在且完好
  let verify_rt = Runtime::new()?;
  verify_rt.block_on(async {
    let check_session = store.new_session()?;
    for thread_id in 0..num_threads {
      for i in 0..iterations_per_thread {
        let key = format!("churn:t{}:i{}", thread_id, pad(i, 4));
        let expected_val = format!("payload_churn_{}_{}", thread_id, pad(i, 4));
        let actual = check_session.read(key.as_bytes()).await?;
        assert_eq!(actual, Some(expected_val.into_bytes()));
      }
    }
    assert_eq!(
      store.entry_count(),
      num_threads * iterations_per_thread,
      "条目总数必须完全吻合"
    );
    aok::Result::<()>::Ok(())
  })?;

  info!("客户端会话高频创建与销毁与 Epoch 槽位零泄漏验证通过");
  OK
}

/// 验证纯内存同步批量读取（Session 与 BatchSession）
#[test]
fn test_try_read_batch_in_memory() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let env = open_store("try_read_batch_in_memory.db", config(2048, 64 * 1024, 16)?)?;
    let session = env.store.new_session()?;

    const COUNT: usize = 25;
    let mut keys = Vec::with_capacity(COUNT);
    for i in 0..COUNT {
      let k = format!("batch_inmem:{}", pad(i, 4));
      let v = format!("batch_val:{}", pad(i, 4));
      session.upsert(k.as_bytes(), v.as_bytes()).await?;
      keys.push(k.into_bytes());
    }

    // 1. 测试 StoreSession::try_read_batch_in_memory
    let mut collected = vec![None; COUNT];
    session.try_read_batch_in_memory(&keys, |idx, val| {
      collected[idx] = val.map(|v| v.to_vec());
    })?;

    for (i, item) in collected.iter().enumerate() {
      let expected = format!("batch_val:{}", pad(i, 4)).into_bytes();
      assert_eq!(item.as_deref(), Some(expected.as_slice()));
    }

    // 2. 测试 BatchStoreSession::try_read_batch_in_memory
    let batch = session.enter_batch();
    let mut batch_collected = vec![None; COUNT];
    batch.try_read_batch_in_memory(&keys, |idx, val| {
      batch_collected[idx] = val.map(|v| v.to_vec());
    })?;

    for (i, item) in batch_collected.iter().enumerate() {
      let expected = format!("batch_val:{}", pad(i, 4)).into_bytes();
      assert_eq!(item.as_deref(), Some(expected.as_slice()));
    }

    // 3. 测试空切片与未命中情况
    let empty_keys: Vec<Vec<u8>> = Vec::new();
    batch.try_read_batch_in_memory(&empty_keys, |_idx, _val| {
      panic!("空键切片不应回调");
    })?;

    let missing_keys = vec![b"missing_key_0".to_vec(), b"missing_key_1".to_vec()];
    let mut missing_results = vec![Some(vec![]); 2];
    batch.try_read_batch_in_memory(&missing_keys, |idx, val| {
      missing_results[idx] = val.map(|v| v.to_vec());
    })?;
    assert_eq!(missing_results, vec![None, None]);

    info!("纯内存同步批量读取（Session 与 BatchSession）验证通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}
