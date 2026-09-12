//! 防御性语义测试：版本链逆向回溯、截断地址防御、Tag 碰撞探测回溯与纪元保护无锁读。

use std::{
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
  thread,
};

use aok::{OK, Void};
use compio::runtime::Runtime;
use log::info;
use whasher::{fast_hash, new_hash_map};
use whlog::SECTOR_ALIGNMENT;
use windex::HashBucketEntry;

use crate::support::{config, open_store, pad};

/// 对标 Garnet Tsavorite RecordInfo.PreviousAddress 机制 —— 通过 prev_address 逆向回溯全量版本链
///
/// 单个 Key 经历创建 -> 变长更新 -> 只读区 CopyUpdate -> 墓碑删除 -> 复活更新 5 个完整生命周期，
/// 验证从最新版本逻辑地址出发，沿着 prev_address 指针链逆向扫描，
/// 能够完整无遗漏地追溯所有历史版本。
#[test]
fn test_version_chain_backward_scan() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let env = open_store("version_chain.db", config(1024, 64 * 1024, 16)?)?;
    let store = env.store;
    let session = store.new_session()?;

    let key = b"entity:order:9999";

    // 1. 版本 1：初始创建
    let v1 = b"v1_created";
    let addr1 = session.upsert(key, v1).await?;
    assert!(addr1 > 0);

    // 2. 版本 2：变长更新（触发 RCU 追加）
    let v2 = b"v2_updated_with_longer_payload";
    let addr2 = session.upsert(key, v2).await?;
    assert!(addr2 > addr1);

    // 3. 版本 3：推进只读区后的 CopyUpdate
    store.shift_read_only_address(store.tail_address());
    let v3 = b"v3_copy_update_after_readonly";
    let addr3 = session.upsert(key, v3).await?;
    assert!(addr3 > addr2);

    // 4. 版本 4：推进只读区后墓碑删除（只读区不可原地修改，触发追加 Tombstone 记录）
    store.shift_read_only_address(store.tail_address());
    let deleted = session.delete(key).await?;
    assert!(deleted);
    // 索引直查须携带会话前缀物理键（session_string_key）
    let str_k = session.session_string_key(key);
    let rec_tombstone_addr = store.index.lookup(&str_k)[0];
    assert!(rec_tombstone_addr > addr3);

    // 5. 版本 5：复活写入新值
    let v5 = b"v5_resurrected_order";
    let addr5 = session.upsert(key, v5).await?;
    assert!(addr5 > rec_tombstone_addr);

    // 回读验证当前最新值为 v5
    let current = session.read(key).await?;
    assert_eq!(current, Some(v5.to_vec()));

    // === 逆向追溯版本链（Backward Scan）===
    // 物理记录键为会话前缀物理键，回读比对须携带前缀
    let str_k = session.session_string_key(key);
    // 检查第 5 代 (v5)
    let rec5 = store.hlog.read_record(addr5).await?;
    assert_eq!(rec5.key()?, str_k.as_slice());
    assert_eq!(rec5.value()?, v5);
    assert!(!rec5.is_tombstone()?);
    assert_eq!(rec5.prev_address()?, rec_tombstone_addr);

    // 检查第 4 代 (墓碑记录)
    let rec4 = store.hlog.read_record(rec_tombstone_addr).await?;
    assert_eq!(rec4.key()?, str_k.as_slice());
    assert!(rec4.is_tombstone()?);
    assert_eq!(rec4.prev_address()?, addr3);

    // 检查第 3 代 (v3)
    let rec3 = store.hlog.read_record(addr3).await?;
    assert_eq!(rec3.key()?, str_k.as_slice());
    assert_eq!(rec3.value()?, v3);
    assert!(!rec3.is_tombstone()?);
    // v3 写入前已推进 read_only 至链首之上：探针不运行、无匹配即无 elide
    // （严格对标 C# elideSourceRecord 需 HasMainLogSrc——仅可变区命中的记录可 elide），
    // 盲插前驱保持链接 addr2
    assert_eq!(rec3.prev_address()?, addr2);

    // 检查第 2 代 (v2)
    let rec2 = store.hlog.read_record(addr2).await?;
    assert_eq!(rec2.key()?, str_k.as_slice());
    assert_eq!(rec2.value()?, v2);
    assert!(!rec2.is_tombstone()?);
    // 优化3：v1 为链首唯一记录且 prev=0 < BeginAddress，v2 盲插时 v1 被脱钩 elide，
    // v2 前驱接管 v1 的前驱（0）
    assert_eq!(rec2.prev_address()?, 0);

    // 检查第 1 代 (v1，初始记录，prev_address 必须为 0)
    let rec1 = store.hlog.read_record(addr1).await?;
    assert_eq!(rec1.key()?, str_k.as_slice());
    assert_eq!(rec1.value()?, v1);
    assert!(!rec1.is_tombstone()?);
    assert_eq!(rec1.prev_address()?, 0, "根记录前驱地址必须为 0");

    info!("通过 prev_address 逆向回溯全量版本链验证通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 Garnet Tsavorite LogAccessor.ShiftBeginAddress 机制 ——
/// 已截断地址安全防御与版本链逆向回溯边界截断
///
/// 验证目标：
/// 1. 写入第 0 页数据并完成刷盘落盘。
/// 2. 推进 ReadOnly 与 Head 地址将第 0 页逐出内存。
/// 3. 推进 BeginAddress 截断第 0 页（物理与逻辑均失效）。
/// 4. 验证对已截断地址记录执行 `read()`：安全防御并返回 `Ok(None)`，同时自愈删除索引槽位。
/// 5. 验证对已截断记录执行 `delete()`：安全防御并返回 `Ok(false)`。
/// 6. 验证已截断记录重新 Upsert：成功分配全新根记录并恢复读写。
/// 7. 验证版本链逆向回溯（Backward Scan）：当追溯至小于 BeginAddress 的前驱地址时安全终止。
#[test]
fn test_edge_truncated_address_defense_and_backward_scan_termination() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let page_size = SECTOR_ALIGNMENT; // 4096
    let env = open_store("edge_truncation.db", config(1024, page_size, 16)?)?;
    let store = env.store;
    let session = store.new_session()?;

    // 1. 在第 0 页写入待截断的记录
    let trunc_k1 = b"trunc:item:001";
    let trunc_v1 = b"payload_to_be_truncated_001";
    let addr_trunc1 = session.upsert(trunc_k1, trunc_v1).await?;
    assert!(addr_trunc1 < page_size as u64);

    let trunc_k2 = b"trunc:item:002";
    let trunc_v2 = b"payload_to_be_truncated_002";
    let addr_trunc2 = session.upsert(trunc_k2, trunc_v2).await?;
    assert!(addr_trunc2 < page_size as u64);

    // 写入足够大的填充使分配指针跨越第 0 页边界进入第 1 页
    let pad = vec![b'P'; 3950];
    let addr_pad = session.upsert(b"padding_turn_page", &pad).await?;
    assert!(addr_pad >= page_size as u64);

    // 在第 1 页写入活跃存活记录
    let live_k = b"live:item:100";
    let live_v = b"live_payload_page_one";
    let addr_live = session.upsert(live_k, live_v).await?;
    assert!(addr_live >= page_size as u64);

    // 2. 将数据刷盘并推进 Head 与 BeginAddress 彻底截断第 0 页
    store.flush_all().await?;
    store.shift_read_only_address(page_size as u64);
    store.shift_head_address(page_size as u64);
    store.shift_begin_address(page_size as u64).await?;
    assert_eq!(store.begin_address(), page_size as u64);

    // 3. 安全防御回读已截断记录：必须返回 None，绝不抛出 Panic 或未捕获 I/O 错误
    let read_trunc1 = session.read(trunc_k1).await?;
    assert_eq!(read_trunc1, None, "已截断的记录回读必须安全返回 None");

    // 4. 安全防御删除已截断记录：必须返回 false
    let del_trunc2 = session.delete(trunc_k2).await?;
    assert!(!del_trunc2, "删除已被截断的记录必须返回 false");

    // 5. 存活记录读取不受任何影响
    let read_live = session.read(live_k).await?;
    assert_eq!(read_live, Some(live_v.to_vec()));

    // 6. 重新 Upsert 已截断的 Key：生成全新的根记录并正常读写
    let trunc_k1_revived = b"trunc_k1_resurrected_in_active_page";
    let addr_trunc1_new = session.upsert(trunc_k1, trunc_k1_revived).await?;
    assert!(addr_trunc1_new >= page_size as u64);
    assert_eq!(
      session.read(trunc_k1).await?,
      Some(trunc_k1_revived.to_vec())
    );

    // 7. 版本链跨越截断分界线时的逆向扫描安全终止
    // 在当前活跃区第 1 页写入版本 1：
    let chain_key = b"chain:item:split";
    let v1 = b"chain_version_one";
    let addr_chain1 = session.upsert(chain_key, v1).await?;
    assert!(addr_chain1 >= page_size as u64);

    // 写入大填充推进到第 2 页
    let pad2 = vec![b'Q'; 3950];
    let addr_pad2 = session.upsert(b"padding_turn_page_2", &pad2).await?;
    assert!(addr_pad2 >= 2 * page_size as u64);

    // 在第 2 页写入版本 2（prev 指向第 1 页的 addr_chain1）
    let v2 = b"chain_version_two_expanded";
    let addr_chain2 = session.upsert(chain_key, v2).await?;
    assert!(addr_chain2 >= 2 * page_size as u64);

    // 验证 addr_chain2 位于比 addr_chain1 更靠后的页面上
    let chain1_page = addr_chain1 / page_size as u64;
    let chain2_page = addr_chain2 / page_size as u64;
    assert!(
      chain2_page > chain1_page,
      "pad2 必须将 chain2 推进到新页面: c1_page={chain1_page}, c2_page={chain2_page}"
    );

    // 刷盘并推进 BeginAddress 截断 addr_chain2 之前的所有历史页面
    let cut_addr = chain2_page * page_size as u64;
    store.flush_all().await?;
    store.shift_read_only_address(cut_addr);
    store.shift_head_address(cut_addr);
    store.shift_begin_address(cut_addr).await?;
    assert_eq!(store.begin_address(), cut_addr);

    // 回读 v2 依然完好
    assert_eq!(session.read(chain_key).await?, Some(v2.to_vec()));

    // 从 v2 逆向追溯前驱：
    let rec_v2 = store.hlog.read_record(addr_chain2).await?;
    assert_eq!(rec_v2.value()?, v2);
    // 优化3（严格对标 InternalUpsert.cs:CreateNewRecordUpsert elideSourceRecord +
    // Helpers.cs:CanElide）：v1 为链首唯一记录且 prev=0 < BeginAddress，v2 盲插时
    // v1 被脱钩 elide，v2 前驱直接接管 v1 的前驱（0），不再指向 addr_chain1
    let prev = rec_v2.prev_address()?;
    assert_eq!(prev, 0);

    // 验证截断防御边界逻辑：v2 前驱 0 恒低于 begin_address，追溯安全终止；
    // 读取已被截断物理丢弃的 v1 旧版本必须返回越界错误
    assert!(prev < store.begin_address(), "前驱版本位于历史无效区");
    let read_past_trunc = store.hlog.read_record(addr_chain1).await;
    assert!(read_past_trunc.is_err(), "读取截断地址必须返回越界错误");

    info!("已截断地址安全防御与版本链逆向回溯边界截断验证通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 Garnet TsavoriteBase.cs FindTag 与 FindRecord.cs TraceBackForKeyMatch ——
/// 首项快速探针、墓碑探测与 Tag 碰撞回退
///
/// 参照源码：
/// - `TsavoriteBase.cs:L226-265` (`FindTag`)
/// - `FindRecord.cs:L160-181` (`TraceBackForKeyMatch`)
/// - `InternalRead.cs:L73-125`
///
/// 验证目标：
/// 1. 首项快速探针（FindTag）：常态下单槽位加载直接命中返回，零堆分配与零循环；
/// 2. 首项命中墓碑直接判定不存在（返回 Some(None)）；
/// 3. Tag 碰撞与反向链表回溯（TraceBackForKeyMatch）：
///    - 探针命中首项但 key 不匹配时，沿 prev_address 成功回溯命中前驱版本；
///    - 多槽位碰撞时，回退到完整候选路径成功命中目标键；
///    - 不存在的键在遇到同 Tag 碰撞后准确判定 NotFound；
/// 4. 彻底未写入的 Tag 极速判定不存在（Some(None)）；
/// 5. 数据落盘驱逐后正确返回 None（RECORD_ON_DISK）并顺利通过异步回退加载。
#[test]
fn test_find_tag_probe_traceback_and_collision() -> Void {
  let rt = Runtime::new()?;

  rt.block_on(async {
    let env = open_store("find_tag_traceback.db", config(64, 16 * 1024, 16)?)?;
    let store = env.store;
    let session = store.new_session()?;

    // 1. 首项快速探针黄金命中路径（99.9% 场景）
    let key_gold = b"gold_key_fast_probe_001";
    let val_gold = b"gold_val_fast_payload";
    session.upsert(key_gold, val_gold).await?;

    let gold_res = session.try_read_in_memory(key_gold, |v| v.to_vec())?;
    assert_eq!(
      gold_res,
      Some(Some(val_gold.to_vec())),
      "首项快速探针必须纯同步零拷贝命中返回值"
    );

    // 2. 首项命中墓碑直接判定不存在 (NOTFOUND)
    let tomb_key = b"tombstone_fast_key_002";
    let tomb_val = b"tombstone_initial_val";
    session.upsert(tomb_key, tomb_val).await?;
    assert!(session.delete(tomb_key).await?);

    let tomb_res = session.try_read_in_memory(tomb_key, |v| v.len())?;
    assert_eq!(
      tomb_res,
      Some(None),
      "首项探针检测到墓碑时必须直接返回 Some(None)"
    );

    // 3. 构造命中同一 (哈希桶, Tag) 组合的物理键三元组（哈希输入为会话前缀 + KeyTag + 用户键）：
    //    key_a / key_b 用于写路径碰撞，key_c 完全不写入，验证 Tag 碰撞与回溯
    //    (TraceBackForKeyMatch)
    let mask = 64 - 1; // 64 桶
    let mut first_of_combo: whasher::HashMap<(usize, u16), Vec<u8>> = new_hash_map();
    let mut pair_of_combo: whasher::HashMap<(usize, u16), (Vec<u8>, Vec<u8>)> = new_hash_map();
    let mut triple: Option<[Vec<u8>; 3]> = None;

    for i in 0..200_000u32 {
      let candidate = format!("collision_key_{i}").into_bytes();
      let h = fast_hash(&session.session_string_key(&candidate));
      let combo = ((h as usize) & mask, HashBucketEntry::tag_from_hash(h));

      // 第三把同组合键出现即构成三元组
      if let Some((first, second)) = pair_of_combo.get(&combo).cloned() {
        triple = Some([first, second, candidate]);
        break;
      }
      match first_of_combo.get(&combo) {
        // 同组合第二把键出现时登记为碰撞对
        Some(first) => {
          pair_of_combo.insert(combo, (first.clone(), candidate));
        }
        None => {
          first_of_combo.insert(combo, candidate);
        }
      }
    }

    let Some([key_a, key_b, key_c_non_exist]) = triple else {
      return aok::err!("必须在候选空间内构造出同 (桶, Tag) 三元组碰撞键");
    };

    let val_a = b"val_payload_a_for_collision";
    let val_b = b"val_payload_b_for_collision";

    // 3.1 测试 TraceBackForKeyMatch：
    // 先写入 key_a，随后在 HLog 追加 key_b 但将其 prev_address 指向 key_a，
    // 并将索引槽位更新为 key_b 的地址（物理层直调统一携带会话前缀物理键）。
    // 当查询 key_a 时，find_tag 命中 key_b 的地址，key_b 不匹配 key_a，
    // 系统沿着 prev_address 成功回溯命中 key_a！
    let addr_a = session.upsert(&key_a, val_a).await?;
    let phys_a = session.session_string_key(&key_a);
    let phys_b = session.session_string_key(&key_b);
    let addr_b = store.hlog.append(&phys_b, val_b, addr_a, false)?;
    assert!(store.index.update_address(&phys_a, addr_a, addr_b));

    let traceback_res = session.try_read_in_memory(&key_a, |v| v.to_vec())?;
    assert_eq!(
      traceback_res,
      Some(Some(val_a.to_vec())),
      "TraceBackForKeyMatch 必须沿 prev_address 反向链表成功回溯匹配目标键"
    );

    let direct_b_res = session.try_read_in_memory(&key_b, |v| v.to_vec())?;
    assert_eq!(
      direct_b_res,
      Some(Some(val_b.to_vec())),
      "首项记录与 key_b 直接匹配时立即命中"
    );

    // 3.2 测试同 Tag 第三键完全未写入的碰撞回退：
    // 索引槽位与回溯链上只有 key_a / key_b，第三把同 (桶, Tag) 碰撞键从未写入，
    // 反向链与候选槽位均无此键时，必须判定未找到并返回 Some(None)
    let miss_collision_res = session.try_read_in_memory(&key_c_non_exist, |v| v.len())?;
    assert_eq!(
      miss_collision_res,
      Some(None),
      "Tag 碰撞但反向链和候选槽位均无此键时，必须判定未找到并返回 Some(None)"
    );

    // 4. 完全不存在的 Tag（连哈希桶都没有匹配 Tag）
    let completely_missing = b"random_unregistered_key_9999";
    let no_tag_res = session.try_read_in_memory(completely_missing, |v| v.len())?;
    assert_eq!(
      no_tag_res,
      Some(None),
      "哈希表中无对应 Tag 时极速返回 Some(None)"
    );

    // 5. 落盘驱逐后返回 None (RECORD_ON_DISK) 并通过异步回退 read_with 成功加载
    let disk_k = b"disk_fast_probe_evict_key";
    let disk_v = b"disk_fast_probe_evict_val";
    session.upsert(disk_k, disk_v).await?;

    store.flush_all().await?;
    store.flush_and_evict_all().await?;
    let tail = store.hlog.tail_address();
    store.shift_read_only_address(tail);
    store.shift_head_address(tail);

    let disk_probe_res = session.try_read_in_memory(disk_k, |v| v.to_vec())?;
    assert_eq!(
      disk_probe_res, None,
      "页面已被驱逐至磁盘时，try_read_in_memory 必须返回 None"
    );

    let async_loaded = session.read_with(disk_k, |v| v.to_vec()).await?;
    assert_eq!(
      async_loaded,
      Some(disk_v.to_vec()),
      "异步回退路径必须成功从磁盘读取已驱逐记录"
    );

    info!("FindTag 与 TraceBackForKeyMatch 首项快速探针、回溯与碰撞验证通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 Garnet Tsavorite InternalRead（Immutable Region 纯指针直读）与
/// AllocatorBase.GetPhysicalAddress —— LightEpoch 保护下不可变只读区多线程并发无锁直读
///
/// 验证目标：
/// 1. 批量写入多条记录，使得部分记录进入不可变只读区（ReadOnlyAddress 推进）。
/// 2. 验证 Session 的 `try_read_in_memory` 能够稳定命中并读取不可变区数据。
/// 3. 多线程并发读取：创建多个 Session（各持有独立 Epoch 参与者与 Guard），
///    8 个线程高并发调用 `try_read_in_memory` 与 `read_with` 回读历史数据。
/// 4. 在多线程并发直读期间，主线程继续追加写入新记录并触发只读边界滑动，
///    验证并发读写无数据竞争、无死锁、读出的历史不可变数据 100% 正确。
#[test]
fn test_epoch_protected_multithreaded_lock_free_reads() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    // 页面大小 4096，总槽位 64，mutable_fraction=0.5
    let env = open_store("lock_free_store.db", config(4096, 4096, 64)?)?;
    let store = env.store;
    let session = store.new_session()?;

    let num_keys = 200;
    let mut keys = Vec::with_capacity(num_keys);

    // 1. 批量写入数据跨越多个页面
    for i in 0..num_keys {
      let k = format!("lock_free_key:{}", pad(i, 4)).into_bytes();
      let v = format!(
        "lock_free_val_payload_{}_{}",
        pad(i, 4),
        "x".repeat(30).as_str()
      )
      .into_bytes();
      session.upsert(&k, &v).await?;
      keys.push((k, v));
    }

    let ro_addr = store.hlog.read_only_address();
    assert!(ro_addr > 0, "跨页写入后 ReadOnlyAddress 应已自动推进");

    // 2. 单线程验证 try_read_in_memory 命中
    for (k, v) in &keys {
      let read_res = session.try_read_in_memory(k, |val| val.to_vec())?;
      assert_eq!(read_res, Some(Some(v.clone())));
    }

    // 3. 多线程并发无锁直读
    let thread_count = 8;
    let mut handles = Vec::with_capacity(thread_count);
    let keys_arc = Arc::new(keys.clone());
    let stop_signal = Arc::new(AtomicBool::new(false));

    for thread_id in 0..thread_count {
      let store_clone = Arc::clone(&store);
      let keys_ref = Arc::clone(&keys_arc);
      let stop = Arc::clone(&stop_signal);

      let handle = thread::spawn(move || {
        let thread_session = store_clone.new_session().expect("创建线程 session 成功");
        let mut loop_count = 0;
        while !stop.load(Ordering::Acquire) && loop_count < 100 {
          for (k, v) in keys_ref.iter() {
            let res = thread_session
              .try_read_in_memory(k, |val| val.to_vec())
              .expect("try_read_in_memory 成功");
            assert_eq!(
              res,
              Some(Some(v.clone())),
              "线程 {thread_id} 读取 key={:?} 必须与写入值匹配",
              String::from_utf8_lossy(k)
            );
          }
          loop_count += 1;
        }
      });
      handles.push(handle);
    }

    // 4. 在读取线程并发读取的同时，主线程继续追加新数据并推进只读边界
    for i in num_keys..(num_keys + 50) {
      let k = format!("dynamic_key:{}", pad(i, 4)).into_bytes();
      let v = format!("dynamic_val_{}", pad(i, 4)).into_bytes();
      session.upsert(&k, &v).await?;
    }

    stop_signal.store(true, Ordering::Release);

    for handle in handles {
      handle.join().expect("读取线程顺利退出无 panic");
    }

    // 5. 校验全部初始键在并发写之后依然完整可读
    for (k, v) in &keys {
      let res = session.try_read_in_memory(k, |val| val.to_vec())?;
      assert_eq!(res, Some(Some(v.clone())));
    }

    info!("LightEpoch 保护下的不可变只读区多线程并发无锁直读验证通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}
