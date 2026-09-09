//! 空间复活与读缓存测试：revivification 槽位复用、copy_reads_to_tail 晋升、ReadCache 挂载与脱钩。

use std::sync::atomic::Ordering;

use aok::{OK, Void};
use compio::runtime::Runtime;
use log::info;
use whlog::SECTOR_ALIGNMENT;

use crate::support::{config, open_store};

/// 对标 Garnet Tsavorite TryCopyToTail / CopyReadsToTail ——
/// 落盘冷数据读取后自动晋升至 Tail 活跃区
///
/// 验证开启 copy_reads_to_tail 后，首次读取从磁盘回填并自动晋升至 Tail，
/// 后续对该键的再次读取直接命中内存直读（`try_read_in_memory` 成功），避免二次磁盘 I/O。
#[test]
fn test_copy_reads_to_tail() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let page_size = SECTOR_ALIGNMENT;
    let env = open_store("copy_to_tail.db", config(1024, page_size, 16)?)?;
    let store = env.store;
    let session = store.new_session()?;

    let cold_k = b"cold_key_for_copy_to_tail";
    let cold_v = b"cold_payload_initial_disk_data";

    let addr_init = session.upsert(cold_k, cold_v).await?;
    assert!(addr_init < page_size as u64);

    // 填充跨越第 0 页
    let pad = vec![b'P'; 3000];
    session.upsert(b"pad_k1", &pad).await?;
    let addr_pad = session.upsert(b"pad_k2", &pad).await?;
    assert!(addr_pad >= page_size as u64);

    // 刷盘并驱逐第 0 页至磁盘
    store.flush_all().await?;
    store.shift_read_only_address(page_size as u64);
    store.shift_head_address(page_size as u64);

    assert!(store.hlog.is_on_disk(addr_init));
    assert!(!store.hlog.is_in_memory(addr_init));

    // 未开启 copy_reads_to_tail 时：读取成功但不会拷贝回 Tail
    session.set_copy_reads_to_tail(false);
    let val1 = session.read(cold_k).await?;
    assert_eq!(val1, Some(cold_v.to_vec()));
    // 再次内存探测依然无法命中内存（必须回退到磁盘）
    assert!(session.try_read_in_memory(cold_k, |_| ())?.is_none());

    // 开启 copy_reads_to_tail 时：首次从磁盘读完后自动晋升回 Tail
    session.set_copy_reads_to_tail(true);
    let val2 = session.read(cold_k).await?;
    assert_eq!(val2, Some(cold_v.to_vec()));

    // 关键断言：此时该记录已被原子晋升到 Tail 内存中！
    // 再次读取时，纯同步内存探针直接精准命中，返回 Some(Some(val))，完全绕过磁盘！
    let mem_hit = session.try_read_in_memory(cold_k, |v| v.to_vec())?;
    assert_eq!(
      mem_hit,
      Some(Some(cold_v.to_vec())),
      "晋升后必须 100% 命中 DRAM 同步内存直读"
    );

    info!("对照 C# Garnet TryCopyToTail 冷读自动晋升至 Tail 活跃区验证通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 Garnet Tsavorite 空间复活机制 —— In-Chain Revivification 与 FreeRecordPool 槽位回收复用
#[test]
fn test_revivification() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let page_size = SECTOR_ALIGNMENT;
    let env = open_store(
      "reviv.db",
      config(1024, page_size, 16)?.with_revivification(true),
    )?;
    let store = env.store;
    let session = store.new_session()?;
    session.set_record_elision(true);

    let k1 = b"reviv_key1";
    let v1 = b"original_value_01";
    let addr1 = session.upsert(k1, v1).await?;

    // 1. 测试空间脱钩与 FreeRecordPool 槽位回收：
    // 删除 k1，由于 k1 在内存可变区且无前驱版本，触发 HandleRecordElision 直接从索引脱钩并放入 reviv_pool
    let deleted = session.delete(k1).await?;
    assert!(deleted, "单记录删除成功");
    assert_eq!(session.read(k1).await?, None);

    // 验证复活池成功回收了槽位
    assert!(
      store.reviv_pool.put_count.load(Ordering::Relaxed) >= 1,
      "删除的槽位必须成功归还入 FreeRecordPool"
    );

    // 2. 测试分配拦截与槽位复用 (BlockAllocate.cs TryTakeFreeRecord):
    // 写入一个新 key2（大小相仿），优先从 FreeRecordPool 借出刚刚回收的 addr1 槽位！
    let curr_tail = store.hlog.tail_address();
    let k2 = b"reviv_key2";
    let v2 = b"reused_value__02";
    let addr2 = session.upsert(k2, v2).await?;

    assert_eq!(
      addr2, addr1,
      "新记录必须精准复用被删除槽位 addr1，零多余内存开销"
    );
    assert_eq!(
      store.hlog.tail_address(),
      curr_tail,
      "从复活池分配记录时 TailAddress 绝对不推进"
    );

    // 验证读取新 key2 内容准确无误
    assert_eq!(session.read(k2).await?, Some(v2.to_vec()));

    // 3. 测试链内原地复活 (In-Chain Revivification):
    // 强制写入一条带有墓碑的前驱记录（物理层直调统一携带会话前缀物理键）
    let k3 = b"reviv_key3";
    let v3_old = b"chain_val_001";
    let phys_k3 = session.session_string_key(k3);
    let addr3 = session.append_record(&phys_k3, v3_old, 0, true).await?;
    store.index.insert(&phys_k3, addr3)?;

    let tail_before_reviv = store.hlog.tail_address();
    let v3_new = b"chain_val_002";
    let addr3_reviv = session.upsert(k3, v3_new).await?;

    assert_eq!(
      addr3_reviv, addr3,
      "墓碑记录在长度匹配时必须在链内原地复活，地址保持不变"
    );
    assert_eq!(
      store.hlog.tail_address(),
      tail_before_reviv,
      "链内原地复活不推进 TailAddress"
    );
    assert_eq!(session.read(k3).await?, Some(v3_new.to_vec()));

    info!("对照 C# Garnet 空间复活与 FreeRecordPool 回收复用验证通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 Garnet Tsavorite ReadCache 独立只读非脏页内存日志 —— 冷读回填晋升与原子脱钩
///
/// 验证点：
/// 1. 启用 `with_read_cache(true)`；
/// 2. 写入冷数据落盘并驱逐出 DRAM（HLog is_on_disk）；
/// 3. 从磁盘读取冷数据后，数据自动挂载入纯内存 ReadCache（零持久化写放大，HLog 尾部不推进）；
/// 4. 后续读请求 100% 命中 DRAM 同步内存直读快路径；
/// 5. 后续对该 Key 写入新值（Upsert），单次 CAS 原子脱钩旧 ReadCache 链，新记录写入主日志 Tail；
/// 6. 删除 Key 时，原子脱钩 ReadCache 链并追加墓碑记录。
#[test]
fn test_read_cache_promotion_and_detach() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let page_size = SECTOR_ALIGNMENT;
    let num_pages = 8;
    let env = open_store(
      "read_cache.db",
      config(64, page_size, num_pages)?.with_read_cache(true),
    )?;
    let store = env.store;
    let session = store.new_session()?;

    assert!(store.read_cache.is_enabled);

    let key = b"read_cache_key_01";
    let val = b"read_cache_val_01";

    let init_addr = session.upsert(key, val).await?;
    assert!(init_addr < page_size as u64);

    // 填充数据使得 tail 自然跨越第 0 页进入第 1 页
    let pad = vec![b'P'; 3000];
    session.upsert(b"pad_k1", &pad).await?;
    let addr_pad = session.upsert(b"pad_k2", &pad).await?;
    assert!(addr_pad >= page_size as u64);

    // 刷盘并驱逐第 0 页到磁盘
    store.flush_all().await?;
    store.shift_read_only_address(page_size as u64);
    store.shift_head_address(page_size as u64);

    assert!(store.hlog.is_on_disk(init_addr));
    assert!(!store.hlog.is_in_memory(init_addr));

    let tail_before_read = store.hlog.tail_address();

    // 首次冷读：从磁盘读取
    let res = session.read(key).await?;
    assert_eq!(res, Some(val.to_vec()));

    // 验证 HLog Tail 并没有因为冷读而追加（区别于 copy_reads_to_tail 的写放大）
    assert_eq!(
      store.hlog.tail_address(),
      tail_before_read,
      "ReadCache 冷读回填纯在 DRAM 分配器中完成，HLog 尾部零写放大"
    );

    // 验证后续内存探针 100% 命中 ReadCache！
    let mem_hit = session.try_read_in_memory(key, |v| v.to_vec())?;
    assert_eq!(
      mem_hit,
      Some(Some(val.to_vec())),
      "冷读完成后必须 100% 命中 ReadCache 纯内存直读"
    );

    // 测试写操作（Upsert）：原子脱钩 ReadCache
    let new_val = b"read_cache_val_updated";
    let new_addr = session.upsert(key, new_val).await?;
    assert!(
      new_addr >= store.hlog.head_address(),
      "新记录必须写入 HLog 内存活跃区"
    );

    // 读取验证新值
    let updated_hit = session.try_read_in_memory(key, |v| v.to_vec())?;
    assert_eq!(updated_hit, Some(Some(new_val.to_vec())));

    // 测试删除操作（Delete）：delete_raw 直调物理键，须携带会话前缀
    let phys_key = session.session_string_key(key);
    let deleted = session.delete_raw(&phys_key).await?;
    assert!(deleted, "删除应返回 true");
    let after_del = session.try_read_in_memory(key, |v| v.to_vec())?;
    assert_eq!(after_del, Some(None), "删除后内存探针确认不存在或为墓碑");

    info!("对照 C# Garnet ReadCache 独立只读非脏页内存日志验证通过");
    OK
  })
}
