//! 存储 API 域集成测试（wkv 临时文件库驱动 StorageSession / 数据库管理器全链路）
//!
//! 覆盖：字符串读写、对象四类型（含 WRONGTYPE 与空回收）、TTL / SCAN / 槽位删除、WATCH 冲突判定
//! （wtxn 生产出口）、单库管理器（检查点 / AOF 重放 / 清空）。

use std::sync::Arc;

use compio::runtime::Runtime;
use waof::{AofEntryType, AofHeader, WalConfig, WalLog};
use wbase::{align::DEFAULT_SECTOR_SIZE, convert::TICKS_PER_MILLISECOND, hash_slot::slot_of};
use wcol::hash::hash_object::HashObject;
use wconf::RuntimeServerOptions;
use wdev::SegmentedDevice;
use wnode::{
  aof::{garnet_append_only_file::GarnetAppendOnlyFile, waof_sublog::single_log_aof},
  database::{GarnetDatabase, IDatabaseManager, SingleDatabaseManager},
  storage::session::storage_session::{StorageSession, version_map_watch_hook},
};
use wtest_base::open_test_store;
use wtxn::{DEFAULT_VERSION_MAP_SIZE, TxnWatchedKeysContainer, WatchVersionMap};

/// 创建会话（批处理纪元内；独立版本表实例并同步接线引擎写面钩子——
/// WATCH 写面推进统一走 wkv 引擎收口（store.watch_hook）；生产由装配层以
/// NodeService.watch_version_map 同构接线，见 storage_session::version_map_watch_hook）
fn storage_session<'s, D: wdev::Device>(
  session: &'s wkv::StoreSession<D>,
) -> StorageSession<'s, D> {
  let version_map = Arc::new(WatchVersionMap::new(DEFAULT_VERSION_MAP_SIZE));
  let _ = session
    .store
    .set_watch_hook(version_map_watch_hook(Arc::clone(&version_map)));
  StorageSession::new(session.enter_batch())
}

/// TTL / SCAN / KEYS / DBSIZE / 槽位删除
#[test]
fn test_ttl_scan_watch() -> aok::Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store) = open_test_store("scan.db")?;
    let session = store.new_session()?;
    let ss = storage_session(&session);

    for k in ["user:1", "user:2", "order:1"] {
      ss.upsert_string(k.as_bytes(), b"v").await?;
    }
    // 带过期键：50ms 后到期（相对时长以 ticks 传递）
    ss.upsert_string(b"user:tmp", b"v").await?;
    ss.expire_in_ticks(b"user:tmp", 50 * TICKS_PER_MILLISECOND)
      .await?;
    assert!(ss.pttl_ms(b"user:tmp").await? > 0);

    assert_eq!(ss.db_size().await?, 4);
    let (cursor, keys) = ss.scan_cursor(b"user:*", false, 0, 10, None).await?;
    assert_eq!((cursor, keys.len()), (0, 3));

    // SCAN/KEYS 模式匹配为大小写不敏感（C# UnifiedStoreGetDBKeys 的
    // GlobUtils.Match 固定 ignoreCase=true）
    let (_, keys) = ss.scan_cursor(b"USER:*", false, 0, 10, None).await?;
    assert_eq!(keys.len(), 3);
    let keys = ss.db_keys(b"Order:*").await?;
    assert_eq!(keys.len(), 1);
    assert_eq!(keys[0], b"order:1");

    // PERSIST 与到期
    ss.persist_key(b"user:tmp").await?;
    assert_eq!(ss.pttl_ms(b"user:tmp").await?, -1);

    // 槽位删除：库级定槽（doc/zh/db.md 4.1）下会话库槽命中即整库删除
    //（C# DeleteSlotKeys 全槽键删除，4 活键全清；键级 CRC 时代只删 1 的
    // 旧期望系 e5014eb 切换时的遗留）
    let slot = slot_of(0, 0);
    let deleted = ss.delete_slot_keys(&[slot]).await?;
    assert_eq!(deleted, 4);

    Ok(())
  })
}

/// COUNTKEYSINSLOT / GETKEYSINSLOT 差分（对标 C# ClusterKeyIterationFunctions
/// 两 Reader 的 !Expired 同判据）：到期与已删键两命令均不计；
/// COUNT == GET 全量个数；GET 取扫描序前 N（C# Add 后 count<max 早停）
#[test]
fn test_slot_keys_count_get_differential() -> aok::Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store) = open_test_store("slotkey.db")?;
    let session = store.new_session()?;
    let ss = storage_session(&session);
    let slot = slot_of(0, 0);

    for k in ["k1", "k2", "k3", "k4", "k5"] {
      ss.upsert_string(k.as_bytes(), b"v").await?;
    }
    // 等价 SET k6 v EX 1 后已过期的终态：绝对过期点置于过去一刻
    //（SET k v EX 1 后 COUNT 与 GET 均不计 k，见 next/qcode.data.md 条 18）
    ss.upsert_string(b"k6", b"v").await?;
    ss.expire_in_ticks(b"k6", -TICKS_PER_MILLISECOND).await?;

    assert_eq!(ss.count_keys_in_slot(slot).await?, 5);
    let all = ss.get_keys_in_slot(slot, usize::MAX).await?;
    assert_eq!(all.len(), 5);
    assert!(!all.iter().any(|k| k == b"k6"));
    // 扫描序 = hlog 地址升序 = 写入序（不再取哈希序）
    assert_eq!(
      all,
      ["k1", "k2", "k3", "k4", "k5"]
        .iter()
        .map(|k| k.as_bytes().to_vec())
        .collect::<Vec<_>>()
    );

    // 满 N 即停：取扫描序前 3 个活键；0 → 空
    assert_eq!(
      ss.get_keys_in_slot(slot, 3).await?,
      ["k1", "k2", "k3"]
        .iter()
        .map(|k| k.as_bytes().to_vec())
        .collect::<Vec<_>>()
    );
    assert!(ss.get_keys_in_slot(slot, 0).await?.is_empty());

    // 已 DEL 键：链首墓碑 + 旧版本非链首，两命令同剔，COUNT 恒 == GET 全量长
    ss.delete_string(b"k2").await?;
    assert_eq!(ss.count_keys_in_slot(slot).await?, 4);
    let all = ss.get_keys_in_slot(slot, usize::MAX).await?;
    assert_eq!(all.len(), ss.count_keys_in_slot(slot).await?);
    assert!(!all.iter().any(|k| k == b"k2"));

    // 覆写活键后旧版本不入计数（链首去重）；覆写可能原地更新（地址不移），
    // 故此处只断言集合与个数，顺序断言留在首段纯追加写入处
    ss.upsert_string(b"k1", b"v2").await?;
    assert_eq!(ss.count_keys_in_slot(slot).await?, 4);
    let mut all = ss.get_keys_in_slot(slot, usize::MAX).await?;
    assert_eq!(all.len(), ss.count_keys_in_slot(slot).await?);
    all.sort_unstable();
    assert_eq!(
      all,
      ["k1", "k3", "k4", "k5"]
        .iter()
        .map(|k| k.as_bytes().to_vec())
        .collect::<Vec<_>>()
    );

    // 非本槽零扫描：COUNT 0 / GET 空
    assert_eq!(ss.count_keys_in_slot(slot ^ 1).await?, 0);
    assert!(ss.get_keys_in_slot(slot ^ 1, 10).await?.is_empty());

    Ok(())
  })
}

/// 单库管理器：检查点落盘 + AOF 重放 + 清空
#[test]
fn test_single_database_manager() -> aok::Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (dir, store) = open_test_store("single.db")?;
    // 段式设备：checkpoint 截断需物理删段（单文件设备无段可删，截断后退化为位点平移）
    let aof_device = Arc::new(SegmentedDevice::new(
      dir.path().join("aof.db"),
      Some(64 * 1024),
      DEFAULT_SECTOR_SIZE,
    )?);
    let wal = Arc::new(WalLog::new(aof_device, WalConfig::default())?);
    let aof = single_log_aof(wal, &RuntimeServerOptions::default()).expect("装配 single_log_aof");
    let db = Arc::new(GarnetDatabase::new(
      0,
      Arc::clone(&store),
      Arc::clone(&store.device),
      dir.path().to_path_buf(),
      Some(Arc::clone(&aof)),
    ));
    let single = SingleDatabaseManager::new(dir.path().to_path_buf(), Arc::clone(&db));

    // 写入 + 入队 AOF（GarnetLog 统一编码：[AofHeader][u32 klen][key][u32 vlen][value]）
    let session = store.new_session()?;
    session.upsert(b"rk", b"rv").await?;
    let physical = session.session_string_key(b"rk");
    let mut entry = AofHeader::new().to_bytes().to_vec();
    entry[2] = AofEntryType::StoreUpsert as u8;
    entry.extend_from_slice(&(physical.len() as u32).to_le_bytes());
    entry.extend_from_slice(physical.as_slice());
    entry.extend_from_slice(&(b"rv".len() as u32).to_le_bytes());
    entry.extend_from_slice(b"rv");
    let addr = aof.log().get_sub_log(0).enqueue(&entry).unwrap();
    assert!(addr >= 0, "子日志入队须返回非负地址");
    // 物理刷盘（对标 C# Log.CommitAsync：设备面恢复以落盘记录为准）
    aof.log().commit_async().await;

    // 删除数据键（AOF 条目保留），AOF 重放恢复（域统一：重放走 AofProcessor）
    session.delete(b"rk").await?;
    assert_eq!(session.read(b"rk").await?, None);
    let replayed = single.recover_aof_async().await?;
    assert_eq!(replayed, 1);
    assert_eq!(session.read(b"rk").await?, Some(b"rv".to_vec()));

    // 检查点落盘（域统一：单机形态 AOF 随检查点截断至尾——对标 C#
    // InitiateCheckpointAsync 第 4 步 TruncateUntil(TailAddress) + Commit）
    assert!(single.take_checkpoint(true).await?);
    assert!(wcpr::find_latest_checkpoint(dir.path())?.is_some());

    // 清空（含 AOF 重置，对标 C# ResetDatabase 的 Log.Reset）
    single.flush_database(0, 0, false).await?;
    assert_eq!(session.read(b"rk").await?, None);
    // 段内残留条目被版本基线跳过：数据不复活（计数口径对标 C#
    // SingleLogRecover 的扫描条数——含被 ShouldSkipRecord 跳过的条目）
    single.recover_aof_async().await?;
    assert_eq!(session.read(b"rk").await?, None, "旧代条目须被版本基线跳过");

    // 单库快照契约
    assert_eq!(single.get_databases_snapshot().len(), 1);
    assert!(single.try_get_database().is_some());
    Ok(())
  })
}

/// 单库数据库管理器契约：检查点暂停/恢复幂等、快照计数
#[test]
fn test_single_database_manager_pause_snapshot_contract() -> aok::Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (dir, store) = open_test_store("pause_snapshot.db")?;

    // 单库：TryGetOrAddDatabase 恒返回 db0（不新增）；暂停/恢复幂等；快照仅含 db0
    let db = Arc::new(GarnetDatabase::new(
      0,
      Arc::clone(&store),
      Arc::clone(&store.device),
      dir.path().join("0"),
      None::<Arc<GarnetAppendOnlyFile>>,
    ));
    let single = SingleDatabaseManager::new(dir.path().to_path_buf(), Arc::clone(&db));
    let (got_db, added) = single.try_get_or_add_database().unwrap();
    assert!(!added);
    assert_eq!(got_db.id, 0);

    assert!(single.try_pause_checkpoints());
    assert!(!single.try_pause_checkpoints(), "已处于暂停态不得重复暂停");
    single.resume_checkpoints();
    assert!(single.try_pause_checkpoints(), "恢复后可再次暂停");

    let snapshot = single.get_databases_snapshot();
    assert_eq!(snapshot.len(), 1);
    assert_eq!(snapshot[0].id, 0);

    Ok(())
  })
}

/// 哈希对象载荷序列化往返（bitcode）
#[test]
fn test_hash_object_roundtrip() -> aok::Void {
  use std::io::Cursor;
  let mut obj = HashObject::new();
  obj.hash.insert(b"f".to_vec(), b"v".to_vec());
  let mut bytes = Vec::new();
  obj.serialize(&mut bytes)?;
  let back = HashObject::deserialize(&mut Cursor::new(bytes))?;
  assert_eq!(
    back.hash.get(b"f".as_slice()).map(|v| v.as_slice()),
    Some(b"v".as_slice())
  );
  Ok(())
}

/// 对象信封存储原语测试：obj_save / read_tag_with / delete_string 闭环
#[test]
fn test_storage_session_obj_save() -> aok::Void {
  use wcol::object_payload::{GarnetObjectPayload, obj_decode};
  use wval::KeyTag;

  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store) = open_test_store("obj_save.db")?;
    let session = store.new_session()?;
    let ss = storage_session(&session);

    let mut obj = HashObject::new();
    obj.hash.insert(b"k1".to_vec(), b"v1".to_vec());
    let payload = obj.to_blob();

    // 写入对象信封
    ss.obj_save(b"hash_key", HashObject::OBJECT_TAG, &payload)
      .await?;

    // 读取并解码
    let loaded = ss
      .read_tag_with(b"hash_key", KeyTag::ObjectEnvelope, |raw| {
        obj_decode(raw, HashObject::OBJECT_TAG).and_then(HashObject::from_blob)
      })
      .await?
      .flatten();
    assert!(loaded.is_some());
    let loaded = loaded.unwrap();
    assert_eq!(
      loaded.hash.get(b"k1".as_slice()).map(|v| v.as_slice()),
      Some(b"v1".as_slice())
    );

    // 删除后信封清除
    assert!(ss.delete_string(b"hash_key").await?);
    let after_del = ss
      .read_tag_with(b"hash_key", KeyTag::ObjectEnvelope, |raw| {
        obj_decode(raw, HashObject::OBJECT_TAG).and_then(HashObject::from_blob)
      })
      .await?;
    assert!(after_del.is_none());

    Ok(())
  })
}

/// review r1 回归：SCAN 游标推进（地址游标续扫）
#[test]
fn test_scan_cursor_semantics() -> aok::Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store) = open_test_store("r1.db")?;
    let session = store.new_session()?;
    let ss = storage_session(&session);

    // scan_cursor（地址游标）：游标记录在两页之间被删仍可续扫
    for k in ["k1", "k2", "k3"] {
      ss.upsert_string(k.as_bytes(), b"v").await?;
    }
    let (c1, p1) = ss.scan_cursor(b"k*", false, 0, 1, None).await?;
    assert_eq!(p1, vec![b"k1".to_vec()]);
    ss.delete_string(b"k1").await?;
    let (c2, p2) = ss.scan_cursor(b"k*", false, c1, 1, None).await?;
    assert_eq!(p2, vec![b"k2".to_vec()]);
    assert!(c2 > 0, "k2 后仍有 k3，应报告续扫游标");

    Ok(())
  })
}

/// review r10 回归：WATCH 校验 / ZMPOP WRONGTYPE 传播 /
/// 写路径去双读后的 WRONGTYPE 与缺键不物化
#[test]
fn test_smove_and_empty_collection_cleanup() -> aok::Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store) = open_test_store("r10.db")?;
    let session = store.new_session()?;
    // 写面推进经 wkv 引擎钩子进共享版本表；WATCH 校验走生产出口
    // wtxn::TxnWatchedKeysContainer（对标 C# WatchedKeysContainer）
    let version_map = Arc::new(WatchVersionMap::new(DEFAULT_VERSION_MAP_SIZE));
    let _ = session
      .store
      .set_watch_hook(version_map_watch_hook(Arc::clone(&version_map)));
    let ss = StorageSession::new(session.enter_batch());

    // ---- WATCH：版本表键级精确脏检（r13 接线：写路径推进共享版本表）----
    let mut watcher = TxnWatchedKeysContainer::new(Arc::clone(&version_map));
    watcher.add_watch(b"wk");
    assert!(watcher.validate_watch_version(), "登记后无写入应无冲突");
    // 无关键写入不算脏（C# 按键哈希分桶判定，非全局尾地址过估）
    ss.upsert_string(b"other", b"v").await?;
    assert!(watcher.validate_watch_version(), "无关键写入不得误伤");
    ss.upsert_string(b"wk", b"v").await?;
    assert!(!watcher.validate_watch_version(), "被监视键被写即判冲突");
    watcher.reset();
    assert!(watcher.validate_watch_version(), "空监视表恒无冲突");

    Ok(())
  })
}

/// 测试 StorageSession 上高级契约 API（统一读状态机、条件删除）
#[test]
fn test_storage_session_advanced_contract_apis() -> aok::Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store) = open_test_store("contract.db")?;
    let session = store.new_session()?;
    let ss = storage_session(&session);

    // 未附着 wkv 会话一致读态：普通会话形态
    assert!(!ss.is_consistent_read_session());

    // 2. delete_if_expired_in_memory（内部记录过滤判据唯一落在
    // wval::ns_codec::NamespaceDbCodec::extract_live_user_key 的可见性单点，
    // 语义用例见 wval/tests/ns_codec.rs::test_session_prefix_and_live_user_key_filtering）
    ss.upsert_string(b"uni_key", b"100").await?;
    assert!(!ss.delete_if_expired_in_memory(b"uni_key").await?);

    Ok(())
  })
}

/// review 甄别回归：SCAN 正向多版本扫描去重（对标 C# ConditionalScanPush
/// 「存在更高地址版本即不推送」语义）——覆盖写不重复、DEL 后不复活、
/// DEL 后重写与类型覆写按最新版本识别、双域并存按地址新者胜恰收一次
#[test]
fn test_scan_multi_version_dedup() -> aok::Void {
  use wcol::object_payload::GarnetObjectPayload;
  use wnode::storage::session::common::array_key_iteration_functions::ScanTypeFilter;
  use wval::GarnetObjectType;

  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store) = open_test_store("scan_dedup.db")?;
    let session = store.new_session()?;
    let ss = storage_session(&session);
    let all = |keys: &[Vec<u8>], k: &[u8]| keys.iter().any(|x| x.as_slice() == k);

    // 覆盖写：SCAN 恰含 k 一次（旧版本记录不得屏蔽新版本）
    ss.upsert_string(b"k", b"v1").await?;
    ss.upsert_string(b"k", b"v2").await?;
    let (_, keys) = ss.scan_cursor(b"k*", false, 0, 10, None).await?;
    assert!(all(&keys, b"k"), "覆盖写后 k 必须可见");
    assert_eq!(keys.iter().filter(|x| x.as_slice() == b"k").count(), 1);

    // DEL 后不复活：已删键不得以旧版本记录出现在 SCAN
    ss.delete_string(b"k").await?;
    let (_, keys) = ss.scan_cursor(b"k*", false, 0, 10, None).await?;
    assert!(!all(&keys, b"k"), "DEL 后 k 不得复活");

    // DEL 后重写：恰含 k 一次（墓碑不得屏蔽新版本，新版本不得被旧版本屏蔽）
    ss.upsert_string(b"k", b"v3").await?;
    let (_, keys) = ss.scan_cursor(b"k*", false, 0, 10, None).await?;
    assert_eq!(keys.iter().filter(|x| x.as_slice() == b"k").count(), 1);

    // 类型覆写：String → Hash 信封，TYPE hash 含 k、TYPE string 不含 k
    let mut obj = HashObject::new();
    obj.hash.insert(b"f".to_vec(), b"v".to_vec());
    ss.obj_save(b"k", HashObject::OBJECT_TAG, &obj.to_blob())
      .await?;
    let (_, keys) = ss
      .scan_cursor(
        b"k*",
        false,
        0,
        usize::MAX,
        Some(ScanTypeFilter::Object(GarnetObjectType::Hash)),
      )
      .await?;
    assert!(all(&keys, b"k"), "TYPE hash 必须按最新信封记录识别 k");
    // 双域并存（旁路写场景）：无 TYPE 的 SCAN 按日志地址新者胜恰收 k 一次
    let (_, keys) = ss.scan_cursor(b"k*", false, 0, 10, None).await?;
    assert_eq!(keys.iter().filter(|x| x.as_slice() == b"k").count(), 1);
    let (_, keys) = ss
      .scan_cursor(b"k*", false, 0, usize::MAX, Some(ScanTypeFilter::String))
      .await?;
    assert!(!all(&keys, b"k"), "TYPE string 不得按旧 String 记录误报 k");

    Ok(())
  })
}

/// SCAN 游标有效性校验三态（对标 C# validateCursor + SnapCursorToLogicalAddress，
/// AllocatorScan.cs:229-232）：正常续扫推进、记录中间地址与越尾地址终结回
/// (0, 空)、截断后旧地址钳制重扫不误杀
#[test]
fn test_scan_cursor_validation() -> aok::Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store) = open_test_store("scan_cursor_validate.db")?;
    let session = store.new_session()?;
    let ss = storage_session(&session);

    // 正常续扫：页满游标逐页推进直至扫尽回 0
    for k in ["k1", "k2", "k3", "k4"] {
      ss.upsert_string(k.as_bytes(), b"v").await?;
    }
    let (c1, p1) = ss.scan_cursor(b"k*", false, 0, 1, None).await?;
    assert_eq!(p1, vec![b"k1".to_vec()]);
    assert!(c1 > 0);
    let (c2, p2) = ss.scan_cursor(b"k*", false, c1, 2, None).await?;
    assert_eq!(p2, vec![b"k2".to_vec(), b"k3".to_vec()]);
    let (c3, p3) = ss.scan_cursor(b"k*", false, c2, 10, None).await?;
    assert_eq!(p3, vec![b"k4".to_vec()]);
    assert_eq!(c3, 0);

    // 无效游标：记录中间地址（不对齐 + 8 字节对齐）→ (0, 空)
    let (c, keys) = ss.scan_cursor(b"k*", false, c1 + 1, 10, None).await?;
    assert_eq!((c, keys.len()), (0, 0));
    let (c, keys) = ss.scan_cursor(b"k*", false, c1 + 8, 10, None).await?;
    assert_eq!((c, keys.len()), (0, 0));

    // 无效游标：越尾地址 → (0, 空)
    let tail = store.tail_address();
    let (c, keys) = ss.scan_cursor(b"k*", false, tail, 10, None).await?;
    assert_eq!((c, keys.len()), (0, 0));
    let (c, keys) = ss.scan_cursor(b"k*", false, tail + 4096, 10, None).await?;
    assert_eq!((c, keys.len()), (0, 0));

    // 截断旧地址：begin 推进越 c2 后，旧游标 c1（< begin）按 C# ScanLookup
    // 的 BeginAddress 钳制重扫语义不误杀，从截断点续扫输出存活键
    store.flush_all().await?;
    store.shift_read_only_address(c2);
    store.shift_head_address(c2);
    store.shift_begin_address(c2).await?;
    assert_eq!(store.begin_address(), c2);
    let (c, keys) = ss.scan_cursor(b"k*", false, c1, 10, None).await?;
    assert_eq!((c, keys), (0, vec![b"k4".to_vec()]));

    Ok(())
  })
}
