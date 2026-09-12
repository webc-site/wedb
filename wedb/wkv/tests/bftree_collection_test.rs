//! BfTree 集合端到端集成测试 (Hash / Set / ZSet / List)
//!
//! 验证点：
//! 1. 四大集合的增删改查、点查、范围扫描、双端队列操作；
//! 2. 严格删空生命周期：size 降为 0 时写墓碑、清理元数据并释放删除底层磁盘文件；
//! 3. CPR 检查点快照与故障重启恢复：恢复后存根自愈、数据完好且可继续写入与删空。

use std::{fs, sync::Arc};

use aok::{OK, Result, Void};
use compio::runtime::Runtime;
use tempfile::tempdir;
use wbase::time::now_ticks;
use wcpr::CheckpointType;
use wdev::SegmentedDevice;
use wkv::{CheckpointManager, StoreConfig, TtlOpt, WedbStore};

/// 构造独立临时目录中的全新引擎与会话
fn open_store(dir: &tempfile::TempDir, name: &str) -> Result<Arc<WedbStore<SegmentedDevice>>> {
  let config = StoreConfig::new(1024, 64 * 1024, 16, 0.5)?
    .with_range_index_dir(dir.path().join("range_indexes"));
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(name))?);
  Ok(Arc::new(WedbStore::open(config, device)?))
}

#[test]
fn test_bftree_hash_crud_and_scan() -> Void {
  Runtime::new()?.block_on(async {
    let dir = tempdir()?;
    let store = open_store(&dir, "hash.db")?;
    let session = store.new_session()?;
    let key = b"my_hash";

    // 1. 初始为空
    assert_eq!(session.bftree_hlen(key).await?, 0);
    assert_eq!(session.bftree_hget(key, b"f1").await?, None);

    // 2. 插入字段
    assert!(session.bftree_hset(key, b"f1", b"v1").await?);
    assert!(session.bftree_hset(key, b"f2", b"v2").await?);
    // 覆盖已有字段返回 false
    assert!(!session.bftree_hset(key, b"f1", b"v1_updated").await?);

    assert_eq!(session.bftree_hlen(key).await?, 2);
    assert_eq!(
      session.bftree_hget(key, b"f1").await?,
      Some(b"v1_updated".to_vec())
    );
    assert_eq!(session.bftree_hget(key, b"f2").await?, Some(b"v2".to_vec()));

    // 3. 扫描
    let records = session.bftree_hscan(key, b"", 10).await?;
    assert_eq!(records.len(), 2);
    assert_eq!(records[0], (b"f1".to_vec(), b"v1_updated".to_vec()));
    assert_eq!(records[1], (b"f2".to_vec(), b"v2".to_vec()));

    let mut scanned = Vec::new();
    let count = session
      .bftree_hscan_stream(key, b"", 10, |k, v| {
        scanned.push((k.to_vec(), v.to_vec()));
        true
      })
      .await?;
    assert_eq!(count, 2);
    assert_eq!(scanned, records);

    // 4. 删除单个字段
    assert!(session.bftree_hdel(key, b"f1").await?);
    assert_eq!(session.bftree_hlen(key).await?, 1);
    assert_eq!(session.bftree_hget(key, b"f1").await?, None);
    assert_eq!(session.bftree_hget(key, b"f2").await?, Some(b"v2".to_vec()));

    // 5. 删空触发严格生命周期释放
    assert!(session.bftree_hdel(key, b"f2").await?);
    assert_eq!(session.bftree_hlen(key).await?, 0);
    assert_eq!(session.bftree_hget(key, b"f2").await?, None);
    assert!(session.load_meta(key).await?.is_none());

    // 磁盘数据文件必须已被 unlink 删除
    let ri_dir = dir.path().join("range_indexes").join("rangeindex");
    let entries: Vec<_> = fs::read_dir(&ri_dir)?
      .filter_map(|e| e.ok())
      .filter(|e| e.path().extension().is_some_and(|ext| ext == "bftree"))
      .collect();
    assert!(
      entries.is_empty(),
      "删空后不得残留孤儿数据文件: {entries:?}"
    );

    aok::Result::<()>::Ok(())
  })?;
  OK
}

#[test]
fn test_bftree_set_crud_and_scan() -> Void {
  Runtime::new()?.block_on(async {
    let dir = tempdir()?;
    let store = open_store(&dir, "set.db")?;
    let session = store.new_session()?;
    let key = b"my_set";

    assert_eq!(session.bftree_scard(key).await?, 0);
    assert!(!session.bftree_sismember(key, b"m1").await?);

    // 添加成员
    assert!(session.bftree_sadd(key, b"m1").await?);
    assert!(session.bftree_sadd(key, b"m2").await?);
    // 重复添加返回 false
    assert!(!session.bftree_sadd(key, b"m1").await?);

    assert_eq!(session.bftree_scard(key).await?, 2);
    assert!(session.bftree_sismember(key, b"m1").await?);
    assert!(session.bftree_sismember(key, b"m2").await?);
    assert!(!session.bftree_sismember(key, b"m3").await?);
    assert_eq!(
      session
        .bftree_smismember(key, &[b"m1", b"m2", b"m3"])
        .await?,
      vec![true, true, false]
    );
    assert_eq!(
      session.bftree_smembers(key).await?,
      vec![b"m1".to_vec(), b"m2".to_vec()]
    );

    // 批量添加成员
    assert_eq!(
      session
        .bftree_sadd_batch(key, &[b"m2", b"m3", b"m4"])
        .await?,
      2 // m2 已存在，新增 m3, m4
    );
    assert_eq!(session.bftree_scard(key).await?, 4);

    // 扫描
    let members = session.bftree_sscan(key, b"", 10).await?;
    assert_eq!(members.len(), 4);
    assert_eq!(members[0], b"m1".to_vec());
    assert_eq!(members[1], b"m2".to_vec());
    assert_eq!(members[2], b"m3".to_vec());
    assert_eq!(members[3], b"m4".to_vec());

    // 批量移除成员
    assert_eq!(
      session
        .bftree_srem_batch(key, &[b"m1".as_slice(), b"m3", b"nonexistent"])
        .await?,
      2
    );
    assert_eq!(session.bftree_scard(key).await?, 2);
    assert_eq!(
      session
        .bftree_smismember(key, &[b"m1", b"m2", b"m3", b"m4"])
        .await?,
      vec![false, true, false, true]
    );

    // 移除单个成员
    assert!(session.bftree_srem(key, b"m4").await?);
    assert_eq!(session.bftree_scard(key).await?, 1);
    assert!(!session.bftree_sismember(key, b"m4").await?);

    // 删空触发释放
    assert!(session.bftree_srem(key, b"m2").await?);
    assert_eq!(session.bftree_scard(key).await?, 0);
    assert!(session.load_meta(key).await?.is_none());

    let ri_dir = dir.path().join("range_indexes").join("rangeindex");
    let entries: Vec<_> = fs::read_dir(&ri_dir)?
      .filter_map(|e| e.ok())
      .filter(|e| e.path().extension().is_some_and(|ext| ext == "bftree"))
      .collect();
    assert!(entries.is_empty(), "删空后不得残留孤儿数据文件");

    // 验证全新集合初始批量插入时 size 与 version 正确维护
    let new_set_key = b"batch_initial_set";
    assert_eq!(
      session
        .bftree_sadd_batch(new_set_key, &[b"x1", b"x2", b"x3"])
        .await?,
      3
    );
    assert_eq!(session.bftree_scard(new_set_key).await?, 3);
    assert!(session.bftree_srem(new_set_key, b"x1").await?);
    assert_eq!(session.bftree_scard(new_set_key).await?, 2);
    assert!(session.load_meta(new_set_key).await?.is_some());

    aok::Result::<()>::Ok(())
  })?;
  OK
}

#[test]
fn test_bftree_zset_crud_and_scan() -> Void {
  Runtime::new()?.block_on(async {
    let dir = tempdir()?;
    let store = open_store(&dir, "zset.db")?;
    let session = store.new_session()?;
    let key = b"my_zset";

    assert_eq!(session.bftree_zcard(key).await?, 0);
    assert_eq!(session.bftree_zscore(key, b"alice").await?, None);

    // 添加成员与分值
    assert!(session.bftree_zadd(key, b"alice", 100.0).await?);
    assert!(session.bftree_zadd(key, b"bob", 85.5).await?);
    assert!(session.bftree_zadd(key, b"charlie", 95.0).await?);
    // 更新已有成员返回 false
    assert!(!session.bftree_zadd(key, b"bob", 88.0).await?);

    assert_eq!(session.bftree_zcard(key).await?, 3);
    assert_eq!(session.bftree_zscore(key, b"alice").await?, Some(100.0));
    assert_eq!(session.bftree_zscore(key, b"bob").await?, Some(88.0));

    // NaN 防御
    assert!(
      session
        .bftree_zadd(key, b"nan_member", f64::NAN)
        .await
        .is_err()
    );

    // 全区间 O(1) 元数据直读计数
    assert_eq!(
      session
        .bftree_zcount(key, f64::NEG_INFINITY, f64::INFINITY)
        .await?,
      3
    );

    // 按分值范围统计
    assert_eq!(session.bftree_zcount(key, 80.0, 96.0).await?, 2);
    assert_eq!(session.bftree_zcount(key, 90.0, 105.0).await?, 2);

    // 开闭区间统计
    assert_eq!(
      session
        .bftree_zcount_ext(key, 88.0, false, 100.0, false)
        .await?,
      1
    ); // 仅 95.0

    // 按分值范围有序扫描
    let range = session.bftree_zrange_by_score(key, 80.0, 105.0).await?;
    assert_eq!(range.len(), 3);
    assert_eq!(range[0], (b"bob".to_vec(), 88.0));
    assert_eq!(range[1], (b"charlie".to_vec(), 95.0));
    assert_eq!(range[2], (b"alice".to_vec(), 100.0));

    // 按分值选项扫描 (开区间 + LIMIT)
    let ext_range = session
      .bftree_zrange_by_score_ext(
        key,
        wcol::ZRangeByScoreOpt::new(88.0, false, 100.0, true, 0, 1),
      )
      .await?;
    assert_eq!(ext_range.len(), 1);
    assert_eq!(ext_range[0], (b"charlie".to_vec(), 95.0));

    // 按下标排名扫描 [1, 2]
    let idx_range = session.bftree_zrange_by_index(key, 1, 2).await?;
    assert_eq!(idx_range.len(), 2);
    assert_eq!(idx_range[0], (b"charlie".to_vec(), 95.0));
    assert_eq!(idx_range[1], (b"alice".to_vec(), 100.0));

    // 移除成员
    assert!(session.bftree_zrem(key, b"bob").await?);
    assert!(session.bftree_zrem(key, b"charlie").await?);
    assert_eq!(session.bftree_zcard(key).await?, 1);

    // 删空触发释放
    assert!(session.bftree_zrem(key, b"alice").await?);
    assert_eq!(session.bftree_zcard(key).await?, 0);
    assert!(session.load_meta(key).await?.is_none());

    let ri_dir = dir.path().join("range_indexes").join("rangeindex");
    let entries: Vec<_> = fs::read_dir(&ri_dir)?
      .filter_map(|e| e.ok())
      .filter(|e| e.path().extension().is_some_and(|ext| ext == "bftree"))
      .collect();
    assert!(entries.is_empty(), "删空后不得残留孤儿数据文件");

    aok::Result::<()>::Ok(())
  })?;
  OK
}

#[test]
fn test_bftree_list_crud() -> Void {
  Runtime::new()?.block_on(async {
    let dir = tempdir()?;
    let store = open_store(&dir, "list.db")?;
    let session = store.new_session()?;
    let key = b"my_list";

    assert_eq!(session.bftree_llen(key).await?, 0);
    assert_eq!(session.bftree_lindex(key, 0).await?, None);

    // lpush / rpush
    assert_eq!(session.bftree_rpush(key, b"two").await?, 1);
    assert_eq!(session.bftree_lpush(key, b"one").await?, 2);
    assert_eq!(session.bftree_rpush(key, b"three").await?, 3);
    assert_eq!(session.bftree_llen(key).await?, 3);

    // lindex 正向与逆向索引
    assert_eq!(session.bftree_lindex(key, 0).await?, Some(b"one".to_vec()));
    assert_eq!(session.bftree_lindex(key, 1).await?, Some(b"two".to_vec()));
    assert_eq!(
      session.bftree_lindex(key, 2).await?,
      Some(b"three".to_vec())
    );
    assert_eq!(
      session.bftree_lindex(key, -1).await?,
      Some(b"three".to_vec())
    );
    assert_eq!(session.bftree_lindex(key, -2).await?, Some(b"two".to_vec()));
    assert_eq!(session.bftree_lindex(key, 3).await?, None);

    // lrange
    let all = session.bftree_lrange(key, 0, -1).await?;
    assert_eq!(
      all,
      vec![b"one".to_vec(), b"two".to_vec(), b"three".to_vec()]
    );

    // lpop
    assert_eq!(session.bftree_lpop(key).await?, Some(b"one".to_vec()));
    assert_eq!(session.bftree_llen(key).await?, 2);

    // rpop
    assert_eq!(session.bftree_rpop(key).await?, Some(b"three".to_vec()));
    assert_eq!(session.bftree_llen(key).await?, 1);

    // 弹出最后一个元素，触发删空与文件清理
    assert_eq!(session.bftree_lpop(key).await?, Some(b"two".to_vec()));
    assert_eq!(session.bftree_llen(key).await?, 0);
    assert_eq!(session.bftree_lpop(key).await?, None);
    assert!(session.load_meta(key).await?.is_none());

    let ri_dir = dir.path().join("range_indexes").join("rangeindex");
    let entries: Vec<_> = fs::read_dir(&ri_dir)?
      .filter_map(|e| e.ok())
      .filter(|e| e.path().extension().is_some_and(|ext| ext == "bftree"))
      .collect();
    assert!(entries.is_empty(), "删空后不得残留孤儿数据文件");

    aok::Result::<()>::Ok(())
  })?;
  OK
}

#[test]
fn test_bftree_session_delete_cleans_file() -> Void {
  Runtime::new()?.block_on(async {
    let dir = tempdir()?;
    let store = open_store(&dir, "del_clean.db")?;
    let session = store.new_session()?;

    session.bftree_hset(b"h_key", b"f", b"v").await?;
    session.bftree_sadd(b"s_key", b"m").await?;
    session.bftree_zadd(b"z_key", b"m", 1.0).await?;
    session.bftree_lpush(b"l_key", b"e").await?;

    let ri_dir = dir.path().join("range_indexes").join("rangeindex");
    let count = fs::read_dir(&ri_dir)?
      .filter_map(|e| e.ok())
      .filter(|e| e.path().extension().is_some_and(|ext| ext == "bftree"))
      .count();
    assert_eq!(count, 4, "创建 4 个集合应对应 4 个数据文件");

    // 调用 session.delete 统一删除
    assert!(session.delete(b"h_key").await?);
    assert!(session.delete(b"s_key").await?);
    assert!(session.delete(b"z_key").await?);
    assert!(session.delete(b"l_key").await?);

    let count_after = fs::read_dir(&ri_dir)?
      .filter_map(|e| e.ok())
      .filter(|e| e.path().extension().is_some_and(|ext| ext == "bftree"))
      .count();
    assert_eq!(count_after, 0, "delete 集合后底层数据文件必须全部被清理");

    aok::Result::<()>::Ok(())
  })?;
  OK
}

#[test]
fn test_bftree_checkpoint_and_recovery() -> Void {
  Runtime::new()?.block_on(async {
    let dir = tempdir()?;
    let ckpt_dir = dir.path().join("checkpoints");
    let db_path = dir.path().join("cpr_collection.db");
    let token;

    {
      let store = open_store(&dir, "cpr_collection.db")?;
      let session = store.new_session()?;

      session.bftree_hset(b"cpr_hash", b"k1", b"v1").await?;
      session.bftree_hset(b"cpr_hash", b"k2", b"v2").await?;

      session.bftree_sadd(b"cpr_set", b"m1").await?;
      session.bftree_sadd(b"cpr_set", b"m2").await?;

      session.bftree_zadd(b"cpr_zset", b"alice", 10.0).await?;
      session.bftree_zadd(b"cpr_zset", b"bob", 20.0).await?;

      session.bftree_rpush(b"cpr_list", b"item1").await?;
      session.bftree_rpush(b"cpr_list", b"item2").await?;

      let meta = store
        .create_checkpoint(&ckpt_dir, CheckpointType::FoldOver)
        .await?;
      token = meta.token;
    } // 模拟崩溃与关闭

    // 从检查点恢复
    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
    let recovered = Arc::new(CheckpointManager::recover(&ckpt_dir, token, device).await?);
    let session = recovered.new_session()?;

    // 验证 Hash 恢复
    assert_eq!(session.bftree_hlen(b"cpr_hash").await?, 2);
    assert_eq!(
      session.bftree_hget(b"cpr_hash", b"k1").await?,
      Some(b"v1".to_vec())
    );
    assert_eq!(
      session.bftree_hget(b"cpr_hash", b"k2").await?,
      Some(b"v2".to_vec())
    );

    // 验证 Set 恢复
    assert_eq!(session.bftree_scard(b"cpr_set").await?, 2);
    assert!(session.bftree_sismember(b"cpr_set", b"m1").await?);
    assert!(session.bftree_sismember(b"cpr_set", b"m2").await?);

    // 验证 ZSet 恢复
    assert_eq!(session.bftree_zcard(b"cpr_zset").await?, 2);
    assert_eq!(
      session.bftree_zscore(b"cpr_zset", b"alice").await?,
      Some(10.0)
    );
    assert_eq!(
      session.bftree_zscore(b"cpr_zset", b"bob").await?,
      Some(20.0)
    );

    // 验证 List 恢复
    assert_eq!(session.bftree_llen(b"cpr_list").await?, 2);
    assert_eq!(
      session.bftree_lrange(b"cpr_list", 0, -1).await?,
      vec![b"item1".to_vec(), b"item2".to_vec()]
    );

    // 恢复后继续追加写入与删除
    assert!(session.bftree_hset(b"cpr_hash", b"k3", b"v3").await?);
    assert_eq!(session.bftree_hlen(b"cpr_hash").await?, 3);

    assert_eq!(session.bftree_rpush(b"cpr_list", b"item3").await?, 3);
    assert_eq!(session.bftree_llen(b"cpr_list").await?, 3);

    // 删空验证
    session.delete(b"cpr_hash").await?;
    session.delete(b"cpr_set").await?;
    session.delete(b"cpr_zset").await?;
    session.delete(b"cpr_list").await?;

    aok::Result::<()>::Ok(())
  })?;
  OK
}

#[test]
fn test_bftree_strict_drain_with_ttl_and_version_fence() -> Void {
  Runtime::new()?.block_on(async {
    let dir = tempdir()?;
    let store = open_store(&dir, "strict_drain.db")?;
    let session = store.new_session()?;
    let key = b"ttl_drain_bftree_hash";

    // 1. 创建 BfTree Hash 并插入字段
    assert!(session.bftree_hset(key, b"f1", b"v1").await?);
    assert!(session.bftree_hset(key, b"f2", b"v2").await?);
    assert_eq!(session.bftree_hlen(key).await?, 2);

    // 2. 设置随键 TTL
    let future_ticks = now_ticks() + 3600 * 10_000_000;
    session
      .expire_at(key, future_ticks, TtlOpt::default())
      .await?;
    assert!(session.has_ttl_tag(key)?);
    assert!(session.ttl_of(key).await?.is_some());

    let old_meta = session.load_meta(key).await?.expect("meta exists");
    let old_key_id = old_meta.key_id;
    let old_version = old_meta.version;
    assert_eq!(old_version, 1);

    // 3. 删除第 1 个字段
    assert!(session.bftree_hdel(key, b"f1").await?);
    assert_eq!(session.bftree_hlen(key).await?, 1);
    assert!(session.ttl_of(key).await?.is_some());

    // 4. 删除第 2 个字段（最后一个字段），触发严格删空自愈
    assert!(session.bftree_hdel(key, b"f2").await?);
    assert_eq!(session.bftree_hlen(key).await?, 0);

    // 5. 校验：元记录被清除，随键 TTL 彻底清理，无孤儿 TTL
    assert!(session.load_meta(key).await?.is_none());
    assert!(!session.contains_key(key).await?);
    assert_eq!(session.ttl_of(key).await?, None);
    assert!(!session.contains_key_raw(&session.ttl_key(key)).await?);

    // 6. 底层数据文件必须已被释放
    let ri_dir = dir.path().join("range_indexes").join("rangeindex");
    let entries: Vec<_> = fs::read_dir(&ri_dir)?
      .filter_map(|e| e.ok())
      .filter(|e| e.path().extension().is_some_and(|ext| ext == "bftree"))
      .collect();
    assert!(
      entries.is_empty(),
      "严格删空后磁盘数据文件必须已被释放: {entries:?}"
    );

    // 7. 重建同名键：分配新 key_id，旧数据完全不可见，版本号栅栏隔离
    assert!(session.bftree_hset(key, b"f1", b"v1_reborn").await?);
    let new_meta = session.load_meta(key).await?.expect("new meta");
    assert_ne!(new_meta.key_id, old_key_id);
    assert_eq!(new_meta.version, 1);
    assert_eq!(session.bftree_hlen(key).await?, 1);
    assert_eq!(
      session.bftree_hget(key, b"f1").await?,
      Some(b"v1_reborn".to_vec())
    );
    assert_eq!(session.bftree_hget(key, b"f2").await?, None);

    aok::Result::<()>::Ok(())
  })?;
  OK
}

#[test]
fn test_bftree_ltrim_drain_and_empty_batch_safeguard() -> Void {
  Runtime::new()?.block_on(async {
    let dir = tempdir()?;
    let store = open_store(&dir, "ltrim_drain.db")?;
    let session = store.new_session()?;

    // 1. 验证空批次 SADD 不会创建孤儿树文件
    let empty_key = b"empty_batch_key";
    let added = session
      .bftree_sadd_batch(empty_key, &[] as &[&[u8]])
      .await?;
    assert_eq!(added, 0);
    assert_eq!(session.bftree_scard(empty_key).await?, 0);
    assert!(session.load_meta(empty_key).await?.is_none());

    let ri_dir = dir.path().join("range_indexes").join("rangeindex");
    let entries: Vec<_> = fs::read_dir(&ri_dir)?
      .filter_map(|e| e.ok())
      .filter(|e| e.path().extension().is_some_and(|ext| ext == "bftree"))
      .collect();
    assert!(
      entries.is_empty(),
      "空批次操作不得遗留孤儿数据文件: {entries:?}"
    );

    // 2. 测试 LTRIM 删空自愈
    let list_key = b"ltrim_drain_key";
    assert_eq!(session.bftree_rpush(list_key, b"item1").await?, 1);
    assert_eq!(session.bftree_rpush(list_key, b"item2").await?, 2);
    assert_eq!(session.bftree_llen(list_key).await?, 2);

    // 裁剪越界区间保留 0 项，触发删空
    let remaining = session.bftree_ltrim(list_key, 5, 2).await?;
    assert_eq!(remaining, 0);
    assert_eq!(session.bftree_llen(list_key).await?, 0);
    assert!(session.load_meta(list_key).await?.is_none());

    let entries_after: Vec<_> = fs::read_dir(&ri_dir)?
      .filter_map(|e| e.ok())
      .filter(|e| e.path().extension().is_some_and(|ext| ext == "bftree"))
      .collect();
    assert!(
      entries_after.is_empty(),
      "LTRIM 删空后数据文件必须已被 unlink: {entries_after:?}"
    );

    aok::Result::<()>::Ok(())
  })?;
  OK
}
