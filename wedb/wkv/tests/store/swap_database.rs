//! SWAPDB 跨库交换数据面内核测试（实现见 wkv/src/session/swap.rs，
//! C# 映射 MultiDatabaseManager.cs:TrySwapDatabases）。
//!
//! 共享单日志多库模型下交换为全 tag 真实搬移：
//! - String / ObjectEnvelope / Meta（RangeIndex 元记录 + 树文件）全域互换；
//! - 随键 TTL / ETag 旁路记录严格跟随；
//! - 已过期键交换后目标库惰性清除（TTL 语义跨库不变）；
//! - 交换经物理写端口，checkpoint 恢复后交换视图持久。

use std::{
  fs::create_dir_all,
  sync::{Arc, atomic::Ordering::Relaxed},
};

use aok::{OK, Void};
use compio::runtime::Runtime;
use log::info;
use tempfile::tempdir;
use wbase::time::now_ticks;
use wbftree::{StorageBackendType, TreeTuning};
use wcpr::CheckpointType;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wval::KeyTag;

use crate::support::{config, open_store, open_store_in};

/// 与 C# 测试一致的默认树调优（min_record=8 / max_record=1024 / max_key_len=128）
const TUNE: TreeTuning = TreeTuning {
  cache_size: 65536,
  min_record_size: 8,
  max_record_size: 1024,
  max_key_len: 128,
  leaf_page_size: 0,
};

/// 全域交换：字符串 / 对象信封 / 随键 TTL 与 ETag 互换，同名键正确交叉
#[test]
fn test_swap_databases_full_domain() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let env = open_store("swap_full_domain.db", config(2048, 64 * 1024, 16)?)?;
    let store = env.store;

    // db0：字符串 + TTL + ETag、对象信封（Hash 类）
    let s0 = store.new_session()?;
    s0.set_context(0, 0);
    s0.upsert(b"str0", b"v0").await?;
    s0.put_ttl(b"str0", i64::MAX).await?;
    s0.put_etag(b"str0", 42).await?;
    s0.upsert_tag(b"obj0", KeyTag::ObjectEnvelope, b"\x03hash-payload")
      .await?;

    // db1：字符串 + 对象信封（ZSet 类）+ TTL
    let s1 = store.new_session()?;
    s1.set_context(0, 1);
    s1.upsert(b"str1", b"v1").await?;
    s1.upsert_tag(b"obj1", KeyTag::ObjectEnvelope, b"\x01zset-payload")
      .await?;
    s1.put_ttl(b"obj1", i64::MAX).await?;

    // 同名键交叉：两库各有 own，交换后各自读到对方值
    s0.upsert(b"own", b"from0").await?;
    s1.upsert(b"own", b"from1").await?;

    s0.swap_databases(0, 1).await?;

    // db0 现持有原 db1 数据
    assert_eq!(s0.read(b"str1").await?, Some(b"v1".to_vec()));
    let env_k = s0.session_tag_key(KeyTag::ObjectEnvelope, b"obj1");
    assert_eq!(
      s0.read_raw(&env_k).await?,
      Some(b"\x01zset-payload".to_vec()),
      "对象信封须整值随库迁移"
    );
    assert_eq!(
      s0.ttl_of(b"obj1").await?,
      Some(i64::MAX),
      "对象键 TTL 须随迁（put_ttl 裸写内核逐位相等，粗化只归 expire_at 入口）"
    );
    assert_eq!(
      s0.read(b"own").await?,
      Some(b"from1".to_vec()),
      "同名键须交叉"
    );
    assert_eq!(s0.read(b"str0").await?, None, "原 db0 键须搬离");
    assert_eq!(s0.etag_of(b"str0").await?, None, "原 db0 ETag 须随键搬离");

    // db1 现持有原 db0 数据（含 TTL / ETag 旁路记录）
    assert_eq!(s1.read(b"str0").await?, Some(b"v0".to_vec()));
    // put_ttl 裸写内核不粗化：i64::MAX 原样落盘（值域裁决归 expire_at /
    // network_expire 两入口）
    assert_eq!(s1.ttl_of(b"str0").await?, Some(i64::MAX));
    assert_eq!(
      s1.etag_of(b"str0").await?,
      Some(42),
      "ETag 旁路记录须随键搬移"
    );
    let env_k = s1.session_tag_key(KeyTag::ObjectEnvelope, b"obj0");
    assert_eq!(
      s1.read_raw(&env_k).await?,
      Some(b"\x03hash-payload".to_vec())
    );
    assert_eq!(s1.read(b"own").await?, Some(b"from0".to_vec()));
    assert_eq!(s1.read(b"str1").await?, None);
    assert_eq!(s1.ttl_of(b"obj1").await?, None);

    info!("swap_databases 全域交换测试通过");
    OK
  })
}

/// 带 TTL 键交换后过期：目标库惰性清除，双方正确消失
#[test]
fn test_swap_databases_expired_key_purges_on_target() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let env = open_store("swap_expired.db", config(2048, 64 * 1024, 16)?)?;
    let store = env.store;

    // db0 键带已过期 TTL（过去时间戳，未触发惰性清除）
    let s0 = store.new_session()?;
    s0.set_context(0, 0);
    s0.upsert(b"dying", b"v").await?;
    s0.put_ttl(b"dying", now_ticks() - 1).await?;

    let s1 = store.new_session()?;
    s1.set_context(0, 1);
    s1.upsert(b"alive", b"w").await?;

    s0.swap_databases(0, 1).await?;

    // 交换后 db0 只有 alive；dying 搬至 db1 且 TTL 跟随，读取时惰性清除
    assert_eq!(s0.read(b"alive").await?, Some(b"w".to_vec()));
    assert_eq!(s1.read(b"dying").await?, None, "过期键交换后须在目标库消失");
    assert_eq!(s1.ttl_of(b"dying").await?, None, "过期清除须连同 TTL 记录");

    info!("swap_databases 过期键跟随测试通过");
    OK
  })
}

/// RangeIndex 交换：元记录交叉重写 + 树文件保留（回归旧实现整树删除缺陷）
#[test]
fn test_swap_databases_range_index_keeps_trees() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let config = StoreConfig::new(1024, 64 * 1024, 16, 0.5)?
      .with_range_index_dir(dir.path().join("range_indexes"));
    let store = open_store_in(&dir, "swap_ri.db", config)?;

    let s0 = store.new_session()?;
    s0.set_context(0, 0);
    s0.range_index_create(b"ri0", StorageBackendType::Disk, TUNE)
      .await?;
    s0.range_index_set(b"ri0", b"f0", b"value0").await?;

    let s1 = store.new_session()?;
    s1.set_context(0, 1);
    s1.range_index_create(b"ri1", StorageBackendType::Disk, TUNE)
      .await?;
    s1.range_index_set(b"ri1", b"f1", b"value1").await?;

    s0.swap_databases(0, 1).await?;

    // 交换后 db0 读到原 db1 的索引（树文件按用户键命名，元记录交叉重写即重挂）
    assert_eq!(
      s0.range_index_get(b"ri1", b"f1").await?,
      Some(b"value1".to_vec()),
      "交换后 db0 须持有原 db1 索引数据"
    );
    assert!(!s0.range_index_exists(b"ri0").await?);
    // db1 读到原 db0 的索引
    assert_eq!(
      s1.range_index_get(b"ri0", b"f0").await?,
      Some(b"value0".to_vec()),
      "交换后 db1 须持有原 db0 索引数据"
    );
    assert!(!s1.range_index_exists(b"ri1").await?);

    // 交换后再写目标库索引：树可继续服务（生命周期重挂成功）
    s0.range_index_set(b"ri1", b"f0", b"cross!").await?;
    assert_eq!(
      s0.range_index_get(b"ri1", b"f0").await?,
      Some(b"cross!".to_vec())
    );

    info!("swap_databases RangeIndex 树保留测试通过");
    OK
  })
}

/// 交换结果持久：快照落盘 → 全新引擎恢复 → 交换后视图 1:1 重现
#[test]
fn test_swap_databases_survives_checkpoint_recovery() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("swap_recover.db");
    let ckpt_dir = dir.path().join("checkpoints");
    let token;

    {
      let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
      let store = Arc::new(WedbStore::open(config(2048, 64 * 1024, 16)?, device)?);
      let session = store.new_session()?;

      session.set_context(0, 0);
      session.upsert(b"k0", b"v0").await?;
      session.put_ttl(b"k0", i64::MAX).await?;
      session
        .upsert_tag(b"h0", KeyTag::ObjectEnvelope, b"\x03hash")
        .await?;
      session.set_context(0, 1);
      session.upsert(b"k1", b"v1").await?;

      session.swap_databases(0, 1).await?;

      let meta = store
        .create_checkpoint(&ckpt_dir, CheckpointType::Snapshot)
        .await?;
      token = meta.token;
    } // 模拟停机

    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
    let recovered = Arc::new(WedbStore::recover(&ckpt_dir, token, device).await?);
    let session = recovered.new_session()?;

    // 恢复后交换视图持久：db0 持有原 db1 数据，db1 持有原 db0 数据
    session.set_context(0, 0);
    assert_eq!(session.read(b"k1").await?, Some(b"v1".to_vec()));
    assert_eq!(session.read(b"k0").await?, None);
    session.set_context(0, 1);
    assert_eq!(session.read(b"k0").await?, Some(b"v0".to_vec()));
    assert_eq!(
      session.ttl_of(b"k0").await?,
      Some(i64::MAX),
      "TTL 须随交换持久（put_ttl 裸写内核逐位相等）"
    );
    let env_k = session.session_tag_key(KeyTag::ObjectEnvelope, b"h0");
    assert_eq!(session.read_raw(&env_k).await?, Some(b"\x03hash".to_vec()));

    info!("swap_databases 检查点恢复持久性测试通过");
    OK
  })
}

/// 换号成对记录原子批重建闭环（doc/zh/db.md 即时原子提交段 0x06 端到端固化）：
/// SWAPDB (1, 2) 后提交检查点，同 device 走引擎唯一的内容恢复入口
/// （`WedbStore::recover`，恢复段单趟扫描内同趟完成 DbMeta 重建），断言——
/// 两库指向按成对记录 1:1 互换重现（无「双库同指一域」中间态）、
/// 交换后各库数据随互换域可读（成对记录与同值 0x02 随落批整体原子持久化）、
/// 重启分配水位越过互换涉及的两个历史号（后续新号零撞）
#[test]
fn test_swap_pair_record_survives_rebuild() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let cpr_dir = dir.path().join("checkpoints");
    create_dir_all(&cpr_dir)?;
    let device = Arc::new(SegmentedDevice::single_file(
      dir.path().join("swap_pair_rebuild.db"),
    )?);
    let (v1, v2, next_before);
    let cpr_token: u128;
    {
      let store = Arc::new(WedbStore::open(
        config(2048, 64 * 1024, 16)?,
        Arc::clone(&device),
      )?);
      let s1 = store.new_session()?;
      s1.set_context(0, 1);
      s1.upsert(b"k1", b"one").await?;
      let s2 = store.new_session()?;
      s2.set_context(0, 2);
      s2.upsert(b"k2", b"two").await?;
      v1 = store.vdb.get_virtual_ids(0, 1).1;
      v2 = store.vdb.get_virtual_ids(0, 2).1;
      assert!(v1 > 0 && v2 > 0 && v1 != v2, "两库建档各持独立非零号");

      s1.swap_databases(1, 2).await?;
      assert_eq!(
        store.vdb.get_virtual_ids(0, 1).1,
        v2,
        "内存末态：库 1 指向原库 2 域"
      );
      assert_eq!(
        store.vdb.get_virtual_ids(0, 2).1,
        v1,
        "内存末态：库 2 指向原库 1 域"
      );
      next_before = store.vdb.next_virtual_id.load(Relaxed);
      cpr_token = store
        .create_checkpoint(&cpr_dir, CheckpointType::Snapshot)
        .await?
        .token;
      drop(s1);
      drop(s2);
      drop(store);
    }
    // 重启：根域快照经 0x06 成对记录（与同值 0x02 随落记录）收敛为互换末态
    let store2 = Arc::new(WedbStore::recover(&cpr_dir, cpr_token, Arc::clone(&device)).await?);
    assert_eq!(
      (
        store2.vdb.get_virtual_ids(0, 1).1,
        store2.vdb.get_virtual_ids(0, 2).1
      ),
      (v2, v1),
      "重建后两库指向 1:1 互换重现，绝无中间态"
    );
    assert!(
      store2.vdb.next_virtual_id.load(Relaxed) >= next_before,
      "重启分配水位不回退（0x05 落盘水位与映射折叠取大）"
    );
    assert!(
      store2.vdb.next_virtual_id.load(Relaxed) > v1.max(v2),
      "水位越过互换涉及的两个历史号"
    );

    // 交换语义持久：库 1 读到原库 2 数据，原库 1 键不再可见
    let r1 = store2.new_session()?;
    assert!(r1.set_context(0, 1));
    assert_eq!(
      r1.read(b"k2").await?,
      Some(b"two".to_vec()),
      "互换后数据随域可读"
    );
    assert_eq!(r1.read(b"k1").await?, None, "库 1 原键已随域搬到库 2");
    let r2 = store2.new_session()?;
    assert!(r2.set_context(0, 2));
    assert_eq!(r2.read(b"k1").await?, Some(b"one".to_vec()));
    assert_eq!(r2.read(b"k2").await?, None);

    // 重启后新分配号与历史号零撞：FLUSHDB 换号即取新号
    store2.flush_database(0, 1).await?;
    let nv = store2.vdb.get_virtual_ids(0, 1).1;
    assert!(
      nv != v1 && nv != v2 && nv > v1.max(v2),
      "重建后换号必取严格新高号（撞号即旧域幽灵复活）"
    );
    info!("SWAPDB 成对记录重建闭环测试通过");
    OK
  })
}

/// 同库交换与跨命名空间隔离
#[test]
fn test_swap_databases_same_db_and_namespace_isolation() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let env = open_store("swap_isolation.db", config(2048, 64 * 1024, 16)?)?;
    let store = env.store;

    let ns1 = store.new_session()?;
    ns1.set_context(1, 0);
    ns1.upsert(b"ns1-key", b"vns").await?;

    let s = store.new_session()?;
    s.set_context(0, 0);
    s.upsert(b"a", b"va").await?;
    s.set_context(0, 1);
    s.upsert(b"b", b"vb").await?;

    // 同库交换为无操作
    s.set_context(0, 0);
    s.swap_databases(1, 1).await?;
    assert_eq!(s.read(b"a").await?, Some(b"va".to_vec()));

    // 交换不波及其他命名空间
    s.swap_databases(0, 1).await?;
    assert_eq!(
      ns1.read(b"ns1-key").await?,
      Some(b"vns".to_vec()),
      "交换不得波及其他命名空间"
    );

    info!("swap_databases 同库与命名空间隔离测试通过");
    OK
  })
}
