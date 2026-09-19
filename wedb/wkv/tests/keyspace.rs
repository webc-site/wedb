//! keyspace 统计集成测试（对标 Garnet PopulateKeyspaceInfo → GetKeyspaceStats
//! → GetDatabaseKeyspaceStats → StorageSession.KeyspaceStats 单内核）
//!
//! 覆盖：无 TTL 键计数、有 TTL 未过期计入 expireCount、已过期键两栏均不计、
//! 删除后的键不计、同键多版本去重、集合键经对象信封记录计数、多 ns/db 隔离计数、
//! 全量驱逐至磁盘冷区后统计口径不变、换号退役窗口内同空间活域不被误判死亡，
//! 以及冷库（ns 在册而库路由未装载）统计贴不错号与只读零副作用。
//!
//! 统计出口只有生产面 [`wkv::WedbStore::keyspace_stats`]（INFO KEYSPACE 段
//! 数据面，按在册库逐库返回 `(库号, 活键数, 带 TTL 数)`，含零键库行）。

use std::{
  fs::create_dir_all,
  sync::{Arc, atomic::Ordering::Relaxed},
  time::Duration,
};

use aok::{OK, Void};
use compio::{runtime::Runtime, time::sleep};
use tempfile::tempdir;
use wbase::{
  convert::{TICKS_PER_MILLISECOND, TICKS_PER_SECOND},
  time::now_ticks,
};
use wcpr::CheckpointType;
use wdev::SegmentedDevice;
use wkv::{TtlOpt, WedbStore};
use wtest_base::{open_test_store, test_store_config};
use wval::{GarnetObjectType, KeyTag, MetaValue};

/// 冷库用例的逻辑命名空间（与根域 (0, 0) 错开：非根域库级路由表按冷租户
/// 条款零常驻，重启后天然处于「ns 在册而库路由未装载」的冷库形态）
const COLD_NS: u64 = 7;

/// 测试 1: 基础计数语义——无 TTL 键计入 keyCount、有 TTL 未过期计入
/// expireCount、删除后的键不计、同键多版本只计一次
#[test]
fn test_keyspace_basic_counts() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store) = open_test_store("basic")?;
    let session = store.new_session()?;

    // 空库：在册 0 库两栏均为 0
    assert_eq!(store.keyspace_stats(0).await?, [(0, 0, 0)]);

    // 无 TTL 键：计入 keyCount，不计入 expireCount
    for k in ["k1", "k2", "k3"] {
      session.upsert(k.as_bytes(), b"v").await?;
    }
    assert_eq!(store.keyspace_stats(0).await?, [(0, 3, 0)]);

    // 有 TTL 未过期：同时计入两栏
    assert_eq!(
      session
        .expire_at(b"k2", now_ticks() + TICKS_PER_SECOND * 60, TtlOpt::NONE)
        .await?,
      1
    );
    assert_eq!(store.keyspace_stats(0).await?, [(0, 3, 1)]);

    // 同键覆盖写产生多个日志版本，统计仍只计一次
    session.upsert(b"k1", b"v2").await?;
    session.upsert(b"k1", b"v3").await?;
    assert_eq!(store.keyspace_stats(0).await?, [(0, 3, 1)]);

    // 删除后的键不计（墓碑屏蔽历史版本）
    assert!(session.delete(b"k3").await?);
    assert_eq!(store.keyspace_stats(0).await?, [(0, 2, 1)]);

    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// 测试 2: 已过期键两栏均不计（GC 关闭、过期记录未物理清除时同样排除）
#[test]
fn test_keyspace_expired_not_counted() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store) = open_test_store("expired")?;
    let session = store.new_session()?;

    session.upsert(b"dead", b"v").await?;
    session.upsert(b"alive", b"v").await?;
    assert_eq!(
      session
        .expire_at(
          b"dead",
          now_ticks() + TICKS_PER_MILLISECOND * 50,
          TtlOpt::NONE
        )
        .await?,
      1
    );
    sleep(Duration::from_millis(120)).await;

    // 过期记录仍在日志中，统计须排除：存活 1 个且无 TTL
    assert_eq!(store.keyspace_stats(0).await?, [(0, 1, 0)]);

    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// 测试 3: 集合键经对象信封记录计数，孤儿元记录（size = 0）不计，
/// 集合 TTL 计入 expireCount
#[test]
fn test_keyspace_collections() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store) = open_test_store("collections")?;
    let session = store.new_session()?;

    // 存活集合对象信封（Hash）计 1 个，无 TTL
    // （通用对象一律 ObjectEnvelope 信封存储，KeyTag::Meta 仅供 RangeIndex）
    let envelope = [GarnetObjectType::Hash as u8, b'b', b'i', b't', b's'];
    session
      .upsert_tag(b"coll", KeyTag::ObjectEnvelope, &envelope)
      .await?;
    assert_eq!(store.keyspace_stats(0).await?, [(0, 1, 0)]);

    // 集合键设 TTL 计入 expireCount
    assert_eq!(
      session
        .expire_at(b"coll", now_ticks() + TICKS_PER_SECOND * 60, TtlOpt::NONE)
        .await?,
      1
    );
    assert_eq!(store.keyspace_stats(0).await?, [(0, 1, 1)]);

    // 孤儿元记录（size = 0）不计入 keyCount
    let ghost = MetaValue::new(2, GarnetObjectType::Hash, 0);
    let meta_k = session.session_meta_key(b"ghost");
    session.upsert_raw(&meta_k, &ghost.to_bytes()).await?;
    assert_eq!(store.keyspace_stats(0).await?, [(0, 1, 1)]);

    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// 测试 4: 多 ns/db 隔离计数——同名用户键按 (ns, db) 分桶各计一次，
/// TTL 亦按隔离槽位独立判定；统计严格按租户在册库逐库出行
#[test]
fn test_keyspace_ns_db_isolation() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store) = open_test_store("ns_db")?;
    let session = store.new_session()?;

    // (ns=0, db=0): a, b
    session.upsert(b"a", b"v").await?;
    session.upsert(b"b", b"v").await?;
    // (ns=0, db=1): a, c
    session.set_active_db(1);
    session.upsert(b"a", b"v").await?;
    session.upsert(b"c", b"v").await?;
    // (ns=1, db=0): a, d
    session.set_context(1, 0);
    session.upsert(b"a", b"v").await?;
    session.upsert(b"d", b"v").await?;

    // 逐库分行：ns 0 两库、ns 1 单库，同名键跨库跨空间各计一次
    assert_eq!(store.keyspace_stats(0).await?, [(0, 2, 0), (1, 2, 0)]);
    assert_eq!(store.keyspace_stats(1).await?, [(0, 2, 0)]);

    // 仅 (0,1) 的 a 设 TTL：该库行 expires +1，其余同名键不受影响
    session.set_context(0, 1);
    assert_eq!(
      session
        .expire_at(b"a", now_ticks() + TICKS_PER_SECOND * 60, TtlOpt::NONE)
        .await?,
      1
    );
    assert_eq!(store.keyspace_stats(0).await?, [(0, 2, 0), (1, 2, 1)]);
    assert_eq!(store.keyspace_stats(1).await?, [(0, 2, 0)]);

    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// 测试 5: 全量驱逐至磁盘冷区后统计口径不变（[begin, tail) 全区间扫描覆盖磁盘段）
#[test]
fn test_keyspace_after_evict() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store) = open_test_store("evict")?;
    let session = store.new_session()?;

    for i in 0..8 {
      let key = format!("k{i}");
      session.upsert(key.as_bytes(), b"value").await?;
    }
    assert_eq!(
      session
        .expire_at(b"k0", now_ticks() + TICKS_PER_SECOND * 60, TtlOpt::NONE)
        .await?,
      1
    );

    // 全量刷盘并驱逐：全部记录滑入磁盘冷区
    store.flush_and_evict_all().await?;
    assert_eq!(store.keyspace_stats(0).await?, [(0, 8, 1)]);

    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// 测试 6: EXPDELSCAN（expired_key_deletion_scan，与内置 GC 共享 collect_expired
/// 内核）——目标库过滤（缺省 0 库不越权删他库过期键）、过期键物理删除闭环
/// （数据 + raw 层 TTL 记录）、无 TTL 与未到期键不受影响、双口径返回
/// （对标 Garnet ExpiredKeyDeletionScan 的 (numExpiredKeysFound, totalRecordsScanned)）
#[test]
fn test_expired_key_deletion_scan() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store) = open_test_store("expdelscan")?;
    let session = store.new_session()?;

    // (db 0)：过期键 + 无 TTL 键 + 未到期键
    session.upsert(b"dead0", b"v").await?;
    session.upsert(b"live0", b"v").await?;
    session.upsert(b"later0", b"v").await?;
    assert_eq!(
      session
        .expire_at(
          b"dead0",
          now_ticks() + TICKS_PER_MILLISECOND * 50,
          TtlOpt::NONE
        )
        .await?,
      1
    );
    assert_eq!(
      session
        .expire_at(b"later0", now_ticks() + TICKS_PER_SECOND * 60, TtlOpt::NONE)
        .await?,
      1
    );
    // (db 1)：同名过期键，缺省扫描不得越权删除
    session.set_active_db(1);
    session.upsert(b"dead1", b"v").await?;
    assert_eq!(
      session
        .expire_at(
          b"dead1",
          now_ticks() + TICKS_PER_MILLISECOND * 50,
          TtlOpt::NONE
        )
        .await?,
      1
    );
    sleep(Duration::from_millis(120)).await;

    // 缺省 0 库扫描：只删 dead0，db 1 的 dead1 不动
    session.set_context(0, 0);
    let (deleted, scanned) = store.expired_key_deletion_scan(None).await?;
    assert_eq!(deleted, 1, "缺省扫描只删 0 库过期键");
    assert!(scanned >= deleted, "扫描记录数覆盖删除数");
    assert!(!session.contains_key(b"dead0").await?);
    assert_eq!(session.read_raw(&session.ttl_key(b"dead0")).await?, None);
    assert_eq!(session.read(b"live0").await?, Some(b"v".to_vec()));
    assert!(session.pttl_ms(b"later0").await? > 0);
    // 切 db 1 验证：缺省扫描不越权删除他库过期键（raw 直读物理 TTL 记录仍在；
    // 不走 read 判定——已过期键会被读路径惰性淘汰干扰判定）
    session.set_context(0, 1);
    assert!(
      session
        .read_raw(&session.ttl_key(b"dead1"))
        .await?
        .is_some()
    );

    // 指定 db 1 扫描：物理删除 dead1（数据 + raw 层 TTL 记录）
    let (deleted, _) = store.expired_key_deletion_scan(Some(1)).await?;
    assert_eq!(deleted, 1);
    assert_eq!(session.read_raw(&session.ttl_key(b"dead1")).await?, None);
    assert!(!session.contains_key(b"dead1").await?);

    // 已删键的陈旧日志版本被双检放行：重扫零删除
    let (deleted, _) = store.expired_key_deletion_scan(None).await?;
    assert_eq!(deleted, 0);

    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// 测试 7: 换号退役窗口内的活域不被误判死亡——FLUSHDB(0, 0) 退役 vdb 0 后
/// gc_dead 含键 0，与在用 vns 0 同号：库 0 归零、同空间库 1 键数须原样报出；
/// FLUSHNS 退役 vns 后该空间全域才判死
#[test]
fn test_keyspace_after_db_swap() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store) = open_test_store("db_swap")?;
    let session = store.new_session()?;

    for k in ["a0", "b0", "c0"] {
      session.upsert(k.as_bytes(), b"v").await?;
    }
    session.set_active_db(1);
    for k in ["a1", "b1"] {
      session.upsert(k.as_bytes(), b"v").await?;
    }
    assert_eq!(store.keyspace_stats(0).await?, [(0, 3, 0), (1, 2, 0)]);

    // 库级换号：退役旧 vdb 0（键 0 与在用 vns 0 同号碰撞）——逐库分行即见
    // 0 库归零而 1 库键数原样，退役域不误伤在用库
    store.flush_database(0, 0).await?;
    assert_eq!(
      store.keyspace_stats(0).await?,
      [(0, 0, 0), (1, 2, 0)],
      "退役窗口内同空间活库不得被误判清零"
    );

    // 命名空间级换号：退役 vns 0，旧域全部判死——新空间仅剩清库流程自身登记的
    // 空库，活键计数归零（展示侧按 C# 口径剔除零键库行，INFO KEYSPACE 不出条目）
    store.flush_namespace(0).await?;
    assert!(
      store
        .keyspace_stats(0)
        .await?
        .iter()
        .all(|&(_, keys, expires)| keys == 0 && expires == 0),
      "退役空间的域须归零"
    );

    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// 测试 8: 冷库（`ns` 在册而库路由未装载）统计贴不错号 + 全程只读零副作用
///
/// 回归旧慢路径断链（INFO 逐库 `set_active_db` 且丢弃切库结果）：严格会话遇
/// 冷库时 `set_context` 返回 false、不改会话任何状态，旧内核仍按**上一库**
/// 物理前缀扫描并把上一库计数贴到本库号上——同一计数串贴到多个错误库号。
/// 现由引擎单内核只读枚举在册库（[`wkv::WedbStore::keyspace_stats`]），口径
/// 对标 C# `MultiDatabaseManager.cs:GetDatabasesSnapshot`（未实例化库不出现在
/// 快照里）：冷库既不得凭空出行，更不得把他库计数贴到自身库号上。
///
/// 两库键数/TTL 数刻意互不相同（db 0 = 2 键 1 TTL，db 1 = 3 键 0 TTL），
/// 任何贴号/并号形态都会立即失配。
#[test]
fn test_keyspace_cold_db_not_misattributed() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let cpr_dir = dir.path().join("checkpoints");
    create_dir_all(&cpr_dir)?;
    let device = Arc::new(SegmentedDevice::single_file(
      dir.path().join("cold_keyspace.db"),
    )?);
    let mut config = test_store_config();
    config.gc.enabled = false;

    // 首进程：非根租户两库各灌不同键数
    let store = Arc::new(WedbStore::open(config, Arc::clone(&device))?);
    let session = store.new_session()?;
    session.set_context(COLD_NS, 0);
    session.upsert(b"a", b"v").await?;
    session.upsert(b"b", b"v").await?;
    assert_eq!(
      session
        .expire_at(b"a", now_ticks() + TICKS_PER_SECOND * 60, TtlOpt::NONE)
        .await?,
      1
    );
    session.set_context(COLD_NS, 1);
    for k in ["c", "d", "e"] {
      session.upsert(k.as_bytes(), b"v").await?;
    }
    assert_eq!(
      store.keyspace_stats(COLD_NS).await?,
      [(0, 2, 1), (1, 3, 0)],
      "暖态逐库归属基线"
    );
    let vns = store
      .vdb
      .vns_of_ns(COLD_NS)
      .expect("set_context 须建逻辑命名空间映射");
    let token = store
      .create_checkpoint(&cpr_dir, CheckpointType::Snapshot)
      .await?
      .token;
    drop(session);
    drop(store);

    // 重启装载映射面：ns 标量映射常驻在册，非根域库级路由表零常驻（冷库）
    let store = Arc::new(WedbStore::recover(&cpr_dir, token, device).await?);
    assert_eq!(
      store.vdb.vns_of_ns(COLD_NS),
      Some(vns),
      "ns 映射须由磁盘 DbMeta 还原"
    );
    assert!(
      store.vdb.registered_dbs(vns).is_empty(),
      "冷库前提：库路由未装载"
    );

    let tail = store.tail_address();
    let allocated = store.vdb.next_virtual_id.load(Relaxed);

    // 冷库整租户零出行：既不虚报他库计数，也不因未装载而造出零键行
    let rows = store.keyspace_stats(COLD_NS).await?;
    assert!(rows.is_empty(), "冷库统计不得出行: {rows:?}");

    // 严格会话切冷库被拒（上下文不物化）——统计口径不随之漂移，仍零出行
    let strict = store.new_session()?;
    strict.set_strict_context(true);
    assert!(
      !strict.set_context(COLD_NS, 1),
      "冷库严格会话拒绝盲分配（挂起面语义）"
    );
    let rows = store.keyspace_stats(COLD_NS).await?;
    assert!(
      rows.is_empty(),
      "上下文未物化亦不得把任何库计数贴到库号上: {rows:?}"
    );

    // 点查装载 db 0（首访冷装载，即协议层挂起面所走路径）：只出 db 0 自身行，
    // db 1 仍在册外不出行——贴号形态在此必失配
    store.resolve_context(COLD_NS, 0).await?;
    assert_eq!(
      store.keyspace_stats(COLD_NS).await?,
      [(0, 2, 1)],
      "已装载库计各归本号，未装载库不得被贴号"
    );

    // 装载 db 1 后两行各归各号（错号/并号一律失配）
    store.resolve_context(COLD_NS, 1).await?;
    assert_eq!(
      store.keyspace_stats(COLD_NS).await?,
      [(0, 2, 1), (1, 3, 0)],
      "逐库归属须各自正确"
    );

    // 本运行期从未访问的租户：统计既不出行，也不得为其建命名空间映射
    //（`vns_of_ns` + `registered_dbs` 为只读甄别口径；一旦退化为
    // `get_or_create_ns` / `get_or_create_db` 即把只读 INFO 变成改写存储状态）
    let unknown = COLD_NS + 1;
    let rows = store.keyspace_stats(unknown).await?;
    assert!(rows.is_empty(), "未在册租户不得出行: {rows:?}");
    assert!(
      store.vdb.vns_of_ns(unknown).is_none(),
      "统计绝不为未在册租户盲分配虚拟命名空间"
    );

    // 全程只读：日志零追加（无 KeyTag::DbMeta 写盘）+ 虚库号零消耗
    assert_eq!(
      store.tail_address(),
      tail,
      "统计不得追加日志记录（只读命令零写副作用）"
    );
    assert_eq!(
      store.vdb.next_virtual_id.load(Relaxed),
      allocated,
      "统计不得盲分配新虚拟库 ID"
    );

    drop(strict);
    aok::Result::<()>::Ok(())
  })?;
  OK
}
