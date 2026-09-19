//! 按库清空（flush_database / flush_all_databases）隔离语义测试。
//!
//! flush_database / flush_all_databases 语义测试（实现与 C# 映射见 wkv/src/store.rs）
//!
//! 共享单日志多库模型：清库 0 不得波及库 1 与其它命名空间的同号库，
//! 随键 TTL 旁路记录与对象信封域记录一并清除；换号联动 BfTree 树回收——
//! FLUSHDB/FLUSHNS 后旧域 RI 树同步摘除注册并投递待释放队列，数据文件由
//! 后台释放消费在纪元排空后删除，同名重建索引可成功。

use std::{
  fs::create_dir_all,
  path::Path,
  sync::{Arc, atomic::Ordering::Relaxed},
  thread::spawn,
  time::{Duration, Instant},
};

use aok::{OK, Result, Void};
use compio::{runtime::Runtime, time::sleep};
use log::info;
use tempfile::{TempDir, tempdir};
use wbftree::{ScanReturnField, StorageBackendType, TreeTuning};
use wcpr::CheckpointType;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wval::{GarnetObjectType, KeyTag};

use crate::support::{config, open_store};

/// 与 range_index 会话层测试一致的默认树调优
const RI_TUNE: TreeTuning = TreeTuning {
  cache_size: 65536,
  min_record_size: 8,
  max_record_size: 1024,
  max_key_len: 128,
  leaf_page_size: 0,
};

/// 构造挂载 RangeIndex 目录的存储实例（目录由调用方保活）
fn open_ri_store(dir: &TempDir, name: &str) -> Result<Arc<WedbStore<SegmentedDevice>>> {
  let cfg = StoreConfig::new(1024, 64 * 1024, 16, 0.5)?.with_range_index_dir(dir.path().join("ri"));
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(name))?);
  Ok(Arc::new(WedbStore::open(cfg, device)?))
}

/// 驱动待释放队列消费一轮（本测试引擎无运行时释放任务，手动承接生产
/// spawn_bftree_reclaimer 的消费位），再轮询等待纪元延迟删除收敛
/// （参与者进出推进收割，绝不长等）
async fn wait_file_gone(path: &Path, store: &Arc<WedbStore<SegmentedDevice>>) -> Void {
  store.drain_bftree_release(usize::MAX);
  for _ in 0..2500 {
    if !path.exists() {
      return OK;
    }
    drop(store.new_session()?);
    sleep(Duration::from_millis(2)).await;
  }
  panic!("纪元延迟删除未收敛: {}", path.display());
}

/// 驱动释放消费并确认延迟动作已收割完毕（无活跃读者时注册即收割，参与者
/// 进出兜底推进），供「文件须留存」反向断言前确保释放动作确已执行
async fn release_settled(store: &Arc<WedbStore<SegmentedDevice>>) -> Void {
  store.drain_bftree_release(usize::MAX);
  for _ in 0..50 {
    drop(store.new_session()?);
    sleep(Duration::from_millis(2)).await;
  }
  OK
}

/// 多库写入 → flush_database(0, 0) → 库 0 全清（String/ObjectEnvelope 域与
/// TTL 旁路记录），库 1 与命名空间 1 的同号库数据完好
#[test]
fn test_flush_database_isolation() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let env = open_store("flush_database.db", config(2048, 64 * 1024, 16)?)?;
    let store = env.store;

    // 库 0：两个 String 键（其一带 TTL 侧车记录）+ 一个对象信封域记录
    let db0 = store.new_session()?;
    db0.set_context(0, 0);
    db0.upsert(b"k0:plain", b"v0").await?;
    db0.upsert(b"k0:ttl", b"v0").await?;
    db0.put_ttl(b"k0:ttl", i64::MAX).await?;
    db0
      .upsert_tag(b"k0:obj", KeyTag::ObjectEnvelope, b"\x01payload")
      .await?;

    // 库 1：数据 + TTL 侧车记录，清库 0 后必须完好
    let db1 = store.new_session()?;
    db1.set_context(0, 1);
    db1.upsert(b"k1", b"v1").await?;
    db1.put_ttl(b"k1", i64::MAX).await?;

    // 命名空间 1 的同号库 0：不得被 flush_database(0, 0) 波及
    let ns1 = store.new_session()?;
    ns1.set_context(1, 0);
    ns1.upsert(b"kns", b"vns").await?;

    // O(1) 换号清库不统计删除数，返回广播面域值 (vns, 旧 vdb)
    let (vns, domain_db) = store.flush_database(0, 0).await?;
    assert_eq!((vns, domain_db), (0, 0), "根域映射恒 (0, 0)");

    // 库 0 全清：String 域、对象信封域与 TTL 旁路记录一并消失
    db0.set_context(0, 0);
    assert_eq!(db0.read(b"k0:plain").await?, None);
    assert_eq!(db0.read(b"k0:ttl").await?, None);
    assert_eq!(db0.ttl_of(b"k0:ttl").await?, None, "TTL 侧车记录须随键清除");
    let env_key = db0.session_tag_key(KeyTag::ObjectEnvelope, b"k0:obj");
    let obj_hit = db0.read_raw_with(&env_key, |_| ()).await?;
    assert!(obj_hit.is_none(), "对象信封域记录须一并清除");

    // 库 1 完好：数据与 TTL 侧车记录不受影响
    db1.set_context(0, 1);
    assert_eq!(db1.read(b"k1").await?, Some(b"v1".to_vec()));
    // put_ttl 裸写内核不粗化：i64::MAX 原样落盘（值域裁决归 expire_at /
    // network_expire 两入口）
    assert_eq!(db1.ttl_of(b"k1").await?, Some(i64::MAX));

    // 命名空间 1 的库 0 完好
    ns1.set_context(1, 0);
    assert_eq!(ns1.read(b"kns").await?, Some(b"vns".to_vec()));

    info!("flush_database 单库隔离测试通过");
    OK
  })
}

///
/// 多库写入 → flush_all_databases → 全部域无存活用户键（FLUSHALL 全清语义）
#[test]
fn test_flush_all_databases() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let env = open_store("flush_all_databases.db", config(2048, 64 * 1024, 16)?)?;
    let store = env.store;

    for (ns, db, key) in [(0, 0, b"a"), (0, 1, b"b"), (1, 0, b"c")] {
      let session = store.new_session()?;
      session.set_context(ns, db);
      session.upsert(key, b"v").await?;
    }

    store.flush_all_databases().await?;

    for (ns, db, key) in [(0u64, 0u64, &b"a"[..]), (0, 1, b"b"), (1, 0, b"c")] {
      let session = store.new_session()?;
      session.set_context(ns, db);
      assert_eq!(session.read(key).await?, None, "{ns}/{db} 域应已清空");
    }

    info!("flush_all_databases 全清测试通过");
    OK
  })
}

/// FLUSHDB 换号联动 RI 树回收：换号臂只投递不物理释放（数据文件在返回时
/// 仍在盘，从库回放流水线不 await 任何文件删除），后台消费经纪元屏障
/// 延迟销毁旧域树文件；同名重建索引可成功且为全新空树
/// （根域换号退役 vdb 0 后旁表死亡守卫不得误杀新域登记）
#[test]
fn test_flush_database_reclaims_range_index_tree() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempfile::tempdir()?;
    let store = open_ri_store(&dir, "flush_ri_reclaim.db")?;
    let s0 = store.new_session()?;
    s0.set_context(0, 0);
    s0.range_index_create(b"idx", StorageBackendType::Disk, RI_TUNE)
      .await?;
    s0.range_index_set(b"idx", b"field", b"value").await?;
    let data_path = store.range_index().data_file_path_for_key(b"idx");
    assert!(data_path.exists(), "RI 数据文件须在盘");

    store.flush_database(0, 0).await?;

    // 异步屏障语义：换号返回只完成摘注册与投递，物理释放未在调用线程发生
    assert!(
      data_path.exists(),
      "换号臂不得同步执行纪元注册后的删除（doc/zh/db.md 不阻塞复制流水线）"
    );

    // 旧树延迟销毁：后台消费位驱动后，引擎排空与数据文件删除经纪元屏障收敛
    wait_file_gone(&data_path, &store).await?;

    // 同名重建可成功（修复点：修复前 create_bftree contains_key 拦截报
    // IndexExists，索引卡死为不可建不可删），且为全新空树
    s0.set_context(0, 0);
    s0.range_index_create(b"idx", StorageBackendType::Disk, RI_TUNE)
      .await?;
    assert!(
      s0.range_index_get(b"idx", b"field").await?.is_none(),
      "重建后须为空树，不得继承旧域数据"
    );
    s0.range_index_set(b"idx", b"field2", b"value2").await?;
    assert_eq!(
      s0.range_index_get(b"idx", b"field2").await?,
      Some(b"value2".to_vec()),
      "重建索引须可写可读"
    );

    info!("FLUSHDB 联动 RI 树回收测试通过");
    OK
  })
}

/// 世代守卫：旧域待释放批次落地前同名重建，新世代数据文件与数据不得被
/// 后台释放误删（摘注册与物理释放跨队列分离后窗口远超纪元排空本身，
/// release_detached 凭 live_indexes 同键条目重登记裁决跳过 unlink）
#[test]
fn test_flush_reclaim_deferred_preserves_rebuilt_generation() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempfile::tempdir()?;
    let store = open_ri_store(&dir, "flush_ri_generation.db")?;
    let s0 = store.new_session()?;
    s0.set_context(0, 0);
    s0.range_index_create(b"idx", StorageBackendType::Disk, RI_TUNE)
      .await?;
    // field+value 总长受 RI_TUNE.min_record_size=8 下限约束（RI 谓词门禁
    // validate_ri_kv 对标 C# RangeIndexSet 的 InvalidKV），数据须合规
    s0.range_index_set(b"idx", b"old_field", b"value0").await?;
    let data_path = store.range_index().data_file_path_for_key(b"idx");

    store.flush_database(0, 0).await?;

    // 释放落地前同名重建（新世代文件与旧代同源路径）
    s0.set_context(0, 0);
    s0.range_index_create(b"idx", StorageBackendType::Disk, RI_TUNE)
      .await?;
    s0.range_index_set(b"idx", b"new_field", b"value1").await?;
    assert!(data_path.exists(), "新世代数据文件须在盘");

    // 驱动释放消费并确认延迟动作已收割：旧引擎被弃，unlink 被世代守卫拦下
    release_settled(&store).await?;
    assert!(
      data_path.exists(),
      "旧域待释放批次不得删除新世代在用的数据文件"
    );
    assert_eq!(
      s0.range_index_get(b"idx", b"new_field").await?,
      Some(b"value1".to_vec()),
      "新世代索引数据须完好"
    );

    info!("换号延迟释放世代守卫测试通过");
    OK
  })
}

/// 世代守卫的**删除时点**复查：旧域批次已挂入纪元延迟队列之后、延迟动作真正
/// unlink 之前才同名重建（即落在排空窗口内），新世代数据文件与数据仍不得被删。
///
/// 与 test_flush_reclaim_deferred_preserves_rebuilt_generation 的分工：那一例重建
/// 发生在登记延迟动作之前，只锁住登记时点的短路；本例用长读会话钉住旧纪元，
/// 把「已登记、未收割」的排空窗口确定性打开后再重建——修复前守卫只在登记时点
/// 判一次，动作收割时无条件 unlink，本例即红（新树句柄继续向孤儿 inode 写，
/// 重启惰性恢复报数据文件缺失）。不依赖真实 200ms 后台轮询时序。
#[test]
fn test_flush_reclaim_generation_guard_at_unlink_time() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempfile::tempdir()?;
    let store = open_ri_store(&dir, "flush_ri_guard_unlink.db")?;
    let s0 = store.new_session()?;
    s0.set_context(0, 0);
    s0.range_index_create(b"idx", StorageBackendType::Disk, RI_TUNE)
      .await?;
    s0.range_index_set(b"idx", b"old_field", b"value0").await?;
    let data_path = store.range_index().data_file_path_for_key(b"idx");
    // 旧世代引擎实例：延迟动作确已收割的观测点（is_disposed 由弃树置位）
    let old_tree = store.range_index().get_tree(b"idx").unwrap();

    store.flush_database(0, 0).await?;

    // 长读会话钉住旧纪元 → 驱动消费位登记延迟动作：动作在册但收割被阻塞，
    // 排空窗口确定性打开（驱动须在守卫之外，见 LightEpoch 调用方契约注释）
    let reader = store.new_session()?;
    let guard = reader.participant().enter();
    {
      let store = Arc::clone(&store);
      spawn(move || store.drain_bftree_release(usize::MAX))
        .join()
        .unwrap();
    }

    // 窗口内同名重建：注册表重登记、旧工件被建树路径 unlink 后重建新文件
    s0.set_context(0, 0);
    s0.range_index_create(b"idx", StorageBackendType::Disk, RI_TUNE)
      .await?;
    s0.range_index_set(b"idx", b"new_field", b"value1").await?;
    assert!(data_path.exists(), "新世代数据文件须在盘");

    // 读者退场、纪元排空：延迟动作真正执行 unlink 的时点
    drop(guard);
    drop(reader);
    release_settled(&store).await?;

    assert!(
      old_tree.is_disposed(),
      "前置条件未成立：延迟释放动作未收割，本例的 unlink 时点断言无意义"
    );
    assert!(
      data_path.exists(),
      "登记在先、同名重建落在排空窗口内的世代，unlink 时点复查必须拦下删除"
    );
    assert_eq!(
      s0.range_index_get(b"idx", b"new_field").await?,
      Some(b"value1".to_vec()),
      "新世代索引数据须完好"
    );

    info!("排空窗口内同名重建的 unlink 时点世代守卫测试通过");
    OK
  })
}

/// 活跃长读事务下换号臂照常返回（复制位点可推进），物理释放推迟至纪元
/// 排空之后：长读钉住旧纪元期间驱动后台消费位，文件删除不得发生；
/// 读者退场后经纪元屏障收敛删除。
///
/// 释放驱动必须在长读守卫之外（独立线程，对位生产 spawn_bftree_reclaimer
/// 的独立后台任务形态）：LightEpoch 调用方契约——驱动线程不得持旧纪元守卫，
/// 对标 C# libs/storage/Tsavorite/cs/src/core/Epochs/LightEpoch.cs:
/// BumpCurrentEpoch(Action) 收尾的 ProtectAndDrain 会把调用线程自身保护条目
/// 刷新至最新纪元，同线程驱动等于自解除长读保护（wepoch
/// action_runs_immediately_when_nobody_else_is_protected 同款语义）
#[test]
fn test_flush_reclaim_deferred_until_epoch_drain() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempfile::tempdir()?;
    let store = open_ri_store(&dir, "flush_ri_longread.db")?;
    let s0 = store.new_session()?;
    s0.set_context(0, 0);
    s0.range_index_create(b"idx", StorageBackendType::Disk, RI_TUNE)
      .await?;
    s0.range_index_set(b"idx", b"field", b"value").await?;
    let data_path = store.range_index().data_file_path_for_key(b"idx");

    // 活跃长读会话：纪元保护在册，模拟在途读事务钉住旧安全回收纪元
    let reader = store.new_session()?;
    let guard = reader.participant().enter();

    store.flush_database(0, 0).await?;
    // 后台释放任务线程驱动消费位（独立于长读守卫线程，见测试级文档注释）
    {
      let store = Arc::clone(&store);
      spawn(move || store.drain_bftree_release(usize::MAX))
        .join()
        .unwrap();
    }
    assert!(
      data_path.exists(),
      "长读事务在册期间物理释放必须未落地（等待本地纪元排空）"
    );

    // 读者退场后收割收敛（参与者进出推进纪元）
    drop(guard);
    drop(reader);
    wait_file_gone(&data_path, &store).await?;

    info!("长读事务下换号异步释放推迟测试通过");
    OK
  })
}

/// FLUSHNS 换号联动 RI 树回收：整命名空间旧域树文件被延迟销毁，同名重建可成功
#[test]
fn test_flush_namespace_reclaims_range_index_tree() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempfile::tempdir()?;
    let store = open_ri_store(&dir, "flushns_ri_reclaim.db")?;
    let s1 = store.new_session()?;
    s1.set_context(1, 0);
    s1.range_index_create(b"idx", StorageBackendType::Disk, RI_TUNE)
      .await?;
    s1.range_index_set(b"idx", b"field", b"value").await?;
    let data_path = store.range_index().data_file_path_for_key(b"idx");
    assert!(data_path.exists(), "RI 数据文件须在盘");

    store.flush_namespace(1).await?;
    wait_file_gone(&data_path, &store).await?;

    // 新命名空间世代同名重建可成功且为空树
    s1.set_context(1, 0);
    s1.range_index_create(b"idx", StorageBackendType::Disk, RI_TUNE)
      .await?;
    assert!(
      s1.range_index_get(b"idx", b"field").await?.is_none(),
      "重建后须为空树"
    );

    info!("FLUSHNS 联动 RI 树回收测试通过");
    OK
  })
}

/// RI.DEL（删空自愈）注销旁表登记后，FLUSHDB 不再持有该键，同名重建可成功
#[test]
fn test_ri_delete_unregisters_flush_reclaim_entry() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempfile::tempdir()?;
    let store = open_ri_store(&dir, "ri_del_reclaim.db")?;
    let s0 = store.new_session()?;
    s0.set_context(0, 0);
    s0.range_index_create(b"idx", StorageBackendType::Disk, RI_TUNE)
      .await?;

    // 删除面唯一收敛点：注销换号回收旁表登记并销毁树与数据文件（删键臂随键清 TTL）
    s0.handle_bftree_drain_and_delete(b"idx", false).await?;
    let data_path = store.range_index().data_file_path_for_key(b"idx");
    wait_file_gone(&data_path, &store).await?;

    store.flush_database(0, 0).await?;

    s0.set_context(0, 0);
    s0.range_index_create(b"idx", StorageBackendType::Disk, RI_TUNE)
      .await?;
    info!("RI.DEL 注销后 FLUSHDB 同名重建测试通过");
    OK
  })
}

/// 升阶注册面经会话一参收口（session::register_bftree_key 单点自取会话当前域）：
/// promote_collection_to_bftree 登记后，异库 FLUSHDB 不得回收他域键（数据文件与
/// 在线树原样保留——域取错即旁表按域取走失配，此处双向变红），本域换号才联动
/// 摘除并延迟销毁；同名再升阶可成功（登记缺失则升阶树残留 live_indexes，
/// create_bftree 撞 IndexExists 拦截）
#[test]
fn test_promote_bftree_registers_via_session_port() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempfile::tempdir()?;
    let store = open_ri_store(&dir, "promote_reclaim_port.db")?;

    // 库 1 会话升阶集合为 BfTree 分页态：登记域须为会话当前域 (vns 0, 库 1 的 vdb)
    let s1 = store.new_session()?;
    s1.set_context(0, 1);
    s1.promote_collection_to_bftree(
      b"coll",
      GarnetObjectType::Hash,
      vec![(b"field1".to_vec(), b"value1".to_vec())],
      i64::MAX,
    )
    .await?;
    let coll_path = store.range_index().data_file_path_for_key(b"coll");
    assert!(coll_path.exists(), "升阶树数据文件须在盘");

    // 异库 FLUSHDB(0,0)：旁表按域取数，库 1 的登记不得被取走——升阶树与文件不动
    store.flush_database(0, 0).await?;
    assert!(
      coll_path.exists(),
      "FLUSHDB(0,0) 不得回收库 1 会话登记的升阶树（会话收口取域错位即此处变红）"
    );
    assert!(
      store.range_index().get_tree(b"coll").is_some(),
      "他域换号不得摘除本域在线树"
    );

    // 本域 FLUSHDB(0,1)：换号联动取走库 1 登记，同步摘注册 + 投递待释放队列
    store.flush_database(0, 1).await?;
    assert!(
      store.range_index().get_tree(b"coll").is_none(),
      "本域换号后升阶树须已摘除注册"
    );
    wait_file_gone(&coll_path, &store).await?;

    // 同名再升阶可成功（修复面：登记未随会话域进旁表时旧树残留注册，此处撞
    // IndexExists 上抛）且为新域数据
    s1.set_context(0, 1);
    s1.promote_collection_to_bftree(
      b"coll",
      GarnetObjectType::Hash,
      vec![(b"field2".to_vec(), b"value2".to_vec())],
      i64::MAX,
    )
    .await?;
    assert!(coll_path.exists(), "重建升阶树数据文件须在盘");
    info!("升阶旁表登记会话一参收口测试通过");
    OK
  })
}

/// 跨租户 RI 隔离：租户 A（库 1）在用 RI 树不受租户 B（库 0）FLUSHDB 波及，
/// B 域换号回收照常生效且同名可重建
///
/// 回归收敛点：换号路径严禁追加全局 range_index.clear_all——其摘除全部域
/// 注册并删除全部树文件与检查点快照，他域活树文件被删后经惰性激活重建为
/// 空树，索引数据静默清零；该调用回潮时本测试在 A 域数据文件存在性断言
/// 处即变红（已实测反证）
#[test]
fn test_flush_database_cross_tenant_ri_isolation() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempfile::tempdir()?;
    let store = open_ri_store(&dir, "flush_cross_tenant_ri.db")?;

    // 租户 A：库 1 建 RI 树并写入数据
    let sa = store.new_session()?;
    sa.set_context(0, 1);
    sa.range_index_create(b"idx", StorageBackendType::Disk, RI_TUNE)
      .await?;
    sa.range_index_set(b"idx", b"field", b"value").await?;
    let a_path = store.range_index().data_file_path_for_key(b"idx");
    assert!(a_path.exists(), "A 域 RI 数据文件须在盘");

    // 租户 B：库 0 建 RI 树（旁表按域回收的正向样本）
    let sb = store.new_session()?;
    sb.set_context(0, 0);
    sb.range_index_create(b"bidx", StorageBackendType::Disk, RI_TUNE)
      .await?;
    sb.range_index_set(b"bidx", b"field", b"bvalue").await?;
    let b_path = store.range_index().data_file_path_for_key(b"bidx");
    assert!(b_path.exists(), "B 域 RI 数据文件须在盘");

    // 租户 B FLUSHDB：仅回收库 0 旧域
    store.flush_database(0, 0).await?;

    // B 域树文件经纪元延迟删除收敛；A 域树文件必须原样在盘
    wait_file_gone(&b_path, &store).await?;
    assert!(
      a_path.exists(),
      "FLUSHDB(0,0) 不得波及租户 A 的 RI 数据文件"
    );

    // A 的 RI 命令全部正常，旧数据完好（全局 clear_all 回潮时此处清零变红）
    sa.set_context(0, 1);
    assert_eq!(
      sa.range_index_get(b"idx", b"field").await?,
      Some(b"value".to_vec()),
      "租户 A 的索引数据不得被 FLUSHDB(0,0) 清零"
    );
    sa.range_index_set(b"idx", b"field2", b"value2").await?;
    let mut scanned = Vec::new();
    let count = sa
      .range_index_scan_stream(b"idx", b"field", 10, ScanReturnField::Key, |k, _| {
        scanned.push(k.to_vec());
        true
      })
      .await?;
    assert_eq!(count, 2);
    assert_eq!(scanned, [b"field".to_vec(), b"field2".to_vec()]);

    // A 域 RI.DEL 后同名重建不受 B 清库影响
    sa.handle_bftree_drain_and_delete(b"idx", false).await?;
    wait_file_gone(&a_path, &store).await?;
    sa.range_index_create(b"idx", StorageBackendType::Disk, RI_TUNE)
      .await?;
    sa.range_index_set(b"idx", b"fresh", b"value9").await?;
    assert_eq!(
      sa.range_index_get(b"idx", b"fresh").await?,
      Some(b"value9".to_vec())
    );

    // B 域同名重建照常可成功且为空树（旁表按域回收职责未受收敛影响）
    sb.set_context(0, 0);
    sb.range_index_create(b"bidx", StorageBackendType::Disk, RI_TUNE)
      .await?;
    assert!(
      sb.range_index_get(b"bidx", b"field").await?.is_none(),
      "B 域重建须为空树"
    );

    info!("跨租户 FLUSHDB RI 隔离测试通过");
    OK
  })
}

/// 换号原子批「新映射 → 旧域墓碑 → 0x05 水位」落盘重启重建闭环（doc/zh/db.md
/// 即时原子提交段端到端固化）：FLUSHDB/FLUSHNS 换号后提交检查点，同 device 走
/// 引擎唯一的内容恢复入口（`WedbStore::recover`，恢复段单趟扫描内同趟重建），断言——旧域按退役角色恢复
/// （库级墓碑 vns=Some / 空间级墓碑 vns=None）、0x05 落盘水位令分配号不回落、
/// 旧域数据在新代域不可读（绝不复活混读）、重建后再换号取全新号与一切
/// 历史已分配号零撞
#[test]
fn test_flush_atomic_batch_survives_rebuild() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let cpr_dir = dir.path().join("checkpoints");
    create_dir_all(&cpr_dir)?;
    let device = Arc::new(SegmentedDevice::single_file(
      dir.path().join("flush_batch_rebuild.db"),
    )?);
    let (old_vdb, new_vdb, vns5, next_before);
    let cpr_token: u128;
    {
      let store = Arc::new(WedbStore::open(
        config(2048, 64 * 1024, 16)?,
        Arc::clone(&device),
      )?);
      // 根域库 1：写数据后 FLUSHDB 换号（首轮旧号即建档默认号）
      let s1 = store.new_session()?;
      s1.set_context(0, 1);
      s1.upsert(b"retired", b"payload").await?;
      let (_, pre_vdb) = store.vdb.get_virtual_ids(0, 1);
      let (r_vns, r_old) = store.flush_database(0, 1).await?;
      old_vdb = r_old;
      assert_eq!(r_vns, 0, "根域库换号返回域值 vns=0");
      assert_eq!(old_vdb, pre_vdb, "首轮 FLUSHDB 退役号即换号前在用号");
      let (_, nv) = store.vdb.get_virtual_ids(0, 1);
      new_vdb = nv;
      assert_ne!(old_vdb, new_vdb, "换号后新旧号必不同");

      // 命名空间 5 建档 + 写数据后 FLUSHNS 换号（空间级退役角色样本）
      let s5 = store.new_session()?;
      assert!(s5.set_context(5, 0), "新租户上下文物化");
      s5.upsert(b"ns5", b"payload").await?;
      let (ns5_vns, _) = store.vdb.get_virtual_ids(5, 0);
      vns5 = ns5_vns;
      assert!(vns5 > 0, "新租户分配非零 vns");
      store.flush_namespace(5).await?;

      next_before = store.vdb.next_virtual_id.load(Relaxed);
      // 提交检查点：重建扫描面须覆盖全批 DbMeta 记录
      cpr_token = store
        .create_checkpoint(&cpr_dir, CheckpointType::Snapshot)
        .await?
        .token;
      drop(s1);
      drop(s5);
      drop(store);
    }
    // 重启：检查点恢复段单趟扫描内完成 DbMeta 重建（原子批落盘 ⇒ 重建末态收敛）
    let store2 = Arc::new(WedbStore::recover(&cpr_dir, cpr_token, Arc::clone(&device)).await?);

    // 死亡账本按退役角色恢复：库级墓碑 vns=Some、空间级墓碑 vns=None
    let db_dead = store2.vdb.gc_dead.get(&old_vdb).expect("库级墓碑须恢复");
    assert_eq!(db_dead.vns, Some(0), "库级退役记录携 vns=Some 角色");
    assert!(store2.vdb.is_dead_domain(0, old_vdb), "旧库域判死");
    assert!(
      !store2.vdb.is_dead_domain(0, new_vdb),
      "换代新库域不得判死（角色比对误伤回归位）"
    );
    let ns_dead = store2.vdb.gc_dead.get(&vns5).expect("空间级墓碑须恢复");
    assert_eq!(ns_dead.vns, None, "空间级退役记录 vns=None 角色");
    assert!(store2.vdb.is_dead_ns(vns5), "旧空间判死");

    // 0x05 落盘水位与映射折叠取大：重建后分配水位不回落
    assert!(
      store2.vdb.next_virtual_id.load(Relaxed) >= next_before,
      "重启分配水位不得回落到历史已分配号之下（0x05 水位兜底）"
    );

    // 旧域数据不可读：库 1 新代域干净，绝不复活混读
    let s2 = store2.new_session()?;
    assert!(s2.set_context(0, 1), "重建后库映射已装载可物化");
    assert_eq!(
      s2.read(b"retired").await?,
      None,
      "退役旧域数据不得在新代库可读"
    );
    assert!(
      !store2
        .vdb
        .is_dead_domain(0, store2.vdb.get_virtual_ids(0, 1).1),
      "重建后库 1 指向在用的新代域"
    );

    // 重建后再换号：取严格大于一切历史号的全新号，零撞号
    let (r_vns2, retired_now) = store2.flush_database(0, 1).await?;
    assert_eq!((r_vns2, retired_now), (0, new_vdb), "二次换号退役首轮新号");
    let (_, nv2) = store2.vdb.get_virtual_ids(0, 1);
    assert!(
      nv2 > new_vdb && nv2 != old_vdb,
      "重建后新号不与任何历史号撞"
    );
    info!("换号原子批落盘重启重建测试通过");
    OK
  })
}

/// FLUSHDB 槽位级单元格真 O(1) 换号 + 落后代数会话零脏写（next/flushdb-number-swap-o1-cell.md 验收）：
/// 1. 根租户 1000 库全在册后换号 500 号库——旁观库的单元格 Arc 指针与指向
///    全部原样保留（单格换指零整表重建；写时克隆整表替换形态下每次换号都会
///    重排全部格对象，本断言封死该回归），目标库换指新号、旧号入死亡账本；
/// 2. 落后代数会话（缓存换号前上下文）的后续写入必经单元格重解析落进新域：
///    换出的旧物理域点查绝无该新键（零脏写），旧域生前数据保持完整待延时
///    GC——「换号即空、旧域只退役不改写」两端同时封死。
#[test]
fn test_flush_db_slot_o1_and_stale_session_no_dirty_write() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let env = open_store("flush_slot_o1.db", config(2048, 64 * 1024, 16)?)?;
    let store = env.store;
    const TARGET_DB: u64 = 500;

    // 根租户灌入 1000 库映射（纯内存原语建格，不写库数据）
    for db in 0..1000u64 {
      store.vdb.get_or_create_db(0, db);
    }
    let vns = store.vdb.get_or_create_ns(0).0;
    let routing = store.vdb.routing_for(vns);

    // 换号前快照：旁观库单元格 Arc 指针 + 全体指向 + 目标库旧指向
    let bystanders: Vec<(u64, usize, u64)> = (0..1000u64)
      .filter(|&db| db != TARGET_DB)
      .map(|db| {
        (
          db,
          Arc::as_ptr(&routing.table.cell_or_insert(db, 0)) as usize,
          store.vdb.route_vdb_of(vns, db).unwrap_or(0),
        )
      })
      .collect();
    let old_vdb = store
      .vdb
      .route_vdb_of(vns, TARGET_DB)
      .expect("目标库已在册");

    // 落后代数会话：缓存目标库换号前上下文（last_generation 钉死在此刻）
    let stale = store.new_session()?;
    assert!(stale.set_context(0, TARGET_DB), "目标库上下文物化");

    // FLUSHDB：内存换号段单格原子换指
    let (r_vns, r_old) = store.flush_database(0, TARGET_DB).await?;
    assert_eq!((r_vns, r_old), (vns, old_vdb), "换号退役旧号即换号前指向");
    let new_vdb = store.vdb.route_vdb_of(vns, TARGET_DB).expect("目标库在册");
    assert_ne!(new_vdb, old_vdb, "换号后目标库单元格指向新号");
    assert!(
      store.vdb.gc_dead.get(&old_vdb).is_some(),
      "旧库号须登记死亡账本"
    );

    // O(1) 结构证明：999 个旁观库单元格对象与指向零扰动——换号只触目标格，
    // 不存在任何整表重建/克隆路径
    for (db, cell_ptr, vdb) in &bystanders {
      assert_eq!(
        Arc::as_ptr(&routing.table.cell_or_insert(*db, 0)) as usize,
        *cell_ptr,
        "旁观库 db={db} 单元格对象必为换号前同一分配"
      );
      assert_eq!(
        store.vdb.route_vdb_of(vns, *db),
        Some(*vdb),
        "旁观库 db={db} 指向零漂移"
      );
    }

    // 落后代数会话写入：session_prefix 经代数落后慢路径重解析，必落新域
    stale.upsert(b"ghost", b"stale-write").await?;
    assert_eq!(
      stale.read(b"ghost").await?,
      Some(b"stale-write".to_vec()),
      "逻辑上下文重读落新域"
    );

    // 换出旧物理域零脏写：直设虚拟上下文点查旧域，绝无换号后的新键
    let old_domain = store.new_session()?;
    old_domain.set_virtual_context(vns, old_vdb);
    let ghost_key = old_domain.session_string_key(b"ghost");
    assert!(
      old_domain
        .read_raw_with(&ghost_key, |_| ())
        .await?
        .is_none(),
      "落后代数会话的写入绝不脏落换出旧域"
    );

    // 新物理域承接写入且旧域生前数据保持完整（只退役、不改写、不复活混读）
    let new_domain = store.new_session()?;
    new_domain.set_virtual_context(vns, new_vdb);
    let ghost_key = new_domain.session_string_key(b"ghost");
    assert!(
      new_domain
        .read_raw_with(&ghost_key, |_| ())
        .await?
        .is_some(),
      "换号后写入落新域"
    );

    // 内存换号段基准（验收「千库租户 < 1 微秒」口径）：release 下逐次取
    // 最快单轮样本断言 1μs；debug 构建计时噪声大仅执行不裁决，结构性
    // O(1) 已由上方单元格指针零扰动断言封死
    let mut fastest = u64::MAX;
    for _ in 0..2000 {
      let t0 = Instant::now();
      let (nv, retired) = store.vdb.flush_db(0, TARGET_DB, 0, 0);
      let elapsed_ns = t0.elapsed().as_nanos() as u64;
      assert!(retired.is_some(), "重复换号链式退役上一代号");
      assert_eq!(
        store.vdb.route_vdb_of(vns, TARGET_DB),
        Some(nv),
        "换号后目标格恒指向最新换入号"
      );
      fastest = fastest.min(elapsed_ns);
    }
    if cfg!(not(debug_assertions)) {
      assert!(
        fastest < 1_000,
        "千库租户内存换号段最快单轮 {fastest}ns，须 < 1μs（doc/zh/db.md 1.3）"
      );
    }

    info!("FLUSHDB 槽位单元格 O(1) 换号与落后会话零脏写测试通过");
    OK
  })
}

/// FLUSHDB 换号批落盘 → 重启重建闭环（doc/zh/db.md「即时原子提交」段
/// 端到端固化）：写数据 → flush_database 换号 → 提交检查点 → 同 device 走
/// 引擎唯一的内容恢复入口（`WedbStore::recover`）重建，断言——逻辑库按盘上新映射解析到换号后新域
/// （旧域数据不可达不可读）、gc_dead 账本按 0x04 墓碑原样恢复（退役角色
/// vns=Some 精确还原，同空间活域不误伤）、分配水位按 0x05 落盘水位折叠
/// 只抬不回退（重建后再 FLUSHDB 取严格新高号，绝不与磁盘在用域撞号）
#[test]
fn test_flush_database_batch_survives_rebuild() -> Void {
  const LOGIC_NS: u64 = 9;
  const LOGIC_DB: u64 = 1;
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let cpr_dir = dir.path().join("checkpoints");
    create_dir_all(&cpr_dir)?;
    let device = Arc::new(SegmentedDevice::single_file(
      dir.path().join("flush_rebuild.db"),
    )?);
    let (old_vns, old_vdb, new_vdb, watermark, dead_expired_at, dead_tail);
    let cpr_token: u128;
    {
      let store = Arc::new(WedbStore::open(
        config(2048, 64 * 1024, 16)?,
        Arc::clone(&device),
      )?);
      let session = store.new_session()?;
      session.set_context(LOGIC_NS, LOGIC_DB);
      session.upsert(b"flush_k", b"flush_v").await?;
      old_vns = session.active_vns.load(Relaxed);
      old_vdb = session.active_vdb.load(Relaxed);

      // 换号批成组落盘：新映射 + 0x04 退役墓碑 + 0x05 分配水位（safe-order）
      store.flush_database(LOGIC_NS, LOGIC_DB).await?;
      session.set_context(LOGIC_NS, LOGIC_DB);
      new_vdb = session.active_vdb.load(Relaxed);
      assert_ne!(old_vdb, new_vdb, "FLUSHDB 须换新域号");
      watermark = store.vdb.next_virtual_id.load(Relaxed);
      let dead = store
        .vdb
        .gc_dead
        .get(&old_vdb)
        .expect("换号须登记旧域死亡账本");
      assert_eq!(dead.vns, Some(old_vns), "库级退役角色为 vns=Some");
      dead_expired_at = dead.expired_at;
      dead_tail = dead.tail_address;

      // 旧域数据经逻辑库不可达（路由已换指新域）
      assert_eq!(session.read(b"flush_k").await?, None, "旧域数据不可读");

      // 提交检查点：重建扫描面须覆盖到换号批全部记录
      cpr_token = store
        .create_checkpoint(&cpr_dir, CheckpointType::Snapshot)
        .await?
        .token;
      drop(session);
      drop(store);
    }

    // 重启：检查点恢复段单趟扫描内完成换号批重建
    let store2 = Arc::new(WedbStore::recover(&cpr_dir, cpr_token, Arc::clone(&device)).await?);

    // ns 标量基线按 0x01 记录还原，逻辑库点查命中盘上新映射（绝不另起新号）
    assert_eq!(
      store2.vdb.ns_map.pin().get(&LOGIC_NS).copied(),
      Some(old_vns),
      "重建须还原磁盘 NS_MAP"
    );
    let (r_vns, r_vdb) = store2.resolve_context(LOGIC_NS, LOGIC_DB).await?;
    assert_eq!(
      (r_vns, r_vdb),
      (old_vns, new_vdb),
      "点查装载命中换号批新映射，绝不回退旧域"
    );

    // gc_dead 账本按 0x04 墓碑原样恢复：到期时刻、截断线与退役角色全量一致
    let dead = store2
      .vdb
      .gc_dead
      .get(&old_vdb)
      .expect("重建须恢复旧域死亡账本");
    assert_eq!(dead.vns, Some(old_vns), "退役角色恢复（库级 vns=Some）");
    assert_eq!(dead.expired_at, dead_expired_at, "到期时刻原样恢复");
    assert_eq!(dead.tail_address, dead_tail, "截断线原样恢复");

    // 退役角色版判定：旧域判死、同空间活域（新号）不因裸 id 碰撞误伤
    assert!(
      store2.vdb.is_dead_domain(old_vns, old_vdb),
      "旧域须判死（紧缩跳过承接）"
    );
    assert!(
      !store2.vdb.is_dead_domain(old_vns, new_vdb),
      "同空间活域不得误判死亡"
    );

    // 分配水位按 0x05 落盘水位折叠：只抬不回退，越过全部历史在用号
    let watermark2 = store2.vdb.next_virtual_id.load(Relaxed);
    assert!(
      watermark2 >= watermark && watermark2 > old_vns.max(old_vdb).max(new_vdb),
      "重建水位须 >= 0x05 落盘值且越过全部在用号: before={watermark} after={watermark2}"
    );

    // 重建后再 FLUSHDB：新号严格越过水位（与磁盘在用域零撞），且旧域数据
    // 仍不可达（重建不复活）
    let session2 = store2.new_session()?;
    assert!(session2.set_context(LOGIC_NS, LOGIC_DB), "装载后上下文物化");
    assert_eq!(session2.read(b"flush_k").await?, None, "重建不复活旧域数据");
    session2.upsert(b"after_k", b"after_v").await?;
    assert_eq!(session2.read(b"after_k").await?, Some(b"after_v".to_vec()));
    drop(session2);
    store2.flush_database(LOGIC_NS, LOGIC_DB).await?;
    let s = store2.new_session()?;
    s.set_context(LOGIC_NS, LOGIC_DB);
    let new_vdb2 = s.active_vdb.load(Relaxed);
    drop(s);
    assert!(
      new_vdb2 >= watermark2 && new_vdb2 != old_vdb && new_vdb2 != new_vdb,
      "重建后换号取严格新高号（撞号即旧域幽灵复活）: new={new_vdb2} watermark={watermark2}"
    );
    drop(store2);

    info!("FLUSHDB 换号批落盘重建闭环测试通过");
    OK
  })
}
