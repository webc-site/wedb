//! GEO STORE 族目的键分层态清退回归（票 zcode-r18-geo 发现一 / 发现二）
//!
//! 发现一：GEOSEARCHSTORE / GEORADIUS STORE 族目的键原为分层态 zset 时，
//! 快路径信封写对 Meta 域盲写被 `obj_load_custom_sync` Meta 分层闸遮蔽
//! （已 ACK 写入不可见 + 孤儿树泄漏）；收口为同步臂 `SyncStoreWindow::begin`
//! 对 Meta 域拒写降级（r15-zset 单点）+ 慢路径收尾并回 zset 既有单点
//! `store_dest_cold`（persist 清 TTL → rmw 窗跨「域快照 → 落笔复验 → 信封
//! 写回/删空回收」→ 窗释放后 `retire_tiered_dest` 按结果分流清退树态残留，
//! `keep_ttl` 适配 r15-zset 分流语义）。判据对齐 `zset_r15_parity.rs`：
//! 回执即可见、Meta 存根消亡、信封接管、AOF 回放主从一致。
//!
//! 发现二（doc/zh/deviations.md 第 19 条）：GEOADD XX 缺失键与 STORE 族
//! 0 命中不创建空键（C# InitialUpdater 残留空对象为上游缺陷，Rust 对齐
//! Redis 恒不创建），锁用例防按 C# 改回。

use std::sync::Arc;

use aok::Void;
use compio::runtime::Runtime;
use tempfile::{TempDir, tempdir};
use waof::{WalConfig, WalLog};
use wcol::{geo::geo_hash::GeoHash, types::member_ttl::encode_member};
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::{
  resp::{
    garnet_api::{GarnetApi, StoreGarnetApi},
    resp_server_session::{RespServerSession, RespServerSessionOptions},
  },
  service::NodeService,
};
use wnode_test::auto_exec;
use wresp::command::RespCommand;
use wval::{GarnetObjectType, KeyTag};

type TestStore = WedbStore<SegmentedDevice>;

/// 主/副本各一套：存储 + WAL（range_index_dir 落树文件，副本发布必需）
struct Node {
  store: Arc<TestStore>,
  service: NodeService<SegmentedDevice>,
  wal: Arc<WalLog<SegmentedDevice>>,
  _dir: TempDir,
}

fn open_node(tag: &str) -> aok::Result<Node> {
  let dir = tempdir()?;
  let store_device = Arc::new(SegmentedDevice::single_file(
    dir.path().join(format!("{tag}.db")),
  )?);
  let wal_device = Arc::new(SegmentedDevice::single_file(
    dir.path().join(format!("{tag}.wal")),
  )?);
  let mut config = StoreConfig::new(2048, 1024 * 1024, 16, 0.5)?;
  config.range_index_dir = Some(dir.path().to_path_buf());
  let store = Arc::new(WedbStore::open(config, store_device)?);
  let wal = Arc::new(WalLog::new(wal_device, WalConfig::default())?);
  let service = NodeService::with_wal(Arc::clone(&store), Arc::clone(&wal))?;
  Ok(Node {
    store,
    service,
    wal,
    _dir: dir,
  })
}

/// 无 WAL 单存储环境（命令面判据用）
fn open_env(tag: &str) -> (Runtime, GarnetApi, Arc<TestStore>, TempDir) {
  let dir = tempdir().unwrap();
  let config = StoreConfig::new(2048, 1024 * 1024, 16, 0.5).unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag)).unwrap());
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  (
    Runtime::new().unwrap(),
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
    store,
    dir,
  )
}

fn api_of(node: &Node) -> aok::Result<GarnetApi> {
  Ok(Arc::new(StoreGarnetApi::new(node.store.new_session()?)))
}

fn session_with(api: &GarnetApi) -> RespServerSession {
  let mut s = RespServerSession::new(1, RespServerSessionOptions::default());
  s.set_garnet_api(api.clone());
  s
}

/// 手工升阶（与生产 export_entries 同形 entries：成员 + encode_member 记录）
async fn promote_zset(store: &Arc<TestStore>, key: &[u8], members: &[(&[u8], f64)]) -> Void {
  let ents: Vec<(Vec<u8>, Vec<u8>)> = members
    .iter()
    .map(|(m, s)| (m.to_vec(), encode_member(&s.to_be_bytes(), None)))
    .collect();
  let sess = store.new_session().unwrap();
  sess
    .promote_collection_to_bftree(key, GarnetObjectType::SortedSet, ents, i64::MAX, false)
    .await?;
  Ok(())
}

/// 物理域取证：指定 tag 的物理记录是否在场（双域残留 / 清退闭环判据）
async fn record_present(store: &Arc<TestStore>, tag: KeyTag, key: &[u8]) -> bool {
  let sess = store.new_session().unwrap();
  let rec_k = sess.session_tag_key(tag, key);
  sess.read_raw(&rec_k).await.unwrap().is_some()
}

/// 双成员 geo 源（palermo/catania，C# RespSortedSetGeoTests 官方坐标对）
fn seed_geo_src(api: &GarnetApi, rt: &Runtime, s: &mut RespServerSession, src: &[u8]) {
  assert_eq!(
    auto_exec(
      api,
      rt,
      s,
      RespCommand::Geoadd,
      &[
        src,
        b"13.361389",
        b"38.115556",
        b"palermo",
        b"15.087269",
        b"37.309",
        b"catania"
      ]
    ),
    b":2\r\n"
  );
}

/// GEOSEARCHSTORE dst src FROMLONLAT 13.361389 38.115556 BYRADIUS 200 km ASC
/// （圆心 palermo，200km 双命中）
fn geosearchstore_exec(
  api: &GarnetApi,
  rt: &Runtime,
  s: &mut RespServerSession,
  dst: &[u8],
  src: &[u8],
) -> Vec<u8> {
  auto_exec(
    api,
    rt,
    s,
    RespCommand::Geosearchstore,
    &[
      dst,
      src,
      b"FROMLONLAT",
      b"13.361389",
      b"38.115556",
      b"BYRADIUS",
      b"200",
      b"km",
      b"ASC",
    ],
  )
}

/// 主从命令面逐帧一致（AOF 回放收敛判据；回放后为信封态，命令段直出）
fn assert_master_replica_parity(rt: &Runtime, node_p: &Node, node_r: &Node, key: &[u8]) {
  let papi = api_of(node_p).unwrap();
  let mut ps = session_with(&papi);
  let rapi = api_of(node_r).unwrap();
  let mut rs = session_with(&rapi);
  for (cmd, args) in [
    (RespCommand::Exists, vec![key]),
    (RespCommand::Zcard, vec![key]),
    (
      RespCommand::Zrange,
      vec![key, b"0" as &[u8], b"-1", b"WITHSCORES"],
    ),
  ] {
    let on_primary = auto_exec(&papi, rt, &mut ps, cmd, &args);
    let on_replica = auto_exec(&rapi, rt, &mut rs, cmd, &args);
    assert_eq!(
      on_primary, on_replica,
      "命令 {cmd} 主从不一致（回放面树残留/信封缺口）"
    );
  }
}

/// ---- 发现一：树态 dst 的 GEOSEARCHSTORE 回执即可见 + 单域闭环 + AOF 回放一致 ----
#[test]
fn geosearchstore_tiered_dest_retired_visible_and_replay_consistent() -> Void {
  let rt = Runtime::new()?;
  let primary = open_node("geo-retire-primary")?;
  let papi = api_of(&primary)?;
  let mut ps = session_with(&papi);

  seed_geo_src(&papi, &rt, &mut ps, b"src");
  // dst 预置旧成员后升阶（修复前旧树经 Meta 分层闸遮蔽新信封，ZRANGE 永回 [old]）
  rt.block_on(promote_zset(
    &primary.store,
    b"dst",
    &[(b"old" as &[u8], 9.0)],
  ))?;
  assert!(
    rt.block_on(record_present(&primary.store, KeyTag::Meta, b"dst")),
    "fixture：dst 应为分层树态（元记录在册）"
  );

  // 同步臂 begin 拒写 Meta 域 → 慢路径 store_dest_cold 闭环回 :2
  assert_eq!(
    geosearchstore_exec(&papi, &rt, &mut ps, b"dst", b"src"),
    b":2\r\n"
  );
  // 回执即可见新内容（不被旧树遮蔽；修复前此处读回旧树 [old]）
  assert_eq!(
    auto_exec(
      &papi,
      &rt,
      &mut ps,
      RespCommand::Zrange,
      &[b"dst", b"0", b"-1"]
    ),
    b"*2\r\n$7\r\npalermo\r\n$7\r\ncatania\r\n"
  );
  assert_eq!(
    auto_exec(&papi, &rt, &mut ps, RespCommand::Zcard, &[b"dst"]),
    b":2\r\n"
  );
  // 单域闭环：Meta 存根 + 旧树清退（keep_ttl=true 保信封），信封接管
  assert!(
    !rt.block_on(record_present(&primary.store, KeyTag::Meta, b"dst")),
    "GEOSEARCHSTORE 后分层存根残留：retire_tiered_dest 未闭环"
  );
  assert!(
    rt.block_on(record_present(
      &primary.store,
      KeyTag::ObjectEnvelope,
      b"dst"
    )),
    "GEOSEARCHSTORE 后信封记录缺席：慢路径写回未落库"
  );

  // AOF 回放：副本收敛后命令面逐帧一致（信封接管 + Meta 清退两跳条目齐全）
  rt.block_on(primary.wal.commit())?;
  let replica = open_node("geo-retire-replica")?;
  let replayed = rt.block_on(
    primary
      .service
      .replay_into_session(replica.service.session()),
  )?;
  assert!(replayed > 0, "副本应消费到 STORE 族条目");
  assert_master_replica_parity(&rt, &primary, &replica, b"dst");
  Ok(())
}

/// ---- 发现一：GEORADIUS … STORE dst 词形同 face 同闭环 ----
#[test]
fn georadius_store_tiered_dest_retired() -> Void {
  let (rt, api, store, _dir) = open_env("geo-radius-store.db");
  let mut s = session_with(&api);

  seed_geo_src(&api, &rt, &mut s, b"rsrc");
  rt.block_on(promote_zset(&store, b"rdst", &[(b"old" as &[u8], 9.0)]))?;

  // GEORADIUS src lon lat 200 km ASC STORE dst：同走 geo_search_commands store 臂
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Georadius,
      &[
        b"rsrc",
        b"13.361389",
        b"38.115556",
        b"200",
        b"km",
        b"ASC",
        b"STORE",
        b"rdst"
      ]
    ),
    b":2\r\n"
  );
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Zrange,
      &[b"rdst", b"0", b"-1"]
    ),
    b"*2\r\n$7\r\npalermo\r\n$7\r\ncatania\r\n",
    "GEORADIUS STORE 写入被旧树遮蔽"
  );
  assert!(
    !rt.block_on(record_present(&store, KeyTag::Meta, b"rdst")),
    "GEORADIUS STORE 后分层存根残留"
  );
  assert!(rt.block_on(record_present(&store, KeyTag::ObjectEnvelope, b"rdst")));
  Ok(())
}

/// ---- 发现一：dest == src 同键树态原地重写（升阶源物化 + 接管 + 清退一次闭环）----
#[test]
fn geosearchstore_dest_eq_src_tiered_in_place() -> Void {
  let (rt, api, store, _dir) = open_env("geo-dest-eq-src.db");
  let mut s = session_with(&api);

  // src == dst 直接建树态：成员分值必须为 GEOADD 同款 geohash 52bit 编码
  //（geo_search 按 geohash 解码坐标求值，裸浮点分值解码不出合法圆心距）；
  // old 为圈外旧值残留
  rt.block_on(promote_zset(
    &store,
    b"geo",
    &[
      (b"old" as &[u8], 7.0),
      (
        b"palermo",
        GeoHash::geo_to_long_value(38.115556, 13.361389) as f64,
      ),
      (
        b"catania",
        GeoHash::geo_to_long_value(37.309, 15.087269) as f64,
      ),
    ],
  ))?;
  // 源装载走分层物化，求值后 store_dest_cold 原地接管：信封换域存活、树清退
  assert_eq!(
    geosearchstore_exec(&api, &rt, &mut s, b"geo", b"geo"),
    b":2\r\n"
  );
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Zrange,
      &[b"geo", b"0", b"-1"]
    ),
    b"*2\r\n$7\r\npalermo\r\n$7\r\ncatania\r\n",
    "同键原地重写被旧树遮蔽"
  );
  assert!(
    !rt.block_on(record_present(&store, KeyTag::Meta, b"geo")),
    "同键重写后分层存根残留"
  );
  assert!(rt.block_on(record_present(&store, KeyTag::ObjectEnvelope, b"geo")));
  Ok(())
}

/// ---- 发现一：源缺失 + 树态 dst → 空结果臂两域齐清（修复前 delete_string
/// 双域删留 Meta+树，读路径旧数据复活）----
#[test]
fn geosearchstore_src_missing_tiered_dest_fully_retired() -> Void {
  let (rt, api, store, _dir) = open_env("geo-src-missing.db");
  let mut s = session_with(&api);

  rt.block_on(promote_zset(&store, b"mdst", &[(b"old" as &[u8], 9.0)]))?;
  assert!(rt.block_on(record_present(&store, KeyTag::Meta, b"mdst")));

  // 源缺失：同步臂 begin 拒写降级 → 慢路径 store_dest_cold 空结果臂删空回收
  // 回 :0（C# EXPIRE(destination, 0)）
  assert_eq!(
    geosearchstore_exec(&api, &rt, &mut s, b"mdst", b"ghost"),
    b":0\r\n"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Exists, &[b"mdst"]),
    b":0\r\n"
  );
  // 整键消亡：两域齐清（keep_ttl=false），无幽灵元记录与孤儿树
  assert!(
    !rt.block_on(record_present(&store, KeyTag::Meta, b"mdst")),
    "源缺失清退后分层存根残留"
  );
  assert!(
    !rt.block_on(record_present(&store, KeyTag::ObjectEnvelope, b"mdst")),
    "源缺失清退后信封记录残留"
  );
  Ok(())
}

/// ---- 发现二锁用例（deviations.md 第 19 条）：GEOADD XX 缺失键不创建 ----
#[test]
fn geoadd_xx_missing_key_leaves_no_key() -> Void {
  let (rt, api, _store, _dir) = open_env("geoadd-xx.db");
  let mut s = session_with(&api);

  // C# InitialUpdater 忽略空集 RemoveKey：GEOADD XX 缺失键残留空 zset（EXISTS=1）；
  // Rust 对齐 Redis 恒 :0 且键不存在
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Geoadd,
      &[b"gk", b"XX", b"13.361389", b"38.115556", b"palermo"]
    ),
    b":0\r\n"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Exists, &[b"gk"]),
    b":0\r\n"
  );
  Ok(())
}

/// ---- 发现二锁用例（deviations.md 第 19 条）：STORE 族 0 命中不创建 ----
#[test]
fn geosearchstore_zero_hit_leaves_no_key() -> Void {
  let (rt, api, _store, _dir) = open_env("geo-zero-hit.db");
  let mut s = session_with(&api);

  seed_geo_src(&api, &rt, &mut s, b"zsrc");
  // 圆心远海半径 1km：0 命中 → :0 且键不创建（C# 删旧后 RMW 空集残留空键）
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Geosearchstore,
      &[
        b"zdst",
        b"zsrc",
        b"FROMLONLAT",
        b"100.0",
        b"0.0",
        b"BYRADIUS",
        b"1",
        b"km",
        b"ASC"
      ]
    ),
    b":0\r\n"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Exists, &[b"zdst"]),
    b":0\r\n"
  );
  Ok(())
}
