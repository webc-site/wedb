//! 懒降阶（tiered demote）AOF / 副本回放端到端集成测试
//! （task/ing/lazy-demote-replica-replay-ttl.md）
//!
//! 判据取 doc/zh/collection.md 3.3「惰性降阶 + 删空自愈」与第 5 节「RESP 应答
//! 透明」：降阶是换域而非换命，信封内容与随键 TTL 全程保持，且对外（含副本与
//! 重启回放面）完全透明。
//!
//! 降阶在盘上是两跳：obj_save 整值写回信封（ObjectStoreUpsert 条目）→ 树清退
//! 元记录（RangeIndexDrop → StoreDelete(Meta) 条目）。修复前副本回放第二跳走
//! 全键级联删臂（delete_string → wkv delete），把上一条目刚落的信封连同随键
//! TTL 一并抹掉——主端存活、副本整键消失、TTL 脱落，主从长期发散；修复后副本
//! 逐域收敛，Meta 条目只以 keep_ttl=true 形排空 Meta 域。
//!
//! 断言面：副本 EXISTS / TYPE / HLEN / HGET / HGETALL 与主端逐帧一致，随键 TTL
//! 主从同刻度，两侧信封记录在册、元记录与树存根双双消亡（无幽灵分层态）。

use std::sync::Arc;

use aok::{OK, Void};
use compio::runtime::Runtime;
use tempfile::tempdir;
use waof::{WalConfig, WalLog};
use wbase::time::now_ticks;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::{
  resp::{
    garnet_api::{GarnetApi, StoreGarnetApi},
    objects::tiered_demote::{TieredDemoteStats, tiered_demote_round},
    resp_server_session::{RespServerSession, RespServerSessionOptions},
  },
  service::NodeService,
};
use wnode_test::auto_exec;
use wresp::command::RespCommand;
use wval::{GarnetObjectType, KeyTag};

/// 主/副本各一套：存储 + WAL（range_index_dir 落树文件，副本发布必需）
struct Node {
  store: Arc<WedbStore<SegmentedDevice>>,
  service: NodeService<SegmentedDevice>,
  wal: Arc<WalLog<SegmentedDevice>>,
  _dir: tempfile::TempDir,
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

fn api_of(store: &Arc<WedbStore<SegmentedDevice>>) -> aok::Result<GarnetApi> {
  Ok(Arc::new(StoreGarnetApi::new(store.new_session()?)))
}

fn session_with(api: &GarnetApi) -> RespServerSession {
  let mut s = RespServerSession::new(1, RespServerSessionOptions::default());
  s.set_garnet_api(api.clone());
  s
}

/// 分层态判据：BfTree 元记录存根在册（wkv load_collection_stub 权威读）
async fn is_tiered(node: &Node, key: &[u8]) -> aok::Result<bool> {
  Ok(
    node
      .store
      .new_session()?
      .load_collection_stub(key)
      .await?
      .is_some(),
  )
}

/// 懒降阶两跳回放：副本必须与主端同态（信封存活 + 随键 TTL 保持）
///
/// fixture 走 wkv promote 直灌（建树臂零降阶评估，与
/// tiered_background_demote.rs 的冷候选构造同源）：22000 字段 × 1B 值，条目维
/// 与体积维双低于紧缩水位，前台无写触碰，故只能由后台评估轮降阶——正是本票
/// 「懒降阶 + 副本回放」的形
#[test]
fn lazy_demote_replay_keeps_envelope_and_ttl_on_replica() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let total = 22_000usize;

    let primary = open_node("demote_primary")?;
    let papi = api_of(&primary.store)?;
    let mut ps = session_with(&papi);

    let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..total)
      .map(|i| (format!("f{i}").into_bytes(), b"v".to_vec()))
      .collect();
    primary
      .store
      .new_session()?
      .promote_collection_to_bftree(b"h", GarnetObjectType::Hash, entries, i64::MAX, false)
      .await?;
    assert!(
      is_tiered(&primary, b"h").await?,
      "fixture：主端应为分层树态（元记录在册）"
    );

    // 随键 TTL：EXPIRE 落 TTL 旁路记录，经 TtlWrite 分流为 PEXPIREAT 确定性条目
    assert_eq!(
      auto_exec(&papi, &rt, &mut ps, RespCommand::Expire, &[b"h", b"3600"]),
      b":1\r\n",
      "分层态键 EXPIRE 应成功"
    );

    // 后台评估轮降阶：obj_save 写回信封 + 树清退，两跳先后入账
    let stats = tiered_demote_round(&primary.store).await;
    assert_eq!(
      stats,
      TieredDemoteStats {
        candidates: 1,
        demoted: 1,
        aborted: 0
      },
      "冷分层键应被评估轮降阶"
    );
    assert!(
      !is_tiered(&primary, b"h").await?,
      "主端降阶后元记录与树存根应释放"
    );
    assert_eq!(
      auto_exec(&papi, &rt, &mut ps, RespCommand::Exists, &[b"h"]),
      b":1\r\n",
      "主端降阶后键全程存活"
    );
    primary.wal.commit().await?;

    // 副本：全新引擎实例 + 独立 WAL，只经回放链路收敛
    let replica = open_node("demote_replica")?;
    let replayed = primary
      .service
      .replay_into_session(replica.service.session())
      .await?;
    assert!(replayed > 0, "副本应消费到降阶两跳条目");

    // 命令面主从逐帧一致（修复前副本此四处全红：整键消失、TTL 脱落）
    let rapi = api_of(&replica.store)?;
    let mut rs = session_with(&rapi);
    for cmd in [RespCommand::Exists, RespCommand::Type, RespCommand::Hlen] {
      let on_primary = auto_exec(&papi, &rt, &mut ps, cmd, &[b"h"]);
      let on_replica = auto_exec(&rapi, &rt, &mut rs, cmd, &[b"h"]);
      assert_eq!(
        on_primary, on_replica,
        "命令 {cmd} 主从不一致（级联删臂回放即副本丢键）"
      );
    }
    assert_eq!(
      auto_exec(&rapi, &rt, &mut rs, RespCommand::Exists, &[b"h"]),
      b":1\r\n",
      "副本懒降阶回放后键必须存活"
    );
    assert_eq!(
      auto_exec(&rapi, &rt, &mut rs, RespCommand::Type, &[b"h"]),
      b"+hash\r\n"
    );
    assert_eq!(
      auto_exec(&rapi, &rt, &mut rs, RespCommand::Hlen, &[b"h"]),
      format!(":{total}\r\n").into_bytes(),
      "副本 HLEN 须与主端一致"
    );
    for field in [b"f0".as_slice(), b"f7", b"f21999"] {
      let on_primary = auto_exec(&papi, &rt, &mut ps, RespCommand::Hget, &[b"h", field]);
      let on_replica = auto_exec(&rapi, &rt, &mut rs, RespCommand::Hget, &[b"h", field]);
      assert_eq!(on_primary, on_replica, "字段 {field:?} 主从不一致");
      assert_eq!(on_primary, b"$1\r\nv\r\n", "抽样字段应可读且值保真");
    }
    let ph = auto_exec(&papi, &rt, &mut ps, RespCommand::Hgetall, &[b"h"]);
    let rh = auto_exec(&rapi, &rt, &mut rs, RespCommand::Hgetall, &[b"h"]);
    assert_eq!(ph, rh, "HGETALL 全量条目须与主端逐字节一致");

    // 随键 TTL 同刻度（绝对 ticks，毫秒粒度对齐镜像往返）：副本脱落即 -2/None
    let pexp = primary.store.new_session()?.ttl_of(b"h").await?;
    let rexp = replica.store.new_session()?.ttl_of(b"h").await?;
    let (Some(pexp), Some(rexp)) = (pexp, rexp) else {
      panic!("主从两侧随键 TTL 均应在册，实际 主 {pexp:?} 从 {rexp:?}");
    };
    assert!(
      (pexp - rexp).abs() < 10_000,
      "副本 TTL 须与主端同刻度（1ms 容差），实际 {pexp} vs {rexp}"
    );
    assert!(pexp > now_ticks(), "fixture：TTL 须在有效期内，实际 {pexp}");

    // 物理域终态：两侧信封记录在册、Meta 记录消亡（副本不得复活分层态，
    // 主端亦不得留幽灵元记录）
    for (tag, node) in [("primary", &primary), ("replica", &replica)] {
      let sess = node.store.new_session()?;
      let env_k = sess.session_tag_key(KeyTag::ObjectEnvelope, b"h");
      assert!(
        sess.read_raw(&env_k).await?.is_some(),
        "{tag} 信封域记录应在册（降阶即整值写回）"
      );
      assert!(
        !is_tiered(node, b"h").await?,
        "{tag} 元记录/树存根应随清退消亡"
      );
    }
    OK
  })
}
