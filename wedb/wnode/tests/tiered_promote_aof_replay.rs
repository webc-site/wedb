//! 集合就地升阶 AOF 数据通道端到端集成测试
//!
//! 验证 todo 的修复：升阶（envelope → wbftree 树）不再只留空存根，历史数据经
//! RangeIndexStreamChunk 通道随 AOF 一并回放。主端升阶后写入全新副本端重放，
//! 副本须重建为等价树态：条目全量可读、计数一致、TYPE 回原集合类型、无信封幻影。
//!
//! 单机制：写入统一经 NodeService 装配的 StoreEventSink（promote 发
//! RangeIndexStream），重放统一经 AofProcessor → RangeIndexManagerReplication
//! 逐键流重组 → publish_migrated_range_index（按携带的 obj_type 重建元记录）。

use std::{mem::take, sync::Arc};

use aok::{OK, Void};
use compio::runtime::Runtime;
use itoa::Buffer as ItoaBuffer;
use tempfile::tempdir;
use waof::{WalConfig, WalLog};
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::{
  resp::{
    garnet_api::{GarnetApi, StoreGarnetApi},
    resp_server_session::{RespServerSession, RespServerSessionOptions},
  },
  service::NodeService,
};
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

fn auto_exec(
  api: &GarnetApi,
  rt: &Runtime,
  s: &mut RespServerSession,
  cmd: RespCommand,
  args: &[&[u8]],
) -> Vec<u8> {
  s.output.clear();
  api.exec(s, cmd, args);
  if !s.output.is_empty() {
    return take(&mut s.output);
  }
  let slow = s
    .take_slow_wait()
    .unwrap_or_else(|| panic!("命令 {cmd} 无输出且未挂起慢路径"));
  let out = rt.block_on(slow.resolve());
  s.output.clear();
  out
}

/// 灌入超过升阶阈值的 hash 字段（分块 HSET 触发就地升阶）
fn fill_hash(api: &GarnetApi, rt: &Runtime, s: &mut RespServerSession, key: &[u8], total: usize) {
  let mut buf = ItoaBuffer::new();
  for chunk_start in (1..=total).step_by(1000) {
    let chunk_end = (chunk_start + 999).min(total);
    let mut args: Vec<Vec<u8>> = Vec::with_capacity((chunk_end - chunk_start + 1) * 2 + 1);
    args.push(key.to_vec());
    for i in chunk_start..=chunk_end {
      args.push(format!("f{i}").into_bytes());
      args.push(buf.format(i).as_bytes().to_vec());
    }
    let slices: Vec<&[u8]> = args.iter().map(|v| v.as_slice()).collect();
    auto_exec(api, rt, s, RespCommand::Hset, &slices);
  }
}

/// 主端升阶 → 副本端全新实例重放 → 树态等价断言
#[test]
fn promote_aof_data_channel_replay_converges_replica() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let total = wcol::TIERED_PROMOTE_THRESHOLD + 10;

    let primary = open_node("promote_primary")?;
    let papi = api_of(&primary.store)?;
    let mut ps = session_with(&papi);
    fill_hash(&papi, &rt, &mut ps, b"h", total);
    primary.wal.commit().await?;

    // 主端确认已升阶：元记录存在、类型为 Hash、计数含升阶触发的最后一写
    let (pmeta, _) = primary
      .store
      .new_session()?
      .load_collection_stub(b"h")
      .await?
      .expect("主端应已就地升阶为树态");
    assert_eq!(pmeta.collection_type, GarnetObjectType::Hash);
    assert_eq!(pmeta.size as usize, total, "主端计数须含全部字段");
    assert_eq!(
      auto_exec(&papi, &rt, &mut ps, RespCommand::Type, &[b"h"]),
      b"+hash\r\n"
    );

    // 副本：全新引擎实例 + 独立 WAL，零内存状态，统一重放链路闭环
    let replica = open_node("promote_replica")?;
    let replayed = primary
      .service
      .replay_into_session(replica.service.session())
      .await?;
    assert!(replayed > 0, "副本应消费到升阶数据通道条目");

    // 副本树态与主端等价：元记录为 Hash、计数一致
    let (rmeta, _) = replica
      .store
      .new_session()?
      .load_collection_stub(b"h")
      .await?
      .expect("副本经数据通道回放后应为树态（非空存根）");
    assert_eq!(
      rmeta.collection_type,
      GarnetObjectType::Hash,
      "副本元记录类型须为原集合类型 Hash，不得硬编码 RangeIndex"
    );
    assert_eq!(
      rmeta.size as usize, total,
      "副本树计数须与主端一致（含升阶前历史数据）"
    );

    // 命令面一致性：TYPE / HLEN / 抽样 HGET 全部对齐，且末段（升阶触发批）不缺失
    let rapi = api_of(&replica.store)?;
    let mut rs = session_with(&rapi);
    assert_eq!(
      auto_exec(&rapi, &rt, &mut rs, RespCommand::Type, &[b"h"]),
      b"+hash\r\n"
    );
    let hlen = auto_exec(&rapi, &rt, &mut rs, RespCommand::Hlen, &[b"h"]);
    assert_eq!(
      hlen,
      format!(":{total}\r\n").into_bytes(),
      "HLEN 须与主端一致"
    );

    for field in [b"f1".as_slice(), b"f65536", b"f65545", b"f65546"] {
      let on_primary = auto_exec(&papi, &rt, &mut ps, RespCommand::Hget, &[b"h", field]);
      let on_replica = auto_exec(&rapi, &rt, &mut rs, RespCommand::Hget, &[b"h", field]);
      assert_eq!(
        on_primary, on_replica,
        "字段 {field:?} 主从不一致（数据通道漏灌即幻影/缺值）"
      );
      assert!(
        on_primary.starts_with(b"$"),
        "抽样字段应可读，实际 {on_primary:?}"
      );
    }

    // 全量条目主从逐条相等（快照树与重建树逐字节同构）
    let ph = auto_exec(&papi, &rt, &mut ps, RespCommand::Hgetall, &[b"h"]);
    let rh = auto_exec(&rapi, &rt, &mut rs, RespCommand::Hgetall, &[b"h"]);
    assert_eq!(ph, rh, "HGETALL 全量条目须与主端逐字节一致");
    assert!(ph.starts_with(b"*"), "HGETALL 应为数组回帧，实际 {ph:?}");

    // 信封域无幽灵残留：信封墓碑条目回放后，主从两侧信封物理键均已摘除
    for (tag, store) in [("primary", &primary.store), ("replica", &replica.store)] {
      let sess = store.new_session()?;
      let env_k = sess.session_tag_key(KeyTag::ObjectEnvelope, b"h");
      assert!(
        sess.read_raw(&env_k).await?.is_none(),
        "{tag} 信封域不应有幽灵残留"
      );
    }
    OK
  })
}

/// 分层重灌流（replace=true）端到端回放：首升阶后 HDEL 少量字段触发同名二次
/// 升阶（先建后拆换入，流不再前导 RangeIndexDrop），副本对既有树必须以
/// replace 形态换入重放——若 replace 位未打通，副本回放撞 live_indexes 在册
/// 旧树被拒（AlreadyExists 静默跳过），旧树残留 f1 幽灵、HGETALL 与主端发散
#[test]
fn tiered_reflush_stream_replays_with_replace_over_existing_tree() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let total = wcol::TIERED_PROMOTE_THRESHOLD + 10;

    let primary = open_node("reflush_primary")?;
    let papi = api_of(&primary.store)?;
    let mut ps = session_with(&papi);
    fill_hash(&papi, &rt, &mut ps, b"h", total);

    // 分层态 HDEL 单成员：跌不回死区 → 走 apply_rmw_post_operate 重灌臂
    // （promote replace=true），键全程存活
    assert_eq!(
      auto_exec(&papi, &rt, &mut ps, RespCommand::Hdel, &[b"h", b"f1"]),
      b":1\r\n",
      "HDEL 应答为删除计数 1"
    );
    assert_eq!(
      auto_exec(&papi, &rt, &mut ps, RespCommand::Hget, &[b"h", b"f1"]),
      b"$-1\r\n",
      "主端 f1 应已删除"
    );
    primary.wal.commit().await?;

    let replica = open_node("reflush_replica")?;
    let replayed = primary
      .service
      .replay_into_session(replica.service.session())
      .await?;
    assert!(replayed > 0, "副本应消费到升阶 + 重灌数据通道条目");

    // 重放判据（replace 打通的唯一红绿面）：副本树内容必须随重灌流换入收敛，
    // f1 不得以幽灵复活；仅断言元记录计数会被 meta 域 StoreUpsert 回放掩盖，
    // 数据面逐字节比对才鉴别「流撞旧树被拒、meta 却已收敛」的发散
    let rapi = api_of(&replica.store)?;
    let mut rs = session_with(&rapi);
    assert_eq!(
      auto_exec(&rapi, &rt, &mut rs, RespCommand::Hget, &[b"h", b"f1"]),
      b"$-1\r\n",
      "副本重灌流必须以 replace 换入旧树，f1 残留即回放被 AlreadyExists 拒"
    );
    let hlen = auto_exec(&rapi, &rt, &mut rs, RespCommand::Hlen, &[b"h"]);
    assert_eq!(
      hlen,
      format!(":{}\r\n", total - 1).into_bytes(),
      "HLEN 须与主端一致"
    );
    let ph = auto_exec(&papi, &rt, &mut ps, RespCommand::Hgetall, &[b"h"]);
    let rh = auto_exec(&rapi, &rt, &mut rs, RespCommand::Hgetall, &[b"h"]);
    assert_eq!(ph, rh, "重灌后 HGETALL 全量条目须与主端逐字节一致");
    OK
  })
}
