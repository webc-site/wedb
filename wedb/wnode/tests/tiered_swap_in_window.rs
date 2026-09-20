//! 分层安全换入窗（自迁移封写）并发回归——agy 轮 6 三条同根意见的收口证明
//!
//! 覆盖的缺陷形：出账重灌臂（expire_sweep_or_rebuild）与物化降级通道
//! （run_async_rmw 物化 → apply_rmw_post_operate 换入/清退）原为「放守卫 →
//! 建树快照（百毫秒级）→ 换入取锁」长窗，窗内并发同键写臂已 ACK 落旧树、
//! 随 publish replace=true 整树顶替静默丢失，且其 TieredCollectionWrite
//! 镜像已入 AOF、副本与重启重放反而持有——主从长期发散。
//!
//! 修复语义（方案甲：自迁移）：出账/物化/懒降阶臂经
//! `try_swap_in_window` 登记同键迁移 claim 封窗——窗内并发写臂在四探测门被
//! MigrationBusy/存储忙拒绝，窗内零写提交、AOF 镜像序仍 = 树内提交序。
//!
//! 断言口径（与 rename_semantics.rs 并发窗同形）：窗口内并发 HSET 同键要么
//! 被拒（错误帧、不计数）、要么整体先于快照（随换入存活），终态无「已 ACK
//! 丢失」；副本按 AOF 全量重放后与主端逐字段一致（delta 镜像不丢）。
//! compio 单 worker 下写者与封窗臂的调度竞态不落断言面——两分支均被
//! 「ACK 写全集存活 + 计数精确」不变量覆盖。

use std::{mem::take, str::from_utf8, sync::Arc};

use aok::{OK, Void};
use compio::runtime::{Runtime, spawn};
use tempfile::tempdir;
use waof::{WalConfig, WalLog};
use wbase::{convert::TICKS_PER_SECOND, time::now_ticks};
use wcol::types::member_ttl::encode_member;
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

type TestStore = WedbStore<SegmentedDevice>;

/// 主/副本各一套：存储 + WAL（range_index_dir 落树文件，副本发布必需）
struct Node {
  store: Arc<TestStore>,
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

fn api_of(store: &Arc<TestStore>) -> aok::Result<GarnetApi> {
  Ok(Arc::new(StoreGarnetApi::new(store.new_session()?)))
}

fn session_with(api: &GarnetApi) -> RespServerSession {
  let mut s = RespServerSession::new(1, RespServerSessionOptions::default());
  s.set_garnet_api(api.clone());
  s
}

/// 异步域命令泵（慢路径 resolve 直接 await，不嵌套 block_on——并发写者任务
/// 依赖同一 worker 轮转调度）
async fn exec_cmd(
  api: &GarnetApi,
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
  let out = slow.resolve().await;
  s.output.clear();
  out
}

/// 手工升阶 hash（entries 为 member_ttl 编码形态；next_expiry 为灌入批水位）
async fn promote_hash(
  store: &Arc<TestStore>,
  key: &[u8],
  entries: Vec<(Vec<u8>, Vec<u8>)>,
  next_expiry: i64,
) -> Void {
  let sess = store.new_session()?;
  sess
    .promote_collection_to_bftree(key, GarnetObjectType::Hash, entries, next_expiry, false)
    .await?;
  assert!(
    sess.load_collection_stub(key).await?.is_some(),
    "键应处于 wbftree 分层态"
  );
  OK
}

/// 三条存活种子 + 两条已到期种子（过期刻度为过去时刻）；水位随灌入批落过去
/// 值——首个触键命令必触发出账窗口（sweep → expired=2 → 整值重灌换入）
async fn promote_hash_with_expired_seeds(store: &Arc<TestStore>, key: &[u8]) -> Void {
  let past = now_ticks() - TICKS_PER_SECOND;
  promote_hash(
    store,
    key,
    vec![
      (b"f1".to_vec(), encode_member(b"v1", None)),
      (b"f2".to_vec(), encode_member(b"v2", None)),
      (b"f3".to_vec(), encode_member(b"v3", None)),
      (b"e1".to_vec(), encode_member(b"x1", Some(past))),
      (b"e2".to_vec(), encode_member(b"x2", Some(past))),
    ],
    past,
  )
  .await
}

/// 并发同键 HSET 写者：逐条经完整 RESP 慢路径发 HSET w{i} v{i}，整数应答记
/// ACK、错误帧（'-'）记拒绝并停；返回 (ACK 字段全集, 拒绝应答帧)
async fn hset_writer(
  store: Arc<TestStore>,
  key: &'static [u8],
  rounds: u32,
) -> aok::Result<(Vec<Vec<u8>>, Option<Vec<u8>>)> {
  let api = api_of(&store)?;
  let mut s = session_with(&api);
  let mut acked = Vec::new();
  let mut rejected = None;
  for i in 0..rounds {
    let field = format!("w{i:05}");
    let owned = [
      key.to_vec(),
      field.clone().into_bytes(),
      format!("v{i}").into_bytes(),
    ];
    let refs: Vec<&[u8]> = owned.iter().map(|v| v.as_slice()).collect();
    let reply = exec_cmd(&api, &mut s, RespCommand::Hset, &refs).await;
    match reply.first() {
      Some(b':') => acked.push(field.into_bytes()),
      Some(b'-') => {
        // 封窗内的存储忙拒绝（MigrationBusy 经慢路径漏斗出存储忙错误帧）
        rejected = Some(reply);
        break;
      }
      _ => panic!("HSET 异常应答: {reply:?}"),
    }
  }
  Ok((acked, rejected))
}

/// HGETALL 应答帧解析为 (字段, 值) 全集（扫描 $len 段重建 bulk 项，成对折叠）
fn parse_hgetall(frame: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
  let mut fields = Vec::new();
  let mut i = 0;
  while i < frame.len() {
    if frame[i] == b'$' {
      let end = &frame[i + 1..];
      let nl = end.iter().position(|&b| b == b'\r').expect("bulk 长度行");
      let len: usize = from_utf8(&end[..nl]).unwrap().parse().unwrap();
      let start = i + 1 + nl + 2;
      fields.push(frame[start..start + len].to_vec());
      i = start + len + 2;
    } else {
      i += 1;
    }
  }
  fields
    .chunks(2)
    .map(|c| (c[0].clone(), c.get(1).cloned().unwrap_or_default()))
    .collect()
}

/// 出账重灌窗口：水位越过（含到期种子）后 HLEN 触发 sweep → 整值重灌换入，
/// 并发写者同键 HSET 要么被封窗拒绝、要么先于快照随换入存活——已 ACK 写全集
/// 在终态树中在位、HLEN 精确（已 ACK 丢失即红）
#[test]
fn sweep_rebuild_window_concurrent_hset_not_lost() -> Void {
  Runtime::new()?.block_on(async {
    let node = open_node("swapin_sweep_primary")?;
    let api = api_of(&node.store)?;
    let mut s = session_with(&api);
    promote_hash_with_expired_seeds(&node.store, b"h").await?;

    const ROUNDS: u32 = 2000;
    let writer = spawn(hset_writer(Arc::clone(&node.store), b"h", ROUNDS));
    // 主臂触发出账窗口：水位越过 → sweep expired=2 → claim 封窗 → 重灌换入
    let hlen = exec_cmd(&api, &mut s, RespCommand::Hlen, &[b"h"]).await;
    let (acked, rejected) = writer.await.expect("并发写任务异常退出")?;

    // 拒绝形态出现时必须是错误帧而非静默成功（窗口封堵可见）
    if let Some(frame) = &rejected {
      assert_eq!(
        frame.first(),
        Some(&b'-'),
        "拒绝必须以错误帧呈现: {frame:?}"
      );
    }

    // 全部 ACK 写入存活（拒绝臂零副作用；存活臂随快照换入收敛）
    let live = exec_cmd(&api, &mut s, RespCommand::Hgetall, &[b"h"]).await;
    let pairs = parse_hgetall(&live);
    assert!(
      !pairs.iter().any(|(f, _)| f == b"e1" || f == b"e2"),
      "已出账到期种子不得残留"
    );
    for f in ["f1", "f2", "f3"] {
      assert!(
        pairs.iter().any(|(f2, _)| f2 == f.as_bytes()),
        "存活种子 {f} 不得随重灌丢失"
      );
    }
    for (i, f) in acked.iter().enumerate() {
      let want = format!("v{i}");
      let got = pairs
        .iter()
        .find(|(f2, _)| f2 == f)
        .map(|(_, v)| v.clone())
        .unwrap_or_default();
      assert_eq!(got, want.as_bytes(), "ACK 写入 {f:?} 随换入丢失");
    }
    // 计数精确：存活种子 + 去重 ACK 写入
    let expect = format!(":{}\r\n", 3 + acked.len());
    assert_eq!(hlen, expect.as_bytes(), "HLEN 应答须精确（含出账扣减）");
    let hlen2 = exec_cmd(&api, &mut s, RespCommand::Hlen, &[b"h"]).await;
    assert_eq!(hlen2, expect.into_bytes(), "HLEN 终态须精确");
    OK
  })
}

/// 副本回放后树态鉴别（镜像重放路由正确性：非信封幻影）+
/// 信封物理键无残留（镜像条目误落信封通道即红）
async fn assert_replica_tiered_no_ghost(replica: &Node, key: &[u8]) -> Void {
  let (rmeta, _) = replica
    .store
    .new_session()?
    .load_collection_stub(key)
    .await?
    .expect("副本经流/镜像重放后应为树态（非空存根）");
  assert_eq!(rmeta.collection_type, GarnetObjectType::Hash);
  let rsess = replica.store.new_session()?;
  let env_k = rsess.session_tag_key(KeyTag::ObjectEnvelope, key);
  assert!(
    rsess.read_raw(&env_k).await?.is_none(),
    "副本信封域不应有幻影残留"
  );
  OK
}

/// 物化降级换入窗口（agy-r6-design 条 2 精确路径）：HDEL 穿透物化通道
///（claim 封窗 → 全扫物化 → 对象层求值 → promote replace=true 整树换入），
/// 物化对象超升阶阈值使收尾必走换入臂；并发写者同键 HSET 要么被封窗拒绝、
/// 要么先于物化扫描随换入存活；主从按 AOF 全量重放收敛同一树态终态
///（稳态写 delta 镜像 + 换入流乱序即红）
#[test]
fn materialize_swap_in_window_concurrent_hset_not_lost() -> Void {
  Runtime::new()?.block_on(async {
    let node = open_node("swapin_mat_primary")?;
    let api = api_of(&node.store)?;
    let mut s = session_with(&api);
    // 种子超升阶阈值：物化后对象 should_promote，收尾必走 promote replace=true
    // 换入臂（迟滞死区内走懒降阶清退臂，其副本回放面系独立缺口不混入本测）
    let total = wcol::TIERED_PROMOTE_THRESHOLD + 10;
    let mut entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(total);
    for i in 0..total {
      entries.push((format!("f{i:06}").into_bytes(), encode_member(b"v", None)));
    }
    promote_hash(&node.store, b"h", entries, i64::MAX).await?;

    const ROUNDS: u32 = 2000;
    let writer = spawn(hset_writer(Arc::clone(&node.store), b"h", ROUNDS));
    // 删除重命令穿透物化降级臂（封窗内求值 + promote 换入收尾）
    let hdel = exec_cmd(&api, &mut s, RespCommand::Hdel, &[b"h", b"f000000"]).await;
    assert_eq!(hdel, b":1\r\n", "HDEL 存活字段应答删除计数 1");
    let (acked, _rejected) = writer.await.expect("并发写任务异常退出")?;

    // 终态：HDEL 字段消亡、全部 ACK 写入在位、计数精确
    let live = exec_cmd(&api, &mut s, RespCommand::Hgetall, &[b"h"]).await;
    let pairs = parse_hgetall(&live);
    assert!(
      !pairs.iter().any(|(f, _)| f == b"f000000"),
      "HDEL 字段应消亡"
    );
    for (i, f) in acked.iter().enumerate() {
      let want = format!("v{i}");
      let got = pairs
        .iter()
        .find(|(f2, _)| f2 == f)
        .map(|(_, v)| v.clone())
        .unwrap_or_default();
      assert_eq!(got, want.as_bytes(), "ACK 写入 {f:?} 随换入丢失");
    }
    assert_eq!(
      pairs.len(),
      (total - 1) + acked.len(),
      "终态条目数须精确（种子 + ACK 写入 - 删除）"
    );

    // 主从一致：AOF 全量重放收敛同一树态终态（delta 镜像不丢）
    node.wal.commit().await?;
    let replica = open_node("swapin_mat_replica")?;
    node
      .service
      .replay_into_session(replica.service.session())
      .await?;
    assert_replica_tiered_no_ghost(&replica, b"h").await?;
    let rapi = api_of(&replica.store)?;
    let mut rs = session_with(&rapi);
    let rlive = exec_cmd(&rapi, &mut rs, RespCommand::Hgetall, &[b"h"]).await;
    let rpairs = parse_hgetall(&rlive);
    assert_eq!(
      pairs.len(),
      rpairs.len(),
      "主从条目数须一致（delta 镜像丢失即红）"
    );
    for (f, v) in &pairs {
      assert_eq!(
        rpairs.iter().find(|(f2, _)| f2 == f).map(|(_, v2)| v2),
        Some(v),
        "副本缺字段 {f:?} 或值发散"
      );
    }
    let ph = exec_cmd(&api, &mut s, RespCommand::Hlen, &[b"h"]).await;
    let rh = exec_cmd(&rapi, &mut rs, RespCommand::Hlen, &[b"h"]).await;
    assert_eq!(ph, rh, "HLEN 主从须一致");
    OK
  })
}
