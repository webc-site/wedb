//! 分层稳态写臂 AOF 命令镜像端到端集成测试
//!
//! 验证审查意见【高】的修复：升阶流只镜像升阶时点内容，分层树内稳态写
//!（HSET 族 / SADD / ZADD 族 / LPUSH 族）此后不经任何 AOF/复制镜像——副本树
//! 永久缺字段、meta.size 滞后。修复后每条生效的稳态写经
//! StoreEvent::TieredCollectionWrite 入账 ObjectStoreRMW 条目（RESP 命令语义
//! 镜像），副本/恢复端 object_store_rmw 先经 load_collection_stub 探分层态
//! 路由分层臂逐条重放收敛。
//!
//! 单机制：主端 finish 链 emit（tiered_collection_ops 各族 arm 锁内单点），
//! 重放统一经 AofProcessor → object_store_rmw → tiered_replay_arm（主端
//! try_tiered_arm 同一判定单点）。四族各一枚「升阶 → 稳态写 → 全新副本重放」
//! 场景，断言新写入数据在副本**树**上在位（走信封通道即树缺新字段 + 信封域
//! 幻影，双层断言鉴别路由正确性）。

use std::sync::Arc;

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

/// 重放后树态鉴别断言（重放路由正确性的双层判据）：
/// 1. 副本元记录为分层态且判别类型一致（非幻影信封）；
/// 2. 副本信封物理键无残留——若镜像条目误落信封通道，信封空对象重建会在
///    ObjectEnvelope 域造出含新字段的幻影对象，此处即红
async fn assert_tiered_no_envelope_ghost(
  replica: &Node,
  key: &[u8],
  tag: GarnetObjectType,
) -> Void {
  let (rmeta, _) = replica
    .store
    .new_session()?
    .load_collection_stub(key)
    .await?
    .expect("副本经镜像条目回放后应为树态（非空存根）");
  assert_eq!(
    rmeta.collection_type, tag,
    "副本元记录类型须为分层集合类型，不得退化信封"
  );
  let rsess = replica.store.new_session()?;
  let env_k = rsess.session_tag_key(KeyTag::ObjectEnvelope, key);
  assert!(
    rsess.read_raw(&env_k).await?.is_none(),
    "副本信封域不应有幻影残留（镜像条目误走信封通道即红）"
  );
  OK
}

/// 稳态写前确认主端已升阶（元记录在场且类型一致）
async fn assert_promoted(
  store: &Arc<WedbStore<SegmentedDevice>>,
  key: &[u8],
  tag: GarnetObjectType,
) -> Void {
  let (meta, _) = store
    .new_session()?
    .load_collection_stub(key)
    .await?
    .expect("主端应已就地升阶为树态");
  assert_eq!(meta.collection_type, tag);
  OK
}

fn fill_hash(api: &GarnetApi, rt: &Runtime, s: &mut RespServerSession, key: &[u8], total: usize) {
  let mut buf = ItoaBuffer::new();
  let mut chunk_start = 1;
  let mut field_buf = Vec::with_capacity(16384 * 32);
  let mut ranges = Vec::with_capacity(16384 * 2);

  while chunk_start <= total {
    let chunk_end = (chunk_start + 16383).min(total);
    field_buf.clear();
    ranges.clear();

    let mut slices: Vec<&[u8]> = Vec::with_capacity((chunk_end - chunk_start + 1) * 2 + 1);
    slices.push(key);

    for i in chunk_start..=chunk_end {
      let num_bytes = buf.format(i).as_bytes();

      let f_start = field_buf.len();
      field_buf.push(b'f');
      field_buf.extend_from_slice(num_bytes);
      let f_end = field_buf.len();
      ranges.push(f_start..f_end);

      let v_start = f_end;
      field_buf.extend_from_slice(num_bytes);
      let v_end = field_buf.len();
      ranges.push(v_start..v_end);
    }

    for r in &ranges {
      slices.push(&field_buf[r.clone()]);
    }
    auto_exec(api, rt, s, RespCommand::Hset, &slices);
    chunk_start = chunk_end + 1;
  }
}

fn fill_set(api: &GarnetApi, rt: &Runtime, s: &mut RespServerSession, key: &[u8], total: usize) {
  let mut buf = ItoaBuffer::new();
  let mut chunk_start = 1;
  let mut field_buf = Vec::with_capacity(16384 * 16);
  let mut ranges = Vec::with_capacity(16384);

  while chunk_start <= total {
    let chunk_end = (chunk_start + 16383).min(total);
    field_buf.clear();
    ranges.clear();

    let mut slices: Vec<&[u8]> = Vec::with_capacity(chunk_end - chunk_start + 2);
    slices.push(key);

    for i in chunk_start..=chunk_end {
      let num_bytes = buf.format(i).as_bytes();

      let m_start = field_buf.len();
      field_buf.push(b'm');
      field_buf.extend_from_slice(num_bytes);
      let m_end = field_buf.len();
      ranges.push(m_start..m_end);
    }

    for r in &ranges {
      slices.push(&field_buf[r.clone()]);
    }
    auto_exec(api, rt, s, RespCommand::Sadd, &slices);
    chunk_start = chunk_end + 1;
  }
}

fn fill_zset(api: &GarnetApi, rt: &Runtime, s: &mut RespServerSession, key: &[u8], total: usize) {
  let mut buf = ItoaBuffer::new();
  let mut chunk_start = 1;
  let mut field_buf = Vec::with_capacity(16384 * 32);
  let mut ranges = Vec::with_capacity(16384 * 2);

  while chunk_start <= total {
    let chunk_end = (chunk_start + 16383).min(total);
    field_buf.clear();
    ranges.clear();

    let mut slices: Vec<&[u8]> = Vec::with_capacity((chunk_end - chunk_start + 1) * 2 + 1);
    slices.push(key);

    for i in chunk_start..=chunk_end {
      let num_bytes = buf.format(i).as_bytes();

      let score_start = field_buf.len();
      field_buf.extend_from_slice(num_bytes);
      let score_end = field_buf.len();
      ranges.push(score_start..score_end);

      let m_start = score_end;
      field_buf.push(b'm');
      field_buf.extend_from_slice(num_bytes);
      let m_end = field_buf.len();
      ranges.push(m_start..m_end);
    }

    for r in &ranges {
      slices.push(&field_buf[r.clone()]);
    }
    auto_exec(api, rt, s, RespCommand::Zadd, &slices);
    chunk_start = chunk_end + 1;
  }
}

fn fill_list(api: &GarnetApi, rt: &Runtime, s: &mut RespServerSession, key: &[u8], total: usize) {
  let mut buf = ItoaBuffer::new();
  let mut chunk_start = 1;
  let mut field_buf = Vec::with_capacity(16384 * 16);
  let mut ranges = Vec::with_capacity(16384);

  while chunk_start <= total {
    let chunk_end = (chunk_start + 16383).min(total);
    field_buf.clear();
    ranges.clear();

    let mut slices: Vec<&[u8]> = Vec::with_capacity(chunk_end - chunk_start + 2);
    slices.push(key);

    for i in chunk_start..=chunk_end {
      let num_bytes = buf.format(i).as_bytes();

      let val_start = field_buf.len();
      field_buf.extend_from_slice(num_bytes);
      let val_end = field_buf.len();
      ranges.push(val_start..val_end);
    }

    for r in &ranges {
      slices.push(&field_buf[r.clone()]);
    }
    auto_exec(api, rt, s, RespCommand::Rpush, &slices);
    chunk_start = chunk_end + 1;
  }
}

/// hash 族：升阶 → HSET 新字段 + HINCRBY delta 稳态写 → 副本重放树态在位
#[test]
fn tiered_hash_steady_state_write_mirrors_to_replica() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let total = wcol::TIERED_PROMOTE_THRESHOLD + 10;

    let primary = open_node("steady_hash_primary")?;
    let papi = api_of(&primary.store)?;
    let mut ps = session_with(&papi);
    fill_hash(&papi, &rt, &mut ps, b"h", total);
    assert_promoted(&primary.store, b"h", GarnetObjectType::Hash).await?;

    // 升阶后稳态写：新字段 HSET + delta HINCRBY（镜像的是命令语义非最终值）
    assert_eq!(
      auto_exec(
        &papi,
        &rt,
        &mut ps,
        RespCommand::Hset,
        &[b"h", b"steady", b"v1"]
      ),
      b":1\r\n",
      "分层态 HSET 新字段应答新增计数 1"
    );
    assert_eq!(
      auto_exec(
        &papi,
        &rt,
        &mut ps,
        RespCommand::Hincrby,
        &[b"h", b"ctr", b"5"]
      ),
      b":5\r\n",
      "分层态 HINCRBY 新字段应答增量后值"
    );
    assert_eq!(
      auto_exec(
        &papi,
        &rt,
        &mut ps,
        RespCommand::Hincrby,
        &[b"h", b"ctr", b"7"]
      ),
      b":12\r\n",
      "分层态 HINCRBY 既有字段应答累计值"
    );
    primary.wal.commit().await?;

    // 副本：全新引擎实例统一重放链路闭环（升阶流 + 稳态写镜像条目）
    let replica = open_node("steady_hash_replica")?;
    primary
      .service
      .replay_into_session(replica.service.session())
      .await?;
    assert_tiered_no_envelope_ghost(&replica, b"h", GarnetObjectType::Hash).await?;

    // 树态在位 + 计数一致（树缺新字段即镜像漏发 / 走信封通道）
    let rapi = api_of(&replica.store)?;
    let mut rs = session_with(&rapi);
    assert_eq!(
      auto_exec(&rapi, &rt, &mut rs, RespCommand::Hget, &[b"h", b"steady"]),
      b"$2\r\nv1\r\n",
      "副本树必须含升阶后 HSET 的新字段"
    );
    assert_eq!(
      auto_exec(&rapi, &rt, &mut rs, RespCommand::Hget, &[b"h", b"ctr"]),
      b"$2\r\n12\r\n",
      "副本树 HINCRBY delta 逐条重放须与主端累计一致"
    );
    let ph = auto_exec(&papi, &rt, &mut ps, RespCommand::Hlen, &[b"h"]);
    let rh = auto_exec(&rapi, &rt, &mut rs, RespCommand::Hlen, &[b"h"]);
    assert_eq!(ph, rh, "HLEN 主从须一致（稳态写 meta.size 滞后即红）");
    assert_eq!(rh, format!(":{}\r\n", total + 2).into_bytes());
    OK
  })
}

/// set 族：升阶 → SADD 新成员稳态写 → 副本重放树态在位
#[test]
fn tiered_set_steady_state_write_mirrors_to_replica() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let total = wcol::TIERED_PROMOTE_THRESHOLD + 10;

    let primary = open_node("steady_set_primary")?;
    let papi = api_of(&primary.store)?;
    let mut ps = session_with(&papi);
    fill_set(&papi, &rt, &mut ps, b"s", total);
    assert_promoted(&primary.store, b"s", GarnetObjectType::Set).await?;

    // 升阶后稳态写：SADD 新成员（重复成员不置脏不镜像，新成员必镜像）
    assert_eq!(
      auto_exec(&papi, &rt, &mut ps, RespCommand::Sadd, &[b"s", b"steady"]),
      b":1\r\n",
      "分层态 SADD 新成员应答新增计数 1"
    );
    primary.wal.commit().await?;

    let replica = open_node("steady_set_replica")?;
    primary
      .service
      .replay_into_session(replica.service.session())
      .await?;
    assert_tiered_no_envelope_ghost(&replica, b"s", GarnetObjectType::Set).await?;

    let rapi = api_of(&replica.store)?;
    let mut rs = session_with(&rapi);
    assert_eq!(
      auto_exec(
        &rapi,
        &rt,
        &mut rs,
        RespCommand::Sismember,
        &[b"s", b"steady"]
      ),
      b":1\r\n",
      "副本树必须含升阶后 SADD 的新成员"
    );
    let ph = auto_exec(&papi, &rt, &mut ps, RespCommand::Scard, &[b"s"]);
    let rh = auto_exec(&rapi, &rt, &mut rs, RespCommand::Scard, &[b"s"]);
    assert_eq!(ph, rh, "SCARD 主从须一致");
    assert_eq!(rh, format!(":{}\r\n", total + 1).into_bytes());
    OK
  })
}

/// zset 族：升阶 → ZADD 新成员稳态写 → 副本重放树态在位
#[test]
fn tiered_zset_steady_state_write_mirrors_to_replica() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let total = wcol::TIERED_PROMOTE_THRESHOLD + 10;

    let primary = open_node("steady_zset_primary")?;
    let papi = api_of(&primary.store)?;
    let mut ps = session_with(&papi);
    fill_zset(&papi, &rt, &mut ps, b"z", total);
    assert_promoted(&primary.store, b"z", GarnetObjectType::SortedSet).await?;

    // 升阶后稳态写：ZADD 新成员
    assert_eq!(
      auto_exec(
        &papi,
        &rt,
        &mut ps,
        RespCommand::Zadd,
        &[b"z", b"9.5", b"steady"]
      ),
      b":1\r\n",
      "分层态 ZADD 新成员应答新增计数 1"
    );
    primary.wal.commit().await?;

    let replica = open_node("steady_zset_replica")?;
    primary
      .service
      .replay_into_session(replica.service.session())
      .await?;
    assert_tiered_no_envelope_ghost(&replica, b"z", GarnetObjectType::SortedSet).await?;

    // 树态在位：ZSCORE 主从逐字节一致（RESP2 下为 bulk 分值帧）
    let rapi = api_of(&replica.store)?;
    let mut rs = session_with(&rapi);
    let ph = auto_exec(&papi, &rt, &mut ps, RespCommand::Zscore, &[b"z", b"steady"]);
    let rh = auto_exec(&rapi, &rt, &mut rs, RespCommand::Zscore, &[b"z", b"steady"]);
    assert_eq!(ph, rh, "ZSCORE 主从须一致");
    assert!(
      rh.starts_with(b"$"),
      "新成员分值应可读，实际 {rh:?}——树缺成员即镜像漏发"
    );
    let ph = auto_exec(&papi, &rt, &mut ps, RespCommand::Zcard, &[b"z"]);
    let rh = auto_exec(&rapi, &rt, &mut rs, RespCommand::Zcard, &[b"z"]);
    assert_eq!(ph, rh, "ZCARD 主从须一致");
    assert_eq!(rh, format!(":{}\r\n", total + 1).into_bytes());
    OK
  })
}

/// list 族：升阶 → RPUSH 新元素稳态写 → 副本重放树态在位
#[test]
fn tiered_list_steady_state_write_mirrors_to_replica() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let total = wcol::TIERED_PROMOTE_THRESHOLD + 10;

    let primary = open_node("steady_list_primary")?;
    let papi = api_of(&primary.store)?;
    let mut ps = session_with(&papi);
    fill_list(&papi, &rt, &mut ps, b"l", total);
    assert_promoted(&primary.store, b"l", GarnetObjectType::List).await?;

    // 升阶后稳态写：RPUSH 新元素
    assert_eq!(
      auto_exec(&papi, &rt, &mut ps, RespCommand::Rpush, &[b"l", b"steady"]),
      format!(":{}\r\n", total + 1).into_bytes(),
      "分层态 RPUSH 应答推入后长度"
    );
    primary.wal.commit().await?;

    let replica = open_node("steady_list_replica")?;
    primary
      .service
      .replay_into_session(replica.service.session())
      .await?;
    assert_tiered_no_envelope_ghost(&replica, b"l", GarnetObjectType::List).await?;

    // 树态在位：尾端元素即新元素 + LLEN 主从一致
    let rapi = api_of(&replica.store)?;
    let mut rs = session_with(&rapi);
    assert_eq!(
      auto_exec(&rapi, &rt, &mut rs, RespCommand::Lindex, &[b"l", b"-1"]),
      b"$6\r\nsteady\r\n",
      "副本树尾端必须是升阶后 RPUSH 的新元素"
    );
    let ph = auto_exec(&papi, &rt, &mut ps, RespCommand::Llen, &[b"l"]);
    let rh = auto_exec(&rapi, &rt, &mut rs, RespCommand::Llen, &[b"l"]);
    assert_eq!(ph, rh, "LLEN 主从须一致");
    assert_eq!(rh, format!(":{}\r\n", total + 1).into_bytes());
    OK
  })
}
