//! 集合就地升阶 AOF 数据通道端到端集成测试
//!
//! 验证 todo 的修复：升阶（envelope → wbftree 树）不再只留空存根，历史数据经
//! RangeIndexStreamChunk 通道随 AOF 一并回放。主端升阶后写入全新副本端重放，
//! 副本须重建为等价树态：条目全量可读、计数一致、TYPE 回原集合类型、无信封幻影。
//!
//! 单机制：写入统一经 NodeService 装配的 StoreEventSink（promote 发
//! RangeIndexStream），重放统一经 AofProcessor → RangeIndexManagerReplication
//! 逐键流重组 → publish_migrated_range_index（按携带的 obj_type 重建元记录）。

use std::{
  mem::take,
  sync::Arc,
  thread::sleep,
  time::{Duration, Instant},
};

use aok::{OK, Void};
use compio::runtime::Runtime;
use itoa::Buffer as ItoaBuffer;
use tempfile::tempdir;
use waof::{WalConfig, WalLog};
use wbase::time::now_ticks;
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

/// 灌入触发就地升阶的 hash 字段（分块 HSET；`value_bytes = 0` 走计数维阈值，
/// 取正数则按字段体积维阈值升阶，见 [`fill_fields`]）
fn fill_hash(
  api: &GarnetApi,
  rt: &Runtime,
  s: &mut RespServerSession,
  key: &[u8],
  total: usize,
  value_bytes: usize,
) {
  fill_fields(api, rt, s, key, b"f", 1, total, value_bytes);
}

/// 灌入 `[start, end]` 区间、字段名带可辨前缀的 hash 字段（分块 HSET；不重叠
/// 区间可叠加至阈值突破，规避 HSET 覆盖已挂 TTL 字段的语义干扰）
///
/// 值取序号原文（`value_bytes = 0`，只推条目数、走
/// [`wcol::TIERED_PROMOTE_THRESHOLD`] 计数维）或定长 `'v'` 串（推
/// [`wcol::TIERED_PROMOTE_BYTES`] 体积维、条目数可停在计数阈之下）。体积维取
/// 定长同值系口径选择：异值大载荷自 `envelope_overflow` 容量门（票
/// tiered-hset-mixed-large-value-storage-err-repro，异值面由
/// tiered_hset_mixed_large_value.rs 承接）后会经页容量提前升阶，改走树内
/// 直写；本用例判定面是水位透传，取同值让中间态恒走信封整值入账路径
#[allow(clippy::too_many_arguments)]
fn fill_fields(
  api: &GarnetApi,
  rt: &Runtime,
  s: &mut RespServerSession,
  key: &[u8],
  prefix: &[u8],
  start: usize,
  end: usize,
  value_bytes: usize,
) {
  let mut buf = ItoaBuffer::new();
  let mut chunk_start = start;
  while chunk_start <= end {
    let chunk_end = (chunk_start + 999).min(end);
    let mut args: Vec<Vec<u8>> = Vec::with_capacity((chunk_end - chunk_start + 1) * 2 + 1);
    args.push(key.to_vec());
    for i in chunk_start..=chunk_end {
      let mut field = prefix.to_vec();
      field.extend_from_slice(buf.format(i).as_bytes());
      args.push(field);
      args.push(if value_bytes == 0 {
        buf.format(i).as_bytes().to_vec()
      } else {
        vec![b'v'; value_bytes]
      });
    }
    let slices: Vec<&[u8]> = args.iter().map(|v| v.as_slice()).collect();
    // 灌入批回帧必须是整数计数：存储层拒绝须在发生的这一块当场报，不留到
    // 后续断言处再以「未升阶」的假象误导定位
    let rep = auto_exec(api, rt, s, RespCommand::Hset, &slices);
    assert!(
      rep.starts_with(b":"),
      "HSET 灌入批 {chunk_start}..{chunk_end} 回帧异常: {:?}",
      String::from_utf8_lossy(&rep)
    );
    chunk_start = chunk_end + 1;
  }
}

/// 确定性等待成员水位越线：严格 `now_ticks() > next_expiry` 才开扫出账（对齐
/// 分层计数门 `now <= meta.next_expiry` 的短路口径，见 tiered_collection_ops
/// ::sweep_expired_members 与 wcol 成员刻度 `expiry < now` 判定）
///
/// 到期刻度是命令解析段读的真钟，应用段（分层物化）耗时随调度涨，不定死
/// sleep 时长即无「等不够误红 / 等太久空耗」两头 flake；超 `cap` 仍不越线即
/// 水位刻度本身被写歪，判红而非挂死
fn wait_past_watermark(next_expiry: i64, cap: Duration) -> bool {
  let deadline = Instant::now() + cap;
  loop {
    if now_ticks() > next_expiry {
      return true;
    }
    if Instant::now() >= deadline {
      return false;
    }
    sleep(Duration::from_millis(5));
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
    fill_hash(&papi, &rt, &mut ps, b"h", total, 0);
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

/// 带成员 TTL 的集合：分层重灌流透传成员 TTL 水位副本（ReplayInput.arg2 增载）
///
/// 场景：先灌满触发就地升阶（升阶批无成员 TTL，水位 MAX 为真值），随后分层态
/// HPEXPIRE 给 f1/f2 挂 TTL——tiered RMW 收尾臂整值重灌（promote replace=true），
/// 灌入批含 TTL 头 → earliest_expiry 提取真水位随流透传。
///
/// 修复前：副本发布执行体落 `MetaValue::new` 的 i64::MAX 假水位 → 分层计数门
/// `now <= next_expiry` 恒真 → 已到期成员永不出账，副本 HLEN 虚高与主端发散
/// （stub.rs 升阶头注明确警告的后果）。
///
/// 断言：副本元记录水位与主端逐位一致（非假 MAX）；TTL 越线后两端 HLEN 出账
/// 口径一致（到期成员扣除、计数收敛）。
///
/// 时序口径（门禁并跑偶红的根因，改这两行数前必读）：HPEXPIRE 的到期刻度在
/// **解析段**读一次钟（object_store_utils.rs:compute_expiration_ticks，对位 C#
/// HashCommands.cs:628 `HashExpire` 的
/// `DateTimeOffset.UtcNow.AddMilliseconds(expiration).UtcTicks`），应用段再读一次
/// 钟作比（hash_object.rs:set_expiration，对位 C# HashObject.cs:571
/// `SetExpiration` 的 `expiration <= DateTimeOffset.UtcNow.Ticks`），两次读钟之间
/// 夹整趟分层物化（HPEXPIRE 不在分层快通道命令表内，须先把全树物化成
/// HashObject）。间隔一旦 ≥ TTL，C# 语义就是回
/// `ExpireResult::KeyAlreadyExpired = 2`（HashObject.cs:685 `enum ExpireResult`）
/// **并当场摘除该成员**，水位随即失去真值——生产码与 C# 同构无缺陷，红相纯属
/// 测试侧把 TTL 写成调度不可承受的魔法数（门禁 (2632/3028) 并跑实测回过
/// `*2\r\n:2\r\n:2\r\n`，即 300ms 被物化间隔撞穿）。
///
/// 两头收口（12 核实测：perl CPU 燃烧进程造过订阅，逐档 TTL 探针对照回值 1/2
/// 定间隔上界。计数维 65546 成员：idle <300ms、1x 过订阅 ∈(600,900]ms、3x
/// ∈(1800,2400]ms；体积维 8000 成员：3x 过订阅 <300ms、6x ∈(300,600]ms）：
/// - fixture 走体积维升阶（物化间隔 ∝ 成员数，条目数压到计数阈之下）
/// - ttl_ms 取 3000（6x 过订阅实测上界的 5 倍余量），越线等待改为按主端已读到
///   的真水位刻度作差（[`wait_past_watermark`]），废除 `sleep(ttl_ms + 400)` 猜时长
#[test]
fn promote_stream_carries_member_expiry_watermark_to_replica() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    // 体积维升阶 fixture：8000 字段 × 600B ≈ 4.8MB 越 wcol::TIERED_PROMOTE_BYTES
    // 真实升阶（口径逐字对位 collection_adaptive_tiering
    // ::test_hash_adaptive_tiering_by_memory_bytes）；成员数压到计数阈的 1/8，
    // 分层物化间隔随之缩水，计数维升阶由本文件另两支覆盖，重灌臂与水位通道两支
    // 完全同路
    let total = 8000usize;
    let field_bytes = 600usize;
    let ttl_ms = 3000u64;

    let primary = open_node("promote_wm_primary")?;
    let papi = api_of(&primary.store)?;
    let mut ps = session_with(&papi);

    // 一次性灌满触发就地升阶（升阶批无成员 TTL）
    fill_hash(&papi, &rt, &mut ps, b"h", total, field_bytes);
    // 分层态 HPEXPIRE：整值重灌臂（replace 换树），灌入批含 f1/f2 TTL 头，
    // earliest_expiry 真水位随流透传
    let ttl = ttl_ms.to_string();
    let field_count = b"2";
    let rep = auto_exec(
      &papi,
      &rt,
      &mut ps,
      RespCommand::Hpexpire,
      &[b"h", ttl.as_bytes(), b"FIELDS", field_count, b"f1", b"f2"],
    );
    assert_eq!(
      rep, b"*2\r\n:1\r\n:1\r\n",
      "分层态字段 TTL 应设置成功（回 2 即物化间隔撞穿 TTL，见函数头注时序口径）"
    );
    primary.wal.commit().await?;

    // 主端元记录水位为重灌批真刻度（重灌臂 next_expiry 入参已打通）
    let sess = primary.store.new_session()?;
    let (pmeta, _) = sess
      .load_collection_stub(b"h")
      .await?
      .expect("主端应已就地升阶为树态");
    assert_eq!(pmeta.size as usize, total);
    assert!(
      pmeta.next_expiry < i64::MAX,
      "主端水位应为带 TTL 成员的真刻度，实际 i64::MAX"
    );

    // 副本重放：水位逐位一致（修复前 = i64::MAX 假水位，此处即红）
    let replica = open_node("promote_wm_replica")?;
    let replayed = primary
      .service
      .replay_into_session(replica.service.session())
      .await?;
    assert!(replayed > 0, "副本应消费到升阶/重灌数据通道条目");
    let rsess = replica.store.new_session()?;
    let (rmeta, _) = rsess
      .load_collection_stub(b"h")
      .await?
      .expect("副本经数据通道回放后应为树态");
    assert_eq!(
      rmeta.next_expiry, pmeta.next_expiry,
      "副本水位须与主端逐位一致（假 MAX 即 arg2 水位通道漏载）"
    );

    // TTL 越线：两端 HLEN 出账口径一致（到期成员 f1/f2 扣除；副本假水位时
    // 计数门恒真不出账，HLEN 虚高 total 即红）。以主端读到的真水位刻度作差
    // 等越线，不定死 sleep 时长
    assert!(
      wait_past_watermark(pmeta.next_expiry, Duration::from_secs(30)),
      "主端水位 {} 未在 30s 内越线",
      pmeta.next_expiry
    );
    let rapi = api_of(&replica.store)?;
    let mut rs = session_with(&rapi);
    let expect = format!(":{}\r\n", total - 2).into_bytes();
    let ph = auto_exec(&papi, &rt, &mut ps, RespCommand::Hlen, &[b"h"]);
    assert_eq!(ph, expect, "主端越线后 HLEN 应扣除到期成员");
    let rh = auto_exec(&rapi, &rt, &mut rs, RespCommand::Hlen, &[b"h"]);
    assert_eq!(rh, ph, "副本越线后 HLEN 出账口径须与主端一致");
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
    fill_hash(&papi, &rt, &mut ps, b"h", total, 0);

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
