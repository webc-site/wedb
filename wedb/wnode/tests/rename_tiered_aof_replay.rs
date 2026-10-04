//! 分层键 RENAME 的 AOF 数据通道端到端集成测试
//!
//! 缺陷：RENAME 分层键（wbftree 树态：RangeIndex / 就地升阶集合）的新键元记录
//! 走裸原语 upsert_raw，AOF 写监听对 Meta 域直接放行 → 新键无任何复制镜像条目，
//! 主从与重启回放后新键丢失。
//!
//! 修复：rename_range_index 落 meta 前复用升阶流通道（StoreEvent::RangeIndexStream
//! → RangeIndexStreamChunk），把新键在线树 CPR 快照分块灌入 AOF；副本端逐键流
//! 重组 → publish_migrated_range_index 按携带的原集合类型重建元记录。
//!
//! 断言：主库 RENAME 后，副本端全新实例重放 → 新键树态可访问且与主端等价
//! （类型 / 计数 / 抽样与全量读一致），旧键在两端严格消失。
//!
//! 单机制：写入统一经 NodeService 装配的 StoreEventSink，重放统一经
//! AofProcessor → RangeIndexManagerReplication（与 tiered_promote_aof_replay
//! 同一闭环，仅迁移源由升阶臂换为 RENAME 慢路径）。

use aok::{OK, Void};
use compio::runtime::Runtime;
use itoa::Buffer as ItoaBuffer;
use wnode::resp::{garnet_api::GarnetApi, resp_server_session::RespServerSession};
use wnode_test::{api_of, auto_exec, open_node, session_on as session_with};
use wresp::command::RespCommand;
use wval::{GarnetObjectType, KeyTag};

/// 灌入超过升阶阈值的 hash 字段（分块 HSET 触发就地升阶）
fn fill_hash(api: &GarnetApi, rt: &Runtime, s: &mut RespServerSession, key: &[u8], total: usize) {
  fill_fields(api, rt, s, key, b"f", 1, total);
}

/// 灌入 `[start, end]` 区间、字段名带可辨前缀的 hash 字段（分块 HSET 触发就地
/// 升阶；前缀可辨使 dst 覆写场景的源/目标树内容可分辨）
fn fill_fields(
  api: &GarnetApi,
  rt: &Runtime,
  s: &mut RespServerSession,
  key: &[u8],
  prefix: &[u8],
  start: usize,
  end: usize,
) {
  let mut buf = ItoaBuffer::new();
  let mut chunk_start = start;
  let mut field_buf = Vec::with_capacity(16384 * 32);
  let mut ranges = Vec::with_capacity(16384 * 2);

  while chunk_start <= end {
    let chunk_end = (chunk_start + 16383).min(end);
    field_buf.clear();
    ranges.clear();

    let mut slices: Vec<&[u8]> = Vec::with_capacity((chunk_end - chunk_start + 1) * 2 + 1);
    slices.push(key);

    for i in chunk_start..=chunk_end {
      let num_bytes = buf.format(i).as_bytes();

      let f_start = field_buf.len();
      field_buf.extend_from_slice(prefix);
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

/// 主库 RENAME 升阶集合键 → 副本端全新实例重放 → 新键树态等价、旧键消失
#[test]
fn rename_tiered_hash_aof_replay_converges_replica() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let total = wcol::TIERED_PROMOTE_THRESHOLD + 10;

    let primary = open_node("rename_tiered_primary")?;
    let papi = api_of(&primary.store)?;
    let mut ps = session_with(&papi);
    fill_hash(&papi, &rt, &mut ps, b"h", total);
    primary.wal.commit().await?;

    // 前置：主端已升阶为树态
    let (pmeta, _) = primary
      .store
      .new_session()?
      .load_collection_stub(b"h")
      .await?
      .expect("主端应已就地升阶为树态");
    assert_eq!(pmeta.collection_type, GarnetObjectType::Hash);
    assert_eq!(pmeta.size as usize, total);

    // RENAME h → h2（Meta 域慢路径：整树快照迁移 + RangeIndexStream 数据通道）
    assert_eq!(
      auto_exec(&papi, &rt, &mut ps, RespCommand::Rename, &[b"h", b"h2"]),
      b"+OK\r\n"
    );
    primary.wal.commit().await?;

    // 主端新键可访问、旧键严格消失
    assert_eq!(
      auto_exec(&papi, &rt, &mut ps, RespCommand::Type, &[b"h2"]),
      b"+hash\r\n"
    );
    assert_eq!(
      auto_exec(&papi, &rt, &mut ps, RespCommand::Exists, &[b"h"]),
      b":0\r\n"
    );

    // 副本：全新引擎实例 + 独立 WAL，零内存状态，统一重放链路闭环
    let replica = open_node("rename_tiered_replica")?;
    let replayed = primary
      .service
      .replay_into_session(replica.service.session())
      .await?;
    assert!(replayed > 0, "副本应消费到 RENAME 数据通道条目");

    // 副本新键树态与主端等价（修复前：新键零镜像条目，此处 load 为 None）
    let (rmeta, _) = replica
      .store
      .new_session()?
      .load_collection_stub(b"h2")
      .await?
      .expect("副本经数据通道回放后新键应为树态（非空存根）");
    assert_eq!(
      rmeta.collection_type,
      GarnetObjectType::Hash,
      "副本元记录类型须为原集合类型 Hash，不得硬编码 RangeIndex"
    );
    assert_eq!(
      rmeta.size as usize, total,
      "副本树计数须与主端一致（迁移不丢条目）"
    );

    // 命令面一致性：TYPE / HLEN / 抽样 / 全量逐字节对齐
    let rapi = api_of(&replica.store)?;
    let mut rs = session_with(&rapi);
    assert_eq!(
      auto_exec(&rapi, &rt, &mut rs, RespCommand::Type, &[b"h2"]),
      b"+hash\r\n"
    );
    assert_eq!(
      auto_exec(&rapi, &rt, &mut rs, RespCommand::Hlen, &[b"h2"]),
      format!(":{total}\r\n").into_bytes(),
      "副本 HLEN 须与主端一致"
    );
    for field in [b"f1".as_slice(), b"f65536", b"f65545", b"f65546"] {
      let on_primary = auto_exec(&papi, &rt, &mut ps, RespCommand::Hget, &[b"h2", field]);
      let on_replica = auto_exec(&rapi, &rt, &mut rs, RespCommand::Hget, &[b"h2", field]);
      assert_eq!(
        on_primary, on_replica,
        "字段 {field:?} 主从不一致（数据通道漏灌即幻影/缺值）"
      );
      assert!(
        on_primary.starts_with(b"$"),
        "抽样字段应可读，实际 {on_primary:?}"
      );
    }
    let ph = auto_exec(&papi, &rt, &mut ps, RespCommand::Hgetall, &[b"h2"]);
    let rh = auto_exec(&rapi, &rt, &mut rs, RespCommand::Hgetall, &[b"h2"]);
    assert_eq!(ph, rh, "HGETALL 全量条目须与主端逐字节一致");

    // 旧键在副本侧严格消失，新键信封域无幽灵残留
    assert_eq!(
      auto_exec(&rapi, &rt, &mut rs, RespCommand::Exists, &[b"h"]),
      b":0\r\n",
      "副本旧键应随 RangeIndexDrop 条目一并消失"
    );
    let sess = replica.store.new_session()?;
    let env_k = sess.session_tag_key(KeyTag::ObjectEnvelope, b"h2");
    assert!(
      sess.read_raw(&env_k).await?.is_none(),
      "副本信封域不应有幽灵残留"
    );
    OK
  })
}

/// RENAME 到已存在分层键（dst 覆写）：dst 换入走先建后拆原子顶替，副本以
/// replace 形态重放换入源树，无 IndexExists 拒发布、内容收敛为源树
///
/// 护栏断言：dst 换入统一经 publish_tree_from_snapshot_locked 在 dst 条带写锁内
/// rename 原子顶替（先建后拆纪律，禁手工 remove_file 直写 dst 数据路径的先删
/// 后建形——失败无回滚、窗口内并发写丢失）。副本若被 IndexExists 拦截或换入
/// 失败，h2 保留 g 系旧树内容即红。
#[test]
fn rename_tiered_dst_overwrite_swaps_replica_tree() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let total = wcol::TIERED_PROMOTE_THRESHOLD + 10;

    let primary = open_node("rename_dst_primary")?;
    let papi = api_of(&primary.store)?;
    let mut ps = session_with(&papi);

    // h1 = f 系字段、h2 = g 系字段（两棵内容可分辨的分层树）
    fill_fields(&papi, &rt, &mut ps, b"h1", b"f", 1, total);
    fill_fields(&papi, &rt, &mut ps, b"h2", b"g", 1, total);
    primary.wal.commit().await?;
    for key in [b"h1".as_slice(), b"h2"] {
      let (meta, _) = primary
        .store
        .new_session()?
        .load_collection_stub(key)
        .await?
        .expect("源/目标键均应已升阶为树态");
      assert_eq!(meta.size as usize, total);
    }

    // RENAME h1 → h2（dst 为分层键：整树换入覆盖）
    assert_eq!(
      auto_exec(&papi, &rt, &mut ps, RespCommand::Rename, &[b"h1", b"h2"]),
      b"+OK\r\n"
    );
    primary.wal.commit().await?;

    // 主端：h2 内容收敛为源树（f 系），g 系消失；h1 严格消失
    assert_eq!(
      auto_exec(&papi, &rt, &mut ps, RespCommand::Type, &[b"h2"]),
      b"+hash\r\n"
    );
    assert_eq!(
      auto_exec(&papi, &rt, &mut ps, RespCommand::Hlen, &[b"h2"]),
      format!(":{total}\r\n").into_bytes()
    );
    assert_eq!(
      auto_exec(&papi, &rt, &mut ps, RespCommand::Hget, &[b"h2", b"g1"]),
      b"$-1\r\n",
      "主端 dst 覆写后 g 系旧内容应消失"
    );
    assert!(
      auto_exec(&papi, &rt, &mut ps, RespCommand::Hget, &[b"h2", b"f1"]).starts_with(b"$"),
      "主端 h2 应为源树 f 系内容"
    );

    // 副本重放：h2 以 replace 换入重放（IndexExists 拦截即保留 g 系旧树，红）
    let replica = open_node("rename_dst_replica")?;
    let replayed = primary
      .service
      .replay_into_session(replica.service.session())
      .await?;
    assert!(replayed > 0, "副本应消费到 RENAME 数据通道条目");
    let rapi = api_of(&replica.store)?;
    let mut rs = session_with(&rapi);
    assert_eq!(
      auto_exec(&rapi, &rt, &mut rs, RespCommand::Type, &[b"h2"]),
      b"+hash\r\n",
      "副本 h2 应为源集合类型（IndexExists 拒发布会保留旧类型旧内容）"
    );
    assert_eq!(
      auto_exec(&rapi, &rt, &mut rs, RespCommand::Hget, &[b"h2", b"g1"]),
      b"$-1\r\n",
      "副本 dst 覆写必须以 replace 换入源树，g 系残留即回放被拒"
    );
    assert_eq!(
      auto_exec(&rapi, &rt, &mut rs, RespCommand::Hget, &[b"h2", b"f1"]),
      auto_exec(&papi, &rt, &mut ps, RespCommand::Hget, &[b"h2", b"f1"]),
      "副本 f1 须与主端一致（内容为源树）"
    );
    let ph = auto_exec(&papi, &rt, &mut ps, RespCommand::Hgetall, &[b"h2"]);
    let rh = auto_exec(&rapi, &rt, &mut rs, RespCommand::Hgetall, &[b"h2"]);
    assert_eq!(ph, rh, "HGETALL 全量条目须与主端逐字节一致");
    assert_eq!(
      auto_exec(&rapi, &rt, &mut rs, RespCommand::Exists, &[b"h1"]),
      b":0\r\n",
      "副本旧键 h1 应严格消失"
    );
    OK
  })
}

/// RENAME 分层键到已有 String 记录的键：dst 残留清退——无双型、无旧 TTL 借尸
///
/// 修复前：Meta 域慢路径臂无目标键清退 → dst String 记录与随键旧 TTL 存活，
/// GET 回旧字符串值（一键双型：GET 按 String 域命中、HGETALL 按 Meta 应答），
/// PTTL 报 dst 旧 TTL（源键无 TTL 时）。
#[test]
fn rename_tiered_over_string_dst_clears_residual_record_and_ttl() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let total = wcol::TIERED_PROMOTE_THRESHOLD + 10;

    let primary = open_node("rename_clr_primary")?;
    let papi = api_of(&primary.store)?;
    let mut ps = session_with(&papi);

    // h：无 TTL 分层键；k：带 String 记录 + 随键 TTL 的目标键
    fill_hash(&papi, &rt, &mut ps, b"h", total);
    let stale_payload = b"stale-string-payload";
    assert_eq!(
      auto_exec(
        &papi,
        &rt,
        &mut ps,
        RespCommand::Set,
        &[b"k", stale_payload]
      ),
      b"+OK\r\n"
    );
    assert_eq!(
      auto_exec(&papi, &rt, &mut ps, RespCommand::Expire, &[b"k", b"10000"]),
      b":1\r\n"
    );
    primary.wal.commit().await?;

    // RENAME h → k（Meta 域慢路径：dst 残留清退 → 整树迁移）
    assert_eq!(
      auto_exec(&papi, &rt, &mut ps, RespCommand::Rename, &[b"h", b"k"]),
      b"+OK\r\n"
    );
    primary.wal.commit().await?;

    // 主端断言：无双型（String 域已清退，GET 报 WRONGTYPE）、类型为 hash、
    // 源键无 TTL 时 dst 旧 TTL 一并清退（修复前残留 ~10000s 即红）
    assert!(
      auto_exec(&papi, &rt, &mut ps, RespCommand::Get, &[b"k"]).starts_with(b"-WRONGTYPE"),
      "主端 dst String 残留未清退即一键双型（GET 回旧字符串值）"
    );
    assert_eq!(
      auto_exec(&papi, &rt, &mut ps, RespCommand::Type, &[b"k"]),
      b"+hash\r\n"
    );
    assert_eq!(
      auto_exec(&papi, &rt, &mut ps, RespCommand::Pttl, &[b"k"]),
      b":-1\r\n",
      "源键无 TTL 时 dst 旧 TTL 必须清退，不得借尸还魂"
    );
    assert_eq!(
      auto_exec(&papi, &rt, &mut ps, RespCommand::Exists, &[b"h"]),
      b":0\r\n"
    );
    let expect = auto_exec(&papi, &rt, &mut ps, RespCommand::Hgetall, &[b"k"]);

    // 副本重放：同口径收敛（String 墓碑与 TTL 清退条目随 AOF 回放）
    let replica = open_node("rename_clr_replica")?;
    let replayed = primary
      .service
      .replay_into_session(replica.service.session())
      .await?;
    assert!(replayed > 0, "副本应消费到 RENAME 数据通道条目");
    let rapi = api_of(&replica.store)?;
    let mut rs = session_with(&rapi);
    assert!(
      auto_exec(&rapi, &rt, &mut rs, RespCommand::Get, &[b"k"]).starts_with(b"-WRONGTYPE"),
      "副本 dst String 残留未清退即双型"
    );
    assert_eq!(
      auto_exec(&rapi, &rt, &mut rs, RespCommand::Type, &[b"k"]),
      b"+hash\r\n"
    );
    assert_eq!(
      auto_exec(&rapi, &rt, &mut rs, RespCommand::Pttl, &[b"k"]),
      b":-1\r\n",
      "副本 dst 旧 TTL 应随清退条目一并消失"
    );
    assert_eq!(
      auto_exec(&rapi, &rt, &mut rs, RespCommand::Hgetall, &[b"k"]),
      expect,
      "HGETALL 全量条目须与主端逐字节一致"
    );
    assert_eq!(
      auto_exec(&rapi, &rt, &mut rs, RespCommand::Exists, &[b"h"]),
      b":0\r\n"
    );
    OK
  })
}

/// 主库 RENAME 纯 RangeIndex 键 → 副本端重放 → 新键索引可访问、旧键消失
#[test]
fn rename_range_index_aof_replay_converges_replica() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let primary = open_node("rename_ri_primary")?;
    let papi = api_of(&primary.store)?;
    let mut ps = session_with(&papi);

    // 纯 RangeIndex 键：RI.CREATE + 两条 RI.SET（字段与值取引擎 64B 记录下限之上）
    let field1 = [b'a'; 64];
    let field2 = [b'b'; 64];
    let val1 = [b'x'; 64];
    let val2 = [b'y'; 64];
    assert_eq!(
      auto_exec(
        &papi,
        &rt,
        &mut ps,
        RespCommand::Ricreate,
        &[b"ri", b"DISK"]
      ),
      b"+OK\r\n"
    );
    for (field, val) in [(&field1[..], &val1[..]), (&field2[..], &val2[..])] {
      assert_eq!(
        auto_exec(
          &papi,
          &rt,
          &mut ps,
          RespCommand::Riset,
          &[b"ri", field, val]
        ),
        b"+OK\r\n"
      );
    }
    primary.wal.commit().await?;

    assert_eq!(
      auto_exec(&papi, &rt, &mut ps, RespCommand::Rename, &[b"ri", b"ri2"]),
      b"+OK\r\n"
    );
    primary.wal.commit().await?;

    // 主端新键可访问（RI.COUNT 直读 Meta.size 短路）、旧键严格消失
    assert_eq!(
      auto_exec(&papi, &rt, &mut ps, RespCommand::Ricount, &[b"ri2"]),
      b":2\r\n"
    );
    assert_eq!(
      auto_exec(&papi, &rt, &mut ps, RespCommand::Exists, &[b"ri"]),
      b":0\r\n"
    );

    // 副本端全新实例重放（修复前：新键零镜像条目，副本侧 ri2 缺失）
    let replica = open_node("rename_ri_replica")?;
    let replayed = primary
      .service
      .replay_into_session(replica.service.session())
      .await?;
    assert!(replayed > 0, "副本应消费到 RENAME 数据通道条目");

    let rapi = api_of(&replica.store)?;
    let mut rs = session_with(&rapi);
    assert_eq!(
      auto_exec(&rapi, &rt, &mut rs, RespCommand::Ricount, &[b"ri2"]),
      b":2\r\n",
      "副本新键计数须与主端一致"
    );
    assert_eq!(
      auto_exec(&rapi, &rt, &mut rs, RespCommand::Type, &[b"ri2"]),
      b"+rangeindex\r\n",
      "副本新键类型须为 RangeIndex"
    );
    // 点查逐条一致（经 load_range_index_stub + 在线树原生读取）
    for (field, val) in [(&field1[..], &val1[..]), (&field2[..], &val2[..])] {
      let on_primary = auto_exec(&papi, &rt, &mut ps, RespCommand::Riget, &[b"ri2", field]);
      let on_replica = auto_exec(&rapi, &rt, &mut rs, RespCommand::Riget, &[b"ri2", field]);
      assert_eq!(
        on_primary,
        format!("${}\r\n{}\r\n", val.len(), String::from_utf8_lossy(val)).into_bytes()
      );
      assert_eq!(on_primary, on_replica, "字段 {field:?} 主从不一致");
    }
    // 旧键副本侧严格消失
    assert_eq!(
      auto_exec(&rapi, &rt, &mut rs, RespCommand::Exists, &[b"ri"]),
      b":0\r\n",
      "副本旧键应随 RangeIndexDrop 条目一并消失"
    );
    OK
  })
}

/// RENAME 带 etag 残留的 dst（曾为条件写键）：段三 ETag 旁路清退经写监听
/// KeyTag::Etag 快速分流恰发一次 EtagWrite(None) → Setwithetag(0) 条目，
/// 副本回放侧 del_etag 同调——回放终态与主端等值，副本无孤儿 etag
///
/// 对标 C# RENAME 记录级重写 HasETag=false（UnifiedStore/VarLenInputMethods.cs
/// GetUpsertFieldInfo → LogRecord.TryCopyOptionals RemoveETag）：new 记录不携带
/// ETag、dst 旧记录尾随 ETag 一体消亡；主端命令面终态基线重置（无残留错基线）
#[test]
fn rename_tiered_over_etag_dst_replay_leaves_no_orphan_etag() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let total = wcol::TIERED_PROMOTE_THRESHOLD + 10;

    let primary = open_node("rename_etag_dst_primary")?;
    let papi = api_of(&primary.store)?;
    let mut ps = session_with(&papi);

    // dst：条件写置 etag = 1（String 记录 + KeyTag::Etag 旁路在场）
    assert_eq!(
      auto_exec(
        &papi,
        &rt,
        &mut ps,
        RespCommand::Setwithetag,
        &[b"k", b"etagged-payload"]
      ),
      b":1\r\n",
      "SETWITHETAG 初写应答 etag = 1"
    );
    // h：分层树源键
    fill_hash(&papi, &rt, &mut ps, b"h", total);
    primary.wal.commit().await?;

    // RENAME h → k：段三清退 String/信封/ETag 旁路残留后整树换入
    assert_eq!(
      auto_exec(&papi, &rt, &mut ps, RespCommand::Rename, &[b"h", b"k"]),
      b"+OK\r\n"
    );
    primary.wal.commit().await?;

    // 主端：无双型、旁路缺席（条件写基线重置，非残留 1）
    assert!(
      auto_exec(&papi, &rt, &mut ps, RespCommand::Get, &[b"k"]).starts_with(b"-WRONGTYPE"),
      "主端 dst String 残留未清退即一键双型"
    );
    assert_eq!(
      auto_exec(&papi, &rt, &mut ps, RespCommand::Type, &[b"k"]),
      b"+hash\r\n"
    );
    let psess = primary.store.new_session()?;
    assert_eq!(
      psess.etag_of(b"k").await?,
      None,
      "主端 RENAME 后新键不得携带 dst 旧 etag（内核段三清退 + 调用方无 etag 源键不回填）"
    );

    // 副本重放：Setwithetag(0) 条目随 AOF 消费，副本侧无孤儿 etag、树态等值
    let replica = open_node("rename_etag_dst_replica")?;
    let replayed = primary
      .service
      .replay_into_session(replica.service.session())
      .await?;
    assert!(replayed > 0, "副本应消费到 RENAME 数据通道条目");
    let rsess = replica.store.new_session()?;
    assert_eq!(
      rsess.etag_of(b"k").await?,
      None,
      "副本回放终态不得残留孤儿 etag（清退镜像条目缺席即主从发散）"
    );
    let etag_k = rsess.etag_key(b"k");
    assert!(
      rsess.read_raw(&etag_k).await?.is_none(),
      "副本 etag 旁路物理记录应缺席"
    );

    let rapi = api_of(&replica.store)?;
    let mut rs = session_with(&rapi);
    assert_eq!(
      auto_exec(&rapi, &rt, &mut rs, RespCommand::Type, &[b"k"]),
      b"+hash\r\n"
    );
    assert_eq!(
      auto_exec(&rapi, &rt, &mut rs, RespCommand::Hlen, &[b"k"]),
      format!(":{total}\r\n").into_bytes(),
      "副本 HLEN 须与主端一致"
    );
    assert_eq!(
      auto_exec(&rapi, &rt, &mut rs, RespCommand::Exists, &[b"h"]),
      b":0\r\n",
      "副本旧键应随 RangeIndexDrop 条目一并消失"
    );
    OK
  })
}
