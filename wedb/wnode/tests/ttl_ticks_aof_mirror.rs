//! TTL 变更 AOF 镜像 ticks 精确性回归测试（票 zcode-r15-expire 发现四）
//!
//! 缺陷背景：`on_aof_store_event` 的 TtlWrite 臂收口前把主端线性化后的绝对
//! ticks 经 `unix_time_in_milliseconds_from_ticks`（整数除法截断）折成 Unix
//! 毫秒随 PEXPIREAT 条目入队，重放臂 `expire_at_milliseconds_to_ticks` 还原
//! ——落库 ticks 仅 16 对齐不保证毫秒对齐，往返恒向下截断，副本/恢复端 TTL
//! 系统性前移至多 1ms，主从 PTTL/PEXPIRETIME 可观测差 1 毫秒。修复：条目
//! arg1 改携主端绝对 .NET Ticks 原值（对标 C# WriteLogRMW 的
//! ExpirationWithOption.Word 原样入队：libs/server/Storage/Functions/
//! UnifiedStore/PrivateMethods.cs:106-118，与 EtagWrite 携 etag 原值同型），
//! 重放臂按 ticks 直设，主从/恢复后落库过期时刻逐位一致。
//!
//! 验证点（票面发现四第 3 条）：非毫秒对齐 TTL 键（EXPIRE 相对秒落库
//! 100ns 粒度 ticks，几乎必然非毫秒对齐；构造循环显式断言 `X mod 10_000
//! != 0`），副本端全新实例重放后，落库 TTL ticks 与主端逐位相等；Persist
//! 形态镜像同步收敛（副本 TTL 消失）。

use std::{str::from_utf8, sync::Arc};

use aok::Void;
use compio::runtime::Runtime;
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
  storage::session::common::ttl_sync::ttl_of_sync,
};
use wnode_test::auto_exec;
use wresp::command::RespCommand;

/// 主/副本各一套：存储 + WAL（重放经 NodeService 统一闭环）
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

/// 落库 TTL 原始 ticks 直读（单线程断言面；None = 无 TTL）
fn raw_ttl(store: &Arc<WedbStore<SegmentedDevice>>, key: &[u8]) -> aok::Result<Option<i64>> {
  let session = store.new_session()?;
  let batch = session.enter_batch();
  Ok(ttl_of_sync(&batch, key)?.value().flatten())
}

/// 非毫秒对齐 TTL 键的镜像逐位一致性：主端 EXPIRE 相对秒（now + N 秒，
/// 100ns 粒度）落库 ticks X（`X mod 10_000 != 0` 显式成立），副本端全新
/// 实例重放后落库 ticks 逐位等于 X——旧毫秒中转恒向下截断即在此分叉
#[test]
fn ttl_mirror_carries_exact_ticks_to_replica() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let primary = open_node("ttl-mirror-primary")?;
    let api = api_of(&primary.store)?;
    let mut s = session_with(&api);

    // 构造非毫秒对齐落库值：EXPIRE 相对秒 → now + 100s（100ns 粒度），
    // P(毫秒对齐) = 1/100，未对齐即重灌（覆盖式 SET+EXPIRE），至多 50 轮
    let key = b"tm:k";
    let mut expire_ticks = None;
    for _ in 0..50 {
      assert_eq!(
        auto_exec(&api, &rt, &mut s, RespCommand::Set, &[key, b"v"]),
        b"+OK\r\n"
      );
      assert_eq!(
        auto_exec(&api, &rt, &mut s, RespCommand::Expire, &[key, b"100"]),
        b":1\r\n"
      );
      let x = raw_ttl(&primary.store, key)?.expect("EXPIRE 后 TTL 必在场");
      if x % 10_000 != 0 {
        expire_ticks = Some(x);
        break;
      }
    }
    let x = expire_ticks.expect("50 轮内应构造出非毫秒对齐落库值");
    assert!(x > now_ticks(), "EXPIRE 100s 的落库值须在未来");

    primary.wal.commit().await?;

    // 副本：全新引擎实例 + 独立 WAL，零内存状态，统一重放链路闭环
    let replica = open_node("ttl-mirror-replica")?;
    let replayed = primary
      .service
      .replay_into_session(replica.service.session())
      .await?;
    assert!(replayed > 0, "副本应消费到 TTL 镜像条目");

    // 核心断言：重放端落库 ticks 与主端逐位一致
    //（收口前：毫秒中转截断 → 副本 = X - (X mod 10_000)，恒前移 <1ms）
    let y = raw_ttl(&replica.store, key)?.expect("重放后副本 TTL 必在场");
    assert_eq!(
      y,
      x,
      "副本落库 TTL ticks 须与主端逐位一致（毫秒中转截断即在此分叉：\
       主 {x} vs 副 {y}，差 {}μs）",
      x - y
    );

    // Persist 形态镜像同步收敛：主端 PERSIST → 副本 TTL 消失
    assert_eq!(
      auto_exec(&api, &rt, &mut s, RespCommand::Persist, &[key]),
      b":1\r\n"
    );
    primary.wal.commit().await?;
    let replica2 = open_node("ttl-mirror-replica2")?;
    primary
      .service
      .replay_into_session(replica2.service.session())
      .await?;
    assert_eq!(
      raw_ttl(&replica2.store, key)?,
      None,
      "Persist 重放后副本 TTL 必消失"
    );

    // 命令面（副本 PTTL/PEXPIRETIME 可读、值域合理）：副本端按 ticks 直设，
    // PTTL 剩余毫秒须落在 (0, 100_000]（与主端 EXPIRE 100s 口径一致，非
    // 提前 1ms 消失的畸形面）
    assert_eq!(
      auto_exec(&api, &rt, &mut s, RespCommand::Set, &[b"tm:k2", b"v"]),
      b"+OK\r\n"
    );
    assert_eq!(
      auto_exec(&api, &rt, &mut s, RespCommand::Expire, &[b"tm:k2", b"100"]),
      b":1\r\n"
    );
    primary.wal.commit().await?;
    let replica3 = open_node("ttl-mirror-replica3")?;
    primary
      .service
      .replay_into_session(replica3.service.session())
      .await?;
    let rapi3 = api_of(&replica3.store)?;
    let mut rs3 = session_with(&rapi3);
    let pttl3 = auto_exec(&rapi3, &rt, &mut rs3, RespCommand::Pttl, &[b"tm:k2"]);
    let v: i64 = from_utf8(&pttl3)?
      .trim_end_matches("\r\n")
      .trim_start_matches(':')
      .parse()?;
    assert!(
      (0..=100_000).contains(&v),
      "副本 PTTL 应为 (0s, 100s] 毫秒值域：{v}"
    );
    let pexpiretime = auto_exec(&rapi3, &rt, &mut rs3, RespCommand::Pexpiretime, &[b"tm:k2"]);
    let t: i64 = from_utf8(&pexpiretime)?
      .trim_end_matches("\r\n")
      .trim_start_matches(':')
      .parse()?;
    assert!(t > 0, "副本 PEXPIRETIME 应为正毫秒时刻：{t}");

    Ok(())
  })
}
