//! TTL 重放臂逐位精度回归（票 zcode-r149c-setexabs 案一，deviations.md §143）
//!
//! 缺陷背景：`wkv::StoreSession::expire_at` 会话入口头部曾无条件施加 4-bit
//! coarse 粗化，宣称「护住 AOF 重放/迁移导入/复制应用三个外部入口且对
//! 16 对齐值幂等恒等」——对 SETEX/SET EX/GETEX 族不成立：这些命令按 C#
//! 一手形态存**裸全精度 ticks**（garnet/libs/server/Resp/BasicCommands.cs
//! :552-555 直取 `DateTimeOffset.UtcNow.Ticks` 入 `StringInput`，存储侧
//! MainStore/RMWMethods.cs TrySetExpiration 裸写；粗化唯二参打包构造器
//! ExpirationWithOption.cs:20-24 的 EXPIRE 族路径），其落库值低 4 位以
//! 15/16 概率非零，重放臂经旧头部闸被清低 4 位，副本与主端存值分叉
//! （≤15 ticks = 1.5μs 提前过期）。修复形态：粗化唯 wnode `network_expire`
//! 命令边界一处施加，会话入口与内核恒等裸写（对标 C# 存储侧 word 形恒等
//! 装载 UnifiedStore/RMWMethods.cs:216、:228）。
//!
//! 判据（真链路闭环，零 mock）：主端经命令面（SETEX / SET EX / GETEX EX）
//! 构造非 16 对齐 TTL 键 + EXPIRE 对照键（低 4 位恒零），wal.commit 后经
//! NodeService::replay_into_session 全量入副本新实例，逐键断言副本落库
//! ticks 与主端**逐位相等**；同锁收口后 EXPIRE 族边界粗化单机制在场。

use std::{str::from_utf8, sync::Arc};

use aok::Void;
use compio::runtime::Runtime;
use tempfile::tempdir;
use waof::{WalConfig, WalLog};
use wbase::{
  convert::{TICKS_PER_SECOND, unix_time_in_milliseconds_from_ticks},
  time::now_ticks,
};
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

/// 主端命令面写入四条 TTL 键并回读落库 ticks：
/// (setex, set_ex, getex_ex, expire)。前三族为裸 ticks（低 4 位以 15/16
/// 概率非零），任一命中对齐值即覆盖重写重灌（各键覆盖式重写换新 now，
/// 至多 50 轮，确定性收敛）；expire 对照键恒 16 对齐
fn seed_command_surface(
  api: &GarnetApi,
  rt: &Runtime,
  s: &mut RespServerSession,
  store: &Arc<WedbStore<SegmentedDevice>>,
) -> aok::Result<[i64; 4]> {
  let keys: [&[u8]; 4] = [b"rp:setex", b"rp:setex_ex", b"rp:getex", b"rp:expire"];
  for round in 0..50 {
    assert_eq!(
      auto_exec(api, rt, s, RespCommand::Setex, &[keys[0], b"60", b"v"]),
      b"+OK\r\n"
    );
    assert_eq!(
      auto_exec(api, rt, s, RespCommand::Set, &[keys[1], b"v", b"EX", b"60"]),
      b"+OK\r\n"
    );
    assert_eq!(
      auto_exec(api, rt, s, RespCommand::Set, &[keys[2], b"v"]),
      b"+OK\r\n"
    );
    assert_eq!(
      auto_exec(api, rt, s, RespCommand::Getex, &[keys[2], b"EX", b"60"]),
      b"$1\r\nv\r\n"
    );
    assert_eq!(
      auto_exec(api, rt, s, RespCommand::Set, &[keys[3], b"v"]),
      b"+OK\r\n"
    );
    assert_eq!(
      auto_exec(api, rt, s, RespCommand::Expire, &[keys[3], b"60"]),
      b":1\r\n"
    );
    let mut stored = [0i64; 4];
    for (slot, key) in stored.iter_mut().zip(keys) {
      *slot = raw_ttl(store, key)?.expect("命令面写入后 TTL 必在场");
    }
    // 裸 ticks 三族全部非 16 对齐（旧头部闸下必分叉）+ 对照键恒对齐
    if stored[..3].iter().all(|x| x & 0xF != 0) && stored[3] & 0xF == 0 {
      return Ok(stored);
    }
    assert!(round < 49, "50 轮内应构造出三族非对齐 + 对照对齐形态");
  }
  unreachable!()
}

/// 真链路主副逐位对拍：主端命令面（SETEX/SET EX/GETEX EX 裸 ticks +
/// EXPIRE 边界粗化对照）落库后，副本全新实例经统一重放链路逐键位等
#[test]
fn ttl_replay_bitwise_parity_for_raw_ticks_families() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let primary = open_node("replay-parity-primary")?;
    let api = api_of(&primary.store)?;
    let mut s = session_with(&api);

    let stored = seed_command_surface(&api, &rt, &mut s, &primary.store)?;
    let now = now_ticks();
    for x in stored {
      assert!(x > now + 59 * TICKS_PER_SECOND, "种子 TTL 在未来");
    }

    primary.wal.commit().await?;

    let replica = open_node("replay-parity-replica")?;
    let replayed = primary
      .service
      .replay_into_session(replica.service.session())
      .await?;
    assert!(replayed > 0, "副本应消费到 TTL 镜像条目");

    let keys: [&[u8]; 4] = [b"rp:setex", b"rp:setex_ex", b"rp:getex", b"rp:expire"];
    let family = ["SETEX", "SET EX", "GETEX EX", "EXPIRE（对照）"];
    for ((key, expected), name) in keys.iter().zip(stored).zip(family) {
      let got = raw_ttl(&replica.store, key)?.expect("重放后副本 TTL 必在场");
      assert_eq!(
        got,
        expected,
        "{name} 族副本落库 ticks 须与主端逐位一致（旧会话入口头部粗化即在此\
         分叉：主 {expected} vs 副 {got}，差 {} ticks）",
        expected - got
      );
    }

    // 副本命令面读值域合理（重放臂存的是原值而非 0 哨兵）：PEXPIRETIME
    // 毫秒时刻与主端落库 ticks 经换算单点折算逐位一致
    let rapi = api_of(&replica.store)?;
    let mut rs = session_with(&rapi);
    for (key, expected) in keys.iter().zip(stored) {
      let frame = auto_exec(&rapi, &rt, &mut rs, RespCommand::Pexpiretime, &[key]);
      let ms: i64 = from_utf8(&frame)?
        .trim_end_matches("\r\n")
        .trim_start_matches(':')
        .parse()?;
      assert_eq!(
        ms,
        unix_time_in_milliseconds_from_ticks(expected),
        "副本 PEXPIRETIME 须与主端落库 ticks 经换算单点折算一致：{key:?}"
      );
    }

    Ok(())
  })
}
