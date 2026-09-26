//! GETEX 换算归并双侧对拍锁（票 zcode-r149c-setexabs 案二，deviations.md §143）
//!
//! 背景：GETEX EX/PX/EXAT/PXAT 的 epoch 乘加在命令层曾存第三份手写形，
//! 归并至 `wbase::convert::try_expire_after_to_ticks` /
//! `try_expire_at_to_ticks` checked 通用形单源后**绝对/相对域行为逐位
//! 不变**。本测以真命令链路（RespServerSession + GarnetApi，零 mock）焊死
//! 归并前后同判据：
//! - 绝对域（EXAT/PXAT）：gate 内落库 ticks 与 C# 同名换算
//!   （garnet/libs/common/ConvertUtils.cs UnixTimestampIn*ToTicks 的 rust
//!   单源 `unix_timestamp_in_{seconds,milliseconds}_to_ticks`）逐位相等，
//!   含 gate 上界 `MAX_UNIX_TIME_SECONDS` 恰点（非饱和非钳制，直乘直加）；
//! - 相对域（EX/PX）：落库 ticks 落在 [now_before + d, now_after + d]
//!   闭区间（同一 now 读取先后夹逼，逐位对拍由换算层单元测试与
//!   convert::tests::try_convert_parities 承担）；
//! - gate 帧：EX 超 TimeSpan 上界 / EXAT 超 DateTimeOffset 上界 →
//!   INVALIDEXP；PX 取 gate 上界自身（正滴答 now 下加法必越 i64::MAX）→
//!   OVERFLOWEXP；非正值 → value is out of range；
//! - 绝对过去值：折算不在未来 → 既有 TTL 原样保留（GetexExpiry::None
//!   不动臂，BasicCommands.cs:175 三态）。

use std::sync::Arc;

use aok::Void;
use compio::runtime::Runtime;
use tempfile::tempdir;
use waof::{WalConfig, WalLog};
use wbase::{
  convert::{
    TICKS_PER_MILLISECOND, TICKS_PER_SECOND, unix_timestamp_in_milliseconds_to_ticks,
    unix_timestamp_in_seconds_to_ticks,
  },
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
use wresp::{cmd_strings as cs, command::RespCommand};

struct Node {
  store: Arc<WedbStore<SegmentedDevice>>,
  _service: NodeService<SegmentedDevice>,
  _wal: Arc<WalLog<SegmentedDevice>>,
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
    _service: service,
    _wal: wal,
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

fn err_frame(msg: &str) -> Vec<u8> {
  format!("-{msg}\r\n").into_bytes()
}

/// 落库 TTL 原始 ticks 直读（None = 无 TTL）
fn raw_ttl(store: &Arc<WedbStore<SegmentedDevice>>, key: &[u8]) -> aok::Result<Option<i64>> {
  let session = store.new_session()?;
  let batch = session.enter_batch();
  Ok(ttl_of_sync(&batch, key)?.value().flatten())
}

/// 绝对域逐位锁：EXAT/PXAT（含 gate 上界恰点）落库 ticks 与 wbase 换算
/// 单点逐位相等；相对域 EX/PX now 夹逼；过去 EXAT 既有 TTL 不动
#[test]
fn getex_absolute_relative_domains_bitwise_after_convert_merge() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let node = open_node("getex-parity")?;
    let api = api_of(&node.store)?;
    let mut s = session_with(&api);

    // EXAT 常规点：2049-01-30（未来），非饱和直乘直加逐位等换算单点
    assert_eq!(
      auto_exec(&api, &rt, &mut s, RespCommand::Set, &[b"g:exat", b"v"]),
      b"+OK\r\n"
    );
    assert_eq!(
      auto_exec(
        &api,
        &rt,
        &mut s,
        RespCommand::Getex,
        &[b"g:exat", b"EXAT", b"2500000000"]
      ),
      b"$1\r\nv\r\n"
    );
    assert_eq!(
      raw_ttl(&node.store, b"g:exat")?.expect("EXAT 后 TTL 在场"),
      unix_timestamp_in_seconds_to_ticks(2_500_000_000),
      "EXAT 落库须与换算单点逐位相等（归并前后同判据）"
    );

    // EXAT gate 上界恰点（<= max_val 放行，DateTimeOffset.MaxValue 域）
    assert_eq!(
      auto_exec(
        &api,
        &rt,
        &mut s,
        RespCommand::Getex,
        &[b"g:exat", b"EXAT", b"253402300799"]
      ),
      b"$1\r\nv\r\n"
    );
    assert_eq!(
      raw_ttl(&node.store, b"g:exat")?.expect("EXAT cap 后 TTL 在场"),
      unix_timestamp_in_seconds_to_ticks(253_402_300_799),
      "EXAT gate 上界恰点须逐位等换算单点"
    );

    // PXAT 毫秒域
    assert_eq!(
      auto_exec(&api, &rt, &mut s, RespCommand::Set, &[b"g:pxat", b"v"]),
      b"+OK\r\n"
    );
    assert_eq!(
      auto_exec(
        &api,
        &rt,
        &mut s,
        RespCommand::Getex,
        &[b"g:pxat", b"PXAT", b"2500000000000"]
      ),
      b"$1\r\nv\r\n"
    );
    assert_eq!(
      raw_ttl(&node.store, b"g:pxat")?.expect("PXAT 后 TTL 在场"),
      unix_timestamp_in_milliseconds_to_ticks(2_500_000_000_000),
      "PXAT 落库须与毫秒域换算单点逐位相等"
    );

    // 相对域 EX/PX：读写 now 夹逼（换算式 now + d*scale 的确定性区间）
    for (key, opt, num, ticks_per) in [
      (
        b"g:ex".as_slice(),
        b"EX".as_slice(),
        60i64,
        TICKS_PER_SECOND,
      ),
      (
        b"g:px".as_slice(),
        b"PX".as_slice(),
        60_000i64,
        TICKS_PER_MILLISECOND,
      ),
    ] {
      assert_eq!(
        auto_exec(&api, &rt, &mut s, RespCommand::Set, &[key, b"v"]),
        b"+OK\r\n"
      );
      let num_bytes = num.to_string();
      let before = now_ticks();
      assert_eq!(
        auto_exec(
          &api,
          &rt,
          &mut s,
          RespCommand::Getex,
          &[key, opt, num_bytes.as_bytes()]
        ),
        b"$1\r\nv\r\n"
      );
      let after = now_ticks();
      let stored = raw_ttl(&node.store, key)?.expect("相对域 GETEX 后 TTL 在场");
      assert!(
        (before + num * ticks_per..=after + num * ticks_per).contains(&stored),
        "相对域 {opt:?} 落库须在 now 夹逼区间内：before={} after={} stored={stored}",
        before + num * ticks_per,
        after + num * ticks_per
      );
    }

    // 过去绝对值：折算不在未来 → 既有 TTL 原样保留（不动臂）
    assert_eq!(
      auto_exec(
        &api,
        &rt,
        &mut s,
        RespCommand::Setex,
        &[b"g:keep", b"600", b"v"]
      ),
      b"+OK\r\n"
    );
    let keep = raw_ttl(&node.store, b"g:keep")?.expect("SETEX 后 TTL 在场");
    assert_eq!(
      auto_exec(
        &api,
        &rt,
        &mut s,
        RespCommand::Getex,
        &[b"g:keep", b"EXAT", b"1"]
      ),
      b"$1\r\nv\r\n"
    );
    assert_eq!(
      raw_ttl(&node.store, b"g:keep")?.expect("EXAT 过去值后 TTL 仍在场"),
      keep,
      "过去绝对值折算不在未来 → 既有 TTL 逐位不动"
    );

    // gate 帧分流：EXAT 超 cap / EX 超 TimeSpan 上界 → INVALIDEXP；
    // PX 取上界自身（加法越 i64::MAX）→ OVERFLOWEXP；非正值 → 越界帧
    assert_eq!(
      auto_exec(
        &api,
        &rt,
        &mut s,
        RespCommand::Getex,
        &[b"g:exat", b"EXAT", b"253402300800"]
      ),
      err_frame(cs::RESP_ERR_GENERIC_INVALIDEXP_IN_GETEX)
    );
    assert_eq!(
      auto_exec(
        &api,
        &rt,
        &mut s,
        RespCommand::Getex,
        &[b"g:ex", b"EX", b"922337203686"]
      ),
      err_frame(cs::RESP_ERR_GENERIC_INVALIDEXP_IN_GETEX)
    );
    assert_eq!(
      auto_exec(
        &api,
        &rt,
        &mut s,
        RespCommand::Getex,
        &[b"g:px", b"PX", b"922337203685477"]
      ),
      err_frame(cs::RESP_ERR_OVERFLOWEXP_IN_GETEX)
    );
    assert_eq!(
      auto_exec(
        &api,
        &rt,
        &mut s,
        RespCommand::Getex,
        &[b"g:ex", b"EX", b"0"]
      ),
      err_frame(cs::RESP_ERR_GENERIC_VALUE_IS_OUT_OF_RANGE)
    );

    // 帧分流后键态不受污染（gate 在写路径之前）
    assert_eq!(
      raw_ttl(&node.store, b"g:exat")?.expect("g:exat TTL 仍在场"),
      unix_timestamp_in_seconds_to_ticks(253_402_300_799),
      "gate 拒绝帧不得改写既有 TTL"
    );

    Ok(())
  })
}
