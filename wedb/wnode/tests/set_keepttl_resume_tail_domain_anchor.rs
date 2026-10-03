//! SET/RESTORE/ETag 续跑尾参跨换号幽灵 TTL 回归锁（票
//! wnode-set-keepttl-resume-tail-no-domain-anchor-ghost-ttl-across-swap）
//!
//! 缺陷面：SET 族降级重放尾参只携过期刻度不携物理域锚。快臂在持窗内读得旧
//! TTL 刻度（KEEPTTL 置 KeepTtl / 值提交后 TTL 降级置 Pending / GET 成帧保留
//! 置 ReplyEcho / ETag 余腿降级置 EtagResume::Pending），exec 降级快照尾参
//! 追加与慢臂 exec_slow 之间隔 await 调度窗，他会话并发 FLUSHDB/FLUSHNS/
//! SWAPDB 的 bump_generation 落入即换号：慢臂 replay 的 session_prefix 改指
//! 新代域，KeepTtl 直取快照刻度把死域旧键的过期刻度盖进新域键成幽灵 TTL
//! （键在用户视角刚被清库抹零后重写、理应无 TTL，却于未来某刻静默消失）；
//! Pending/ReplyEcho/EtagResume::Pending 补投余腿在新域留孤 TTL 旁路。
//! C# 对照面（BasicCommands.cs:772-847）：KEEPTTL 读旧回填与值写在单记录
//! RMW 记录闩锁内一体，FLUSHDB 整库独立实例截断（DatabaseManagerBase.cs
//! :301），线性化序里不存在「TTL 判据取自一域、落笔落另一域」的交叠形。
//!
//! 修复形态：TtlResume / EtagResume 尾参随刻度同点捕获物理域锚 `(vns, vdb)`
//! （virtual_domain 单源投影，非第二套域身份），TAIL_LEN 单源扩宽；慢臂
//! 剥参后与当前解析域现比——同域语义逐字节不变，跨域 KeepTtl 弃刻度改现读
//! 当前域 ttl_of（Full 臂既有现读式，回到 C# 线性化终态新键无 TTL）、
//! Pending/ReplyEcho/EtagResume::Pending 跳余腿补投只按原契约出应答帧。
//!
//! 测试全真存储真协议帧，无 mock：换号注入体采 migrate_cross_generation_
//! ttl.rs:122-160 flush_db 真原语先例（vdb.flush_db 换号段 → DbMeta 原子批
//! 入账）；慢臂直驱（带尾参快照，与降级快照投递同径）锁确定性跨域契约，
//! e2e 形在「快臂降级后、慢臂复窗前」的泵间隙插并发 FLUSHDB 锁端到端窗。

use std::{
  mem::take,
  str::from_utf8,
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
};

use compio::runtime::Runtime;
use parking_lot::Mutex;
use wbase::{convert::expire_after_to_ticks, crc64::hash as crc64_hash, time::now_ticks};
use wdev::SegmentedDevice;
use wkv::{DbMetaRecord, SessionLocking, WedbStore};
use wnode::resp::{
  EtagResume, TtlLeg, TtlResume, garnet_api::GarnetApi, resp_server_session::RespServerSession,
  slow_path::SlowWait,
};
use wnode_test::{degrade_env, session_on as session_with, slow_direct_resume as slow_direct};
use wresp::{command::RespCommand, length::try_write_length};

const EXPIRE_SECONDS: &[u8] = b"100";
const PTTL_UPPER_MS: i64 = 130_000;
const REPLY_OK: &[u8] = b"+OK\r\n";

/// 单命令往返泵（nx_conditional_ttl_degrade_replay.rs 同款）；`inject` 在
/// 快臂降级挂起后、慢臂复窗前（挂起体 resolve 前）执行——正是 bump_generation
/// 可落入的 await 调度窗，e2e 注入点
fn exec_inject(
  rt: &Runtime,
  api: &GarnetApi,
  s: &mut RespServerSession,
  cmd: RespCommand,
  args: &[&[u8]],
  inject: Option<&dyn Fn()>,
) -> (Vec<u8>, bool) {
  s.output.clear();
  api.exec(s, cmd, args);
  match s.take_slow_wait() {
    Some(slow) => {
      if let Some(f) = inject {
        f();
      }
      let mut out = take(&mut s.output);
      out.extend(rt.block_on(slow.resolve()));
      (out, true)
    }
    None => (take(&mut s.output), false),
  }
}

fn exec(
  rt: &Runtime,
  api: &GarnetApi,
  s: &mut RespServerSession,
  cmd: RespCommand,
  args: &[&[u8]],
) -> Vec<u8> {
  exec_inject(rt, api, s, cmd, args, None).0
}

/// ETag 族慢臂直驱（同径，尾参为 EtagResume 线形）
fn etag_slow_direct(
  rt: &Runtime,
  api: &GarnetApi,
  cmd: RespCommand,
  args: &[&[u8]],
  resume: EtagResume,
) -> Vec<u8> {
  let mut snapshot: Vec<Vec<u8>> = args.iter().map(|a| a.to_vec()).collect();
  snapshot.push(resume.tail_bytes());
  rt.block_on(
    SlowWait::for_command(
      api,
      cmd,
      snapshot,
      wconf::DEFAULT_RESP_VERSION,
      SessionLocking::Basic,
    )
    .resolve(),
  )
}

/// FLUSHDB 内核真原语注入体（migrate_cross_generation_ttl.rs:122-160 同款：
/// vdb.flush_db 换号段 → [新映射, 旧域墓碑, 0x05 分配水位] 原子批入账）；
/// `expect_sync`：true = 空闲窗调用，DbMeta 批须同步落盘（先例同款断言）；
/// false = 降级窗内注入——环形页正翻转，批同步落盘走引擎正常降级异步回放
/// 路径（vdb.flush_db 内存换号已即时生效，持久化入账异步补账不阻域锚
/// 测试语义），仅要求批入账本身无引擎错
fn flushdb_kernel(store: &Arc<WedbStore<SegmentedDevice>>, logic_db: u64, expect_sync: bool) {
  let expired_at =
    expire_after_to_ticks(now_ticks(), store.config.gc.db_gc_reclaim_delay_secs as i64);
  let tail_address = store.tail_address();
  let (vns, new_vdb, old_vdb_opt) = store.vdb.flush_db(0, logic_db, expired_at, tail_address);
  let old_vdb = old_vdb_opt.expect("既有映射换出旧号");
  let session = store.new_session().expect("注入体解析会话");
  let degraded = session
    .try_persist_dbmeta_sync(&[
      Some(DbMetaRecord::DbMap {
        vns,
        logic_db,
        vdb: new_vdb,
      }),
      Some(DbMetaRecord::GcDeadDb {
        expired_at,
        vns,
        old_vdb,
        tail_address,
      }),
      Some(DbMetaRecord::NextId {
        next_virtual_id: store.vdb.next_virtual_id.load(Ordering::Relaxed),
      }),
    ])
    .expect("flush 换号批入账");
  if expect_sync {
    assert!(degraded.is_empty(), "flush 批同步落盘，不允许降级未落");
  }
}

/// 当前会话同域锚（经 vdb 解析面单源取值）
fn cur_domain(store: &Arc<WedbStore<SegmentedDevice>>) -> (u64, u64) {
  let (vns, vdb, ..) = store.vdb.get_virtual_ids_with_created(0, 0);
  (vns, vdb)
}

fn parse_resp_int(resp: &[u8]) -> Option<i64> {
  let body = resp.strip_prefix(b":")?;
  from_utf8(body.strip_suffix(b"\r\n")?).ok()?.parse().ok()
}

fn pttl_ms(rt: &Runtime, api: &GarnetApi, s: &mut RespServerSession, key: &[u8]) -> i64 {
  let out = exec(rt, api, s, RespCommand::Pttl, &[key]);
  parse_resp_int(&out).unwrap_or_else(|| panic!("PTTL 应答须为整数: {out:?}"))
}

fn get_val(rt: &Runtime, api: &GarnetApi, s: &mut RespServerSession, key: &[u8]) -> Vec<u8> {
  exec(rt, api, s, RespCommand::Get, &[key])
}

/// `[etag, value]` / `[etag, nil]` 期望帧单源（RESP2，与 write_etag_val_array 同形）
fn etag_pair(etag: i64, val: Option<&[u8]>) -> Vec<u8> {
  let mut out = format!("*2\r\n:{etag}\r\n").into_bytes();
  match val {
    Some(v) => {
      out.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
      out.extend_from_slice(v);
      out.extend_from_slice(b"\r\n");
    }
    None => out.extend_from_slice(b"$-1\r\n"),
  }
  out
}

fn bulk_frame(val: &[u8]) -> Vec<u8> {
  let mut frame = format!("${}\r\n", val.len()).into_bytes();
  frame.extend_from_slice(val);
  frame.extend_from_slice(b"\r\n");
  frame
}

/// 合法 RESTORE 载荷（类型 0x00 + 长度前缀 + 值 + rdb 版本 11 + crc64，
/// nx_conditional_ttl_degrade_replay.rs 同款单源形制）
fn restore_payload(val: &[u8]) -> Vec<u8> {
  let mut encoded_len = [0u8; 5];
  let written = try_write_length(val.len() as u32, &mut encoded_len).unwrap();
  let mut payload = Vec::new();
  payload.push(0x00);
  payload.extend_from_slice(&encoded_len[..written]);
  payload.extend_from_slice(val);
  payload.extend_from_slice(&11u16.to_le_bytes());
  let crc = crc64_hash(&payload);
  payload.extend_from_slice(&crc);
  payload
}

/// 验证点 1（KeepTtl 跨域直驱）：快臂旧域读得刻度降级（值未提交形），慢臂
/// 复窗前 FLUSHDB 换号——重放落值新域，KEEPTTL 域比对失配弃刻度改现读当前
/// 域 ttl_of（新域键缺席 → None）→ 新键无 TTL；同域对照（无换号直驱）刻度
/// 回填语义逐字节不变。修复前跨域直取刻度 = 死域刻度盖进新域键的幽灵 TTL
#[test]
fn keepttl_tail_cross_domain_rereads_new_domain_ttl() {
  let (rt, api, store, _dir) = degrade_env("dom-anchor-keepttl.db");
  let mut s = session_with(&api);
  let ticks = now_ticks() + 100 * 10_000_000;

  // 同域对照：KeepTtl 尾参域锚 = 慢臂当前解析域 → 刻度直取回填
  assert_eq!(
    exec(
      &rt,
      &api,
      &mut s,
      RespCommand::Set,
      &[b"ck", b"v1", b"EX", EXPIRE_SECONDS]
    ),
    REPLY_OK
  );
  let kept = slow_direct(
    &rt,
    &api,
    RespCommand::Set,
    &[b"ck", b"v2", b"KEEPTTL"],
    TtlResume::KeepTtl(TtlLeg {
      ticks,
      domain: cur_domain(&store),
    }),
  );
  assert_eq!(kept, REPLY_OK, "同域 KeepTtl 续跑应答帧与基线逐字节等");
  assert!(
    (1..=PTTL_UPPER_MS).contains(&pttl_ms(&rt, &api, &mut s, b"ck")),
    "同域 KeepTtl 刻度回填语义零漂移"
  );

  // 跨域：尾参携换号前旧域锚，慢臂现解析落新空域
  let stale = cur_domain(&store);
  flushdb_kernel(&store, 0, true);
  assert_ne!(cur_domain(&store), stale, "注入前提：换号已生效");
  let replayed = slow_direct(
    &rt,
    &api,
    RespCommand::Set,
    &[b"dk", b"v2", b"KEEPTTL"],
    TtlResume::KeepTtl(TtlLeg {
      ticks,
      domain: stale,
    }),
  );
  assert_eq!(
    replayed, REPLY_OK,
    "跨域 KeepTtl 重放应答帧形与无换号基线逐字节等（+OK）"
  );
  assert_eq!(
    get_val(&rt, &api, &mut s, b"dk"),
    bulk_frame(b"v2"),
    "跨域 KeepTtl 重放照常落值新域（写路径不受域比对影响）"
  );
  assert_eq!(
    pttl_ms(&rt, &api, &mut s, b"dk"),
    -1,
    "跨域 KeepTtl 弃死域刻度改现读：新键无 TTL（修复前旧域刻度盖进新域 → 幽灵 TTL）"
  );
}

/// 验证点 2（Pending 跨域直驱）：快臂值已提交旧域、TTL 待投降级，慢臂复窗前
/// 换号——跳余腿补投只出 +OK：新域零孤 TTL 旁路（值已随死域退役）；同域
/// 对照补投语义不变。修复前补投把旧域刻度落进新域成无人认领的孤 TTL
#[test]
fn ttl_pending_tail_cross_domain_skips_ttl_leg() {
  let (rt, api, store, _dir) = degrade_env("dom-anchor-pending.db");
  let mut s = session_with(&api);
  let ticks = now_ticks() + 100 * 10_000_000;

  // 同域对照：值已由快臂同款通道提交，Pending 尾参同域补投
  assert_eq!(
    exec(&rt, &api, &mut s, RespCommand::Set, &[b"cp", b"committed"]),
    REPLY_OK
  );
  let healed = slow_direct(
    &rt,
    &api,
    RespCommand::Set,
    &[b"cp", b"v1", b"EX", EXPIRE_SECONDS, b"NX"],
    TtlResume::Pending(TtlLeg {
      ticks,
      domain: cur_domain(&store),
    }),
  );
  assert_eq!(healed, REPLY_OK);
  assert!(
    (1..=PTTL_UPPER_MS).contains(&pttl_ms(&rt, &api, &mut s, b"cp")),
    "同域 Pending 补投语义零漂移"
  );

  // 跨域：值腿已随死域退役，禁把旧域刻度补投进新域
  let stale = cur_domain(&store);
  flushdb_kernel(&store, 0, true);
  assert_ne!(cur_domain(&store), stale);
  let out = slow_direct(
    &rt,
    &api,
    RespCommand::Set,
    &[b"dp", b"v1", b"EX", EXPIRE_SECONDS, b"NX"],
    TtlResume::Pending(TtlLeg {
      ticks,
      domain: stale,
    }),
  );
  assert_eq!(out, REPLY_OK, "跨域 Pending 只按原契约出 +OK 应答帧");
  assert_eq!(
    get_val(&rt, &api, &mut s, b"dp"),
    b"$-1\r\n",
    "跨域 Pending 值不重放（值腿已随死域退役）"
  );
  assert_eq!(
    pttl_ms(&rt, &api, &mut s, b"dp"),
    -2,
    "跨域 Pending 新域零孤 TTL 旁路（值腿随死域退役键缺席，PTTL 按 REPLY_TTL_MISSING 回 -2；修复前补投落孤刻度回正数）"
  );
}

/// 验证点 3（ReplyEcho 跨域直驱）：GET 成帧保留形值已提交旧域，慢臂复窗前
/// 换号——跳补投出零字节；同域对照补投语义不变
#[test]
fn replyecho_tail_cross_domain_skips_ttl_leg() {
  let (rt, api, store, _dir) = degrade_env("dom-anchor-replyecho.db");
  let mut s = session_with(&api);
  let ticks = now_ticks() + 100 * 10_000_000;

  // 同域对照：ReplyEcho 补投 TTL 出零字节
  assert_eq!(
    exec(&rt, &api, &mut s, RespCommand::Set, &[b"cr", b"committed"]),
    REPLY_OK
  );
  let echo = slow_direct(
    &rt,
    &api,
    RespCommand::Set,
    &[b"cr", b"newer", b"EX", EXPIRE_SECONDS, b"GET"],
    TtlResume::ReplyEcho(TtlLeg {
      ticks,
      domain: cur_domain(&store),
    }),
  );
  assert_eq!(echo, b"", "同域 ReplyEcho 零字节帧语义零漂移");
  assert!(
    (1..=PTTL_UPPER_MS).contains(&pttl_ms(&rt, &api, &mut s, b"cr")),
    "同域 ReplyEcho 补投语义零漂移"
  );

  // 跨域：跳补投仍出零字节（应答已由快臂保留）
  let stale = cur_domain(&store);
  flushdb_kernel(&store, 0, true);
  assert_ne!(cur_domain(&store), stale);
  let echo = slow_direct(
    &rt,
    &api,
    RespCommand::Set,
    &[b"dr", b"newer", b"EX", EXPIRE_SECONDS, b"GET"],
    TtlResume::ReplyEcho(TtlLeg {
      ticks,
      domain: stale,
    }),
  );
  assert_eq!(echo, b"", "跨域 ReplyEcho 仍零字节（帧形不变）");
  assert_eq!(
    pttl_ms(&rt, &api, &mut s, b"dr"),
    -2,
    "跨域 ReplyEcho 跳补投：新域零孤 TTL 旁路（值腿随死域退役键缺席，PTTL 回 -2；修复前补投落孤刻度）"
  );
}

/// 验证点 4（RESTORE Pending 跨域直驱）：快臂值已提交旧域、TTL 待投降级，
/// 慢臂复窗前换号——+OK 帧不变、新域无键无 TTL；同域对照补投闭环
#[test]
fn restore_pending_tail_cross_domain_no_orphan_ttl() {
  let (rt, api, store, _dir) = degrade_env("dom-anchor-restore.db");
  let mut s = session_with(&api);
  let ticks = now_ticks() + 100 * 10_000_000;

  // 同域对照：值在、TTL 待投，Pending 尾参补投回 +OK
  assert_eq!(
    exec(
      &rt,
      &api,
      &mut s,
      RespCommand::Restore,
      &[b"cr", b"0", &restore_payload(b"v1")]
    ),
    REPLY_OK
  );
  let healed = slow_direct(
    &rt,
    &api,
    RespCommand::Restore,
    &[b"cr", EXPIRE_SECONDS, &restore_payload(b"v1")],
    TtlResume::Pending(TtlLeg {
      ticks,
      domain: cur_domain(&store),
    }),
  );
  assert_eq!(healed, REPLY_OK);
  assert!(
    (1..=PTTL_UPPER_MS).contains(&pttl_ms(&rt, &api, &mut s, b"cr")),
    "同域 RESTORE Pending 补投语义零漂移"
  );

  // 跨域：值腿已随死域退役，禁补投
  let stale = cur_domain(&store);
  flushdb_kernel(&store, 0, true);
  assert_ne!(cur_domain(&store), stale);
  let out = slow_direct(
    &rt,
    &api,
    RespCommand::Restore,
    &[b"dr", EXPIRE_SECONDS, &restore_payload(b"v1")],
    TtlResume::Pending(TtlLeg {
      ticks,
      domain: stale,
    }),
  );
  assert_eq!(out, REPLY_OK, "跨域 RESTORE Pending 只按原契约出 +OK 帧");
  assert_eq!(get_val(&rt, &api, &mut s, b"dr"), b"$-1\r\n");
  assert_eq!(
    pttl_ms(&rt, &api, &mut s, b"dr"),
    -2,
    "跨域 RESTORE 新域零孤 TTL 旁路（值腿随死域退役键缺席，PTTL 回 -2；修复前补投落孤刻度）"
  );
}

/// 验证点 5（EtagResume::Pending 跨域直驱）：ETag 写族值腿已提交旧域、
/// TTL / etag 余腿降级，慢臂复窗前换号——成功帧逐字节不变、TTL 与 etag
/// 余腿均不落新域；同域对照余腿补投闭环
#[test]
fn etag_pending_tail_cross_domain_skips_legs() {
  let (rt, api, store, _dir) = degrade_env("dom-anchor-etag.db");
  let mut s = session_with(&api);
  let ticks = now_ticks() + 100 * 10_000_000;

  // 同域对照：SETWITHETAG 值已提交，Pending 尾参补投 etag 出整数帧
  assert_eq!(
    exec(&rt, &api, &mut s, RespCommand::Set, &[b"ce", b"v1"]),
    REPLY_OK
  );
  let healed = etag_slow_direct(
    &rt,
    &api,
    RespCommand::Setwithetag,
    &[b"ce", b"v1"],
    EtagResume::Pending {
      ttl: TtlLeg {
        ticks: 0,
        domain: cur_domain(&store),
      },
      new_etag: 1,
      found: true,
    },
  );
  assert_eq!(healed, b":1\r\n");
  assert_eq!(
    exec(&rt, &api, &mut s, RespCommand::Getwithetag, &[b"ce"]),
    etag_pair(1, Some(b"v1")),
    "同域 etag 余腿补投语义零漂移"
  );

  // 同域对照二：SETIFMATCH TTL 保留形（ticks 非零）补投旧刻度
  assert_eq!(
    exec(
      &rt,
      &api,
      &mut s,
      RespCommand::Set,
      &[b"ct", b"v1", b"EX", EXPIRE_SECONDS]
    ),
    REPLY_OK
  );
  assert_eq!(
    exec(&rt, &api, &mut s, RespCommand::Set, &[b"ct", b"v2"]),
    REPLY_OK
  );
  let healed = etag_slow_direct(
    &rt,
    &api,
    RespCommand::Setifmatch,
    &[b"ct", b"v2", b"0"],
    EtagResume::Pending {
      ttl: TtlLeg {
        ticks,
        domain: cur_domain(&store),
      },
      new_etag: 1,
      found: true,
    },
  );
  assert_eq!(healed, etag_pair(1, None));
  assert!(
    (1..=PTTL_UPPER_MS).contains(&pttl_ms(&rt, &api, &mut s, b"ct")),
    "同域 etag Pending TTL 余腿补投语义零漂移"
  );

  // 跨域：值腿已随死域退役，跳 TTL 与 etag 双余腿只出成功帧
  let stale = cur_domain(&store);
  flushdb_kernel(&store, 0, true);
  assert_ne!(cur_domain(&store), stale);
  let out = etag_slow_direct(
    &rt,
    &api,
    RespCommand::Setwithetag,
    &[b"de", b"v1"],
    EtagResume::Pending {
      ttl: TtlLeg {
        ticks: 0,
        domain: stale,
      },
      new_etag: 5,
      found: true,
    },
  );
  assert_eq!(out, b":5\r\n", "跨域 etag Pending 只按原契约出整数成功帧");
  assert_eq!(
    exec(&rt, &api, &mut s, RespCommand::Getwithetag, &[b"de"]),
    b"$-1\r\n",
    "跨域 etag 余腿不落新域（缺席键 GETWITHETAG 回 null bulk，存在才回 [etag, value]）"
  );
  assert_eq!(
    pttl_ms(&rt, &api, &mut s, b"de"),
    -2,
    "跨域 etag Pending 新域零孤 TTL 旁路（键缺席 PTTL 回 -2）"
  );
}

/// 验证点 6（e2e 真原语窗形）：KEEPTTL 翻转风暴中，首个降级笔在「快臂
/// KeepTtl/Pending 置位降级后、慢臂复窗前」的泵间隙注入 FLUSHDB 真原语——
/// 降级笔应答 +OK 帧形不变、其键在新域无幽灵 TTL；注入后段正常笔（同域）
/// TTL 闭环如常（现读臂同域有 TTL 形对照）。修复前降级笔跨窗重放把旧域
/// 刻度盖进新域键 → PTTL > 0 幽灵
#[test]
fn flushdb_in_degrade_window_keepttl_no_ghost_ttl() {
  let (rt, api, store, _dir) = degrade_env("dom-anchor-e2e.db");
  let mut s = session_with(&api);
  let injected = Arc::new(AtomicBool::new(false));
  let injected_key = Arc::new(Mutex::new(String::new()));

  let total = 600;
  let mut degraded = 0usize;
  for i in 0..total {
    let key = format!("fk{i}");
    let val = vec![b'm' + (i % 26) as u8; 600 + i % 11 * 100];
    assert_eq!(
      exec(
        &rt,
        &api,
        &mut s,
        RespCommand::Set,
        &[key.as_bytes(), b"base", b"EX", EXPIRE_SECONDS]
      ),
      REPLY_OK
    );
    let inj_store = Arc::clone(&store);
    let inj_flag = Arc::clone(&injected);
    let inj_key = Arc::clone(&injected_key);
    let inj_key_name = key.clone();
    let inject = move || {
      if !inj_flag.swap(true, Ordering::Relaxed) {
        *inj_key.lock() = inj_key_name.clone();
        flushdb_kernel(&inj_store, 0, false);
      }
    };
    let (out, was_degraded) = exec_inject(
      &rt,
      &api,
      &mut s,
      RespCommand::Set,
      &[key.as_bytes(), &val, b"KEEPTTL"],
      Some(&inject),
    );
    degraded += was_degraded as usize;
    assert_eq!(
      out, REPLY_OK,
      "换号窗内降级笔应答帧形与无换号基线逐字节等（+OK）"
    );
  }
  assert!(
    degraded > 0,
    "测试前提：环形日志 append 总量越过 4 页容量应至少触发一次降级"
  );
  assert!(
    injected.load(Ordering::Relaxed),
    "注入前提：首降级笔已注入 FLUSHDB"
  );
  let key = injected_key.lock().clone();
  assert_eq!(
    pttl_ms(&rt, &api, &mut s, key.as_bytes()),
    -1,
    "跨换号降级笔的新域键无幽灵 TTL（修复前旧域刻度盖进新域 → 未来静默消失）"
  );

  // 注入后段正常笔（同域闭环）：TTL 如常在场（域比对同域零扰动的对照形）
  assert_eq!(
    exec(
      &rt,
      &api,
      &mut s,
      RespCommand::Set,
      &[b"gk", b"v", b"EX", EXPIRE_SECONDS]
    ),
    REPLY_OK
  );
  assert!(
    (1..=PTTL_UPPER_MS).contains(&pttl_ms(&rt, &api, &mut s, b"gk")),
    "同域无换号笔 TTL 闭环零回归"
  );
}
