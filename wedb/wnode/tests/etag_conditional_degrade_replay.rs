//! ETag 写族（SETIFMATCH / SETIFGREATER / SETWITHETAG）快臂降级信号吞没回归
//! （票 zcode-r139c-etag2 案一）
//!
//! 缺陷面：rust 快臂写共同体 `apply_etag_write` 以 `apply_set_with_expiry(...)?`
//! 调用，`?` 只传播 `Err(())`，`Ok(false)`（RI 门磁盘候选 / `try_upsert_sync`
//! 环形页翻转 / `put_ttl_sync` 环形页翻转三源）的 bool 被求值丢弃，代码续写
//! `put_etag_sync` 并回 `Ok(true)`，调用方即出 `[new_etag, nil]` / 整数成功帧
//! ——
//! * 情形 A（值腿未提交）：旧值保留、etag 侧写已前进并被应答成功，SETIFMATCH
//!   假 ACK 丢写，且他客户端持旧 etag 再试即判失配（「恰一者命中」CAS 反转为
//!   「零者命中且已回执」）；
//! * 情形 B（值已提交、TTL / etag 余腿降级）：C# 侧值 / 过期 / ETag 在同一
//!   记录单次 RMW 锁内一体落库（RMWMethods.cs:InPlaceUpdater 420-434），
//!   绝不存在半提交形态。
//!
//! 修复形态：`apply_etag_write` 显式分流，值腿 `Ok(false)` 必达调用方沿既有
//! `truncate` + 降级通道交慢臂同窗重放（不建二通道）；情形 B 的已裁决新 etag
//! 与待投刻度经 [`EtagResume`] 随 exec 降级快照在**同一 pending_slow 通道**
//! 追加 17 字节尾参承载（沿 [`wnode::resp::TtlResume`] / MSETNX / DEL 尾参先例），
//! 慢臂剥尾参后持窗仅补投余腿出与快臂逐字节一致的成功帧，杜绝整命令重放自碰
//! 已提交值（Missing 臂重放翻成 Hit 后条件重判失配、etag 侧写永缺；KEEPTTL 形
//! 回填读已被 upsert 清退的 None 静默丢 TTL）。
//!
//! 测试全真存储真协议帧，无 mock：压力翻转夹具（hyperloglog.rs /
//! nx_conditional_ttl_degrade_replay.rs 同款 16KB×4 页小环形日志，回绕复用
//! 槽位遇未驱逐旧页恒现 PageNotReady）验证 e2e 终态；慢臂直驱（带尾参快照，
//! 与降级快照投递同径）定点锁死 Pending 续跑契约与全量重放自碰反证。

use std::{mem::take, sync::Arc};

use compio::runtime::Runtime;
use tempfile::{TempDir, tempdir};
use wbase::time::now_ticks;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::resp::{
  EtagResume,
  garnet_api::{GarnetApi, StoreGarnetApi},
  resp_server_session::{RespServerSession, RespServerSessionOptions},
  slow_path::SlowWait,
};
use wresp::command::RespCommand;

/// 过期 100 秒（Garnet EX 口径为秒），PTTL 上界含粗化余量宽窗
const EXPIRE_SECONDS: &[u8] = b"100";
const PTTL_UPPER_MS: i64 = 130_000;
const REPLY_OK: &[u8] = b"+OK\r\n";

/// 压力翻转执行域（nx_conditional_ttl_degrade_replay.rs 同款）：16KB × 4 页
/// 小环形日志，持续写入回绕复用槽位必遇 PageNotReady（无后台刷盘下旧页未
/// 驱逐），即「值腿 / TTL 腿 / etag 腿恰遭环形页翻转」生产形态的确定性微缩
fn degrade_env(tag: &str) -> (Runtime, GarnetApi, Arc<WedbStore<SegmentedDevice>>, TempDir) {
  let dir = tempdir().unwrap();
  let config = StoreConfig::new(1024, 16 * 1024, 4, 0.5).unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag)).unwrap());
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  let api: GarnetApi = Arc::new(StoreGarnetApi::new(store.new_session().unwrap()));
  (Runtime::new().unwrap(), api, store, dir)
}

fn session_with(api: &GarnetApi) -> RespServerSession {
  let mut s = RespServerSession::new(1, RespServerSessionOptions::default());
  s.set_garnet_api(api.clone());
  s
}

/// 单命令往返泵：快路径降级（零应答挂起 SlowWait）时按网络泵同款方式驱动挂起
/// 体闭环（exec 降级快照携本族续跑尾参，走 pending_slow 通道本尊），返回
/// (应答字节, 本命令是否经历降级)
fn exec(
  rt: &Runtime,
  api: &GarnetApi,
  s: &mut RespServerSession,
  cmd: RespCommand,
  args: &[&[u8]],
) -> (Vec<u8>, bool) {
  s.output.clear();
  api.exec(s, cmd, args);
  if s.output.is_empty() {
    let slow = s.take_slow_wait().expect("降级必须挂起 SlowWait");
    (rt.block_on(slow.resolve()), true)
  } else {
    (take(&mut s.output), false)
  }
}

fn parse_resp_int(resp: &[u8]) -> Option<i64> {
  let body = resp.strip_prefix(b":")?;
  str::from_utf8(body.strip_suffix(b"\r\n")?)
    .ok()?
    .parse()
    .ok()
}

/// 读 PTTL 毫秒（复用 exec 泵，TTL 记录磁盘候选态自动降级闭环）
fn pttl_ms(rt: &Runtime, api: &GarnetApi, s: &mut RespServerSession, key: &[u8]) -> i64 {
  let (out, _) = exec(rt, api, s, RespCommand::Pttl, &[key]);
  parse_resp_int(&out).unwrap_or_else(|| panic!("PTTL 应答须为整数: {out:?}"))
}

/// `[etag, value]` / `[etag, nil]` 期望帧单源（RESP2，与
/// `write_etag_val_array` 同形）
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

/// 慢臂直驱（与降级快照投递同径）：快照尾参恒带本族续跑标记（exec 契约）
fn slow_direct(
  rt: &Runtime,
  api: &GarnetApi,
  cmd: RespCommand,
  args: &[&[u8]],
  resume: EtagResume,
) -> Vec<u8> {
  let mut snapshot: Vec<Vec<u8>> = args.iter().map(|a| a.to_vec()).collect();
  snapshot.push(resume.tail_bytes());
  rt.block_on(SlowWait::for_command(api, cmd, snapshot, wconf::DEFAULT_RESP_VERSION).resolve())
}

/// 验证点 a（压力 e2e，情形 A/B）：环形页翻转风暴下逐键「普通 SET 建基线 →
/// SETIFMATCH given=0 命中臂覆写携 EX 100」——每笔应答恒单帧 `[1, nil]`，
/// 终态每键 GETWITHETAG 必见本笔新值（修复前情形 A：值未落库、etag 已前进且
/// 已回执成功）且 TTL 在场（修复前情形 B：Pending 标记落入弃置量、TTL 静默丢）
#[test]
fn setifmatch_hit_degrade_storm_never_fake_ack() {
  let (rt, api, _store, _dir) = degrade_env("etag2-setifmatch-storm.db");
  let mut s = session_with(&api);
  let total = 600;
  let mut degraded = 0usize;
  for i in 0..total {
    let key = format!("em{i}");
    let base = vec![b'B'; 500];
    assert_eq!(
      exec(
        &rt,
        &api,
        &mut s,
        RespCommand::Set,
        &[key.as_bytes(), &base]
      )
      .0,
      REPLY_OK
    );
    let val = vec![b'a' + (i % 26) as u8; 600 + i % 11 * 100];
    let (out, was_degraded) = exec(
      &rt,
      &api,
      &mut s,
      RespCommand::Setifmatch,
      &[key.as_bytes(), &val, b"0", b"EX", EXPIRE_SECONDS],
    );
    degraded += was_degraded as usize;
    assert_eq!(
      out,
      etag_pair(1, None),
      "SETIFMATCH {key} 命中臂成功帧须单帧 [1, nil]（假 ACK 面：修复前值未落库亦回执成功）"
    );
  }
  assert!(
    degraded > 0,
    "测试前提：环形日志 append 总量越过 4 页容量应至少触发一次降级"
  );
  for i in 0..total {
    let key = format!("em{i}");
    let val = vec![b'a' + (i % 26) as u8; 600 + i % 11 * 100];
    let (out, _) = exec(
      &rt,
      &api,
      &mut s,
      RespCommand::Getwithetag,
      &[key.as_bytes()],
    );
    assert_eq!(
      out,
      etag_pair(1, Some(&val)),
      "SETIFMATCH {key} 终态值必为本笔所写新值（修复前情形 A 应答成功后旧值残留）"
    );
    let ttl = pttl_ms(&rt, &api, &mut s, key.as_bytes());
    assert!(
      (1..=PTTL_UPPER_MS).contains(&ttl),
      "SETIFMATCH {key} EX 终态 TTL 必在场（修复前情形 B TTL 永缺 → PTTL {ttl}）"
    );
  }
}

/// 验证点 b（压力 e2e，SETWITHETAG 无过期形态）：同键连续 SETWITHETAG 覆写
/// ——etag 严格逐笔 +1（修复前情形 A 值未落而 etag 前进，成功回执与存储态
/// 发散；情形 B etag 腿降级则 etag 少推进），终态值与 etag 与逐笔回执全等
#[test]
fn setwithetag_degrade_storm_advances_exactly_once_per_write() {
  let (rt, api, _store, _dir) = degrade_env("etag2-setwithetag-storm.db");
  let mut s = session_with(&api);
  let keys = 60;
  let rounds = 12;
  let mut degraded = 0usize;
  // 末位字节嵌轮次，保证逐笔覆写的值互不相同（终态对拍须锁定末笔）
  let mk_val = |i: usize, round: usize| -> Vec<u8> {
    let mut v = vec![b'c' + (i % 26) as u8; 700 + i % 7 * 90];
    let last = v.len() - 1;
    v[last] = b'0' + round as u8;
    v
  };
  for round in 0..rounds {
    for i in 0..keys {
      let key = format!("sw{i}");
      let val = mk_val(i, round);
      let (out, was_degraded) = exec(
        &rt,
        &api,
        &mut s,
        RespCommand::Setwithetag,
        &[key.as_bytes(), &val],
      );
      degraded += was_degraded as usize;
      assert_eq!(
        parse_resp_int(&out),
        Some((round + 1) as i64),
        "SETWITHETAG {key} 第 {round} 轮回执须为逐笔 +1 的 :{}（帧形亦须单整数帧）",
        round + 1
      );
    }
  }
  assert!(
    degraded > 0,
    "测试前提：环形日志 append 总量越过 4 页容量应至少触发一次降级"
  );
  for i in 0..keys {
    let key = format!("sw{i}");
    let val = mk_val(i, rounds - 1);
    let (out, _) = exec(
      &rt,
      &api,
      &mut s,
      RespCommand::Getwithetag,
      &[key.as_bytes()],
    );
    assert_eq!(
      out,
      etag_pair(rounds as i64, Some(&val)),
      "SETWITHETAG {key} 终态 etag 与值须与末笔回执全等（修复前降级笔值 / etag 发散）"
    );
  }
}

/// 慢臂续跑契约定点（直驱带尾参快照，票面「若全量重放对情形 B 自碰已提交值」
/// 的实测判据）：
/// 1) Missing 臂值已提交态：Pending 尾参出快臂同款 `[6, nil]` 并落 etag 6，
///    Full 尾参整命令重放读态翻成 Hit、条件重判失配回 `[0, 新值]` 且 etag
///    侧写永缺——本族尾参必须承载已裁决新 etag 的反证；
/// 2) 命中臂 TTL 保留形值已提交态：Pending 尾参补投携带的旧刻度，Full 尾参
///    回填读已被快臂 upsert 清退的 None 静默丢 TTL 反证；
/// 3) SETWITHETAG 值已提交态：Pending 尾参补投 etag（ticks=0 即 TTL 腿已闭环）
///    出整数成功帧；
/// 4) Full 尾参维持既有全量重放语义零漂移（无残留态键上正常命中写入）。
#[test]
fn slow_pending_tail_heals_committed_value() {
  let (rt, api, _store, _dir) = degrade_env("etag2-slow-tail.db");
  let mut s = session_with(&api);
  // 未来过期刻度（now + 100s，.NET Ticks 100ns 单位，与快臂换算同源）
  let ticks = now_ticks() + 100 * 10_000_000;

  // 1) SETIFMATCH Missing 臂自碰态：快臂对缺失键 given=5 裁决 new_etag=6 后
  //    值腿已同步落库（本夹具以同款普通 SET 复刻「值在、TTL 缺、etag 侧写
  //    缺」的降级瞬态）
  assert_eq!(
    exec(&rt, &api, &mut s, RespCommand::Set, &[b"t1", b"v1"]).0,
    REPLY_OK
  );
  let healed = slow_direct(
    &rt,
    &api,
    RespCommand::Setifmatch,
    &[b"t1", b"v1", b"5"],
    EtagResume::Pending {
      ticks: 0,
      new_etag: 6,
    },
  );
  assert_eq!(
    healed,
    etag_pair(6, None),
    "Pending 尾参须出快臂同款 [6, nil] 成功帧，不得重判条件"
  );
  assert_eq!(
    exec(&rt, &api, &mut s, RespCommand::Getwithetag, &[b"t1"]).0,
    etag_pair(6, Some(b"v1")),
    "Pending 续跑后 etag 侧写必落库"
  );
  // 反证：无尾参承载（Full）时同一已提交瞬态整命令重放自碰——Missing 臂翻成
  // Hit 后 given=5 与 existing=0 失配，回 [0, 新值] 且 etag 永缺
  assert_eq!(
    exec(&rt, &api, &mut s, RespCommand::Set, &[b"t1b", b"v1"]).0,
    REPLY_OK
  );
  assert_eq!(
    slow_direct(
      &rt,
      &api,
      RespCommand::Setifmatch,
      &[b"t1b", b"v1", b"5"],
      EtagResume::Full,
    ),
    etag_pair(0, Some(b"v1")),
    "Full 尾参维持既有全量重放语义（本臂自碰失配即情形 B 须携尾参的判据）"
  );
  assert_eq!(
    exec(&rt, &api, &mut s, RespCommand::Getwithetag, &[b"t1b"]).0,
    etag_pair(0, Some(b"v1")),
    "Full 重放失配臂零写：etag 侧写永缺（修复前情形 B 终态）"
  );

  // 2) 命中臂 TTL 保留形自碰态：快臂 upsert 已清旧 TTL，Pending 尾参携覆写前
  //    读得的旧刻度补投
  assert_eq!(
    exec(
      &rt,
      &api,
      &mut s,
      RespCommand::Set,
      &[b"t2", b"v1", b"EX", EXPIRE_SECONDS]
    )
    .0,
    REPLY_OK
  );
  assert_eq!(
    exec(&rt, &api, &mut s, RespCommand::Set, &[b"t2", b"v2"]).0,
    REPLY_OK
  );
  let healed = slow_direct(
    &rt,
    &api,
    RespCommand::Setifmatch,
    &[b"t2", b"v2", b"0"],
    EtagResume::Pending { ticks, new_etag: 1 },
  );
  assert_eq!(healed, etag_pair(1, None));
  assert!(
    (1..=PTTL_UPPER_MS).contains(&pttl_ms(&rt, &api, &mut s, b"t2")),
    "Pending 续跑旧 TTL 必回填（修复前静默丢 → PTTL -1）"
  );
  // 反证：Full 重放回填读已被清退的 None，+OK 形应答而 TTL 静默丢
  assert_eq!(
    exec(
      &rt,
      &api,
      &mut s,
      RespCommand::Set,
      &[b"t2b", b"v1", b"EX", EXPIRE_SECONDS]
    )
    .0,
    REPLY_OK
  );
  assert_eq!(
    exec(&rt, &api, &mut s, RespCommand::Set, &[b"t2b", b"v2"]).0,
    REPLY_OK
  );
  assert_eq!(
    slow_direct(
      &rt,
      &api,
      RespCommand::Setifmatch,
      &[b"t2b", b"v2", b"0"],
      EtagResume::Full,
    ),
    etag_pair(1, None)
  );
  assert_eq!(
    pttl_ms(&rt, &api, &mut s, b"t2b"),
    -1,
    "Full 重放自碰已提交值即 TTL 静默丢（情形 B 须携尾参的第二判据）"
  );

  // 3) SETWITHETAG 值已提交态（TTL 腿已闭环、etag 腿降级，ticks=0）
  assert_eq!(
    exec(&rt, &api, &mut s, RespCommand::Set, &[b"t3", b"v1"]).0,
    REPLY_OK
  );
  let healed = slow_direct(
    &rt,
    &api,
    RespCommand::Setwithetag,
    &[b"t3", b"v1"],
    EtagResume::Pending {
      ticks: 0,
      new_etag: 1,
    },
  );
  assert_eq!(
    healed, b":1\r\n",
    "SETWITHETAG Pending 尾参须出快臂同款整数帧"
  );
  assert_eq!(
    exec(&rt, &api, &mut s, RespCommand::Getwithetag, &[b"t3"]).0,
    etag_pair(1, Some(b"v1"))
  );

  // 4) Full 尾参既有全量重放语义零漂移：无残留态键上正常命中写入并落 TTL
  assert_eq!(
    exec(
      &rt,
      &api,
      &mut s,
      RespCommand::Set,
      &[b"t4", b"v1", b"EX", EXPIRE_SECONDS]
    )
    .0,
    REPLY_OK
  );
  assert_eq!(
    slow_direct(
      &rt,
      &api,
      RespCommand::Setifgreater,
      &[b"t4", b"v2", b"9", b"EX", EXPIRE_SECONDS],
      EtagResume::Full,
    ),
    etag_pair(9, None),
    "Full 尾参 SETIFGREATER 全量重放语义零漂移"
  );
  assert_eq!(
    exec(&rt, &api, &mut s, RespCommand::Getwithetag, &[b"t4"]).0,
    etag_pair(9, Some(b"v2"))
  );
  assert!(
    (1..=PTTL_UPPER_MS).contains(&pttl_ms(&rt, &api, &mut s, b"t4")),
    "Full 重放自身携 EX 时 TTL 必落"
  );
}

/// 尾参线格式锁（exec 快照与慢臂逆解析的通道契约）：17 字节 = 模式字节 +
/// 8 字节 LE 新 etag + 8 字节 LE 过期刻度；Full=b'0'+零，Pending=b'1'+两枚
/// 载荷；形态不符按 Full 兜底
#[test]
fn etag_resume_tail_wire_format() {
  assert_eq!(
    EtagResume::Full.tail_bytes(),
    [
      vec![b'0'],
      0i64.to_le_bytes().to_vec(),
      0i64.to_le_bytes().to_vec()
    ]
    .concat()
  );
  let ticks = now_ticks() + 10_000_000;
  assert_eq!(
    EtagResume::Pending {
      ticks,
      new_etag: 77
    }
    .tail_bytes(),
    [
      vec![b'1'],
      77i64.to_le_bytes().to_vec(),
      ticks.to_le_bytes().to_vec()
    ]
    .concat()
  );
  assert_eq!(
    EtagResume::from_tail(Some(
      &EtagResume::Pending {
        ticks,
        new_etag: 77
      }
      .tail_bytes()
    )),
    EtagResume::Pending {
      ticks,
      new_etag: 77
    }
  );
  // 16 字节越长度门（TAIL_LEN=17），真形态不符输入 → Full 兜底
  assert_eq!(EtagResume::from_tail(Some(&[b'1'; 16])), EtagResume::Full);
  assert_eq!(
    EtagResume::from_tail(Some(&EtagResume::Full.tail_bytes())),
    EtagResume::Full
  );
  assert_eq!(EtagResume::from_tail(None), EtagResume::Full);
  assert_eq!(EtagResume::Full, EtagResume::default());
}
