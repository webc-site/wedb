//! NX 族条件命令快臂「值先落库、TTL 后置」遭 put_ttl_sync PageNotReady 降级后
//! 整命令慢路重放自碰三形回归（票 wnode-nx-conditional-ttl-degrade-replay-selfhit）
//!
//! 缺陷面：rust 快臂把 C# 单次 CAS（KeyAdminCommands.cs:105/:109 SETEXNX 过期内嵌、
//! BasicCommands.cs:786 NetworkSET_Conditional 单次 SET_Conditional RMW）拆成
//! 「值 upsert 先、TTL put 后」两步，TTL 步遭环形页翻转返回 Ok(false) 后整命令
//! 转慢路径重放，慢臂无「本会话刚写」标记即自碰已提交值——RESTORE 误回
//! BUSYKEY、SET NX 误回 nil、SET KEEPTTL 回填读已被快臂 upsert 清除的 None
//! 而 TTL 静默丢，三形客户端均无法据应答自愈。
//!
//! 修复形态：快臂值已提交后 TTL 降级置 [`wnode::resp::TtlResume::Pending`]，
//! exec 降级快照经既有 pending_slow 通道追加 9 字节尾参（模式字节 + 8 字节 LE
//! 过期刻度，沿 MSETNX/DEL 尾参先例），慢臂剥尾参后跳过整命令重放、持窗仅
//! 补投 TTL 出成功帧。
//!
//! GET 回旧值形增量（票 zcode-r153c-setrangeget 案一）：GET 臂接回同一标记
//! 通道，值已提交降级就地重映射为 [`wnode::resp::TtlResume::ReplyEcho`]
//! （b'3'），保留已成旧值/nil 应答（缺席键 nil 帧前置成帧），慢臂补投 TTL 出
//! 零字节，杜绝全量重放自碰已提交值——新值冒充旧值回显、NX+GET+EX 反判
//! 转永续、KEEPTTL+GET 回填读已清墓碑静默丢三形。
//!
//! 测试全真存储真协议帧，无 mock：压力翻转夹具（hyperloglog.rs degrade_env
//! 同款 16KB×4 页小环形日志，回绕复用槽位遇未驱逐旧页恒现 PageNotReady，
//! append.rs:40）验证 e2e 终态；慢臂直驱（带尾参快照，与降级快照投递同径）
//! 定点锁死自碰三形的续跑契约与 Full 尾参的全量重放零漂移。

use std::{mem::take, str::from_utf8, sync::Arc};

use compio::runtime::Runtime;
use tempfile::{TempDir, tempdir};
use wbase::{crc64::hash as crc64_hash, time::now_ticks};
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::resp::{
  TtlResume,
  garnet_api::{GarnetApi, StoreGarnetApi},
  resp_server_session::{RespServerSession, RespServerSessionOptions},
  slow_path::SlowWait,
};
use wresp::{command::RespCommand, length::try_write_length};

/// 过期 100 秒（Garnet RESTORE/EX 口径为秒），PTTL 上界含粗化余量宽窗
const EXPIRE_SECONDS: &[u8] = b"100";
const PTTL_UPPER_MS: i64 = 130_000;
/// 既有 BUSYKEY 错误帧（cmd_strings::RESP_ERR_BUSSYKEY 经 write_error_raw 成帧）
const REPLY_BUSYKEY: &[u8] = b"-BUSYKEY Target key name already exists.\r\n";
const REPLY_OK: &[u8] = b"+OK\r\n";

/// 压力翻转执行域（hyperloglog.rs degrade_env 同款）：16KB × 4 页小环形日志，
/// 持续写入回绕复用槽位必遇 PageNotReady（无后台刷盘下旧页未驱逐），即
/// 「值已提交、TTL 恰逢环形页翻转」生产形态的确定性微缩
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

/// 单命令往返泵：快路径降级（挂起 SlowWait 登记）时按网络泵同款方式驱动
/// 挂起体闭环（exec 降级快照携续跑尾参，走 pending_slow 通道本尊），返回
/// (应答字节, 本命令是否经历降级)。降级笔会话已累积应答（GET ReplyEcho 形
/// 保留的成帧）先冲出、挂起体产出按流水线顺序并入——resolve_slow_wait_into
/// 同口径
fn exec(
  rt: &Runtime,
  api: &GarnetApi,
  s: &mut RespServerSession,
  cmd: RespCommand,
  args: &[&[u8]],
) -> (Vec<u8>, bool) {
  s.output.clear();
  api.exec(s, cmd, args);
  match s.take_slow_wait() {
    Some(slow) => {
      let mut out = take(&mut s.output);
      out.extend(rt.block_on(slow.resolve()));
      (out, true)
    }
    None => (take(&mut s.output), false),
  }
}

/// RESP2 bulk 帧构造（逐字节比对基准）
fn bulk_frame(val: &[u8]) -> Vec<u8> {
  let mut frame = format!("${}\r\n", val.len()).into_bytes();
  frame.extend_from_slice(val);
  frame.extend_from_slice(b"\r\n");
  frame
}

/// 解析 RESP 整数回执（:N\r\n），非整数回 None
fn parse_resp_int(resp: &[u8]) -> Option<i64> {
  let body = resp.strip_prefix(b":")?;
  from_utf8(body.strip_suffix(b"\r\n")?).ok()?.parse().ok()
}

/// 读 PTTL 毫秒（复用 exec 泵，TTL 记录磁盘候选态自动降级闭环）
fn pttl_ms(rt: &Runtime, api: &GarnetApi, s: &mut RespServerSession, key: &[u8]) -> i64 {
  let (out, _) = exec(rt, api, s, RespCommand::Pttl, &[key]);
  parse_resp_int(&out).unwrap_or_else(|| panic!("PTTL 应答须为整数: {out:?}"))
}

/// 合法 RESTORE 载荷（类型 0x00 + 长度前缀 + 值 + rdb 版本 11 + crc64，
/// crc 含类型字节起算的 rust 口径，deviations §21；与 types.rs 推导单源同构）
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

/// 慢臂直驱（与降级快照投递同径）：快照尾参恒带续跑标记（exec 契约）
fn slow_direct(
  rt: &Runtime,
  api: &GarnetApi,
  cmd: RespCommand,
  args: &[&[u8]],
  resume: TtlResume,
) -> Vec<u8> {
  let mut snapshot: Vec<Vec<u8>> = args.iter().map(|a| a.to_vec()).collect();
  snapshot.push(resume.tail_bytes());
  rt.block_on(SlowWait::for_command(api, cmd, snapshot, wconf::DEFAULT_RESP_VERSION).resolve())
}

/// 验证点 a（压力 e2e）：环形页翻转风暴下逐笔 RESTORE k{i} ttl=100——
/// 每笔终态应答恒 +OK（修复后降级笔经尾参续跑，修复前自碰已提交值误回
/// BUSYKEY），且每键 TTL 记录最终在场（修复前 TTL 永缺 → PTTL -1）
#[test]
fn restore_ttl_degrade_replay_never_busykey() {
  let (rt, api, _store, _dir) = degrade_env("nx-ttl-restore-storm.db");
  let mut s = session_with(&api);
  let total = 800;
  let mut degraded = 0usize;
  for i in 0..total {
    let key = format!("rk{i}");
    let val = vec![b'a' + (i % 26) as u8; 700 + i % 7 * 90];
    let payload = restore_payload(&val);
    let (out, was_degraded) = exec(
      &rt,
      &api,
      &mut s,
      RespCommand::Restore,
      &[key.as_bytes(), EXPIRE_SECONDS, &payload],
    );
    degraded += was_degraded as usize;
    assert_eq!(
      out, REPLY_OK,
      "RESTORE {key} 降级重放不得自碰已提交值回 BUSYKEY（修复前翻转笔误回 BUSYKEY、TTL 永缺）"
    );
  }
  assert!(
    degraded > 0,
    "测试前提：环形日志 append 总量越过 4 页容量应至少触发一次降级"
  );
  for i in 0..total {
    let key = format!("rk{i}");
    let ttl = pttl_ms(&rt, &api, &mut s, key.as_bytes());
    assert!(
      ttl > 0 && ttl <= PTTL_UPPER_MS,
      "RESTORE {key} 终态 TTL 记录必在场（修复前降级笔 TTL 永缺 → PTTL {ttl}）"
    );
  }
}

/// 验证点 b（压力 e2e）：SET k v EX 100 NX 翻转风暴——每笔恒 +OK（修复前
/// 自碰已提交值回 nil 而值已在、TTL 丢），每键 TTL 在场
#[test]
fn set_nx_ex_degrade_replay_never_nil() {
  let (rt, api, _store, _dir) = degrade_env("nx-ttl-setnx-storm.db");
  let mut s = session_with(&api);
  let total = 800;
  let mut degraded = 0usize;
  for i in 0..total {
    let key = format!("nk{i}");
    let val = vec![b'x' + (i % 26) as u8; 600 + i % 9 * 110];
    let (out, was_degraded) = exec(
      &rt,
      &api,
      &mut s,
      RespCommand::Set,
      &[key.as_bytes(), &val, b"EX", EXPIRE_SECONDS, b"NX"],
    );
    degraded += was_degraded as usize;
    assert_eq!(
      out, REPLY_OK,
      "SET NX EX 降级重放不得自碰已提交值回 nil（修复前翻转笔回 nil 而值已在、TTL 丢）"
    );
  }
  assert!(
    degraded > 0,
    "测试前提：环形日志 append 总量越过 4 页容量应至少触发一次降级"
  );
  for i in 0..total {
    let key = format!("nk{i}");
    let ttl = pttl_ms(&rt, &api, &mut s, key.as_bytes());
    assert!(
      ttl > 0 && ttl <= PTTL_UPPER_MS,
      "SET NX EX {key} 终态 TTL 必在场（修复前降级笔 TTL 永缺 → PTTL {ttl}）"
    );
  }
}

/// 验证点 c（压力 e2e）：SET k v EX 100 建 TTL 后 SET k v2 KEEPTTL 翻转风暴
/// ——每笔 KEEPTTL 恒 +OK 且原 TTL 保留（修复前快臂 upsert 清 TTL 后降级，
/// 重放回填读已清 TTL 得 None，+OK 而 TTL 静默丢 → PTTL -1）
#[test]
fn set_keepttl_degrade_replay_keeps_ttl() {
  let (rt, api, _store, _dir) = degrade_env("nx-ttl-keepttl-storm.db");
  let mut s = session_with(&api);
  let total = 600;
  let mut degraded = 0usize;
  for i in 0..total {
    let key = format!("kk{i}");
    let base = vec![b'b'; 500];
    let (out, _) = exec(
      &rt,
      &api,
      &mut s,
      RespCommand::Set,
      &[key.as_bytes(), &base, b"EX", EXPIRE_SECONDS],
    );
    assert_eq!(out, REPLY_OK);
    let val = vec![b'm' + (i % 26) as u8; 600 + i % 11 * 100];
    let (out, was_degraded) = exec(
      &rt,
      &api,
      &mut s,
      RespCommand::Set,
      &[key.as_bytes(), &val, b"KEEPTTL"],
    );
    degraded += was_degraded as usize;
    assert_eq!(out, REPLY_OK, "KEEPTTL 恒回 +OK");
  }
  assert!(
    degraded > 0,
    "测试前提：环形日志 append 总量越过 4 页容量应至少触发一次降级"
  );
  for i in 0..total {
    let key = format!("kk{i}");
    let ttl = pttl_ms(&rt, &api, &mut s, key.as_bytes());
    assert!(
      ttl > 0 && ttl <= PTTL_UPPER_MS,
      "KEEPTTL {key} 旧 TTL 必保留（修复前降级笔回填读已清 TTL → TTL 静默丢 PTTL {ttl}）"
    );
  }
}

/// 慢臂续跑契约定点（不带随机性，直驱带尾参快照）：
/// 1) RESTORE Pending 尾参对「值已提交」态补投 TTL 回 +OK，不自碰 BUSYKEY；
/// 2) SET NX EX Pending 尾参同态回 +OK 而非 nil，TTL 随刻度落；
/// 3) SET KEEPTTL Pending 尾参补投携带的旧刻度（回填读已清 TTL 面免疫）；
/// 4) Full 尾参维持既有全量重放语义零漂移（已存在键 RESTORE 恒 BUSYKEY）
#[test]
fn slow_resume_tail_heals_selfhit_and_full_tail_no_drift() {
  let (rt, api, _store, _dir) = degrade_env("nx-ttl-slow-tail.db");
  let mut s = session_with(&api);
  // 未来过期刻度（now + 100s，.NET Ticks 100ns 单位，与快臂换算同源）
  let ticks = now_ticks() + 100 * 10_000_000;

  // 1) RESTORE 自碰态：键值已由快臂同款通道提交（ttl=0 无 TTL 落库），
  //    慢臂带 Pending 尾参重入本命令载荷——修复前存活探针判存在即误回 BUSYKEY
  let payload = restore_payload(b"committed");
  assert_eq!(
    exec(
      &rt,
      &api,
      &mut s,
      RespCommand::Restore,
      &[b"h-r", b"0", &payload]
    )
    .0,
    REPLY_OK
  );
  let healed = slow_direct(
    &rt,
    &api,
    RespCommand::Restore,
    &[b"h-r", EXPIRE_SECONDS, &payload],
    TtlResume::Pending(ticks),
  );
  assert_eq!(healed, REPLY_OK, "RESTORE Pending 尾参不得自碰 BUSYKEY");
  assert!(
    (1..=PTTL_UPPER_MS).contains(&pttl_ms(&rt, &api, &mut s, b"h-r")),
    "RESTORE Pending 续跑后 TTL 记录必在场"
  );

  // 4) Full 尾参零漂移：同载荷对已存在键全量重放仍回既有 BUSYKEY 帧
  let full = slow_direct(
    &rt,
    &api,
    RespCommand::Restore,
    &[b"h-r", EXPIRE_SECONDS, &payload],
    TtlResume::Full,
  );
  assert_eq!(full, REPLY_BUSYKEY, "Full 尾参维持既有重放语义");

  // 2) SET NX EX 自碰态：值已提交（键在、TTL 缺），Pending 尾参回 +OK 补 TTL
  assert_eq!(
    exec(&rt, &api, &mut s, RespCommand::Set, &[b"h-n", b"v1"]).0,
    REPLY_OK
  );
  let healed = slow_direct(
    &rt,
    &api,
    RespCommand::Set,
    &[b"h-n", b"v1", b"EX", EXPIRE_SECONDS, b"NX"],
    TtlResume::Pending(ticks),
  );
  assert_eq!(healed, REPLY_OK, "SET NX Pending 尾参不得自碰回 nil");
  assert!(
    (1..=PTTL_UPPER_MS).contains(&pttl_ms(&rt, &api, &mut s, b"h-n")),
    "SET NX Pending 续跑后 TTL 必在场"
  );
  // Full 尾参零漂移：既有 NX 语义对存活键恒 nil
  assert_eq!(
    slow_direct(
      &rt,
      &api,
      RespCommand::Set,
      &[b"h-n", b"v1", b"EX", EXPIRE_SECONDS, b"NX"],
      TtlResume::Full,
    ),
    b"$-1\r\n",
    "Full 尾参 NX 自碰既有 nil 语义零漂移"
  );

  // 3) SET KEEPTTL 自碰态：快臂 upsert 已清旧 TTL，Pending 尾参携旧刻度
  //    补投——修复前重放回填读已清 TTL 得 None，+OK 而 TTL 静默丢
  assert_eq!(
    exec(
      &rt,
      &api,
      &mut s,
      RespCommand::Set,
      &[b"h-k", b"v1", b"EX", EXPIRE_SECONDS]
    )
    .0,
    REPLY_OK
  );
  // 模拟快臂尾部态：值覆写（自带清 TTL）后降级，尾参携覆写前读得的旧刻度
  assert_eq!(
    exec(&rt, &api, &mut s, RespCommand::Set, &[b"h-k", b"v2"]).0,
    REPLY_OK
  );
  let healed = slow_direct(
    &rt,
    &api,
    RespCommand::Set,
    &[b"h-k", b"v2", b"KEEPTTL"],
    TtlResume::Pending(ticks),
  );
  assert_eq!(healed, REPLY_OK);
  assert!(
    (1..=PTTL_UPPER_MS).contains(&pttl_ms(&rt, &api, &mut s, b"h-k")),
    "KEEPTTL Pending 续跑旧 TTL 必回填（修复前静默丢 → PTTL -1）"
  );
}

/// 尾参线格式锁（exec 快照与慢臂逆解析的通道契约）：9 字节 = 模式字节 +
/// 8 字节 LE 刻度；Full=b'0'+零，Pending=b'1'+刻度，KeepTtl=b'2'+刻度，
/// ReplyEcho=b'3'+刻度（票 zcode-r153c-setrangeget 案一扩模，三旧形零漂移）；
/// 形态不符按 Full 兜底
#[test]
fn ttl_resume_tail_wire_format() {
  assert_eq!(
    TtlResume::Full.tail_bytes(),
    [vec![b'0'], 0i64.to_le_bytes().to_vec()].concat()
  );
  let ticks = now_ticks() + 10_000_000;
  assert_eq!(
    TtlResume::Pending(ticks).tail_bytes(),
    [vec![b'1'], ticks.to_le_bytes().to_vec()].concat()
  );
  assert_eq!(
    TtlResume::KeepTtl(ticks).tail_bytes(),
    [vec![b'2'], ticks.to_le_bytes().to_vec()].concat()
  );
  assert_eq!(
    TtlResume::ReplyEcho(ticks).tail_bytes(),
    [vec![b'3'], ticks.to_le_bytes().to_vec()].concat()
  );
  assert_eq!(
    TtlResume::from_tail(Some(&TtlResume::KeepTtl(ticks).tail_bytes())),
    TtlResume::KeepTtl(ticks)
  );
  assert_eq!(
    TtlResume::from_tail(Some(&TtlResume::ReplyEcho(ticks).tail_bytes())),
    TtlResume::ReplyEcho(ticks)
  );
  assert_eq!(TtlResume::from_tail(Some(&[b'0'; 1])), TtlResume::Full);
  assert_eq!(TtlResume::from_tail(None), TtlResume::Full);
  assert_eq!(TtlResume::Full, TtlResume::default());
}

/// GET 案一·形一（压力 e2e）：预置旧值后 SET k v EX 100 GET 翻转风暴——
/// 每笔应答逐字节等于命令前旧值（修复前降级笔全量重放自碰已提交值，把本
/// 命令刚写入的新值冒充旧值回显），且每键 TTL 终态在场、值即命令新值
#[test]
fn set_get_ex_degrade_never_echoes_new_value() {
  let (rt, api, _store, _dir) = degrade_env("srg-get-ex-storm.db");
  let mut s = session_with(&api);
  let total = 600;
  let mut degraded = 0usize;
  for i in 0..total {
    let key = format!("gk{i}");
    let old = vec![b'o' + (i % 26) as u8; 600 + i % 9 * 110];
    let val = vec![b'n' + (i % 26) as u8; 700 + i % 7 * 90];
    assert_eq!(
      exec(&rt, &api, &mut s, RespCommand::Set, &[key.as_bytes(), &old]).0,
      REPLY_OK
    );
    let (out, was_degraded) = exec(
      &rt,
      &api,
      &mut s,
      RespCommand::Set,
      &[key.as_bytes(), &val, b"EX", EXPIRE_SECONDS, b"GET"],
    );
    degraded += was_degraded as usize;
    assert_eq!(
      out,
      bulk_frame(&old),
      "SET k v EX 100 GET 降级笔必回命令前旧值（修复前翻转笔回显刚写入的新值）"
    );
  }
  assert!(
    degraded > 0,
    "测试前提：环形日志 append 总量越过 4 页容量应至少触发一次降级"
  );
  for i in 0..total {
    let key = format!("gk{i}");
    let val = vec![b'n' + (i % 26) as u8; 700 + i % 7 * 90];
    let ttl = pttl_ms(&rt, &api, &mut s, key.as_bytes());
    assert!(
      ttl > 0 && ttl <= PTTL_UPPER_MS,
      "SET..EX..GET {key} 终态 TTL 必在场（修复前降级笔 TTL 永缺 → PTTL {ttl}）"
    );
    assert_eq!(
      exec(&rt, &api, &mut s, RespCommand::Get, &[key.as_bytes()]).0,
      bulk_frame(&val),
      "SET..EX..GET {key} 值字节按命令本意落库"
    );
  }
}

/// GET 案一·形二（压力 e2e）：SET k v EX 100 NX GET 缺席键翻转风暴——
/// 每笔恒 nil（修复前降级笔反判 NX 违例、应答翻成刚提交新值的 bulk），
/// 且每键 TTL 必在场（修复前跳 apply 永丢转永续）
#[test]
fn set_nx_get_ex_degrade_absent_key_always_nil() {
  let (rt, api, _store, _dir) = degrade_env("srg-get-nx-storm.db");
  let mut s = session_with(&api);
  let total = 600;
  let mut degraded = 0usize;
  for i in 0..total {
    let key = format!("ng{i}");
    let val = vec![b'v' + (i % 26) as u8; 600 + i % 9 * 110];
    let (out, was_degraded) = exec(
      &rt,
      &api,
      &mut s,
      RespCommand::Set,
      &[key.as_bytes(), &val, b"EX", EXPIRE_SECONDS, b"NX", b"GET"],
    );
    degraded += was_degraded as usize;
    assert_eq!(
      out, b"$-1\r\n",
      "SET k v EX 100 NX GET 缺席键恒 nil（修复前降级笔反判回显新值 bulk）"
    );
  }
  assert!(
    degraded > 0,
    "测试前提：环形日志 append 总量越过 4 页容量应至少触发一次降级"
  );
  for i in 0..total {
    let key = format!("ng{i}");
    let ttl = pttl_ms(&rt, &api, &mut s, key.as_bytes());
    assert!(
      ttl > 0 && ttl <= PTTL_UPPER_MS,
      "NX GET {key} TTL 必在场（修复前降级笔跳 apply TTL 永丢 → PTTL {ttl}）"
    );
  }
}

/// GET 案一·形三（压力 e2e）：SET k v EX 100 建 TTL 后 SET k v2 KEEPTTL GET
/// 翻转风暴——每笔应答逐字节等于旧值且旧 TTL 保留（修复前重放回填读已被
/// 快臂 upsert 清退的墓碑得 None，回显新值且 TTL 静默丢）
#[test]
fn set_keepttl_get_degrade_keeps_ttl_and_old_value() {
  let (rt, api, _store, _dir) = degrade_env("srg-get-keepttl-storm.db");
  let mut s = session_with(&api);
  let total = 600;
  let mut degraded = 0usize;
  for i in 0..total {
    let key = format!("kg{i}");
    let v1 = vec![b'a'; 500 + i % 5 * 90];
    let v2 = vec![b'q' + (i % 26) as u8; 600 + i % 11 * 100];
    assert_eq!(
      exec(
        &rt,
        &api,
        &mut s,
        RespCommand::Set,
        &[key.as_bytes(), &v1, b"EX", EXPIRE_SECONDS]
      )
      .0,
      REPLY_OK
    );
    let (out, was_degraded) = exec(
      &rt,
      &api,
      &mut s,
      RespCommand::Set,
      &[key.as_bytes(), &v2, b"KEEPTTL", b"GET"],
    );
    degraded += was_degraded as usize;
    assert_eq!(
      out,
      bulk_frame(&v1),
      "SET k v KEEPTTL GET 降级笔必回命令前旧值（修复前翻转笔回显新值）"
    );
  }
  assert!(
    degraded > 0,
    "测试前提：环形日志 append 总量越过 4 页容量应至少触发一次降级"
  );
  for i in 0..total {
    let key = format!("kg{i}");
    let ttl = pttl_ms(&rt, &api, &mut s, key.as_bytes());
    assert!(
      ttl > 0 && ttl <= PTTL_UPPER_MS,
      "KEEPTTL GET {key} 旧 TTL 必保留（修复前降级笔回填读已清墓碑静默丢 → PTTL {ttl}）"
    );
  }
}

/// GET 案一慢臂续跑契约定点（直驱带尾参快照，不带随机性）：
/// 1) ReplyEcho 尾参补投 TTL 出零字节、绝不重放写值；
/// 2) Full 尾参 GET 形维持既有全量重放零漂移（回命令前旧值、写新值与 TTL）
///    ——提交前降级（upsert Ok(Err)）抽帧转重放的快臂侧对应形态；
/// 3) KeepTtl 尾参 GET 形重放按刻度回填，免疫快臂 TTL 腿清退墓碑
#[test]
fn slow_get_tail_reply_echo_heals_and_full_keep_ttl_no_drift() {
  let (rt, api, _store, _dir) = degrade_env("srg-get-slow-tail.db");
  let mut s = session_with(&api);
  let ticks = now_ticks() + 100 * 10_000_000;

  // 1) ReplyEcho：模拟快臂值已提交、应答已成帧保留后的挂起体——仅补投
  //    TTL 出零字节，值与应答均不再触达
  assert_eq!(
    exec(&rt, &api, &mut s, RespCommand::Set, &[b"g-r", b"committed"]).0,
    REPLY_OK
  );
  let echo = slow_direct(
    &rt,
    &api,
    RespCommand::Set,
    &[b"g-r", b"newer", b"EX", EXPIRE_SECONDS, b"GET"],
    TtlResume::ReplyEcho(ticks),
  );
  assert_eq!(echo, b"", "ReplyEcho 尾参须返零字节（应答已由快臂保留）");
  assert!(
    (1..=PTTL_UPPER_MS).contains(&pttl_ms(&rt, &api, &mut s, b"g-r")),
    "ReplyEcho 续跑后 TTL 必在场"
  );
  assert_eq!(
    exec(&rt, &api, &mut s, RespCommand::Get, &[b"g-r"]).0,
    bulk_frame(b"committed"),
    "ReplyEcho 不得重放覆写值"
  );

  // 2) Full 尾参 GET 形全量重放零漂移：回命令前旧值并落新值 + TTL
  let full = slow_direct(
    &rt,
    &api,
    RespCommand::Set,
    &[b"g-r", b"newer", b"EX", EXPIRE_SECONDS, b"GET"],
    TtlResume::Full,
  );
  assert_eq!(
    full,
    bulk_frame(b"committed"),
    "Full 尾参 GET 重放回命令前旧值（提交前降级既有收敛形）"
  );
  assert!(
    (1..=PTTL_UPPER_MS).contains(&pttl_ms(&rt, &api, &mut s, b"g-r")),
    "Full 尾参 GET 重放须落新 TTL"
  );

  // 3) KeepTtl 尾参 GET 形重放回填：先裸 SET 覆写并清 TTL 模拟快臂 upsert
  //    清退态（现值 mid、TTL 缺）
  assert_eq!(
    exec(&rt, &api, &mut s, RespCommand::Set, &[b"g-r", b"mid"]).0,
    REPLY_OK
  );
  let kept = slow_direct(
    &rt,
    &api,
    RespCommand::Set,
    &[b"g-r", b"late", b"KEEPTTL", b"GET"],
    TtlResume::KeepTtl(ticks),
  );
  assert_eq!(
    kept,
    bulk_frame(b"mid"),
    "KeepTtl GET 重放回命令前旧值（mid 为裸 SET 覆写后的现值）"
  );
  assert!(
    (1..=PTTL_UPPER_MS).contains(&pttl_ms(&rt, &api, &mut s, b"g-r")),
    "KeepTtl 尾参 GET 重放必按刻度回填旧 TTL（修复前读已清墓碑静默丢）"
  );
  assert_eq!(
    exec(&rt, &api, &mut s, RespCommand::Get, &[b"g-r"]).0,
    bulk_frame(b"late")
  );
}
