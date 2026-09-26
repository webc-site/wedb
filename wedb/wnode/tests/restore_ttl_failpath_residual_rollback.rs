//! RESTORE 值已落库后 TTL 硬故障的残值补偿删回归（票 zcode-r153c-restcrc 立案一）
//!
//! 缺陷面：rust 快臂把 C# 单记录条件写（libs/server/Resp/KeyAdminCommands.cs:
//! 102-112 SETEXNX 值与过期同记录、libs/server/Storage/Session/MainStore/
//! MainStoreOps.cs:258-287 SET_Conditional 单次 RMW 一体提交，IOError 下记录
//! 未提交键不存续）拆成「try_insert_sync 值落库 → put_ttl 后置落 TTL」两步，
//! TTL 步设备级硬故障（Err，非环形页翻转降级）时快臂直写 RESP_ERR_GENERIC、
//! 慢臂两处 put_ttl 传错——值已提交而 TTL 缺席，键以无 TTL 永存态存续，客户
//! 端重试恒撞 BUSYKEY 自愈死锁，唯一绕路外部 DEL。
//!
//! 修复形态：三处故障臂同窗补偿删（restore_residual_rollback 单源，快慢臂共
//! 用；MSETNX 快臂回滚同款内核 try_delete_sync_with_prefix），墓碑镜像经物理
//! 写监听恰一次入 AOF，重放/副本终态与主端 absent 收敛；删失败 log::error
//! 留痕不 panic（MSETNX 回滚先例同款残余形）。
//!
//! 测试全真存储真协议帧，无 mock：故障注入沿 wkv/tests/write_kernel_failpath.rs
//! 的 StoreEventSink 写镜像注错形制——TtlWrite(Some) 镜像硬失败即 put_ttl 返
//! Err 且 TTL 记录已物理生效（「已生效 + 镜像缺失」Err 契约），预置「值写成
//! 功、TTL 写返 Err」；失败镜像不入对账日志，日志即 AOF/副本重放视角。

use std::{
  io::Error,
  mem::take,
  str::from_utf8,
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
};

use compio::runtime::Runtime;
use parking_lot::Mutex;
use tempfile::tempdir;
use wbase::{crc64::hash as crc64_hash, time::now_ticks};
use wdev::SegmentedDevice;
use wkv::{Error as WkvError, StoreConfig, StoreEvent, StoreEventSink, WedbStore};
use wnode::resp::{
  TtlResume,
  garnet_api::{GarnetApi, StoreGarnetApi},
  resp_server_session::{RespServerSession, RespServerSessionOptions},
  slow_path::SlowWait,
};
use wresp::{command::RespCommand, length::try_write_length};
use wval::NamespaceDbCodec;

/// 测试键（每测试独占一 store 实例，注错判据单键）
const KEY: &[u8] = b"restore-failpath-key";
/// 过期 100 秒（Garnet RESTORE/EX 口径为秒），PTTL 上界含粗化余量宽窗
const EXPIRE_SECONDS: &[u8] = b"100";
const PTTL_UPPER_MS: i64 = 130_000;

/// 应答帧（wresp 单点常量派生：RESP_ERR_GENERIC 无大写前缀走 ERR 前缀补全，
/// RESP_ERR_SLOW_PATH_STORAGE 自带 ERR 前缀直写）
const REPLY_OK: &[u8] = b"+OK\r\n";
const REPLY_ERR_GENERIC: &[u8] = b"-ERR generic error\r\n";
const REPLY_ERR_SLOW_PATH: &[u8] = b"-ERR slow path storage error\r\n";

/// 对账事件记号（到达序）：set=数据 Write 非墓碑、del=数据墓碑、
/// ttl+=TtlWrite(Some)、ttl-=TtlWrite(None)。注错拒发的镜像不入账（AOF 视角）
const EV_SET: &str = "set";
const EV_DEL: &str = "del";
const EV_TTL_SET: &str = "ttl+";
const EV_TTL_DEL: &str = "ttl-";

/// 注错上下文：armed 期间对 KEY 的 TTL 值镜像（TtlWrite(Some)）恒败——模拟
/// put_ttl 设备级硬故障（write_kernel_failpath 同款「已生效 + 镜像缺失」契约）；
/// 放行事件按到达序入对账日志
struct TtlFaultCtx {
  armed: AtomicBool,
  log: Mutex<Vec<&'static str>>,
}

fn ttl_value_mirror_fault(
  ctx: &TtlFaultCtx,
  _ver: i64,
  _aof_session_id: i32,
  event: StoreEvent<'_>,
) -> wkv::Result<()> {
  match event {
    // TTL 值镜像注错（注错臂先于记账，失败镜像不入 AOF）
    StoreEvent::TtlWrite {
      key,
      expire_at: Some(_),
      ..
    } if ctx.armed.load(Ordering::Relaxed) && key == KEY => Err(WkvError::Io(Error::other(
      "injected ttl value mirror failure",
    ))),
    // 数据物理写镜像（物理键解包后比对用户键）
    StoreEvent::Write { key, tombstone, .. } => {
      if NamespaceDbCodec::decode_tagged_key(key).is_ok_and(|(_, _, _, user_key)| user_key == KEY) {
        ctx.log.lock().push(if tombstone { EV_DEL } else { EV_SET });
      }
      Ok(())
    }
    // TTL 旁路镜像（已携解包用户键）
    StoreEvent::TtlWrite { key, expire_at, .. } if key == KEY => {
      ctx.log.lock().push(if expire_at.is_some() {
        EV_TTL_SET
      } else {
        EV_TTL_DEL
      });
      Ok(())
    }
    _ => Ok(()),
  }
}

/// 装配：真盘 wkv + 注错 sink（sink 注入先于会话创建，write_kernel_failpath
/// 同款注错形态；无 AOF 处理器占用 sink，事件流即对账视角）
fn harness(tag: &str, ctx: Arc<TtlFaultCtx>) -> (Runtime, GarnetApi, tempfile::TempDir) {
  let dir = tempdir().unwrap();
  let config = StoreConfig::new(1024, 64 * 1024, 16, 0.5).unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag)).unwrap());
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  assert!(store.set_event_sink(StoreEventSink::new(ctx, ttl_value_mirror_fault)));
  let api: GarnetApi = Arc::new(StoreGarnetApi::new(store.new_session().unwrap()));
  (Runtime::new().unwrap(), api, dir)
}

/// 挂接分派器的会话（降级链路须经 session.garnet_api 挂起 SlowWait）
fn session_with(api: &GarnetApi) -> RespServerSession {
  let mut s = RespServerSession::new(1, RespServerSessionOptions::default());
  s.set_garnet_api(api.clone());
  s
}

/// 单命令往返泵：快路径降级（零应答挂起 SlowWait）时按网络泵同款方式闭环
/// （msetnx_atomic.rs 同款）
fn exec(
  rt: &Runtime,
  api: &GarnetApi,
  s: &mut RespServerSession,
  cmd: RespCommand,
  args: &[&[u8]],
) -> Vec<u8> {
  s.output.clear();
  api.exec(s, cmd, args);
  if s.output.is_empty() {
    let slow = s.take_slow_wait().expect("降级必须挂起 SlowWait");
    rt.block_on(slow.resolve())
  } else {
    take(&mut s.output)
  }
}

/// 慢臂直驱（与降级快照投递同径，nx_conditional_ttl_degrade_replay.rs 同款）：
/// 快照尾参恒带续跑标记（exec 契约）
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

/// 解析 RESP 整数回执（:N\r\n）
fn parse_resp_int(resp: &[u8]) -> i64 {
  let body = resp.strip_prefix(b":").expect("应答须为整数帧");
  from_utf8(body.strip_suffix(b"\r\n").unwrap())
    .unwrap()
    .parse()
    .unwrap()
}

/// 读 PTTL 毫秒
fn pttl_ms(rt: &Runtime, api: &GarnetApi, s: &mut RespServerSession, key: &[u8]) -> i64 {
  parse_resp_int(&exec(rt, api, s, RespCommand::Pttl, &[key]))
}

/// 合法 RESTORE 载荷（类型 0x00 + 长度前缀 + 值 + rdb 版本 11 + crc64，
/// crc 含类型字节起算的 rust 口径，deviations §21）
fn restore_payload(val: &[u8]) -> Vec<u8> {
  let mut encoded_len = [0u8; 5];
  let written = try_write_length(val.len() as u32, &mut encoded_len).unwrap();
  let mut payload = Vec::with_capacity(1 + written + val.len() + 2 + 8);
  payload.push(0x00);
  payload.extend_from_slice(&encoded_len[..written]);
  payload.extend_from_slice(val);
  payload.extend_from_slice(&11u16.to_le_bytes());
  let crc = crc64_hash(&payload);
  payload.extend_from_slice(&crc);
  payload
}

/// 验证点 a（快臂）：RESTORE 带正 ttl 遭 TTL 硬故障出 -ERR 帧后，同窗补偿删
/// 使 GET 回 nil、EXISTS 回 :0，同命令重试出 +OK（无 BUSYKEY 死锁），AOF
/// 对账序 = 值 → TTL 清退 → 数据墓碑（重放端按序终态 absent 与主端一致）
#[test]
fn restore_fast_arm_ttl_hard_fault_compensates_residual() {
  let ctx = Arc::new(TtlFaultCtx {
    armed: AtomicBool::new(false),
    log: Mutex::new(Vec::new()),
  });
  let (rt, api, _dir) = harness("restore-ttl-fail-fast.db", Arc::clone(&ctx));
  let mut s = session_with(&api);
  let payload = restore_payload(b"payload");

  // 注错窗口：值镜像放行落库、TTL 值镜像硬故障
  ctx.armed.store(true, Ordering::Relaxed);
  assert_eq!(
    exec(
      &rt,
      &api,
      &mut s,
      RespCommand::Restore,
      &[KEY, EXPIRE_SECONDS, &payload]
    ),
    REPLY_ERR_GENERIC,
    "TTL 硬故障须回 -ERR 错误帧"
  );
  ctx.armed.store(false, Ordering::Relaxed);

  // 分裂态清除断言：键须彻底清退（修复前值永存：GET 有值、EXISTS :1）
  assert_eq!(
    exec(&rt, &api, &mut s, RespCommand::Get, &[KEY]),
    b"$-1\r\n",
    "补偿删后 GET 须回 nil（修复前无 TTL 永存僵尸键）"
  );
  assert_eq!(
    exec(&rt, &api, &mut s, RespCommand::Exists, &[KEY]),
    b":0\r\n",
    "补偿删后 EXISTS 须回 :0"
  );

  // BUSYKEY 死锁解除：同命令重试恒 +OK（修复前存活探针判存在恒 -BUSYKEY）
  assert_eq!(
    exec(
      &rt,
      &api,
      &mut s,
      RespCommand::Restore,
      &[KEY, EXPIRE_SECONDS, &payload]
    ),
    REPLY_OK,
    "补偿删后重试不得撞 BUSYKEY"
  );
  assert!(
    (1..=PTTL_UPPER_MS).contains(&pttl_ms(&rt, &api, &mut s, KEY)),
    "重试成功后 TTL 记录必在场"
  );

  // AOF/副本对账：失败笔 ttl+ 镜像注错拒发不入账，补偿删序 = ttl-（级联清
  // TTL，含已物理生效的半提交记录）→ del（数据墓碑）——重放端按序终态
  // absent 与主端一致；重试笔 = set → ttl+
  assert_eq!(
    ctx.log.lock().as_slice(),
    [EV_SET, EV_TTL_DEL, EV_DEL, EV_SET, EV_TTL_SET],
    "AOF 镜像序：值 → TTL 清退 → 数据墓碑（absent 终态）→ 重试值 → TTL"
  );
}

/// 验证点 b（慢臂同构锁）：同输入同注错下慢臂 upsert_string 后 put_ttl 硬
/// 故障交执行域统一应答（-ERR slow path storage error），终态与快臂同构：
/// 键缺席 + 重试 +OK + AOF 对账序一致
#[test]
fn restore_slow_arm_ttl_hard_fault_same_terminal_state() {
  let ctx = Arc::new(TtlFaultCtx {
    armed: AtomicBool::new(false),
    log: Mutex::new(Vec::new()),
  });
  let (rt, api, _dir) = harness("restore-ttl-fail-slow.db", Arc::clone(&ctx));
  let mut s = session_with(&api);
  let payload = restore_payload(b"payload");

  // 慢臂直驱（Full 尾参全量重放）
  ctx.armed.store(true, Ordering::Relaxed);
  let out = slow_direct(
    &rt,
    &api,
    RespCommand::Restore,
    &[KEY, EXPIRE_SECONDS, &payload],
    TtlResume::Full,
  );
  assert_eq!(
    out, REPLY_ERR_SLOW_PATH,
    "慢臂 TTL 硬故障须交执行域统一应答"
  );
  ctx.armed.store(false, Ordering::Relaxed);

  // 同输入同终态：键缺席
  assert_eq!(
    exec(&rt, &api, &mut s, RespCommand::Get, &[KEY]),
    b"$-1\r\n",
    "慢臂补偿删后 GET 须回 nil"
  );
  assert_eq!(
    exec(&rt, &api, &mut s, RespCommand::Exists, &[KEY]),
    b":0\r\n",
    "慢臂补偿删后 EXISTS 须回 :0"
  );

  // 快路径重试出 +OK 且 TTL 在场
  assert_eq!(
    exec(
      &rt,
      &api,
      &mut s,
      RespCommand::Restore,
      &[KEY, EXPIRE_SECONDS, &payload]
    ),
    REPLY_OK,
    "慢臂补偿删后快路径重试不得撞 BUSYKEY"
  );
  assert!(
    (1..=PTTL_UPPER_MS).contains(&pttl_ms(&rt, &api, &mut s, KEY)),
    "重试成功后 TTL 记录必在场"
  );

  // 快慢双臂同构锁：AOF 对账序逐位一致
  assert_eq!(
    ctx.log.lock().as_slice(),
    [EV_SET, EV_TTL_DEL, EV_DEL, EV_SET, EV_TTL_SET],
    "慢臂与快臂 AOF 镜像序须同构"
  );
}

/// 验证点 c（Pending 续跑臂）：快臂零 ttl 落库成功后，Pending 续跑补投 TTL
/// 硬故障的补偿对象严格为快臂已提交残值——键缺席，解除注错重试 +OK
#[test]
fn restore_pending_resume_fault_compensates_fast_arm_value() {
  let ctx = Arc::new(TtlFaultCtx {
    armed: AtomicBool::new(false),
    log: Mutex::new(Vec::new()),
  });
  let (rt, api, _dir) = harness("restore-ttl-fail-pending.db", Arc::clone(&ctx));
  let mut s = session_with(&api);
  let payload = restore_payload(b"committed");

  // 预置快臂已提交态：零 ttl RESTORE 落值（Pending 续跑臂的补偿对象即此残值）
  assert_eq!(
    exec(
      &rt,
      &api,
      &mut s,
      RespCommand::Restore,
      &[KEY, b"0", &payload]
    ),
    REPLY_OK
  );
  assert_eq!(ctx.log.lock().as_slice(), [EV_SET], "预置自检：仅值落库");

  // 注错窗口：Pending 续跑补投 TTL 硬故障
  ctx.armed.store(true, Ordering::Relaxed);
  let ticks = now_ticks() + 100 * 10_000_000;
  let out = slow_direct(
    &rt,
    &api,
    RespCommand::Restore,
    &[KEY, EXPIRE_SECONDS, &payload],
    TtlResume::Pending(ticks),
  );
  assert_eq!(
    out, REPLY_ERR_SLOW_PATH,
    "Pending 续跑补投硬故障须交执行域统一应答"
  );
  ctx.armed.store(false, Ordering::Relaxed);

  // 快臂已提交残值被补偿删：键缺席（补投失败臂与快臂 Err 臂同缝同法）
  assert_eq!(
    exec(&rt, &api, &mut s, RespCommand::Get, &[KEY]),
    b"$-1\r\n",
    "Pending 补投失败后快臂已提交残值须被补偿删"
  );
  assert_eq!(
    exec(&rt, &api, &mut s, RespCommand::Exists, &[KEY]),
    b":0\r\n",
    "Pending 补投失败后 EXISTS 须回 :0"
  );

  // 解除注错重试 +OK 且 TTL 在场
  assert_eq!(
    exec(
      &rt,
      &api,
      &mut s,
      RespCommand::Restore,
      &[KEY, EXPIRE_SECONDS, &payload]
    ),
    REPLY_OK,
    "补偿删后重试不得撞 BUSYKEY"
  );
  assert!(
    (1..=PTTL_UPPER_MS).contains(&pttl_ms(&rt, &api, &mut s, KEY)),
    "重试成功后 TTL 记录必在场"
  );

  // 对账：Pending 失败笔 = ttl- → del（补投已物理生效的 TTL 记录被级联清退
  // 后数据墓碑收尾），重试笔 = set → ttl+
  assert_eq!(
    ctx.log.lock().as_slice(),
    [EV_SET, EV_TTL_DEL, EV_DEL, EV_SET, EV_TTL_SET],
    "Pending 失败笔补偿删 AOF 镜像序"
  );
}

/// 验证点 d（零 ttl 零回归锁）：expiry<=0 不进 put_ttl，注错在臂恒 +OK——
/// 补偿只挂 TTL 硬故障臂，零 ttl 路径零涟漪
#[test]
fn restore_zero_ttl_ignores_ttl_fault_injection() {
  let ctx = Arc::new(TtlFaultCtx {
    armed: AtomicBool::new(false),
    log: Mutex::new(Vec::new()),
  });
  let (rt, api, _dir) = harness("restore-ttl-fail-zero.db", Arc::clone(&ctx));
  let mut s = session_with(&api);
  let payload = restore_payload(b"eternal");

  ctx.armed.store(true, Ordering::Relaxed);
  assert_eq!(
    exec(
      &rt,
      &api,
      &mut s,
      RespCommand::Restore,
      &[KEY, b"0", &payload]
    ),
    REPLY_OK,
    "零 ttl 不进 put_ttl，注错在臂恒 +OK"
  );
  ctx.armed.store(false, Ordering::Relaxed);
  assert_eq!(
    exec(&rt, &api, &mut s, RespCommand::Get, &[KEY]),
    b"$7\r\neternal\r\n",
    "零 ttl 落库值可读回"
  );
  assert_eq!(
    exec(&rt, &api, &mut s, RespCommand::Exists, &[KEY]),
    b":1\r\n",
    "零 ttl 键存活"
  );
  assert_eq!(ctx.log.lock().as_slice(), [EV_SET], "零 ttl 无 TTL 镜像");
}
