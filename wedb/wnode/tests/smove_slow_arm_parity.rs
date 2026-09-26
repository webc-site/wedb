//! SMOVE 慢臂三态化收敛语义锁（票 zcode-r155c-smovettl 案四）
//!
//! 背景：smove_cold 旧臂经 obj_load 将 SealedLoad::Missing 折叠为空集对象后，
//! 以 `src.set.is_empty()` 单判据在同键检与 dst 类型门之前短路 :0，把「源在场
//! 空集 ∧ dst 异构」形的 dst 判定序折叠掉——快臂（set_move，Present 空集不早
//! 退，dst 门出 -WRONGTYPE）与 C#（SetOps.cs:285-304 记录在场恒 OK、dst 门先
//! 于摘除）判定序不同构。修复：src 装载转引 obj_load_shortcircuit 三态壳，
//! 判定序收敛为 Missing 短路 → 同键 :0 → dst 三态门 → take，与快臂逐位同构。
//!
//! 本锁两格（稳态该形经全写门构造不可达，属防御序收敛，直灌夹具绕命令门现形）：
//! a) 快慢双径同构锁：直灌「Set 标签空载荷信封」在场源 + 字符串 dst，分驱快臂
//!    与 flush_and_evict_all 强制降级慢臂（§100 冷键降级形制），断双臂
//!    -WRONGTYPE 帧逐字节全等且两键零变异；
//! b) src 缺失形序锁：源经 TTL 判缺（信封在场 + 已到期键级 TTL 旁路记录，
//!    读内核判死于装载核 TTL 门 Due→Missing）。到期记录以 Ttl 标签直灌：
//!    expire 命令路带「写侧过期即删」臂（wkv expire_at_apply 落笔过去时刻
//!    即物理清除信封与 TTL），键根本不成磁盘候选、慢臂不可达（此系本锁前
//!    提，非产品缺口）+ 字符串 dst，断双臂恒 :0——:0 先于 dst 判定
//!    （hllsec2 案二格 a 钉死序、§19 源缺失臂口径），dst 若先判即现
//!    -WRONGTYPE，序反转即红灯。

use std::{mem::take, sync::Arc};

use compio::runtime::Runtime;
use tempfile::tempdir;
use wbase::{convert::TICKS_PER_MILLISECOND, time::now_ticks};
use wcol::{object_payload::GarnetObjectPayload, set::set_object::SetObject};
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::{
  resp::{
    garnet_api::{GarnetApi, StoreGarnetApi},
    resp_server_session::{RespServerSession, RespServerSessionOptions},
  },
  storage::session::storage_session::StorageSession,
};
use wresp::command::RespCommand;
use wval::{I64Codec, KeyTag};

type TestStore = WedbStore<SegmentedDevice>;

/// 小容量单文件存储执行域（object_cold_degrade.rs 同款，附 store 句柄供
/// 直灌与驱逐）
fn open_env(tag: &str) -> (Runtime, GarnetApi, Arc<TestStore>, tempfile::TempDir) {
  let dir = tempdir().unwrap();
  let config = StoreConfig::new(1024, 64 * 1024, 16, 0.5).unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag)).unwrap());
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  (
    Runtime::new().unwrap(),
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
    store,
    dir,
  )
}

/// 挂接分派器的会话（降级链路须经 session.garnet_api 挂起 SlowWait）
fn session_with(api: &GarnetApi) -> RespServerSession {
  let mut s = RespServerSession::new(1, RespServerSessionOptions::default());
  s.set_garnet_api(api.clone());
  s
}

/// 同步闭环执行：热键形态必须同步应答（快臂实证）
fn sync_exec(
  api: &GarnetApi,
  s: &mut RespServerSession,
  cmd: RespCommand,
  args: &[&[u8]],
) -> Vec<u8> {
  s.output.clear();
  api.exec(s, cmd, args);
  let out = take(&mut s.output);
  assert!(!out.is_empty(), "无降级形态的 {cmd} 必须同步闭环而非挂起");
  out
}

/// 冷键降级注入执行：快路径必须空应答挂起 SlowWait（绝不同步误答），
/// 驱动挂起体闭环后返回慢路径应答字节（慢臂实证，§100 冷键降级形制）
fn cold_exec(
  api: &GarnetApi,
  rt: &Runtime,
  s: &mut RespServerSession,
  cmd: RespCommand,
  args: &[&[u8]],
) -> Vec<u8> {
  s.output.clear();
  api.exec(s, cmd, args);
  assert!(
    s.output.is_empty(),
    "冷键 {cmd} 必须降级挂起而非同步应答：{:?}",
    String::from_utf8_lossy(&s.output)
  );
  let slow = s
    .take_slow_wait()
    .unwrap_or_else(|| panic!("冷键 {cmd} 降级未挂起 SlowWait"));
  let out = rt.block_on(slow.resolve());
  s.output.clear();
  out
}

/// 冷态换入后的杂项读取：不预设臂型，同步应答或挂起慢路径皆可，只取字节
fn any_exec(
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

/// a) 快慢双径同构锁：源在场空集信封 + dst 字符串键，双臂 -WRONGTYPE
/// 帧逐字节全等且两键零变异（防 is_empty 折叠臂回归：修复前慢臂该形回 :0）
#[test]
fn smove_in_place_empty_src_both_arms_wrongtype_frame_parity() {
  let (rt, api, store, _dir) = open_env("smove-empty-src-parity.db");

  // 直灌：src = Set 标签空载荷信封（命令门 SADD→SREM 删空即整键回收，
  // 在场空集唯直灌可构造）；dst = 字符串键
  {
    let session = store.new_session().unwrap();
    let ss = StorageSession::new(session.enter_batch());
    let blob = SetObject::new().to_blob();
    rt.block_on(async {
      ss.obj_save(b"empty_src", SetObject::OBJECT_TAG, &blob)
        .await
        .unwrap();
      ss.upsert_string(b"dst_str", b"v").await.unwrap();
    });
  }

  let mut s = session_with(&api);
  // 快臂：src Present 空集不早退，dst 类型门出 -WRONGTYPE
  let out_fast = sync_exec(
    &api,
    &mut s,
    RespCommand::Smove,
    &[b"empty_src", b"dst_str", b"m"],
  );
  // 慢臂：全键落冷强制降级（exec_slow 冷键分派），重放走 smove_cold
  rt.block_on(store.flush_and_evict_all()).unwrap();
  let out_slow = cold_exec(
    &api,
    &rt,
    &mut s,
    RespCommand::Smove,
    &[b"empty_src", b"dst_str", b"m"],
  );

  assert!(
    out_fast.starts_with(b"-WRONGTYPE"),
    "快臂在场空集 + 异构 dst 必出 -WRONGTYPE，实际: {:?}",
    String::from_utf8_lossy(&out_fast)
  );
  assert_eq!(
    out_fast, out_slow,
    "快慢双臂 -WRONGTYPE 帧必逐字节全等（折叠臂回归即此格红灯）"
  );

  // 两键零变异：src 仍为在场空集信封（TYPE set / SCARD :0），dst 字符串原样
  assert_eq!(
    any_exec(&api, &rt, &mut s, RespCommand::Type, &[b"empty_src"]),
    b"+set\r\n",
    "双臂被拒后 src 仍须在场且为 set 标签"
  );
  assert_eq!(
    any_exec(&api, &rt, &mut s, RespCommand::Scard, &[b"empty_src"]),
    b":0\r\n",
    "src 空集载荷零变异（未被搬运/回收改动）"
  );
  assert_eq!(
    any_exec(&api, &rt, &mut s, RespCommand::Get, &[b"dst_str"]),
    b"$1\r\nv\r\n",
    "dst 字符串键零变异"
  );
}

/// b) src 缺失形序锁：源经 TTL 判缺 + dst 字符串键，双臂恒 :0——
/// Missing→:0 短路先于 dst 判定，序反转（dst 先判）即现 -WRONGTYPE 红灯
#[test]
fn smove_missing_src_shortcircuit_precedes_destination_check_both_arms() {
  let (rt, api, store, _dir) = open_env("smove-missing-src-order.db");

  // 直灌：src = 带单成员信封 + 已到期键级 TTL 旁路记录（Ttl 标签 I64 大端
  // 绝对 ticks，时刻取过去值：快臂装载核 TTL 门 Due→Missing 判缺，§19 源
  // 缺失臂同款判死；flush_and_evict_all 后信封+TTL 成磁盘候选，慢臂经该键
  // 磁盘候选触发降级挂起）。前提锁：会话 expire_in_ticks/expire_at_ticks
  // 命令路径带「写侧过期即删」臂（wkv expire_at_apply：is_expired_or_now→
  // purge_expired），落笔过去时刻会当场物理清除信封与 TTL 记录，键根本不
  // 成磁盘候选、慢臂不可达；纯无键同样不可达慢臂。故绕命令门直灌到期旁路
  // 记录，以在场到期形钉 Missing 臂；dst = 字符串
  {
    let session = store.new_session().unwrap();
    let ss = StorageSession::new(session.enter_batch());
    let mut obj = SetObject::new();
    obj.set.insert(b"m".to_vec());
    let blob = obj.to_blob();
    rt.block_on(async {
      ss.obj_save(b"exp_src", SetObject::OBJECT_TAG, &blob)
        .await
        .unwrap();
      ss.upsert_string(b"dst_str2", b"v").await.unwrap();
      // 过去时刻绝对 ticks：读内核判死于 TTL 门，且不触发写侧过期即删臂
      ss.upsert_tag(
        b"exp_src",
        KeyTag::Ttl,
        &I64Codec::encode(now_ticks() - 10 * TICKS_PER_MILLISECOND),
      )
      .await
      .unwrap();
    });
  }

  let mut s = session_with(&api);
  // 快臂：src 判缺 → :0 先于 dst（dst 字符串键不得引 WRONGTYPE）
  assert_eq!(
    sync_exec(
      &api,
      &mut s,
      RespCommand::Smove,
      &[b"exp_src", b"dst_str2", b"m"],
    ),
    b":0\r\n",
    "快臂源判缺恒 :0（§19 口径）"
  );
  // 慢臂：落冷降级重放，smove_cold src 三态装载 Missing 臂短路 :0，
  // 先于同键检与 dst 三态门（hllsec2 案二格 a 钉死序）
  rt.block_on(store.flush_and_evict_all()).unwrap();
  assert_eq!(
    cold_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Smove,
      &[b"exp_src", b"dst_str2", b"m"],
    ),
    b":0\r\n",
    "慢臂源判缺 :0 必先于 dst 判定（序反转此格必红）"
  );

  // dst 零变异且仍为字符串（dst 若被先判即应答 -WRONGTYPE，键亦不该动）
  assert_eq!(
    any_exec(&api, &rt, &mut s, RespCommand::Get, &[b"dst_str2"]),
    b"$1\r\nv\r\n",
    "src 缺失臂不得触碰 dst 判定与数据"
  );
}
