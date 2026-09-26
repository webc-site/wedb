//! RENAME/RENAMENX 快路径对「物理在位、TTL 已过期未清退」目标键残留的双域
//! 预清缺失与 NX 写原语误用回归（票 zcode-r147c-renamedb 案一 P1 / 案二 P2）
//!
//! 案一（P1，数据面）：快径 `rename_sync` 的新键预清块整段门在 `if !nx`，
//! RENAMENX 的 ObjectEnvelope 臂因此跳过目标键旁域清退——存活探针（含 TTL
//! 裁决）判死的过期 String 残留物理仍在，信封插入后出两种终态分叉：
//! 源键带 TTL 时 `put_ttl_sync` 以源 TTL 覆写残留 TTL 门，读面 String 域优先
//! 命中 → 集合键不可达且暴露残值（WRONGTYPE，跨域脑裂恒可见）；源键无 TTL
//! 时残留的过期 TTL 旁路对三域通用（ttl_sync `probe_alive_domain_with_prefix`
//! 单一门），信封与 String 同判已死 → 整键隐死（rename 后键消失）。慢径
//! `rename_slow` Obj 臂的 `delete_string(new_key)` 位于 NX 判定之后、无条件
//! 执行，双臂不同构即多路径行为分叉。
//!
//! 案二（P2，应答形）：快径 `(true, Str)` 用纯物理 NX 插口 `try_insert_sync`，
//! 探针放行后物理在场即假回 `:0`（客户端读作「目标已存活未 rename」），慢径
//! Str 臂同输入用 `upsert_string`（SET 语义）回 `:1`。同机理亦见于
//! `(true, Obj)` 的 `try_insert_tag_sync`（过期信封残留 → 假 `:0`）。
//!
//! C# 契约（修复所本）：UnifiedStoreOps.cs:RENAME
//! - :298 `GET(newKey)` + :301-306 `if (isNX && newExists)` —— 存活判定
//!   （GET 内 CheckExpiry 令过期键报 NOTFOUND）是**唯一** NX 裁决；
//! - :338-347 `needDeleteNewKey` 与 isNX 无关联；
//! - :363 `SET(newKey, in logRecord)` —— isNX 与非 isNX 共用同一写入口
//!   （Tsavorite Upsert），过期 LogRecord 直接被新记录替换，rust 分域布局下
//!   须显式清退目标域旧记录方等价。
//!
//! 修复形态：Obj 域预清移出 `!nx` 块无条件执行（复用 `try_delete_sync` 单点，
//! 零新增判据）；写新键收为 String/信封域各一套 upsert（SET 语义），`nx`
//! 不再参与写原语选型，`Ok(Ok(false))` 假拒臂随之消除。
//!
//! 夹具：过期不过 purge 的「物理残留态」经 `put_ttl_sync` 裸写过去刻度确定性
//! 构造（RESP 面无法自然构造过期未清态，先例
//! restore_expired_residual_busykey.rs / probe_alive_single_point.rs；
//! `open_test_store` 已关 gc，残留恒在）。慢臂对偶锁直驱 `SlowWait::for_command`
//! （与快臂降级快照投递同径，RENAME 族快照无续跑尾参），非 mock。

use std::{mem::take, str::from_utf8, sync::Arc};

use compio::runtime::Runtime;
use wbase::{convert::TICKS_PER_SECOND, time::now_ticks};
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  resp::{
    garnet_api::{GarnetApi, StoreGarnetApi},
    resp_server_session::{RespServerSession, RespServerSessionOptions},
    slow_path::SlowWait,
  },
  storage::session::common::ttl_sync::put_ttl_sync,
};
use wresp::command::RespCommand;
use wtest_base::open_test_store;

const REPLY_OK: &[u8] = b"+OK\r\n";
const REPLY_ONE: &[u8] = b":1\r\n";
const REPLY_ZERO: &[u8] = b":0\r\n";
const REPLY_NIL: &[u8] = b"$-1\r\n";
const REPLY_NO_TTL: &[u8] = b":-1\r\n";
const REPLY_TYPE_HASH: &[u8] = b"+hash\r\n";
const REPLY_TYPE_NONE: &[u8] = b"+none\r\n";

fn api_of(store: &Arc<WedbStore<SegmentedDevice>>) -> GarnetApi {
  Arc::new(StoreGarnetApi::new(store.new_session().unwrap()))
}

fn session_with(api: &GarnetApi) -> RespServerSession {
  let mut s = RespServerSession::new(1, RespServerSessionOptions::default());
  s.set_garnet_api(api.clone());
  s
}

/// 快臂执行：零应答即整体降级（磁盘候选/环形页翻转），沿既有通道泵慢臂至
/// 完成，应答与快臂同道返回
fn exec(
  rt: &Runtime,
  api: &GarnetApi,
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

/// 慢臂直驱（与降级快照投递同径：exec_slow → rename_slow）
fn exec_slow(
  rt: &Runtime,
  api: &GarnetApi,
  cmd: RespCommand,
  old_key: &[u8],
  new_key: &[u8],
) -> Vec<u8> {
  rt.block_on(
    SlowWait::for_command(
      api,
      cmd,
      vec![old_key.to_vec(), new_key.to_vec()],
      wconf::DEFAULT_RESP_VERSION,
    )
    .resolve(),
  )
}

/// 裸写过去刻度构造「物理在位、TTL 已过期未清退」残留态
fn expire_residual(store: &Arc<WedbStore<SegmentedDevice>>, key: &[u8]) {
  let session = store.new_session().unwrap();
  let batch = session.enter_batch();
  put_ttl_sync(&batch, key, now_ticks() - TICKS_PER_SECOND).unwrap();
}

/// 残留态前置确证：逻辑已死（EXISTS :0），bulk 读为 nil
fn assert_residual_dead(rt: &Runtime, api: &GarnetApi, s: &mut RespServerSession, key: &[u8]) {
  assert_eq!(
    exec(rt, api, s, RespCommand::Exists, &[key]),
    REPLY_ZERO,
    "目标键须先判死（过期残留前提）"
  );
  assert_eq!(exec(rt, api, s, RespCommand::Pttl, &[key]), b":-2\r\n");
}

/// TTL 随迁断言（EXPIRE 3600 写入值秒级读数宽窗，杜绝双臂执行间隔与
/// 秒级取整抖动误伤；绝对值不校 100s/-1 等错态）
fn assert_ttl_migrated(rt: &Runtime, api: &GarnetApi, s: &mut RespServerSession, key: &[u8]) {
  let ttl = exec(rt, api, s, RespCommand::Ttl, &[key]);
  let secs: i64 = ttl
    .strip_prefix(b":")
    .and_then(|b| b.strip_suffix(b"\r\n"))
    .and_then(|b| from_utf8(b).ok())
    .and_then(|v| v.parse().ok())
    .unwrap_or_else(|| panic!("TTL 非整数回执：{ttl:?}"));
  assert!(
    (3580..=3601).contains(&secs),
    "{:?} 须携带随迁 TTL ≈3600s，实测 {secs}s",
    String::from_utf8_lossy(key)
  );
}

/// 案一场景 A 锁：源为带 TTL 的 hash，目标为过期 String 残留——RENAMENX 须
/// :1，读面须命中信封域（修复前：残留 String 优先命中 → WRONGTYPE，
/// 且残留 TTL 门被随迁 TTL 改判现势，跨域脑裂恒可见）
#[test]
fn renamenx_obj_src_with_ttl_over_expired_string_residual() {
  let (_dir, store) = open_test_store("rename-nx-obj-ttl.db").unwrap();
  let rt = Runtime::new().unwrap();
  let api = api_of(&store);
  let mut s = session_with(&api);

  assert_eq!(
    exec(&rt, &api, &mut s, RespCommand::Set, &[b"dst_a", b"residue"]),
    REPLY_OK
  );
  expire_residual(&store, b"dst_a");
  assert_residual_dead(&rt, &api, &mut s, b"dst_a");
  assert_eq!(
    exec(&rt, &api, &mut s, RespCommand::Get, &[b"dst_a"]),
    REPLY_NIL
  );

  assert_eq!(
    exec(
      &rt,
      &api,
      &mut s,
      RespCommand::Hset,
      &[b"src_a", b"f", b"v"]
    ),
    REPLY_ONE
  );
  assert_eq!(
    exec(&rt, &api, &mut s, RespCommand::Expire, &[b"src_a", b"3600"]),
    REPLY_ONE
  );

  assert_eq!(
    exec(
      &rt,
      &api,
      &mut s,
      RespCommand::Renamenx,
      &[b"src_a", b"dst_a"]
    ),
    REPLY_ONE,
    "案一场景 A：RENAMENX 须 :1"
  );
  assert_eq!(
    exec(&rt, &api, &mut s, RespCommand::Hget, &[b"dst_a", b"f"]),
    b"$1\r\nv\r\n",
    "修复前目标键双域并存，HGET 取到残留 String → WRONGTYPE"
  );
  assert_eq!(
    exec(&rt, &api, &mut s, RespCommand::Type, &[b"dst_a"]),
    REPLY_TYPE_HASH
  );
  assert_ttl_migrated(&rt, &api, &mut s, b"dst_a");
  assert_eq!(
    exec(&rt, &api, &mut s, RespCommand::Exists, &[b"src_a"]),
    REPLY_ZERO,
    "源键须随 rename 消失"
  );
}

/// 案一场景 B 锁：源为无 TTL 的 hash，目标为过期 String 残留——RENAMENX 须
/// :1 且键存活（修复前：残留过期 TTL 旁路三域通用，信封与 String 同判已死
/// → 整键隐死，rename 后键消失）
#[test]
fn renamenx_obj_src_without_ttl_over_expired_string_residual() {
  let (_dir, store) = open_test_store("rename-nx-obj-nottl.db").unwrap();
  let rt = Runtime::new().unwrap();
  let api = api_of(&store);
  let mut s = session_with(&api);

  assert_eq!(
    exec(&rt, &api, &mut s, RespCommand::Set, &[b"dst_b", b"residue"]),
    REPLY_OK
  );
  expire_residual(&store, b"dst_b");
  assert_residual_dead(&rt, &api, &mut s, b"dst_b");

  assert_eq!(
    exec(
      &rt,
      &api,
      &mut s,
      RespCommand::Hset,
      &[b"src_b", b"f", b"v"]
    ),
    REPLY_ONE
  );

  assert_eq!(
    exec(
      &rt,
      &api,
      &mut s,
      RespCommand::Renamenx,
      &[b"src_b", b"dst_b"]
    ),
    REPLY_ONE,
    "案一场景 B：RENAMENX 须 :1"
  );
  assert_eq!(
    exec(&rt, &api, &mut s, RespCommand::Exists, &[b"dst_b"]),
    REPLY_ONE,
    "修复前残留过期 TTL 令整键隐死（EXISTS :0，数据丢失）"
  );
  assert_eq!(
    exec(&rt, &api, &mut s, RespCommand::Hget, &[b"dst_b", b"f"]),
    b"$1\r\nv\r\n"
  );
  assert_eq!(
    exec(&rt, &api, &mut s, RespCommand::Pttl, &[b"dst_b"]),
    REPLY_NO_TTL,
    "源键无 TTL：残留 TTL 须随预清退，新键不得借尸还魂"
  );
  assert_eq!(
    exec(&rt, &api, &mut s, RespCommand::Exists, &[b"src_b"]),
    REPLY_ZERO
  );
}

/// 案一/案二同机理的 Obj 假拒锁：目标为过期信封残留（源同为 hash）——
/// 修复前 `(true, Obj)` 走纯物理 NX 插口，探针判死而物理在场即假回 :0
#[test]
fn renamenx_obj_src_over_expired_envelope_residual() {
  let (_dir, store) = open_test_store("rename-nx-obj-env.db").unwrap();
  let rt = Runtime::new().unwrap();
  let api = api_of(&store);
  let mut s = session_with(&api);

  assert_eq!(
    exec(
      &rt,
      &api,
      &mut s,
      RespCommand::Hset,
      &[b"dst_c", b"f", b"old"]
    ),
    REPLY_ONE
  );
  expire_residual(&store, b"dst_c");
  assert_residual_dead(&rt, &api, &mut s, b"dst_c");

  assert_eq!(
    exec(
      &rt,
      &api,
      &mut s,
      RespCommand::Hset,
      &[b"src_c", b"f", b"v"]
    ),
    REPLY_ONE
  );

  assert_eq!(
    exec(
      &rt,
      &api,
      &mut s,
      RespCommand::Renamenx,
      &[b"src_c", b"dst_c"]
    ),
    REPLY_ONE,
    "过期信封残留目标键：修复前物理 NX 插假拒回 :0"
  );
  assert_eq!(
    exec(&rt, &api, &mut s, RespCommand::Hget, &[b"dst_c", b"f"]),
    b"$1\r\nv\r\n",
    "终态须为源键载荷整体搬移，非残留旧值"
  );
  assert_eq!(
    exec(&rt, &api, &mut s, RespCommand::Pttl, &[b"dst_c"]),
    REPLY_NO_TTL
  );
  assert_eq!(
    exec(&rt, &api, &mut s, RespCommand::Exists, &[b"src_c"]),
    REPLY_ZERO
  );
}

/// 案二锁：源为 String、目标为过期 String 残留——RENAMENX 须 :1（修复前快径
/// `try_insert_sync` 物理在场假回 :0，慢径同输入回 :1，双臂分叉）；
/// 同锁维持存活目标键的 :0 契约零漂移（C# :301-306）
#[test]
fn renamenx_str_src_over_expired_string_residual() {
  let (_dir, store) = open_test_store("rename-nx-str.db").unwrap();
  let rt = Runtime::new().unwrap();
  let api = api_of(&store);
  let mut s = session_with(&api);

  assert_eq!(
    exec(&rt, &api, &mut s, RespCommand::Set, &[b"dst_s", b"residue"]),
    REPLY_OK
  );
  expire_residual(&store, b"dst_s");
  assert_residual_dead(&rt, &api, &mut s, b"dst_s");

  assert_eq!(
    exec(&rt, &api, &mut s, RespCommand::Set, &[b"src_s", b"val2"]),
    REPLY_OK
  );
  assert_eq!(
    exec(
      &rt,
      &api,
      &mut s,
      RespCommand::Renamenx,
      &[b"src_s", b"dst_s"]
    ),
    REPLY_ONE,
    "案二：过期残留目标键 RENAMENX 须 :1（修复前假回 :0）"
  );
  assert_eq!(
    exec(&rt, &api, &mut s, RespCommand::Get, &[b"dst_s"]),
    b"$4\r\nval2\r\n"
  );
  assert_eq!(
    exec(&rt, &api, &mut s, RespCommand::Pttl, &[b"dst_s"]),
    REPLY_NO_TTL,
    "SET 语义单写入口自清新键残留 TTL"
  );
  assert_eq!(
    exec(&rt, &api, &mut s, RespCommand::Exists, &[b"src_s"]),
    REPLY_ZERO
  );

  // 零漂移：目标键存活 → :0，双键俱在（探针为唯一 NX 判据，写面不另起裁决）
  assert_eq!(
    exec(&rt, &api, &mut s, RespCommand::Set, &[b"src_l", b"v1"]),
    REPLY_OK
  );
  assert_eq!(
    exec(&rt, &api, &mut s, RespCommand::Set, &[b"dst_l", b"v2"]),
    REPLY_OK
  );
  assert_eq!(
    exec(
      &rt,
      &api,
      &mut s,
      RespCommand::Renamenx,
      &[b"src_l", b"dst_l"]
    ),
    REPLY_ZERO
  );
  assert_eq!(
    exec(&rt, &api, &mut s, RespCommand::Get, &[b"src_l"]),
    b"$2\r\nv1\r\n"
  );
  assert_eq!(
    exec(&rt, &api, &mut s, RespCommand::Get, &[b"dst_l"]),
    b"$2\r\nv2\r\n"
  );

  // 零漂移：目标为存活 hash、源为 hash → :0 且信封域不动
  assert_eq!(
    exec(
      &rt,
      &api,
      &mut s,
      RespCommand::Hset,
      &[b"src_h", b"f", b"v"]
    ),
    REPLY_ONE
  );
  assert_eq!(
    exec(
      &rt,
      &api,
      &mut s,
      RespCommand::Hset,
      &[b"dst_h", b"f", b"w"]
    ),
    REPLY_ONE
  );
  assert_eq!(
    exec(
      &rt,
      &api,
      &mut s,
      RespCommand::Renamenx,
      &[b"src_h", b"dst_h"]
    ),
    REPLY_ZERO
  );
  assert_eq!(
    exec(&rt, &api, &mut s, RespCommand::Hget, &[b"dst_h", b"f"]),
    b"$1\r\nw\r\n"
  );
  assert_eq!(
    exec(&rt, &api, &mut s, RespCommand::Hget, &[b"src_h", b"f"]),
    b"$1\r\nv\r\n"
  );
}

/// 目标键残留 × 源键形态（与快臂各案同形）
#[derive(Clone, Copy)]
enum Shape {
  /// 源 hash 带 TTL × 目标过期 String 残留（案一场景 A）
  ObjTtlVsString,
  /// 源 hash 无 TTL × 目标过期 String 残留（案一场景 B）
  ObjVsString,
  /// 源 hash × 目标过期信封残留（`(true, Obj)` 假拒形）
  ObjVsEnvelope,
  /// 源 String × 目标过期 String 残留（案二形）
  StrVsString,
  /// 源 String × 目标存活 String（`:0` 零漂移形）
  StrVsAlive,
}

impl Shape {
  const ALL: [Self; 5] = [
    Self::ObjTtlVsString,
    Self::ObjVsString,
    Self::ObjVsEnvelope,
    Self::StrVsString,
    Self::StrVsAlive,
  ];

  fn tag(self) -> &'static str {
    match self {
      Self::ObjTtlVsString => "a",
      Self::ObjVsString => "b",
      Self::ObjVsEnvelope => "c",
      Self::StrVsString => "d",
      Self::StrVsAlive => "e",
    }
  }

  /// 两臂共同应有的应答（案一/案二修复后的 C# 契约形）
  fn expect(self) -> &'static [u8] {
    match self {
      Self::StrVsAlive => REPLY_ZERO,
      _ => REPLY_ONE,
    }
  }

  /// 目标键终态值面绝对锁（源键载荷整体搬移到位，防两臂同错的相对断言空转）
  fn expect_value(self) -> &'static [u8] {
    match self {
      Self::StrVsString => b"$4\r\nval2\r\n",
      Self::StrVsAlive => b"$2\r\nv2\r\n",
      _ => b"$1\r\nv\r\n",
    }
  }
}

/// 铺一形态：源键 `{p}:s`、目标键 `{p}:d`
fn prepare(
  rt: &Runtime,
  store: &Arc<WedbStore<SegmentedDevice>>,
  api: &GarnetApi,
  s: &mut RespServerSession,
  p: &str,
  shape: Shape,
) {
  let (src, dst) = (format!("{p}:s"), format!("{p}:d"));
  let (src, dst) = (src.as_bytes(), dst.as_bytes());
  match shape {
    Shape::ObjTtlVsString => {
      assert_eq!(
        exec(rt, api, s, RespCommand::Set, &[dst, b"residue"]),
        REPLY_OK
      );
      expire_residual(store, dst);
      assert_eq!(
        exec(rt, api, s, RespCommand::Hset, &[src, b"f", b"v"]),
        REPLY_ONE
      );
      assert_eq!(
        exec(rt, api, s, RespCommand::Expire, &[src, b"3600"]),
        REPLY_ONE
      );
    }
    Shape::ObjVsString => {
      assert_eq!(
        exec(rt, api, s, RespCommand::Set, &[dst, b"residue"]),
        REPLY_OK
      );
      expire_residual(store, dst);
      assert_eq!(
        exec(rt, api, s, RespCommand::Hset, &[src, b"f", b"v"]),
        REPLY_ONE
      );
    }
    Shape::ObjVsEnvelope => {
      assert_eq!(
        exec(rt, api, s, RespCommand::Hset, &[dst, b"f", b"old"]),
        REPLY_ONE
      );
      expire_residual(store, dst);
      assert_eq!(
        exec(rt, api, s, RespCommand::Hset, &[src, b"f", b"v"]),
        REPLY_ONE
      );
    }
    Shape::StrVsString => {
      assert_eq!(
        exec(rt, api, s, RespCommand::Set, &[dst, b"residue"]),
        REPLY_OK
      );
      expire_residual(store, dst);
      assert_eq!(
        exec(rt, api, s, RespCommand::Set, &[src, b"val2"]),
        REPLY_OK
      );
    }
    Shape::StrVsAlive => {
      assert_eq!(exec(rt, api, s, RespCommand::Set, &[src, b"v1"]), REPLY_OK);
      assert_eq!(exec(rt, api, s, RespCommand::Set, &[dst, b"v2"]), REPLY_OK);
    }
  }
}

/// 终态指纹（存在性 + 类型 + DUMP 载荷逐字节，通型适用 String/集合）。
/// 不含 TTL：双臂执行时刻相差致秒/毫秒取整抖动，TTL 由绝对断言另行收口
fn fingerprint(
  rt: &Runtime,
  api: &GarnetApi,
  s: &mut RespServerSession,
  key: &[u8],
) -> Vec<Vec<u8>> {
  [RespCommand::Exists, RespCommand::Type, RespCommand::Dump]
    .iter()
    .map(|cmd| exec(rt, api, s, *cmd, &[key]))
    .collect()
}

/// 目标键值面探针（集合形态走 HGET、字符串形态走 GET，两臂同径）
fn value_probe(
  rt: &Runtime,
  api: &GarnetApi,
  s: &mut RespServerSession,
  key: &[u8],
  shape: Shape,
) -> Vec<u8> {
  match shape {
    Shape::ObjTtlVsString | Shape::ObjVsString | Shape::ObjVsEnvelope => {
      exec(rt, api, s, RespCommand::Hget, &[key, b"f"])
    }
    _ => exec(rt, api, s, RespCommand::Get, &[key]),
  }
}

/// 快慢臂同构锁（票面方案 2 第 (3) 条）：同一「目标键过期残留」输入两副键，
/// 快臂一副、慢臂直驱一副（与快臂降级快照投递同径），应答逐字节等值、终态
/// 数据面逐字节等值；RENAME（非 NX）臂同锁
#[test]
fn rename_both_arms_byte_parity_on_expired_residual_targets() {
  let (_dir, store) = open_test_store("rename-nx-parity.db").unwrap();
  let rt = Runtime::new().unwrap();
  let api = api_of(&store);
  let mut s = session_with(&api);

  for shape in Shape::ALL {
    let (f_src, f_dst) = (format!("f{}:s", shape.tag()), format!("f{}:d", shape.tag()));
    let (w_src, w_dst) = (format!("w{}:s", shape.tag()), format!("w{}:d", shape.tag()));
    prepare(
      &rt,
      &store,
      &api,
      &mut s,
      &format!("f{}", shape.tag()),
      shape,
    );
    prepare(
      &rt,
      &store,
      &api,
      &mut s,
      &format!("w{}", shape.tag()),
      shape,
    );

    let fast = exec(
      &rt,
      &api,
      &mut s,
      RespCommand::Renamenx,
      &[f_src.as_bytes(), f_dst.as_bytes()],
    );
    let slow = exec_slow(
      &rt,
      &api,
      RespCommand::Renamenx,
      w_src.as_bytes(),
      w_dst.as_bytes(),
    );
    assert_eq!(
      fast,
      slow,
      "形态 {} 快慢臂 RENAMENX 应答分叉（快 {fast:?} 慢 {slow:?}）",
      shape.tag()
    );
    assert_eq!(fast, shape.expect(), "形态 {} 应答绝对值", shape.tag());
    assert_eq!(
      fingerprint(&rt, &api, &mut s, f_dst.as_bytes()),
      fingerprint(&rt, &api, &mut s, w_dst.as_bytes()),
      "形态 {} 目标键终态数据面分叉",
      shape.tag()
    );
    let (f_val, w_val) = (
      value_probe(&rt, &api, &mut s, f_dst.as_bytes(), shape),
      value_probe(&rt, &api, &mut s, w_dst.as_bytes(), shape),
    );
    assert_eq!(
      f_val,
      w_val,
      "形态 {} 目标键值面双臂分叉（快 {f_val:?} 慢 {w_val:?}）",
      shape.tag()
    );
    assert_eq!(
      f_val,
      shape.expect_value(),
      "形态 {} 目标键终态绝对值（源载荷整体搬移）",
      shape.tag()
    );
    let src_gone = if matches!(shape, Shape::StrVsAlive) {
      REPLY_ONE
    } else {
      REPLY_ZERO
    };
    for src in [f_src.as_bytes(), w_src.as_bytes()] {
      assert_eq!(
        exec(&rt, &api, &mut s, RespCommand::Exists, &[src]),
        src_gone,
        "形态 {} 源键收尾绝对锁",
        shape.tag()
      );
    }
  }

  // TTL 面双臂锁（值面与源键收尾已由循环内绝对锁覆盖）：形一源 TTL 随迁、
  // 形二残留 TTL 随预清退，两臂同终态
  assert_ttl_migrated(&rt, &api, &mut s, b"fa:d");
  assert_ttl_migrated(&rt, &api, &mut s, b"wa:d");
  for dst in [b"fb:d".as_slice(), b"wb:d".as_slice()] {
    assert_eq!(
      exec(&rt, &api, &mut s, RespCommand::Pttl, &[dst]),
      REPLY_NO_TTL,
      "形二：源键无 TTL 时残留 TTL 须随预清退，新键不得借尸还魂"
    );
  }
  assert_eq!(
    exec(&rt, &api, &mut s, RespCommand::Type, &[b"wd:s"]),
    REPLY_TYPE_NONE,
    "案二形：慢臂迁移后源键须整体消失"
  );

  // RENAME（非 NX）双臂同锁：残留目标键覆写搬迁，两臂同出 +OK、终态逐字节等值
  prepare(&rt, &store, &api, &mut s, "rnf", Shape::ObjTtlVsString);
  prepare(&rt, &store, &api, &mut s, "rnw", Shape::ObjTtlVsString);
  let fast = exec(
    &rt,
    &api,
    &mut s,
    RespCommand::Rename,
    &[b"rnf:s", b"rnf:d"],
  );
  let slow = exec_slow(&rt, &api, RespCommand::Rename, b"rnw:s", b"rnw:d");
  assert_eq!(fast, slow, "RENAME 非 NX 臂快慢应答分叉");
  assert_eq!(fast, REPLY_OK);
  assert_eq!(
    fingerprint(&rt, &api, &mut s, b"rnf:d"),
    fingerprint(&rt, &api, &mut s, b"rnw:d"),
    "RENAME 非 NX 臂终态分叉"
  );
  assert_eq!(
    exec(&rt, &api, &mut s, RespCommand::Hget, &[b"rnw:d", b"f"]),
    b"$1\r\nv\r\n"
  );
}
