//! TTL 同步读快路径语义回归测试
//!
//! 对标 garnet ReadMethods.cs:Reader 内 LogRecordUtils.cs:CheckExpiry 同栈判定
//!（HasExpiration && Expiration < UtcNow.Ticks，严格小于）：
//! - 未过期 TTL 键同步读零拷贝放行（不再全量降级异步慢路径）；
//! - 已过期键读路径快路径直接 NOTFOUND：GET 回 nil、TTL 回 -2、EXISTS 回 0、
//!   SETNX 视键缺失写入成功，物理清理留写路径惰性清退与后台 GC。

use std::{mem::take, str::from_utf8, sync::Arc};

use tempfile::tempdir;
use wbase::{
  convert::{
    TICKS_PER_SECOND, unix_time_in_milliseconds_from_ticks, unix_time_in_seconds_from_ticks,
  },
  time::now_ticks,
};
use wdev::SegmentedDevice;
use wkv::{BatchStoreSession, StoreResult, WedbStore};
use wnode::{
  resp::{
    key_admin_commands::{ExpireTimeCmd, TtlCmd},
    resp_server_session::RespServerSession,
  },
  storage::session::common::ttl_sync::{
    del_ttl_sync, probe_alive, put_ttl_sync, read_adjudicated_user_sync, ttl_of_sync,
  },
};
use wtest_base::test_store_config;

type TestBatch<'a> = BatchStoreSession<'a, SegmentedDevice>;

fn with_test_env(f: impl FnOnce(&mut RespServerSession, &TestBatch)) {
  let dir = tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("resp.db")).unwrap());
  // GC 关闭保持 TTL 惰性过期语义（过期键物理驻留由快路径逻辑过期覆盖）
  let config = test_store_config();
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  let session = store.new_session().unwrap();
  let batch = session.enter_batch();
  let mut resp = RespServerSession::default();
  f(&mut resp, &batch);
}

/// 写入键并赋予已过期的 TTL（put_ttl_sync 落过去时刻 ticks，惰性未清除）
fn set_expired(batch: &TestBatch, key: &[u8], val: &[u8]) {
  batch.try_upsert_sync(key, val).unwrap().unwrap();
  put_ttl_sync(batch, key, now_ticks() - TICKS_PER_SECOND).unwrap();
  // 前置自检：TTL 记录在场且已过期
  let exp = ttl_of_sync(batch, key)
    .unwrap()
    .value()
    .unwrap()
    .expect("TTL 在场");
  assert!(exp < now_ticks(), "前置自检：TTL 必须已过期");
}

/// 读命令族对已过期键的应答口径（对标 C# Reader 内 CheckExpiry 失败即 NOTFOUND）：
/// GET nil / TTL -2 / EXISTS 0；未过期键放行快路径应答不变
#[test]
fn expired_key_read_commands_notfound() {
  with_test_env(|s, batch| {
    // 已过期键
    set_expired(batch, b"fp:dead", b"Value");
    let mut out = Vec::new();
    s.network_get(&[b"fp:dead"], batch, &mut out).unwrap();
    assert_eq!(out, b"$-1\r\n", "GET 已过期键必须回 nil");

    out.clear();
    s.network_ttl(TtlCmd::Ttl, &[b"fp:dead"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":-2\r\n", "TTL 已过期键必须回 -2（键不存在）");

    out.clear();
    s.network_exists(&[b"fp:dead"], batch, None, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n", "EXISTS 已过期键必须回 0");

    // 对照组：未过期 TTL 键放行快路径，应答口径不变
    batch
      .try_upsert_sync(b"fp:live", b"Value")
      .unwrap()
      .unwrap();
    put_ttl_sync(batch, b"fp:live", now_ticks() + 60 * TICKS_PER_SECOND).unwrap();
    out.clear();
    s.network_get(&[b"fp:live"], batch, &mut out).unwrap();
    assert_eq!(out, b"$5\r\nValue\r\n", "未过期 TTL 键必须快路径直读命中");

    out.clear();
    s.network_ttl(TtlCmd::Ttl, &[b"fp:live"], batch, &mut out)
      .unwrap();
    assert!(out.starts_with(b":"), "未过期键 TTL 必须返回正数而非降级");

    out.clear();
    s.network_exists(&[b"fp:live"], batch, None, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");
  });
}

// libs/server/Resp/BasicCommands.cs:NetworkSETNX——已过期键视同不存在，
// NX 写入成功且旧 TTL 同步清退（写面自愈闭环），新值存活可读
#[test]
fn setnx_on_expired_key_succeeds_and_clears_ttl() {
  with_test_env(|s, batch| {
    set_expired(batch, b"fp:nx", b"old");
    let mut out = Vec::new();
    s.network_setnx(&[b"fp:nx", b"new"], batch, None, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n", "SETNX 对已过期键必须视缺失写入成功");

    assert_eq!(
      ttl_of_sync(batch, b"fp:nx").unwrap().value(),
      Some(None),
      "覆盖写入必须同步清退旧 TTL 记录"
    );
    out.clear();
    s.network_get(&[b"fp:nx"], batch, &mut out).unwrap();
    assert_eq!(out, b"$3\r\nnew\r\n", "写入后的新值必须存活可读");
  });
}

/// 存储层内核三态：read_adjudicated_user_sync 对过期 String 键直接闭环
/// NOTFOUND（不降级、不误探信封域回 WRONGTYPE）；probe_alive 视同不存在
#[test]
fn adjudicated_probe_expired_key_tri_state() {
  with_test_env(|_s, batch| {
    set_expired(batch, b"fp:tri", b"v");

    // 读裁决：NotFound = 快路径 NOTFOUND（修复前为降级异步）
    let read = read_adjudicated_user_sync(batch, b"fp:tri", |v| v.len()).unwrap();
    assert_eq!(
      read,
      StoreResult::NotFound,
      "过期键必须快路径闭环 NOTFOUND 而非降级异步"
    );

    // 存活探针：Ok(Some(false)) = 视同不存在（修复前为 Ok(None) 降级）
    assert_eq!(
      probe_alive(batch, b"fp:tri").unwrap(),
      Some(false),
      "过期键 probe_alive 必须视同不存在"
    );

    // 对照组：未过期键存活且读命中；缺失键存活探针同 Some(false)
    batch.try_upsert_sync(b"fp:alive", b"v").unwrap().unwrap();
    put_ttl_sync(batch, b"fp:alive", now_ticks() + 60 * TICKS_PER_SECOND).unwrap();
    let live = read_adjudicated_user_sync(batch, b"fp:alive", |v| v.len()).unwrap();
    assert_eq!(live, StoreResult::Success(Ok(1)), "未过期键快路径直读命中");
    assert_eq!(probe_alive(batch, b"fp:alive").unwrap(), Some(true));
    assert_eq!(probe_alive(batch, b"fp:missing").unwrap(), Some(false));
  });
}

/// `:N\r\n` 整数帧解析（断言面辅助；非整数帧即 panic 暴露走样应答）
fn parse_int_frame(frame: &[u8]) -> i64 {
  let body = frame
    .strip_prefix(b":")
    .and_then(|f| f.strip_suffix(b"\r\n"))
    .unwrap_or_else(|| panic!("TTL 族应答须为整数帧，实际 {frame:?}"));
  from_utf8(body).unwrap().parse().unwrap()
}

/// TTL/PTTL/EXPIRETIME/PEXPIRETIME 四命令连发取帧（快径直调；`Ok(false)`
/// 即 panic——本组用例全内存闭环，不允许出现降级三态）
fn four_ttl_frames(s: &mut RespServerSession, batch: &TestBatch, key: &[u8]) -> Vec<Vec<u8>> {
  let mut frames = Vec::new();
  let mut grab = |out: &mut Vec<u8>| {
    assert!(!out.is_empty(), "TTL 族快径须直出应答帧（禁降级三态）");
    frames.push(take(out));
  };
  let mut out = Vec::new();
  assert!(
    s.network_ttl(TtlCmd::Ttl, &[key], batch, &mut out).unwrap(),
    "TTL 快径闭环"
  );
  grab(&mut out);
  assert!(
    s.network_ttl(TtlCmd::Pttl, &[key], batch, &mut out)
      .unwrap(),
    "PTTL 快径闭环"
  );
  grab(&mut out);
  assert!(
    s.network_expiretime(ExpireTimeCmd::Expiretime, &[key], batch, &mut out)
      .unwrap(),
    "EXPIRETIME 快径闭环"
  );
  grab(&mut out);
  assert!(
    s.network_expiretime(ExpireTimeCmd::Pexpiretime, &[key], batch, &mut out)
      .unwrap(),
    "PEXPIRETIME 快径闭环"
  );
  grab(&mut out);
  frames
}

/// TTL 族四命令全臂矩阵（票 zcode-r127c-genexpire1 测试验证点·表驱动）：
/// 无 TTL 键 -1、缺席 -2、已过期 -2、孤儿 TTL 记录（TTL 未来在场而数据三域
/// 皆缺）-2——折叠终判权归数据走查，TTL 记录读到何值不改判（根除「键全亡
/// 报 -1」的判序收口）；未到期键 EXPIRETIME/PEXPIRETIME 与 ConvertUtils
/// 绝对换算逐字节钉死（该臂不依 now 时钟，确定值）
#[test]
fn ttl_family_state_matrix_frames() {
  with_test_env(|s, batch| {
    batch.try_upsert_sync(b"mx:nottl", b"v").unwrap().unwrap();
    set_expired(batch, b"mx:expired", b"v");
    let exp_ticks = now_ticks() + 40_000 * TICKS_PER_SECOND;
    batch.try_upsert_sync(b"mx:live", b"v").unwrap().unwrap();
    put_ttl_sync(batch, b"mx:live", exp_ticks).unwrap();
    // 孤儿 TTL 记录：未来 ticks 在场、数据三域皆缺（竞态窗「TTL 先删臂」的
    // 镜像残态——数据终判臂必须无视 TTL 值恒出 -2）
    put_ttl_sync(batch, b"mx:orphan", exp_ticks).unwrap();

    for key in [
      b"mx:missing".as_slice(),
      b"mx:expired".as_slice(),
      b"mx:orphan".as_slice(),
    ] {
      for frame in four_ttl_frames(s, batch, key) {
        assert_eq!(frame, b":-2\r\n", "{key:?} 键亡/孤儿臂四命令恒 -2");
      }
    }
    for frame in four_ttl_frames(s, batch, b"mx:nottl") {
      assert_eq!(frame, b":-1\r\n", "{frame:?} 无 TTL 在场键四命令恒 -1");
    }

    let [ttl, pttl, expiretime, pexpiretime] =
      four_ttl_frames(s, batch, b"mx:live").try_into().unwrap();
    assert!(
      (39_000..=40_000).contains(&parse_int_frame(&ttl)),
      "TTL 未到期须正数秒域，实际 {ttl:?}"
    );
    assert!(
      (39_000_000..=40_000_000).contains(&parse_int_frame(&pttl)),
      "PTTL 未到期须正数毫秒域，实际 {pttl:?}"
    );
    // 绝对域无 now 时钟参与，逐字节钉值（对标 ConvertUtils.cs SecondsFrom /
    // MillisecondsFrom 的 UnixTimeIn*FromTicks 换算臂）
    assert_eq!(
      expiretime,
      format!(":{}\r\n", unix_time_in_seconds_from_ticks(exp_ticks)).into_bytes()
    );
    assert_eq!(
      pexpiretime,
      format!(":{}\r\n", unix_time_in_milliseconds_from_ticks(exp_ticks)).into_bytes()
    );
  });
}

/// 确定性造窗（串行编排真实交错形态；镜像写侧既定序——DEL 级联与 EXPIRE
/// 过去戳臂皆先 del_ttl 后删数据，keys.rs 过去戳臂同序）：远 TTL 键三段
/// 窗——段一 TTL 未剔、数据在场，四命令皆正；段二 del_ttl_sync 已落、数据
/// 在场（旧形竞态窗内第二读落点），四命令皆 -1（在场真实无 TTL，唯一合法
/// -1 出口，绝无 -2 与正数撕裂）；段三数据删除、键全亡，反复观测恒 -2——
/// 钉死「-1 仅数据在场真实无 TTL 可达」「键全亡后永不出 -1」。
/// PERSIST→TTL 两拍只观测 正数/-1，键全程未亡绝无 -2
#[test]
fn staged_delete_window_and_persist_beats() {
  with_test_env(|s, batch| {
    batch.try_upsert_sync(b"win:k", b"v").unwrap().unwrap();
    put_ttl_sync(batch, b"win:k", now_ticks() + 40_000 * TICKS_PER_SECOND).unwrap();

    // 段一：TTL 在场未到期
    for frame in four_ttl_frames(s, batch, b"win:k") {
      assert!(
        parse_int_frame(&frame) > 0,
        "段一四命令皆正，实际 {frame:?}"
      );
    }

    // 段二：真实删除级联第一步（先剔 TTL 旁路记录），数据仍在场
    assert!(
      del_ttl_sync(batch, b"win:k").unwrap(),
      "del_ttl 同步段须闭环"
    );
    for frame in four_ttl_frames(s, batch, b"win:k") {
      assert_eq!(frame, b":-1\r\n", "窗内数据在场只可 -1，实际 {frame:?}");
    }

    // 段三：级联第二步删数据，键全亡；反复观测钉死恒 -2（第三态根除）
    batch.try_delete_sync(b"win:k").unwrap().unwrap();
    for _ in 0..5 {
      for frame in four_ttl_frames(s, batch, b"win:k") {
        assert_eq!(frame, b":-2\r\n", "键全亡后禁出 -1/正数，实际 {frame:?}");
      }
    }

    // PERSIST→TTL 两拍：应答只落 {正数, -1} 两翼（键永不亡，绝不可能 -2）
    batch.try_upsert_sync(b"win:p", b"v").unwrap().unwrap();
    put_ttl_sync(batch, b"win:p", now_ticks() + 40_000 * TICKS_PER_SECOND).unwrap();
    let before = four_ttl_frames(s, batch, b"win:p");
    assert!(
      before.iter().all(|f| parse_int_frame(f) > 0),
      "PERSIST 前两拍皆正，实际 {before:?}"
    );
    let mut out = Vec::new();
    assert!(s.network_persist(&[b"win:p"], batch, &mut out).unwrap());
    assert_eq!(out, b":1\r\n");
    let after = four_ttl_frames(s, batch, b"win:p");
    for frame in &after {
      assert_eq!(frame, b":-1\r\n", "PERSIST 回执 :1 后恒 -1，实际 {frame:?}");
    }
    assert_eq!(
      probe_alive(batch, b"win:p").unwrap(),
      Some(true),
      "PERSIST 不杀键"
    );
  });
}
