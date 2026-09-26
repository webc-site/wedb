//! GEOADD 选项词形双臂 parity 锁（票 zcode-r147c-geozadd 案一·执行方案第 3 点）
//!
//! 锁形制：
//! 1. 慢臂带选项词四形——升阶（分层）键经 auto_exec 慢臂执行
//!    GEOADD NX / XX / CH / NX CH 多三元组，应答字节与内存态同步臂同输入
//!    序列逐字节全等，且数据侧 ZCARD/ZSCORE/GEOPOS 一致（「慢臂带选项词」
//!    现树零覆盖的漂移屏障缺口收口；选项扫描单源 scan_geo_add_options 与
//!    回写门单源 should_write_back 并轨后行为不变的回归回摆锁）。
//! 2. 回写门并轨前后行为不变形锁——缺键 + XX 零变更不建键（首式
//!    `!existed && 空` 支）、既存键 NX 全挡应答 :0 数据不动、无选项覆写
//!    零应答照常写回（existed 支门开）、慢臂零变更臂固化不动。
//!    （`-` 短路支对 GEOADD 恒不成立：对象层唯一出口 write_int64 恒出
//!    :N 帧，见 wcol/src/zset/geo_impl.rs:geo_add。）

use std::sync::Arc;

use aok::Void;
use compio::runtime::Runtime;
use tempfile::TempDir;
use wcol::{geo::geo_hash::GeoHash, types::member_ttl::encode_member};
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::resp::{
  garnet_api::{GarnetApi, StoreGarnetApi},
  resp_server_session::{RespServerSession, RespServerSessionOptions},
};
use wnode_test::auto_exec;
use wresp::command::RespCommand;
use wval::GarnetObjectType;

type TestStore = WedbStore<SegmentedDevice>;

fn open_env(tag: &str) -> (Runtime, GarnetApi, Arc<TestStore>, TempDir) {
  let dir = tempfile::tempdir().unwrap();
  let config = StoreConfig::new(2048, 1024 * 1024, 16, 0.5).unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag)).unwrap());
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  (
    Runtime::new().unwrap(),
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
    store,
    dir,
  )
}

fn session_with(api: &GarnetApi) -> RespServerSession {
  let mut s = RespServerSession::new(1, RespServerSessionOptions::default());
  s.set_garnet_api(api.clone());
  s
}

/// 手工升阶（与生产 export_entries 同形 entries：成员 + encode_member 记录）
async fn promote_zset(store: &Arc<TestStore>, key: &[u8], members: &[(&[u8], f64)]) -> Void {
  let ents: Vec<(Vec<u8>, Vec<u8>)> = members
    .iter()
    .map(|(m, s)| (m.to_vec(), encode_member(&s.to_be_bytes(), None)))
    .collect();
  let sess = store.new_session().unwrap();
  sess
    .promote_collection_to_bftree(key, GarnetObjectType::SortedSet, ents, i64::MAX, false)
    .await?;
  Ok(())
}

/// palermo 基准分值（对象层以 GeoHash 整数为分值，慢臂起步态与同步臂
/// GEOADD 起步同值：geo_to_long_value(纬度, 经度)）
fn palermo_score() -> f64 {
  GeoHash::geo_to_long_value(38.115556, 13.361389) as f64
}

/// 坐标对（lon, lat）字节词元
const C0: (&[u8], &[u8]) = (b"13.361389", b"38.115556"); // palermo 基准
const C1: (&[u8], &[u8]) = (b"15.087269", b"37.502669"); // catania
const C2: (&[u8], &[u8]) = (b"12.0", b"37.0"); // palermo 挡位尝试（NX）
const C3: (&[u8], &[u8]) = (b"14.25", b"37.0"); // palermo 第一次覆写（XX）
const C4: (&[u8], &[u8]) = (b"12.5", b"36.5"); // nowhere 尝试（XX 挡新增）
const C5: (&[u8], &[u8]) = (b"13.5", b"38.5"); // palermo 第二次覆写（CH 臂）
const C6: (&[u8], &[u8]) = (b"14.75", b"38.75"); // bar 新增（CH 臂）
const C7: (&[u8], &[u8]) = (b"12.5", b"37.5"); // palermo 挡位尝试（NX CH）
const C8: (&[u8], &[u8]) = (b"15.5", b"38.25"); // baz 新增（NX CH 臂）

/// 带选项四形序列（每形多三元组）：返回逐命令应答
///
/// 语义推导锚（wcol/src/zset/geo_impl.rs:geo_add，CH 缺省回 elements_added、
/// 带 CH 回 elements_changed；NX 只挡更新臂、XX 只挡新增臂）：
/// - `NX C1 catania C2 palermo`：catania 新增、palermo 既存被 NX 挡 → :1
/// - `XX C3 palermo C4 nowhere`：palermo 更新（无 CH 不计）、nowhere 挡新增 → :0
/// - `CH C5 palermo C6 bar`：palermo 更新 + bar 新增 → :2
/// - `NX CH C7 palermo C8 baz`：palermo 挡、baz 新增 → :1
fn geo_add_option_forms(api: &GarnetApi, rt: &Runtime, s: &mut RespServerSession) -> [Vec<u8>; 4] {
  [
    auto_exec(
      api,
      rt,
      s,
      RespCommand::Geoadd,
      &[b"gk", b"NX", C1.0, C1.1, b"catania", C2.0, C2.1, b"palermo"],
    ),
    auto_exec(
      api,
      rt,
      s,
      RespCommand::Geoadd,
      &[b"gk", b"XX", C3.0, C3.1, b"palermo", C4.0, C4.1, b"nowhere"],
    ),
    auto_exec(
      api,
      rt,
      s,
      RespCommand::Geoadd,
      &[b"gk", b"CH", C5.0, C5.1, b"palermo", C6.0, C6.1, b"bar"],
    ),
    auto_exec(
      api,
      rt,
      s,
      RespCommand::Geoadd,
      &[
        b"gk", b"NX", b"CH", C7.0, C7.1, b"palermo", C8.0, C8.1, b"baz",
      ],
    ),
  ]
}

/// 慢臂带选项词四形：分层键（auto_exec 必经慢臂）与内存态同步臂逐字节
/// parity + 硬编码应答 + 数据侧一致
#[test]
fn slow_arm_tiered_geoadd_option_forms_parity() -> Void {
  // ---- 内存态基准（同步臂）----
  let (rt_m, api_m, _store_m, _dir_m) = open_env("geoadd-slow-parity-mem.db");
  let mut sm = session_with(&api_m);
  assert_eq!(
    auto_exec(
      &api_m,
      &rt_m,
      &mut sm,
      RespCommand::Geoadd,
      &[b"gk", C0.0, C0.1, b"palermo"]
    ),
    b":1\r\n"
  );
  // 控制键：palermo 终态坐标 C5 的等价锚（GEOPOS 由 geohash 反解，输入词元
  // 非输出词元，故以同坐标新成员为基准做逐字节对照）
  assert_eq!(
    auto_exec(
      &api_m,
      &rt_m,
      &mut sm,
      RespCommand::Geoadd,
      &[b"ctrl", C5.0, C5.1, b"x"]
    ),
    b":1\r\n"
  );
  let mem_outs = geo_add_option_forms(&api_m, &rt_m, &mut sm);
  // 语义推导锚：四形应答
  assert_eq!(mem_outs[0], b":1\r\n", "NX 臂：仅新增 catania");
  assert_eq!(mem_outs[1], b":0\r\n", "XX 臂：无 CH 只计新增");
  assert_eq!(mem_outs[2], b":2\r\n", "CH 臂：更新 + 新增共 2");
  assert_eq!(mem_outs[3], b":1\r\n", "NX CH 臂：palermo 挡、baz 新增");

  // ---- 分层态（慢臂）：同成员集起步 ----
  let (rt_t, api_t, store_t, _dir_t) = open_env("geoadd-slow-parity-tiered.db");
  rt_t.block_on(promote_zset(
    &store_t,
    b"gk",
    &[(b"palermo" as &[u8], palermo_score())],
  ))?;
  let mut st = session_with(&api_t);
  let tiered_outs = geo_add_option_forms(&api_t, &rt_t, &mut st);
  for (i, (t, m)) in tiered_outs.iter().zip(mem_outs.iter()).enumerate() {
    assert_eq!(t, m, "四形第 {} 应答慢臂与同步臂分叉", i + 1);
  }

  // ---- 数据侧一致：ZCARD / ZSCORE / GEOPOS 逐字节 ----
  assert_eq!(
    auto_exec(&api_t, &rt_t, &mut st, RespCommand::Zcard, &[b"gk"]),
    auto_exec(&api_m, &rt_m, &mut sm, RespCommand::Zcard, &[b"gk"])
  );
  assert_eq!(
    auto_exec(&api_t, &rt_t, &mut st, RespCommand::Zcard, &[b"gk"]),
    b":4\r\n"
  );
  for member in [&b"catania"[..], &b"bar"[..], &b"baz"[..]] {
    assert_eq!(
      auto_exec(
        &api_t,
        &rt_t,
        &mut st,
        RespCommand::Zscore,
        &[b"gk", member]
      ),
      auto_exec(
        &api_m,
        &rt_m,
        &mut sm,
        RespCommand::Zscore,
        &[b"gk", member]
      ),
      "成员 {member:?} 分值双臂分叉"
    );
  }
  assert_eq!(
    auto_exec(
      &api_t,
      &rt_t,
      &mut st,
      RespCommand::Zscore,
      &[b"gk", b"nowhere"]
    ),
    b"$-1\r\n",
    "XX 臂挡新增却落了 nowhere（回写门误放行幻成员）"
  );
  // palermo 经两度覆写后终态 = C5（慢臂更新确已写回），与内存控制键逐字节
  let pos_t = auto_exec(
    &api_t,
    &rt_t,
    &mut st,
    RespCommand::Geopos,
    &[b"gk", b"palermo"],
  );
  let pos_m = auto_exec(
    &api_m,
    &rt_m,
    &mut sm,
    RespCommand::Geopos,
    &[b"gk", b"palermo"],
  );
  let pos_c = auto_exec(
    &api_m,
    &rt_m,
    &mut sm,
    RespCommand::Geopos,
    &[b"ctrl", b"x"],
  );
  assert_eq!(pos_t, pos_m, "palermo 坐标双臂分叉");
  assert_eq!(
    pos_t, pos_c,
    "palermo 未更新至 C5（XX/CH 覆写臂慢路径失效）"
  );
  Ok(())
}

/// 回写门并轨行为不变形锁：缺键 XX 不建键、既存 NX 全挡不动数据、
/// 无选项零应答覆写照常写回、慢臂零变更臂固化不动
#[test]
fn write_back_gate_invariant_locks() -> Void {
  // ---- 形一：缺键 + XX → :0 且键不创建（should_write_back 首式 `!existed && 空` 支）----
  let (rt, api, _store, _dir) = open_env("geoadd-gate.db");
  let mut s = session_with(&api);
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Geoadd,
      &[b"miss", b"XX", C0.0, C0.1, b"palermo"]
    ),
    b":0\r\n"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Exists, &[b"miss"]),
    b":0\r\n",
    "缺键 XX 零变更后回写门放行建了幻键"
  );

  // ---- 形二：既存键 NX 全挡 → :0、分值不动；无选项覆写 → :0 但变更必固化 ----
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Geoadd,
      &[b"gk2", C0.0, C0.1, b"m0"]
    ),
    b":1\r\n"
  );
  let score_before = auto_exec(&api, &rt, &mut s, RespCommand::Zscore, &[b"gk2", b"m0"]);
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Geoadd,
      &[b"gk2", b"NX", C1.0, C1.1, b"m0"]
    ),
    b":0\r\n"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zscore, &[b"gk2", b"m0"]),
    score_before,
    "NX 全挡臂数据被误改"
  );
  // 无选项覆写既有成员：added=0 应答 :0，但 existed 支门开、变更必固化
  // （门若误拦则分值不动）
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Geoadd,
      &[b"gk2", C5.0, C5.1, b"m0"]
    ),
    b":0\r\n"
  );
  let score_after = auto_exec(&api, &rt, &mut s, RespCommand::Zscore, &[b"gk2", b"m0"]);
  assert_ne!(score_after, score_before, "零应答覆写臂写回被门误拦");
  // 终态 = C5：与同坐标控制键成员的分值/坐标逐字节
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Geoadd,
      &[b"ctrl2", C5.0, C5.1, b"x"]
    ),
    b":1\r\n"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Geopos, &[b"gk2", b"m0"]),
    auto_exec(&api, &rt, &mut s, RespCommand::Geopos, &[b"ctrl2", b"x"]),
    "覆写后坐标与同坐标控制成员分叉"
  );
  assert_eq!(
    score_after,
    auto_exec(&api, &rt, &mut s, RespCommand::Zscore, &[b"ctrl2", b"x"]),
    "覆写后分值与同坐标控制成员分叉"
  );

  // ---- 形三：慢臂 existed 支 NX 全挡零变更 → :0、值不动、键存续 ----
  let (rt_t, api_t, store_t, _dir_t) = open_env("geoadd-gate-tiered.db");
  rt_t.block_on(promote_zset(
    &store_t,
    b"gk3",
    &[(b"palermo" as &[u8], palermo_score())],
  ))?;
  let mut st = session_with(&api_t);
  let before = auto_exec(
    &api_t,
    &rt_t,
    &mut st,
    RespCommand::Zscore,
    &[b"gk3", b"palermo"],
  );
  assert_eq!(
    auto_exec(
      &api_t,
      &rt_t,
      &mut st,
      RespCommand::Geoadd,
      &[b"gk3", b"NX", C1.0, C1.1, b"palermo"]
    ),
    b":0\r\n"
  );
  assert_eq!(
    auto_exec(
      &api_t,
      &rt_t,
      &mut st,
      RespCommand::Zscore,
      &[b"gk3", b"palermo"]
    ),
    before,
    "慢臂 NX 全挡臂数据被误改"
  );
  assert_eq!(
    auto_exec(&api_t, &rt_t, &mut st, RespCommand::Exists, &[b"gk3"]),
    b":1\r\n"
  );
  Ok(())
}
