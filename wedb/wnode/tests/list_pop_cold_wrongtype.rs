//! 阻塞弹出族慢路径 WRONGTYPE 终止语义回归测试
//!
//! 修复前：list_commands/slow.rs 的 Blpop/Brpop 冷态臂与
//! pop_first_nonempty_cold 用 let-else 把 WRONGTYPE（错误帧已写出）与
//! MISSING 同等折叠进 continue，导致「-WRONGTYPE」后拼出成功帧或 null 帧、
//! 且后续合法键元素被静默弹出消费。修复后与同步对正基线
//! （blocking.rs list_blocking_pop / pop_first_nonempty 三态匹配）及 C#
//! 真源（ListCommands.cs ListBlockingPop 293-298 行 IsTypeMismatch 立即
//! 写错误行 return true；ListPopMultiple WRONGTYPE 单帧终止；
//! CollectionItemBroker TryGetResult failOnSrcTypeMismatch=true）三径同构：
//! 遇第一个类型不符键立即终结整条命令，后续键不被触碰、无空值帧追加。
//!
//! 对标 garnet/test/standalone/Garnet.test.collections/
//! RespBlockingCollectionTests.cs 的 WRONGTYPE 用例族的冷态（磁盘降级）形态。

use std::sync::Arc;

use compio::runtime::Runtime;
use tempfile::tempdir;
use wconf::DEFAULT_RESP_VERSION;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::resp::{
  garnet_api::{GarnetApi, StoreGarnetApi},
  resp_server_session::{RespServerSession, RespServerSessionOptions},
};
use wnode_test::err_frame;
use wresp::{cmd_strings::RESP_ERR_WRONG_TYPE, command::RespCommand};

/// 小容量单文件存储执行域（hll_cold_degrade.rs 同款配置，附 store 句柄供
/// flush_and_evict_all 冷化构造磁盘候选降级场景）
fn open_env(
  tag: &str,
) -> (
  Runtime,
  GarnetApi,
  Arc<WedbStore<SegmentedDevice>>,
  tempfile::TempDir,
) {
  let dir = tempdir().unwrap();
  let config = StoreConfig::new(1024, 64 * 1024, 16, 0.5).unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag)).unwrap());
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  let api: GarnetApi = Arc::new(StoreGarnetApi::new(store.new_session().unwrap()));
  (Runtime::new().unwrap(), api, store, dir)
}

/// 挂接分派器的独立会话（经纪未注入域，同步段立即可取路径口径）
fn session_with(api: &GarnetApi) -> RespServerSession {
  let mut s = RespServerSession::new(1, RespServerSessionOptions::default());
  s.set_garnet_api(api.clone());
  s
}

/// 慢路径异步臂直答（冷键降级后的 exec_slow 完整应答字节）
fn exec_slow(rt: &Runtime, api: &GarnetApi, cmd: RespCommand, args: &[&[u8]]) -> Vec<u8> {
  rt.block_on(Arc::clone(api).exec_slow(
    cmd,
    args.iter().map(|a| a.to_vec()).collect(),
    DEFAULT_RESP_VERSION,
  ))
}

/// 建键定式：kstr 为 String、klst 为非空 List [a, b]（同步段写出即断言）
fn seed(api: &GarnetApi) {
  let mut s = session_with(api);
  api.exec(&mut s, RespCommand::Set, &[b"kstr", b"v"]);
  assert_eq!(s.output, b"+OK\r\n");
  s.output.clear();
  api.exec(&mut s, RespCommand::Rpush, &[b"klst", b"a", b"b"]);
  assert_eq!(s.output, b":2\r\n");
}

/// 冷化：全量刷盘驱逐，两键均仅驻留磁盘（磁盘候选）
fn evict(rt: &Runtime, store: &Arc<WedbStore<SegmentedDevice>>) {
  rt.block_on(store.flush_and_evict_all()).unwrap();
}

/// 冷键清单逐元素核对（LRANGE 0 -1 直答，弹出侧未被消费的证明点）
fn list_snapshot(rt: &Runtime, api: &GarnetApi) -> Vec<u8> {
  exec_slow(rt, api, RespCommand::Lrange, &[b"klst", b"0", b"-1"])
}

const TWO_ELEMS: &[u8] = b"*2\r\n$1\r\na\r\n$1\r\nb\r\n";

/// BLPOP kstr klst 0 冷态慢路径：仅一条 -WRONGTYPE 帧，无拼帧，klst 不被弹出
#[test]
fn blpop_cold_wrongtype_terminates_command() {
  let (rt, api, store, _dir) = open_env("blpop-cold-wt.db");
  seed(&api);
  evict(&rt, &store);

  // 快路径预探：任一键磁盘候选 → 整体降级挂起（无应答写出）
  let mut s = session_with(&api);
  api.exec(&mut s, RespCommand::Blpop, &[b"kstr", b"klst", b"0"]);
  assert!(
    s.output.is_empty(),
    "冷键 BLPOP 必须整体降级慢路径：{:?}",
    String::from_utf8_lossy(&s.output)
  );

  // 慢路径直答：仅 WRONGTYPE 单帧（修复后：None 即 return，不续扫 klst）
  let out = exec_slow(&rt, &api, RespCommand::Blpop, &[b"kstr", b"klst", b"0"]);
  assert_eq!(out, err_frame(RESP_ERR_WRONG_TYPE));

  // klst 元素未被静默消费
  assert_eq!(list_snapshot(&rt, &api), TWO_ELEMS);
}

/// BRPOP klst kstr 0 冷态慢路径：首个键命中有效列表先弹出属正当语义
/// （C# 按 key 序扫描），类型不符键居后时不触达；反向键序验证 WRONGTYPE
/// 居首即终止
#[test]
fn brpop_cold_wrongtype_order_sensitive() {
  let (rt, api, store, _dir) = open_env("brpop-cold-wt.db");
  seed(&api);
  evict(&rt, &store);

  // WRONGTYPE 键居首：单错误帧终止，klst 不被触碰
  let out = exec_slow(&rt, &api, RespCommand::Brpop, &[b"kstr", b"klst", b"0"]);
  assert_eq!(out, err_frame(RESP_ERR_WRONG_TYPE));
  assert_eq!(list_snapshot(&rt, &api), TWO_ELEMS);

  // klst 居首：右端弹出 b，回 [klst, b]（键序语义与同步/C# 一致）
  let out = exec_slow(&rt, &api, RespCommand::Brpop, &[b"klst", b"kstr", b"0"]);
  assert_eq!(out, b"*2\r\n$4\r\nklst\r\n$1\r\nb\r\n");
  assert_eq!(list_snapshot(&rt, &api), b"*1\r\n$1\r\na\r\n");
}

/// LMPOP/BLMPOP 冷态慢路径 WRONGTYPE：仅单错误帧，无 null array / null 帧拼接
/// （修复前 pop_first_nonempty_cold 折叠 continue，!handled 分支再补 null）
#[test]
fn lmpop_blmpop_cold_wrongtype_single_frame() {
  let (rt, api, store, _dir) = open_env("lmpop-cold-wt.db");
  seed(&api);
  evict(&rt, &store);

  // LMPOP 2 kstr klst LEFT
  let out = exec_slow(
    &rt,
    &api,
    RespCommand::Lmpop,
    &[b"2", b"kstr", b"klst", b"LEFT"],
  );
  assert_eq!(out, err_frame(RESP_ERR_WRONG_TYPE));

  // BLMPOP 0 2 kstr klst LEFT（同为单帧，无 `*-1` 尾巴）
  let out = exec_slow(
    &rt,
    &api,
    RespCommand::Blmpop,
    &[b"0", b"2", b"kstr", b"klst", b"LEFT"],
  );
  assert_eq!(out, err_frame(RESP_ERR_WRONG_TYPE));

  // klst 元素完好
  assert_eq!(list_snapshot(&rt, &api), TWO_ELEMS);
}

/// 快慢同字节：热键同步臂与冷键降级慢路径在 WRONGTYPE 边界下输出严格全等
/// （BLPOP 与 LMPOP 双族），并顺带核对慢路径文档注释语义（全 MISSING →
/// 调用方补 null 形态应答的裸终止路径仍成立）
#[test]
fn wrongtype_fast_slow_byte_equal() {
  let (rt, api, store, _dir) = open_env("pop-wt-fastslow.db");
  seed(&api);

  // ---- 热态同步臂（立即可取路径基线）----
  let mut s = session_with(&api);
  api.exec(&mut s, RespCommand::Blpop, &[b"kstr", b"klst", b"0"]);
  let blpop_warm = s.output.clone();
  s.output.clear();
  api.exec(
    &mut s,
    RespCommand::Lmpop,
    &[b"2", b"kstr", b"klst", b"LEFT"],
  );
  let lmpop_warm = s.output.clone();
  assert_eq!(blpop_warm, err_frame(RESP_ERR_WRONG_TYPE));
  assert_eq!(lmpop_warm, err_frame(RESP_ERR_WRONG_TYPE));

  // ---- 冷态慢路径（磁盘候选降级）----
  evict(&rt, &store);
  let blpop_cold = exec_slow(&rt, &api, RespCommand::Blpop, &[b"kstr", b"klst", b"0"]);
  let lmpop_cold = exec_slow(
    &rt,
    &api,
    RespCommand::Lmpop,
    &[b"2", b"kstr", b"klst", b"LEFT"],
  );
  assert_eq!(blpop_cold, blpop_warm, "BLPOP 快慢字节流必须全等");
  assert_eq!(lmpop_cold, lmpop_warm, "LMPOP 快慢字节流必须全等");
}

/// 冷态全 MISSING 键耗尽：慢路径仍由调用方补写 null 形态应答
/// （回归保护：修复不得破坏 MISSING/空列表的 continue→null 收尾路径）
#[test]
fn cold_all_missing_still_returns_null_array() {
  let (rt, api, _store, _dir) = open_env("pop-cold-missing.db");
  // 纯内存独立会话域（键不存在不产生磁盘候选，直接走同步立即可取路径口径
  // 的空值收尾；慢路径同款收尾经 exec_slow 直答核对）
  let out = exec_slow(
    &rt,
    &api,
    RespCommand::Blpop,
    &[b"nokey-a", b"nokey-b", b"0"],
  );
  assert_eq!(out, b"*-1\r\n");
  let out = exec_slow(&rt, &api, RespCommand::Lmpop, &[b"1", b"nokey-c", b"LEFT"]);
  assert_eq!(out, b"*-1\r\n");
  let out = exec_slow(
    &rt,
    &api,
    RespCommand::Blmpop,
    &[b"0", b"1", b"nokey-d", b"LEFT"],
  );
  assert_eq!(out, b"$-1\r\n");
}
