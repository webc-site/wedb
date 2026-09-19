//! RangeIndex 谓词互斥门禁端到端测试（对标 C# 主存记录类型判别的三门面）
//!
//! 判据单一来源：记录自己的物理域事实（`KeyTag::Meta` 存活元记录
//! `collection_type == RangeIndex`），不吃命令位图（位图整链删除的论证见
//! task/ing/ri-predicate-gate.md §三）。按方向拆成独立用例，任一方向的
//! 回归不遮蔽其余方向：
//!
//! 1. 白名单放行：RI 键上 TYPE / EXISTS / DEL 正常，读数据命令回 WRONGTYPE
//! 2. RENAME 专项：RI 键不因门禁被拒（迁移本体未落地，见该文档 §五.1）
//! 3. 拦截方向一（普通写打 RI 键）：字符串写入口一律 WRONGTYPE
//!    （`wnode/src/resp/basic_commands/set.rs:ri_write_gate`）
//! 4. 拦截方向二（RI 命令打非 RI 键）：字符串键 / 集合信封键一律 WRONGTYPE
//!    （建索引入口三态与存根装载入口，`wkv/src/range_index` 的 ops 与 stub）
//! 5. 缺失键：三域皆缺才是索引缺失，答 not found / no such range index
//!
//! 夹具字段与取值用 `field1` / `value1`（12 字节）并显式 `MINRECORD 8`，
//! 详见 [`seed_ri`]：入参长度校验先于门禁，短字段会遮蔽门语义。

use std::sync::Arc;

use compio::runtime::Runtime;
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
};
use wnode_test::err_frame;
use wtest_base::{resp_frame_str, test_store_config};

/// 装配带 RangeIndex 目录的真存储会话消费者（每测试独立临时目录，GC 关闭）
fn consumer() -> (tempfile::TempDir, RespSessionConsumer) {
  let dir = tempfile::tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("ri_gate.db")).unwrap());
  let mut config = test_store_config();
  config.range_index_dir = Some(dir.path().join("ri"));
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  let session = store.new_session().unwrap();
  let consumer = RespSessionConsumer::new(
    1,
    RespServerSessionOptions::default(),
    Arc::new(StoreGarnetApi::new(session)),
  );
  (dir, consumer)
}

/// 单命令往返：同步段消费 + 慢路径闭环（RI 族与降级命令在异步段应答）
fn cmd(rt: &Runtime, c: &mut RespSessionConsumer, args: &[&str]) -> Vec<u8> {
  let mut scratch = c.take_recv_scratch();
  scratch.extend_from_slice(&resp_frame_str(args));
  c.return_recv_scratch(scratch);
  let mut out = Vec::new();
  let remaining = c.try_consume_messages_into(&mut out);
  assert_eq!(remaining, Some(0), "帧应被完整消费: {args:?}");
  if let Some(slow) = c.take_slow_wait() {
    rt.block_on(async {
      out.extend_from_slice(&slow.resolve().await);
    });
  }
  out
}

/// 应答是否为 WRONGTYPE 错误
fn is_wrongtype(out: &[u8]) -> bool {
  out.starts_with(b"-WRONGTYPE")
}

/// 应答文本（断言失败信息用）
fn text(out: &[u8]) -> String {
  String::from_utf8_lossy(out).into_owned()
}

/// 建一个存活 RI 键并写入一条字段（各方向共用夹具）
///
/// 显式 `MINRECORD 8`：测试小预算配置的默认 MINRECORD 为 64，`field1` +
/// `value1` 共 12 字节会先被入参长度校验拒掉，遮蔽门语义
fn seed_ri(rt: &Runtime, c: &mut RespSessionConsumer, key: &str) {
  assert_eq!(
    cmd(rt, c, &["RI.CREATE", key, "MEMORY", "MINRECORD", "8"]),
    b"+OK\r\n",
    "RI.CREATE 应成功"
  );
  assert_eq!(
    cmd(rt, c, &["RI.SET", key, "field1", "value1"]),
    b"+OK\r\n",
    "RI.SET 应成功"
  );
}

/// 方向 1：白名单放行（TYPE / EXISTS / DEL 正常，读数据命令 WRONGTYPE）
#[test]
fn ri_key_read_gate_folds_wrongtype_and_whitelist_passes() {
  let rt = Runtime::new().unwrap();
  let (_dir, mut c) = consumer();

  seed_ri(&rt, &mut c, "idx");

  // TYPE / EXISTS 为白名单方向：不回 WRONGTYPE
  assert_eq!(cmd(&rt, &mut c, &["TYPE", "idx"]), b"+rangeindex\r\n");
  assert_eq!(cmd(&rt, &mut c, &["EXISTS", "idx"]), b":1\r\n");

  // 读面三域折叠：字符串与哈希读命令打 RI 键一律 WRONGTYPE
  for args in [
    vec!["GET", "idx"],
    vec!["STRLEN", "idx"],
    vec!["HGET", "idx", "field1"],
    vec!["GETRANGE", "idx", "0", "-1"],
  ] {
    let out = cmd(&rt, &mut c, &args);
    assert!(
      is_wrongtype(&out),
      "{args:?} 应回 WRONGTYPE: {}",
      text(&out)
    );
  }

  // DEL 为白名单方向：删后读回 nil（视同不存在，而非残留 WRONGTYPE）
  assert_eq!(cmd(&rt, &mut c, &["DEL", "idx"]), b":1\r\n");
  assert_eq!(cmd(&rt, &mut c, &["GET", "idx"]), b"$-1\r\n");
  assert_eq!(cmd(&rt, &mut c, &["EXISTS", "idx"]), b":0\r\n");
  assert_eq!(cmd(&rt, &mut c, &["TYPE", "idx"]), b"+none\r\n");
}

/// 方向 1 附：RENAME /// 方向 2：RENAME 专项——RI 键是白名单方向，不被 WRONGTYPE 拦截
///
/// 迁移本体已落地（慢路径 `rename_range_index` 支持 RangeIndex 树迁移与改名）：
/// 原键 idx 迁移到新键 idx2，原键清退不可达，新键完整继承 rangeindex 记录与树内字段。
#[test]
fn ri_key_rename_not_wrongtyped() {
  let rt = Runtime::new().unwrap();
  let (_dir, mut c) = consumer();

  seed_ri(&rt, &mut c, "idx");

  let out = cmd(&rt, &mut c, &["RENAME", "idx", "idx2"]);
  assert_eq!(out, b"+OK\r\n", "RENAME 应成功迁移 RangeIndex 键");
  // 原键已迁移清退：TYPE 为 none
  assert_eq!(cmd(&rt, &mut c, &["TYPE", "idx"]), b"+none\r\n");
  // 新键继承 RI 记录与树内字段
  assert_eq!(cmd(&rt, &mut c, &["TYPE", "idx2"]), b"+rangeindex\r\n");
  assert_eq!(
    cmd(&rt, &mut c, &["RI.GET", "idx2", "field1"]),
    b"$6\r\nvalue1\r\n"
  );

  // RENAMENX：同名自改返回 1（对标 C# UnifiedStoreOps.cs:248）
  assert_eq!(
    cmd(&rt, &mut c, &["RENAMENX", "idx2", "idx2"]),
    b":1\r\n",
    "同名自改时 RENAMENX 应返回 1"
  );
  seed_ri(&rt, &mut c, "idx_existing");
  assert_eq!(
    cmd(&rt, &mut c, &["RENAMENX", "idx2", "idx_existing"]),
    b":0\r\n",
    "目标键已存在时 RENAMENX 应返回 0"
  );
  assert_eq!(
    cmd(&rt, &mut c, &["RENAMENX", "idx2", "idx3"]),
    b":1\r\n",
    "目标键不存在时 RENAMENX 应返回 1"
  );
  assert_eq!(cmd(&rt, &mut c, &["TYPE", "idx3"]), b"+rangeindex\r\n");
  assert_eq!(
    cmd(&rt, &mut c, &["RI.GET", "idx3", "field1"]),
    b"$6\r\nvalue1\r\n"
  );
}

/// 方向 2 之一：字符串写入口打 RI 键一律 WRONGTYPE，且 RI 记录不受损
#[test]
fn string_write_on_range_index_key_is_wrongtype() {
  let rt = Runtime::new().unwrap();
  let (_dir, mut c) = consumer();

  seed_ri(&rt, &mut c, "idx");

  // 四个入口逐个覆盖：裸 SET 盲写、SETEX/PSETEX 盲写、SET 选项族写共同体、
  // 条件写 XX 族（该族写前先 DELETE 清退，最易波及 RI 键）
  for args in [
    vec!["SET", "idx", "v"],
    vec!["SETEX", "idx", "10", "v"],
    vec!["PSETEX", "idx", "10000", "v"],
    vec!["SET", "idx", "v", "EX", "10"],
    vec!["SET", "idx", "v", "KEEPTTL"],
    vec!["SET", "idx", "v", "XX"],
    vec!["GETSET", "idx", "v"],
  ] {
    let out = cmd(&rt, &mut c, &args);
    assert!(
      is_wrongtype(&out),
      "{args:?} 打 RI 键应回 WRONGTYPE: {}",
      text(&out)
    );
  }

  // 门拦住写面：索引类型与树内字段完好
  assert_eq!(cmd(&rt, &mut c, &["TYPE", "idx"]), b"+rangeindex\r\n");
  assert_eq!(
    cmd(&rt, &mut c, &["RI.GET", "idx", "field1"]),
    b"$6\r\nvalue1\r\n"
  );

  // 放行方向不得被误伤：普通字符串键写入与 RI 键自身读写互不干扰
  assert_eq!(cmd(&rt, &mut c, &["SET", "plain", "v"]), b"+OK\r\n");
  assert_eq!(cmd(&rt, &mut c, &["GET", "plain"]), b"$1\r\nv\r\n");
  assert_eq!(
    cmd(&rt, &mut c, &["RI.SET", "idx", "field2", "value2"]),
    b"+OK\r\n"
  );
}

/// 方向 2 之二：RI 命令打非 RI 键一律 WRONGTYPE（建索引入口与存根装载入口）
#[test]
fn range_index_commands_on_non_ri_keys_are_wrongtype() {
  let rt = Runtime::new().unwrap();
  let (_dir, mut c) = consumer();

  // 字符串域命中：建索引入口此前误报 "index already exists"
  assert_eq!(cmd(&rt, &mut c, &["SET", "s", "v"]), b"+OK\r\n");
  for args in [
    vec!["RI.CREATE", "s", "MEMORY"],
    vec!["RI.SET", "s", "field1", "value1"],
    vec!["RI.GET", "s", "field1"],
    vec!["RI.DEL", "s", "field1"],
  ] {
    let out = cmd(&rt, &mut c, &args);
    assert!(
      is_wrongtype(&out),
      "{args:?} 打字符串键应回 WRONGTYPE: {}",
      text(&out)
    );
  }
  // 非 RI 键未被误建索引：字符串值原样在位
  assert_eq!(cmd(&rt, &mut c, &["GET", "s"]), b"$1\r\nv\r\n");
  assert_eq!(cmd(&rt, &mut c, &["TYPE", "s"]), b"+string\r\n");

  // 集合信封域命中（C# ValueIsObject 先于类型白名单，此前误报 no such index）
  assert_eq!(cmd(&rt, &mut c, &["HSET", "h", "f", "v"]), b":1\r\n");
  for args in [
    vec!["RI.CREATE", "h", "MEMORY"],
    vec!["RI.SET", "h", "field1", "value1"],
    vec!["RI.GET", "h", "f"],
  ] {
    let out = cmd(&rt, &mut c, &args);
    assert!(
      is_wrongtype(&out),
      "{args:?} 打集合信封键应回 WRONGTYPE: {}",
      text(&out)
    );
  }
  assert_eq!(cmd(&rt, &mut c, &["TYPE", "h"]), b"+hash\r\n");
  assert_eq!(cmd(&rt, &mut c, &["HGET", "h", "f"]), b"$1\r\nv\r\n");
}

/// 方向 2 之三：三域皆缺才是索引缺失（缺键答 not found / no such range index）
#[test]
fn range_index_missing_key_reports_missing_index() {
  let rt = Runtime::new().unwrap();
  let (_dir, mut c) = consumer();

  // 三域皆缺：建索引放行
  assert_eq!(cmd(&rt, &mut c, &["TYPE", "fresh"]), b"+none\r\n");
  assert_eq!(cmd(&rt, &mut c, &["GET", "fresh"]), b"$-1\r\n");
  seed_ri(&rt, &mut c, "fresh");

  // 装载面：索引缺失与类型不符是两条不同应答
  assert_eq!(
    cmd(&rt, &mut c, &["RI.SET", "gone", "field1", "value1"]),
    b"-ERR no such range index\r\n"
  );
  assert_eq!(
    cmd(&rt, &mut c, &["RI.GET", "gone", "field1"]),
    b"-ERR range index not found\r\n"
  );
  // 重复创建只在已存活 RI 元记录上报 already exists
  assert_eq!(
    cmd(&rt, &mut c, &["RI.CREATE", "fresh", "MEMORY"]),
    err_frame(wbftree::ERR_INDEX_ALREADY_EXISTS)
  );
}
