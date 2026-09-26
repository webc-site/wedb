//! INCR 族 '+前导零' 文法锁与 RI 键 OBJECT ENCODING raw 口径锁
//!（工单 zcode-r24-wvalstring）
//!
//! 用例一：`SET k "+007"` 后 INCR 族恒报 not-integer 且键值不被改写——C# 旧值
//! 路径 IsValidNumber → NumUtils.TryReadInt64 存在 '+' 绕过缺陷（前导零检查
//! 不剥 '+'，Utf8Parser D 格式收下 "+007" 得 7），rust 统一 strict_i64 严格
//! 文法，偏差登记见 doc/zh/deviations.md 条目 32a；对照组 "+5"/"-7"/"007"
//! 断言既有口径不变。快慢路径共用同一解析单点（read_user 闭包搂 strict_i64），
//! 热键快路径即锁全链。
//!
//! 用例二：RI.CREATE 后 OBJECT ENCODING 回 "raw"（对齐 C# 非 ValueIsObject
//! 记录 else 臂恒 raw，UnifiedStore ReadMethods.cs:69-72；修复前 rust 把
//! RangeIndex 落集合默认臂回 hashtable），TYPE 回 "rangeindex"；集合类四型
//! 编码映射平项不回归（快路径已收敛 encoding_of_object_type 单点）。

use std::{mem::take, sync::Arc};

use compio::runtime::Runtime;
use tempfile::tempdir;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::resp::{
  garnet_api::{GarnetApi, StoreGarnetApi},
  resp_server_session::{RespServerSession, RespServerSessionOptions},
};
use wnode_test::auto_exec;
use wresp::command::RespCommand;

/// 小容量单文件存储执行域（object_cold_degrade.rs 同款配置）
fn open_env(tag: &str) -> (Runtime, GarnetApi, tempfile::TempDir) {
  let dir = tempdir().unwrap();
  let config = StoreConfig::new(1024, 64 * 1024, 16, 0.5).unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag)).unwrap());
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  (
    Runtime::new().unwrap(),
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
    dir,
  )
}

/// 挂接分派器的会话
fn session_with(api: &GarnetApi) -> RespServerSession {
  let mut s = RespServerSession::new(1, RespServerSessionOptions::default());
  s.set_garnet_api(api.clone());
  s
}

/// 同步闭环执行（热键无降级形态，必须同步应答）
fn sync_exec(
  api: &GarnetApi,
  s: &mut RespServerSession,
  cmd: RespCommand,
  args: &[&[u8]],
) -> Vec<u8> {
  s.output.clear();
  api.exec(s, cmd, args);
  let out = take(&mut s.output);
  assert!(!out.is_empty(), "{cmd} 必须同步闭环而非挂起");
  out
}

/// not-integer 错误帧（C# CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER）
const NOT_INTEGER: &[u8] = b"-ERR value is not an integer or out of range.\r\n";

/// INCR 族旧值 '+前导零' 文法锁："+007" 全族拒绝且键值保持；对照组
/// "+5"/"-7" 双侧一致自增、"007"（无 '+' 前缀）两侧同拒（deviations 条目 32a）
#[test]
fn incr_plus_leading_zero_rejected_and_value_untouched() {
  let (_rt, api, _dir) = open_env("incr-grammar.db");
  let mut s = session_with(&api);

  // 主组：INCR / DECR / INCRBY 0 / DECRBY 0 全臂拒，键值恒为 "+007"
  assert_eq!(
    sync_exec(&api, &mut s, RespCommand::Set, &[b"k", b"+007"]),
    b"+OK\r\n"
  );
  for (cmd, args) in [
    (RespCommand::Incr, Vec::new()),
    (RespCommand::Decr, Vec::new()),
    (RespCommand::Incrby, vec![b"0".as_slice()]),
    (RespCommand::Decrby, vec![b"0".as_slice()]),
  ] {
    let mut full: Vec<&[u8]> = vec![b"k"];
    full.extend(args);
    assert_eq!(
      sync_exec(&api, &mut s, cmd, &full),
      NOT_INTEGER,
      "{cmd} 对旧值 \"+007\" 必须拒写"
    );
    assert_eq!(
      sync_exec(&api, &mut s, RespCommand::Get, &[b"k"]),
      b"$4\r\n+007\r\n",
      "{cmd} 拒写后键值必须保持"
    );
  }

  // 对照组 "+5"：+ 号形态两侧一致收下（rust strict_i64 认 '+5'，C# Utf8Parser
  // 同收），INCR 自增写回 "6"
  assert_eq!(
    sync_exec(&api, &mut s, RespCommand::Set, &[b"k", b"+5"]),
    b"+OK\r\n"
  );
  assert_eq!(
    sync_exec(&api, &mut s, RespCommand::Incr, &[b"k"]),
    b":6\r\n"
  );

  // 对照组 "-7"：负号两侧一致，INCR 得 -6
  assert_eq!(
    sync_exec(&api, &mut s, RespCommand::Set, &[b"k", b"-7"]),
    b"+OK\r\n"
  );
  assert_eq!(
    sync_exec(&api, &mut s, RespCommand::Incr, &[b"k"]),
    b":-6\r\n"
  );

  // 对照组 "007"：无 '+' 前缀的前导零两侧同拒（C# 前导零检查本臂生效）
  assert_eq!(
    sync_exec(&api, &mut s, RespCommand::Set, &[b"k", b"007"]),
    b"+OK\r\n"
  );
  assert_eq!(
    sync_exec(&api, &mut s, RespCommand::Incr, &[b"k"]),
    NOT_INTEGER
  );
  assert_eq!(
    sync_exec(&api, &mut s, RespCommand::Get, &[b"k"]),
    b"$3\r\n007\r\n"
  );
}

/// RI 键 OBJECT ENCODING 锁：RI.CREATE 后恒回 raw（C# RI 主存记录不置
/// ValueIsObject，HandleObjectEncoding else 臂恒 raw），TYPE 回 rangeindex；
/// 集合四型编码映射平项不回归
#[test]
fn ri_key_object_encoding_raw_and_type_lock() {
  let (rt, api, _dir) = open_env("ri-encoding.db");
  let mut s = session_with(&api);

  // RI 键：Meta 元记录 collection_type = RangeIndex → raw / rangeindex
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Ricreate, &[b"ri", b"DISK"]),
    b"+OK\r\n"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::ObjectEncoding, &[b"ri"]),
    b"$3\r\nraw\r\n",
    "RI 键编码必须回 raw 对齐 C# 非 ValueIsObject 记录口径"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Type, &[b"ri"]),
    b"+rangeindex\r\n"
  );

  // 平项：String 域 raw、集合信封四型映射（收敛单点后不得漂移）
  assert_eq!(
    sync_exec(&api, &mut s, RespCommand::Set, &[b"st", b"v"]),
    b"+OK\r\n"
  );
  assert_eq!(
    sync_exec(&api, &mut s, RespCommand::ObjectEncoding, &[b"st"]),
    b"$3\r\nraw\r\n"
  );
  assert_eq!(
    sync_exec(&api, &mut s, RespCommand::Hset, &[b"h", b"f", b"v"]),
    b":1\r\n"
  );
  assert_eq!(
    sync_exec(&api, &mut s, RespCommand::ObjectEncoding, &[b"h"]),
    b"$9\r\nhashtable\r\n"
  );
  assert_eq!(
    sync_exec(&api, &mut s, RespCommand::Zadd, &[b"z", b"1", b"m"]),
    b":1\r\n"
  );
  assert_eq!(
    sync_exec(&api, &mut s, RespCommand::ObjectEncoding, &[b"z"]),
    b"$8\r\nskiplist\r\n"
  );
  assert_eq!(
    sync_exec(&api, &mut s, RespCommand::Rpush, &[b"l", b"a"]),
    b":1\r\n"
  );
  assert_eq!(
    sync_exec(&api, &mut s, RespCommand::ObjectEncoding, &[b"l"]),
    b"$9\r\nquicklist\r\n"
  );
}
