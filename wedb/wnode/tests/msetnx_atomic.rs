//! MSETNX 原子性回归（全有或全无 + 降级重放语义一致）
//!
//! 对标 C# MainStoreOps.cs:MSET_Conditional（全键排他锁内 EXISTS 判定 +
//! 锁内批量 SET + Commit）与 Resp/ArrayCommands.cs:NetworkMSETNX（按
//! status 回 :0/:1）：快路径部分存在全不写回 :0、全不存在全写回 :1；
//! 判定段 / 写入段降级整体移交慢路径（exec_slow C::Msetnx）续跑，应答
//! 与存储终态恒一致，杜绝半提交误答。

use std::{
  sync::Arc,
  time::{SystemTime, UNIX_EPOCH},
};

use compio::runtime::Runtime;
use tempfile::tempdir;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::resp::{
  garnet_api::{GarnetApi, StoreGarnetApi},
  resp_server_session::{RespServerSession, RespServerSessionOptions},
};
use wresp::command::RespCommand;

/// 小容量单文件存储执行域（tests/store_garnet_api_dispatch.rs 同款配置）
fn open_api(path: &str) -> (GarnetApi, tempfile::TempDir) {
  let dir = tempdir().unwrap();
  let config = StoreConfig::new(1024, 64 * 1024, 16, 0.5).unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(path)).unwrap());
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  (
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
    dir,
  )
}

/// 挂接分派器的会话（降级链路须经 session.garnet_api 挂起 SlowWait）
fn session_with(api: &GarnetApi) -> RespServerSession {
  let mut s = RespServerSession::new(1, RespServerSessionOptions::default());
  s.set_garnet_api(api.clone());
  s
}

#[test]
fn msetnx_all_missing_writes_all() {
  let (api, _dir) = open_api("msetnx-all-missing.db");
  let mut s = session_with(&api);

  // 全不存在：全写入回 :1（C# NOTFOUND → 1）
  api.exec(&mut s, RespCommand::Msetnx, &[b"k1", b"v1", b"k2", b"v2"]);
  assert_eq!(s.output, b":1\r\n");

  // 两键均已生效
  s.output.clear();
  api.exec(&mut s, RespCommand::Get, &[b"k1"]);
  assert_eq!(s.output, b"$2\r\nv1\r\n");
  s.output.clear();
  api.exec(&mut s, RespCommand::Get, &[b"k2"]);
  assert_eq!(s.output, b"$2\r\nv2\r\n");
}

#[test]
fn msetnx_any_existing_writes_none() {
  let (api, _dir) = open_api("msetnx-any-existing.db");
  let mut s = session_with(&api);

  // k1 预置
  api.exec(&mut s, RespCommand::Set, &[b"k1", b"old"]);
  s.output.clear();

  // 任一键已存在：全不写入回 :0
  api.exec(&mut s, RespCommand::Msetnx, &[b"k1", b"new", b"k2", b"v2"]);
  assert_eq!(s.output, b":0\r\n");

  // 已存在键值不变、缺失键保持缺失（全有或全无）
  s.output.clear();
  api.exec(&mut s, RespCommand::Get, &[b"k1"]);
  assert_eq!(s.output, b"$3\r\nold\r\n");
  s.output.clear();
  api.exec(&mut s, RespCommand::Get, &[b"k2"]);
  assert_eq!(s.output, b"$-1\r\n");
}

#[test]
fn msetnx_object_key_counts_as_existing() {
  let (api, _dir) = open_api("msetnx-object-key.db");
  let mut s = session_with(&api);

  // 对象键（信封域）同计存在（C# EXISTS 走 unified 域同口径）
  api.exec(&mut s, RespCommand::Hset, &[b"obj", b"f", b"v"]);
  s.output.clear();

  api.exec(&mut s, RespCommand::Msetnx, &[b"obj", b"x", b"k2", b"v2"]);
  assert_eq!(s.output, b":0\r\n");

  s.output.clear();
  api.exec(&mut s, RespCommand::Get, &[b"k2"]);
  assert_eq!(s.output, b"$-1\r\n");
}

#[test]
fn msetnx_expired_key_treated_as_missing() {
  let (api, _dir) = open_api("msetnx-expired-missing.db");
  let mut s = session_with(&api);

  // k1 预置后打上已过期的绝对过期（PEXPIREAT 过去毫秒即删，wkv applied=2
  // 物理删除语义）：NX 判定视同缺失，MSETNX 通过并覆盖写入（Redis MSETNX
  // 对过期键同语义）
  api.exec(&mut s, RespCommand::Set, &[b"k1", b"v1"]);
  s.output.clear();
  let past_ms = (SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .unwrap()
    .as_millis() as u64)
    - 10_000;
  api.exec(
    &mut s,
    RespCommand::Pexpireat,
    &[b"k1", past_ms.to_string().as_bytes()],
  );
  s.output.clear();

  api.exec(&mut s, RespCommand::Msetnx, &[b"k1", b"x", b"k2", b"v2"]);
  assert_eq!(s.output, b":1\r\n");

  // 全部键已生效：k1 为本次 MSETNX 的值（旧值连同过期记录一并清退）
  s.output.clear();
  api.exec(&mut s, RespCommand::Get, &[b"k1"]);
  assert_eq!(s.output, b"$1\r\nx\r\n");
  s.output.clear();
  api.exec(&mut s, RespCommand::Get, &[b"k2"]);
  assert_eq!(s.output, b"$2\r\nv2\r\n");
}

#[test]
fn msetnx_slow_full_verdict_matches_fast_semantics() {
  let rt = Runtime::new().unwrap();
  let (api, _dir) = open_api("msetnx-slow-verdict.db");

  // 判定段降级模式（尾参 b"0"）· 全不存在：裁决通过 → 全写回 :1
  let out = rt.block_on(Arc::clone(&api).exec_slow(
    RespCommand::Msetnx,
    vec![
      b"k1".to_vec(),
      b"v1".to_vec(),
      b"k2".to_vec(),
      b"v2".to_vec(),
      b"0".to_vec(),
    ],
    wconf::DEFAULT_RESP_VERSION,
  ));
  assert_eq!(out, b":1\r\n");

  // 判定段降级模式 · 部分存活（k1 已由上一段写入）：整体不写回 :0
  let out = rt.block_on(Arc::clone(&api).exec_slow(
    RespCommand::Msetnx,
    vec![
      b"k1".to_vec(),
      b"x".to_vec(),
      b"k3".to_vec(),
      b"v3".to_vec(),
      b"0".to_vec(),
    ],
    wconf::DEFAULT_RESP_VERSION,
  ));
  assert_eq!(out, b":0\r\n");

  // 终态：k3 未写入、k1 值不变（裁决失败绝不半提交）
  let mut s = session_with(&api);
  api.exec(&mut s, RespCommand::Get, &[b"k3"]);
  assert_eq!(s.output, b"$-1\r\n");
  s.output.clear();
  api.exec(&mut s, RespCommand::Get, &[b"k1"]);
  assert_eq!(s.output, b"$2\r\nv1\r\n");
}

#[test]
fn msetnx_slow_resume_completes_partial_write() {
  let rt = Runtime::new().unwrap();
  let (api, _dir) = open_api("msetnx-slow-resume.db");
  let mut s = session_with(&api);

  // 模拟快路径写入段降级现场：判定已整体通过、前缀键 k1 已持久写入
  api.exec(&mut s, RespCommand::Set, &[b"k1", b"v1"]);
  s.output.clear();

  // 补写模式（尾参 b"1"）：不重判存活，续写全部键值（重写已写键同值
  // 幂等）回 :1——降级前判定为"全部不存在将写入"，终应答与终态一致
  let out = rt.block_on(Arc::clone(&api).exec_slow(
    RespCommand::Msetnx,
    vec![
      b"k1".to_vec(),
      b"v1".to_vec(),
      b"k2".to_vec(),
      b"v2".to_vec(),
      b"1".to_vec(),
    ],
    wconf::DEFAULT_RESP_VERSION,
  ));
  assert_eq!(out, b":1\r\n");

  s.output.clear();
  api.exec(&mut s, RespCommand::Get, &[b"k2"]);
  assert_eq!(s.output, b"$2\r\nv2\r\n");
  s.output.clear();
  api.exec(&mut s, RespCommand::Get, &[b"k1"]);
  assert_eq!(s.output, b"$2\r\nv1\r\n");
}
