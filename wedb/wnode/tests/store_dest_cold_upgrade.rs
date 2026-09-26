//! STORE 族冷收尾升阶判链回归（票 wnode-store-dest-cold-upgrade-bypass）
//!
//! 缺陷形态：`store_dest_cold_common` 非空臂曾裸 `obj_save_clear_ttl` 写信封，
//! 零 `should_promote`/零 `envelope_overflow` 判链——>=65536 条目或超页配置的
//! STORE 大结果走冷路径（磁盘候选）即直写超大信封：whlog RecordTooLarge 拒、
//! 错误帧回吐（同命令 C# 成功回基数），或经异步 upsert_tag 臂落位超页信封后
//! 永无升阶修复点。修复后冷臂与同步漏斗 `obj_save_or_gc` 同款判链（wcol 单源
//! 谓词），命中窗内就地走 bftree+Meta 升阶漏斗。
//!
//! 验证点：
//! a) 冷键 >=65536 条目 SINTERSTORE / ZUNIONSTORE：目标键终态 Meta+bftree
//!    （存活域探针 = KeyTag::Meta，信封随升阶元记录落盘删除），非错误帧回基数；
//! b) 多小源 SUNIONSTORE 折叠出超页结果信封（单源信封装得下、结果装不下，
//!    条目数远未达阈）+ 小页预算：无错误帧且就地升阶（C# 对拍成功回基数），
//!    判链命中的是超页维 [`envelope_overflow`]（而非条目数维）；
//! c) 同一 STORE 命令同步/冷双路径终态与 RESP 全等：小结果信封臂 TTL 清零与
//!    成员覆写不回退，旧 String 域清退双臂一致，大结果升阶终态双臂全等
//!    （守已归档窗序票 wnode-store-cold-window-ttl-clear-outsides-critical-section）。

use std::{mem::take, sync::Arc};

use compio::runtime::Runtime;
use tempfile::TempDir;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::{
  resp::{
    garnet_api::{GarnetApi, StoreGarnetApi},
    resp_server_session::{RespServerSession, RespServerSessionOptions},
  },
  storage::session::common::ttl_sync::probe_alive_domain,
};
use wresp::command::RespCommand;
use wval::{GarnetObjectType, KeyTag};

type TestStore = WedbStore<SegmentedDevice>;

struct Env {
  rt: Runtime,
  store: Arc<TestStore>,
  api: GarnetApi,
  _dir: TempDir,
}

fn env_with_pages(tag: &str, page_size: usize, num_pages: usize) -> Env {
  let dir = TempDir::new().unwrap();
  let config = StoreConfig::new(2048, page_size, num_pages, 0.5).unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag)).unwrap());
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  let api: GarnetApi = Arc::new(StoreGarnetApi::new(store.new_session().unwrap()));
  Env {
    rt: Runtime::new().unwrap(),
    store,
    api,
    _dir: dir,
  }
}

fn env(tag: &str) -> Env {
  env_with_pages(tag, 1024 * 1024, 16)
}

fn session(env: &Env) -> RespServerSession {
  let mut s = RespServerSession::new(1, RespServerSessionOptions::default());
  s.set_garnet_api(env.api.clone());
  s
}

/// 单命令执行：同步段直出优先，冷键挂慢路径则由 block_on 承担网络泵闭环
fn exec(env: &Env, s: &mut RespServerSession, cmd: RespCommand, args: &[&[u8]]) -> Vec<u8> {
  s.output.clear();
  env.api.exec(s, cmd, args);
  if !s.output.is_empty() {
    return take(&mut s.output);
  }
  let slow = s
    .take_slow_wait()
    .unwrap_or_else(|| panic!("命令 {cmd} 无输出且未挂起慢路径"));
  let out = env.rt.block_on(slow.resolve());
  s.output.clear();
  out
}

/// 冷化：全键落盘为磁盘候选，装载型命令自此走慢路径臂
fn freeze(env: &Env) {
  env.rt.block_on(env.store.flush_and_evict_all()).unwrap();
}

/// 非错误帧断言（缺陷形态下超页信封被 RecordTooLarge 拒即在此炸出错误帧）
fn assert_no_error_frame(reply: &[u8], what: &str) {
  assert!(
    !reply.starts_with(b"-"),
    "{what} 不应回错误帧，实际：{:?}",
    String::from_utf8_lossy(reply)
  );
}

/// 分层终态判据：Meta 存根在册且 size 精确；信封域必已消亡
/// （存活域探针 = KeyTag::Meta，杜绝「Meta + 超页信封」双域残留）
fn assert_tiered(env: &Env, key: &[u8], expect_type: GarnetObjectType, expect_size: usize) {
  let sess = env.store.new_session().unwrap();
  let loaded = env
    .rt
    .block_on(sess.load_collection_stub(key))
    .unwrap()
    .unwrap_or_else(|| {
      panic!(
        "key='{}' 应为 Meta+bftree 分层终态",
        String::from_utf8_lossy(key)
      )
    });
  let (meta, _stub) = loaded;
  assert_eq!(meta.collection_type, expect_type);
  assert_eq!(
    meta.size as usize,
    expect_size,
    "key='{}' 升阶后元记录 size 失真",
    String::from_utf8_lossy(key)
  );
  let batch = sess.enter_batch();
  let domain = probe_alive_domain(&batch, key)
    .unwrap()
    .expect("探针应可裁决（写回落笔于内存可变区）");
  assert_eq!(
    domain,
    Some(KeyTag::Meta),
    "key='{}' 终态存活域应为 Meta（信封非空即判链旁路仍在位）",
    String::from_utf8_lossy(key)
  );
}

/// `SADD key m0..m{count-1}`（16384 员分批，同 collection_adaptive_tiering 泵形）
fn fill_set(env: &Env, s: &mut RespServerSession, key: &[u8], count: usize) {
  for start in (0..count).step_by(16384) {
    let end = (start + 16384).min(count);
    let mut args: Vec<Vec<u8>> = Vec::with_capacity(end - start + 1);
    args.push(key.to_vec());
    for i in start..end {
      args.push(format!("m{i}").into_bytes());
    }
    let slices: Vec<&[u8]> = args.iter().map(Vec::as_slice).collect();
    exec(env, s, RespCommand::Sadd, &slices);
  }
}

/// `ZADD key i m{i}` × count（同员同分，与 fill_set 同构）
fn fill_zset(env: &Env, s: &mut RespServerSession, key: &[u8], count: usize) {
  for start in (0..count).step_by(8192) {
    let end = (start + 8192).min(count);
    let mut args: Vec<Vec<u8>> = Vec::with_capacity((end - start) * 2 + 1);
    args.push(key.to_vec());
    for i in start..end {
      args.push(i.to_string().into_bytes());
      args.push(format!("m{i}").into_bytes());
    }
    let slices: Vec<&[u8]> = args.iter().map(Vec::as_slice).collect();
    exec(env, s, RespCommand::Zadd, &slices);
  }
}

/// 验证点 a（set 臂）：>=65536 条目冷键 SINTERSTORE 终态 Meta+bftree 非信封，
/// 回基数无错误帧，目标键 TTL 随写清零不回退
#[test]
fn sinterstore_cold_large_result_promotes_bftree() {
  let total = wcol::TIERED_PROMOTE_THRESHOLD + 10;
  let env = env("store-cold-promote-set.db");
  let mut s = session(&env);
  fill_set(&env, &mut s, b"src1", total);
  fill_set(&env, &mut s, b"src2", total);
  // 目标键既存小集合 + 挂 TTL：冷化后验证 SET 语义清零与旧信封域被升阶承接
  exec(&env, &mut s, RespCommand::Sadd, &[b"dst", b"stale"]);
  assert_eq!(
    exec(&env, &mut s, RespCommand::Expire, &[b"dst", b"600"]),
    b":1\r\n"
  );
  freeze(&env);

  let reply = exec(
    &env,
    &mut s,
    RespCommand::Sinterstore,
    &[b"dst", b"src1", b"src2"],
  );
  assert_no_error_frame(&reply, "冷态大结果 SINTERSTORE");
  assert_eq!(reply, format!(":{total}\r\n").as_bytes());
  assert_tiered(&env, b"dst", GarnetObjectType::Set, total);
  assert_eq!(
    exec(&env, &mut s, RespCommand::Scard, &[b"dst"]),
    format!(":{total}\r\n").as_bytes()
  );
  assert_eq!(
    exec(&env, &mut s, RespCommand::Sismember, &[b"dst", b"m0"]),
    b":1\r\n"
  );
  assert_eq!(
    exec(&env, &mut s, RespCommand::Sismember, &[b"dst", b"stale"]),
    b":0\r\n"
  );
  // TTL 清零语义不回退（升阶迁移臂本身不动 TTL 旁路，SET 语义随写窗内显式清退）
  assert_eq!(exec(&env, &mut s, RespCommand::Pttl, &[b"dst"]), b":-1\r\n");
}

/// 验证点 a（zset 臂）：>=65536 条目冷键 ZUNIONSTORE 同判链升阶
#[test]
fn zunionstore_cold_large_result_promotes_bftree() {
  let total = wcol::TIERED_PROMOTE_THRESHOLD + 10;
  let env = env("store-cold-promote-zset.db");
  let mut s = session(&env);
  fill_zset(&env, &mut s, b"zsrc", total);
  exec(&env, &mut s, RespCommand::Zadd, &[b"zdst", b"1", b"stale"]);
  assert_eq!(
    exec(&env, &mut s, RespCommand::Expire, &[b"zdst", b"600"]),
    b":1\r\n"
  );
  freeze(&env);

  let reply = exec(
    &env,
    &mut s,
    RespCommand::Zunionstore,
    &[b"zdst", b"1", b"zsrc"],
  );
  assert_no_error_frame(&reply, "冷态大结果 ZUNIONSTORE");
  assert_eq!(reply, format!(":{total}\r\n").as_bytes());
  assert_tiered(&env, b"zdst", GarnetObjectType::SortedSet, total);
  assert_eq!(
    exec(&env, &mut s, RespCommand::Zcard, &[b"zdst"]),
    format!(":{total}\r\n").as_bytes()
  );
  assert_eq!(
    exec(&env, &mut s, RespCommand::Pttl, &[b"zdst"]),
    b":-1\r\n"
  );
}

/// 验证点 b：多小源 SUNIONSTORE 折叠出超页结果（单源 ~3KB 信封 4KB 页装得下、
/// 结果 ~30KB 装不下；条目数 300 远未达 65536 阈）——判链命中超页维
/// [`envelope_overflow`]，冷 STORE 无错误帧且就地升阶，回基数与 C# 对拍
#[test]
fn sunionstore_cold_page_overflow_promotes_no_error_frame() {
  let n_src = 10usize;
  let per_src = 30usize;
  let total = n_src * per_src;
  // 成员 100B（< 128B 树键契约上限，升阶建树可受理），且逐源逐员唯一——
  // 单源信封 ~3KB < 4KB 页（保持信封态可种入），union 结果 ~30KB > 4KB 页
  let member = |i: usize, j: usize| {
    let mut m = format!("mem_{i:02}_{j:04}_").into_bytes();
    m.resize(100, b'a' + (i as u8) + (j as u8 % 26));
    m
  };
  let env = env_with_pages("store-cold-promote-overflow.db", 4096, 64);
  let mut s = session(&env);
  let mut src_names: Vec<Vec<u8>> = Vec::with_capacity(n_src);
  for i in 0..n_src {
    let name = format!("u:src{i}").into_bytes();
    for j in 0..per_src {
      assert_no_error_frame(
        &exec(&env, &mut s, RespCommand::Sadd, &[&name, &member(i, j)]),
        "超页维种入单源",
      );
    }
    // 单源保持信封（未达条目阈、信封未超页），基数精确
    assert_eq!(
      exec(&env, &mut s, RespCommand::Scard, &[&name]),
      format!(":{per_src}\r\n").as_bytes(),
      "单源应完整种入 {per_src} 员",
    );
    src_names.push(name);
  }
  freeze(&env);

  let mut args: Vec<&[u8]> = vec![b"u:dst"];
  for n in &src_names {
    args.push(n.as_slice());
  }
  let reply = exec(&env, &mut s, RespCommand::Sunionstore, &args);
  assert_no_error_frame(
    &reply,
    "冷态超页结果 SUNIONSTORE（判链缺失时此处为 RecordTooLarge 错误帧）",
  );
  assert_eq!(reply, format!(":{total}\r\n").as_bytes());
  assert_tiered(&env, b"u:dst", GarnetObjectType::Set, total);
  assert_eq!(
    exec(&env, &mut s, RespCommand::Scard, &[b"u:dst"]),
    format!(":{total}\r\n").as_bytes()
  );
  assert_eq!(
    exec(
      &env,
      &mut s,
      RespCommand::Sismember,
      &[b"u:dst", &member(3, 17)]
    ),
    b":1\r\n"
  );
}

/// 验证点 c：同一 STORE 命令同步/冷双路径终态与 RESP 全等——小结果信封臂
/// （TTL 清零 + 成员覆写）、旧 String 域清退、大结果升阶臂三组双生子逐字节比
#[test]
fn store_dest_sync_cold_paths_parity() {
  let total = wcol::TIERED_PROMOTE_THRESHOLD + 10;
  let env = env("store-cold-promote-parity.db");
  let mut s = session(&env);
  // 组 P（信封臂覆写 + TTL）：psrc ∩ 目标键 p:hot（同步快臂）/ p:cold（冷化后慢臂）
  exec(&env, &mut s, RespCommand::Sadd, &[b"p:src", b"a", b"b"]);
  exec(&env, &mut s, RespCommand::Sadd, &[b"p:hot", b"x"]);
  exec(&env, &mut s, RespCommand::Sadd, &[b"p:cold", b"x"]);
  assert_eq!(
    exec(&env, &mut s, RespCommand::Expire, &[b"p:hot", b"600"]),
    b":1\r\n"
  );
  assert_eq!(
    exec(&env, &mut s, RespCommand::Expire, &[b"p:cold", b"600"]),
    b":1\r\n"
  );
  // 组 S（旧 String 域清退）
  exec(&env, &mut s, RespCommand::Set, &[b"s:hot", b"strval"]);
  exec(&env, &mut s, RespCommand::Set, &[b"s:cold", b"strval"]);
  // 组 L（升阶臂）：共享大源，l:hot 冷化前、l:cold 冷化后；双目标键预挂 TTL
  // 验证升阶臂 SET 语义清零双侧对等
  fill_set(&env, &mut s, b"l:src1", total);
  fill_set(&env, &mut s, b"l:src2", total);
  exec(&env, &mut s, RespCommand::Sadd, &[b"l:hot", b"pre"]);
  exec(&env, &mut s, RespCommand::Sadd, &[b"l:cold", b"pre"]);
  assert_eq!(
    exec(&env, &mut s, RespCommand::Expire, &[b"l:hot", b"600"]),
    b":1\r\n"
  );
  assert_eq!(
    exec(&env, &mut s, RespCommand::Expire, &[b"l:cold", b"600"]),
    b":1\r\n"
  );

  // 冷化前：同步/热慢路径臂
  let p_hot = exec(
    &env,
    &mut s,
    RespCommand::Sinterstore,
    &[b"p:hot", b"p:src"],
  );
  let s_hot = exec(
    &env,
    &mut s,
    RespCommand::Sinterstore,
    &[b"s:hot", b"p:src"],
  );
  // 旧 String 域清退验证即时紧跟本路径 STORE（信封新在内存态，避开冷化后
  // GET 的既有磁盘边界行为，纯比对清退语义）：GET 必回 WRONGTYPE 非 strval
  let get_hot = exec(&env, &mut s, RespCommand::Get, &[b"s:hot"]);
  let l_hot = exec(
    &env,
    &mut s,
    RespCommand::Sinterstore,
    &[b"l:hot", b"l:src1", b"l:src2"],
  );
  assert_no_error_frame(&l_hot, "热态大结果 SINTERSTORE");
  assert_eq!(l_hot, format!(":{total}\r\n").as_bytes());
  assert_tiered(&env, b"l:hot", GarnetObjectType::Set, total);

  freeze(&env);

  // 冷化后：磁盘候选慢路径臂（同构命令冷孪生）
  let p_cold = exec(
    &env,
    &mut s,
    RespCommand::Sinterstore,
    &[b"p:cold", b"p:src"],
  );
  let s_cold = exec(
    &env,
    &mut s,
    RespCommand::Sinterstore,
    &[b"s:cold", b"p:src"],
  );
  let get_cold = exec(&env, &mut s, RespCommand::Get, &[b"s:cold"]);
  let l_cold = exec(
    &env,
    &mut s,
    RespCommand::Sinterstore,
    &[b"l:cold", b"l:src1", b"l:src2"],
  );
  assert_no_error_frame(&l_cold, "冷态大结果 SINTERSTORE");

  // RESP 应答逐字节全等
  assert_eq!(p_hot, p_cold, "信封臂同命令双路径应答不一致");
  assert_eq!(p_hot, b":2\r\n", "单源 SINTERSTORE 结果 = 源集合 {{a,b}}");
  assert_eq!(s_hot, s_cold, "String 域目标同命令双路径应答不一致");
  assert_eq!(l_hot, l_cold, "升阶臂同命令双路径应答不一致");
  // 终态全等：TTL 清零、集合基数、旧 String 域清退（GET 同帧 WRONGTYPE）
  assert_eq!(
    exec(&env, &mut s, RespCommand::Pttl, &[b"p:hot"]),
    exec(&env, &mut s, RespCommand::Pttl, &[b"p:cold"]),
    "双路径 TTL 清零不一致"
  );
  assert_eq!(
    exec(&env, &mut s, RespCommand::Pttl, &[b"p:cold"]),
    b":-1\r\n"
  );
  assert_eq!(
    exec(&env, &mut s, RespCommand::Smembers, &[b"p:hot"]),
    exec(&env, &mut s, RespCommand::Smembers, &[b"p:cold"]),
    "信封臂双路径终态成员不一致"
  );
  // 旧 String 域清退双路径全等：各自 STORE 后紧跟的 GET 同帧 WRONGTYPE
  // （仍残留字符串域即此处炸出，且绝不等 strval）
  assert_eq!(get_hot, get_cold, "旧 String 域清退双路径不一致");
  assert!(
    !get_hot.starts_with(b"$6\r\nstrval"),
    "STORE 覆写后旧 String 域未清退（GET 仍回 strval）：{:?}",
    String::from_utf8_lossy(&get_hot)
  );
  assert!(
    get_hot.starts_with(b"-"),
    "GET 覆写后集合键应回 WRONGTYPE 错误帧：{:?}",
    String::from_utf8_lossy(&get_hot)
  );
  assert!(
    exec(&env, &mut s, RespCommand::Type, &[b"s:cold"]) == b"+set\r\n".to_vec(),
    "STORE 覆写后目标键类型应为 set"
  );
  // 大结果双路径终态：同为 Meta+bftree、size 全等、TTL 清零
  assert_tiered(&env, b"l:cold", GarnetObjectType::Set, total);
  assert_eq!(
    exec(&env, &mut s, RespCommand::Pttl, &[b"l:hot"]),
    b":-1\r\n"
  );
  assert_eq!(
    exec(&env, &mut s, RespCommand::Pttl, &[b"l:cold"]),
    b":-1\r\n"
  );
}
