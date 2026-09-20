//! 集合类型自适应混合分层引擎集成测试
//!
//! 验证：
//! 1. 小中规模内存态信封操作与 O(1) 标量计数；
//! 2. 跨阈值 (`wcol::TIERED_PROMOTE_THRESHOLD`) 自动就地升阶为独立分页 BfTree；
//! 3. 分层态下的 Hash/Set/ZSet/List 增删改查；
//! 4. 删空自愈（Strict Empty Deletion）：元素清零原子销毁底层树与清理元记录。

use std::{mem::take, str::from_utf8_unchecked, sync::Arc};

use compio::runtime::Runtime;
use itoa::Buffer as ItoaBuffer;
use tempfile::tempdir;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::resp::{
  garnet_api::{GarnetApi, StoreGarnetApi},
  resp_server_session::{RespServerSession, RespServerSessionOptions},
};
use wresp::command::RespCommand;

fn open_env(
  tag: &str,
) -> (
  Runtime,
  GarnetApi,
  Arc<WedbStore<SegmentedDevice>>,
  tempfile::TempDir,
) {
  let dir = tempdir().unwrap();
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

fn auto_exec(
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

#[test]
fn test_hash_adaptive_tiering_and_o1_count() {
  let (rt, api, store, _dir) = open_env("test-adaptive-hash.db");
  let mut s = session_with(&api);

  // 1. 小集合内存态操作
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Hset,
      &[b"myhash", b"f1", b"v1", b"f2", b"v2"]
    ),
    b":2\r\n"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Hlen, &[b"myhash"]),
    b":2\r\n"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Hget, &[b"myhash", b"f1"]),
    b"$2\r\nv1\r\n"
  );

  // 2. 批量写入跨越 TIERED_PROMOTE_THRESHOLD 门限触发自动升阶
  let total = wcol::TIERED_PROMOTE_THRESHOLD + 10;
  let mut buf = ItoaBuffer::new();
  for chunk_start in (3..=total).step_by(1000) {
    let chunk_end = (chunk_start + 999).min(total);
    let mut args: Vec<Vec<u8>> = Vec::with_capacity((chunk_end - chunk_start + 1) * 2 + 1);
    args.push(b"myhash".to_vec());
    for i in chunk_start..=chunk_end {
      args.push(format!("f{i}").into_bytes());
      args.push(buf.format(i).as_bytes().to_vec());
    }
    let arg_slices: Vec<&[u8]> = args.iter().map(|v| v.as_slice()).collect();
    auto_exec(&api, &rt, &mut s, RespCommand::Hset, &arg_slices);
  }

  // 3. 升阶后 O(1) 标量计数验证
  let expected_len = format!(":{total}\r\n");
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Hlen, &[b"myhash"]),
    expected_len.as_bytes()
  );

  // 4. 分层态下的点查与点写
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Hget, &[b"myhash", b"f1"]),
    b"$2\r\nv1\r\n"
  );
  let target_field = format!("f{}", total - 1);
  let target_val = format!("{}", total - 1);
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Hget,
      &[b"myhash", target_field.as_bytes()]
    ),
    format!("${}\r\n{}\r\n", target_val.len(), target_val).as_bytes()
  );
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Hincrby,
      &[b"myhash", target_field.as_bytes(), b"1"]
    ),
    format!(":{total}\r\n").as_bytes()
  );
  let updated_val = format!("{total}");
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Hget,
      &[b"myhash", target_field.as_bytes()]
    ),
    format!("${}\r\n{}\r\n", updated_val.len(), updated_val).as_bytes()
  );

  // 5. 校验底层 BfTree 存根存在
  let sess = store.new_session().unwrap();
  let stub_opt = rt.block_on(sess.load_collection_stub(b"myhash")).unwrap();
  assert!(stub_opt.is_some(), "升阶后应存在 BfTree 存根");
  let (meta, _) = stub_opt.unwrap();
  assert_eq!(meta.size as usize, total);

  // 6. 删空自愈验证
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Del, &[b"myhash"]),
    b":1\r\n"
  );
  let stub_after_del = rt.block_on(sess.load_collection_stub(b"myhash")).unwrap();
  assert!(
    stub_after_del.is_none(),
    "DEL 后底层 BfTree 存根应被物理释放"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Hlen, &[b"myhash"]),
    b":0\r\n"
  );
}

#[test]
fn test_set_adaptive_tiering_and_o1_count() {
  let (rt, api, store, _dir) = open_env("test-adaptive-set.db");
  let mut s = session_with(&api);

  // 1. 小集合内存态操作
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Sadd,
      &[b"myset", b"m1", b"m2", b"m3"]
    ),
    b":3\r\n"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Scard, &[b"myset"]),
    b":3\r\n"
  );
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Sismember,
      &[b"myset", b"m1"]
    ),
    b":1\r\n"
  );

  // 2. 批量写入跨越 TIERED_PROMOTE_THRESHOLD 门限触发自动升阶
  let total = wcol::TIERED_PROMOTE_THRESHOLD + 10;
  for chunk_start in (4..=total).step_by(1000) {
    let chunk_end = (chunk_start + 999).min(total);
    let mut args: Vec<Vec<u8>> = Vec::with_capacity(chunk_end - chunk_start + 2);
    args.push(b"myset".to_vec());
    for i in chunk_start..=chunk_end {
      args.push(format!("m{i}").into_bytes());
    }
    let arg_slices: Vec<&[u8]> = args.iter().map(|v| v.as_slice()).collect();
    auto_exec(&api, &rt, &mut s, RespCommand::Sadd, &arg_slices);
  }

  // 3. 升阶后 SCARD O(1) 标量计数验证
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Scard, &[b"myset"]),
    format!(":{total}\r\n").as_bytes()
  );

  // 4. 分层态下的 SISMEMBER / SREM / SADD
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Sismember,
      &[b"myset", b"m1"]
    ),
    b":1\r\n"
  );
  let target_member = format!("m{}", total - 1);
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Sismember,
      &[b"myset", target_member.as_bytes()]
    ),
    b":1\r\n"
  );
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Sismember,
      &[b"myset", b"not_exist"]
    ),
    b":0\r\n"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Srem, &[b"myset", b"m1"]),
    b":1\r\n"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Scard, &[b"myset"]),
    format!(":{}\r\n", total - 1).as_bytes()
  );

  // 5. 校验底层 BfTree 存根存在
  let sess = store.new_session().unwrap();
  let stub_opt = rt.block_on(sess.load_collection_stub(b"myset")).unwrap();
  assert!(stub_opt.is_some(), "升阶后应存在 BfTree 存根");
  let (meta, _) = stub_opt.unwrap();
  assert_eq!(meta.size as usize, total - 1);
}

#[test]
fn test_zset_adaptive_tiering_and_o1_count() {
  let (rt, api, store, _dir) = open_env("test-adaptive-zset.db");
  let mut s = session_with(&api);

  // 1. 小集合内存态操作
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Zadd,
      &[b"myzset", b"1.5", b"m1", b"2.5", b"m2"]
    ),
    b":2\r\n"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zcard, &[b"myzset"]),
    b":2\r\n"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zscore, &[b"myzset", b"m1"]),
    b"$3\r\n1.5\r\n"
  );

  // 2. 批量写入跨越 TIERED_PROMOTE_THRESHOLD 门限触发自动升阶
  let total = wcol::TIERED_PROMOTE_THRESHOLD + 10;
  let mut buf = ItoaBuffer::new();
  for chunk_start in (3..=total).step_by(1000) {
    let chunk_end = (chunk_start + 999).min(total);
    let mut args: Vec<Vec<u8>> = Vec::with_capacity((chunk_end - chunk_start + 1) * 2 + 1);
    args.push(b"myzset".to_vec());
    for i in chunk_start..=chunk_end {
      args.push(buf.format(i).as_bytes().to_vec());
      args.push(format!("m{i}").into_bytes());
    }
    let arg_slices: Vec<&[u8]> = args.iter().map(|v| v.as_slice()).collect();
    auto_exec(&api, &rt, &mut s, RespCommand::Zadd, &arg_slices);
  }

  // 3. 升阶后 ZCARD O(1) 标量计数验证
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zcard, &[b"myzset"]),
    format!(":{total}\r\n").as_bytes()
  );

  // 4. 分层态下的 ZSCORE / ZINCRBY / ZREM
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Zscore, &[b"myzset", b"m1"]),
    b"$3\r\n1.5\r\n"
  );
  let target_member = format!("m{}", total - 1);
  let target_score_str = format!("{}", total - 1);
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Zscore,
      &[b"myzset", target_member.as_bytes()]
    ),
    format!("${}\r\n{}\r\n", target_score_str.len(), target_score_str).as_bytes()
  );
  let incr_res = auto_exec(
    &api,
    &rt,
    &mut s,
    RespCommand::Zincrby,
    &[b"myzset", b"10.5", target_member.as_bytes()],
  );
  let expected_score_str = format!("{}", (total - 1) as f64 + 10.5);
  assert_eq!(
    incr_res,
    format!("${}\r\n{expected_score_str}\r\n", expected_score_str.len()).as_bytes()
  );

  // 5. 校验底层 BfTree 存根存在
  let sess = store.new_session().unwrap();
  let stub_opt = rt.block_on(sess.load_collection_stub(b"myzset")).unwrap();
  assert!(stub_opt.is_some(), "升阶后应存在 BfTree 存根");
  let (meta, _) = stub_opt.unwrap();
  assert_eq!(meta.size as usize, total);
}

#[test]
fn test_list_adaptive_tiering_and_o1_count() {
  let (rt, api, store, _dir) = open_env("test-adaptive-list.db");
  let mut s = session_with(&api);

  // 1. 小集合内存态操作
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Rpush,
      &[b"mylist", b"v1", b"v2", b"v3"]
    ),
    b":3\r\n"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Llen, &[b"mylist"]),
    b":3\r\n"
  );

  // 2. 批量写入跨越 TIERED_PROMOTE_THRESHOLD 门限触发自动升阶
  let total = wcol::TIERED_PROMOTE_THRESHOLD + 10;
  let mut buf = ItoaBuffer::new();
  for chunk_start in (4..=total).step_by(1000) {
    let chunk_end = (chunk_start + 999).min(total);
    let mut args: Vec<Vec<u8>> = Vec::with_capacity(chunk_end - chunk_start + 2);
    args.push(b"mylist".to_vec());
    for i in chunk_start..=chunk_end {
      args.push(buf.format(i).as_bytes().to_vec());
    }
    let arg_slices: Vec<&[u8]> = args.iter().map(|v| v.as_slice()).collect();
    auto_exec(&api, &rt, &mut s, RespCommand::Rpush, &arg_slices);
  }

  // 3. 升阶后 LLEN O(1) 标量计数验证
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Llen, &[b"mylist"]),
    format!(":{total}\r\n").as_bytes()
  );

  // 4. 分层态下的 LPUSH / LPOP / RPOP
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Lpop, &[b"mylist"]),
    b"$2\r\nv1\r\n"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Llen, &[b"mylist"]),
    format!(":{}\r\n", total - 1).as_bytes()
  );

  // 5. 校验底层 BfTree 存根存在
  let sess = store.new_session().unwrap();
  let stub_opt = rt.block_on(sess.load_collection_stub(b"mylist")).unwrap();
  assert!(stub_opt.is_some(), "升阶后应存在 BfTree 存根");
  let (meta, _) = stub_opt.unwrap();
  assert_eq!(meta.size as usize, total - 1);
}

#[test]
fn test_hash_adaptive_tiering_by_memory_bytes() {
  let (rt, api, store, _dir) = open_env("test-adaptive-bytes.db");
  let mut s = session_with(&api);

  // 写入 8000 个 600B 字段（条目数 8000 < 65536，但总体积 ~5MB > 4MB TIERED_PROMOTE_BYTES）
  let val_bytes = vec![b'v'; 600];
  for chunk_start in (1..=8000).step_by(1000) {
    let chunk_end = (chunk_start + 999).min(8000);
    let mut args: Vec<Vec<u8>> = Vec::with_capacity((chunk_end - chunk_start + 1) * 2 + 1);
    args.push(b"big_hash".to_vec());
    for i in chunk_start..=chunk_end {
      args.push(format!("field_{i}").into_bytes());
      args.push(val_bytes.clone());
    }
    let arg_slices: Vec<&[u8]> = args.iter().map(|v| v.as_slice()).collect();
    auto_exec(&api, &rt, &mut s, RespCommand::Hset, &arg_slices);
  }

  // 1. 验证 O(1) 标量计数
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Hlen, &[b"big_hash"]),
    b":8000\r\n"
  );

  // 2. 校验底层 BfTree 存根已自动升阶生成
  let sess = store.new_session().unwrap();
  let stub_opt = rt.block_on(sess.load_collection_stub(b"big_hash")).unwrap();
  assert!(
    stub_opt.is_some(),
    "超过 4MB 内存体积应触发自动升阶为 BfTree"
  );
  let (meta, _) = stub_opt.unwrap();
  assert_eq!(meta.size as usize, 8000);

  // 3. 校验分层态下的点查正确性
  let read_res = auto_exec(
    &api,
    &rt,
    &mut s,
    RespCommand::Hget,
    &[b"big_hash", b"field_500"],
  );
  assert_eq!(
    read_res,
    format!("${}\r\n{}\r\n", val_bytes.len(), unsafe {
      from_utf8_unchecked(&val_bytes)
    })
    .as_bytes()
  );
}
