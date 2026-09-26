//! GET 族读漏斗「已过期未清退对象键」恒缺失形锁测（deviations §133，零行为改动纯增量锁）
//!
//! 在 garnet 中的相对路径:
//! libs/server/Storage/Functions/MainStore/ReadMethods.cs:Reader（:31 ValueIsObject
//! 判型先行、:37 CheckExpiry 后至）——C# 对已过期未清退对象键回 -WRONGTYPE，其
//! UnifiedStore/ReadMethods.cs:22 反序（CheckExpiry 先行）。rust 采 UnifiedStore
//! Reader 同侧：过期先于判型，GET nil / GETRANGE 空 bulk / STRLEN :0。
//!
//! 对照形态（C# -WRONGTYPE）以本注释备查（仿 §113 夹具族「对照形态注释备查」
//! 形制），严禁按主存 Reader 序回改判型先行。

use std::sync::Arc;

use compio::runtime::Runtime;
use wbase::time::now_ticks;
use wnode::{
  RespSessionConsumer,
  resp::{
    garnet_api::{GarnetApi, StoreGarnetApi},
    resp_server_session::RespServerSessionOptions,
    slow_path::SlowWait,
  },
  storage::session::common::ttl_sync::put_ttl_sync,
};
use wnode_test::roundtrip;
use wresp::command::RespCommand;
use wtest_base::open_test_store;

/// 慢臂直驱 RESP2 帧（与快臂同版）
const RESP_V2: u8 = 2;

/// 慢臂直驱（与降级快照投递同径，不经会话快路径）
fn slow_direct(rt: &Runtime, api: &GarnetApi, cmd: RespCommand, args: &[&[u8]]) -> Vec<u8> {
  let snapshot: Vec<Vec<u8>> = args.iter().map(|a| a.to_vec()).collect();
  rt.block_on(async {
    SlowWait::for_command(api, cmd, snapshot, RESP_V2)
      .resolve()
      .await
  })
}

/// 已过期未清退对象键（SADD 入簿 + put_ttl_sync 过去刻度，不经 EXPIRE 的
/// Redis 7.4 过去即删语义）：GET/GETRANGE/STRLEN 恒缺失形，快慢双臂逐字节全等
#[test]
fn expired_object_key_get_family_answers_missing_shape() {
  let (_dir, store) = open_test_store("expired-object-get-funnel.db").unwrap();
  let ttl_session = store.new_session().unwrap();
  let batch = ttl_session.enter_batch();
  let api: GarnetApi = Arc::new(StoreGarnetApi::new(store.new_session().unwrap()));
  let rt = Runtime::new().unwrap();
  let mut c = RespSessionConsumer::new(1, RespServerSessionOptions::default(), api.clone());

  // 对象键入簿
  assert_eq!(roundtrip(&rt, &mut c, &[b"SADD", b"k", b"a"]), b":1\r\n");
  // 过去刻度 TTL 直写：信封记录在簿、TTL 门恒 Due，「已过期未清退」形
  put_ttl_sync(&batch, b"k", now_ticks().saturating_sub(1)).unwrap();

  // 快臂三读：GET nil / GETRANGE 空 bulk / STRLEN :0
  // （C# 主存 Reader 形为 -WRONGTYPE，对照注释备查，见文件头）
  assert_eq!(roundtrip(&rt, &mut c, &[b"GET", b"k"]), b"$-1\r\n");
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"GETRANGE", b"k", b"0", b"-1"]),
    b"$0\r\n\r\n"
  );
  assert_eq!(roundtrip(&rt, &mut c, &[b"STRLEN", b"k"]), b":0\r\n");

  // 慢臂同帧：逐字节全等
  assert_eq!(
    slow_direct(&rt, &api, RespCommand::Get, &[b"k"]),
    b"$-1\r\n"
  );
  assert_eq!(
    slow_direct(&rt, &api, RespCommand::Getrange, &[b"k", b"0", b"-1"]),
    b"$0\r\n\r\n"
  );
  assert_eq!(
    slow_direct(&rt, &api, RespCommand::Strlen, &[b"k"]),
    b":0\r\n"
  );
}
