//! 端到端集成测试：会话核心 → 存储执行域（RespSessionConsumer +
//! StoreGarnetApi 装配），对标 garnet/test/standalone/Garnet.test/RespTests.cs
//! 的 GET/SET/DEL/EXPIRE 用例族——命令经完整会话主循环（解析 → 门控 →
//! GarnetApi 分派 → 存储落盘 → 应答回写）而非直调命令方法
use std::sync::Arc;

use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
};
use wtest_base::test_store_config;

/// 装配带真存储执行域的会话消费者（每测试独立临时目录，GC 关闭）
fn consumer() -> RespSessionConsumer {
  let dir = tempfile::tempdir().unwrap().keep();
  let device = Arc::new(SegmentedDevice::single_file(dir.join("e2e.db")).unwrap());
  // 小预算测试配置（对标 C# 16MB 基线），GC 关闭保持历史语义
  let config = test_store_config();
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  let session = store.new_session().unwrap();
  RespSessionConsumer::new(
    1,
    RespServerSessionOptions::default(),
    Arc::new(StoreGarnetApi::new(session)),
  )
}

/// 单命令往返
fn roundtrip(consumer: &mut RespSessionConsumer, frame: &[u8]) -> Vec<u8> {
  let (consumed, out) = pump(consumer, frame);
  assert_eq!(consumed, Some(0), "帧应被完整消费: {frame:?}");
  out
}

/// GET/SET/DEL/EXPIRE/TTL 核心数据命令闭环
/// 泵等价消费（直填会话接收缓冲 → 唯一入口 → 应答取出）
/// 返回 (消费后残余, 应答)：Some(0) = 完整消费，None = 协议违规
fn pump(consumer: &mut RespSessionConsumer, frame: &[u8]) -> (Option<usize>, Vec<u8>) {
  let mut scratch = consumer.take_recv_scratch();
  scratch.extend_from_slice(frame);
  consumer.return_recv_scratch(scratch);
  let mut resp = Vec::new();
  let remaining = consumer.try_consume_messages_into(&mut resp);
  (remaining, resp)
}

#[test]
fn core_data_commands_execute_in_store_domain() {
  let mut c = consumer();

  // SET k v → +OK
  assert_eq!(
    roundtrip(&mut c, b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n"),
    b"+OK\r\n"
  );
  // GET k → $1 v
  assert_eq!(
    roundtrip(&mut c, b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"),
    b"$1\r\nv\r\n"
  );
  // SETEX k 10 v2 → +OK；TTL k ∈ (0,10]
  assert_eq!(
    roundtrip(
      &mut c,
      b"*4\r\n$5\r\nSETEX\r\n$1\r\nk\r\n$2\r\n10\r\n$2\r\nv2\r\n"
    ),
    b"+OK\r\n"
  );
  let out = roundtrip(&mut c, b"*2\r\n$3\r\nTTL\r\n$1\r\nk\r\n");
  let secs: i64 = String::from_utf8_lossy(&out)
    .trim()
    .trim_start_matches(':')
    .parse()
    .unwrap();
  assert!((1..=10).contains(&secs), "TTL 应在有效区间: {secs}");
  // TTL missing → -2；TTL k（持久）→ -1；EXPIRE 延长
  assert_eq!(
    roundtrip(&mut c, b"*2\r\n$3\r\nTTL\r\n$7\r\nmissing\r\n"),
    b":-2\r\n"
  );
  // DEL k → :1；GET k → $-1
  assert_eq!(
    roundtrip(&mut c, b"*2\r\n$3\r\nDEL\r\n$1\r\nk\r\n"),
    b":1\r\n"
  );
  assert_eq!(
    roundtrip(&mut c, b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"),
    b"$-1\r\n"
  );
}

/// 多键数组族与键管理族（MSET/MGET/EXISTS/RENAME）
#[test]
fn array_and_key_admin_commands_execute() {
  let mut c = consumer();

  // MSET a 1 b 2 → +OK
  assert_eq!(
    roundtrip(
      &mut c,
      b"*5\r\n$4\r\nMSET\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n"
    ),
    b"+OK\r\n"
  );
  // MGET a b missing → [1, 2, nil]
  assert_eq!(
    roundtrip(
      &mut c,
      b"*4\r\n$4\r\nMGET\r\n$1\r\na\r\n$1\r\nb\r\n$7\r\nmissing\r\n"
    ),
    b"*3\r\n$1\r\n1\r\n$1\r\n2\r\n$-1\r\n"
  );
  // EXISTS a b → :2
  assert_eq!(
    roundtrip(&mut c, b"*3\r\n$6\r\nEXISTS\r\n$1\r\na\r\n$1\r\nb\r\n"),
    b":2\r\n"
  );
  // RENAME a c → +OK；GET a → nil；GET c → 1
  assert_eq!(
    roundtrip(&mut c, b"*3\r\n$6\r\nRENAME\r\n$1\r\na\r\n$1\r\nc\r\n"),
    b"+OK\r\n"
  );
  assert_eq!(
    roundtrip(&mut c, b"*2\r\n$3\r\nGET\r\n$1\r\na\r\n"),
    b"$-1\r\n"
  );
  assert_eq!(
    roundtrip(&mut c, b"*2\r\n$3\r\nGET\r\n$1\r\nc\r\n"),
    b"$1\r\n1\r\n"
  );
  // TYPE b → +string；STRLEN b → :1
  assert_eq!(
    roundtrip(&mut c, b"*2\r\n$4\r\nTYPE\r\n$1\r\nb\r\n"),
    b"+string\r\n"
  );
  assert_eq!(
    roundtrip(&mut c, b"*2\r\n$6\r\nSTRLEN\r\n$1\r\nb\r\n"),
    b":1\r\n"
  );
}

/// 同帧管道多命令：逐条执行逐条应答（游标推进与输出聚合）
#[test]
fn pipeline_frames_all_executed() {
  let mut c = consumer();
  let frame = b"\
    *3\r\n$3\r\nSET\r\n$2\r\np1\r\n$2\r\nv1\r\n\
    *3\r\n$3\r\nSET\r\n$2\r\np2\r\n$2\r\nv2\r\n\
    *2\r\n$3\r\nGET\r\n$2\r\np1\r\n\
    *2\r\n$3\r\nGET\r\n$2\r\np2\r\n";
  let (consumed, out) = pump(&mut c, frame);
  assert_eq!(consumed, Some(0));
  assert_eq!(out, b"+OK\r\n+OK\r\n$2\r\nv1\r\n$2\r\nv2\r\n");
}

/// INCR 族与 GETSET（读改写闭环）
#[test]
fn incr_family_executes() {
  let mut c = consumer();
  assert_eq!(
    roundtrip(&mut c, b"*2\r\n$4\r\nINCR\r\n$3\r\ncnt\r\n"),
    b":1\r\n"
  );
  assert_eq!(
    roundtrip(&mut c, b"*3\r\n$6\r\nINCRBY\r\n$3\r\ncnt\r\n$2\r\n10\r\n"),
    b":11\r\n"
  );
  assert_eq!(
    roundtrip(&mut c, b"*2\r\n$4\r\nDECR\r\n$3\r\ncnt\r\n"),
    b":10\r\n"
  );
  // GETSET cnt 0 → 旧值 10
  assert_eq!(
    roundtrip(&mut c, b"*3\r\n$6\r\nGETSET\r\n$3\r\ncnt\r\n$1\r\n0\r\n"),
    b"$2\r\n10\r\n"
  );
}

/// 未接入分派表的命令明确报错（对标 C# ProcessAdminCommands 兜底
/// RESP_ERR_GENERIC_UNK_CMD），绝不静默吞命令；已接入族（LPUSH/HSET 等）
/// 由 resp_objects_dispatch.rs 覆盖真执行
#[test]
fn unhooked_command_reports_unknown() {
  let mut c = consumer();
  // WATCH 属事务慢命令族，未入同步分派表
  assert_eq!(
    roundtrip(&mut c, b"*2\r\n$5\r\nWATCH\r\n$1\r\nk\r\n"),
    b"-ERR unknown command\r\n"
  );
  // VectorSET 族依赖 VectorManager 装配域，未入分派表
  assert_eq!(
    roundtrip(&mut c, b"*2\r\n$4\r\nVADD\r\n$1\r\nk\r\n"),
    b"-ERR unknown command\r\n"
  );
}

/// 参数校验仍由命令层承接（错误应答逐字节对标 C#）
#[test]
fn arity_errors_surface() {
  let mut c = consumer();
  assert_eq!(
    roundtrip(&mut c, b"*1\r\n$3\r\nGET\r\n"),
    b"-ERR wrong number of arguments for 'GET' command\r\n"
  );
  assert_eq!(
    roundtrip(&mut c, b"*3\r\n$6\r\nEXPIRE\r\n$1\r\nk\r\n$2\r\nxx\r\n"),
    b"-ERR value is not an integer or out of range.\r\n"
  );
}
