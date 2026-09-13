//! 端到端集成测试：FLUSHDB / FLUSHALL / SWAPDB / HCOLLECT `*` / ZCOLLECT `*`
//! 慢路径真执行
//!
//! 对标 garnet/test/standalone/Garnet.test 的 FlushTests / MultiDatabaseTests
//! （FlushDatabase、SwapDatabase）与 RespHashTests 的 HCOLLECT 用例族：
//! 命令同步段仅校验参数，清库 / 跨库交换 / 全库对象收集经
//! [`wnode::resp::slow_path::SlowWait`] 挂起后异步闭环
use std::sync::Arc;

use compio::runtime::Runtime;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
};

/// 装配带真存储执行域的会话消费者（每测试独立临时目录，GC 关闭）
fn consumer() -> RespSessionConsumer {
  let dir = tempfile::tempdir().unwrap().keep();
  let device = Arc::new(SegmentedDevice::single_file(dir.join("admin.db")).unwrap());
  let mut config = StoreConfig::new(16384, 65536, 64, 0.5).unwrap();
  config.gc.enabled = false;
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  let session = store.new_session().unwrap();
  RespSessionConsumer::new(
    1,
    RespServerSessionOptions {
      allow_multi_db: true,
      ..RespServerSessionOptions::default()
    },
    Arc::new(StoreGarnetApi::new(session)),
  )
}

/// 单命令往返（同步快路径）
fn roundtrip(c: &mut RespSessionConsumer, frame: &[u8]) -> Vec<u8> {
  let (consumed, out) = c.try_consume_messages(frame);
  assert_eq!(consumed, frame.len(), "帧应被完整消费: {frame:?}");
  out
}

/// 慢命令往返：同步段消费（挂起不产输出）→ 网络泵 await 慢路径 →
/// 应答按流水线顺序写回（此处 block_on 承担网络泵角色）
fn slow_roundtrip(rt: &Runtime, c: &mut RespSessionConsumer, frame: &[u8]) -> Vec<u8> {
  let (consumed, mut out) = c.try_consume_messages(frame);
  assert_eq!(consumed, frame.len(), "帧应被完整消费: {frame:?}");
  let Some(slow) = c.take_slow_wait() else {
    return out;
  };
  rt.block_on(async {
    out.extend_from_slice(&slow.resolve().await);
  });
  out
}

/// SET 若干键
fn set_keys(c: &mut RespSessionConsumer, keys: &[&str]) {
  for k in keys {
    assert_eq!(
      roundtrip(
        c,
        format!("*3\r\n$3\r\nSET\r\n${}\r\n{k}\r\n$1\r\nv\r\n", k.len()).as_bytes()
      ),
      b"+OK\r\n"
    );
  }
}

/// FLUSHDB / FLUSHALL：清库闭环 + 语法校验
#[test]
fn flushdb_and_flushall_clear_database() {
  let rt = Runtime::new().unwrap();
  let mut c = consumer();
  set_keys(&mut c, &["a", "b"]);
  assert_eq!(
    roundtrip(
      &mut c,
      b"*4\r\n$4\r\nHSET\r\n$2\r\nh1\r\n$1\r\nf\r\n$1\r\nv\r\n"
    ),
    b":1\r\n"
  );

  // 语法错误同步段快答（ASYNC 与 SYNC 互斥 / 未知 token）
  assert_eq!(
    roundtrip(
      &mut c,
      b"*3\r\n$7\r\nFLUSHDB\r\n$5\r\nASYNC\r\n$4\r\nSYNC\r\n"
    ),
    b"-ERR syntax error\r\n"
  );
  assert_eq!(
    roundtrip(&mut c, b"*2\r\n$7\r\nFLUSHDB\r\n$3\r\nfoo\r\n"),
    b"-ERR syntax error\r\n"
  );

  // FLUSHDB SYNC → +OK，字符串与集合键全清（Meta 键走完整删除）
  assert_eq!(
    slow_roundtrip(&rt, &mut c, b"*2\r\n$7\r\nFLUSHDB\r\n$4\r\nSYNC\r\n"),
    b"+OK\r\n"
  );
  assert_eq!(
    slow_roundtrip(&rt, &mut c, b"*1\r\n$6\r\nDBSIZE\r\n"),
    b":0\r\n"
  );
  assert_eq!(
    roundtrip(&mut c, b"*2\r\n$3\r\nGET\r\n$1\r\na\r\n"),
    b"$-1\r\n"
  );

  // 重建数据后 FLUSHALL（单库模型同径）→ +OK 清库
  set_keys(&mut c, &["x"]);
  assert_eq!(
    slow_roundtrip(&rt, &mut c, b"*2\r\n$8\r\nFLUSHALL\r\n$5\r\nASYNC\r\n"),
    b"+OK\r\n"
  );
  assert_eq!(
    slow_roundtrip(&rt, &mut c, b"*1\r\n$6\r\nDBSIZE\r\n"),
    b":0\r\n"
  );

  // UNSAFETRUNCATELOG 破坏性截断路径走通（清库 + 物理段截断）
  set_keys(&mut c, &["t"]);
  assert_eq!(
    slow_roundtrip(
      &rt,
      &mut c,
      b"*2\r\n$7\r\nFLUSHDB\r\n$17\r\nUNSAFETRUNCATELOG\r\n"
    ),
    b"+OK\r\n"
  );
  assert_eq!(
    slow_roundtrip(&rt, &mut c, b"*1\r\n$6\r\nDBSIZE\r\n"),
    b":0\r\n"
  );
}

/// SWAPDB：跨库键值交换（含 TTL 随键迁移）与同库短路
#[test]
fn swapdb_exchanges_databases() {
  let rt = Runtime::new().unwrap();
  let mut c = consumer();
  set_keys(&mut c, &["k1"]);
  // db0 键带 TTL（PX 100s）
  assert_eq!(
    roundtrip(
      &mut c,
      b"*4\r\n$6\r\nPSETEX\r\n$2\r\nk1\r\n$6\r\n100000\r\n$2\r\nv1\r\n"
    ),
    b"+OK\r\n"
  );
  // db1 建键
  assert_eq!(
    roundtrip(&mut c, b"*2\r\n$6\r\nSELECT\r\n$1\r\n1\r\n"),
    b"+OK\r\n"
  );
  set_keys(&mut c, &["k2"]);
  assert_eq!(
    roundtrip(&mut c, b"*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n"),
    b"+OK\r\n"
  );

  // 同库交换短路 +OK（无搬移语义）
  assert_eq!(
    slow_roundtrip(&rt, &mut c, b"*3\r\n$6\r\nSWAPDB\r\n$1\r\n0\r\n$1\r\n0\r\n"),
    b"+OK\r\n"
  );

  // SWAPDB 0 1（慢路径真交换）→ +OK
  assert_eq!(
    slow_roundtrip(&rt, &mut c, b"*3\r\n$6\r\nSWAPDB\r\n$1\r\n0\r\n$1\r\n1\r\n"),
    b"+OK\r\n"
  );

  // db1 现持有原 db0 数据（值与 TTL 均随迁）
  assert_eq!(
    roundtrip(&mut c, b"*2\r\n$6\r\nSELECT\r\n$1\r\n1\r\n"),
    b"+OK\r\n"
  );
  assert_eq!(
    roundtrip(&mut c, b"*2\r\n$3\r\nGET\r\n$2\r\nk1\r\n"),
    b"$2\r\nv1\r\n"
  );
  let ttl = slow_roundtrip(&rt, &mut c, b"*2\r\n$3\r\nTTL\r\n$2\r\nk1\r\n");
  let ttl_str = String::from_utf8_lossy(&ttl);
  assert!(
    ttl_str.starts_with(':') && !ttl_str.starts_with(":-"),
    "TTL 应为正剩余毫秒: {ttl_str}"
  );
  assert_eq!(
    roundtrip(&mut c, b"*2\r\n$3\r\nGET\r\n$2\r\nk2\r\n"),
    b"$-1\r\n",
    "原 db1 键应已搬离"
  );

  // db0 现持有原 db1 数据
  assert_eq!(
    roundtrip(&mut c, b"*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n"),
    b"+OK\r\n"
  );
  assert_eq!(
    roundtrip(&mut c, b"*2\r\n$3\r\nGET\r\n$2\r\nk2\r\n"),
    b"$1\r\nv\r\n"
  );
  assert_eq!(
    roundtrip(&mut c, b"*2\r\n$3\r\nGET\r\n$2\r\nk1\r\n"),
    b"$-1\r\n",
    "原 db0 键应已搬离"
  );
}

/// HCOLLECT `*` / ZCOLLECT `*`：全库对象收集闭环
#[test]
fn hcollect_and_zcollect_star_full_db() {
  let rt = Runtime::new().unwrap();
  let mut c = consumer();
  // 两个 hash 键 + 一个 zset 键 + 干扰字符串键
  assert_eq!(
    roundtrip(
      &mut c,
      b"*4\r\n$4\r\nHSET\r\n$2\r\nha\r\n$1\r\nf\r\n$1\r\nv\r\n"
    ),
    b":1\r\n"
  );
  assert_eq!(
    roundtrip(
      &mut c,
      b"*4\r\n$4\r\nHSET\r\n$2\r\nhb\r\n$1\r\ng\r\n$1\r\nw\r\n"
    ),
    b":1\r\n"
  );
  assert_eq!(
    roundtrip(
      &mut c,
      b"*4\r\n$4\r\nZADD\r\n$2\r\nza\r\n$1\r\n1\r\n$1\r\nm\r\n"
    ),
    b":1\r\n"
  );
  set_keys(&mut c, &["plain"]);

  // HCOLLECT * → +OK（收集后对象仍完整可读）
  assert_eq!(
    slow_roundtrip(&rt, &mut c, b"*2\r\n$8\r\nHCOLLECT\r\n$1\r\n*\r\n"),
    b"+OK\r\n"
  );
  assert_eq!(
    roundtrip(&mut c, b"*3\r\n$4\r\nHGET\r\n$2\r\nha\r\n$1\r\nf\r\n"),
    b"$1\r\nv\r\n"
  );

  // ZCOLLECT * → +OK
  assert_eq!(
    slow_roundtrip(&rt, &mut c, b"*2\r\n$8\r\nZCOLLECT\r\n$1\r\n*\r\n"),
    b"+OK\r\n"
  );
  assert_eq!(
    roundtrip(&mut c, b"*3\r\n$6\r\nZSCORE\r\n$2\r\nza\r\n$1\r\nm\r\n"),
    b"$1\r\n1\r\n"
  );

  // 收集后清库态可再收集（幂等）
  assert_eq!(
    slow_roundtrip(&rt, &mut c, b"*2\r\n$8\r\nHCOLLECT\r\n$1\r\n*\r\n"),
    b"+OK\r\n"
  );
}
