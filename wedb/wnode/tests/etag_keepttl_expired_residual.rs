#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! SETIFMATCH「保留旧 TTL」臂对惰性过期残留刻度的语义回归（对标 C#
//! RMWMethods CopyUpdate TrySetExpiration(arg1 != 0 ? arg1 :
//! srcRecord.Expiration)：expiry 缺省即回填旧刻度）。
//!
//! 缺陷面：与 SET 族 KEEPTTL 六点同型的第七消费面——expiry==0 臂经
//! ttl_of/ttl_of_sync 裸读旧刻度不过期滤，惰性过期残留（GC 未物理清除）的
//! 旧刻度被回填给新值——写入被确认而新值携带已过期 TTL，GET/TTL/EXISTS
//! 全判缺失（数据丢失向 TTL 语义错）。
//!
//! 修复契约（端态钉）：过期残留键上的 SETIFMATCH（expiry 缺省）落新值须
//! 存活（GET 命中）且无 TTL（TTL -1），绝不因回填死刻度落地即死。快臂滤
//! 二次覆盖；慢臂冷残留形态（RecordOnDisk 降级）由 slow.rs 同款滤承接，
//! 契约同源不另造页压场景。可达面注记：存活裁决与回填读取之间的跨期微窗
//! （本修复的真命中面）系相邻同步操作间竞态，端到端确定性构造不可达——
//! 本测试钉同族可确定复现的残留终态（初写臂与保留臂共用同一过滤出口）。
//!
//! SETIFMATCH 的 etag 参数域 ≥0 且无 etag 记录的键恒判失配，须以
//! SETWITHETAG 预置真 etag（过期残留后 etag 旁路记录仍在，SETIFMATCH 按
//! 该记录条件判定）。

use core::str::from_utf8;
use std::{net, net::SocketAddr, sync::Arc};

use compio::{net::TcpStream, runtime::Runtime};
use tempfile::tempdir;
use wnode_test::{cmd, open_aof_provider, start_server};
use wtest_base::{resp_frame, wait_until};

/// 同步 GET 探针（wait_until! 轮询的到期可观测量）：应答首行 `$-1` 即
/// 惰性判死 nil；连接/读写失败按未到期继续轮询（每轮独立连接，无残留态，
/// 不触 etag 旁路记录）
fn get_reply_nil_probe(addr: SocketAddr, key: &[u8]) -> bool {
  use std::io::{Read, Write};
  let Ok(mut s) = net::TcpStream::connect(addr) else {
    return false;
  };
  if s.write_all(&resp_frame(&[b"GET" as &[u8], key])).is_err() {
    return false;
  }
  let mut line = Vec::with_capacity(32);
  let mut byte = [0u8; 1];
  loop {
    match s.read(&mut byte) {
      Ok(0) | Err(_) => return false,
      Ok(_) => {
        line.push(byte[0]);
        if line.ends_with(b"\r\n") {
          return line.starts_with(b"$-1");
        }
      }
    }
  }
}

#[test]
fn setifmatch_keep_ttl_on_expired_residual_writes_alive_value() {
  let rt = Runtime::new().expect("compio runtime");
  let dir = tempdir().expect("tempdir");
  let data_path = dir.path().join("node").join("etag_keepttl_expired.db");
  let provider = open_aof_provider(&data_path);
  let (server, addr) = start_server(Arc::clone(&provider));

  // 预置带 etag 的 1 秒 TTL 键（SETWITHETAG 回新 etag 整数帧），随后轮询至
  // 其自然过期（惰性过期残留态：不做物理清除触发）
  let setup = rt.block_on(async {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let setup = cmd(&mut stream, &[b"SETWITHETAG", b"k", b"v1", b"EX", b"1"]).await;
    assert!(
      setup.starts_with(b":"),
      "SETWITHETAG 回新 etag 整数帧: {setup:?}"
    );
    assert_eq!(cmd(&mut stream, &[b"TTL", b"k"]).await, b":1\r\n");
    setup
  });
  let etag = setup[1..setup.len() - 2].to_vec();

  rt.block_on(async move {
    // 轮询到期可观测量：GET → nil（替换固定 1300ms 睡眠，超时即测试失败）
    wait_until!(
      get_reply_nil_probe(addr, b"k"),
      timeout: 5s,
      step: 20ms,
      "键 k 须在超时窗口内自然过期（GET → nil）"
    );
    let mut stream = TcpStream::connect(addr).await.expect("connect2");
    // 惰性判死确认（残留记录仍在盘上，读路径判过期）
    assert_eq!(cmd(&mut stream, &[b"GET", b"k"]).await, b"$-1\r\n");

    // SETIFMATCH（expiry 缺省 = 保留旧 TTL 臂）：条件按残留 etag 记录判定；
    // 回帧形态随命中/初写臂分叉不钉，存活性终态由 GET/TTL 承载
    let etag_arg = etag.clone();
    let _ = cmd(&mut stream, &[b"SETIFMATCH", b"k", b"v2", &etag_arg]).await;
    assert_eq!(
      cmd(&mut stream, &[b"GET", b"k"]).await,
      b"$2\r\nv2\r\n",
      "新值须存活（不得因回填死刻度落地即死）"
    );
    assert_eq!(
      cmd(&mut stream, &[b"TTL", b"k"]).await,
      b":-1\r\n",
      "过期残留刻度不回填，新值无 TTL"
    );

    // 暖活键二次 SETIFMATCH（快臂滤二次覆盖）；etag 随写递增，按 GETWITHETAG
    // 现值重新对齐条件基线
    let gw = cmd(&mut stream, &[b"GETWITHETAG", b"k"]).await;
    let text = from_utf8(&gw).expect("utf8 回帧");
    let cur: i64 = text
      .strip_prefix("*2\r\n:")
      .unwrap_or_else(|| panic!("etag 整数帧在首元素: {text}"))
      .split("\r\n")
      .next()
      .expect("etag 行")
      .parse()
      .expect("etag 数值");
    let cur_arg = cur.to_string().into_bytes();
    let _ = cmd(&mut stream, &[b"SETIFMATCH", b"k", b"v3", &cur_arg]).await;
    assert_eq!(cmd(&mut stream, &[b"GET", b"k"]).await, b"$2\r\nv3\r\n");
    assert_eq!(cmd(&mut stream, &[b"TTL", b"k"]).await, b":-1\r\n");
  });
  server.stop();
}
