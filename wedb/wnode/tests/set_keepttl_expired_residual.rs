//! SET KEEPTTL 对惰性过期残留键的语义回归（对标 C# RMWMethods.cs:440-444
//! CheckExpiry→ExpireAndResume→InitialUpdater：过期旧记录判死后 InitialUpdater
//! 建新记录不带过期）。
//!
//! 缺陷面：KEEPTTL 经 ttl_of 裸读旧刻度不过期滤，惰性过期残留（GC 未物理
//! 清除）的旧刻度被回填给新值——+OK 已答而新值携带已过期 TTL，GET/TTL/EXISTS
//! 全判缺失，写入被确认后立即不可见（数据丢失向 TTL 语义错）。
//! 修复契约：过期残留刻度不回填，新值无 TTL 存活（TTL -1）。

use std::{net, net::SocketAddr, sync::Arc};

use compio::{net::TcpStream, runtime::Runtime};
use tempfile::tempdir;
use wnode_test::{cmd, open_aof_provider, start_server};
use wtest_base::{resp_frame, wait_until};

/// 同步 GET 探针（wait_until! 轮询的到期可观测量）：应答首行 `$-1` 即
/// 惰性判死 nil；连接/读写失败按未到期继续轮询（每轮独立连接，无残留态）
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
fn set_keepttl_on_expired_residual_writes_alive_value() {
  let rt = Runtime::new().expect("compio runtime");
  let dir = tempdir().expect("tempdir");
  let data_path = dir.path().join("node").join("keepttl_expired.db");
  let provider = open_aof_provider(&data_path);
  let (server, addr) = start_server(Arc::clone(&provider));

  // 预置 1 秒 TTL 键，随后轮询至其自然过期（惰性过期残留态：不做物理清除触发）
  rt.block_on(async {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    assert_eq!(
      cmd(&mut stream, &[b"SET", b"k", b"v1", b"EX", b"1"]).await,
      b"+OK\r\n"
    );
    assert_eq!(cmd(&mut stream, &[b"TTL", b"k"]).await, b":1\r\n");
  });

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

    // KEEPTTL 重写：不得回填死刻度
    assert_eq!(
      cmd(&mut stream, &[b"SET", b"k", b"v2", b"KEEPTTL"]).await,
      b"+OK\r\n",
      "KEEPTTL 重写须确认成功"
    );
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

    // 重写后键暖活无 TTL，仍走快臂（快臂滤二次覆盖）；慢臂冷残留形态
    // （RecordOnDisk 降级）由 slow.rs 同款滤承接，契约同源不另造页压场景
    assert_eq!(
      cmd(&mut stream, &[b"SET", b"k", b"v3", b"KEEPTTL"]).await,
      b"+OK\r\n"
    );
    assert_eq!(cmd(&mut stream, &[b"GET", b"k"]).await, b"$2\r\nv3\r\n");
    assert_eq!(cmd(&mut stream, &[b"TTL", b"k"]).await, b":-1\r\n");
  });
  server.stop();
}
