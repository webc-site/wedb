//! 停机链集合项经纪收口端到端（票 zcode-r23-broker 发现二）
//!
//! 对标 C# StoreWrapper.Dispose 的 `itemBroker?.Dispose()`
//!（libs/server/StoreWrapper.cs:916）：stop() 须解除全部挂起阻塞会话（收
//! 空应答或按 drive.rs 三路竞速的终止广播裁决断连，两种均为既定形态），
//! 置经纪取消位——主循环随 join 排空退出，不再依赖 worker 硬杀；经纪专属
//! 存储会话（独立纪元参与者）不消亡于 enter_batch 纪元临界段内。

use std::{
  io::{Read, Write},
  net::TcpStream,
  sync::Arc,
  thread,
  time::Duration,
};

use tempfile::tempdir;
use wconf::RuntimeServerOptions;
use wnode::service::StorageSessionProvider;
use wnode_test::{session_factory, start_server};
use wtest_base::test_store_config;

/// stop() 经 dispose_item_broker 收口：挂起 BLPOP 会话在停机序内解除
///（连接有限时间内关闭），经纪 is_disposed 置真
#[test]
fn stop_disposes_broker_and_releases_blocked_session() {
  let dir = tempdir().expect("tempdir");
  let data_path = dir.path().join("node").join("broker_stop.db");
  let provider = Arc::new(
    StorageSessionProvider::open_with_config_and_aof(
      test_store_config(),
      &data_path,
      None,
      RuntimeServerOptions::default(),
      session_factory,
    )
    .expect("open provider"),
  );
  let (server, addr) = start_server(Arc::clone(&provider));

  let mut stream = TcpStream::connect(addr).expect("connect");
  stream
    .set_read_timeout(Some(Duration::from_secs(10)))
    .expect("read timeout");
  // BLPOP k 0：无限等待挂起（观察者登记进经纪等待队列）
  stream
    .write_all(b"*3\r\n$5\r\nBLPOP\r\n$1\r\nk\r\n$1\r\n0\r\n")
    .expect("send blpop");
  // 等待挂起落位（命令到达 + 观察者登记）
  thread::sleep(Duration::from_millis(300));

  // 停机：dispose_item_broker 解除挂起观察者、唤醒经纪主循环退出，紧随
  // 其后的 join 即排空屏障——stop() 须在有限时间内返回
  server.stop();

  // 挂起会话在停机序内解除：连接收到 BLPOP 空应答（经纪 force_unblock
  // 胜出）或按终止广播裁决直接关闭（read 返回即断言未依赖硬杀挂死）
  let mut buf = [0u8; 16];
  let n = stream.read(&mut buf).expect("停机后连接应解除而非挂死");
  assert!(
    n == 0 || &buf[..n] == b"*-1\r\n",
    "挂起会话应收空应答或断连，实际 {n} 字节: {:?}",
    &buf[..n]
  );

  // 经纪收口置位（发现二核心断言；旧形态 stop() 链无此步恒为假）
  assert!(
    provider.item_broker().is_disposed(),
    "stop() 须置经纪取消位"
  );
}

/// 未有阻塞等待时 stop() 照常收口（dispose 幂等面：空经纪直通 join 排空）
#[test]
fn stop_with_no_waiters_is_noop_dispose() {
  let dir = tempdir().expect("tempdir");
  let data_path = dir.path().join("node").join("broker_stop_idle.db");
  let provider = Arc::new(
    StorageSessionProvider::open_with_config_and_aof(
      test_store_config(),
      &data_path,
      None,
      RuntimeServerOptions::default(),
      session_factory,
    )
    .expect("open provider"),
  );
  let (server, _addr) = start_server(Arc::clone(&provider));

  server.stop();
  assert!(provider.item_broker().is_disposed());
}
