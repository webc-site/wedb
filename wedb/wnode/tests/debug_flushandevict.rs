//! DEBUG FLUSHANDEVICT 刷并驱逐主存储混合日志回归
//!
//! 对标 C# AdminCommands.cs:NetworkDebug 的 FLUSHANDEVICT 臂（mainStore.Log
//! .FlushAndEvict 刷全脏页并驱逐内存，head 推进至 tail；应答文案
//! "OK head={HeadAddress} tail={TailAddress}"）：命令经慢路径通道在存储
//! 执行域闭环（WedbStore::flush_and_evict_all），驱逐后数据仍可读（磁盘冷读）。

use std::sync::Arc;

use compio::{net::TcpStream, runtime::Runtime};
use tempfile::tempdir;
use wdev::SegmentedDevice;
use wnode::{
  RespSessionConsumer,
  resp::{
    garnet_api::StoreGarnetApi,
    resp_server_session::{ConnectionProtectionOption, RespServerSessionOptions},
  },
  service::StorageSessionProvider,
};
use wnode_test::{cmd, start_server};
use wtest_base::test_store_config;

/// 开启调试命令档的会话工厂（DEBUG 受 enable-debug-command 保护；Yes 档
/// 放行所有连接——Local 档依赖会话 remote_endpoint 接线，生产装配尚未
/// 注入端点，属既有缺口，不在本条范围）
fn debug_session_factory(
  network_sender_id: u64,
  api: StoreGarnetApi<SegmentedDevice>,
) -> Option<RespSessionConsumer> {
  Some(RespSessionConsumer::new(
    network_sender_id,
    RespServerSessionOptions {
      enable_debug_command: ConnectionProtectionOption::Yes,
      ..RespServerSessionOptions::default()
    },
    Arc::new(api),
  ))
}

/// SET 后 DEBUG FLUSHANDEVICT：应答复刻 C# 文案，head 推进至 tail，
/// 数据驱逐后仍可读（磁盘冷读）
#[test]
fn flushandevict_flushes_and_evicts_main_store() {
  let rt = Runtime::new().expect("compio runtime");
  let dir = tempdir().expect("tempdir");

  let provider = Arc::new(
    StorageSessionProvider::open_with_config(
      test_store_config(),
      dir.path().join("flushandevict.db"),
      debug_session_factory,
    )
    .expect("open"),
  );
  let (server, addr) = start_server(Arc::clone(&provider));

  rt.block_on(async {
    let mut stream = TcpStream::connect(addr).await.expect("connect");

    // 写入数据（内存驻留，head 落后 tail）
    assert_eq!(cmd(&mut stream, &[b"SET", b"k1", b"v1"]).await, b"+OK\r\n");
    let store = provider.store();
    let tail = store.tail_address();
    assert!(
      store.head_address() < tail,
      "SET 后数据应驻留内存（head({}) < tail({tail})）",
      store.head_address()
    );

    // DEBUG FLUSHANDEVICT：慢路径闭环，应答对齐 C# OK head/tail 文案
    let reply = cmd(&mut stream, &[b"DEBUG", b"FLUSHANDEVICT"]).await;
    let expect = format!("+OK head={} tail={tail}\r\n", store.head_address());
    assert_eq!(
      String::from_utf8_lossy(&reply),
      expect,
      "驱逐后 head 对齐 tail，应答复刻 C# 文案"
    );
    assert_eq!(
      store.head_address(),
      tail,
      "flush_and_evict 后 head({}) 必须对齐 tail({tail})",
      store.head_address()
    );

    // 驱逐后数据仍可读（读转磁盘冷读）
    assert_eq!(cmd(&mut stream, &[b"GET", b"k1"]).await, b"$2\r\nv1\r\n");

    cmd(&mut stream, &[b"QUIT"]).await;
  });
  server.stop();
}

/// 调试命令档关闭时 DEBUG FLUSHANDEVICT 同其余 DEBUG 臂拒绝（防误开）
#[test]
fn flushandevict_rejected_when_debug_disabled() {
  let rt = Runtime::new().expect("compio runtime");
  let dir = tempdir().expect("tempdir");

  let provider = Arc::new(
    StorageSessionProvider::open_with_config(
      test_store_config(),
      dir.path().join("flushandevict_off.db"),
      wnode_test::session_factory,
    )
    .expect("open"),
  );
  let (server, addr) = start_server(Arc::clone(&provider));

  rt.block_on(async {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let reply = cmd(&mut stream, &[b"DEBUG", b"FLUSHANDEVICT"]).await;
    assert_eq!(
      String::from_utf8_lossy(&reply),
      "-ERR DEBUG command not allowed. If the enable-debug-command option is set to \"local\", you can run it from a local connection, otherwise you need to set this option in the configuration file, and then restart the server.\r\n",
      "debug 保护档关闭拒绝 FLUSHANDEVICT"
    );
    cmd(&mut stream, &[b"QUIT"]).await;
  });
  server.stop();
}
