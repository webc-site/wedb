//! COMMITAOF 命令通道物理提交回归
//!
//! 对标 C# RespAdminCommandsTests 的 COMMITAOF 语义面（AdminCommands.cs:
//! NetworkCOMMITAOF → CommitAofAsync(dbId) → StoreWrapper.CommitAOFAsync →
//! SingleDatabaseManager.CommitToAofAsync → AppendOnlyFile.Log.CommitAsync）：
//! 命令经慢路径通道触发 AOF 物理刷盘，committed_until 推进至 safe_tail。
//!
//! 两形态：
//! 1. aof_commit_ms > 0（auto_commit 关）：SET 后提交位点不自动推进，
//!    COMMITAOF 显式提交推进 committed_until 至尾（手动提交 = 周期提交任务
//!    的触发内核，C# CommitTaskAsync 同款动作）；
//! 2. 缺省 aof_commit_ms = 0（auto_commit 开）：enqueue 同步触发提交，
//!    COMMITAOF 幂等（目标位点已覆盖则短路）；
//! 3. aof_commit_ms > 0 周期臂（C# AofUpsertStoreCommitTaskRecoverTestAsync 对标）：
//!    不手动执行 COMMITAOF，由后台周期提交任务自动将 committed_until 推进至尾，
//!    并在重启恢复后完整读回数据。

use std::{sync::Arc, time::Duration};

use compio::{net::TcpStream, runtime::Runtime, time::sleep};
use tempfile::tempdir;
use wconf::RuntimeServerOptions;
use wnode::service::StorageSessionProvider;
use wnode_test::{read_line_reply, send_cmd, session_factory, start_server};
use wtest_base::test_store_config;

/// aof_commit_ms=50（auto_commit 关）：COMMITAOF 显式推进 committed_until
#[test]
fn commitaof_advances_committed_until_when_auto_commit_off() {
  let rt = Runtime::new().expect("compio runtime");
  let dir = tempdir().expect("tempdir");
  let data_path = dir.path().join("node").join("commitaof_off.db");

  let provider = Arc::new(
    StorageSessionProvider::open_with_config_and_aof(
      test_store_config(),
      &data_path,
      None,
      RuntimeServerOptions {
        commit_frequency_ms: 50,
        ..RuntimeServerOptions::default()
      },
      session_factory,
    )
    .expect("open with aof"),
  );
  let (server, addr) = start_server(Arc::clone(&provider));

  rt.block_on(async {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    send_cmd(&mut stream, &[b"SET", b"k1", b"v1"])
      .await
      .expect("set");
    assert!(read_line_reply(&mut stream).await.starts_with(b"+OK"));

    let aof = provider.aof().expect("aof enabled");
    let tail = aof.log().tail_address().max();
    assert!(tail > 0, "SET 写监听端口须推进 AOF 尾");
    // auto_commit 关：入队不触发提交，位点停在尾之前
    let before = aof
      .log()
      .committed_until_address()
      .get(0)
      .expect("单物理日志");
    assert!(
      before < tail,
      "aof_commit_ms>0 形态 SET 后 committed_until({before}) 不应推进至尾({tail})"
    );

    // COMMITAOF 经慢路径通道物理刷盘（C# 无视结果恒回文案）
    send_cmd(&mut stream, &[b"COMMITAOF"])
      .await
      .expect("commitaof");
    assert_eq!(
      read_line_reply(&mut stream).await,
      b"+AOF file committed\r\n"
    );

    let after = aof
      .log()
      .committed_until_address()
      .get(0)
      .expect("单物理日志");
    assert!(
      after >= tail,
      "COMMITAOF 后 committed_until({after}) 须推进至尾({tail}) 及以上"
    );

    // 带 DBID 形态同款闭环（单 WAL 共享，提交动作与缺省一致）
    send_cmd(&mut stream, &[b"SET", b"k2", b"v2"])
      .await
      .expect("set k2");
    assert!(read_line_reply(&mut stream).await.starts_with(b"+OK"));
    let tail2 = aof.log().tail_address().max();
    send_cmd(&mut stream, &[b"COMMITAOF", b"0"])
      .await
      .expect("commitaof dbid");
    assert_eq!(
      read_line_reply(&mut stream).await,
      b"+AOF file committed\r\n"
    );
    let after2 = aof
      .log()
      .committed_until_address()
      .get(0)
      .expect("单物理日志");
    assert!(after2 >= tail2, "COMMITAOF 0 后提交位点须覆盖新尾 {tail2}");

    send_cmd(&mut stream, &[b"QUIT"]).await.expect("quit");
  });
  server.stop();
}

/// 缺省 auto_commit 开：enqueue 同步触发提交，COMMITAOF 幂等短路
#[test]
fn commitaof_idempotent_when_auto_commit_on() {
  let rt = Runtime::new().expect("compio runtime");
  let dir = tempdir().expect("tempdir");
  let data_path = dir.path().join("node").join("commitaof_on.db");

  let provider = Arc::new(
    StorageSessionProvider::open_with_config_and_aof(
      test_store_config(),
      &data_path,
      None,
      RuntimeServerOptions::default(),
      session_factory,
    )
    .expect("open with aof"),
  );
  let (server, addr) = start_server(Arc::clone(&provider));

  rt.block_on(async {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    send_cmd(&mut stream, &[b"SET", b"k1", b"v1"])
      .await
      .expect("set");
    assert!(read_line_reply(&mut stream).await.starts_with(b"+OK"));

    let aof = provider.aof().expect("aof enabled");
    // auto_commit 开：提交任务经 enqueue 同步派发，等待落定即覆盖当前尾
    aof.log().wait_for_commit_all_async(0).await.unwrap();
    let tail = aof.log().tail_address().max();
    let committed = aof
      .log()
      .committed_until_address()
      .get(0)
      .expect("单物理日志");
    assert!(
      committed >= tail,
      "auto_commit 形态提交位点({committed})须覆盖尾({tail})"
    );

    // COMMITAOF 幂等（目标已被覆盖 → commit_to 短路，应答文案不变）
    send_cmd(&mut stream, &[b"COMMITAOF"])
      .await
      .expect("commitaof");
    assert_eq!(
      read_line_reply(&mut stream).await,
      b"+AOF file committed\r\n"
    );
    assert_eq!(
      aof
        .log()
        .committed_until_address()
        .get(0)
        .expect("单物理日志"),
      committed
    );

    send_cmd(&mut stream, &[b"QUIT"]).await.expect("quit");
  });
  server.stop();
}

/// test/standalone/Garnet.test.scripting/RespAofTests.cs:AofUpsertStoreCommitTaskRecoverTestAsync
/// aof_commit_ms=50（周期提交臂）：无需手动 COMMITAOF，休眠等待周期触发自动推进 committed_until，并在重启后完整恢复
#[test]
fn periodic_commit_advances_committed_until_and_recovers() {
  let rt = Runtime::new().expect("compio runtime");
  let dir = tempdir().expect("tempdir");
  let data_path = dir.path().join("node").join("commitaof_periodic.db");

  let provider = Arc::new(
    StorageSessionProvider::open_with_config_and_aof(
      test_store_config(),
      &data_path,
      None,
      RuntimeServerOptions {
        commit_frequency_ms: 50,
        ..RuntimeServerOptions::default()
      },
      session_factory,
    )
    .expect("open with aof"),
  );
  let (server, addr) = start_server(Arc::clone(&provider));

  rt.block_on(async {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    send_cmd(&mut stream, &[b"SET", b"k_periodic", b"v_periodic"])
      .await
      .expect("set");
    assert!(read_line_reply(&mut stream).await.starts_with(b"+OK"));

    let aof = provider.aof().expect("aof enabled");
    let tail = aof.log().tail_address().max();
    assert!(tail > 0, "SET 写监听端口须推进 AOF 尾");

    // auto_commit 关：写入刚完成时，周期提交尚未到期，committed_until 停在尾前
    let before = aof
      .log()
      .committed_until_address()
      .get(0)
      .expect("单物理日志");
    assert!(
      before < tail,
      "aof_commit_ms=50 形态刚 SET 完 committed_until({before}) 不应立即推进至尾({tail})"
    );

    // 不发送 COMMITAOF，轮询等待周期提交任务（50ms 周期臂）自动触发并推进位点
    let mut committed = false;
    for _ in 0..50 {
      sleep(Duration::from_millis(20)).await;
      let cur = aof
        .log()
        .committed_until_address()
        .get(0)
        .expect("单物理日志");
      if cur >= tail {
        committed = true;
        break;
      }
    }
    assert!(
      committed,
      "周期提交后台任务应在设定间隔后自动推进 committed_until 至尾({tail})"
    );

    send_cmd(&mut stream, &[b"QUIT"]).await.expect("quit");
  });
  server.stop();
  drop(server);
  drop(provider);

  // 重启并重放 AOF（对标 C# AofUpsertStoreCommitTaskRecoverTestAsync 重启恢复断言）
  let provider2 = Arc::new(
    rt.block_on(StorageSessionProvider::open_recovered_with_config_and_aof(
      test_store_config(),
      &data_path,
      None,
      RuntimeServerOptions::default(),
      false,
      session_factory,
    ))
    .expect("open recovered with aof"),
  );
  let (server2, addr2) = start_server(Arc::clone(&provider2));
  rt.block_on(async {
    let mut stream = TcpStream::connect(addr2).await.expect("connect recovered");
    send_cmd(&mut stream, &[b"GET", b"k_periodic"])
      .await
      .expect("get");
    assert_eq!(read_line_reply(&mut stream).await, b"$10\r\nv_periodic\r\n");
    send_cmd(&mut stream, &[b"QUIT"]).await.expect("quit");
  });
  server2.stop();
}
