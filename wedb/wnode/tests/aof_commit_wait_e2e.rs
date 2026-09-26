//! WAIT-FOR-COMMIT 持久性档端到端回归（真实 `StorageSessionProvider` + AOF）
//!
//! 对标 C# RespServerSession.cs:1453 `Send` 内 `if (waitForAofBlocking)` →
//! `storeWrapper.WaitForCommitAsync()`（rust 读点在 `net/handler/drive.rs`
//! 出网写出段，等待经 `SessionProviderFace::wait_for_commit_async` 下达
//! `IDatabaseManager::wait_for_commit_to_aof_async`）：
//!
//! 1. 档位开启（enable_aof && wait_for_commit，解析期 latch）：AOF 相关命令
//!    的应答必在该记录提交落盘之后才出网；
//! 2. 档位关闭（缺省）：应答先行，位点由周期提交任务后补推进——与 1 同
//!    骨架对照，令「应答返回即已刷盘」这条断言真正可证伪。
//!
//! 提交节拍取 `aof_commit_ms = 50`（auto_commit 关 + 周期臂），刚写完时位点
//! 必停在尾前（commitaof.rs 既有口径）。

use std::{sync::Arc, time::Duration};

use compio::{net::TcpStream, runtime::Runtime, time::sleep};
use tempfile::tempdir;
use wconf::RuntimeServerOptions;
use wdev::SegmentedDevice;
use wnode::{
  RespSessionConsumer,
  aof::GarnetAppendOnlyFile,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
  service::StorageSessionProvider,
};
use wnode_test::{read_line_reply, send_cmd, session_factory, start_server};
use wtest_base::test_store_config;

/// WAIT-FOR-COMMIT 档会话工厂（装配期 `--aof --aof-commit-wait` 的会话侧投影
/// 形态：`RespServerSessionOptions::from(&NodeArgs)` 令 enable_aof 与
/// wait_for_commit 同真，会话门 `aof_commit_mode_gate` 自此开启）
fn wait_commit_session(
  network_sender_id: u64,
  api: StoreGarnetApi<SegmentedDevice>,
) -> Option<RespSessionConsumer> {
  Some(RespSessionConsumer::new(
    network_sender_id,
    RespServerSessionOptions {
      enable_aof: true,
      wait_for_commit: true,
      ..RespServerSessionOptions::default()
    },
    Arc::new(api),
  ))
}

/// 提交位点读数（单物理日志槽 0）
fn committed(aof: &GarnetAppendOnlyFile) -> i64 {
  aof
    .log()
    .committed_until_address()
    .get(0)
    .expect("单物理日志")
}

/// 档位开启：+OK 出网前已等 AOF 提交落盘，应答返回时位点必覆盖该记录尾
#[test]
fn wait_for_commit_reply_follows_flush() {
  let rt = Runtime::new().expect("compio runtime");
  let dir = tempdir().expect("tempdir");
  let data_path = dir.path().join("node").join("wait_commit_on.db");

  let provider = Arc::new(
    StorageSessionProvider::open_with_config_and_aof(
      test_store_config(),
      &data_path,
      None,
      RuntimeServerOptions {
        commit_frequency_ms: 50,
        ..RuntimeServerOptions::default()
      },
      wait_commit_session,
    )
    .expect("open with aof"),
  );
  let (server, addr) = start_server(Arc::clone(&provider));

  rt.block_on(async {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    send_cmd(&mut stream, &[b"SET", b"k_wait", b"v_wait"])
      .await
      .expect("set");
    assert!(read_line_reply(&mut stream).await.starts_with(b"+OK"));

    // 应答已返回 → 出网前置等待已把该记录提交落盘（周期臂节拍尚未到期，
    // 位点推进只可能来自等待本身）
    let aof = provider.aof().expect("aof enabled");
    let tail = aof.log().tail_address().max();
    assert!(tail > 0, "SET 写监听端口须推进 AOF 尾");
    let after_reply = committed(aof);
    assert!(
      after_reply >= tail,
      "档位开启时应答返回即须已提交：committed({after_reply}) 应覆盖尾({tail})"
    );

    send_cmd(&mut stream, &[b"QUIT"]).await.expect("quit");
  });
  server.stop();
}

/// 档位关闭（缺省对照）：应答先行于提交，位点在应答返回时仍停在尾前，
/// 其后由周期提交臂补推进（同骨架反证上一条断言出自出网前置等待）
#[test]
fn without_wait_for_commit_reply_precedes_flush() {
  let rt = Runtime::new().expect("compio runtime");
  let dir = tempdir().expect("tempdir");
  let data_path = dir.path().join("node").join("wait_commit_off.db");

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
    send_cmd(&mut stream, &[b"SET", b"k_nowait", b"v_nowait"])
      .await
      .expect("set");
    assert!(read_line_reply(&mut stream).await.starts_with(b"+OK"));

    let aof = provider.aof().expect("aof enabled");
    let tail = aof.log().tail_address().max();
    assert!(tail > 0, "SET 写监听端口须推进 AOF 尾");
    let after_reply = committed(aof);
    assert!(
      after_reply < tail,
      "门关时出网零等待：应答返回时 committed({after_reply}) 应仍停在尾({tail}) 前"
    );

    // 周期提交臂随后补推进（数据不丢，只是应答不等它）
    let mut flushed = false;
    for _ in 0..50 {
      sleep(Duration::from_millis(20)).await;
      if committed(aof) >= tail {
        flushed = true;
        break;
      }
    }
    assert!(flushed, "周期提交任务应推进 committed_until 至尾({tail})");

    send_cmd(&mut stream, &[b"QUIT"]).await.expect("quit");
  });
  server.stop();
}
