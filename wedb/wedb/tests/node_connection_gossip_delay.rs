#![recursion_limit = "256"]
//! gossip 建连等待面 gossip_delay 界集成测试（zcode-r24-gossip）
//!
//! SYN 黑洞形态（主机静默宕机、网络分区、防火墙丢包——恰是故障检测最需要
//! 工作的场景）下，initialize_async 的建连等待必须以 gossip_delay 为上界
//! 到点返回（对标 C# GarnetServerNode.InitializeAsync 的
//! `ReconnectAsync().WaitAsync(gossipDelay)`），不得以 facade 内层
//! cluster_node_timeout 缺省限时（60 秒）钉死等待面；超时放弃不回退
//! initialized 单次语义，重入走快速短路返回。
//!
//! 目标地址 203.0.113.1:7000（TEST-NET-3 官方保留网段，路由黑洞形态）；
//! 环境对保留网段立即报不可达时退化为快速失败分支，两分支契约同为到点
//! 返回且未连接。
//!
//! 同册附 facade 建连零限时钟测：建连限时严格由 timeout_ms 单值驱动，
//! 0 = 在途超时与建连限时同时关闭（与 C# TimeoutChecker 不启用、建连的
//! 毫秒时限非正即不限时臂同态），不得在旧 5 秒臆造兜底刻限时自败。

use std::{
  sync::atomic::Ordering,
  time::{Duration, Instant},
};

use compio::{
  buf::BufResult,
  io::{AsyncRead, AsyncWriteExt},
  net::TcpListener,
  runtime::{Runtime, spawn},
  time::{sleep, timeout},
};
use wedb::{
  client::GarnetClient,
  server::{cluster_provider::ClusterProvider, gossip::node_connection::NodeConnection},
};
use wtest_base::parse_frame_slices;

/// gossip_delay 测试值：远小于 facade 内层 cluster_node_timeout 缺省限时
/// （60 秒），保证断言上界（2 秒）对「界缺失」形态（等待逼近 60 秒）有区分度
const GOSSIP_DELAY: Duration = Duration::from_millis(200);

#[test]
fn initialize_async_bounded_by_gossip_delay() {
  let cp = ClusterProvider::new();
  let conn = NodeConnection::new(0xBEEF, "203.0.113.1".into(), 7000, &cp);

  Runtime::new().unwrap().block_on(async {
    let start = Instant::now();
    conn.initialize_async(GOSSIP_DELAY).await;
    let elapsed = start.elapsed();
    assert!(
      elapsed < Duration::from_secs(2),
      "建连等待应被 gossip_delay({GOSSIP_DELAY:?}) 界住，实测 {elapsed:?}"
    );
    assert!(!conn.is_connected(), "黑洞地址不得建连成功");
    assert!(
      conn.initialized.load(Ordering::Acquire),
      "超时放弃不回退 initialized 单次语义"
    );

    // 超时放弃后重入走 initialized 快速短路返回，不得再次建连等待
    let reentry = Instant::now();
    conn.initialize_async(GOSSIP_DELAY).await;
    assert!(
      reentry.elapsed() < GOSSIP_DELAY,
      "重入应经 initialized 快速短路返回，不得重复建连等待"
    );
  });
}

/// 慢握手靶端首帧应答扣停窗：明显越过已删除的旧 5 秒臆造兜底刻
const SLOW_HANDSHAKE_HOLD: Duration = Duration::from_secs(7);

/// 监视窗：仅作「不限时实现假死即限时红、绝不挂死 nextest」的收场兜底
const CONNECT_MONITOR_WINDOW: Duration = Duration::from_secs(30);

/// timeout_ms = 0 建连不限时钟测：靶端收下连接后把首帧握手应答扣停 7 秒
/// （facade 无凭证、clientName 在位即 SETINFO/SETNAME 两帧，逐帧一请求
/// 一应答），建连不得限时自败、须随握手收场成功且实测活过 5 秒刻——
/// 旧兜底形下本置于 5 秒即落 Timeout 臂必红
#[test]
fn zero_timeout_connect_survives_past_legacy_cap() {
  Runtime::new().unwrap().block_on(async {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap().to_string();
    spawn(async move {
      let Ok((mut sock, _)) = listener.accept().await else {
        return;
      };
      let mut acc: Vec<u8> = Vec::new();
      let mut buf = vec![0u8; 4096];
      let mut served = 0usize;
      loop {
        let BufResult(res, next) = sock.read(buf).await;
        buf = next;
        let Ok(n) = res else { return };
        if n == 0 {
          return;
        }
        acc.extend_from_slice(&buf[..n]);
        let mut consumed = 0;
        while let Some((frame_len, _)) = parse_frame_slices(&acc[consumed..]) {
          consumed += frame_len;
          if served == 0 {
            sleep(SLOW_HANDSHAKE_HOLD).await;
          }
          served += 1;
          let BufResult(res, _) = sock.write_all(b"+OK\r\n".to_vec()).await;
          if res.is_err() {
            return;
          }
        }
        if consumed > 0 {
          acc.drain(..consumed);
        }
        if served >= 2 {
          return;
        }
      }
    })
    .detach();

    let client = GarnetClient::with_config(endpoint, None, None, 0, None);
    let start = Instant::now();
    let res = timeout(CONNECT_MONITOR_WINDOW, client.connect_async()).await;
    let elapsed = start.elapsed();
    client.dispose();
    assert!(
      matches!(res, Ok(Ok(()))),
      "timeout_ms=0 建连应随慢握手在监视窗 ({CONNECT_MONITOR_WINDOW:?}) 内收场成功，实得 {res:?}"
    );
    assert!(
      elapsed > Duration::from_secs(5),
      "建连须活过旧 5 秒兜底刻（实测 {elapsed:?}），旧兜底形此刻即限时爆红"
    );
  });
}
