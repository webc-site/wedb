//! 空闲读等待期 pubsub 推送字节计入 ConsumerEntry 出向镜像回归
//! （task/ing/wnode-drive-loop-pubsub-push-bytes-mirror-omission）
//!
//! 缺陷：C# 任何出网字节统一经 RespServerSession.cs:1462 `Send` 唯一出向
//! 记账点累计 `sessionMetrics.incr_total_net_output_bytes`（订阅推送同轨，
//! PubSubCommands.cs:Publish → Send/SendAndReset）；rust 侧 drive 循环消费段
//! 的 `written` 累计与收尾 `add_net_bytes` 镜像覆盖不到读等待段
//! PushOutcome::Push 臂直写出的推送帧——订阅会话长周期空闲等推送，消费段
//! 永不重临，条目 `net_output_bytes`（monitor_sample 的
//! total_net_output_bytes 直读源）对推送流量永久漏记，INFO STATS 与监视器
//! 瞬时吞吐失真。修复：推送臂写出成功后捕获实发字节数，就地
//! `entry.add_net_bytes(0, push_bytes)` 并同步 `mirror_session_counters`。
//!
//! 测试对标：C# 无同名镜像单测（会话指标原地累计无镜像盲区），按票面验证
//! 点构建真链路 SUBSCRIBE/PUBLISH 回环（装配对标
//! garnet/test/standalone/Garnet.test/RespPubSubTests.cs BasicSUBSCRIBE），
//! 断言空闲订阅连接（不发送任何新命令）的条目镜像出向字节与实发数据量严格
//! 一致，并经 registry.monitor_sample() 采出对应 total_net_output_bytes。

use std::{mem::forget, net::SocketAddr, num::NonZeroUsize, sync::Arc, time::Duration};

use compio::{
  BufResult,
  io::{AsyncRead, AsyncWriteExt},
  net::TcpStream,
  runtime::Runtime,
  time::sleep,
};
use wbase::pool::DEFAULT_BUFFER_SIZE;
use wnode::{
  GarnetServer, servers::consumer_registry::ConsumerRegistry, service::StorageSessionProvider,
};
use wnode_test::{SessionFactory, session_factory};
use wtest_base::{resp_frame, test_store_config};

const QUIT_FRAME: &[u8] = b"*1\r\n$4\r\nQUIT\r\n";

/// 起单 worker 真存储服务器，返回消费者注册表句柄（真发布订阅中枢默认装配，
/// 对标 burst_shrink_buffer_pool::spawn_server）
fn spawn_server() -> (
  GarnetServer<StorageSessionProvider<SessionFactory>>,
  SocketAddr,
  Arc<ConsumerRegistry>,
) {
  let dir = tempfile::tempdir().unwrap();
  let data_path = dir.path().join("push-net-bytes.db");
  let factory: SessionFactory = session_factory;
  let provider = Arc::new(
    StorageSessionProvider::open_with_config(test_store_config(), data_path, factory).unwrap(),
  );
  let registry = Arc::clone(&provider.registry);
  let server = GarnetServer::new(
    &["127.0.0.1:0".to_string()],
    DEFAULT_BUFFER_SIZE,
    8,
    provider,
  )
  .unwrap();
  server.start(NonZeroUsize::new(1)).unwrap();
  let addr = server.local_addr().unwrap();
  // dir 随句柄存活至测试结束（服务器全生命周期数据文件在场）
  forget(dir);
  (server, addr, registry)
}

/// RESP2 `message` 推送帧字节数：*3\r\n$7\r\nmessage\r\n + channel + payload
fn push_frame_len(channel: &[u8], payload: &[u8]) -> usize {
  format!("*3\r\n$7\r\nmessage\r\n${}\r\n", channel.len()).len()
    + channel.len()
    + 2
    + format!("${}\r\n", payload.len()).len()
    + payload.len()
    + 2
}

/// 期望字节数读完（累计，容忍 TCP 分段）
async fn read_until(stream: &mut TcpStream, acc: &mut Vec<u8>, expect: usize) {
  let mut chunk = vec![0u8; 8192];
  while acc.len() < expect {
    let BufResult(res, ret) = stream.read(chunk).await;
    chunk = ret;
    let n = res.expect("对端提前关闭");
    assert!(n > 0, "对端提前关闭（已收 {} 字节）", acc.len());
    acc.extend_from_slice(&chunk[..n]);
  }
}

async fn connect(addr: SocketAddr) -> TcpStream {
  TcpStream::connect(addr).await.unwrap()
}

/// 空闲订阅连接收推送（含超池基准大帧），不再发送任何新命令：条目镜像出向
/// 字节与实发数据量严格一致（修复前：推送臂漏调 add_net_bytes，条目出向恒为
/// SUBSCRIBE 应答字节，monitor_sample 采出的 total_net_output_bytes 严重失真）
#[test]
fn idle_push_bytes_mirrored_into_entry_net_output() {
  let (server, addr, registry) = spawn_server();

  let channel = b"ch";
  let sub_frame = resp_frame(&[b"SUBSCRIBE", channel]);
  let sub_ack = b"*3\r\n$9\r\nsubscribe\r\n$2\r\nch\r\n:1\r\n";
  // 多条小推送 + 一条超池基准规格（64KB）的大推送：覆盖常规臂与大帧扩容臂
  let payloads: Vec<Vec<u8>> = vec![
    b"msg-1".to_vec(),
    b"msg-2".to_vec(),
    vec![b'p'; 96 * 1024],
    b"msg-4".to_vec(),
  ];
  let push_total: usize = payloads
    .iter()
    .map(|p| push_frame_len(channel, p))
    .sum::<usize>();

  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    // 订阅连接：SUBSCRIBE 后进入空闲读挂起，全程零新命令
    let mut sub = connect(addr).await;
    sub.write_all(sub_frame.clone()).await.unwrap();
    let mut sub_acc = Vec::new();
    read_until(&mut sub, &mut sub_acc, sub_ack.len()).await;
    assert_eq!(&sub_acc, sub_ack);

    // 发布连接：逐条 PUBLISH，推送帧经读等待段双路等待直写订阅连接
    let mut publisher = connect(addr).await;
    for payload in &payloads {
      publisher
        .write_all(resp_frame(&[b"PUBLISH", channel, payload]))
        .await
        .unwrap();
      let mut pub_acc = Vec::new();
      read_until(&mut publisher, &mut pub_acc, b":1\r\n".len()).await;
      assert_eq!(&pub_acc, b":1\r\n");
    }
    // 订阅端收齐全部推送帧（ack 之后追加）
    read_until(&mut sub, &mut sub_acc, sub_ack.len() + push_total).await;
    assert_eq!(
      &sub_acc[sub_ack.len()..sub_ack.len() + 6],
      b"*3\r\n$7".as_slice()
    );

    // 发布连接先退场：条目注销后 monitor_sample 仅剩空闲订阅条目
    publisher.write_all(QUIT_FRAME.to_vec()).await.unwrap();
    let mut pub_ok = Vec::new();
    read_until(&mut publisher, &mut pub_ok, b"+OK\r\n".len()).await;
    drop(publisher);
    // 有界轮询至发布连接收场注销（dispose→条目摘除异步链，5s 上界）
    for _ in 0..1000 {
      if registry.monitor_sample().sessions.len() == 1 {
        break;
      }
      sleep(Duration::from_millis(5)).await;
    }

    let sample = registry.monitor_sample();
    assert_eq!(
      sample.sessions.len(),
      1,
      "仅剩空闲订阅连接（推送期间未发任何新命令）"
    );
    let metrics = &sample.sessions[0].metrics;
    // 出向镜像 = SUBSCRIBE 应答 + 全部推送帧实发字节，严格一致
    assert_eq!(
      metrics.total_net_output_bytes,
      (sub_ack.len() + push_total) as u64,
      "空闲等待期推送字节必须计入条目出向镜像（C# Send 唯一记账点口径）"
    );
    // 入向镜像 = 唯一一条 SUBSCRIBE 命令帧（推送不引入入向字节）
    assert_eq!(metrics.total_net_input_bytes, sub_frame.len() as u64);

    // 订阅连接退场收口
    sub.write_all(QUIT_FRAME.to_vec()).await.unwrap();
    let sub_final = sub_acc.len() + b"+OK\r\n".len();
    read_until(&mut sub, &mut sub_acc, sub_final).await;
    drop(sub);
    // 有界轮询至全连接收场注销（5s 上界）
    for _ in 0..1000 {
      if registry.connection_totals().2 == 0 {
        break;
      }
      sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(registry.connection_totals().2, 0, "全连接收场注销");
  });

  server.stop();
}
