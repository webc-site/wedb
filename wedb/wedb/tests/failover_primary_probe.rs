//! 主端 failover 位点探测竞速缺陷用例（票 wfailover-primary-probe-replica-race-and-abort-omission）：
//! 对标 Garnet PrimaryFailoverSession.cs WaitForFirstReplicaSyncAsync 的
//! 「等首个追平副本」契约——离线/建连失败副本的空应答不得误杀整体故障转移，
//! 探测期 FAILOVER ABORT 须即刻打断且不触发从节点接管
use std::{
  net::SocketAddr,
  str::from_utf8,
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
  time::{Duration, Instant},
};

use aok::Void;
use compio::{
  buf::BufResult,
  io::{AsyncRead, AsyncWriteExt},
  net::{TcpListener, TcpStream},
  runtime::{Runtime, spawn},
};
use waof::AofAddress;
use wedb::server::{
  cluster_provider::ClusterProvider,
  failover::{failover_manager::FailoverManager, failover_option::FailoverOption},
  worker::{LocalWorkerSpec, NodeRole, Worker},
};
use wresp::resp_memory_writer::write_bulk_string_to;
use wtest_base::{FailoverNode, wait_for};

/// 装配主节点拓扑：primary_node（本地）挂两个候选副本探测靶端。靶端 worker
/// role 取 Primary 供 get_local_node_primary_endpoints 收集端点、
/// replica_of_node_id 登记供 get_replica_ids/try_stop_writes 走通
fn primary_provider(replica_a_port: u16, replica_b_port: u16) -> Arc<ClusterProvider> {
  let cp = ClusterProvider::new();
  let m = cp.cluster_manager().expect("cluster manager 在场");
  {
    let mut config = m.current_config.write();
    config.initialize_local_worker(LocalWorkerSpec {
      node_id: 0x0000_0000_0000_0000_0000_0000_0000_DE11,
      address: "127.0.0.1",
      port: 7001,
      config_epoch: 3,
      role: NodeRole::Primary,
      replica_of_node_id: None,
      hostname: None,
    });
    for (node_id, port) in [(0x5107u128, replica_a_port), (0xFA57u128, replica_b_port)] {
      config.workers.push(Worker {
        nodeid: Some(node_id),
        address: "127.0.0.1".into(),
        port: port as i32,
        config_epoch: 3,
        role: NodeRole::Primary,
        replica_of_node_id: Some(0x0000_0000_0000_0000_0000_0000_0000_DE11),
        replication_offset: 0,
        hostname: None,
      });
    }
  }
  cp
}

/// 本地位点应答载荷（靶端以此应答 FAILREPLICATIONOFFSET 即视为位点追平）
fn local_offset_reply() -> Arc<String> {
  let cp = ClusterProvider::new();
  Arc::new(
    cp.replication_manager()
      .expect("replication manager 在场")
      .get_current_replication_offset()
      .to_aof_string(),
  )
}

/// 取一个确信无人监听的端口（bind 后立即释放，回环连接必遭拒绝）
async fn closed_port() -> u16 {
  let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
  let port = l.local_addr().unwrap().port();
  drop(l);
  // 让端口从 TIME_WAIT/监听表彻底摘除的窗口由重试的即时拒绝保证：回环
  // 无监听端口连接在毫秒内返回 refused
  port
}

/// 多副本探测跳空应答取首个追平副本（对标 WaitForFirstReplicaSyncAsync
/// 契约语义：等待首个追平 ReplicationOffset 的健康从节点）：离线副本
///（端口无监听，建连失败快速回空应答）排先，正常同步副本经服务端延迟
/// 应答追平位点在后。单次读首包实现必被空应答抢先误杀；循环消费实现跳过
/// 空应答、由同步副本完成接管
#[test]
fn primary_probe_skips_offline_replica_and_takes_synced_one() -> Void {
  Runtime::new()?.block_on(async {
    let offset_reply = local_offset_reply();
    let offline = closed_port().await;
    // 正常副本延迟 400ms 应答：确保离线副本的空应答必然抢先送达
    let synced = FailoverNode::bind(offset_reply, Duration::from_millis(400)).await;
    let cp = primary_provider(offline, synced.port());
    // 探测限时 3s：正常副本 400ms 应答在限时内，空应答不占限时
    cp.set_cluster_node_timeout_ms(3000);

    let m = Arc::new(FailoverManager::new(cp));
    let start = Instant::now();
    assert!(m.try_start_primary_failover(
      "",
      -1,
      FailoverOption::Takeover,
      Duration::from_secs(10)
    ));
    m.wait_failover_done().await;
    let elapsed = start.elapsed();

    assert!(
      synced.takeover_received(),
      "离线副本抢先空应答不得误杀故障转移：位点追平的同步副本应当选接管"
    );
    assert_eq!(m.get_failover_status(), "no-failover");
    assert!(
      elapsed < Duration::from_secs(5),
      "跳过空应答后应即刻等至同步副本应答，不应拖挂 failover_timeout: {elapsed:?}"
    );
    aok::OK
  })
}

/// 探测期 ABORT 即刻打断且不触发接管（对标 C# WaitAsync 的 cts.Token
/// 取消半边）：双副本均可在 30s 后应答追平位点、探测限时无限（0 哨兵），
/// 探测等待只能由中止信号或整体超时唤醒。会话进入 waiting-for-sync 后
/// 下发 abort——须在毫秒级收场、状态归位且两个候选副本均不被下发
/// CLUSTER FAILOVER TAKEOVER
#[test]
fn primary_probe_abort_interrupts_wait_without_takeover() -> Void {
  Runtime::new()?.block_on(async {
    let offset_reply = local_offset_reply();
    // 30s 慢应答：旧实现（recv 不与 race_abort 竞速、abort 只 dispose
    // 连接不打断在途请求）须拖满应答/超时才收场，且应答匹配后仍下发接管
    let slow_a = FailoverNode::bind(Arc::clone(&offset_reply), Duration::from_secs(30)).await;
    let slow_b = FailoverNode::bind(offset_reply, Duration::from_secs(30)).await;
    let cp = primary_provider(slow_a.port(), slow_b.port());
    // 0 = 无限（C# Timeout.InfiniteTimeSpan 哨兵）：单探测不会限时自落
    cp.set_cluster_node_timeout_ms(0);

    let m = Arc::new(FailoverManager::new(cp));
    assert!(m.try_start_primary_failover(
      "",
      -1,
      FailoverOption::Takeover,
      Duration::from_secs(60)
    ));
    assert!(
      wait_for(
        || m.get_failover_status() == "waiting-for-sync",
        Duration::from_secs(5)
      )
      .await,
      "会话应进入 waiting-for-sync 探测等待"
    );

    let start = Instant::now();
    m.try_abort_replica_failover();
    m.wait_failover_done().await;
    let elapsed = start.elapsed();

    assert!(
      elapsed < Duration::from_secs(5),
      "abort 应即刻打断在途位点探测等待，而非拖满副本应答/整体超时: {elapsed:?}"
    );
    assert!(
      !slow_a.takeover_received() && !slow_b.takeover_received(),
      "中止后不得向任何候选副本下发 TAKEOVER（双主脑裂防线）"
    );
    assert_eq!(m.get_failover_status(), "no-failover");
    aok::OK
  })
}

/// stall 接管靶端：握手与 `CLUSTER FAILREPLICATIONOFFSET` 即刻正常应答
///（bulk 位点），对 `CLUSTER FAILOVER` 记录已收请求标记但全程不回应答
///（探测必胜出、在途 takeover 应答只由中止面收口的受控对端）
struct StallTakeoverNode {
  addr: SocketAddr,
  takeover: Arc<AtomicBool>,
}

impl StallTakeoverNode {
  /// 绑定随机端口并启动 accept 循环；offset_reply 为位点应答 bulk 载荷
  async fn bind(offset_reply: Arc<String>) -> Self {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let takeover = Arc::new(AtomicBool::new(false));
    let loop_takeover = Arc::clone(&takeover);
    spawn(async move {
      while let Ok((stream, _)) = listener.accept().await {
        let (offset_reply, takeover) = (Arc::clone(&offset_reply), Arc::clone(&loop_takeover));
        spawn(async move { stall_serve(stream, offset_reply, takeover).await }).detach();
      }
    })
    .detach();
    Self { addr, takeover }
  }

  /// 假端点监听端口
  fn port(&self) -> u16 {
    self.addr.port()
  }

  /// 是否已收到 CLUSTER FAILOVER 接管请求帧（应答被扣发）
  fn takeover_requested(&self) -> bool {
    self.takeover.load(Ordering::Acquire)
  }
}

/// 从缓冲解析一个完整 RESP2 数组帧，返回（帧总字节数，各 bulk 载荷）；
/// 不完整返回 None（口径同 wtest_base 私有帧解析器）
fn parse_frame(buf: &[u8]) -> Option<(usize, Vec<Vec<u8>>)> {
  if buf.first() != Some(&b'*') {
    return None;
  }
  let header_end = buf.iter().position(|b| *b == b'\n')? + 1;
  let argc: usize = from_utf8(&buf[1..header_end - 2]).ok()?.parse().ok()?;
  let mut pos = header_end;
  let mut payloads = Vec::with_capacity(argc);
  for _ in 0..argc {
    if buf.get(pos) != Some(&b'$') {
      return None;
    }
    let len_line_end = buf[pos + 1..].iter().position(|b| *b == b'\n')? + pos + 2;
    let len: usize = from_utf8(&buf[pos + 1..len_line_end - 2])
      .ok()?
      .parse()
      .ok()?;
    let payload_start = len_line_end;
    let payload_end = payload_start + len;
    if payload_end + 2 > buf.len() {
      return None;
    }
    payloads.push(buf[payload_start..payload_end].to_vec());
    pos = payload_end + 2;
  }
  Some((pos, payloads))
}

/// stall 服务循环：FAILREPLICATIONOFFSET 即刻回 bulk 位点、FAILOVER 记
/// 标记后扣发应答，其余帧回 `+OK`（一请求一应答节奏）
async fn stall_serve(mut stream: TcpStream, offset_reply: Arc<String>, takeover: Arc<AtomicBool>) {
  let mut acc: Vec<u8> = Vec::new();
  let mut buf = vec![0u8; 8192];
  loop {
    let BufResult(res, next) = stream.read(buf).await;
    buf = next;
    let n = match res {
      Ok(n) if n > 0 => n,
      _ => break,
    };
    acc.extend_from_slice(&buf[..n]);
    while let Some((frame_len, payloads)) = parse_frame(&acc) {
      acc.drain(..frame_len);
      let is_cmd = |name: &str| {
        payloads.len() >= 2 && payloads[0] == b"CLUSTER" && payloads[1] == name.as_bytes()
      };
      if is_cmd("FAILOVER") {
        // 请求已达：记标记后扣发应答，令 takeover 停在在途等待
        takeover.store(true, Ordering::Release);
        continue;
      }
      let resp = if is_cmd("FAILREPLICATIONOFFSET") {
        // 请求载荷线形锁：真实主端发带 1 字节长度前缀二进制
        // （waof AofAddress::to_aof_binary，对标 C# GarnetClientExtensions.cs:61
        // ToByteArray），解码失败即回错误帧——靶端不校验形则发收交点被掩蔽
        let payload_ok = payloads
          .get(2)
          .is_some_and(|p| AofAddress::from_aof_binary(p).is_some());
        if !payload_ok {
          b"-ERR invalid failreplicationoffset payload\r\n".to_vec()
        } else {
          let mut out = Vec::with_capacity(offset_reply.len() + 16);
          write_bulk_string_to(&mut out, offset_reply.as_bytes());
          out
        }
      } else {
        b"+OK\r\n".to_vec()
      };
      if stream.write_all(resp).await.is_err() {
        return;
      }
    }
  }
}

/// 接管请求在途期中止即刻收口（对标 C# InitiateReplicaTakeOverAsync 的
/// WaitAsync(clusterTimeout, cts.Token) 取消半边）：候选副本位点即刻追平
/// 当选、TAKEOVER 请求已达靶端但应答被扣发，探测限时无限、接管限时 10s
/// 挂起在途应答。会话进入 taking-over-as-primary 后下发 abort——须毫秒级
/// 收场而非挂满 10s 接管限时
#[test]
fn primary_takeover_inflight_abort_interrupts_wait() -> Void {
  Runtime::new()?.block_on(async {
    let node = StallTakeoverNode::bind(local_offset_reply()).await;
    let cp = primary_provider(node.port(), node.port());
    // 探测限时无限（0 哨兵 = C# InfiniteTimeSpan）：本用例只裁决 takeover
    // 在途臂；接管应答扣发，旧实现只能由 10s cluster_timeout 单方收口
    cp.set_cluster_node_timeout_ms(10000);

    let m = Arc::new(FailoverManager::new(cp));
    assert!(m.try_start_primary_failover(
      "",
      -1,
      FailoverOption::Takeover,
      Duration::from_secs(60)
    ));
    assert!(
      wait_for(
        || m.get_failover_status() == "taking-over-as-primary",
        Duration::from_secs(5)
      )
      .await,
      "会话应进入 taking-over-as-primary 接管下发阶段"
    );
    assert!(
      wait_for(|| node.takeover_requested(), Duration::from_secs(5)).await,
      "TAKEOVER 请求应已送达候选副本（C# 同口径：请求先于 WaitAsync 发出）"
    );

    let start = Instant::now();
    m.try_abort_replica_failover();
    m.wait_failover_done().await;
    let elapsed = start.elapsed();

    assert!(
      elapsed < Duration::from_secs(5),
      "abort 应即刻打断在途 takeover 应答等待，而非挂满 10s cluster_timeout: {elapsed:?}"
    );
    assert_eq!(m.get_failover_status(), "no-failover");
    aok::OK
  })
}
