//! 突发大应答/大推送的低水位容量收敛回归
//! （task/ing/wnode-burst-shrink-drains-buffer-pool-and-pubsub-bloat）
//!
//! 缺陷一（重借蚕食池队列）：命令应答超池基准规格扩容后，写出段曾以
//! `buffer_pool.get_ref(基准规格)` 重新借出复位——旧扩容块析构迁移至高级
//! 层级或就地弃置，新块弹自基准层级，每遇大应答池闲置配额即被蚕食，突发
//! 流量下基准层级迅速排空、热路径退化为堆分配抖动。C# 原型响应缓冲恒为
//! sendBufferSize（GarnetTcpNetworkSender.cs:210 SendResponse 分片写出 +
//! RespServerSession.cs:1461 SendAndReset 复位游标），从不因大应答扩容
//! 换块。修复：写出完成后就地收缩回基准规格，连接全程仅占用建连借出的
//! 单一配额。
//!
//! 缺陷二（推送臂缺失收缩）：订阅推送写出收尾曾无低水位收敛，长连接发送
//! 缓冲永久驻留峰值容量（订阅会话常驻等待推送，膨胀无法回落）。修复：
//! 推送臂与命令臂同一收敛。
//!
//! 测试对标：发送缓冲恒定规格为 C# 协议内建行为，无同名 C# 单测，按票面
//! 验证点构建池计数回归；订阅回环对标
//! garnet/test/standalone/Garnet.test/RespPubSubTests.cs 的
//! SUBSCRIBE/PUBLISH 装配（BasicSUBSCRIBE）。

use std::{mem::forget, net::SocketAddr, num::NonZeroUsize, sync::Arc, time::Duration};

use compio::{
  BufResult,
  io::{AsyncRead, AsyncWriteExt},
  net::TcpStream,
  runtime::Runtime,
  time::sleep,
};
use wbase::pool::{DEFAULT_BUFFER_SIZE, LimitedFixedBufferPool};
use wnode::{GarnetServer, service::StorageSessionProvider};
use wnode_test::{SessionFactory, session_factory};
use wtest_base::{resp_frame, test_store_config};

/// 起单 worker 真存储服务器（池基准规格 = DEFAULT_BUFFER_SIZE，真发布订阅
/// 中枢默认装配，对标 tls_push_tests::spawn_server 明文形态）
fn spawn_server() -> (
  GarnetServer<StorageSessionProvider<SessionFactory>>,
  SocketAddr,
  Arc<LimitedFixedBufferPool>,
) {
  let dir = tempfile::tempdir().unwrap();
  let data_path = dir.path().join("burst-shrink.db");
  let factory: SessionFactory = session_factory;
  let provider =
    StorageSessionProvider::open_with_config(test_store_config(), data_path, factory).unwrap();
  let server = GarnetServer::new(
    &["127.0.0.1:0".to_string()],
    DEFAULT_BUFFER_SIZE,
    8,
    Arc::new(provider),
  )
  .unwrap();
  server.start(NonZeroUsize::new(1)).unwrap();
  let addr = server.local_addr().unwrap();
  let pool = server.buffer_pool().clone();
  // dir 随句柄存活至测试结束（服务器全生命周期数据文件在场）
  forget(dir);
  (server, addr, pool)
}

/// bulk string 应答长度：$<len>\r\n + payload + \r\n
fn bulk_reply_len(payload: &[u8]) -> usize {
  format!("${}\r\n", payload.len()).len() + payload.len() + 2
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

/// QUIT 后借空池并统计基准规格闲置块数（借到穿透堆即止；take 出的块不归还）
///
/// 基准层级闲置块是池复用能力的命脉：缺陷形态下发送缓冲以扩容后容量迁移
/// 高级层级或直接弃置，基准层级每连接净减一块
fn count_base_level_blocks(pool: &LimitedFixedBufferPool) -> usize {
  let alloc0 = pool.allocated_count();
  let mut blocks = Vec::new();
  while pool.allocated_count() == alloc0 {
    let mut b = pool.get_ref(DEFAULT_BUFFER_SIZE);
    if pool.allocated_count() != alloc0 {
      break; // 池已借空，本次为穿透新分配
    }
    assert_eq!(
      b.capacity(),
      DEFAULT_BUFFER_SIZE,
      "基准层级块容量必须为池基准规格"
    );
    blocks.push(b.take_buffer());
  }
  assert_eq!(pool.borrowed_count(), 0, "统计收尾无在途借出");
  blocks.len()
}

/// 突发大应答不蚕食缓冲池：连续大 GET 后池计数稳定，连接退出后发送缓冲以
/// 基准规格回池（修复前：重借逐轮弹走基准层级闲置块、扩容块弃置/迁移，
/// 计数逐轮漂移、基准层级被排空）
#[test]
fn burst_large_replies_stabilize_pool_quota() {
  let (server, addr, pool) = spawn_server();

  // 200KB 大 value：单命令应答超 OUTPUT_WATERMARK_BYTES（128KB），响应缓冲
  // 必然扩容越过池基准规格
  let value = vec![b'x'; 200 * 1024];
  let set_frame = resp_frame(&[b"SET", b"big", &value]);
  let get_frame = resp_frame(&[b"GET", b"big"]);
  let get_reply_len = bulk_reply_len(&value);

  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let mut stream = connect(addr).await;
    stream.write_all(set_frame).await.unwrap();
    let mut acc = Vec::new();
    read_until(&mut stream, &mut acc, b"+OK\r\n".len()).await;
    assert_eq!(&acc, b"+OK\r\n");

    // 基线：握手块已归还、响应块借出中（连接单一配额）
    let (alloc0, free0, borrowed0) = (
      pool.allocated_count(),
      pool.free_count(),
      pool.borrowed_count(),
    );
    assert_eq!(borrowed0, 1, "连接存活期仅占用建连借出的单一配额");

    for round in 0..6 {
      stream.write_all(get_frame.clone()).await.unwrap();
      let mut acc = Vec::new();
      read_until(&mut stream, &mut acc, get_reply_len).await;

      assert_eq!(
        pool.allocated_count(),
        alloc0,
        "第 {round} 轮大应答后穿透堆分配（重借蚕食形态下逐轮 +1）"
      );
      assert_eq!(
        pool.free_count(),
        free0,
        "第 {round} 轮大应答后池闲置配额被蚕食迁移"
      );
      assert_eq!(pool.borrowed_count(), borrowed0);
    }

    stream
      .write_all(b"*1\r\n$4\r\nQUIT\r\n".to_vec())
      .await
      .unwrap();
    let mut acc = Vec::new();
    read_until(&mut stream, &mut acc, b"+OK\r\n".len()).await;
    assert_eq!(&acc, b"+OK\r\n");
    drop(stream);
    // 有界轮询至泵收场归还发送缓冲（resp_pooled 随 drive_loop 返回 RAII
    // 归还的异步链，5s 上界）
    for _ in 0..1000 {
      if pool.borrowed_count() == 0 {
        break;
      }
      sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(pool.borrowed_count(), 0, "连接收场后无在途借出");
    assert_eq!(pool.free_count(), free0 + 1, "发送缓冲随泵收场回池");
    // 收敛规格验证：基准层级闲置块 >= 1（发送缓冲收敛回基准规格并归还入池）；
    // 缺陷形态下发送缓冲以扩容容量弃置/迁移，基准层级被排空（0 块）
    assert!(
      count_base_level_blocks(&pool) >= 1,
      "发送缓冲必须收敛回池基准规格后回池"
    );
  });

  server.stop();
}

/// 订阅大推送后发送缓冲不驻留峰值容量：长连接退出后发送缓冲以基准规格回池
///（修复前：推送写出收尾缺失低水位收敛，扩容容量常驻至连接退出，
/// 以扩容容量弃置/迁移，基准层级净减）
#[test]
fn push_large_payload_shrinks_send_buffer() {
  let (server, addr, pool) = spawn_server();

  let payload = vec![b'y'; 200 * 1024];
  let channel = b"ch";
  // RESP2 推送帧：*3 message <ch> <payload>
  let push_frame_len = b"*3\r\n$7\r\nmessage\r\n$2\r\nch\r\n".len()
    + format!("${}\r\n", payload.len()).len()
    + payload.len()
    + 2;
  let sub_ack_len = b"*3\r\n$9\r\nsubscribe\r\n$2\r\nch\r\n:1\r\n".len();
  let publish_frame = resp_frame(&[b"PUBLISH", channel, &payload]);

  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    // 订阅连接：SUBSCRIBE 后不再发任何输入帧，空闲挂读等待推送
    let mut sub = connect(addr).await;
    sub
      .write_all(resp_frame(&[b"SUBSCRIBE", channel]))
      .await
      .unwrap();
    let mut sub_acc = Vec::new();
    read_until(&mut sub, &mut sub_acc, sub_ack_len).await;
    assert_eq!(&sub_acc, b"*3\r\n$9\r\nsubscribe\r\n$2\r\nch\r\n:1\r\n");

    // 基线：订阅连接握手块已归还、发送缓冲借出中
    let (free0, borrowed0) = (pool.free_count(), pool.borrowed_count());
    assert_eq!(borrowed0, 1);

    // 发布连接：PUBLISH 200KB → 推送帧入订阅连接邮箱，随读段双路等待
    // 唤醒直写网络
    let mut publisher = connect(addr).await;
    publisher.write_all(publish_frame).await.unwrap();
    let mut pub_acc = Vec::new();
    read_until(&mut publisher, &mut pub_acc, b":1\r\n".len()).await;
    assert_eq!(&pub_acc, b":1\r\n");

    // 订阅端收推送帧（ack 之后追加）
    read_until(&mut sub, &mut sub_acc, sub_ack_len + push_frame_len).await;

    // 双连接 QUIT，各自泵收场
    publisher
      .write_all(b"*1\r\n$4\r\nQUIT\r\n".to_vec())
      .await
      .unwrap();
    let mut pub_acc = Vec::new();
    read_until(&mut publisher, &mut pub_acc, b"+OK\r\n".len()).await;
    drop(publisher);
    sub
      .write_all(b"*1\r\n$4\r\nQUIT\r\n".to_vec())
      .await
      .unwrap();
    read_until(
      &mut sub,
      &mut sub_acc,
      sub_ack_len + push_frame_len + b"+OK\r\n".len(),
    )
    .await;
    drop(sub);
    // 有界轮询至两连接泵收场全量归还（5s 上界）
    for _ in 0..1000 {
      if pool.borrowed_count() == 0 {
        break;
      }
      sleep(Duration::from_millis(5)).await;
    }

    // 全连接收场：两连接各占用 1 块（发送缓冲复用握手块并就地收敛回基准规格）；
    // 连接退出后 2 块全量归还
    assert_eq!(pool.borrowed_count(), 0);
    assert_eq!(pool.free_count(), free0 + 2);
    assert!(
      count_base_level_blocks(&pool) >= 2,
      "大推送后发送缓冲必须收敛回池基准规格（缺陷形态下常驻峰值容量退出）"
    );
  });

  server.stop();
}
