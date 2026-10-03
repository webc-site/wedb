//! 量化 worker 存活装配级锁测（票：task/ing/wnode-quant-worker-handle-discard-instant-cancel）
//!
//! 缺陷形态：start_quantization_tasks 返回 Vec<JoinHandle<()>>，生产唯一拉起
//! 点 get_session（service.rs）语句末即 Drop——compio JoinHandle Drop 即
//! task.cancel，量化 worker 未经首轮 poll 即灭：量化开关打开的部署建表/
//! 回填永无人执行（静默失能），量化通道只进不出无界积压，监督快照恒无
//! quantization_worker 行（观测脱节）。
//!
//! 修复契约：拉起点内句柄就地 detach 交执行器持有，签名收敛无返回。
//!
//! 本测全链走生产装配（open_with_config_and_aof + 网络会话经 get_session
//! 惰性拉起 + TCP VADD 生产命令面），零 mock：
//!   1. worker 存活跨多轮 poll：建表（1005 样本训练）+ 回填消费完成后，
//!      INFO server 段 bg_task_health 仍出 `quantization_worker=alive`；
//!   2. 通道条目被真实消费：qnt 双计数递增（建表成功臂与回填臂各自单点
//!      递增，冻结与消费在计数面可区分）；
//!   3. 无界积压反证：消费收敛后量化通道归空（只进不出形态被打破）。
//!
//! 量化触发锚：VADD 带 BIN 量化器（Spherical1Bit，required_vectors 恒
//! 1000，wvector/tests/quant_train_barrier.rs 同锚），插入数越过门槛后
//! insert 臂返回 QuantizationRequested 推通道。

use std::{
  sync::{Arc, atomic::Ordering},
  time::Duration,
};

use compio::{
  net::TcpStream,
  runtime::Runtime,
  time::{sleep, timeout},
};
use wconf::RuntimeServerOptions;
use wnode::service::StorageSessionProvider;
use wnode_test::{read_line_reply, read_reply, send_cmd, session_factory, start_server, vec_bytes};
use wtest_base::test_store_config;

const DIM: usize = 32;
/// Spherical1Bit 训练门槛 1000 + 余量（门槛判定为 total_used() > 1000，
/// 多插无害：表已训练后重复建表项按幂等终态弹项）
const ELEMENTS: usize = 1005;
/// 全用例有界时限（训练 + 回填在 server runtime 异步收敛，宽松上限）
const DEADLINE: Duration = Duration::from_secs(60);

/// 经 get_session 生产拉起点存活断言：worker detach 后真实 poll 存活，
/// 量化通道被真实消费（建表 + 回填），无界积压形态被打破
#[test]
fn quantization_worker_survives_production_spawn_path() {
  let rt = Runtime::new().expect("compio runtime");
  let dir = tempfile::tempdir().expect("tempdir");
  rt.block_on(async move {
    let provider = Arc::new(
      StorageSessionProvider::open_with_config_and_aof(
        test_store_config(),
        dir.path().join("quant_worker.db"),
        None,
        RuntimeServerOptions::default(),
        session_factory,
      )
      .expect("open with aof")
      .with_vector_set_preview(true),
    );
    let (server, addr) = start_server(Arc::clone(&provider));

    // 生产拉起点点亮：首命令字节驱动 handler 握手 → get_session →
    // start_quantization_tasks detach 拉起（修复前句柄在此即生即灭）
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    send_cmd(&mut stream, &[b"PING"]).await.expect("ping");
    let pong = read_line_reply(&mut stream).await;
    assert_eq!(pong, b"+PONG\r\n", "PING 应答");

    // 越过训练门槛：BIN 量化器（Spherical1Bit required_vectors=1000），
    // 第 1001 个插入起 insert 臂返回 QuantizationRequested 推通道
    let mut id = [0u8; 4];
    timeout(DEADLINE, async {
      for i in 0..ELEMENTS {
        id.copy_from_slice(&(i as u32).to_le_bytes());
        let vec = vec_bytes(i as u64, DIM);
        send_cmd(&mut stream, &[b"VADD", b"hk", b"FP32", &vec, &id, b"BIN"])
          .await
          .expect("vadd");
        let reply = read_line_reply(&mut stream).await;
        assert_eq!(
          reply,
          b":1\r\n",
          "VADD 应为新增成功（第 {i} 个）: {}",
          String::from_utf8_lossy(&reply)
        );
      }
    })
    .await
    .expect("VADD 写入超时");

    // 通道条目被真实消费：建表成功臂与回填臂计数各自递增（修复前恒 0
    // ——worker 未 poll，通道只进不出）
    timeout(DEADLINE, async {
      loop {
        let built = provider
          .vector_manager
          .quantization_requests_processed
          .load(Ordering::Relaxed);
        let backfilled = provider
          .vector_manager
          .quantization_backfills_processed
          .load(Ordering::Relaxed);
        if built >= 1 && backfilled >= 1 {
          break;
        }
        sleep(Duration::from_millis(50)).await;
      }
    })
    .await
    .expect("量化通道消费超时（worker 未存活或通道积压无人消费）");

    // 无界积压反证：消费收敛后通道归空（等尾部 backfill 项出队）
    timeout(DEADLINE, async {
      while !provider.vector_manager.quantization_channel.is_empty() {
        sleep(Duration::from_millis(50)).await;
      }
    })
    .await
    .expect("量化通道归空超时");

    // worker 存活跨多轮 poll：消费完成后 INFO server 段 bg_task_health
    // 出 quantization_worker 行且 alive（修复前 worker 未经首轮 poll，
    // 监督注册表无此行——INFO 观测面脱节的直接断言）。INFO 渲染形态为
    // wmetric garnet_info_metrics 的 `name=value` 逗号分隔单行（bg_task_health
    // 值域），断言按等号 kv 子串搜索
    send_cmd(&mut stream, &[b"INFO", b"server"])
      .await
      .expect("info");
    let info = read_reply(&mut stream).await;
    assert!(
      info
        .windows(b"quantization_worker=alive".len())
        .any(|w| w == b"quantization_worker=alive"),
      "bg_task_health 须出 quantization_worker=alive 行: {}",
      String::from_utf8_lossy(&info)
    );

    send_cmd(&mut stream, &[b"QUIT"]).await.expect("quit");
    server.stop();
  });
}
