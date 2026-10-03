//! 量化 worker 自持会话非根域闭环锁测（票：
//! task/ing/wnode-vector-quantization-worker-session-domain-miss）
//!
//! 缺陷形态：量化 worker 每次处理项自持专用会话（bind_dedicated_session →
//! 工厂 store.new_session 裸根域，会话初值 vns/vdb 恒 0），而建表训练取样与
//! 回填逐 id 读写全走 TLS 会话前缀寻址记录——非根域向量集（SELECT 非 0 库）
//! 的量化链全域错位：训练取样逐 id 读 miss 静默返 false，请求按 Failed 终态
//! 消费零报错，建表/回填计数恒 0，该集量化永久停摆全精度。
//!
//! 修复契约：try_process_quantization_request 绑定自持会话后、读索引前，按
//! split_registry_key 解出的 (vns, vdb) 经守卫侧落域单点 set_entry_domain
//! 落条目物理域（逻辑域经 version_domain_of 一次换算直设，形态对齐
//! tiered_demote 单键落域窗）；单点覆盖重建/建表/回填全链。
//!
//! 本测全链走生产装配（open_with_config_and_aof + TCP 命令面，零 mock）：
//!   1. SELECT 1 切非根域 → VADD 带 BIN 量化器越过训练门槛（Spherical1Bit
//!      required_vectors 恒 1000，vector_quantization_worker_lifecycle.rs 同锚；
//!      库级定槽随慢臂快照尾参携会话域槽位，线上 VADD 不携槽参）；
//!   2. 训练 + 回填被真实消费：qnt 双计数递增（缺陷形态下取样恒 miss、
//!      建表按弃返 false，双计数恒 0，本步超时即回归）；
//!   3. VEMB RAW 命中断言：应答量化器名为 bin 且量化载荷窄于全精度宽度
//!      （缺陷形态下量化记录写他域，连接域读 miss 静默回退全精度 FP32，
//!      载荷恒全宽 DIM*4 字节）。

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
use wnode_test::{cmd, read_line_reply, send_cmd, session_factory, start_server, vec_bytes};
use wtest_base::test_store_config;

const DIM: usize = 32;
/// Spherical1Bit 训练门槛 1000 + 余量（门槛判定为 total_used() > 1000）
const ELEMENTS: usize = 1005;
/// 全用例有界时限（训练 + 回填在 server runtime 异步收敛，宽松上限）
const DEADLINE: Duration = Duration::from_secs(60);

/// 非根域量化闭环：SELECT 1 定域 → VADD 越门槛 → worker 训练 + 回填 →
/// 本域 VEMB RAW 命中量化记录（窄于全精度宽度）
#[test]
fn quantization_worker_trains_non_root_domain_set() {
  let rt = Runtime::new().expect("compio runtime");
  let dir = tempfile::tempdir().expect("tempdir");
  rt.block_on(async move {
    let provider = Arc::new(
      StorageSessionProvider::open_with_config_and_aof(
        test_store_config(),
        dir.path().join("quant_nonroot.db"),
        None,
        RuntimeServerOptions::default(),
        session_factory,
      )
      .expect("open with aof")
      .with_vector_set_preview(true),
    );
    let (server, addr) = start_server(Arc::clone(&provider));

    let mut stream = TcpStream::connect(addr).await.expect("connect");
    send_cmd(&mut stream, &[b"PING"]).await.expect("ping");
    let pong = read_line_reply(&mut stream).await;
    assert_eq!(pong, b"+PONG\r\n", "PING 应答");

    // 切非根域（SELECT 非 0 库）：后续 VADD 的元素/量化记录按会话域
    // (ns 0, db 1) 落位，库级定槽随慢臂快照尾参携本域槽位
    send_cmd(&mut stream, &[b"SELECT", b"1"])
      .await
      .expect("select");
    let selected = read_line_reply(&mut stream).await;
    assert_eq!(
      selected,
      b"+OK\r\n",
      "SELECT 1 应答: {}",
      String::from_utf8_lossy(&selected)
    );

    // 越过训练门槛：BIN 量化器，第 1001 个插入起 insert 臂返回
    // QuantizationRequested 推通道（建表项键携非根域前缀）
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

    // 训练 + 回填被真实消费（缺陷形态：worker 自持会话恒根域，非根域集
    // 训练取样逐 id 读 miss → build 按弃返 false，双计数恒 0，本步超时）。
    // 回填计数须达分片总数：建表臂按 quantization_task_count 逐分片派发，
    // 全数完成方覆盖全 id 区间，尾部 VEMB 命中才不落未回填分片
    let shards = provider
      .vector_manager
      .quantization_task_count
      .load(Ordering::Relaxed)
      .max(1);
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
        if built >= 1 && backfilled >= shards as u64 {
          break;
        }
        sleep(Duration::from_millis(50)).await;
      }
    })
    .await
    .expect("量化通道消费超时（非根域集训练取样失训或通道积压无人消费）");

    // 消费收敛：全部回填分片出队完毕
    timeout(DEADLINE, async {
      while !provider.vector_manager.quantization_channel.is_empty() {
        sleep(Duration::from_millis(50)).await;
      }
    })
    .await
    .expect("量化通道归空超时");

    // 本域 VEMB RAW 命中断言：量化器名 bin + 量化载荷窄于全精度宽度
    //（缺陷形态：量化记录落根域，连接域读 miss 静默回退 FP32 全宽
    // DIM*4 字节，回退应答仍名 bin，宽度是唯一可辨域错位面）
    let raw = cmd(&mut stream, &[b"VEMB", b"hk", &id, b"RAW"]).await;
    let quant_pos = raw
      .windows(6)
      .position(|w| w == b"+bin\r\n")
      .unwrap_or_else(|| {
        panic!(
          "VEMB RAW 应答量化器名应为 bin: {}",
          String::from_utf8_lossy(&raw)
        )
      });
    let header = &raw[quant_pos + 6..];
    assert_eq!(
      header.first(),
      Some(&b'$'),
      "量化载荷应为 bulk 帧: {}",
      String::from_utf8_lossy(&raw)
    );
    let nl = header
      .iter()
      .position(|&b| b == b'\n')
      .expect("bulk 头完整");
    let payload_len: usize = String::from_utf8_lossy(&header[1..nl])
      .trim_end()
      .parse()
      .expect("bulk 长头可解析");
    assert!(
      payload_len > 0 && payload_len < DIM * 4,
      "量化载荷宽 {payload_len} 须落在 (0, {}) 开区间——全宽即连接域读 miss \
       静默回退全精度（量化记录未落本域）",
      DIM * 4
    );

    send_cmd(&mut stream, &[b"QUIT"]).await.expect("quit");
    server.stop();
  });
}
