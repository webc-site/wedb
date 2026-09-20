//! 大值端到端集成测试（对标 C# Garnet 大值通道：ServerOptions.cs:46 PageSize =
//! "16m" 默认页 + KVSettings.cs:159 DefaultMaxInlineValueSize = 1MB 的
//! SET → GET → 检查点 → 重启恢复 → AOF 重放全链一致语义）
//!
//! 生产装配链路验证：`StoreConfig::auto()` 系内存预算规划器按预算推导大页
//! （≥ 128MB 预算页容量 ≥ 2MB，单页内联承载 1MB 基线大值；生产 ≥ 1GB 预算
//! 推导 16MB 页，对标 C# 默认），RESP 命令面 SET 100KB~1MB 值经
//! StorageSessionProvider 完整装配（引擎 + AOF）后：
//! 1. SET/GET 逐字节一致；
//! 2. SAVE 检查点后重启（--recover 语义）检查点内大值恢复一致；
//! 3. SAVE 后增量大值（仅存 AOF）经重放追平一致。

use std::sync::Arc;

use compio::{net::TcpStream, runtime::Runtime};
use tempfile::tempdir;
use wkv::StoreConfig;
use wnode::service::StorageSessionProvider;
use wnode_test::{read_bulk_reply, read_line_reply, send_cmd, session_factory, start_server};

/// 大值尺寸档位：100KB / 1MB（C# DefaultMaxInlineValueSize 基线）/ 512KB
const VALUE_SIZES: [usize; 3] = [100 * 1024, 1024 * 1024, 512 * 1024];

/// 内容含位置指纹的大值载荷（任何错位/截断都会被逐字节比对击穿）
fn payload(size: usize, seed: u8) -> Vec<u8> {
  (0..size)
    .map(|i| (i as u32 as u8).wrapping_add(seed))
    .collect()
}

/// 大值逐字节校验
fn assert_payload_eq(actual: &[u8], expected: &[u8]) {
  assert_eq!(actual.len(), expected.len(), "大值长度必须一致");
  assert_eq!(actual, expected, "大值内容必须逐字节一致");
}

/// SET 大值并断言 +OK，随后 GET 逐字节回读比对
async fn set_and_verify(stream: &mut TcpStream, key: &[u8], value: &[u8]) {
  send_cmd(stream, &[b"SET", key, value])
    .await
    .expect("SET 发送");
  assert!(
    read_line_reply(stream).await.starts_with(b"+OK"),
    "SET {key:?} 必须成功"
  );
  send_cmd(stream, &[b"GET", key]).await.expect("GET 发送");
  let got = read_bulk_reply(stream).await.expect("GET 必须命中");
  assert_payload_eq(&got, value);
}

/// SET 100KB~1MB 大值 → 读回一致 → SAVE 检查点 → 重启恢复 → AOF 增量重放全链一致
#[test]
fn large_values_survive_checkpoint_restart_and_aof_replay() {
  let rt = Runtime::new().expect("compio runtime");
  let dir = tempdir().expect("tempdir");
  let data_path = dir.path().join("node").join("large_value.db");

  // 生产装配推导：256MB 预算 → 页容量 ≥ 2MB，1MB 值可内联（16MB 页见
  // wkv store_config_page_size_scales_with_budget 的规划器断言）
  let config = StoreConfig::auto_with_budget(256 * 1024 * 1024);
  assert!(
    config.page_size >= 2 * 1024 * 1024,
    "生产规划器必须为大值负载推导大页，当前 {} 字节",
    config.page_size
  );

  // ---- 第一代进程：大值 SET/GET → SAVE → 增量大值（仅 AOF）→ 停服
  let values: Vec<(Vec<u8>, Vec<u8>)> = VALUE_SIZES
    .iter()
    .enumerate()
    .map(|(i, &size)| (format!("big:{i}").into_bytes(), payload(size, i as u8 + 1)))
    .collect();

  let provider = Arc::new(
    StorageSessionProvider::open_with_config_and_aof(
      config.clone(),
      &data_path,
      None,
      None,
      session_factory,
    )
    .expect("open with aof"),
  );
  let (server, addr) = start_server(Arc::clone(&provider));
  rt.block_on(async {
    let mut stream = TcpStream::connect(addr).await.expect("connect");

    // 1. 大值 SET → GET 逐字节一致（含 1MB 基线档位）
    for (key, value) in &values {
      set_and_verify(&mut stream, key, value).await;
    }

    // 2. SAVE：大值全部进检查点
    send_cmd(&mut stream, &[b"SAVE"]).await.expect("save");
    assert!(
      read_line_reply(&mut stream).await.starts_with(b"+OK"),
      "SAVE 须成功（检查点 + 版本推进 + AOF 截断）"
    );

    // 3. SAVE 后增量大值：仅存于 AOF，恢复靠重放追平
    let incremental = (b"big:after-save".as_slice(), payload(512 * 1024, 9));
    set_and_verify(&mut stream, incremental.0, &incremental.1).await;

    // 物理刷盘（设备面恢复以落盘为准）
    provider
      .aof()
      .expect("aof enabled")
      .log()
      .commit_async()
      .await;
    send_cmd(&mut stream, &[b"QUIT"]).await.expect("quit");
  });
  server.stop();
  drop(server);
  drop(provider);

  // ---- 第二代进程：--recover 重启，检查点大值 + AOF 增量大值全量可读
  let provider2 = Arc::new(
    rt.block_on(StorageSessionProvider::open_recovered_with_config_and_aof(
      config,
      &data_path,
      None,
      None,
      false,
      session_factory,
    ))
    .expect("open recovered"),
  );
  let (server2, addr2) = start_server(Arc::clone(&provider2));
  rt.block_on(async {
    let mut stream = TcpStream::connect(addr2).await.expect("reconnect");

    // 检查点内大值逐字节一致
    for (key, value) in &values {
      send_cmd(&mut stream, &[b"GET", key])
        .await
        .expect("GET 发送");
      let got = read_bulk_reply(&mut stream)
        .await
        .expect("检查点大值必须命中");
      assert_payload_eq(&got, value);
    }

    // AOF 重放追平的增量大值逐字节一致
    send_cmd(&mut stream, &[b"GET", b"big:after-save"])
      .await
      .expect("GET 发送");
    let got = read_bulk_reply(&mut stream)
      .await
      .expect("AOF 增量大值必须命中");
    assert_payload_eq(&got, &payload(512 * 1024, 9));

    send_cmd(&mut stream, &[b"QUIT"]).await.expect("quit");
  });
  server2.stop();
}
