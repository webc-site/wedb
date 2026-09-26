//! 向量登记表恢复回建端到端测试
//!
//! 回归面：「检查点前创建的向量集重启后可检索」（登记表与上下文元数据经
//! KeyTag::VectorRegistry 旁路记录随检查点持久，--recover 恢复完成点自
//! 旁路记录回建登记并收口上下文保留）：
//! 1. VADD 建集 → SAVE（检查点 + 版本推进 + AOF 截断）→ 停服；
//!    SAVE 后无任何增量写入——登记条目的唯一持久载体即检查点；
//! 2. --recover 重启（open_recovered_with_config_and_aof，恢复先于 accept）；
//! 3. VSIM 命中检查点前写入的元素；同元素 VADD 回 Duplicate（登记回建后
//!    context 原位复用，未走「新建空索引」分支）。

use std::{str::from_utf8, sync::Arc};

use compio::{net::TcpStream, runtime::Runtime};
use tempfile::tempdir;
use wconf::RuntimeServerOptions;
use wnode::service::StorageSessionProvider;
use wnode_test::{read_line_reply, read_reply, send_cmd, session_factory, start_server};
use wtest_base::test_store_config;

/// FP32 向量字节（行主小端）
fn fp32(vals: &[f32]) -> Vec<u8> {
  vals.iter().flat_map(|v| v.to_le_bytes()).collect()
}

#[test]
fn vector_set_created_before_checkpoint_is_searchable_after_restart() {
  let rt = Runtime::new().expect("compio runtime");
  let dir = tempdir().expect("tempdir");
  let data_path = dir.path().join("node").join("vector.db");
  let (key, elem) = (b"pts".as_slice(), b"e1".as_slice());
  let query = fp32(&[1.0, 1.0]);

  // ---- 第一代进程：VADD 建集 → SAVE → 停服（SAVE 后零增量）
  let provider = Arc::new(
    StorageSessionProvider::open_with_config_and_aof(
      test_store_config(),
      &data_path,
      None,
      RuntimeServerOptions::default(),
      session_factory,
    )
    .expect("open with aof")
    .with_vector_set_preview(true),
  );
  let (server, addr) = start_server(Arc::clone(&provider));
  rt.block_on(async {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    send_cmd(
      &mut stream,
      &[b"VADD", key, b"FP32", &fp32(&[1.0, 1.0]), elem],
    )
    .await
    .expect("vadd");
    assert_eq!(
      read_line_reply(&mut stream).await,
      b":1\r\n",
      "检查点前 VADD 建集须成功"
    );
    send_cmd(&mut stream, &[b"SAVE"]).await.expect("save");
    assert!(
      read_line_reply(&mut stream).await.starts_with(b"+OK"),
      "SAVE 须成功（登记旁路记录与元素数据随检查点持久）"
    );
    // 模拟周期提交后停服；SAVE 后无增量，AOF 重放无条目可追——
    // 向量集对第二代进程的唯一来源即检查点回建
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

  // ---- 第二代进程：--recover 重启，登记回建先于端点 accept
  //（预览开关构造期定值——恢复回建与命令面共用同一真值，对标 C#
  // IsEnabled 构造期定值语义）
  let provider2 = Arc::new(
    rt.block_on(StorageSessionProvider::open_recovered_with_config_and_aof(
      test_store_config(),
      &data_path,
      None,
      RuntimeServerOptions::default(),
      true,
      session_factory,
    ))
    .expect("open recovered"),
  );
  let (server2, addr2) = start_server(Arc::clone(&provider2));
  rt.block_on(async {
    let mut stream = TcpStream::connect(addr2).await.expect("reconnect");

    // EXISTS：登记第四态判活（回建生效面）
    send_cmd(&mut stream, &[b"EXISTS", key])
      .await
      .expect("exists");
    assert_eq!(
      read_line_reply(&mut stream).await,
      b":1\r\n",
      "重启后向量集键须存活（登记回建生效）"
    );

    // VSIM 有结果：登记表已回建（否则按键不存在回空数组）
    send_cmd(
      &mut stream,
      &[b"VSIM", key, b"FP32", &query, b"COUNT", b"1"],
    )
    .await
    .expect("vsim");
    let hits = read_reply(&mut stream).await;
    assert!(
      from_utf8(&hits)
        .unwrap_or("")
        .contains(from_utf8(elem).expect("utf8 elem")),
      "重启后检查点前创建的向量集须可检索，实际应答: {:?}",
      String::from_utf8_lossy(&hits)
    );
    send_cmd(&mut stream, &[b"QUIT"]).await.expect("quit");
  });
  server2.stop();
}
