#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! 默认装配臂 (false,false) 向量管理器配对锁测
//!
//! 回归面（票 wnode-default-arm-missing-attach-vector-manager-reclaim-bypass）：
//! 四装配臂唯默认冷启动臂漏挂 `attach_vector_manager`——生产 (false,false)
//! 臂经 `open_with_config` → `from_parts` → `reattach_checkpoint_dir` 换装
//! 双件，`database_manager` 未注入向量管理器，FLUSH 族登记域回收静默旁路
//!（`SingleDatabaseManager::reclaim_registry` 未注入即跳过）：向量登记幽灵
//! 驻留死域、清理/量化任务无效巡游、--recover 回建复活已清域登记。
//!
//! C# 对位：VectorManager 随 StoreWrapper 单装配路径无条件配对
//!（garnet/libs/host/GarnetServer.cs:420 构造 / :468 CreateStore 形参），
//! 无「漏挂」形态。
//!
//! 锁测两件：
//! 1. 默认臂 + 预览开 → VADD 建集 → FLUSHDB → 配对在场 + 死域登记整域回收
//!    + 命令面不可达（VCARD 归零）；
//! 2. 同形数据落盘（FLUSHDB 后 SAVE，登记墓碑为唯一持久载体）→ 同参
//!    --recover 重启 → 回建不复活已清域登记。

use std::{path::Path, sync::Arc};

use compio::{net::TcpStream, runtime::Runtime};
use tempfile::tempdir;
use wconf::NodeArgs;
use wnode::service::StorageSessionProvider;
use wnode_test::{
  cmd, fp32_le_bytes as fp32, read_line_reply, send_cmd, session_factory, start_server,
};
use wtest_base::test_store_config;
use wval::SessionPrefixBuf;

/// 默认冷启动臂节点参数（(false,false) + 向量预览开）
fn default_arm_node(data_dir: &Path) -> NodeArgs {
  NodeArgs {
    dir: data_dir.to_path_buf(),
    enable_vector_set_preview: true,
    ..NodeArgs::default()
  }
}

/// FP32 向量字节（行主小端）
/// VADD 建集（:1 应答锁）
async fn vadd(stream: &mut TcpStream, key: &[u8]) {
  send_cmd(
    stream,
    &[b"VADD", key, b"FP32", &fp32(&[1.0, 2.0, 3.0]), b"elem1"],
  )
  .await
  .expect("vadd send");
  assert_eq!(
    read_line_reply(stream).await,
    b":1\r\n",
    "VADD 建集须成功（预览已点亮，命令面可达）"
  );
}

/// 默认臂配对在场 + FLUSHDB 登记域回收：修复前 reattach 换装即断链，
/// FLUSH 族四回收口在默认臂全空转，死域登记幽灵驻留
#[test]
fn default_arm_pairs_vector_manager_and_reclaims_on_flushdb() {
  let rt = Runtime::new().expect("compio runtime");
  let dir = tempdir().expect("tempdir");
  let data_dir = dir.path().join("data");
  let data_path = data_dir.join("wedb.db");

  // 生产默认臂装配（open_from_args_with_config (false,false) 臂）：检查点
  // 目录换装双件后 database_manager 必须持向量管理器句柄
  let provider = Arc::new(
    rt.block_on(StorageSessionProvider::open_from_args_with_config(
      test_store_config(),
      &default_arm_node(&data_dir),
      &data_path,
      session_factory,
    ))
    .expect("open default arm"),
  );
  assert!(
    provider.database_manager.try_vector_manager().is_some(),
    "默认臂 database_manager 须配对向量管理器（FLUSH 族回收联动在场）"
  );

  let (server, addr) = start_server(Arc::clone(&provider));
  rt.block_on(async {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    vadd(&mut stream, b"vs_a").await;

    // 清库前 db0 物理域：登记条目在位
    let (dead_vns, dead_vdb, ..) = provider.store().vdb.get_virtual_ids_with_created(0, 0);
    let dead = SessionPrefixBuf::new(dead_vns, dead_vdb);
    assert_eq!(
      provider
        .vector_manager
        .registry_domain_count(dead.as_slice()),
      1,
      "清库前 db0 应恰有一项登记"
    );

    // FLUSHDB：登记域随换号回收（+OK 在两轮回收扫尾完成后才应答）
    assert_eq!(cmd(&mut stream, &[b"FLUSHDB"]).await, b"+OK\r\n");
    let (live_vns, live_vdb, ..) = provider.store().vdb.get_virtual_ids_with_created(0, 0);
    assert_ne!(
      (dead_vns, dead_vdb),
      (live_vns, live_vdb),
      "FLUSHDB 应换号（逻辑库重指新物理域）"
    );
    assert_eq!(
      provider
        .vector_manager
        .registry_domain_count(dead.as_slice()),
      0,
      "FLUSHDB 后死域登记应整域回收（幽灵驻留即回收旁路复现）"
    );
    assert_eq!(
      provider
        .vector_manager
        .registry_domain_count(SessionPrefixBuf::new(live_vns, live_vdb).as_slice()),
      0,
      "新域在重建前不得有登记条目"
    );
    // 命令面不可达：登记摘除断四态判活，VCARD 归零
    assert_eq!(cmd(&mut stream, &[b"VCARD", b"vs_a"]).await, b":0\r\n");
    send_cmd(&mut stream, &[b"QUIT"]).await.expect("quit");
  });
  server.stop();
  drop(server);
  drop(provider);
}

/// 默认臂 FLUSHDB 后落盘，--recover 重启回建不复活已清域登记：回收写透
/// 登记墓碑（修复前旁路缺墓碑，回建扫描链首存活记录即复活幽灵登记）
#[test]
fn default_arm_flushed_registry_not_resurrected_on_recover() {
  let rt = Runtime::new().expect("compio runtime");
  let dir = tempdir().expect("tempdir");
  let data_dir = dir.path().join("data");
  let data_path = data_dir.join("wedb.db");
  let node = default_arm_node(&data_dir);

  // ---- 第一代：默认臂 VADD → FLUSHDB → SAVE（FLUSHDB 后零增量，检查点是
  // 登记面唯一持久载体）→ 停服
  let provider = Arc::new(
    rt.block_on(StorageSessionProvider::open_from_args_with_config(
      test_store_config(),
      &node,
      &data_path,
      session_factory,
    ))
    .expect("open default arm"),
  );
  // 清库前 db0 物理域（FLUSHDB 后整域退役，域 id 供第二代幽灵断言）
  let (dead_vns, dead_vdb, ..) = provider.store().vdb.get_virtual_ids_with_created(0, 0);
  let (server, addr) = start_server(Arc::clone(&provider));
  rt.block_on(async {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    vadd(&mut stream, b"vs_ghost").await;
    assert_eq!(cmd(&mut stream, &[b"FLUSHDB"]).await, b"+OK\r\n");
    assert!(
      cmd(&mut stream, &[b"SAVE"]).await.starts_with(b"+OK"),
      "SAVE 须成功"
    );
    send_cmd(&mut stream, &[b"QUIT"]).await.expect("quit");
  });
  server.stop();
  drop(server);
  drop(provider);

  // ---- 第二代：同参 --recover 重启（(true,false) 臂），回建完成点断言
  let node2 = NodeArgs {
    recover: true,
    ..node
  };
  let provider2 = Arc::new(
    rt.block_on(StorageSessionProvider::open_from_args_with_config(
      test_store_config(),
      &node2,
      &data_path,
      session_factory,
    ))
    .expect("open recovered"),
  );
  assert!(
    provider2.database_manager.try_vector_manager().is_some(),
    "恢复臂 database_manager 须配对向量管理器"
  );
  let (live_vns, live_vdb, ..) = provider2.store().vdb.get_virtual_ids_with_created(0, 0);
  let dead = SessionPrefixBuf::new(dead_vns, dead_vdb);
  let live = SessionPrefixBuf::new(live_vns, live_vdb);
  assert_eq!(
    provider2
      .vector_manager
      .registry_domain_count(dead.as_slice()),
    0,
    "回建不得复活已清死域登记（幽灵登记即旁路复现）"
  );
  assert_eq!(
    provider2
      .vector_manager
      .registry_domain_count(live.as_slice()),
    0,
    "活域在重建前不得有登记条目"
  );
  let (server2, addr2) = start_server(Arc::clone(&provider2));
  rt.block_on(async {
    let mut stream = TcpStream::connect(addr2).await.expect("reconnect");
    assert_eq!(
      cmd(&mut stream, &[b"EXISTS", b"vs_ghost"]).await,
      b":0\r\n",
      "已清库向量键重启后不得复活"
    );
    assert_eq!(cmd(&mut stream, &[b"VCARD", b"vs_ghost"]).await, b":0\r\n");
    send_cmd(&mut stream, &[b"QUIT"]).await.expect("quit");
  });
  server2.stop();
}
