#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! 冷启动显式 `--checkpoint-dir` 臂检查点目录同源端到端测试
//!
//! 对标 C# 检查点目录单一真源 GetStoreCheckpointDirectory
//!（garnet/libs/server/Servers/GarnetServerOptions.cs:692，宿主
//! GarnetServer.cs:413/:479 单处消费）：SAVE 写与恢复读恒同目录。
//! 走 [`StorageSessionProvider::open_from_args`] 生产装配路径：
//!
//! 1. AOF off + 显式 checkpoint-dir 冷启（(false,false) 臂）：SAVE 快照落
//!    显式目录，回落目录零产物；
//! 2. 同参 `--recover` 重启（(true,false) 臂）：快照被消费、数据回载；
//! 3. 缺省不传参：回落目录行为不变（两目录串恒同值锁）。

use std::{
  fs,
  path::{Path, PathBuf},
  sync::Arc,
};

use compio::{net::TcpStream, runtime::Runtime};
use tempfile::tempdir;
use wconf::NodeArgs;
use wnode::service::StorageSessionProvider;
use wnode_test::{read_bulk_reply, read_line_reply, send_cmd, session_factory, start_server};
use wtest_base::test_store_config;

/// 检查点目录口径（service.rs checkpoint_dir_of：`{base}/Store/checkpoints`）
fn store_checkpoints(base: &Path) -> PathBuf {
  base.join("Store").join("checkpoints")
}

/// 目录存在且含至少一条目
fn has_entries(path: &Path) -> bool {
  fs::read_dir(path).is_ok_and(|mut rd| rd.next().is_some())
}

/// 写入并 SAVE（AOF off 形态检查点是唯一持久化面）
async fn set_and_save(stream: &mut TcpStream) {
  send_cmd(stream, &[b"SET", b"ck", b"v1"])
    .await
    .expect("set ck");
  assert!(read_line_reply(stream).await.starts_with(b"+OK"));
  send_cmd(stream, &[b"SAVE"]).await.expect("save");
  assert!(
    read_line_reply(stream).await.starts_with(b"+OK"),
    "SAVE 须成功"
  );
}

/// AOF off + 显式 checkpoint-dir 冷启：SAVE 落显式目录而非回落目录；
/// 同参 --recover 重启快照被消费（修复前 SAVE 落回落目录、recover 零命中静默空库）
#[test]
fn cold_start_explicit_checkpoint_dir_save_and_recover() {
  let rt = Runtime::new().expect("compio runtime");
  let dir = tempdir().expect("tempdir");
  let data_dir = dir.path().join("data");
  let ckpt_base = dir.path().join("ckpt");
  let data_path = data_dir.join("wedb.db");
  let node = NodeArgs {
    dir: data_dir.clone(),
    checkpoint_dir: Some(ckpt_base.clone()),
    ..NodeArgs::default()
  };

  // ---- 第一代进程：(false,false) 冷启臂，SET → SAVE → 停服
  let provider = Arc::new(
    rt.block_on(StorageSessionProvider::open_from_args_with_config(
      test_store_config(),
      &node,
      &data_path,
      session_factory,
    ))
    .expect("open from args"),
  );
  // 声明目录即显式真源（覆写后换装的双件同源）
  assert_eq!(
    provider.checkpoint_dir,
    store_checkpoints(&ckpt_base),
    "冷启臂声明目录须为显式 checkpoint-dir 真源"
  );
  let (server, addr) = start_server(Arc::clone(&provider));
  rt.block_on(async {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    set_and_save(&mut stream).await;
    send_cmd(&mut stream, &[b"QUIT"]).await.expect("quit");
  });
  server.stop();
  drop(server);
  drop(provider);
  // 快照产物落显式目录；回落目录零产物
  assert!(
    has_entries(&store_checkpoints(&ckpt_base)),
    "SAVE 快照须落显式 checkpoint 目录"
  );
  assert!(
    !has_entries(&store_checkpoints(&data_dir)),
    "回落目录不得有快照产物"
  );

  // ---- 第二代进程：同参 --recover 重启，数据回载
  let node2 = NodeArgs {
    recover: true,
    ..node.clone()
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
  let (server2, addr2) = start_server(Arc::clone(&provider2));
  rt.block_on(async {
    let mut stream = TcpStream::connect(addr2).await.expect("reconnect");
    send_cmd(&mut stream, &[b"GET", b"ck"])
      .await
      .expect("get ck");
    assert_eq!(
      read_bulk_reply(&mut stream).await,
      Some(b"v1".to_vec()),
      "--recover 须消费显式目录快照回载数据"
    );
    send_cmd(&mut stream, &[b"QUIT"]).await.expect("quit");
  });
  server2.stop();
}

/// 缺省不传 --checkpoint-dir：回落目录行为不变（两目录串恒同值锁 + SAVE/recover 回归）
#[test]
fn cold_start_default_checkpoint_dir_fallback_unchanged() {
  let rt = Runtime::new().expect("compio runtime");
  let dir = tempdir().expect("tempdir");
  let data_dir = dir.path().join("data");
  let data_path = data_dir.join("wedb.db");
  let node = NodeArgs {
    dir: data_dir.clone(),
    ..NodeArgs::default()
  };
  // 恒同值锁：checkpoint_base_dir 回落数据目录，与 open_with_config 嵌入式
  // 回落（data_path 父目录口径）两目录串全等，防回落漂移回归
  let fallback = store_checkpoints(&node.checkpoint_base_dir());
  assert_eq!(
    fallback,
    store_checkpoints(data_path.parent().expect("data_path parent")),
    "缺省回落两目录串须恒同值"
  );

  // ---- 第一代进程：(false,false) 冷启臂，SET → SAVE → 停服
  let provider = Arc::new(
    rt.block_on(StorageSessionProvider::open_from_args_with_config(
      test_store_config(),
      &node,
      &data_path,
      session_factory,
    ))
    .expect("open from args"),
  );
  assert_eq!(
    provider.checkpoint_dir, fallback,
    "缺省冷启声明目录须为回落目录"
  );
  let (server, addr) = start_server(Arc::clone(&provider));
  rt.block_on(async {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    set_and_save(&mut stream).await;
    send_cmd(&mut stream, &[b"QUIT"]).await.expect("quit");
  });
  server.stop();
  drop(server);
  drop(provider);
  assert!(has_entries(&fallback), "缺省回落路径 SAVE 快照须落回落目录");

  // ---- 第二代进程：同参 --recover 重启，数据回载
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
  let (server2, addr2) = start_server(Arc::clone(&provider2));
  rt.block_on(async {
    let mut stream = TcpStream::connect(addr2).await.expect("reconnect");
    send_cmd(&mut stream, &[b"GET", b"ck"])
      .await
      .expect("get ck");
    assert_eq!(
      read_bulk_reply(&mut stream).await,
      Some(b"v1".to_vec()),
      "缺省回落路径 --recover 须回载数据"
    );
    send_cmd(&mut stream, &[b"QUIT"]).await.expect("quit");
  });
  server2.stop();
}
