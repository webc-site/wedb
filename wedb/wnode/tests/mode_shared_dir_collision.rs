//! 单机 / 集群两宿主二进制共用同一 `--dir` 物理件互踩回归
//!
//! 对标 C# 单一命名方案（`GarnetServer.cs:479-484` 两臂共用同一
//! `defaultNamingScheme`、`Options.cs:790-793` LogDir/CheckpointDir 单套）：
//! 模式切换复用同一数据文件，检查点目录恒 `{dir}/Store/checkpoints`、WAL 恒
//! `{dir}/wal/wal.log.<段号>` 分段基名，皆与模式无关。
//!
//! 缺陷面（修复前）：两二进制各用 `wedb-standalone.db` / `wedb-cluster.db`
//! 文件名，指向同一 `--dir` 时后者新建空数据文件当恢复设备、加载前者遗留在
//! 共享 `Store/checkpoints` 的索引（索引地址指向另一物理件）、续写同一
//! `wal/wal.log.<段号>` 段文件，恢复「成功」而数据视图为空/错，且原物理件被写坏。
//!
//! 断言（修复后统一为 `{dir}/wedb.db`，`NodeArgs::data_path()` 唯一真源）：
//! 以单机身份写数据 → 改用集群身份指同一 `--dir` → 正常互认恢复，
//! 绝不静默起服空库、绝不再新建异名数据文件、绝不写坏原物理件。

use std::{fs, path::Path, sync::Arc};

use compio::{net::TcpStream, runtime::Runtime};
use tempfile::tempdir;
use wconf::{DATA_FILE, NodeArgs, RuntimeServerOptions};
use wnode::service::StorageSessionProvider;
use wnode_test::{read_bulk_reply, read_line_reply, send_cmd, session_factory, start_server};
use wtest_base::test_store_config;

/// 列出目录条目名（断言无残留异名数据文件）
fn entries(dir: &Path) -> Vec<String> {
  fs::read_dir(dir)
    .expect("read_dir")
    .map(|e| e.expect("entry").file_name().to_string_lossy().into_owned())
    .collect()
}

#[test]
fn standalone_and_cluster_share_one_data_file_and_recover() {
  let rt = Runtime::new().expect("compio runtime");
  let dir = tempdir().expect("tempdir");
  let node_dir = dir.path().to_path_buf();

  // 两宿主二进制均以 NodeArgs::data_path() 取数据路径：同 dir 恒同一物理件
  // （无模式维度）。此处分别以「单机身份」「集群身份」构造 NodeArgs（除 dir
  // 外配置不同亦不影响数据文件名），断言二者路径完全一致。
  let standalone = NodeArgs {
    dir: node_dir.clone(),
    port: 7101,
    ..Default::default()
  };
  let cluster = NodeArgs {
    dir: node_dir.clone(),
    port: 7202,
    ..Default::default()
  };
  let data_path = standalone.data_path();
  assert_eq!(
    data_path,
    cluster.data_path(),
    "单机与集群数据路径必须同源（互踩根因即异名）"
  );
  assert_eq!(data_path, node_dir.join(DATA_FILE));

  // ---- 第一代（单机身份）：SET → SAVE → 增量 SET（走 AOF）→ 停服
  let provider = Arc::new(
    StorageSessionProvider::open_with_config_and_aof(
      test_store_config(),
      &data_path,
      None,
      RuntimeServerOptions::default(),
      session_factory,
    )
    .expect("单机身份 open with aof"),
  );
  let (server, addr) = start_server(Arc::clone(&provider));
  rt.block_on(async {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    send_cmd(&mut stream, &[b"SET", b"shared", b"v1"])
      .await
      .expect("set shared");
    assert!(read_line_reply(&mut stream).await.starts_with(b"+OK"));
    send_cmd(&mut stream, &[b"SAVE"]).await.expect("save");
    assert!(
      read_line_reply(&mut stream).await.starts_with(b"+OK"),
      "SAVE 须成功（检查点落共享 Store/checkpoints）"
    );
    // SAVE 后增量：仅存在于共享 wal/wal.log.<段号> 段文件
    send_cmd(&mut stream, &[b"SET", b"inc", b"v2"])
      .await
      .expect("set inc");
    assert!(read_line_reply(&mut stream).await.starts_with(b"+OK"));
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
  let seg0_path = provider.store().device.segment_path(0);
  // 记录第一代数据物理件字节数（互认恢复须复用而非新建空件）
  let device_len_before = fs::metadata(&seg0_path).expect("device exists").len();
  assert!(device_len_before > 0, "第一代数据文件须非空落盘");
  drop(provider);

  // 第一代目录形态：仅一份统一命名的数据文件，绝无异名残留件
  let names_after_gen1 = entries(&node_dir);
  assert!(
    names_after_gen1.iter().any(|n| n.starts_with(DATA_FILE)),
    "统一命名的数据文件必须存在：{names_after_gen1:?}"
  );
  assert!(
    !names_after_gen1
      .iter()
      .any(|n| n == "wedb-standalone.db" || n == "wedb-cluster.db"),
    "统一命名后不得再出现按模式区分的数据文件：{names_after_gen1:?}"
  );

  // ---- 第二代（集群身份）：指同一 --dir，从共享检查点 + WAL 恢复
  let provider2 = Arc::new(
    rt.block_on(StorageSessionProvider::open_recovered_with_config_and_aof(
      test_store_config(),
      &data_path,
      None,
      RuntimeServerOptions::default(),
      false,
      session_factory,
    ))
    .expect("集群身份 open recovered"),
  );
  let (server2, addr2) = start_server(Arc::clone(&provider2));
  rt.block_on(async {
    let mut stream = TcpStream::connect(addr2).await.expect("reconnect");
    send_cmd(&mut stream, &[b"GET", b"shared"])
      .await
      .expect("get shared");
    assert_eq!(
      read_bulk_reply(&mut stream).await,
      Some(b"v1".to_vec()),
      "检查点内数据须被集群身份互认恢复（绝不起服空库）"
    );
    send_cmd(&mut stream, &[b"GET", b"inc"])
      .await
      .expect("get inc");
    assert_eq!(
      read_bulk_reply(&mut stream).await,
      Some(b"v2".to_vec()),
      "SAVE 后 WAL 增量须被互认重放"
    );
    send_cmd(&mut stream, &[b"QUIT"]).await.expect("quit");
  });
  server2.stop();
  drop(server2);
  drop(provider2);

  // 原数据物理件被复用且未被写坏/清空（新进程续写只会增长，绝不缩回空件）
  let device_len_after = fs::metadata(&seg0_path).expect("device still exists").len();
  assert!(
    device_len_after >= device_len_before,
    "互认恢复须复用原数据件，不得新建空件顶替"
  );
  // 第二代仍未派生任何异名数据文件（同目录仅一份统一命名件）
  let names_after_gen2 = entries(&node_dir);
  assert!(
    names_after_gen2.iter().any(|n| n.starts_with(DATA_FILE)),
    "恢复后统一命名的数据文件必须依然存在：{names_after_gen2:?}"
  );
  assert!(
    !names_after_gen2
      .iter()
      .any(|n| n == "wedb-standalone.db" || n == "wedb-cluster.db"),
    "恢复后仍不得出现异名数据文件：{names_after_gen2:?}"
  );
}
