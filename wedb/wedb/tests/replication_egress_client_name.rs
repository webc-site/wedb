#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
use std::{str::from_utf8, sync::Arc};

use compio::{net::TcpStream, runtime::Runtime};
use tempfile::tempdir;
use wedb::client::GarnetClient;
use wnode::{
  resp::{RespSessionConsumer, resp_server_session::RespServerSessionOptions},
  service::StorageSessionProvider,
};
use wnode_test::{cmd, read_reply, send_cmd, start_server};
use wtest_base::test_store_config;

fn bulk(frame: &[u8]) -> String {
  assert!(frame.starts_with(b"$"), "期望 bulk 帧: {frame:?}");
  let nl = frame.iter().position(|&b| b == b'\n').expect("bulk header");
  let len: usize = from_utf8(&frame[1..nl - 1])
    .expect("len utf8")
    .parse()
    .expect("len");
  String::from_utf8(frame[nl + 1..nl + 1 + len].to_vec()).expect("body utf8")
}

#[test]
fn egress_client_names_match_garnet_contract() {
  // 1. assembly 链：对标 C# ReplicaDiskbasedSync.cs:115 clientName: nameof(TryReplicateDiskbasedSyncAsync)
  let c1 = GarnetClient::with_auth(
    "127.0.0.1:6379".into(),
    None,
    None,
    Some("TryReplicateDiskbasedSyncAsync".into()),
  );
  assert_eq!(c1.client_name(), Some("TryReplicateDiskbasedSyncAsync"));

  // 2. AofSyncTask 扇出链：对标 C# AofSyncTask.cs:141 clientName: $"AofSyncTask-{idx}:({endpoint})"
  let endpoint = "127.0.0.1:7001";
  let c2 = GarnetClient::with_auth(
    endpoint.into(),
    None,
    None,
    Some(format!("AofSyncTask:({endpoint})")),
  );
  assert_eq!(c2.client_name(), Some("AofSyncTask:(127.0.0.1:7001)"));

  // 3. checkpoint 发送链：对标 C# ReplicaSyncSession.cs:106 clientName: nameof(ReplicaSyncSession.SendCheckpointAsync)
  let c3 = GarnetClient::with_auth(
    "127.0.0.1:6379".into(),
    None,
    None,
    Some("SendCheckpointAsync".into()),
  );
  assert_eq!(c3.client_name(), Some("SendCheckpointAsync"));

  // 4. 无名链（如 replica_diskless_sync / migrate 等）：缺省为 None
  let c4 = GarnetClient::with_auth("127.0.0.1:6379".into(), None, None, None);
  assert_eq!(c4.client_name(), None);
}

#[test]
fn client_list_reflects_egress_diagnostic_names() -> aok::Result<()> {
  let dir = tempdir()?;
  let provider = Arc::new(StorageSessionProvider::open_with_config(
    test_store_config(),
    dir.path().join("node.db"),
    |sender_id, api| {
      Some(RespSessionConsumer::new(
        sender_id,
        RespServerSessionOptions::default(),
        Arc::new(api),
      ))
    },
  )?);
  let (server, addr) = start_server(provider);
  let rt = Runtime::new()?;

  rt.block_on(async {
    // 观察连接
    let mut observer = TcpStream::connect(addr).await?;
    assert_eq!(cmd(&mut observer, &[b"PING"]).await, b"+PONG\r\n");

    // 1. assembly 链
    let client1 = GarnetClient::with_auth(
      addr.to_string(),
      None,
      None,
      Some("TryReplicateDiskbasedSyncAsync".into()),
    );
    client1.connect_async().await?;
    assert!(client1.is_connected());

    // 2. 扇出链
    let client2 = GarnetClient::with_auth(
      addr.to_string(),
      None,
      None,
      Some(format!("AofSyncTask:({addr})")),
    );
    client2.connect_async().await?;
    assert!(client2.is_connected());

    // 3. checkpoint 发送链
    let client3 = GarnetClient::with_auth(
      addr.to_string(),
      None,
      None,
      Some("SendCheckpointAsync".into()),
    );
    client3.connect_async().await?;
    assert!(client3.is_connected());

    // 查询 CLIENT LIST
    send_cmd(&mut observer, &[b"CLIENT", b"LIST"]).await?;
    let list = bulk(&read_reply(&mut observer).await);

    assert!(
      list.contains("name=TryReplicateDiskbasedSyncAsync"),
      "CLIENT LIST 必须包含 TryReplicateDiskbasedSyncAsync: {list}"
    );
    assert!(
      list.contains(&format!("name=AofSyncTask:({addr})")),
      "CLIENT LIST 必须包含 AofSyncTask:({addr}): {list}"
    );
    assert!(
      list.contains("name=SendCheckpointAsync"),
      "CLIENT LIST 必须包含 SendCheckpointAsync: {list}"
    );

    client1.dispose();
    client2.dispose();
    client3.dispose();

    Ok::<(), aok::Error>(())
  })?;

  server.stop();
  Ok(())
}
