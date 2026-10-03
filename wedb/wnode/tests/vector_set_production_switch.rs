//! Vector Set 预览生产开关端到端回归（走 `StorageSessionProvider::open_from_args`
//! 生产装配路径——与 boot.rs:66 / wedb_standalone 同一入口，不经测试内
//! `is_enabled: true` 直构）。
//!
//! 对标 C# defaults.conf:533 `EnableVectorSetPreview: false` 默认关 +
//! StoreWrapper.cs:1054 条件装配 + VectorManager.cs:182 构造注入：
//!
//! 1. 开（`--enable-vector-set-preview`）：VADD/VCARD/VSIM 端到端应答，
//!    命令面整栈可达；
//! 2. 关（缺省）：VADD 回统一拒绝文案（命令面与迁移面共用单点定义），
//!    量化/清理后台链不拉起。

use std::sync::Arc;

use compio::{net::TcpStream, runtime::Runtime};
use tempfile::tempdir;
use wconf::NodeArgs;
use wnode::service::StorageSessionProvider;
use wnode_test::{read_line_reply, read_reply, send_cmd, session_factory, start_server};

/// Vector Set 预览未启用统一拒绝文案（resp/vector/mod.rs 单点定义的镜像）
const DISABLED_ERR: &[u8] = b"-ERR Vector Set (preview) commands are not enabled\r\n";

/// 开关开启：生产构造路径下向量命令端到端应答
#[test]
fn vector_set_preview_enabled_end_to_end() {
  let rt = Runtime::new().expect("compio runtime");
  let dir = tempdir().expect("tempdir");
  let node = NodeArgs {
    enable_vector_set_preview: true,
    ..NodeArgs::default()
  };
  let provider = Arc::new(rt.block_on(async {
    StorageSessionProvider::open_from_args(&node, dir.path().join("vec_on.db"), session_factory)
      .await
      .expect("open from args")
  }));
  // 配置投影进管理器（with_vector_set_preview 装配链单点写）
  assert!(provider.vector_manager.is_enabled());

  let (_server, addr) = start_server(Arc::clone(&provider));
  rt.block_on(async {
    let mut stream = TcpStream::connect(addr).await.expect("connect");

    send_cmd(
      &mut stream,
      &[b"VADD", b"k", b"VALUES", b"2", b"1.5", b"-2.5", b"elem1"],
    )
    .await
    .expect("vadd");
    assert_eq!(read_line_reply(&mut stream).await, b":1\r\n");

    send_cmd(&mut stream, &[b"VCARD", b"k"])
      .await
      .expect("vcard");
    assert_eq!(read_line_reply(&mut stream).await, b":1\r\n");

    // VSIM 命中唯一元素（RESP2 数组：*1 → $5 → elem1）
    send_cmd(
      &mut stream,
      &[
        b"VSIM", b"k", b"VALUES", b"2", b"1.5", b"-2.5", b"COUNT", b"1",
      ],
    )
    .await
    .expect("vsim");
    assert_eq!(read_reply(&mut stream).await, b"*1\r\n$5\r\nelem1\r\n");
  });
}

/// 开关缺省关闭：命令面统一拒绝，管理器恒假（后台链由 get_session 门控不拉起）
#[test]
fn vector_set_preview_disabled_by_default() {
  let rt = Runtime::new().expect("compio runtime");
  let dir = tempdir().expect("tempdir");
  let node = NodeArgs::default();
  let provider = Arc::new(rt.block_on(async {
    StorageSessionProvider::open_from_args(&node, dir.path().join("vec_off.db"), session_factory)
      .await
      .expect("open from args")
  }));
  assert!(!provider.vector_manager.is_enabled());

  let (_server, addr) = start_server(Arc::clone(&provider));
  rt.block_on(async {
    let mut stream = TcpStream::connect(addr).await.expect("connect");

    send_cmd(
      &mut stream,
      &[b"VADD", b"k", b"VALUES", b"2", b"1.5", b"-2.5", b"elem1"],
    )
    .await
    .expect("vadd");
    assert_eq!(read_line_reply(&mut stream).await, DISABLED_ERR);

    send_cmd(&mut stream, &[b"VCARD", b"k"])
      .await
      .expect("vcard");
    assert_eq!(read_line_reply(&mut stream).await, DISABLED_ERR);
  });
}
