//! 对象信封带外通道回归测试（对标 C# UnifiedStore ReadMethods:HandleObjectEncoding
//! 的 ValueIsObject 位语义）
//!
//! 缺陷背景：对象信封与用户字符串曾共用 KeyTag::String 记录通道，靠「值首字节
//! ∈ 0x01..=0x04」嗅探区分——用户 SET k "\x01..." 后 GET 误回 WRONGTYPE、TYPE
//! 误回 zset，且该类值被 AOF 写监听排除（主库重启后键丢失）。修复后对象记录挂
//! KeyTag::ObjectEnvelope 物理键，字符串值内容任意：
//! 1. 二进制字符串值全命令面正确（GET/TYPE/APPEND/SETRANGE/INCR/EXISTS/KEYS）；
//! 2. 该类键经 AOF 重启恢复仍在（数据丢失回归）；
//! 3. 集合键跨型语义（GET→WRONGTYPE、TYPE=hash、DEL/RENAME/EXPIRE/TTL 正常）；
//! 4. SCAN TYPE=string 不排除首字节 0x01..0x04 的字符串键，KEYS 含集合键。

use std::{net::SocketAddr, path::Path, sync::Arc};

use compio::{net::TcpStream, runtime::Runtime};
use tempfile::tempdir;
use wconf::RuntimeServerOptions;
use wnode::{GarnetServer, service::StorageSessionProvider};
use wnode_test::{
  SessionFactory, complete_len, read_bulk_reply, read_reply, send_cmd, session_factory,
  start_server,
};
use wtest_base::test_store_config;

/// 冷启动 / AOF 恢复装配的 provider 具体形态（decorate 收敛为函数指针后方可命名，
/// 对齐 `wnode_test::start_node` 的同款处理）与服务句柄别名
type Provider = StorageSessionProvider<SessionFactory>;
type Server = Arc<GarnetServer<Provider>>;

/// 读取一条完整行式应答（+OK / -ERR / :N）
///
/// 基于 [`wnode_test::read_reply`] 的 String 断言薄封装（IO 收帧逻辑
/// 统一走 wnode_test，本地仅做 trim + utf8 转换以匹配 `assert_eq!` 字符串风格）
async fn read_line_reply(stream: &mut TcpStream) -> String {
  let raw = read_reply(stream).await;
  String::from_utf8_lossy(&raw).trim_end().to_string()
}

/// 读取数组应答并平铺出全部 bulk 元素（KEYS / SCAN；嵌套数组递归展开）
async fn read_array_reply(stream: &mut TcpStream) -> Vec<Vec<u8>> {
  let raw = read_reply(stream).await;
  let mut items = Vec::new();
  collect_array_items(&raw, &mut items);
  items
}

/// 从完整 RESP 数组字节中按帧递归收集全部 bulk 元素
fn collect_array_items(data: &[u8], items: &mut Vec<Vec<u8>>) {
  let Some(&kind) = data.first() else { return };
  let Some(nl) = data.iter().position(|&b| b == b'\n') else {
    return;
  };
  match kind {
    b'$' => {
      let header: i64 = String::from_utf8_lossy(&data[1..nl])
        .trim_end()
        .parse()
        .unwrap_or(-2);
      if header >= 0 {
        items.push(data[nl + 1..nl + 1 + header as usize].to_vec());
      }
    }
    b'*' => {
      let n: i64 = String::from_utf8_lossy(&data[1..nl])
        .trim_end()
        .parse()
        .unwrap_or(0);
      let mut rest = &data[nl + 1..];
      for _ in 0..n {
        let Some(used) = complete_len(rest) else {
          return;
        };
        collect_array_items(&rest[..used], items);
        rest = &rest[used..];
      }
    }
    _ => {}
  }
}

/// 冷启动装配（固定测试配置）：开 AOF 引擎并拉起会话服务，返回 (provider, server, addr)。
///
/// 唯一入参差异项 `data_path`（各用例的 node 子目录下的库文件名）原样透传；其余
/// （`test_store_config()` / `wal_dir=None` / `RuntimeServerOptions::default()` /
/// `session_factory`）九处内联装配完全一致，收敛于此。`provider` 一并返回交调用方持有：
/// AOF 重启用例需经 `provider.aof()` 主动落盘，且须存活至 `server.stop()` 之后（析构
/// 顺序与内联装配一致）；纯读用例以 `_provider` 绑定即可。
fn open_serve(data_path: &Path) -> (Arc<Provider>, Server, SocketAddr) {
  let provider = Arc::new(
    StorageSessionProvider::open_with_config_and_aof(
      test_store_config(),
      data_path,
      None,
      RuntimeServerOptions::default(),
      session_factory as SessionFactory,
    )
    .expect("open with aof"),
  );
  let (server, addr) = start_server(Arc::clone(&provider));
  (provider, server, addr)
}

/// AOF 恢复装配（固定测试配置）：`open_recovered_with_config_and_aof` 重放后拉起会话服务。
///
/// 与 [`open_serve`] 同款收敛，唯一差异项 `data_path`（须与第一代同路径）透传；
/// `vector_preview=false` 六处内联一致。`rt` 供异步恢复臂 `block_on` 驱动。
fn open_serve_recovered(rt: &Runtime, data_path: &Path) -> (Arc<Provider>, Server, SocketAddr) {
  let provider = Arc::new(
    rt.block_on(StorageSessionProvider::open_recovered_with_config_and_aof(
      test_store_config(),
      data_path,
      None,
      RuntimeServerOptions::default(),
      false,
      session_factory as SessionFactory,
    ))
    .expect("open recovered"),
  );
  let (server, addr) = start_server(Arc::clone(&provider));
  (provider, server, addr)
}

/// 场景 1：二进制字符串值全命令面正确（值首字节 0x01 不再误判为对象信封）
#[test]
fn binary_string_value_full_command_surface() {
  let rt = Runtime::new().expect("compio runtime");
  let dir = tempdir().expect("tempdir");
  let data_path = dir.path().join("node").join("binary_string.db");
  let (_provider, server, addr) = open_serve(&data_path);
  rt.block_on(async {
    let mut s = TcpStream::connect(addr).await.expect("connect");

    // SET "\x01\x02bin"：旧实现靠值首字节嗅探，此处值会被误判为对象信封
    let bin = b"\x01\x02bin";
    send_cmd(&mut s, &[b"SET", b"k", bin]).await.expect("set");
    assert_eq!(read_line_reply(&mut s).await, "+OK");

    // GET 回原值（旧实现回 WRONGTYPE）
    send_cmd(&mut s, &[b"GET", b"k"]).await.expect("get");
    assert_eq!(read_bulk_reply(&mut s).await, Some(bin.to_vec()));

    // TYPE=string（旧实现回 zset）
    send_cmd(&mut s, &[b"TYPE", b"k"]).await.expect("type");
    assert_eq!(read_line_reply(&mut s).await, "+string");

    // STRLEN / APPEND
    send_cmd(&mut s, &[b"STRLEN", b"k"]).await.expect("strlen");
    assert_eq!(read_line_reply(&mut s).await, ":5");
    send_cmd(&mut s, &[b"APPEND", b"k", b"!"])
      .await
      .expect("append");
    assert_eq!(read_line_reply(&mut s).await, ":6");
    send_cmd(&mut s, &[b"GET", b"k"]).await.expect("get2");
    assert_eq!(
      read_bulk_reply(&mut s).await,
      Some(b"\x01\x02bin!".to_vec())
    );

    // SETRANGE 正常
    send_cmd(&mut s, &[b"SETRANGE", b"k", b"6", b"?"])
      .await
      .expect("setrange");
    assert_eq!(read_line_reply(&mut s).await, ":7");

    // GETRANGE 正常
    send_cmd(&mut s, &[b"GETRANGE", b"k", b"0", b"1"])
      .await
      .expect("getrange");
    assert_eq!(read_bulk_reply(&mut s).await, Some(b"\x01\x02".to_vec()));

    // INCR 报 not-integer 而非 WRONGTYPE
    send_cmd(&mut s, &[b"INCR", b"k"]).await.expect("incr");
    let err = read_line_reply(&mut s).await;
    assert!(err.starts_with('-'), "INCR 须回错误帧: {err}");
    assert!(
      err.contains("not an integer"),
      "INCR 须报 not-integer: {err}"
    );
    assert!(!err.contains("WRONGTYPE"), "INCR 不得报 WRONGTYPE: {err}");

    // EXISTS 正常
    send_cmd(&mut s, &[b"EXISTS", b"k"]).await.expect("exists");
    assert_eq!(read_line_reply(&mut s).await, ":1");
  });
  server.stop();
}

/// 场景 2：二进制字符串值经 AOF 重启恢复仍在（数据丢失回归）
#[test]
fn binary_string_survives_aof_restart() {
  let rt = Runtime::new().expect("compio runtime");
  let dir = tempdir().expect("tempdir");
  let data_path = dir.path().join("node").join("binary_aof.db");

  // ---- 第一代：SET "\x01hello" → flush AOF → 停服
  let (provider, server, addr) = open_serve(&data_path);
  rt.block_on(async {
    let mut s = TcpStream::connect(addr).await.expect("connect");
    send_cmd(&mut s, &[b"SET", b"k", b"\x01hello"])
      .await
      .expect("set");
    assert_eq!(read_line_reply(&mut s).await, "+OK");
    send_cmd(&mut s, &[b"DEL", b"gone"]).await.expect("del");
    assert_eq!(read_line_reply(&mut s).await, ":0");
    provider
      .aof()
      .expect("aof enabled")
      .log()
      .commit_async()
      .await;
    send_cmd(&mut s, &[b"QUIT"]).await.expect("quit");
  });
  server.stop();
  drop(server);
  drop(provider);

  // ---- 第二代：AOF 重放后键仍在且值原样
  let (_provider2, server2, addr2) = open_serve_recovered(&rt, &data_path);
  rt.block_on(async {
    let mut s = TcpStream::connect(addr2).await.expect("reconnect");
    send_cmd(&mut s, &[b"GET", b"k"]).await.expect("get");
    assert_eq!(
      read_bulk_reply(&mut s).await,
      Some(b"\x01hello".to_vec()),
      "首字节 0x01 的字符串键必须经 AOF 恢复（旧实现被嗅探排除，重启丢键）"
    );
    send_cmd(&mut s, &[b"TYPE", b"k"]).await.expect("type");
    assert_eq!(read_line_reply(&mut s).await, "+string");
  });
  server2.stop();
}

/// 场景 3：集合键跨型语义与 DEL/RENAME/EXPIRE/TTL 生命周期
#[test]
fn collection_key_cross_type_semantics() {
  let rt = Runtime::new().expect("compio runtime");
  let dir = tempdir().expect("tempdir");
  let data_path = dir.path().join("node").join("collection.db");
  let (_provider, server, addr) = open_serve(&data_path);
  rt.block_on(async {
    let mut s = TcpStream::connect(addr).await.expect("connect");

    // HSET 建 hash
    send_cmd(&mut s, &[b"HSET", b"h", b"f", b"v"])
      .await
      .expect("hset");
    assert_eq!(read_line_reply(&mut s).await, ":1");

    // GET → WRONGTYPE（不是 nil）
    send_cmd(&mut s, &[b"GET", b"h"]).await.expect("get h");
    let err = read_line_reply(&mut s).await;
    assert!(err.contains("WRONGTYPE"), "GET 集合键须回 WRONGTYPE: {err}");

    // TYPE → hash
    send_cmd(&mut s, &[b"TYPE", b"h"]).await.expect("type h");
    assert_eq!(read_line_reply(&mut s).await, "+hash");

    // HGET 正常
    send_cmd(&mut s, &[b"HGET", b"h", b"f"])
      .await
      .expect("hget");
    assert_eq!(read_bulk_reply(&mut s).await, Some(b"v".to_vec()));

    // EXISTS → 1
    send_cmd(&mut s, &[b"EXISTS", b"h"])
      .await
      .expect("exists h");
    assert_eq!(read_line_reply(&mut s).await, ":1");

    // EXPIRE + TTL
    send_cmd(&mut s, &[b"EXPIRE", b"h", b"100"])
      .await
      .expect("expire");
    assert_eq!(read_line_reply(&mut s).await, ":1");
    send_cmd(&mut s, &[b"TTL", b"h"]).await.expect("ttl");
    let ttl: i64 = read_line_reply(&mut s)
      .await
      .trim_start_matches(':')
      .parse()
      .expect("ttl int");
    assert!((0..=100).contains(&ttl), "TTL 须在 (0,100]: {ttl}");
    send_cmd(&mut s, &[b"PERSIST", b"h"])
      .await
      .expect("persist");
    assert_eq!(read_line_reply(&mut s).await, ":1");
    send_cmd(&mut s, &[b"TTL", b"h"]).await.expect("ttl2");
    assert_eq!(read_line_reply(&mut s).await, ":-1");

    // RENAME 保留对象类型
    send_cmd(&mut s, &[b"RENAME", b"h", b"h3"])
      .await
      .expect("rename");
    assert_eq!(read_line_reply(&mut s).await, "+OK");
    send_cmd(&mut s, &[b"TYPE", b"h3"]).await.expect("type h3");
    assert_eq!(read_line_reply(&mut s).await, "+hash");
    send_cmd(&mut s, &[b"HGET", b"h3", b"f"])
      .await
      .expect("hget h3");
    assert_eq!(read_bulk_reply(&mut s).await, Some(b"v".to_vec()));
    send_cmd(&mut s, &[b"EXISTS", b"h"])
      .await
      .expect("old gone");
    assert_eq!(read_line_reply(&mut s).await, ":0");

    // DEL 集合键
    send_cmd(&mut s, &[b"DEL", b"h3"]).await.expect("del h3");
    assert_eq!(read_line_reply(&mut s).await, ":1");
    send_cmd(&mut s, &[b"EXISTS", b"h3"])
      .await
      .expect("h3 gone");
    assert_eq!(read_line_reply(&mut s).await, ":0");

    // SET 覆写集合键：键变为 string，HGET 须 WRONGTYPE
    send_cmd(&mut s, &[b"HSET", b"ov", b"f", b"v"])
      .await
      .expect("hset ov");
    assert_eq!(read_line_reply(&mut s).await, ":1");
    send_cmd(&mut s, &[b"SET", b"ov", b"str"])
      .await
      .expect("set ov");
    assert_eq!(read_line_reply(&mut s).await, "+OK");
    send_cmd(&mut s, &[b"TYPE", b"ov"]).await.expect("type ov");
    assert_eq!(read_line_reply(&mut s).await, "+string");
    send_cmd(&mut s, &[b"HGET", b"ov", b"f"])
      .await
      .expect("hget ov");
    let hget_reply = read_line_reply(&mut s).await;
    assert!(
      hget_reply.contains("WRONGTYPE"),
      "SET 覆写后 HGET 须 WRONGTYPE（不得残留信封幽灵记录）: {hget_reply}"
    );
  });
  server.stop();
}

/// 场景 4：SCAN TYPE=string 不排除首字节 0x01..0x04 的字符串键；KEYS 含集合键
#[test]
fn scan_type_and_keys_cover_both_domains() {
  let rt = Runtime::new().expect("compio runtime");
  let dir = tempdir().expect("tempdir");
  let data_path = dir.path().join("node").join("scan.db");
  let (_provider, server, addr) = open_serve(&data_path);
  rt.block_on(async {
    let mut s = TcpStream::connect(addr).await.expect("connect");

    // 数据面：首字节 0x01 的字符串键 + hash 集合键 + 普通字符串键
    send_cmd(&mut s, &[b"SET", b"b1", b"\x01\x02x"])
      .await
      .expect("set b1");
    assert_eq!(read_line_reply(&mut s).await, "+OK");
    send_cmd(&mut s, &[b"HSET", b"h", b"f", b"v"])
      .await
      .expect("hset");
    assert_eq!(read_line_reply(&mut s).await, ":1");
    send_cmd(&mut s, &[b"SET", b"plain", b"v"])
      .await
      .expect("set plain");
    assert_eq!(read_line_reply(&mut s).await, "+OK");

    // KEYS * 包含集合键与字符串键
    send_cmd(&mut s, &[b"KEYS", b"*"]).await.expect("keys");
    let keys = read_array_reply(&mut s).await;
    for want in ["b1", "h", "plain"] {
      assert!(
        keys.iter().any(|k| k.as_slice() == want.as_bytes()),
        "KEYS 须包含 {want}: {keys:?}"
      );
    }

    // SCAN TYPE=string 包含 b1（旧实现按值首字节嗅探将其排除）
    send_cmd(
      &mut s,
      &[b"SCAN", b"0", b"TYPE", b"string", b"COUNT", b"100"],
    )
    .await
    .expect("scan string");
    let strings = read_array_reply(&mut s).await;
    assert!(
      strings.iter().any(|k| k.as_slice() == b"b1"),
      "SCAN TYPE=string 须包含首字节 0x01 的字符串键: {strings:?}"
    );
    assert!(
      !strings.iter().any(|k| k.as_slice() == b"h"),
      "SCAN TYPE=string 不得包含集合键: {strings:?}"
    );

    // SCAN TYPE=hash 包含集合键 h
    send_cmd(&mut s, &[b"SCAN", b"0", b"TYPE", b"hash", b"COUNT", b"100"])
      .await
      .expect("scan hash");
    let hashes = read_array_reply(&mut s).await;
    assert!(
      hashes.iter().any(|k| k.as_slice() == b"h"),
      "SCAN TYPE=hash 须包含 hash 键: {hashes:?}"
    );
    assert!(
      !hashes.iter().any(|k| k.as_slice() == b"b1"),
      "SCAN TYPE=hash 不得包含字符串键: {hashes:?}"
    );
  });
  server.stop();
}

/// 场景 5：对象键 RENAME 经 AOF 重启恢复后新键在、旧键消失、类型正确
///
/// 缺陷背景：rename_sync Obj 域新键 try_upsert_tag_sync 被写监听「信封非墓碑
/// 跳过」分流排除且无显式入账——AOF 只记旧键 StoreDelete，重放后集合键丢失
#[test]
fn object_rename_survives_aof_restart() {
  let rt = Runtime::new().expect("compio runtime");
  let dir = tempdir().expect("tempdir");
  let data_path = dir.path().join("node").join("rename_aof.db");

  // ---- 第一代：建 hash → RENAME → flush → 停服
  let (provider, server, addr) = open_serve(&data_path);
  rt.block_on(async {
    let mut s = TcpStream::connect(addr).await.expect("connect");
    send_cmd(&mut s, &[b"HSET", b"old", b"f1", b"v1"])
      .await
      .expect("hset");
    assert_eq!(read_line_reply(&mut s).await, ":1");
    send_cmd(&mut s, &[b"HSET", b"old", b"f2", b"v2"])
      .await
      .expect("hset2");
    assert_eq!(read_line_reply(&mut s).await, ":1");

    send_cmd(&mut s, &[b"RENAME", b"old", b"new"])
      .await
      .expect("rename");
    assert_eq!(read_line_reply(&mut s).await, "+OK");

    provider
      .aof()
      .expect("aof enabled")
      .log()
      .commit_async()
      .await;
    send_cmd(&mut s, &[b"QUIT"]).await.expect("quit");
  });
  server.stop();
  drop(server);
  drop(provider);

  // ---- 第二代：AOF 重放后新键在（类型正确、载荷完整）、旧键消失
  let (_provider2, server2, addr2) = open_serve_recovered(&rt, &data_path);
  rt.block_on(async {
    let mut s = TcpStream::connect(addr2).await.expect("reconnect");

    send_cmd(&mut s, &[b"TYPE", b"new"])
      .await
      .expect("type new");
    assert_eq!(
      read_line_reply(&mut s).await,
      "+hash",
      "RENAME 新键必须经 AOF 恢复（旧实现新键信封写入不入账，重启后集合键丢失）"
    );
    send_cmd(&mut s, &[b"HGET", b"new", b"f1"])
      .await
      .expect("hget f1");
    assert_eq!(read_bulk_reply(&mut s).await, Some(b"v1".to_vec()));
    send_cmd(&mut s, &[b"HGET", b"new", b"f2"])
      .await
      .expect("hget f2");
    assert_eq!(read_bulk_reply(&mut s).await, Some(b"v2".to_vec()));

    send_cmd(&mut s, &[b"EXISTS", b"old"])
      .await
      .expect("old gone");
    assert_eq!(read_line_reply(&mut s).await, ":0");
  });
  server2.stop();
}

/// 场景 6：GEOADD（resp 层同步散点写回 zset_save_or_gc）经 AOF 重启恢复后
/// 状态一致
///
/// 缺陷背景：GEOADD 直写 obj_save_or_gc，既不经 run_sync_rmw 的
/// ObjectStoreRMW 通知也不经 StorageSession::upsert_tag——回写零 AOF 记录，
/// 重启后集合键整个丢失
#[test]
fn geoadd_survives_aof_restart() {
  let rt = Runtime::new().expect("compio runtime");
  let dir = tempdir().expect("tempdir");
  let data_path = dir.path().join("node").join("geoadd_aof.db");

  // ---- 第一代：GEOADD 建有序集合 → flush → 停服
  let (provider, server, addr) = open_serve(&data_path);
  rt.block_on(async {
    let mut s = TcpStream::connect(addr).await.expect("connect");
    send_cmd(&mut s, &[b"GEOADD", b"geo", b"13.36", b"38.11", b"palermo"])
      .await
      .expect("geoadd");
    assert_eq!(read_line_reply(&mut s).await, ":1");

    provider
      .aof()
      .expect("aof enabled")
      .log()
      .commit_async()
      .await;
    send_cmd(&mut s, &[b"QUIT"]).await.expect("quit");
  });
  server.stop();
  drop(server);
  drop(provider);

  // ---- 第二代：AOF 重放后集合键在（禁用入账时重放零记录，键丢失）
  let (_provider2, server2, addr2) = open_serve_recovered(&rt, &data_path);
  rt.block_on(async {
    let mut s = TcpStream::connect(addr2).await.expect("reconnect");

    send_cmd(&mut s, &[b"TYPE", b"geo"])
      .await
      .expect("type geo");
    assert_eq!(
      read_line_reply(&mut s).await,
      "+zset",
      "GEOADD 写回必须经 AOF 恢复（旧实现散点写回不入账，重启后集合键丢失）"
    );
    send_cmd(&mut s, &[b"ZCARD", b"geo"]).await.expect("zcard");
    assert_eq!(read_line_reply(&mut s).await, ":1");
    send_cmd(&mut s, &[b"ZSCORE", b"geo", b"palermo"])
      .await
      .expect("zscore");
    assert!(
      read_bulk_reply(&mut s).await.is_some(),
      "GEOADD 成员必须经 AOF 恢复"
    );
  });
  server2.stop();
}

/// 场景 6b：HCOLLECT 单键（resp 层同步散点写回）经 AOF 重启恢复后状态一致
#[test]
fn hcollect_single_key_survives_aof_restart() {
  let rt = Runtime::new().expect("compio runtime");
  let dir = tempdir().expect("tempdir");
  let data_path = dir.path().join("node").join("hcollect_aof.db");

  // ---- 第一代：建 hash → HCOLLECT 回写 → flush → 停服
  let (provider, server, addr) = open_serve(&data_path);
  rt.block_on(async {
    let mut s = TcpStream::connect(addr).await.expect("connect");
    send_cmd(&mut s, &[b"HSET", b"h", b"f", b"v"])
      .await
      .expect("hset");
    assert_eq!(read_line_reply(&mut s).await, ":1");

    send_cmd(&mut s, &[b"HCOLLECT", b"h"])
      .await
      .expect("hcollect");
    assert_eq!(read_line_reply(&mut s).await, "+OK");

    provider
      .aof()
      .expect("aof enabled")
      .log()
      .commit_async()
      .await;
    send_cmd(&mut s, &[b"QUIT"]).await.expect("quit");
  });
  server.stop();
  drop(server);
  drop(provider);

  // ---- 第二代：AOF 重放后 HCOLLECT 写回的载荷仍在
  let (_provider2, server2, addr2) = open_serve_recovered(&rt, &data_path);
  rt.block_on(async {
    let mut s = TcpStream::connect(addr2).await.expect("reconnect");
    send_cmd(&mut s, &[b"TYPE", b"h"]).await.expect("type h");
    assert_eq!(read_line_reply(&mut s).await, "+hash");
    send_cmd(&mut s, &[b"HGET", b"h", b"f"])
      .await
      .expect("hget");
    assert_eq!(
      read_bulk_reply(&mut s).await,
      Some(b"v".to_vec()),
      "HCOLLECT 回写必须经 AOF 恢复（旧实现散点写回不入账，重启后回到旧载荷）"
    );
  });
  server2.stop();
}

/// 场景 7：HCOLLECT `*` 全库批扫（storage 层异步段 hash_collect → obj_save）
/// 经 AOF 重启恢复后状态一致
#[test]
fn hcollect_scan_all_survives_aof_restart() {
  let rt = Runtime::new().expect("compio runtime");
  let dir = tempdir().expect("tempdir");
  let data_path = dir.path().join("node").join("hcollect_all_aof.db");

  // ---- 第一代：建 hash → HCOLLECT *（同步快路径不可达，降级 storage 层
  //      异步闭环）→ flush → 停服
  let (provider, server, addr) = open_serve(&data_path);
  rt.block_on(async {
    let mut s = TcpStream::connect(addr).await.expect("connect");
    send_cmd(&mut s, &[b"HSET", b"h", b"f", b"v"])
      .await
      .expect("hset");
    assert_eq!(read_line_reply(&mut s).await, ":1");

    send_cmd(&mut s, &[b"HCOLLECT", b"*"])
      .await
      .expect("hcollect all");
    assert_eq!(read_line_reply(&mut s).await, "+OK");

    provider
      .aof()
      .expect("aof enabled")
      .log()
      .commit_async()
      .await;
    send_cmd(&mut s, &[b"QUIT"]).await.expect("quit");
  });
  server.stop();
  drop(server);
  drop(provider);

  // ---- 第二代：AOF 重放后异步段写回的载荷仍在
  let (_provider2, server2, addr2) = open_serve_recovered(&rt, &data_path);
  rt.block_on(async {
    let mut s = TcpStream::connect(addr2).await.expect("reconnect");
    send_cmd(&mut s, &[b"TYPE", b"h"]).await.expect("type h");
    assert_eq!(read_line_reply(&mut s).await, "+hash");
    send_cmd(&mut s, &[b"HGET", b"h", b"f"])
      .await
      .expect("hget");
    assert_eq!(
      read_bulk_reply(&mut s).await,
      Some(b"v".to_vec()),
      "HCOLLECT 批扫（异步段）回写必须经 AOF 恢复"
    );
  });
  server2.stop();
}

/// 场景 8：集合删空自愈（SPOP 弹空整键回收）经 AOF 重启后不复活
#[test]
fn collection_self_gc_survives_aof_restart() {
  let rt = Runtime::new().expect("compio runtime");
  let dir = tempdir().expect("tempdir");
  let data_path = dir.path().join("node").join("pop_empty_aof.db");

  // ---- 第一代：建 set → SPOP 弹空（整键回收）→ flush → 停服
  let (provider, server, addr) = open_serve(&data_path);
  rt.block_on(async {
    let mut s = TcpStream::connect(addr).await.expect("connect");
    send_cmd(&mut s, &[b"SADD", b"s", b"m"])
      .await
      .expect("sadd");
    assert_eq!(read_line_reply(&mut s).await, ":1");

    send_cmd(&mut s, &[b"SPOP", b"s"]).await.expect("spop");
    let popped = read_bulk_reply(&mut s).await;
    assert_eq!(
      popped.as_deref(),
      Some(b"m".as_slice()),
      "SPOP 弹出唯一成员"
    );

    send_cmd(&mut s, &[b"EXISTS", b"s"]).await.expect("exists");
    assert_eq!(read_line_reply(&mut s).await, ":0");

    provider
      .aof()
      .expect("aof enabled")
      .log()
      .commit_async()
      .await;
    send_cmd(&mut s, &[b"QUIT"]).await.expect("quit");
  });
  server.stop();
  drop(server);
  drop(provider);

  // ---- 第二代：AOF 重放后删空自愈仍生效（SADD 条目 + 墓碑 StoreDelete，
  //      重放先建后删，键不得复活）
  let (_provider2, server2, addr2) = open_serve_recovered(&rt, &data_path);
  rt.block_on(async {
    let mut s = TcpStream::connect(addr2).await.expect("reconnect");
    send_cmd(&mut s, &[b"TYPE", b"s"]).await.expect("type s");
    assert_eq!(
      read_line_reply(&mut s).await,
      "+none",
      "删空自愈墓碑必须入 AOF，重放后集合键不得复活"
    );
    send_cmd(&mut s, &[b"SCARD", b"s"]).await.expect("scard");
    assert_eq!(read_line_reply(&mut s).await, ":0");
  });
  server2.stop();
}
