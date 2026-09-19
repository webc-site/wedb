//! JSON 命令端到端 RESP 集成测试

use std::sync::Arc;

use compio::runtime::Runtime;
use parking_lot::Mutex;
use tempfile::tempdir;
use wnode::{
  MessageConsumerFace, SessionProviderFace, WireFormat,
  resp::{
    garnet_api::GarnetApi, resp_server_session::RespServerSessionOptions,
    resp_session_consumer::RespSessionConsumer,
  },
  service::StorageSessionProvider,
};
use wtest_base::{resp_frame, test_store_config};

fn setup_with_api(db: &str) -> (RespSessionConsumer, GarnetApi) {
  let dir = tempdir().expect("tempdir");
  let shared_api = Arc::new(Mutex::new(None));
  let api_capture = shared_api.clone();
  let provider = StorageSessionProvider::open_with_config(
    test_store_config(),
    dir.path().join(db),
    move |sender_id, api| {
      let g_api = Arc::new(api);
      *api_capture.lock() = Some(g_api.clone());
      Some(RespSessionConsumer::new(
        sender_id,
        RespServerSessionOptions::default(),
        g_api,
      ))
    },
  )
  .expect("provider open");
  let session = provider
    .get_session(WireFormat::Ascii, 1)
    .expect("session create");
  let api = shared_api.lock().take().expect("api captured");
  (session, api)
}

fn setup(db: &str) -> RespSessionConsumer {
  setup_with_api(db).0
}

fn exec(consumer: &mut RespSessionConsumer, parts: &[&[u8]]) -> Vec<u8> {
  let req = resp_frame(parts);
  let mut resp = Vec::new();
  let mut scratch = consumer.take_recv_scratch();
  scratch.extend_from_slice(&req);
  consumer.return_recv_scratch(scratch);
  let consumed = consumer.try_consume_messages_into(&mut resp);
  assert_eq!(consumed, Some(0), "命令须整段消费: {parts:?}");
  resp
}

#[test]
fn resp_json_set_get_del_type_mget() -> aok::Result<()> {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let mut s = setup("json_basic.db");

    // JSON.SET k1 $ {"a":1,"b":[1,2,3]}
    assert_eq!(
      exec(
        &mut s,
        &[b"JSON.SET", b"k1", b"$", b"{\"a\":1,\"b\":[1,2,3]}"]
      ),
      b"+OK\r\n"
    );

    // JSON.GET k1
    assert_eq!(
      exec(&mut s, &[b"JSON.GET", b"k1"]),
      b"$19\r\n{\"a\":1,\"b\":[1,2,3]}\r\n"
    );

    // JSON.GET k1 $
    assert_eq!(
      exec(&mut s, &[b"JSON.GET", b"k1", b"$"]),
      b"$21\r\n[{\"a\":1,\"b\":[1,2,3]}]\r\n"
    );

    // JSON.GET k1 $.a
    assert_eq!(
      exec(&mut s, &[b"JSON.GET", b"k1", b"$.a"]),
      b"$3\r\n[1]\r\n"
    );

    // JSON.TYPE k1
    assert_eq!(exec(&mut s, &[b"JSON.TYPE", b"k1"]), b"+object\r\n");

    // JSON.TYPE k1 $.b
    assert_eq!(
      exec(&mut s, &[b"JSON.TYPE", b"k1", b"$.b"]),
      b"*1\r\n+array\r\n"
    );

    // JSON.SET k2 $ {"a":2}
    assert_eq!(
      exec(&mut s, &[b"JSON.SET", b"k2", b"$", b"{\"a\":2}"]),
      b"+OK\r\n"
    );

    // JSON.MGET k1 k2 k_missing $.a
    assert_eq!(
      exec(&mut s, &[b"JSON.MGET", b"k1", b"k2", b"k_missing", b"$.a"]),
      b"*3\r\n$3\r\n[1]\r\n$3\r\n[2]\r\n$-1\r\n"
    );

    // JSON.DEL k1 $.a
    assert_eq!(exec(&mut s, &[b"JSON.DEL", b"k1", b"$.a"]), b":1\r\n");

    // JSON.GET k1
    assert_eq!(
      exec(&mut s, &[b"JSON.GET", b"k1"]),
      b"$13\r\n{\"b\":[1,2,3]}\r\n"
    );

    // JSON.DEL k1
    assert_eq!(exec(&mut s, &[b"JSON.DEL", b"k1"]), b":1\r\n");

    // EXISTS k1 -> 0
    assert_eq!(exec(&mut s, &[b"EXISTS", b"k1"]), b":0\r\n");

    // JSON.SET with invalid json value returns error
    let set_err = exec(&mut s, &[b"JSON.SET", b"k_bad", b"$", b"{\"invalid\": "]);
    assert!(set_err.starts_with(b"-"));

    // JSON.SET then JSON.GET with INDENT
    assert_eq!(
      exec(
        &mut s,
        &[b"JSON.SET", b"k_pretty", b"$", b"{\"x\":1,\"y\":2}"]
      ),
      b"+OK\r\n"
    );
    let get_pretty = exec(&mut s, &[b"JSON.GET", b"k_pretty", b"INDENT", b"  ", b"$"]);
    assert!(get_pretty.starts_with(b"$"));
    assert!(get_pretty.contains(&b'\n'));

    aok::OK
  })
}

/// TYPE 与 EXISTS 对 JSON 扩展对象键口径一致：TYPE 回 C# modules 注册名
/// "GarnetJSON"（JsonModule.cs:20）而非 none；C# HandleType 对 custom object
/// 无 default 臂输出零字节的 quirk 为刻意差异
#[test]
fn type_exists_agree_on_json_key() -> aok::Result<()> {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let mut s = setup("json_type_exists.db");

    // 缺键：TYPE none 与 EXISTS 0 口径一致
    assert_eq!(exec(&mut s, &[b"TYPE", b"k"]), b"+none\r\n");
    assert_eq!(exec(&mut s, &[b"EXISTS", b"k"]), b":0\r\n");

    // 建键后：TYPE 回注册名（禁回 none），EXISTS 同键 :1
    assert_eq!(
      exec(&mut s, &[b"JSON.SET", b"k", b"$", b"{\"a\":1}"]),
      b"+OK\r\n"
    );
    assert_eq!(exec(&mut s, &[b"TYPE", b"k"]), b"+GarnetJSON\r\n");
    assert_eq!(exec(&mut s, &[b"EXISTS", b"k"]), b":1\r\n");

    // 删键后两命令口径回落一致
    assert_eq!(exec(&mut s, &[b"DEL", b"k"]), b":1\r\n");
    assert_eq!(exec(&mut s, &[b"TYPE", b"k"]), b"+none\r\n");
    assert_eq!(exec(&mut s, &[b"EXISTS", b"k"]), b":0\r\n");

    aok::OK
  })
}
