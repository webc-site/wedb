//! 工单 wnode-object-encoding-extended-label-nil：
//! OBJECT ENCODING 慢路径对扩展标签信封（tag >= 0x40，如 Roaring/JSON）兜底 hashtable 对拍测试
//!
//! 对齐 C# UnifiedStore ReadMethods.cs:48-77 HandleObjectEncoding：非内置对象一律兜底 hashtable，
//! 消除热键（快路 hashtable）与冷键（慢路原先误回 nil）分裂。
//! 验证点：
//! 1. 0x40 (Roaring) 与 0x41 (JSON) 自定义对象键：OBJECT ENCODING 快慢两路恒回 hashtable、
//!    REFCOUNT 恒回 1、IDLETIME 恒回 0、FREQ 恒回不支持；冷键降级后重放与快路逐字节一致；
//! 2. 扩展标签域 (0x40..=0xff) 物理信封直写：快慢两路逐标签兜底 hashtable 对拍；
//! 3. 回归锁：String 域 raw、RangeIndex 域 raw、内置集合类型各型映射、缺失与过期键 nil 四臂不漂移。

use std::{sync::Arc, thread::sleep, time::Duration};

use wnode::resp::{
  garnet_api::{GarnetApi, StoreGarnetApi},
  resp_server_session::RespServerSession,
  slow_path::SlowWait,
};
use wnode_test::test_env;
use wresp::command::RespCommand;
use wtest_base::resp_frame;
use wval::KeyTag;

/// 经会话统一驱动命令（热路径同步写出，冷路径降级后驱动 SlowWait 闭环）
async fn pump(session: &mut RespServerSession, input: &[u8]) -> Vec<u8> {
  session.recv_buffer.clear();
  session.recv_buffer.extend_from_slice(input);
  session.bytes_read = input.len();
  session.read_head = 0;
  session.end_read_head = 0;
  session.output.clear();
  assert_eq!(
    session.try_consume_messages(),
    Some(0),
    "输入命令应整段消费完毕"
  );
  let mut wire = Vec::new();
  if let Some(slow) = session.take_slow_wait() {
    let reply = slow.resolve().await;
    session.resolve_slow_wait_into(&reply, &mut wire);
  } else {
    session.take_output_into(&mut wire);
  }
  wire
}

/// 纯快路径执行（若降级挂起 SlowWait 则返回 None）
fn fast_pump(session: &mut RespServerSession, input: &[u8]) -> Option<Vec<u8>> {
  session.recv_buffer.clear();
  session.recv_buffer.extend_from_slice(input);
  session.bytes_read = input.len();
  session.read_head = 0;
  session.end_read_head = 0;
  session.output.clear();
  assert_eq!(
    session.try_consume_messages(),
    Some(0),
    "输入命令应整段消费完毕"
  );
  if session.take_slow_wait().is_some() {
    None
  } else {
    let mut wire = Vec::new();
    session.take_output_into(&mut wire);
    Some(wire)
  }
}

/// 慢路径直答（显式驱动 GarnetApi 慢路径分派，不走会话快路径）
async fn slow_reply(
  api: &GarnetApi,
  cmd: RespCommand,
  args: &[&[u8]],
  resp_version: u8,
) -> Vec<u8> {
  SlowWait::for_command(
    api,
    cmd,
    args.iter().map(|a| a.to_vec()).collect(),
    resp_version,
  )
  .resolve()
  .await
}

/// 验证点 1：JSON 与 Roaring 自定义对象键，OBJECT 四子命令快慢两路及冷热降级逐字节对拍
#[compio::test]
async fn custom_object_encoding_refcount_idletime_hot_cold_parity() {
  let (_dir, session, mut s) = test_env(false);
  let store = session.store.clone();
  let api: GarnetApi = Arc::new(StoreGarnetApi::new(session));
  s.set_garnet_api(api.clone());

  // 1. 写入 JSON 对象 (tag=0x41) 与 Roaring 位图 (tag=0x40)
  assert_eq!(
    pump(
      &mut s,
      &resp_frame(&[b"JSON.SET", b"jk", b"$", b"{\"a\":1}"])
    )
    .await,
    b"+OK\r\n"
  );
  assert_eq!(
    pump(&mut s, &resp_frame(&[b"R.SETBIT", b"rk", b"42", b"1"])).await,
    b":0\r\n"
  );

  let keys: &[&[u8]] = &[b"jk", b"rk"];
  let expected_enc = b"$9\r\nhashtable\r\n";
  let expected_ref = b":1\r\n";
  let expected_idle = b":0\r\n";
  let expected_freq =
    b"-ERR OBJECT FREQ is not supported: Garnet does not track access frequency (no LFU maxmemory policy).\r\n";

  // 2. 热键状态：快路径直答 vs 慢路径直答（RESP2 与 RESP3 逐字节对拍）
  for &key in keys {
    for ver in [2, 3] {
      s.resp_protocol_version = ver;
      // 快路径直答
      assert_eq!(
        fast_pump(&mut s, &resp_frame(&[b"OBJECT", b"ENCODING", key])),
        Some(expected_enc.to_vec()),
        "热键快路 OBJECT ENCODING 须回 hashtable"
      );
      assert_eq!(
        fast_pump(&mut s, &resp_frame(&[b"OBJECT", b"REFCOUNT", key])),
        Some(expected_ref.to_vec()),
        "热键快路 OBJECT REFCOUNT 须回 1"
      );
      assert_eq!(
        fast_pump(&mut s, &resp_frame(&[b"OBJECT", b"IDLETIME", key])),
        Some(expected_idle.to_vec()),
        "热键快路 OBJECT IDLETIME 须回 0"
      );
      assert_eq!(
        fast_pump(&mut s, &resp_frame(&[b"OBJECT", b"FREQ", key])),
        Some(expected_freq.to_vec()),
        "热键快路 OBJECT FREQ 须回不支持"
      );

      // 慢路径直答（独立调用 object_slow）
      assert_eq!(
        slow_reply(&api, RespCommand::ObjectEncoding, &[key], ver).await,
        expected_enc,
        "慢路 OBJECT ENCODING 须回 hashtable 对齐快路"
      );
      assert_eq!(
        slow_reply(&api, RespCommand::ObjectRefcount, &[key], ver).await,
        expected_ref,
        "慢路 OBJECT REFCOUNT 须回 1 对齐快路"
      );
      assert_eq!(
        slow_reply(&api, RespCommand::ObjectIdletime, &[key], ver).await,
        expected_idle,
        "慢路 OBJECT IDLETIME 须回 0 对齐快路"
      );
      assert_eq!(
        slow_reply(&api, RespCommand::ObjectFreq, &[key], ver).await,
        expected_freq,
        "慢路 OBJECT FREQ 须回不支持对齐快路"
      );
    }
  }

  // 3. 冷键状态：落盘淘汰后变磁盘候选，会话快路必须降级 SlowWait，重放慢路结果与热键严格一致
  store.flush_and_evict_all().await.unwrap();

  for &key in keys {
    // 确证快路径已不可同步应答，必须降级
    assert!(
      fast_pump(&mut s, &resp_frame(&[b"OBJECT", b"ENCODING", key])).is_none(),
      "磁盘候选冷键快路径必须降级挂起 SlowWait"
    );

    // 会话驱动闭环冷键 OBJECT 四命令：应答与热态逐字节同构（杜绝工单报告的冷键回 nil 缺陷）
    assert_eq!(
      pump(&mut s, &resp_frame(&[b"OBJECT", b"ENCODING", key])).await,
      expected_enc,
      "冷键降级重放 OBJECT ENCODING 须回 hashtable"
    );
    assert_eq!(
      pump(&mut s, &resp_frame(&[b"OBJECT", b"REFCOUNT", key])).await,
      expected_ref,
      "冷键降级重放 OBJECT REFCOUNT 须回 1"
    );
    assert_eq!(
      pump(&mut s, &resp_frame(&[b"OBJECT", b"IDLETIME", key])).await,
      expected_idle,
      "冷键降级重放 OBJECT IDLETIME 须回 0"
    );
    assert_eq!(
      pump(&mut s, &resp_frame(&[b"OBJECT", b"FREQ", key])).await,
      expected_freq,
      "冷键降级重放 OBJECT FREQ 须回不支持"
    );
  }
}

/// 验证点 2：扩展标签域 (>=0x40) 物理信封直接注入，快慢两路恒兜底 hashtable
#[compio::test]
async fn extended_tags_envelope_arbitrary_labels_fallback_hashtable() {
  let (_dir, session, mut s) = test_env(false);
  let store = session.store.clone();
  let api: GarnetApi = Arc::new(StoreGarnetApi::new(session));
  s.set_garnet_api(api.clone());

  // 抽取典型扩展标签：0x40 (Roaring), 0x41 (JSON), 0x42 (未知扩展), 0x7f, 0x80, 0xfe
  let test_tags: &[(u8, &[u8])] = &[
    (0x40, b"ext:40"),
    (0x41, b"ext:41"),
    (0x42, b"ext:42"),
    (0x7f, b"ext:7f"),
    (0x80, b"ext:80"),
    (0xfe, b"ext:fe"),
  ];

  let store_sess = store.new_session().unwrap();
  let batch = store_sess.enter_batch();
  for &(tag_byte, key) in test_tags {
    let payload = [tag_byte, 0x01, 0x02, 0x03];
    batch
      .try_upsert_tag_sync(key, KeyTag::ObjectEnvelope, &payload)
      .unwrap()
      .unwrap();
  }
  drop(batch);

  for &(_, key) in test_tags {
    // 快路径直答
    assert_eq!(
      fast_pump(&mut s, &resp_frame(&[b"OBJECT", b"ENCODING", key])),
      Some(b"$9\r\nhashtable\r\n".to_vec()),
      "扩展标签 {key:?} 快路必须兜底 hashtable"
    );
    // 慢路径直答
    assert_eq!(
      slow_reply(&api, RespCommand::ObjectEncoding, &[key], 2).await,
      b"$9\r\nhashtable\r\n",
      "扩展标签 {key:?} 慢路必须兜底 hashtable"
    );
  }
}

/// 验证点 3：回归锁——String 域 raw、RangeIndex 域 raw、内置集合类型映射、缺失/过期键 nil 不受影响
#[compio::test]
async fn regression_locks_standard_types_and_missing_keys() {
  let (_dir, session, mut s) = test_env(true);
  let api: GarnetApi = Arc::new(StoreGarnetApi::new(session));
  s.set_garnet_api(api.clone());

  // 1. String 域：OBJECT ENCODING 恒 raw
  assert_eq!(
    pump(&mut s, &resp_frame(&[b"SET", b"str_k", b"hello"])).await,
    b"+OK\r\n"
  );
  assert_eq!(
    fast_pump(&mut s, &resp_frame(&[b"OBJECT", b"ENCODING", b"str_k"])),
    Some(b"$3\r\nraw\r\n".to_vec())
  );
  assert_eq!(
    slow_reply(&api, RespCommand::ObjectEncoding, &[b"str_k"], 2).await,
    b"$3\r\nraw\r\n"
  );

  // 2. RangeIndex 域：RI.CREATE 后 OBJECT ENCODING 恒 raw（对齐 C# else 臂）
  assert_eq!(
    pump(&mut s, &resp_frame(&[b"RI.CREATE", b"ri_k", b"DISK"])).await,
    b"+OK\r\n"
  );
  assert_eq!(
    fast_pump(&mut s, &resp_frame(&[b"OBJECT", b"ENCODING", b"ri_k"])),
    Some(b"$3\r\nraw\r\n".to_vec())
  );
  assert_eq!(
    slow_reply(&api, RespCommand::ObjectEncoding, &[b"ri_k"], 2).await,
    b"$3\r\nraw\r\n"
  );

  // 3. 四内置集合对象：快慢路径同构映射
  // List -> quicklist
  assert_eq!(
    pump(&mut s, &resp_frame(&[b"RPUSH", b"list_k", b"e"])).await,
    b":1\r\n"
  );
  assert_eq!(
    fast_pump(&mut s, &resp_frame(&[b"OBJECT", b"ENCODING", b"list_k"])),
    Some(b"$9\r\nquicklist\r\n".to_vec())
  );
  assert_eq!(
    slow_reply(&api, RespCommand::ObjectEncoding, &[b"list_k"], 2).await,
    b"$9\r\nquicklist\r\n"
  );

  // SortedSet -> skiplist
  assert_eq!(
    pump(&mut s, &resp_frame(&[b"ZADD", b"zset_k", b"1", b"m"])).await,
    b":1\r\n"
  );
  assert_eq!(
    fast_pump(&mut s, &resp_frame(&[b"OBJECT", b"ENCODING", b"zset_k"])),
    Some(b"$8\r\nskiplist\r\n".to_vec())
  );
  assert_eq!(
    slow_reply(&api, RespCommand::ObjectEncoding, &[b"zset_k"], 2).await,
    b"$8\r\nskiplist\r\n"
  );

  // Hash -> hashtable
  assert_eq!(
    pump(&mut s, &resp_frame(&[b"HSET", b"hash_k", b"f", b"v"])).await,
    b":1\r\n"
  );
  assert_eq!(
    fast_pump(&mut s, &resp_frame(&[b"OBJECT", b"ENCODING", b"hash_k"])),
    Some(b"$9\r\nhashtable\r\n".to_vec())
  );
  assert_eq!(
    slow_reply(&api, RespCommand::ObjectEncoding, &[b"hash_k"], 2).await,
    b"$9\r\nhashtable\r\n"
  );

  // Set -> hashtable
  assert_eq!(
    pump(&mut s, &resp_frame(&[b"SADD", b"set_k", b"m"])).await,
    b":1\r\n"
  );
  assert_eq!(
    fast_pump(&mut s, &resp_frame(&[b"OBJECT", b"ENCODING", b"set_k"])),
    Some(b"$9\r\nhashtable\r\n".to_vec())
  );
  assert_eq!(
    slow_reply(&api, RespCommand::ObjectEncoding, &[b"set_k"], 2).await,
    b"$9\r\nhashtable\r\n"
  );

  // 4. 缺失键：快慢路径 RESP2 回 `$-1\r\n`，RESP3 回 `_\r\n`
  assert_eq!(
    fast_pump(&mut s, &resp_frame(&[b"OBJECT", b"ENCODING", b"nokey"])),
    Some(b"$-1\r\n".to_vec())
  );
  assert_eq!(
    slow_reply(&api, RespCommand::ObjectEncoding, &[b"nokey"], 2).await,
    b"$-1\r\n"
  );
  assert_eq!(
    slow_reply(&api, RespCommand::ObjectEncoding, &[b"nokey"], 3).await,
    b"_\r\n"
  );

  // 5. 过期键：TTL 过去后，快慢路径一致回 nil
  assert_eq!(
    pump(
      &mut s,
      &resp_frame(&[b"SET", b"exp_k", b"val", b"PX", b"1"])
    )
    .await,
    b"+OK\r\n"
  );
  sleep(Duration::from_millis(15));
  assert_eq!(
    pump(&mut s, &resp_frame(&[b"OBJECT", b"ENCODING", b"exp_k"])).await,
    b"$-1\r\n",
    "过期键必须回 nil"
  );
  assert_eq!(
    slow_reply(&api, RespCommand::ObjectEncoding, &[b"exp_k"], 2).await,
    b"$-1\r\n",
    "过期键慢路径必须回 nil"
  );
}
