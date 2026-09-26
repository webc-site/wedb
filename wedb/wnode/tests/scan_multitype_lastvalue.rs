//! SCAN 键面多 TYPE 词元末值覆盖回归（票 zcode-r161c-scantype §五）
//!
//! 修复前分叉：parse_scan_filter 的 TYPE else 臂将 type_unknown 置 true 后
//! 从不复位，粘滞标志使慢路径早退判（garnet_api/slow.rs C::Scan 臂）先于
//! type_filter 消费触发——`SCAN 0 TYPE stream TYPE hash` 回空列表+游标 0，
//! 而 C# NetworkSCAN 的 typeParameterValue 为局部 ReadOnlySpan 直赋
//! （ArrayCommands.cs:305-311），末值 hash 传入 DbScan 正常按 HashObject 过滤。
//! 修复后末值语义两侧全等：末值合法覆清粘滞标志照常扫描，末值非法照常回空。
//!
//! 覆盖面（对标 garnet test/standalone/Garnet.test/RespScanCommandsTests.cs
//! CanUseScanKeysCountAndTypeWithObjects 的 TYPE 过滤面）：
//! SCAN 0 TYPE stream TYPE hash 回全表非空帧锁（游标 0 一轮扫尽、仅 hash 键）；
//! 末值非法反序形 TYPE hash TYPE stream 回空列表+游标 0；单 TYPE 回归
//! （TYPE hash / TYPE string 与多 TYPE 末值形应答逐字节全等）；TYPE 缺省
//! 回归（SCAN 0 全键非空帧，默认 COUNT 10 一轮扫尽）。

use std::sync::Arc;

use compio::runtime::Runtime;
use wnode::resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSession};
use wnode_test::test_env;
use wtest_base::resp_frame;

/// 驱动一轮消费并回线面应答字节，返回 `(应答字节, 是否走慢路径)`
///
/// 慢路径应答经产线冲出口 [`RespServerSession::resolve_slow_wait_into`] 并入，
/// 与真实网络泵同一写出面（scan_keys_alloc_fail_smooth 同款泵夹具）
async fn pump(session: &mut RespServerSession, input: &[u8]) -> (Vec<u8>, bool) {
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
  match session.take_slow_wait() {
    Some(slow) => {
      let reply = slow.resolve().await;
      session.resolve_slow_wait_into(&reply, &mut wire);
      (wire, true)
    }
    None => {
      session.take_output_into(&mut wire);
      (wire, false)
    }
  }
}

/// 解析 SCAN 应答帧 → (游标, 键名列表)
fn parse_scan(frame: &[u8]) -> (i64, Vec<Vec<u8>>) {
  let text = String::from_utf8_lossy(frame);
  let mut parts = text.split("\r\n");
  assert_eq!(parts.next(), Some("*2"), "外层应为 *2: {text}");
  let cursor_hdr = parts.next().unwrap();
  assert!(cursor_hdr.starts_with('$'), "游标 bulk 头: {cursor_hdr}");
  let cursor: i64 = parts.next().unwrap().parse().unwrap();
  let arr_hdr = parts.next().unwrap();
  assert!(arr_hdr.starts_with('*'), "键数组头: {arr_hdr}");
  let n: usize = arr_hdr[1..].parse().unwrap();
  let mut keys = Vec::with_capacity(n);
  for _ in 0..n {
    let hdr = parts.next().unwrap();
    assert!(hdr.starts_with('$'), "键名 bulk 头: {hdr}");
    let len: usize = hdr[1..].parse().unwrap();
    let val = parts.next().unwrap().as_bytes().to_vec();
    assert_eq!(val.len(), len);
    keys.push(val);
  }
  (cursor, keys)
}

/// 灌入混合类型夹具：3 string + 2 hash（各命令走产线路径，无 mock）
async fn fixtures(s: &mut RespServerSession) {
  for key in [b"s1", b"s2", b"s3"] {
    let (out, went_slow) = pump(s, &resp_frame(&[b"SET", key, b"v"])).await;
    assert!(!went_slow, "SET 走快路径");
    assert_eq!(out, b"+OK\r\n");
  }
  for key in [b"h1", b"h2"] {
    let (out, went_slow) = pump(s, &resp_frame(&[b"HSET", key, b"f", b"v"])).await;
    assert!(!went_slow, "HSET 走快路径");
    assert_eq!(out, b":1\r\n");
  }
}

/// SCAN 0 TYPE stream TYPE hash → 末值 hash 覆清粘滞标志，回全表非空帧：
/// 游标 0 一轮扫尽（TYPE 给定页上界 usize::MAX）且仅含两 hash 键
#[test]
fn scan_multi_type_last_valid_returns_hash_frame() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (_dir, store_session, mut s) = test_env(false);
    s.set_garnet_api(Arc::new(StoreGarnetApi::new(store_session)));
    fixtures(&mut s).await;

    let (wire, went_slow) = pump(
      &mut s,
      &resp_frame(&[b"SCAN", b"0", b"TYPE", b"stream", b"TYPE", b"hash"]),
    )
    .await;
    assert!(went_slow, "SCAN 恒降级慢路径游标扫描");
    let (cursor, mut keys) = parse_scan(&wire);
    assert_eq!(cursor, 0, "TYPE 给定页无上限应一轮扫尽归零");
    keys.sort();
    assert_eq!(
      keys,
      vec![b"h1".to_vec(), b"h2".to_vec()],
      "末值 hash 须按 HashObject 过滤回非空帧（粘滞早退形为帧锁）: {wire:?}"
    );

    // 末值非法反序形：合法前值被末值覆盖为未知，回空列表+游标 0（早退臂）
    let (wire, _) = pump(
      &mut s,
      &resp_frame(&[b"SCAN", b"0", b"TYPE", b"hash", b"TYPE", b"stream"]),
    )
    .await;
    assert_eq!(
      wire, b"*2\r\n$1\r\n0\r\n*0\r\n",
      "末值非法须回空帧: {wire:?}"
    );
  });
}

/// 单 TYPE 回归：TYPE hash / TYPE string 独立形与多 TYPE 末值形应答
/// 逐字节全等；TYPE 缺省回归：SCAN 0 全 5 键非空帧、游标 0（默认 COUNT 10）
#[test]
fn scan_single_type_and_default_regressions() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (_dir, store_session, mut s) = test_env(false);
    s.set_garnet_api(Arc::new(StoreGarnetApi::new(store_session)));
    fixtures(&mut s).await;

    // 单 TYPE hash：与多 TYPE 末值 hash 形逐字节全等
    let (single_hash, _) = pump(&mut s, &resp_frame(&[b"SCAN", b"0", b"TYPE", b"hash"])).await;
    let (multi_hash, _) = pump(
      &mut s,
      &resp_frame(&[b"SCAN", b"0", b"TYPE", b"stream", b"TYPE", b"hash"]),
    )
    .await;
    assert_eq!(
      single_hash, multi_hash,
      "单 TYPE hash 与末值覆盖形应答应逐字节全等"
    );
    let (cursor, mut keys) = parse_scan(&single_hash);
    assert_eq!(cursor, 0);
    keys.sort();
    assert_eq!(keys, vec![b"h1".to_vec(), b"h2".to_vec()]);

    // 单 TYPE string：仅 3 字符串键
    let (wire, _) = pump(&mut s, &resp_frame(&[b"SCAN", b"0", b"TYPE", b"string"])).await;
    let (cursor, mut keys) = parse_scan(&wire);
    assert_eq!(cursor, 0);
    keys.sort();
    assert_eq!(keys, vec![b"s1".to_vec(), b"s2".to_vec(), b"s3".to_vec()]);

    // TYPE 缺省回归：无 TYPE 词元 type_unknown 恒 false，全 5 键单页扫尽
    let (wire, _) = pump(&mut s, &resp_frame(&[b"SCAN", b"0"])).await;
    let (cursor, mut keys) = parse_scan(&wire);
    assert_eq!(cursor, 0, "5 键 < 默认 COUNT 10 应单页归零");
    keys.sort();
    assert_eq!(
      keys,
      vec![
        b"h1".to_vec(),
        b"h2".to_vec(),
        b"s1".to_vec(),
        b"s2".to_vec(),
        b"s3".to_vec()
      ],
      "TYPE 缺省须回全键: {wire:?}"
    );
  });
}
