//! wext_json corrupt 载荷 fail-fast 回归（票 wext-json-corrupt-payload-nil-fold-rewrite-breach-fail-fast）
//!
//! C# 对位：工厂层反序列化先行于命令钩子（modules/GarnetJSON/GarnetJsonObject.cs:94
//! `GarnetJsonObject(byte, BinaryReader)` 的 BinaryReader.ReadString + JsonNode.Parse
//! 硬失败抛异常），钩子执行体收不到损坏对象，本无 nil/:0 折叠形态；rust 信封载荷
//! 形态下 from_slice Err 臂折损点共七处（复核席注记订正 4→7，逐一钉死）：
//! 1. set_get.rs json_get_reader（JSON.GET）
//! 2. common.rs eval_json_target（共用头：STRLEN/ARRLEN/ARRINDEX/OBJKEYS/OBJLEN）
//! 3. common.rs mutate_json_target（共用头：NUMINCRBY/NUMMULTBY/TOGGLE/
//!    ARRAPPEND/ARRPOP/ARRINSERT/ARRTRIM/STRAPPEND）
//! 4. mutate.rs json_del_updater（JSON.DEL/FORGET 共用 fns）
//! 5. object.rs json_type_reader（JSON.TYPE，非共用头）
//! 6. resp_encode.rs json_resp_reader（JSON.RESP，非共用头）
//! 7. mutate.rs json_clear_updater（JSON.CLEAR，非共用头，同带 Save 重写 + AOF 重复入账）
//!
//! 收口形态：Err 臂统一写「ERR JSON object decode failed」错误帧并按钩子契约回
//! false（reader false = 错误已写中止、updater false = 放弃落库，wcustom
//! CustomObjectFns 语义零扩充；同通道 wext_roaring ERR_DECODE 先例同形）。
//! 非折损点钉形：root None 臂（空载荷 = 键缺失新建空对象合法语义）仍回 nil；
//! 多键读臂 error_element_to_nil 协议整形不变（wnode/tests/
//! json_corrupt_payload_fail_fast.rs 端到端钉死）。

use wcustom::CustomObjectFns;
use wext_json::JsonCommands;

/// 会话 RESP 协议版本（执行体 nil 帧型裁决入参）
const VER: u8 = 2;

/// fail-fast 错误帧（error.rs ERR_JSON_DECODE_FAILED 单源常量帧形；
/// 同通道对照 wext_roaring 为 -ERR RoaringBitmap object decode failed）
const DECODE_FRAME: &[u8] = b"-ERR JSON object decode failed\r\n";

/// 合法标签 + 非 JSON 字节信封内层载荷（UTF-8 裸词 / 截断 JSON / 非法 UTF-8
/// 二进制 / 尾部垃圾——from_slice/parse_dom 各拒收形态；空载荷不在其列，
/// 空载荷 = 建空对象合法语义非折损点）
const CORRUPT_PAYLOADS: &[&[u8]] = &[
  b"hello",
  br#"{"a": "#,
  b"\xff\xfe\x01\x02",
  br#"{"a":1}trailing"#,
];

/// reader 臂 fail-fast：回 false、精确错误帧、绝不再现 nil 折叠
fn assert_reader_fail_fast(fns: CustomObjectFns, name: &str, args: &[&[u8]]) {
  for payload in CORRUPT_PAYLOADS {
    let mut out = Vec::new();
    assert!(
      !(fns.reader)(payload, args, &mut out, VER),
      "{name} corrupt 载荷 reader 须回 false 中止: {payload:?}"
    );
    assert_eq!(
      out, DECODE_FRAME,
      "{name} corrupt 载荷须落错误帧: {payload:?}"
    );
  }
}

/// updater 臂 fail-fast：回 false、精确错误帧、载荷字节零改写（Save 重写面消除，
/// dispatch CustomObjStep::Done 不再触达 → AOF 零入账）
fn assert_updater_fail_fast(fns: CustomObjectFns, name: &str, args: &[&[u8]]) {
  for payload in CORRUPT_PAYLOADS {
    let mut p = payload.to_vec();
    let mut out = Vec::new();
    assert!(
      !(fns.updater)(&mut p, args, &mut out, VER),
      "{name} corrupt 载荷 updater 须回 false 放弃落库: {payload:?}"
    );
    assert_eq!(
      out, DECODE_FRAME,
      "{name} corrupt 载荷须落错误帧: {payload:?}"
    );
    assert_eq!(
      p.as_slice(),
      *payload,
      "{name} corrupt 载荷被原样重写回库（旧缺陷形：Save + AOF 重复入账）"
    );
  }
}

/// 读族收口：票面处 json_get_reader + 非共用头 json_type_reader、
/// json_resp_reader + 共用头 eval_json_target 全消费族代表
#[test]
fn corrupt_payload_readers_fail_fast() {
  assert_reader_fail_fast(JsonCommands::JSON_GET, "JSON.GET 单路径", &[b"$.a"]);
  assert_reader_fail_fast(JsonCommands::JSON_GET, "JSON.GET 零路径", &[]);
  assert_reader_fail_fast(JsonCommands::JSON_TYPE, "JSON.TYPE", &[b"$"]);
  assert_reader_fail_fast(JsonCommands::JSON_RESP, "JSON.RESP", &[b"$"]);
  assert_reader_fail_fast(JsonCommands::JSON_STRLEN, "JSON.STRLEN", &[b"$"]);
  assert_reader_fail_fast(JsonCommands::JSON_ARRLEN, "JSON.ARRLEN", &[]);
  assert_reader_fail_fast(JsonCommands::JSON_OBJKEYS, "JSON.OBJKEYS", &[b"$"]);
  assert_reader_fail_fast(JsonCommands::JSON_OBJLEN, "JSON.OBJLEN", &[]);
  assert_reader_fail_fast(JsonCommands::JSON_ARRINDEX, "JSON.ARRINDEX", &[b"$", b"1"]);
}

/// 变异族收口：票面处 json_del_updater + 非共用头 json_clear_updater +
/// 共用头 mutate_json_target 消费族代表（updater false = 放弃落库）
#[test]
fn corrupt_payload_updaters_fail_fast_no_rewrite() {
  assert_updater_fail_fast(JsonCommands::JSON_DEL, "JSON.DEL", &[b"$"]);
  assert_updater_fail_fast(JsonCommands::JSON_CLEAR, "JSON.CLEAR", &[b"$"]);
  assert_updater_fail_fast(
    JsonCommands::JSON_NUMINCRBY,
    "JSON.NUMINCRBY",
    &[b"$", b"1"],
  );
  assert_updater_fail_fast(
    JsonCommands::JSON_NUMMULTBY,
    "JSON.NUMMULTBY",
    &[b"$", b"2"],
  );
  assert_updater_fail_fast(JsonCommands::JSON_TOGGLE, "JSON.TOGGLE", &[b"$"]);
  assert_updater_fail_fast(
    JsonCommands::JSON_STRAPPEND,
    "JSON.STRAPPEND",
    &[b"$", b"\"s\""],
  );
  assert_updater_fail_fast(
    JsonCommands::JSON_ARRAPPEND,
    "JSON.ARRAPPEND",
    &[b"$", b"1"],
  );
  assert_updater_fail_fast(JsonCommands::JSON_ARRPOP, "JSON.ARRPOP", &[]);
}

/// root None 臂钉形（非折损点，严禁误改）：空载荷 = 键缺失建空对象合法语义，
/// 共用头 reader 仍按原口径回 nil 帧回 true，不落解码错误帧
#[test]
fn empty_payload_root_none_still_yields_null_not_decode_frame() {
  for (name, fns) in [
    ("JSON.STRLEN", JsonCommands::JSON_STRLEN),
    ("JSON.ARRLEN", JsonCommands::JSON_ARRLEN),
  ] {
    let mut out = Vec::new();
    assert!(
      (fns.reader)(b"", &[], &mut out, VER),
      "{name} 空载荷臂契约不变"
    );
    assert_eq!(
      out, b"$-1\r\n",
      "{name} 空载荷仍回 nil（root None 非折损点）"
    );
  }
}

type ReaderCase<'a> = (&'a str, CustomObjectFns, &'a [&'a [u8]], &'a [u8]);
type RmwCase<'a> = (&'a str, CustomObjectFns, &'a [&'a [u8]], &'a [u8], &'a [u8]);

/// 合法载荷全族应答逐字节回归（收口改动零外溢：成功路径帧形与载荷回写不变）
#[test]
fn legal_payload_family_byte_regression() {
  let payload = br#"{"a":1}"#;

  // 读族：{"a":1} 各命令精确应答（与既有 save_recover/serialization 用例同字节）
  let readers: &[ReaderCase] = &[
    (
      "JSON.GET 全量",
      JsonCommands::JSON_GET,
      &[],
      b"$7\r\n{\"a\":1}\r\n",
    ),
    (
      "JSON.GET $",
      JsonCommands::JSON_GET,
      &[b"$"],
      b"$9\r\n[{\"a\":1}]\r\n",
    ),
    (
      "JSON.TYPE $",
      JsonCommands::JSON_TYPE,
      &[b"$"],
      b"+object\r\n",
    ),
    (
      "JSON.RESP $",
      JsonCommands::JSON_RESP,
      &[b"$"],
      b"*2\r\n$1\r\na\r\n:1\r\n",
    ),
    (
      "JSON.OBJLEN $",
      JsonCommands::JSON_OBJLEN,
      &[b"$"],
      b"$3\r\n[1]\r\n",
    ),
    (
      "JSON.OBJKEYS $",
      JsonCommands::JSON_OBJKEYS,
      &[b"$"],
      b"$7\r\n[[\"a\"]]\r\n",
    ),
    (
      "JSON.STRLEN $",
      JsonCommands::JSON_STRLEN,
      &[b"$"],
      b"$6\r\n[null]\r\n",
    ),
  ];
  for (name, fns, args, want) in readers {
    let mut out = Vec::new();
    assert!(
      (fns.reader)(payload, args, &mut out, VER),
      "{name} 合法载荷 reader 回 true"
    );
    assert_eq!(&out, want, "{name} 合法载荷应答走样");
  }

  // 变异族：成功回 true + 应答精确 + 载荷回写字节精确
  let rmw: &[RmwCase] = &[
    (
      "JSON.NUMINCRBY",
      JsonCommands::JSON_NUMINCRBY,
      &[b"$.a", b"2"],
      b"$3\r\n[3]\r\n",
      br#"{"a":3}"#,
    ),
    (
      "JSON.CLEAR $",
      JsonCommands::JSON_CLEAR,
      &[b"$"],
      b":1\r\n",
      // 根即对象容器：CLEAR 清空对象本体 → "{}"（容器臂先于数值臂）
      b"{}",
    ),
    (
      "JSON.TOGGLE",
      JsonCommands::JSON_TOGGLE,
      &[b"$.a"],
      b"$6\r\n[null]\r\n",
      br#"{"a":1}"#,
    ),
    (
      "JSON.DEL",
      JsonCommands::JSON_DEL,
      &[b"$.a"],
      b":1\r\n",
      b"{}",
    ),
  ];
  for (name, fns, args, want_frame, want_payload) in rmw {
    let mut p = payload.to_vec();
    let mut out = Vec::new();
    assert!(
      (fns.updater)(&mut p, args, &mut out, VER),
      "{name} 合法载荷 updater 回 true"
    );
    assert_eq!(out, *want_frame, "{name} 合法载荷应答走样");
    assert_eq!(p.as_slice(), *want_payload, "{name} 合法载荷回写字节走样");
  }

  // JSON.SET 既有 fail-fast 先例不回退（本 crate 同串对照锚：corrupt 载荷回同帧）
  let mut p = b"hello".to_vec();
  let mut out = Vec::new();
  assert!(
    !(JsonCommands::JSON_SET.updater)(&mut p, &[b"$", b"1"], &mut out, VER),
    "JSON.SET corrupt 载荷 updater 须回 false（既有先例）"
  );
  assert_eq!(out, DECODE_FRAME, "JSON.SET 同串错误帧不回退");
}
