//! JSON 数值原文词形保真锁
//!
//! 在 garnet 中的相对路径:modules/GarnetJSON/GarnetJsonObject.cs:95/:360/:391/:409
//! （四处 `JsonNode.Parse` 全链不带 options）与
//! test/standalone/Garnet.test.extensions/GarnetJSON/JsonCommandsTest.cs
//! （JsonGetComplexPathTests / JsonSetComplexPathOperationsTest /
//! JsonSetNonStaticPathExistingFieldTest）。
//!
//! C# 侧不带 options 的 `JsonNode.Parse` 产出的数字节点由 `JsonElement` 承载，
//! 写回应答走 `JsonElement.WriteTo` 直拷原始文档字节，故 SET 载荷里的数字词形
//! （`1.10` / `1e2` / `-0` / 超 u64 大整数 / 高精度小数）经落库后 GET 逐字节还原，
//! 不被归一为 `1.1` / `100.0` / `0` / `1.8446744073709552e19`。rust 侧由
//! `parse_dom` 的 `use_rawnumber()` 承接同一形态：原文词形即数字的唯一存储形态，
//! 数值语义（JSON.TYPE 判型、过滤器比较、NUMINCRBY 取数）按需现解该文本，
//! DOM 内不落第二数值表示。
//!
//! 自研依据: 数字原文词形（sonic_rs RawNumber 单一存储形态）

use std::str;

use wext_json::{GarnetJsonObject, JsonCommand, SetResult};
use wresp::options::ExistOptions;

/// RESP 协议版本（与既有 JSON 用例一致的 resp2 会话）
const RESP_VER: u8 = 2;

/// 票面点名的四类原文词形 + 超 f64 精度小数：归一化必然改写、原文形态必须逐字节还原
const RAW_TEXTS: &[&str] = &[
  "1.10",                  // 尾零：归一化回 1.1
  "1e2",                   // 指数：归一化回 100.0
  "-0",                    // 负零：归一化回 0
  "18446744073709551616",  // 2^64：归一化回 1.8446744073709552e19
  "1.0000000000000000001", // 超 f64 精度：归一化回 1.0
];

/// 载荷原文经 `JSON.SET` 落库（根替换路径，对标 C# JsonSetGetTests 起始 SET）
fn set_root(doc: &str) -> GarnetJsonObject {
  let mut obj = GarnetJsonObject::create();
  assert_eq!(
    obj.set(b"$", doc.as_bytes(), ExistOptions::None).unwrap(),
    SetResult::Success,
    "{doc} 落库失败"
  );
  obj
}

/// RESP bulk string 帧 → 载荷原文（其余帧型直接失败：本文件只走全量 GET 应答）
fn bulk_body(frame: &[u8]) -> String {
  let s = str::from_utf8(frame).expect("GET 应答必为 UTF-8 文本");
  let rest = s
    .strip_prefix('$')
    .unwrap_or_else(|| panic!("期望 bulk string 应答，实得: {s}"));
  let head_end = rest.find("\r\n").unwrap();
  let len: usize = rest[..head_end].parse().unwrap();
  let body_start = head_end + 2;
  rest[body_start..body_start + len].to_string()
}

/// `JSON.GET` 应答原文（paths 为空 = 全量文档）
fn get_text(obj: &GarnetJsonObject, paths: &[&[u8]]) -> String {
  let mut out = Vec::new();
  obj
    .try_get(paths, &mut out, None, None, None, RESP_VER)
    .unwrap();
  bulk_body(&out)
}

/// JSON.TYPE reader 应答原文
fn type_text(doc: &str, path: &str) -> String {
  let mut out = Vec::new();
  assert!(
    (JsonCommand::Type.fns().reader)(doc.as_bytes(), &[path.as_bytes()], &mut out, RESP_VER),
    "{path} TYPE 执行失败"
  );
  String::from_utf8_lossy(&out).into_owned()
}

/// SET 载荷数字词形 → GET 逐字节对拍：根标量、对象成员、数组元素、缩进面四条出口同态
#[test]
fn raw_number_text_is_byte_exact_on_get() {
  for text in RAW_TEXTS {
    let obj = set_root(text);
    assert_eq!(get_text(&obj, &[]), *text, "根标量 {text} 原文丢失");

    let doc = format!(r#"{{"n":{text}}}"#);
    let obj = set_root(&doc);
    assert_eq!(
      get_text(&obj, &[b"$.n"]),
      format!("[{text}]"),
      "$.n 原文丢失"
    );
    assert_eq!(get_text(&obj, &[]), doc, "全量 GET 原文丢失");

    let obj = set_root(&format!(r#"{{"ns":[{text},1,"{text}"]}}"#));
    assert_eq!(
      get_text(&obj, &[b"$.ns"]),
      format!("[[{text},1,\"{text}\"]]"),
      "数组元素原文丢失"
    );

    // 缩进面：C# 带 indent 同样经 WriteTo 拷原始数字字节，词形不变
    let obj = set_root(&doc);
    let mut out = Vec::new();
    obj
      .try_get(&[b"$"], &mut out, Some("  "), None, None, RESP_VER)
      .unwrap();
    let pretty = bulk_body(&out);
    assert!(pretty.contains(text), "缩进 GET 丢失 {text}：{pretty}");
  }
}

/// GET 两次幂等：读路径不消费原文词形
#[test]
fn repeated_get_is_idempotent() {
  let doc = r#"{"a":1.10,"b":[1e2,-0],"c":18446744073709551616}"#;
  let obj = set_root(doc);
  let first = get_text(&obj, &[]);
  assert_eq!(first, doc);
  assert_eq!(get_text(&obj, &[]), first, "全量 GET 两次不一致");

  let by_path = get_text(&obj, &[b"$.b"]);
  assert_eq!(by_path, "[[1e2,-0]]");
  assert_eq!(get_text(&obj, &[b"$.b"]), by_path, "带路径 GET 两次不一致");
}

/// serialize_object → from_slice 重载保原文：落盘-重载往返不二次归一（C# SerializationTest 口径）
#[test]
fn serialize_then_reload_keeps_raw_number_text() {
  let doc = r#"{"a":1.10,"b":[1e2,-0,18446744073709551616],"c":{"d":1.0000000000000000001}}"#;
  let obj = set_root(doc);

  let mut buf = Vec::new();
  obj.serialize_object(&mut buf).unwrap();
  assert_eq!(str::from_utf8(&buf).unwrap(), doc, "信封序列化丢失原文");

  let back = GarnetJsonObject::from_slice(&buf).unwrap();
  assert_eq!(get_text(&back, &[]), doc, "重载后 GET 丢失原文");
  let mut buf2 = Vec::new();
  back.serialize_object(&mut buf2).unwrap();
  assert_eq!(buf, buf2, "往返序列化不稳定");

  assert_eq!(get_text(&back, &[b"$..d"]), "[1.0000000000000000001]");
}

/// 原文词形节点的 JSON.TYPE 判型与归一化前同态（整数文本仍回 integer）
#[test]
fn raw_number_nodes_keep_type_judgement() {
  let doc = r#"{"i":100,"f":1.10,"e":1e2,"big":18446744073709551616,"neg":-0}"#;
  for (path, want) in [
    ("$.i", "integer"),
    ("$.f", "number"),
    ("$.e", "number"),
    ("$.big", "number"),
    ("$.neg", "number"),
  ] {
    let out = type_text(doc, path);
    assert!(out.contains(want), "{path} 判型非 {want}：{out}");
  }
}

/// 原文词形节点的过滤器比较按数值语义现解文本：命中集与归一化前一致，应答回原文
#[test]
fn filter_comparison_on_raw_numbers_is_numeric() {
  let obj = set_root(r#"{"items":[{"p":8.950},{"p":12.99},{"p":1e2},{"p":100.0}]}"#);
  // 尾零文本 8.950 仍被 < 10 数值命中，应答保原文词形
  assert_eq!(
    get_text(&obj, &[b"$.items[?(@.p < 10)]"]),
    r#"[{"p":8.950}]"#
  );
  // 1e2 与 100.0 同为 f64 语义命中 > 50（与归一化解析同态）
  assert_eq!(
    get_text(&obj, &[b"$.items[?(@.p > 50)]"]),
    r#"[{"p":1e2},{"p":100.0}]"#
  );
  // 等值比较跨词形：字面量 22.99 命中存储文本 22.990
  let obj = set_root(r#"{"items":[{"p":22.990}]}"#);
  assert_eq!(
    get_text(&obj, &[b"$.items[?(@.p == 22.99)]"]),
    r#"[{"p":22.990}]"#
  );
}

/// NUMINCRBY 对原文词形节点按需取数：入参数值生效，结果节点回 canonical 文本
#[test]
fn numincrby_reads_raw_number_on_demand() {
  for (doc, by, want) in [
    (r#"{"n":1.10}"#, "0.05", "1.15"),
    (r#"{"n":1e2}"#, "5", "105"),
    (r#"{"n":2}"#, "1.5", "3.5"),
  ] {
    let mut payload = doc.as_bytes().to_vec();
    let args: Vec<&[u8]> = vec![b"$.n", by.as_bytes()];
    let mut out = Vec::new();
    assert!(
      (JsonCommand::NumIncrBy.fns().updater)(&mut payload, &args, &mut out, RESP_VER),
      "{doc} NUMINCRBY {by} 执行失败"
    );
    let got = String::from_utf8(payload).unwrap();
    assert!(got.contains(want), "{doc} + {by} 应含 {want}，实得 {got}");
  }
}

/// C# JsonCommandsTest 同款用例互锁：complex store 路径取数 + 价格更新
///
/// 期望串逐字抄自
/// test/standalone/Garnet.test.extensions/GarnetJSON/JsonCommandsTest.cs 的
/// TestCase 特性串（JsonGetComplexPathTests / JsonSetComplexPathOperationsTest /
/// JsonSetNonStaticPathExistingFieldTest）
#[test]
fn csharp_json_commands_complex_store_interlock() {
  let complex_json = r#"{
                "store": {
                    "book": [
                        {
                            "category": "reference",
                            "author": "Nigel Rees",
                            "title": "Sayings of the Century",
                            "price": 8.95
                        },
                        {
                            "category": "fiction",
                            "author": "Evelyn Waugh",
                            "title": "Sword of Honour",
                            "price": 12.99
                        },
                        {
                            "category": "fiction",
                            "author": "Herman Melville",
                            "title": "Moby Dick",
                            "price": 8.99
                        },
                        {
                            "category": "fiction",
                            "author": "J. R. R. Tolkien",
                            "title": "The Lord of the Rings",
                            "price": 22.99
                        }
                    ],
                    "bicycle": {
                        "color": "red",
                        "price": 19.95
                    }
                }
            }"#;
  let mut obj = set_root(complex_json);

  assert_eq!(
    get_text(&obj, &[b"$.store.book[*].author"]),
    r#"["Nigel Rees","Evelyn Waugh","Herman Melville","J. R. R. Tolkien"]"#
  );
  assert_eq!(
    get_text(&obj, &[b"$.store.book[2]"]),
    r#"[{"category":"fiction","author":"Herman Melville","title":"Moby Dick","price":8.99}]"#
  );
  assert_eq!(
    get_text(&obj, &[b"$.store.book[?(@.price < 10)]"]),
    concat!(
      r#"[{"category":"reference","author":"Nigel Rees","title":"Sayings of the Century","price":8.95},"#,
      r#"{"category":"fiction","author":"Herman Melville","title":"Moby Dick","price":8.99}]"#
    )
  );
  assert_eq!(get_text(&obj, &[b"$.store.bicycle.color"]), r#"["red"]"#);

  // JsonSetComplexPathOperationsTest：decimal 价格写入后 GET 回 [该价格原文]
  for (path, new_price) in [
    ("$.store.book[0].price", "15.99"),
    ("$.store.book[3].price", "25.99"),
    ("$.store.bicycle.price", "29.99"),
  ] {
    assert_eq!(
      obj
        .set(path.as_bytes(), new_price.as_bytes(), ExistOptions::None)
        .unwrap(),
      SetResult::Success,
      "{path} 更新失败"
    );
    assert_eq!(
      get_text(&obj, &[path.as_bytes()]),
      format!("[{new_price}]"),
      "{path} 价格更新应答与 C# 期望不符"
    );
  }

  // JsonSetNonStaticPathExistingFieldTest：$..price 覆写后旧价格全部消失
  obj.set(b"$..price", b"42.99", ExistOptions::None).unwrap();
  let prices = get_text(&obj, &[b"$..price"]);
  assert_eq!(prices, "[42.99,42.99,42.99,42.99,42.99]");
}
