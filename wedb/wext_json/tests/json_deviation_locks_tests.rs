//! zcode-r24-wextjson 发现八：wext_json 四组「rust 侧修复性偏离」的语义锁。
//!
//! 登记全文见 `doc/zh/deviations.md` 第 15 节。此处逐组钉住 rust 侧的可观测行为，
//! 严禁按 C# 原型缺陷形态回改（回改即破此处断言）。
//!
//! 对位 C#：
//! - garnet/modules/GarnetJSON/JsonCommands.cs:JsonSET.Updater（`Count is 4` 死门）
//! - garnet/modules/GarnetJSON/GarnetJsonObject.cs:Set（`$` 根先于 existOptions）
//! - garnet/modules/GarnetJSON/JSONPath/QueryExpression.cs:RegexEquals（LastIndexOf -1）
//! - garnet/modules/GarnetJSON/GarnetJsonObject.cs:TryGet（多路径键裸拼）

use std::{str::from_utf8, thread::Builder};

use sonic_rs::{JsonContainerTrait, JsonValueTrait, Value};
use wext_json::{GarnetJsonObject, JsonCommand, JsonPath, SetResult};
use wresp::options::ExistOptions;

/// 剥掉 RESP bulk string 帧头取回 JSON 文本，并解析为 Value（解析成功即帧合法性证明）。
fn resp_json(resp: &[u8]) -> Value {
  let s = from_utf8(resp).unwrap();
  assert!(s.starts_with('$'), "期望 RESP bulk string，实得 {s}");
  let json_part = s.split("\r\n").nth(1).unwrap();
  sonic_rs::from_str(json_part)
    .unwrap_or_else(|e| panic!("应答非法 JSON（帧体 {json_part:?}）：{e}"))
}

fn json(src: &str) -> Value {
  sonic_rs::from_str(src).unwrap()
}

// ===================== a) NX/XX 生效面 =====================

/// C# `parseState.Count is 4` 恒 false（合法参数 Count 最大 3），TryGetExistOption
/// 永不调用，故 C# 的 NX/XX 从不生效、恒按 None 无条件覆盖写。rust 侧全量实现：
/// 已存在路径的 NX 写不落地。
#[test]
fn nx_rejects_existing_path() {
  let mut obj = GarnetJsonObject::create();
  assert_eq!(
    obj.set(b"$", br#"{"a":1}"#, ExistOptions::None).unwrap(),
    SetResult::Success
  );

  assert_eq!(
    obj.set(b"$.a", br"2", ExistOptions::Nx).unwrap(),
    SetResult::ConditionNotMet
  );
  let mut out = Vec::new();
  assert!(
    obj
      .try_get(&[b"$.a"], &mut out, None, None, None, 2)
      .unwrap()
  );
  assert_eq!(
    resp_json(&out),
    json("[1]"),
    "NX 命中已存在路径时原值须保留"
  );

  // 同路径 XX 写照常生效，证明条件门按 existOptions 分流而非恒 None
  assert_eq!(
    obj.set(b"$.a", br"2", ExistOptions::Xx).unwrap(),
    SetResult::Success
  );
}

/// 不存在路径的 XX 写不创建成员（C# 形态会直接创建）。
#[test]
fn xx_rejects_missing_path() {
  let mut obj = GarnetJsonObject::create();
  assert_eq!(
    obj.set(b"$", br#"{"a":1}"#, ExistOptions::None).unwrap(),
    SetResult::Success
  );

  assert_eq!(
    obj.set(b"$.b", br"3", ExistOptions::Xx).unwrap(),
    SetResult::ConditionNotMet
  );
  let mut out = Vec::new();
  assert!(
    obj
      .try_get(&[b"$.b"], &mut out, None, None, None, 2)
      .unwrap()
  );
  assert_eq!(resp_json(&out), json("[]"), "XX 未命中时不得创建成员");

  // 同路径无条件下正常创建
  assert_eq!(
    obj.set(b"$.b", br"3", ExistOptions::None).unwrap(),
    SetResult::Success
  );
}

// ===================== b) 根路径 `$` 的条件写 =====================

/// C# `GarnetJsonObject.cs:Set` 在检查 existOptions 之前对 `$` 直接替换根并回 Success
/// （NX 也覆盖已存在根）。rust 侧对齐 RedisJSON：根已存在时 NX 回 ConditionNotMet。
#[test]
fn root_nx_rejects_existing_document() {
  let mut obj = GarnetJsonObject::create();
  assert_eq!(
    obj.set(b"$", br#"{"keep":1}"#, ExistOptions::None).unwrap(),
    SetResult::Success
  );

  assert_eq!(
    obj.set(b"$", br#"{"gone":1}"#, ExistOptions::Nx).unwrap(),
    SetResult::ConditionNotMet
  );
  let mut out = Vec::new();
  assert!(obj.try_get(&[b"$"], &mut out, None, None, None, 2).unwrap());
  assert_eq!(
    resp_json(&out),
    json(r#"[{"keep":1}]"#),
    "根 NX 不得覆盖已存文档"
  );
}

/// 空文档上的 XX 根写同样条件不成立（C# 会直接落根值）。
#[test]
fn root_xx_rejects_empty_document() {
  let mut obj = GarnetJsonObject::create();
  assert_eq!(
    obj.set(b"$", br#"{"a":1}"#, ExistOptions::Xx).unwrap(),
    SetResult::ConditionNotMet
  );
  // 空文档 GET $ 两侧同为 RESP2 nil（C# root is null 即 WriteNull，见 GarnetJsonObject.cs:228）
  let mut out = Vec::new();
  assert!(obj.try_get(&[b"$"], &mut out, None, None, None, 2).unwrap());
  assert_eq!(out, b"$-1\r\n".to_vec());
}

// ===================== c) 引号正则不崩 =====================

/// C# `RegexEquals` 对无斜杠包裹的引号串 pattern 取 `LastIndexOf('/')` 得 -1 后
/// `Substring(1, -2)` 抛 ArgumentOutOfRangeException 掐断连接。rust 侧按需编译该串
/// 作正则，正常匹配、不 panic、不报错帧。
#[test]
fn quoted_regex_operand_matches_without_crash() {
  let val = json(r#"[{"name":"xAby"},{"name":"zzz"}]"#);
  let path = JsonPath::parse("$[?(@.name =~ 'Ab')]").unwrap();
  let hits = path.evaluate(&val);
  assert_eq!(hits.len(), 1, "无斜杠引号 pattern 须按正则匹配");
  assert_eq!(hits[0].get("name").and_then(Value::as_str), Some("xAby"));
}

/// 非法正则的引号串同样不得 panic：rust 侧视为不匹配（C# 侧同形态必抛异常）。
#[test]
fn invalid_quoted_regex_pattern_does_not_panic() {
  let val = json(r#"[{"name":"a[b"}]"#);
  let path = JsonPath::parse(r"$[?(@.name =~ '[')]").unwrap();
  assert_eq!(path.evaluate(&val).len(), 0);
}

// ===================== d) 多路径 GET 的键名转义 =====================

/// C# 多路径分支把 path 原文两侧加裸引号拼成 JSON 键，路径含引号/反斜杠时产出非法
/// JSON。rust 侧以 sonic 序列化键名，转义后整帧仍为合法 JSON 且键可逐字符还原。
#[test]
fn multi_path_key_escapes_special_chars() {
  let mut obj = GarnetJsonObject::create();
  assert_eq!(
    obj
      .set(b"$", br#"{"a\"b":1,"c\\d":2}"#, ExistOptions::None)
      .unwrap(),
    SetResult::Success
  );

  let p1 = br#"$['a\"b']"#;
  let p2 = br#"$['c\\d']"#;
  let mut out = Vec::new();
  assert!(
    obj
      .try_get(&[&p1[..], &p2[..]], &mut out, None, None, None, 2)
      .unwrap()
  );

  let val = resp_json(&out);
  let map = val.as_object().expect("多路径应答须为 JSON 对象");
  let k1 = String::from_utf8(p1.to_vec()).unwrap();
  let k2 = String::from_utf8(p2.to_vec()).unwrap();
  let one = json("[1]");
  let two = json("[2]");
  assert_eq!(map.get(&k1), Some(&one), "含引号路径须以原文为键");
  assert_eq!(map.get(&k2), Some(&two), "含反斜杠路径须以原文为键");
}

// ===================== e) 解析深度接受带（§161 宽向分叉锁） =====================
//
// 对位 C#：`garnet/modules/GarnetJSON/GarnetJsonObject.cs` 四处 `JsonNode.Parse`
// 不带 options，吃 System.Text.Json 默认 MaxDepth=64，>64 层抛 JsonException 收
// 错误帧。rust wext_json 侧无独立深度门——深度可解析性全由 `parse_dom` 的
// sonic `Value` 原生快路承载，而该快路（registry sonic-rs 0.5.10
// value/de.rs:62 `deserialize_newtype_struct(TOKEN,..)`）绕过 serde/de.rs:23
// MAX_ALLOWED_DEPTH=255，故实测 rust 对 64/65 乃至数千层载荷一律收（宽向），
// 登记全文见 doc/zh/deviations.md §161。**严禁按 C# 64 在 SET/GET 热路径加
// O(n) 深度预扫回改**（回改即破此处断言，且无 255 门可依，属双错）。

/// 构造 n 层对象嵌套载荷 `{"a":{"a":...{"a":1}...}}`（真实递归路径，非数组快路）。
fn nest_obj(n: usize) -> Vec<u8> {
  let mut s = String::new();
  for _ in 0..n {
    s.push_str(r#"{"a":"#);
  }
  s.push('1');
  for _ in 0..n {
    s.push('}');
  }
  s.into_bytes()
}

/// 断言某深度载荷经 SET 根替换臂与 GET 回读漏斗双双被 rust 接受。
fn assert_set_get_accepts(depth: usize) {
  let payload = nest_obj(depth);
  // SET 侧：obj.set 根替换臂内部经 parse_dom 解析载荷，收则回 Success
  let mut obj = GarnetJsonObject::create();
  assert_eq!(
    obj.set(b"$", &payload, ExistOptions::None).unwrap(),
    SetResult::Success,
    "{depth} 层载荷 SET 侧须被 rust 接受（C# 于此带拒，宽向分叉）"
  );
  // GET 侧：from_slice 经 parse_dom 装载落库信封，再 try_get 出帧回读
  let obj = GarnetJsonObject::from_slice(&payload)
    .unwrap_or_else(|e| panic!("{depth} 层载荷 from_slice 须被接受，实回 {e}"));
  let mut out = Vec::new();
  assert!(
    obj.try_get(&[b"$"], &mut out, None, None, None, 2).unwrap(),
    "{depth} 层载荷 GET 出帧须成功"
  );
  let got = resp_json(&out);
  assert!(got.is_array(), "GET 单根路径须回带 [] 包裹的 [根]");
}

/// C# 界带 64/65 层：C# 于 65 层起拒，rust wext_json 一律收（SET/GET 双臂）——
/// 深链落在此域内，默认测试线程栈安全（实测约 180 层以内不触栈崩）。
#[test]
fn depth_64_65_csharp_reject_band_rust_accepts_set_get() {
  assert_set_get_accepts(64);
  assert_set_get_accepts(65);
}

/// rust 界带订正锁：议题初判「sonic 255 承载、256 拒」经现码亲验不成立——
/// 255/256/300 层载荷 from_slice+set 全部 Ok，钉住「Value 快路绕过
/// MAX_ALLOWED_DEPTH、无逻辑 255 门」之真形。深链超默认栈崩阈（约 180–200），
/// 故在大栈线程内执行以规避**崩溃面**（另轴，非契约放宽），锁的是应答收/拒形。
#[test]
fn depth_255_256_300_no_sonic_gate_rust_accepts() {
  let child = Builder::new()
    .stack_size(256 * 1024 * 1024)
    .spawn(|| {
      for depth in [255usize, 256, 300] {
        assert_set_get_accepts(depth);
      }
    })
    .expect("spawn 大栈线程失败");
  child
    .join()
    .expect("255/256/300 层应全被 rust 接受；线程 panic 即深度门被误加，须查 §161");
}

// ===================== f) JSON.GET 选项带值收尾无路径（§15 f) 收口分叉锁） =====================
//
// 对位 C#：`garnet/modules/GarnetJSON/JsonCommands.cs:JsonGET.Reader` 选项循环
// （:131-161）在选项对完整消费后抵达参数尾时空 token 落 `offset > parseState.Count`
// 臂（:151-153）回 wrong-num-args 帧。rust 侧 `json_get_reader` 切片模式循环
// （set_get.rs:174-186）耗尽退出、`try_get` 零路径臂（json_object.rs:234-243）回
// 全量美化文档——修复 C# 收口缺陷方向，登记全文见 doc/zh/deviations.md §15 f)。
// **严禁按 C# 缺陷形态回改**（补错误臂即破此处断言）。悬空单选项带
// （`JSON.GET k INDENT`）双侧同落路径错误臂，不在本案锁面，勿扩写。

/// 命令级漏斗：经 JSON.GET 注册项的 reader 执行体直调（args 为键后参数域），
/// 断言回成功应答（非错误帧）并返回原始应答字节。
fn json_get_command(payload: &[u8], args: &[&[u8]]) -> Vec<u8> {
  let fns = JsonCommand::match_command(b"JSON.GET")
    .expect("JSON.GET 须在命令表在册")
    .fns();
  let mut out = Vec::new();
  assert!(
    (fns.reader)(payload, args, &mut out, 2),
    "reader 回 false 即落了错误应答臂，与 §15 f) 裁决相悖"
  );
  out
}

/// RESP bulk string 帧体按长度精确剥离（同 `json_commands_test.rs` 手法）。
fn f_bulk_payload(bytes: &[u8]) -> &[u8] {
  let s = from_utf8(bytes).unwrap();
  assert!(s.starts_with('$'), "期望 RESP bulk string，实得 {s}");
  let first_crlf = s.find("\r\n").unwrap();
  let len: usize = s[1..first_crlf].parse().unwrap();
  let start = first_crlf + 2;
  &bytes[start..start + len]
}

#[test]
fn json_get_trailing_options_no_path_returns_full_pretty_document() {
  // 落库信封：与 json_get_formatting_options_test 同文档，pretty 期望逐字节同构
  let mut obj = GarnetJsonObject::create();
  obj
    .set(
      b"$",
      br#"{"a":1,"b":[1,2],"c":{"x":"y"}}"#,
      ExistOptions::None,
    )
    .unwrap();
  let mut payload = Vec::new();
  obj.serialize_object(&mut payload).unwrap();

  const PRETTY: &[u8] = br#"{
  "a": 1,
  "b": [
    1,
    2
  ],
  "c": {
    "x": "y"
  }
}"#;

  // 形态一：JSON.GET k INDENT "  " —— 选项带值收尾、无路径。C# 于此回
  // wrong-num-args 错误帧；rust 锁全量美化文档成功应答（选项取值忽略，§15 同谱）。
  assert_eq!(
    f_bulk_payload(&json_get_command(&payload, &[b"INDENT", b"  "])),
    PRETTY,
    "INDENT 带值收尾无路径须回全量美化文档（§15 f)）"
  );

  // 形态二：JSON.GET k INDENT i NEWLINE n —— 双选项对完整消费后参数耗尽，同锁。
  assert_eq!(
    f_bulk_payload(&json_get_command(
      &payload,
      &[b"INDENT", b"i", b"NEWLINE", b"n"]
    )),
    PRETTY,
    "INDENT+NEWLINE 带值收尾无路径须回全量美化文档（§15 f)）"
  );

  // 参照用例：JSON.GET k INDENT i $.a 正常路径主干不受影响——单路径回结果数组
  // 形态 [美化匹配]（$ 根为 [全量]，$.a 标量匹配逐字节即 [1] 数组帧）。
  assert_eq!(
    f_bulk_payload(&json_get_command(&payload, &[b"INDENT", b"i", b"$.a"])),
    b"[1]",
    "选项后带路径的正常应答形态须保持不变（参照锁）"
  );
}
