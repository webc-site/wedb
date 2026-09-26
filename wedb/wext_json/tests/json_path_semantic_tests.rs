//! zcode-r24-wextjson 修复轮的语义固化用例：引号字段名原文切片、转义族与码点解析、
//! in/nin 词边界、与或挂链优先级、MatchTokens 容器门、谓词根上下文裁决、数值精确相等。
//!
//! 对位 C# garnet/modules/GarnetJSON/JSONPath/JsonPath.cs（ReadQuotedString /
//! TryParseEscapedCodepoint / TryParseValue / ParseOperator / ParseExpression）与
//! garnet/modules/GarnetJSON/JSONPath/QueryExpression.cs（MatchTokens /
//! EqualsWithStringCoercion）。
//!
//! 自研偏差锁: doc/zh/deviations.md JSON 路径语义锁（C# 对应 modules GarnetJSON? JSONPath 面）

use sonic_rs::Value;
use wext_json::{JsonPath, PathFilter};

fn doc(json: &str) -> Value {
  sonic_rs::from_str(json).unwrap()
}

/// 取路径末端 Field 过滤器的字段名（引号形态解析结果的直接取证口）
fn field_name(path: &str) -> Option<String> {
  let parsed = JsonPath::parse(path).unwrap();
  match parsed.filters.last() {
    Some(PathFilter::Field { name }) => name.clone(),
    other => panic!("期望 Field 终结过滤器，实得 {other:?}"),
  }
}

// ===================== 发现二：引号字段名与转义族 =====================

/// 引号字段名必须保留原文 UTF-8 字节序列（C# 入口 Encoding.UTF8.GetString 后逐 char
/// 拼接，无 Latin-1 mojibake 面）：中文、日文、emoji 键名整族命中。
#[test]
fn quoted_field_name_keeps_non_ascii_text() {
  let val = doc(r#"{"中文":1,"日本語":2,"😀":3}"#);

  let zh = JsonPath::parse("$['中文']").unwrap();
  assert_eq!(zh.evaluate(&val).len(), 1);
  let zh_double = JsonPath::parse("$[\"中文\"]").unwrap();
  assert_eq!(zh_double.evaluate(&val).len(), 1);
  let ja = JsonPath::parse("$['日本語']").unwrap();
  assert_eq!(ja.evaluate(&val).len(), 1);
  let emoji = JsonPath::parse("$['😀']").unwrap();
  assert_eq!(emoji.evaluate(&val).len(), 1);

  // 多字段引号形态同样按原文匹配
  let multi = JsonPath::parse("$['中文','日本語']").unwrap();
  assert_eq!(multi.evaluate(&val).len(), 2);
}

/// 引号内非转义段按原文切片：解析产物逐字节等于路径原文，不产出 U+0080..U+00FF 码点。
#[test]
fn quoted_field_name_is_byte_identical_to_source() {
  assert_eq!(field_name("$['中文']").as_deref(), Some("中文"));
  assert_eq!(field_name("$['a中文b']").as_deref(), Some("a中文b"));
  assert_eq!(field_name("$['😀']").as_deref(), Some("😀"));
}

/// C# ReadQuotedString 的 switch 臂全集：b/t/n/f/r 与 \\ " ' / 原样入串。
#[test]
fn quoted_field_name_supports_full_escape_family() {
  assert_eq!(field_name(r"$['a\bb']").as_deref(), Some("a\u{8}b"));
  assert_eq!(field_name(r"$['a\tb']").as_deref(), Some("a\tb"));
  assert_eq!(field_name(r"$['a\nb']").as_deref(), Some("a\nb"));
  assert_eq!(field_name(r"$['a\fb']").as_deref(), Some("a\u{c}b"));
  assert_eq!(field_name(r"$['a\rb']").as_deref(), Some("a\rb"));
  assert_eq!(field_name(r"$['a\\b']").as_deref(), Some("a\\b"));
  assert_eq!(field_name(r"$['a\/b']").as_deref(), Some("a/b"));
  // 单引号内转义单引号（C# 同臂），双引号内转义双引号
  assert_eq!(field_name(r"$['a\'b']").as_deref(), Some("a'b"));
  assert_eq!(field_name(r#"$["a\"b"]"#).as_deref(), Some("a\"b"));
}

/// C# default 臂：未知转义抛 "Unknown escape character"；串尾落单反斜杠按
/// "Path ended with an open string." 抛错（rust 旧实现静默丢反斜杠）。
#[test]
fn unknown_escape_is_rejected() {
  assert!(JsonPath::parse(r"$['a\qb']").is_err());
  assert!(JsonPath::parse(r"$['\z']").is_err());
  assert!(JsonPath::parse(r"$['中文\']").is_err());
}

/// C# TryParseEscapedCodepoint：`\u` / `\U` + 4 位十六进制；紧随小写 `\u` + 4 位时按
/// 代理对公式合并；落在 surrogate 区或越界即抛 "Invalid UTF-32 code point"。
#[test]
fn unicode_escapes_merge_surrogate_pairs() {
  assert_eq!(field_name(r"$['\u4e2d']").as_deref(), Some("中"));
  assert_eq!(field_name(r"$['\U4e2D']").as_deref(), Some("中"));
  assert_eq!(field_name(r"$['\u0041']").as_deref(), Some("A"));
  assert_eq!(field_name(r"$['\ud83d\ude00']").as_deref(), Some("😀"));
  // 合并段前为合法码点、后随孤立低代理：C# 合并算式产出越界值后仍按非法码点抛错
  assert!(JsonPath::parse(r"$['\ud800']").is_err());
  assert!(JsonPath::parse(r"$['\ud83d']").is_err());
  assert!(JsonPath::parse(r"$['\udc00']").is_err());
  assert!(JsonPath::parse(r"$['\uffff\ud800']").is_err());
  assert!(JsonPath::parse(r"$['\uZZZZ']").is_err());
}

/// C# TryParseValue：字面量首字符门为精确小写，整词由 Match（OrdinalIgnoreCase）匹配；
/// 数值形态不识别前导 '+'。
#[test]
fn literal_tokens_follow_case_gate() {
  // 首字母小写 + 任意大小写词身：C# 命中
  assert!(JsonPath::parse("$[?(@.a == tRUE)]").is_ok());
  assert!(JsonPath::parse("$[?(@.a == fALse)]").is_ok());
  assert!(JsonPath::parse("$[?(@.a == nUlL)]").is_ok());
  // 首字母大写：C# 首字符门即不识别，整侧按 Unexpected character 抛错
  assert!(JsonPath::parse("$[?(@.a == True)]").is_err());
  assert!(JsonPath::parse("$[?(@.a == FALSE)]").is_err());
  assert!(JsonPath::parse("$[?(@.a == NULL)]").is_err());
  assert!(JsonPath::parse("$[?(@.a == +1)]").is_err());
}

// ===================== 发现三：in / nin 词边界 =====================

/// C# ParseOperator 无 nin 分支：`nin` 直落末尾抛 "Could not read query operator."，
/// rust 旧实现吞首字母后按 In 求值（语义反转），现按 C# 报错口径。
#[test]
fn nin_operator_is_rejected() {
  assert!(JsonPath::parse("$[?(@.a nin [1,2])]").is_err());
  assert!(JsonPath::parse("$[?(@.a nin[1,2])]").is_err());
  assert!(JsonPath::parse("$[?(@.a NIN [1,2])]").is_err());
}

/// C# Match("in") 不要求尾随空格：`in [1,2]` 与 `in[1,2]` 两形态同解析同结果。
#[test]
fn in_operator_needs_no_trailing_space() {
  let val = doc(r#"[{"a":[1,2]}]"#);
  let spaced = JsonPath::parse("$[?(@.a in [1,2])]").unwrap();
  let tight = JsonPath::parse("$[?(@.a in[1,2])]").unwrap();
  let upper = JsonPath::parse("$[?(@.a IN [1,2])]").unwrap();
  // C# MatchTokens 的容器门使 In 的右值（数组）永不进比较族 switch，CheckIn 不可达，
  // 故三形态在 C# 侧同为空结果集；rust 逐臂对齐 C# 代码（差异已登记 deviations.md）。
  assert_eq!(spaced.evaluate(&val).len(), tight.evaluate(&val).len());
  assert_eq!(spaced.evaluate(&val).len(), upper.evaluate(&val).len());
}

/// 以 in/nin 起头或含其前缀的成员名不得被误判为运算符（词边界判定的反向锁）。
#[test]
fn member_names_containing_in_are_not_operators() {
  let val = doc(r#"[{"index":1,"a":{"in":2},"ninja":3}]"#);

  let p1 = JsonPath::parse("$[?(@.index == 1)]").unwrap();
  assert_eq!(p1.evaluate(&val).len(), 1);
  let p2 = JsonPath::parse("$[?(@.a.in == 2)]").unwrap();
  assert_eq!(p2.evaluate(&val).len(), 1);
  let p3 = JsonPath::parse("$[?(@.ninja == 3)]").unwrap();
  assert_eq!(p3.evaluate(&val).len(), 1);
  // 紧贴 '@' 的 "in" 属成员名（C# ParsePath 的 default 臂逐字符吞入）
  let p4 = JsonPath::parse("$[?(@.in == 2)]").unwrap();
  assert_eq!(p4.evaluate(&val).len(), 0);
}

// ===================== 发现四：与或挂链优先级 =====================

/// C# ParseExpression 的 parentExpression 链是序敏感挂链而非标准优先级：
/// `a || b && c` → Or(a, And(b, c))，故 a 真而 c 假时整条仍匹配。
#[test]
fn or_then_and_chains_as_csharp_parent_expression() {
  let val =
    doc(r#"[{"a":1,"b":2,"c":9},{"a":9,"b":2,"c":9},{"a":9,"b":2,"c":3},{"a":9,"b":9,"c":9}]"#);
  let path = JsonPath::parse("$[?(@.a == 1 || @.b == 2 && @.c == 3)]").unwrap();
  // Or(A, And(B,C))：元素1 A真→匹配；元素2 A假且C假→不匹配；元素3 B真C真→匹配；元素4 全假
  assert_eq!(path.evaluate(&val).len(), 2);
}

/// `a && b || c` 在 C# 产出 And(a, Or(b, c))：a 假而 b/c 真时整条不匹配
/// （标准优先级归约 (a&&b)||c 会给出相反结果，故此处是形态分叉的判别位）。
#[test]
fn and_then_or_chains_as_csharp_parent_expression() {
  let val = doc(r#"[{"a":9,"b":2,"c":3},{"a":1,"b":9,"c":9}]"#);
  let path = JsonPath::parse("$[?(@.a == 1 && @.b == 2 || @.c == 3)]").unwrap();
  // And(A, Or(B,C))：元素1 A假→不匹配；元素2 A真但 B、C 皆假→不匹配
  assert_eq!(path.evaluate(&val).len(), 0);

  let val2 = doc(r#"[{"a":1,"b":9,"c":3}]"#);
  let path2 = JsonPath::parse("$[?(@.a == 1 && @.b == 2 || @.c == 3)]").unwrap();
  // And(A, Or(B,C))：A真且 C真 → 匹配
  assert_eq!(path2.evaluate(&val2).len(), 1);
}

/// 运算符每次切换都新开一层：`a && b || c && d || e` 在 C# 产出
/// And(A, Or(B, And(C, Or(D, E))))，最外层 A 假即整条不匹配。
#[test]
fn alternating_operators_nest_per_csharp_chain() {
  let path =
    JsonPath::parse("$[?(@.a == 1 && @.b == 2 || @.c == 3 && @.d == 4 || @.e == 5)]").unwrap();
  let val = doc(r#"[{"a":9,"b":9,"c":3,"d":4,"e":9},{"a":1,"b":9,"c":3,"d":4,"e":9}]"#);
  // 元素1 A假 → And 整体假；元素2 A真且 C、D 真 → 命中
  assert_eq!(path.evaluate(&val).len(), 1);

  let val2 = doc(r#"[{"a":1,"b":9,"c":9,"d":4,"e":9}]"#);
  // A真，但 B假且 C假 → Or(B, And(C, Or(D,E))) 假
  assert_eq!(path.evaluate(&val2).len(), 0);
}

/// 同运算符连续：`a && b && c` 并为一棵三子节点 And
/// （C# parentExpression 同型即复用，不新建层级）。
#[test]
fn repeated_operator_folds_into_one_composite() {
  let val = doc(r#"[{"a":1,"b":2,"c":3},{"a":1,"b":2,"c":9}]"#);
  let path = JsonPath::parse("$[?(@.a == 1 && @.b == 2 && @.c == 3)]").unwrap();
  assert_eq!(path.evaluate(&val).len(), 1);
}

/// 分组 `( ... )`：Exists 位解包为内层表达式（C# JsonPath.cs:734-739），
/// 非 Exists 位按单元素 [QueryFilter] 路径侧参与比较。
#[test]
fn grouped_subexpressions_unwrap_at_exists_position() {
  let val = doc(r#"[{"a":1,"b":9},{"a":9,"b":2},{"a":9,"b":9}]"#);
  let path = JsonPath::parse("$[?((@.a == 1) || (@.b == 2))]").unwrap();
  assert_eq!(path.evaluate(&val).len(), 2);

  let not_path = JsonPath::parse("$[?(!(@.a == 1))]").unwrap();
  assert_eq!(not_path.evaluate(&val).len(), 2);

  let mixed = JsonPath::parse("$[?((@.a == 1 || @.b == 2) && @.a != 1)]").unwrap();
  assert_eq!(mixed.evaluate(&val).len(), 1);
}

/// 运算符两侧的空白形态：C# 由 ParseSide 的 EatWhitespace 兜住，且 `!` 前缀只在
/// 循环顶部逐字符判定（`a && !b` 在 C# 侧因 '!' 前有空白而报 Unexpected character）。
#[test]
fn not_prefix_requires_no_whitespace_before_it() {
  assert!(JsonPath::parse("$[?(@.a == 1&&!@.b == 2)]").is_ok());
  assert!(JsonPath::parse("$[?(@.a == 1 && !@.b == 2)]").is_err());
}

// ===================== 发现五：MatchTokens 容器门 =====================

/// C# MatchTokens:210 的门禁：任一侧为数组/对象时落 else 臂（:242-247），该臂仅对
/// Exists / NotEquals 回 true，其余运算符（含 StrictEquals、StrictNotEquals、比较族、
/// In）一律 false —— rust 旧实现对容器做深比较，现按 C# 代码逐臂对齐。
#[test]
fn container_operands_bypass_comparison_family() {
  let val = doc(r#"[{"x":[1,2],"y":[1,2]},{"x":{"k":1},"y":{"k":1}}]"#);

  // Equals / StrictEquals：相等容器在 C# 侧不匹配
  let eq = JsonPath::parse("$[?(@.x == @.y)]").unwrap();
  assert_eq!(eq.evaluate(&val).len(), 0);
  let seq = JsonPath::parse("$[?(@.x === @.y)]").unwrap();
  assert_eq!(seq.evaluate(&val).len(), 0);
  // NotEquals：C# else 臂恒 true
  let ne = JsonPath::parse("$[?(@.x != @.y)]").unwrap();
  assert_eq!(ne.evaluate(&val).len(), 2);
  // StrictNotEquals：C# else 臂未列该运算符，恒 false
  let sne = JsonPath::parse("$[?(@.x !== @.y)]").unwrap();
  assert_eq!(sne.evaluate(&val).len(), 0);
  // 比较族同样恒 false
  let gt = JsonPath::parse("$[?(@.x > @.y)]").unwrap();
  assert_eq!(gt.evaluate(&val).len(), 0);
  let ge = JsonPath::parse("$[?(@.x >= @.y)]").unwrap();
  assert_eq!(ge.evaluate(&val).len(), 0);

  // 容器 vs 标量亦落 else 臂
  let mixed = doc(r#"[{"x":[1,2],"y":"s"}]"#);
  let meq = JsonPath::parse("$[?(@.x == 's')]").unwrap();
  assert_eq!(meq.evaluate(&mixed).len(), 0);
  let mne = JsonPath::parse("$[?(@.x != 's')]").unwrap();
  assert_eq!(mne.evaluate(&mixed).len(), 1);

  // 标量对比较不受容器门影响
  let scalars = doc(r#"[{"x":1,"y":1},{"x":[1],"y":1}]"#);
  let seq2 = JsonPath::parse("$[?(@.x == @.y)]").unwrap();
  assert_eq!(seq2.evaluate(&scalars).len(), 1);

  // 存在性谓词不受容器门影响
  let exists = JsonPath::parse("$[?(@.x)]").unwrap();
  assert_eq!(exists.evaluate(&val).len(), 2);
}

// ===================== 发现六：谓词根上下文裁决 =====================

/// has_predicate 于解析期一次判定：含 Query / QueryScan 的路径为 true，其余 false。
#[test]
fn has_predicate_flag_computed_at_parse() {
  assert!(JsonPath::parse("$[?(@.a == 1)]").unwrap().has_predicate);
  assert!(JsonPath::parse("$..[?(@.a == 1)]").unwrap().has_predicate);
  assert!(
    JsonPath::parse("$.books[?(@.price > 20)]")
      .unwrap()
      .has_predicate
  );
  assert!(!JsonPath::parse("$.a.b").unwrap().has_predicate);
  assert!(!JsonPath::parse("$").unwrap().has_predicate);
  assert!(!JsonPath::parse("$..name").unwrap().has_predicate);
  assert!(!JsonPath::parse("$.arr[1]").unwrap().has_predicate);
  assert!(!JsonPath::parse("$['中文']").unwrap().has_predicate);
}

/// 无谓词路径的变异结果与快照版逐字节一致（根上下文永不读取的语义锁）。
#[test]
fn no_predicate_mutation_is_unaffected_by_root_context() {
  let mut val = doc(r#"{"a":{"b":1},"arr":[1,2,3]}"#);
  let hits = JsonPath::parse("$.a.b")
    .unwrap()
    .replace_matches(&mut val, &doc("9"));
  assert_eq!(hits, 1);
  assert_eq!(val, doc(r#"{"a":{"b":9},"arr":[1,2,3]}"#));

  let deleted = JsonPath::parse("$.arr[*]")
    .unwrap()
    .delete_matches(&mut val);
  assert_eq!(deleted, 3);
  assert_eq!(val, doc(r#"{"a":{"b":9},"arr":[]}"#));
}

/// 谓词路径变异保留变异前快照语义：`$..[?(@.price > 20)]` 命中集与结果与快照版一致。
#[test]
fn predicate_mutation_keeps_pre_mutation_snapshot_semantics() {
  let mut val = doc(r#"{"books":[{"price":30},{"price":10},{"price":50}]}"#);
  let hits = JsonPath::parse("$..[?(@.price > 20)]")
    .unwrap()
    .replace_matches(&mut val, &doc(r#"{"price":0}"#));
  assert_eq!(hits, 2);
  assert_eq!(
    val,
    doc(r#"{"books":[{"price":0},{"price":10},{"price":0}]}"#)
  );
}

// ===================== 发现七：数值相等精确比较 =====================

/// C# EqualsWithStringCoercion 的数值分支为 leftNum.Equals(rightNum)：逐位精确，
/// 0.30000000000000004 与 0.3 判不等（rust 旧实现用 f64::EPSILON 容差判等）。
#[test]
fn numeric_equality_is_bitwise_exact() {
  let acc = doc(r#"[{"a":0.30000000000000004}]"#);
  let eq = JsonPath::parse("$[?(@.a == 0.3)]").unwrap();
  assert_eq!(eq.evaluate(&acc).len(), 0);
  let ne = JsonPath::parse("$[?(@.a != 0.3)]").unwrap();
  assert_eq!(ne.evaluate(&acc).len(), 1);

  let exact = doc(r#"[{"a":0.3}]"#);
  assert_eq!(eq.evaluate(&exact).len(), 1);
  assert_eq!(ne.evaluate(&exact).len(), 0);
}
