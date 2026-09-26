//! 对标 test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs
//!
//! 自研依据: JSONPath 解析器（C# 对应 JSONPath 解析语义）

use wext_json::JsonPath;

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:BooleanQuery_TwoValues
#[test]
fn boolean_query_two_values() {
  assert!(JsonPath::parse("$[?('a' == 'a')]").is_ok());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:BooleanQuery_TwoPaths
#[test]
fn boolean_query_two_paths() {
  assert!(JsonPath::parse("$[?(@.a == @.b)]").is_ok());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:SingleProperty
#[test]
fn single_property() {
  assert!(JsonPath::parse("$.prop").is_ok());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:SingleQuotedProperty
#[test]
fn single_quoted_property() {
  assert!(JsonPath::parse("$['prop']").is_ok());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:SingleQuotedPropertyWithWhitespace
#[test]
fn single_quoted_property_with_whitespace() {
  assert!(JsonPath::parse("$[' prop ']").is_ok());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:SingleQuotedPropertyWithDots
#[test]
fn single_quoted_property_with_dots() {
  assert!(JsonPath::parse("$['a.b.c']").is_ok());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:SingleQuotedPropertyWithBrackets
#[test]
fn single_quoted_property_with_brackets() {
  assert!(JsonPath::parse("$['[a]']").is_ok());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:SinglePropertyWithRoot
#[test]
fn single_property_with_root() {
  assert!(JsonPath::parse("$.a").is_ok());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:SinglePropertyWithRootWithStartAndEndWhitespace
#[test]
fn single_property_with_root_with_start_and_end_whitespace() {
  assert!(JsonPath::parse(" $.a ").is_ok());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:RootWithBadWhitespace
#[test]
fn root_with_bad_whitespace() {
  assert!(JsonPath::parse("$ .a").is_err());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:NoFieldNameAfterDot
#[test]
fn no_field_name_after_dot() {
  assert!(JsonPath::parse("$. ").is_err());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:RootWithBadWhitespace2
#[test]
fn root_with_bad_whitespace2() {
  assert!(JsonPath::parse(" $ .a ").is_err());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:WildcardPropertyWithRoot
#[test]
fn wildcard_property_with_root() {
  assert!(JsonPath::parse("$.*").is_ok());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:WildcardArrayWithRoot
#[test]
fn wildcard_array_with_root() {
  assert!(JsonPath::parse("$[*]").is_ok());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:RootArrayNoDot
#[test]
fn root_array_no_dot() {
  assert!(JsonPath::parse("$[0]").is_ok());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:WildcardArray
#[test]
fn wildcard_array() {
  assert!(JsonPath::parse("[*]").is_ok());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:WildcardArrayWithProperty
#[test]
fn wildcard_array_with_property() {
  assert!(JsonPath::parse("$[*].prop").is_ok());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:QuotedWildcardPropertyWithRoot
#[test]
fn quoted_wildcard_property_with_root() {
  assert!(JsonPath::parse("$['*']").is_ok());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:SingleScanWithRoot
#[test]
fn single_scan_with_root() {
  assert!(JsonPath::parse("$..prop").is_ok());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:QueryTrue
#[test]
fn query_true() {
  assert!(JsonPath::parse("$[?(true)]").is_ok());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:ScanQuery
#[test]
fn scan_query() {
  assert!(JsonPath::parse("$..[?(@.a)]").is_ok());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:WildcardScanWithRoot
#[test]
fn wildcard_scan_with_root() {
  assert!(JsonPath::parse("$..*").is_ok());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:WildcardScanWithRootWithWhitespace
#[test]
fn wildcard_scan_with_root_with_whitespace() {
  assert!(JsonPath::parse(" $..* ").is_ok());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:TwoProperties
#[test]
fn two_properties() {
  assert!(JsonPath::parse("$.a.b").is_ok());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:OnePropertyOneScan
#[test]
fn one_property_one_scan() {
  assert!(JsonPath::parse("$.a..b").is_ok());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:SinglePropertyAndIndexer
#[test]
fn single_property_and_indexer() {
  assert!(JsonPath::parse("$.a[0]").is_ok());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:SinglePropertyAndExistsQuery
#[test]
fn single_property_and_exists_query() {
  assert!(JsonPath::parse("$.a[?(@.b)]").is_ok());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:SinglePropertyAndFilterWithWhitespace
#[test]
fn single_property_and_filter_with_whitespace() {
  assert!(JsonPath::parse("$.a[?(@.b == 1)]").is_ok());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:SinglePropertyAndFilterWithEscapeQuote
#[test]
fn single_property_and_filter_with_escape_quote() {
  assert!(JsonPath::parse(r#"$.a[?(@.b == 'h\'i')]"#).is_ok());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:SinglePropertyAndFilterWithDoubleEscape
#[test]
fn single_property_and_filter_with_double_escape() {
  assert!(JsonPath::parse(r#"$.a[?(@.b == '\\')]"#).is_ok());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:SinglePropertyAndFilterWithRegexAndOptions
#[test]
fn single_property_and_filter_with_regex_and_options() {
  assert!(JsonPath::parse("$.a[?(@.b =~ /pattern/i)]").is_ok());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:SinglePropertyAndFilterWithRegex
#[test]
fn single_property_and_filter_with_regex() {
  assert!(JsonPath::parse("$.a[?(@.b =~ /pattern/)]").is_ok());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:SinglePropertyAndFilterWithEscapedRegex
#[test]
fn single_property_and_filter_with_escaped_regex() {
  assert!(JsonPath::parse(r#"$.a[?(@.b =~ /\/pattern/)]"#).is_ok());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:SinglePropertyAndFilterWithOpenRegex
#[test]
fn single_property_and_filter_with_open_regex() {
  assert!(JsonPath::parse("$.a[?(@.b =~ /pattern)]").is_err());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:SinglePropertyAndFilterWithUnknownEscape
///
/// C# 该用例断言 Throws（ReadQuotedString 的 default 臂对未知转义抛错），rust 旧实现
/// 静默丢反斜杠并断言 is_ok，属反向测试锚，现按 C# 口径翻正为 is_err。
#[test]
fn single_property_and_filter_with_unknown_escape() {
  assert!(JsonPath::parse(r#"$.a[?(@.b == '\z')]"#).is_err());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:SinglePropertyAndFilterWithFalse
#[test]
fn single_property_and_filter_with_false() {
  assert!(JsonPath::parse("$.a[?(@.b == false)]").is_ok());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:SinglePropertyAndFilterWithTrue
#[test]
fn single_property_and_filter_with_true() {
  assert!(JsonPath::parse("$.a[?(@.b == true)]").is_ok());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:SinglePropertyAndFilterWithNull
#[test]
fn single_property_and_filter_with_null() {
  assert!(JsonPath::parse("$.a[?(@.b == null)]").is_ok());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:FilterWithScan
#[test]
fn filter_with_scan() {
  assert!(JsonPath::parse("$.a..[?(@.b == 1)]").is_ok());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:FilterWithNotEquals
#[test]
fn filter_with_not_equals() {
  assert!(JsonPath::parse("$.a[?(@.b != 1)]").is_ok());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:FilterWithNotEquals2
#[test]
fn filter_with_not_equals2() {
  assert!(JsonPath::parse("$.a[?(@.b !== 1)]").is_ok());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:FilterWithLessThan
#[test]
fn filter_with_less_than() {
  assert!(JsonPath::parse("$.a[?(@.b < 1)]").is_ok());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:FilterWithLessThanOrEquals
#[test]
fn filter_with_less_than_or_equals() {
  assert!(JsonPath::parse("$.a[?(@.b <= 1)]").is_ok());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:FilterWithGreaterThan
#[test]
fn filter_with_greater_than() {
  assert!(JsonPath::parse("$.a[?(@.b > 1)]").is_ok());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:FilterWithGreaterThanOrEquals
#[test]
fn filter_with_greater_than_or_equals() {
  assert!(JsonPath::parse("$.a[?(@.b >= 1)]").is_ok());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:FilterWithInteger
#[test]
fn filter_with_integer() {
  assert!(JsonPath::parse("$.a[?(@.b == 42)]").is_ok());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:FilterWithNegativeInteger
#[test]
fn filter_with_negative_integer() {
  assert!(JsonPath::parse("$.a[?(@.b == -42)]").is_ok());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:FilterWithFloat
#[test]
fn filter_with_float() {
  assert!(JsonPath::parse("$.a[?(@.b == 3.14)]").is_ok());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:FilterExistWithAnd
#[test]
fn filter_exist_with_and() {
  assert!(JsonPath::parse("$.a[?(@.b && @.c)]").is_ok());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:FilterExistWithAndOr
#[test]
fn filter_exist_with_and_or() {
  assert!(JsonPath::parse("$.a[?(@.b && @.c || @.d)]").is_ok());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:FilterWithRoot
#[test]
fn filter_with_root() {
  assert!(JsonPath::parse("$.a[?(@.b < $.max)]").is_ok());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:BadOr1
#[test]
fn bad_or1() {
  assert!(JsonPath::parse("$.a[?(@.b ||)]").is_err());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:BaddOr2
#[test]
fn badd_or2() {
  assert!(JsonPath::parse("$.a[?(|| @.b)]").is_err());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:BaddOr3
#[test]
fn badd_or3() {
  assert!(JsonPath::parse("$.a[?(@.b || |)]").is_err());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:BaddOr4
#[test]
fn badd_or4() {
  assert!(JsonPath::parse("$.a[?(@.b || &)]").is_err());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:NoAtAfterOr
#[test]
fn no_at_after_or() {
  assert!(JsonPath::parse("$.a[?(@.b || c)]").is_err());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:NoPathAfterAt
#[test]
fn no_path_after_at() {
  assert!(JsonPath::parse("$.a[?(@)]").is_ok());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:NoPathAfterDot
#[test]
fn no_path_after_dot() {
  assert!(JsonPath::parse("$.a[?(@.)]").is_err());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:NoPathAfterDot2
#[test]
fn no_path_after_dot2() {
  assert!(JsonPath::parse("$.a[?(@.b.)]").is_err());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:FilterWithFloatExp
#[test]
fn filter_with_float_exp() {
  assert!(JsonPath::parse("$.a[?(@.b == 1e5)]").is_ok());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:MultiplePropertiesAndIndexers
#[test]
fn multiple_properties_and_indexers() {
  assert!(JsonPath::parse("$.a.b[0].c['d'][1]").is_ok());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:BadCharactersInIndexer
#[test]
fn bad_characters_in_indexer() {
  assert!(JsonPath::parse("$.a[xyz]").is_err());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:UnclosedIndexer
#[test]
fn unclosed_indexer() {
  assert!(JsonPath::parse("$.a[0").is_err());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:IndexerOnly
#[test]
fn indexer_only() {
  assert!(JsonPath::parse("[0]").is_ok());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:IndexerOnlyWithWhitespace
#[test]
fn indexer_only_with_whitespace() {
  assert!(JsonPath::parse(" [0] ").is_ok());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:MultipleIndexes
#[test]
fn multiple_indexes() {
  assert!(JsonPath::parse("$[0, 1, 2]").is_ok());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:MultipleIndexesWithWhitespace
#[test]
fn multiple_indexes_with_whitespace() {
  assert!(JsonPath::parse("$[ 0 , 1 , 2 ]").is_ok());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:MultipleQuotedIndexes
#[test]
fn multiple_quoted_indexes() {
  assert!(JsonPath::parse("$['a', 'b']").is_ok());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:MultipleQuotedIndexesWithWhitespace
#[test]
fn multiple_quoted_indexes_with_whitespace() {
  assert!(JsonPath::parse("$[ 'a' , 'b' ]").is_ok());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:SlicingIndexAll
#[test]
fn slicing_index_all() {
  assert!(JsonPath::parse("$[:]").is_ok());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:SlicingIndex
#[test]
fn slicing_index() {
  assert!(JsonPath::parse("$[1:5]").is_ok());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:SlicingIndexNegative
#[test]
fn slicing_index_negative() {
  assert!(JsonPath::parse("$[-5:-1]").is_ok());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:SlicingIndexEmptyStop
#[test]
fn slicing_index_empty_stop() {
  assert!(JsonPath::parse("$[1:]").is_ok());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:SlicingIndexEmptyStart
#[test]
fn slicing_index_empty_start() {
  assert!(JsonPath::parse("$[:5]").is_ok());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:SlicingIndexWhitespace
#[test]
fn slicing_index_whitespace() {
  assert!(JsonPath::parse("$[ 1 : 5 ]").is_ok());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:EmptyIndexer
#[test]
fn empty_indexer() {
  assert!(JsonPath::parse("$[]").is_err());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:IndexerCloseInProperty
#[test]
fn indexer_close_in_property() {
  assert!(JsonPath::parse("$.a]").is_err());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:AdjacentIndexers
#[test]
fn adjacent_indexers() {
  assert!(JsonPath::parse("$[0][1]").is_ok());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:MissingDotAfterIndexer
#[test]
fn missing_dot_after_indexer() {
  assert!(JsonPath::parse("$[0]prop").is_ok());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:PropertyFollowingEscapedPropertyName
#[test]
fn property_following_escaped_property_name() {
  assert!(JsonPath::parse(r#"$['a\'b'].c"#).is_ok());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:ArrayOfArrayValue
#[test]
fn array_of_array_value() {
  assert!(JsonPath::parse("$[0][1]").is_ok());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathParseTests.cs:ArrayOfArrayValueWithEscaping
#[test]
fn array_of_array_value_with_escaping() {
  assert!(JsonPath::parse(r#"$['a']['b']"#).is_ok());
}

/// 票 zcode-r24-wextjson 发现一（P0）：`in` 数组字面量含裸 token 必须上抛错误，不得死循环空转
/// 对位 C# JsonPath.cs:TryParseArrayLiteral——括号深度扫描定位边界后交 JSON 解析，非法即抛 JsonException
#[test]
fn array_literal_bare_token_errors_without_hang() {
  assert!(JsonPath::parse("$[?(@.a in [x])]").is_err());
  assert!(JsonPath::parse("$[?(@.a in [+])]").is_err());
  // 正例对照：合法数组字面量保持 Ok（含嵌套数组，对位 C# ArrayOfArrayValue）
  assert!(JsonPath::parse("$[?(@.a in [1,2])]").is_ok());
  assert!(JsonPath::parse("$[?(@.a in [[1,2],[2,3]])]").is_ok());
}
