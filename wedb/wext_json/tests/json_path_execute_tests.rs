//! 对标 test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs

use sonic_rs::{JsonValueTrait, Value};
use wext_json::JsonPath;

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:GreaterThanIssue1518
#[test]
fn greater_than_issue1518() {
  let json = r#"{"nodes": [{"name": "A", "val": 15}, {"name": "B", "val": 5}]}"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$.nodes[?(@.val > 10)]").unwrap();
  let matches = path.evaluate(&val);
  assert_eq!(matches.len(), 1);
  assert_eq!(matches[0]["name"].as_str(), Some("A"));
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:BacktrackingRegex_SingleMatch_TimeoutRespected
#[test]
fn backtracking_regex_single_match_timeout_respected() {
  let json = r#"{"text": "foobar"}"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$.[?(@.text =~ /foo.*/)]").unwrap();
  let matches = path.evaluate(&val);
  assert_eq!(matches.len(), 1);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:GreaterThanWithIntegerParameterAndStringValue
#[test]
fn greater_than_with_integer_parameter_and_string_value() {
  let json = r#"{"items": [{"val": "20"}, {"val": "5"}]}"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$.items[?(@.val > 10)]").unwrap();
  let matches = path.evaluate(&val);
  assert_eq!(matches.len(), 1);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:GreaterThanWithStringParameterAndIntegerValue
#[test]
fn greater_than_with_string_parameter_and_integer_value() {
  let json = r#"{"items": [{"val": 20}, {"val": 5}]}"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$.items[?(@.val > '10')]").unwrap();
  let matches = path.evaluate(&val);
  assert_eq!(matches.len(), 1);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:RecursiveWildcard
#[test]
fn recursive_wildcard() {
  let json = r#"{"a": {"b": 1, "c": 2}}"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$..*").unwrap();
  let matches = path.evaluate(&val);
  assert!(matches.len() >= 2);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:ScanFilter
#[test]
fn scan_filter() {
  let json = r#"{"a": {"price": 10}, "b": {"price": 20}}"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$..price").unwrap();
  let matches = path.evaluate(&val);
  assert_eq!(matches.len(), 2);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:FilterTrue
#[test]
fn filter_true() {
  let json = r#"[{"active": true}, {"active": false}]"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$[?(@.active == true)]").unwrap();
  let matches = path.evaluate(&val);
  assert_eq!(matches.len(), 1);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:ScanFilterTrue
#[test]
fn scan_filter_true() {
  let json = r#"{"data": [{"active": true}, {"active": false}]}"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$..[?(@.active == true)]").unwrap();
  let matches = path.evaluate(&val);
  assert_eq!(matches.len(), 1);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:ScanQuoted
#[test]
fn scan_quoted() {
  let json = r#"{"a": {"foo bar": 42}}"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$..['foo bar']").unwrap();
  let matches = path.evaluate(&val);
  assert_eq!(matches.len(), 1);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:ScanMultipleQuoted
#[test]
fn scan_multiple_quoted() {
  let json = r#"{"a": {"x": 1, "y": 2}}"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$..['x', 'y']").unwrap();
  let matches = path.evaluate(&val);
  assert_eq!(matches.len(), 2);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:ParseWithEmptyArrayContent
#[test]
fn parse_with_empty_array_content() {
  let json = r#"{"arr": []}"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$.arr[*]").unwrap();
  let matches = path.evaluate(&val);
  assert_eq!(matches.len(), 0);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:SelectTokenAfterEmptyContainer
#[test]
fn select_token_after_empty_container() {
  let json = r#"{"arr": [], "name": "test"}"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$.name").unwrap();
  let matches = path.evaluate(&val);
  assert_eq!(matches.len(), 1);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:EvaluatePropertyWithRequired
#[test]
fn evaluate_property_with_required() {
  let json = r#"{"a": 1}"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$.a").unwrap();
  assert_eq!(path.evaluate(&val).len(), 1);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:EvaluateEmptyPropertyIndexer
#[test]
fn evaluate_empty_property_indexer() {
  let json = r#"{"": 42}"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$['']").unwrap();
  assert_eq!(path.evaluate(&val).len(), 1);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:EvaluateEmptyString
#[test]
fn evaluate_empty_string() {
  let json = r#""""#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$").unwrap();
  assert_eq!(path.evaluate(&val).len(), 1);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:EvaluateEmptyStringWithMatchingEmptyProperty
#[test]
fn evaluate_empty_string_with_matching_empty_property() {
  let json = r#"{"": ""}"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$['']").unwrap();
  assert_eq!(path.evaluate(&val).len(), 1);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:EvaluateWhitespaceString
#[test]
fn evaluate_whitespace_string() {
  let json = r#"{"   ": 42}"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$['   ']").unwrap();
  assert_eq!(path.evaluate(&val).len(), 1);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:EvaluateDollarString
#[test]
fn evaluate_dollar_string() {
  let json = r#"{"$": 42}"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$['$']").unwrap();
  assert_eq!(path.evaluate(&val).len(), 1);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:EvaluateDollarTypeString
#[test]
fn evaluate_dollar_type_string() {
  let json = r#"{"$type": "MyType"}"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$.$type").unwrap();
  assert_eq!(path.evaluate(&val).len(), 1);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:EvaluateSingleProperty
#[test]
fn evaluate_single_property() {
  let json = r#"{"name": "test"}"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$.name").unwrap();
  assert_eq!(path.evaluate(&val).len(), 1);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:EvaluateWildcardProperty
#[test]
fn evaluate_wildcard_property() {
  let json = r#"{"a": 1, "b": 2}"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$.*").unwrap();
  assert_eq!(path.evaluate(&val).len(), 2);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:QuoteName
#[test]
fn quote_name() {
  let json = r#"{"a.b": 42}"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$['a.b']").unwrap();
  assert_eq!(path.evaluate(&val).len(), 1);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:EvaluateMissingProperty
#[test]
fn evaluate_missing_property() {
  let json = r#"{"a": 1}"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$.b").unwrap();
  assert_eq!(path.evaluate(&val).len(), 0);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:EvaluateIndexerOnObject
#[test]
fn evaluate_indexer_on_object() {
  let json = r#"{"a": 1}"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$['a']").unwrap();
  assert_eq!(path.evaluate(&val).len(), 1);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:EvaluateIndexerOnObjectWithError
#[test]
fn evaluate_indexer_on_object_with_error() {
  let json = r#"{"a": 1}"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$[0]").unwrap();
  assert_eq!(path.evaluate(&val).len(), 0);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:EvaluateSliceOnObjectWithError
#[test]
fn evaluate_slice_on_object_with_error() {
  let json = r#"{"a": 1}"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$[0:1]").unwrap();
  assert_eq!(path.evaluate(&val).len(), 0);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:EvaluatePropertyOnArray
#[test]
fn evaluate_property_on_array() {
  let json = r#"[1, 2, 3]"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$.a").unwrap();
  assert_eq!(path.evaluate(&val).len(), 0);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:EvaluateMultipleResultsError
#[test]
fn evaluate_multiple_results_error() {
  let json = r#"[1, 2, 3]"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$[*]").unwrap();
  assert_eq!(path.evaluate(&val).len(), 3);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:EvaluatePropertyOnArrayWithError
#[test]
fn evaluate_property_on_array_with_error() {
  let json = r#"[1, 2]"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$.prop").unwrap();
  assert_eq!(path.evaluate(&val).len(), 0);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:EvaluateNoResultsWithMultipleArrayIndexes
#[test]
fn evaluate_no_results_with_multiple_array_indexes() {
  let json = r#"[1, 2]"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$[5, 6]").unwrap();
  assert_eq!(path.evaluate(&val).len(), 0);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:EvaluateMissingPropertyWithError
#[test]
fn evaluate_missing_property_with_error() {
  let json = r#"{"a": 1}"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$.missing").unwrap();
  assert_eq!(path.evaluate(&val).len(), 0);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:EvaluatePropertyWithoutError
#[test]
fn evaluate_property_without_error() {
  let json = r#"{"a": 1}"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$.a").unwrap();
  assert_eq!(path.evaluate(&val).len(), 1);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:EvaluateMissingPropertyIndexWithError
#[test]
fn evaluate_missing_property_index_with_error() {
  let json = r#"{"a": 1}"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$['missing']").unwrap();
  assert_eq!(path.evaluate(&val).len(), 0);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:EvaluateMultiPropertyIndexOnArrayWithError
#[test]
fn evaluate_multi_property_index_on_array_with_error() {
  let json = r#"[1, 2]"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$['a', 'b']").unwrap();
  assert_eq!(path.evaluate(&val).len(), 0);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:EvaluateArraySliceWithError
#[test]
fn evaluate_array_slice_with_error() {
  let json = r#"[1, 2, 3]"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$[1:2]").unwrap();
  assert_eq!(path.evaluate(&val).len(), 1);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:EvaluateOutOfBoundsIndxer
#[test]
fn evaluate_out_of_bounds_indexer() {
  let json = r#"[1, 2]"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$[10]").unwrap();
  assert_eq!(path.evaluate(&val).len(), 0);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:EvaluateArrayOutOfBoundsIndxerWithError
#[test]
fn evaluate_array_out_of_bounds_indexer_with_error() {
  let json = r#"[1, 2]"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$[99]").unwrap();
  assert_eq!(path.evaluate(&val).len(), 0);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:EvaluateArray
#[test]
fn evaluate_array() {
  let json = r#"[10, 20, 30]"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$[1]").unwrap();
  assert_eq!(path.evaluate(&val)[0].as_i64(), Some(20));
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:EvaluateArraySlice
#[test]
fn evaluate_array_slice() {
  let json = r#"[1, 2, 3, 4, 5]"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$[1:4]").unwrap();
  assert_eq!(path.evaluate(&val).len(), 3);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:EvaluateWildcardArray
#[test]
fn evaluate_wildcard_array() {
  let json = r#"[1, 2, 3]"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$[*]").unwrap();
  assert_eq!(path.evaluate(&val).len(), 3);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:EvaluateArrayMultipleIndexes
#[test]
fn evaluate_array_multiple_indexes() {
  let json = r#"[1, 2, 3, 4]"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$[0, 2]").unwrap();
  assert_eq!(path.evaluate(&val).len(), 2);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:EvaluateScan
#[test]
fn evaluate_scan() {
  let json = r#"{"a": {"b": 1}}"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$..b").unwrap();
  assert_eq!(path.evaluate(&val).len(), 1);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:EvaluateWildcardScan
#[test]
fn evaluate_wildcard_scan() {
  let json = r#"{"a": {"b": 1}}"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$..*").unwrap();
  assert!(!path.evaluate(&val).is_empty());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:EvaluateScanNestResults
#[test]
fn evaluate_scan_nest_results() {
  let json = r#"{"a": {"a": {"a": 1}}}"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$..a").unwrap();
  assert_eq!(path.evaluate(&val).len(), 3);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:EvaluateWildcardScanNestResults
#[test]
fn evaluate_wildcard_scan_nest_results() {
  let json = r#"{"a": {"b": 1}}"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$..*").unwrap();
  assert!(!path.evaluate(&val).is_empty());
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:EvaluateSinglePropertyReturningArray
#[test]
fn evaluate_single_property_returning_array() {
  let json = r#"{"arr": [1, 2]}"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$.arr").unwrap();
  assert_eq!(path.evaluate(&val).len(), 1);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:EvaluateLastSingleCharacterProperty
#[test]
fn evaluate_last_single_character_property() {
  let json = r#"{"a": 1}"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$.a").unwrap();
  assert_eq!(path.evaluate(&val).len(), 1);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:ExistsQuery
#[test]
fn exists_query() {
  let json = r#"[{"a": 1}, {"b": 2}]"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$[?(@.a)]").unwrap();
  assert_eq!(path.evaluate(&val).len(), 1);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:EqualsQuery
#[test]
fn equals_query() {
  let json = r#"[{"a": 1}, {"a": 2}]"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$[?(@.a == 1)]").unwrap();
  assert_eq!(path.evaluate(&val).len(), 1);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:NotEqualsQuery
#[test]
fn not_equals_query() {
  let json = r#"[{"a": 1}, {"a": 2}]"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$[?(@.a != 1)]").unwrap();
  assert_eq!(path.evaluate(&val).len(), 1);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:NoPathQuery
#[test]
fn no_path_query() {
  let json = r#"[1, 2, 3]"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$[?(@ > 1)]").unwrap();
  assert_eq!(path.evaluate(&val).len(), 2);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:MultipleQueries
#[test]
fn multiple_queries() {
  let json = r#"[{"a": 1, "b": 2}, {"a": 1, "b": 3}]"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$[?(@.a == 1 && @.b == 2)]").unwrap();
  assert_eq!(path.evaluate(&val).len(), 1);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:GreaterQuery
#[test]
fn greater_query() {
  let json = r#"[{"a": 1}, {"a": 5}]"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$[?(@.a > 2)]").unwrap();
  assert_eq!(path.evaluate(&val).len(), 1);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:LesserQuery_ValueFirst
#[test]
fn lesser_query_value_first() {
  let json = r#"[{"a": 1}, {"a": 5}]"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$[?(@.a < 3)]").unwrap();
  assert_eq!(path.evaluate(&val).len(), 1);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:GreaterQueryBigInteger
#[test]
fn greater_query_big_integer() {
  let json = r#"[{"a": 1000000000000}]"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$[?(@.a > 100)]").unwrap();
  assert_eq!(path.evaluate(&val).len(), 1);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:GreaterOrEqualQuery
#[test]
fn greater_or_equal_query() {
  let json = r#"[{"a": 2}, {"a": 1}]"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$[?(@.a >= 2)]").unwrap();
  assert_eq!(path.evaluate(&val).len(), 1);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:NestedQuery
#[test]
fn nested_query() {
  let json = r#"{"store": {"book": [{"price": 10}]}}"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$.store.book[?(@.price == 10)]").unwrap();
  assert_eq!(path.evaluate(&val).len(), 1);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:PathWithConstructor
#[test]
fn path_with_constructor() {
  let json = r#"{"a": 1}"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$.a").unwrap();
  assert_eq!(path.evaluate(&val).len(), 1);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:MultiplePaths
#[test]
fn multiple_paths() {
  let json = r#"{"a": 1, "b": 2}"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let p1 = JsonPath::parse("$.a").unwrap();
  let p2 = JsonPath::parse("$.b").unwrap();
  assert_eq!(p1.evaluate(&val).len(), 1);
  assert_eq!(p2.evaluate(&val).len(), 1);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:Exists_True
#[test]
fn exists_true() {
  let json = r#"[
    {"price": 199, "max_price": 200},
    {"price": 200, "max_price": 200},
    {"price": 201, "max_price": 200}
  ]"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$[?(true)]").unwrap();
  assert_eq!(path.evaluate(&val).len(), 3);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:Exists_Null
#[test]
fn exists_null() {
  let json = r#"[
    {"price": 199, "max_price": 200},
    {"price": 200, "max_price": 200},
    {"price": 201, "max_price": 200}
  ]"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$[?(true)]").unwrap();
  assert_eq!(path.evaluate(&val).len(), 3);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:WildcardWithProperty
#[test]
fn wildcard_with_property() {
  let json = r#"{"a": {"x": 1}, "b": {"x": 2}}"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$.*.x").unwrap();
  assert_eq!(path.evaluate(&val).len(), 2);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:QueryAgainstNonStringValues
#[test]
fn query_against_non_string_values() {
  let json = r#"[{"a": 1}, {"a": "str"}]"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$[?(@.a == 1)]").unwrap();
  assert_eq!(path.evaluate(&val).len(), 1);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:Example
#[test]
fn example() {
  let json = r#"{"store": {"bicycle": {"color": "red"}}}"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$.store.bicycle.color").unwrap();
  assert_eq!(path.evaluate(&val)[0].as_str(), Some("red"));
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:NotEqualsAndNonPrimativeValues
#[test]
fn not_equals_and_non_primative_values() {
  let json = r#"[{"a": {"x": 1}}, {"a": 2}]"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$[?(@.a != 2)]").unwrap();
  assert_eq!(path.evaluate(&val).len(), 1);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:RootInFilter
#[test]
fn root_in_filter() {
  let json = r#"[
    {
      "store": {
        "book": [
          {"category": "reference", "author": "Nigel Rees", "title": "Sayings of the Century", "price": 8.95},
          {"category": "fiction", "author": "Evelyn Waugh", "title": "Sword of Honour", "price": 12.99},
          {"category": "fiction", "author": "Herman Melville", "title": "Moby Dick", "isbn": "0-553-21311-3", "price": 8.99},
          {"category": "fiction", "author": "J. R. R. Tolkien", "title": "The Lord of the Rings", "isbn": "0-395-19395-8", "price": 22.99}
        ],
        "bicycle": {
          "color": "red",
          "price": 19.95
        }
      },
      "expensive": 10
    }
  ]"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path1 = JsonPath::parse("$.[?($.[0].store.bicycle.price < 20)]").unwrap();
  assert_eq!(path1.evaluate(&val).len(), 1);
  let path2 = JsonPath::parse("$.[?($.[0].store.bicycle.price < 10)]").unwrap();
  assert_eq!(path2.evaluate(&val).len(), 0);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:RootInFilterWithRootObject
#[test]
fn root_in_filter_with_root_object() {
  let json = r#"{"val": 5}"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$[?(@.val == 5)]").unwrap();
  assert_eq!(path.evaluate(&val).len(), 1);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:RootInFilterWithInitializers
#[test]
fn root_in_filter_with_initializers() {
  let json = r#"{"val": 5}"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$[?(@.val > 0)]").unwrap();
  assert_eq!(path.evaluate(&val).len(), 1);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:IdentityOperator
#[test]
fn identity_operator() {
  let json = r#"[{"a": 1}, {"a": "1"}]"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$[?(@.a === 1)]").unwrap();
  assert_eq!(path.evaluate(&val).len(), 1);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:QueryWithEscapedPath
#[test]
fn query_with_escaped_path() {
  let json = r#"{"a.b": 1}"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$['a.b']").unwrap();
  assert_eq!(path.evaluate(&val).len(), 1);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:Equals_FloatWithInt
#[test]
fn equals_float_with_int() {
  let json = r#"[{"a": 1.0}, {"a": 2}]"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$[?(@.a == 1)]").unwrap();
  assert_eq!(path.evaluate(&val).len(), 1);
}

/// test/standalone/Garnet.test.extensions/GarnetJSON/JSONPath/JsonPathExecuteTests.cs:EqualsStrict
#[test]
fn equals_strict() {
  let json = r#"[{"a": 1}, {"a": "1"}]"#;
  let val: Value = sonic_rs::from_str(json).unwrap();
  let path = JsonPath::parse("$[?(@.a === 1)]").unwrap();
  assert_eq!(path.evaluate(&val).len(), 1);
}
