use wresp::{
  key_spec::{
    BeginSearchMethod, FindKeysMethod, KeySpecificationFlags, RespCommandKeySpecification,
  },
  resp_memory_writer::{Resp2, RespWriter},
};

#[test]
fn flags_descriptions_roundtrip() {
  let flags = KeySpecificationFlags::from_wire_names("RW, Insert").unwrap();
  assert_eq!(flags.descriptions(), vec!["RW", "insert"]);
  assert!(KeySpecificationFlags::from_wire_names("BOGUS").is_none());
  assert!(
    KeySpecificationFlags::from_wire_names("None")
      .unwrap()
      .is_empty()
  );
  assert_eq!(
    KeySpecificationFlags::from_wire_name_single("not_key"),
    Some(KeySpecificationFlags::NOT_KEY)
  );
  assert_eq!(
    KeySpecificationFlags::from_wire_name_single("NOTKEY"),
    Some(KeySpecificationFlags::NOT_KEY)
  );
  assert_eq!(
    KeySpecificationFlags::from_wire_name_single("variable_flags"),
    Some(KeySpecificationFlags::VARIABLE_FLAGS)
  );
  assert_eq!(
    KeySpecificationFlags::from_wire_name_single("VARIABLEFLAGS"),
    Some(KeySpecificationFlags::VARIABLE_FLAGS)
  );
}

#[test]
fn to_resp_format_index_range() {
  let ks = RespCommandKeySpecification {
    begin_search: Some(BeginSearchMethod::Index(1)),
    find_keys: Some(FindKeysMethod::Range {
      last_key: 0,
      key_step: 1,
      limit: 0,
    }),
    notes: None,
    flags: KeySpecificationFlags::from_wire_names("RW, Insert").unwrap(),
  };
  let mut w = RespWriter::<Vec<u8>, Resp2>::new();
  ks.to_resp_format(&mut w);
  // RESP2：map 降级倍长数组（flags + begin_search + find_keys = 3 键 → *6）
  let text = String::from_utf8(w.into_inner()).unwrap();
  assert!(text.starts_with("*6\r\n"), "map 头：{text}");
  assert!(
    text.contains("$5\r\nflags\r\n*2\r\n+RW\r\n+insert\r\n"),
    "{text}"
  );
  assert!(text.contains("$12\r\nbegin_search\r\n"), "{text}");
  assert!(text.contains("$9\r\nfind_keys\r\n"), "{text}");
}
