use wresp::{
  argument::{
    ArgumentBase, RespCommandArgument, RespCommandArgumentFlags, RespCommandArgumentType,
  },
  resp_memory_writer::{Resp3, RespWriter},
};

#[test]
fn type_descriptions_and_parse() {
  assert_eq!(RespCommandArgumentType::Key.description(), "key");
  assert_eq!(RespCommandArgumentType::UnixTime.description(), "unix-time");
  assert_eq!(
    RespCommandArgumentType::from_member_name("oneof"),
    Some(RespCommandArgumentType::OneOf)
  );
  assert_eq!(RespCommandArgumentType::from_member_name("bad"), None);
}

#[test]
fn flags_descriptions_and_parse() {
  let flags = RespCommandArgumentFlags::OPTIONAL.union(RespCommandArgumentFlags::MULTIPLE_TOKEN);
  assert_eq!(flags.descriptions(), vec!["optional", "multiple-token"]);
  assert_eq!(
    RespCommandArgumentFlags::from_member_name("MultipleToken"),
    Some(RespCommandArgumentFlags::MULTIPLE_TOKEN)
  );
}

#[test]
fn key_argument_resp_format() {
  let arg = RespCommandArgument::Key {
    base: ArgumentBase {
      name: "key".to_string(),
      display_text: None,
      argument_type: RespCommandArgumentType::Key,
      token: None,
      summary: None,
      argument_flags: RespCommandArgumentFlags::NONE,
    },
    value: Some("key".to_string()),
    key_spec_index: 0,
  };
  let mut w = RespWriter::<Vec<u8>, Resp3>::new();
  arg.to_resp_format(&mut w);
  let text = String::from_utf8(w.into_inner()).unwrap();
  // C# Key 参数 RESP 面只出 name/type/key_spec_index 三键（RESP3 %3）
  assert!(text.starts_with("%3\r\n"), "{text}");
  assert!(text.contains("$14\r\nkey_spec_index\r\n:0\r\n"), "{text}");
}
