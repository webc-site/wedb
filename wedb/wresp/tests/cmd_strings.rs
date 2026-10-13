#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
use wresp::{
  cmd_strings::{
    PUBSUB_PSUBSCRIBE_FRAME_PREFIX, PUBSUB_PUNSUBSCRIBE_FRAME_PREFIX, PUBSUB_PUSH_MSG_PREFIX_RESP2,
    PUBSUB_PUSH_MSG_PREFIX_RESP3, PUBSUB_PUSH_PMSG_PREFIX_RESP2, PUBSUB_PUSH_PMSG_PREFIX_RESP3,
    PUBSUB_PUSH_SMSG_PREFIX_RESP2, PUBSUB_PUSH_SMSG_PREFIX_RESP3, PUBSUB_SSUBSCRIBE_FRAME_PREFIX,
    PUBSUB_SUBSCRIBE_FRAME_PREFIX, PUBSUB_SUNSUBSCRIBE_FRAME_PREFIX,
    PUBSUB_UNSUBSCRIBE_FRAME_PREFIX, RESP_ERR_GENERIC_NOSUCHKEY, RESP_ERR_NOPERM, RESP_OK,
    RESP_RETURN_VAL_N1, abort_with_pubsub_command_disabled, abort_with_syntax_error_option,
    abort_with_unknown_subcommand, abort_with_unknown_subcommand_or_wrong_num_args,
    abort_with_unsupported_option, abort_with_wrong_number_of_arguments, cluster, write_error_raw,
    write_map_len, write_map_len_resp2, write_raw,
  },
  ext::RespVecExt,
};

#[test]
fn write_error_raw_frames_message() {
  let mut out = Vec::new();
  write_error_raw(&mut out, RESP_ERR_GENERIC_NOSUCHKEY);
  assert_eq!(out, b"-ERR no such key\r\n");
}

#[test]
fn write_error_raw_noperm() {
  let mut out = Vec::new();
  write_error_raw(&mut out, RESP_ERR_NOPERM);
  assert_eq!(
    out,
    b"-NOPERM this user has no permissions to run the command\r\n"
  );
}

#[test]
fn write_error_raw_sanitizes_crlf() {
  let mut out = Vec::new();
  write_error_raw(&mut out, "ERR error with \r\ninjection");
  assert_eq!(out, b"-ERR error with \r\n");
}

#[test]
fn wrong_num_args_formats_command_name() {
  let mut out = Vec::new();
  abort_with_wrong_number_of_arguments(&mut out, "GETEX");
  assert_eq!(
    out,
    b"-ERR wrong number of arguments for 'GETEX' command\r\n"
  );
}

#[test]
fn framed_error_helpers_output_single_primitive_bytes() {
  let mut out = Vec::new();
  abort_with_unknown_subcommand(&mut out, "FOO", "CONFIG");
  assert_eq!(out, b"-ERR unknown subcommand 'FOO'. Try CONFIG HELP\r\n");

  out.clear();
  abort_with_unknown_subcommand_or_wrong_num_args(&mut out, "FOO\r\nx", "OBJECT");
  assert_eq!(
    out,
    b"-ERR unknown subcommand or wrong number of arguments for 'FOO'. Try OBJECT HELP\r\n"
  );

  out.clear();
  abort_with_unsupported_option(&mut out, "BAZR");
  assert_eq!(out, b"-ERR Unsupported option BAZR\r\n");

  out.clear();
  abort_with_syntax_error_option(&mut out, "SET", "PX");
  assert_eq!(out, b"-ERR Syntax error in SET option 'PX'\r\n");

  out.clear();
  cluster::write_slot_duplicate_error(&mut out, 902);
  assert_eq!(out, b"-ERR Slot 902 specified multiple times\r\n");

  out.clear();
  cluster::write_slot_range_error(&mut out, 20000, 10000);
  assert_eq!(out, b"-ERR Invalid range 20000 > 10000!\r\n");

  out.clear();
  cluster::write_redirect_error(&mut out, "MOVED", 42, "127.0.0.1", 7379);
  assert_eq!(out, b"-MOVED 42 127.0.0.1:7379\r\n");
}

#[test]
fn map_len_resp2_doubles_array_length() {
  let mut out = Vec::new();
  write_map_len_resp2(&mut out, 2);
  assert_eq!(out, b"*4\r\n");
}

#[test]
fn map_len_dispatches_by_protocol_version() {
  let mut out = Vec::new();
  write_map_len(&mut out, 3, 3);
  assert_eq!(out, b"%3\r\n");

  let mut out = Vec::new();
  write_map_len(&mut out, 3, 2);
  assert_eq!(out, b"*6\r\n");
}

#[test]
fn raw_frames_passthrough() {
  let mut out = Vec::new();
  write_raw(&mut out, RESP_OK);
  write_raw(&mut out, RESP_RETURN_VAL_N1);
  assert_eq!(out, b"+OK\r\n:-1\r\n");
}

#[test]
fn pubsub_command_disabled_fills_template() {
  let mut out = Vec::new();
  abort_with_pubsub_command_disabled(&mut out, "PUBLISH");
  assert_eq!(
    out,
    b"-ERR PUBLISH is disabled, enable it with --pubsub option.\r\n"
  );

  let mut out = Vec::new();
  abort_with_pubsub_command_disabled(&mut out, "PUBSUB NUMPAT");
  assert_eq!(
    out,
    b"-ERR PUBSUB NUMPAT is disabled, enable it with --pubsub option.\r\n"
  );

  let mut out = Vec::new();
  abort_with_pubsub_command_disabled(&mut out, "SUBSCRIBE\r\nINJECT");
  assert_eq!(
    out,
    b"-ERR SUBSCRIBE is disabled, enable it with --pubsub option.\r\n"
  );
}

#[test]
fn pubsub_frame_prefixes_match_runtime_writers() {
  let ack = |arity: usize, name: &[u8]| {
    let mut out = Vec::new();
    let mut w = out.resp_writer2();
    w.write_array_length(arity);
    w.write_bulk_string(name);
    out
  };
  let push = |arity: usize, name: &[u8]| {
    // RESP3 push 头 `><len>\r\n`（与 cmd_strings 常量前缀同源字节；写出器
    // push 口随死代码收敛删除，pubsub 推送帧由常量前缀直出）
    let mut out = format!(">{arity}\r\n").into_bytes();
    let mut w = out.resp_writer3();
    w.write_bulk_string(name);
    out
  };

  assert_eq!(PUBSUB_SUBSCRIBE_FRAME_PREFIX, ack(3, b"subscribe"));
  assert_eq!(PUBSUB_SSUBSCRIBE_FRAME_PREFIX, ack(3, b"ssubscribe"));
  assert_eq!(PUBSUB_PSUBSCRIBE_FRAME_PREFIX, ack(3, b"psubscribe"));
  assert_eq!(PUBSUB_UNSUBSCRIBE_FRAME_PREFIX, ack(3, b"unsubscribe"));
  assert_eq!(PUBSUB_SUNSUBSCRIBE_FRAME_PREFIX, ack(3, b"sunsubscribe"));
  assert_eq!(PUBSUB_PUNSUBSCRIBE_FRAME_PREFIX, ack(3, b"punsubscribe"));

  assert_eq!(PUBSUB_PUSH_MSG_PREFIX_RESP2, ack(3, b"message"));
  assert_eq!(PUBSUB_PUSH_PMSG_PREFIX_RESP2, ack(4, b"pmessage"));
  assert_eq!(PUBSUB_PUSH_SMSG_PREFIX_RESP2, ack(3, b"smessage"));
  assert_eq!(PUBSUB_PUSH_MSG_PREFIX_RESP3, push(3, b"message"));
  assert_eq!(PUBSUB_PUSH_PMSG_PREFIX_RESP3, push(4, b"pmessage"));
  assert_eq!(PUBSUB_PUSH_SMSG_PREFIX_RESP3, push(3, b"smessage"));

  // RESP2 push 退化为数组帧形态（`*<len>\r\n`，同 write_array_length 口径）
  let mut out = Vec::new();
  out.resp_writer2().write_array_length(4);
  assert_eq!(out, b"*4\r\n");
}
