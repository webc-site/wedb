#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
use wnode_test::complete_len;

/// RESP3 单帧型全帧面（票 r21 发现一回归：HELLO 3 会话 read_reply 不再挂起）
#[test]
fn complete_len_resp3_frames() {
  // 行式：_ nil / # bool / , double / ( bignum —— 头行到齐即完整
  assert_eq!(complete_len(b"_\r\n"), Some(3));
  assert_eq!(complete_len(b"#t\r\n"), Some(4));
  assert_eq!(complete_len(b",3.14\r\n"), Some(7));
  assert_eq!(complete_len(b"(12345678901234567890\r\n"), Some(23));
  assert_eq!(complete_len(b",3.14\r"), None);
  // 定长：! blob error / = verbatim —— header 定长 + CRLF
  assert_eq!(complete_len(b"!21\r\nSYNTAX invalid syntax\r\n"), Some(28));
  assert_eq!(complete_len(b"=15\r\ntxt:Some string\r\n"), Some(22));
  assert_eq!(complete_len(b"=15\r\ntxt:Some"), None);
  // 聚合：~ set / > push —— 子帧递归（push 子帧内嵌数组）
  assert_eq!(complete_len(b"~2\r\n+a\r\n+b\r\n"), Some(12));
  assert_eq!(
    complete_len(b">2\r\n*2\r\n$7\r\nmessage\r\n$2\r\nch\r\n$3\r\nfoo\r\n"),
    Some(38)
  );
  // map 对数翻倍（RESP3 HELLO 应答形态）
  assert_eq!(complete_len(b"%1\r\n+k\r\n:v\r\n"), Some(12));
  // 未知首字节仍 None
  assert_eq!(complete_len(b"@x\r\n"), None);
}
