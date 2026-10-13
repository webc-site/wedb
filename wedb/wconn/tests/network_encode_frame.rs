#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! 命令帧编码测试（归位自 wconn::network 内联测试，仅依赖 pub API）

use wconn::network::encode_command;

#[test]
fn encode_command_frame() {
  let mut out = Vec::new();
  encode_command(&mut out, &[b"GET".as_slice(), b"k1".as_slice()]);
  assert_eq!(&out, b"*2\r\n$3\r\nGET\r\n$2\r\nk1\r\n");

  let mut out_str = Vec::new();
  encode_command(&mut out_str, &["GET", "k1"]);
  assert_eq!(&out_str, b"*2\r\n$3\r\nGET\r\n$2\r\nk1\r\n");
}
