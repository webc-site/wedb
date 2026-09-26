//! 命令帧编码：RESP2 数组帧单点编码（数组头与 bulk 元素组合 wresp 写原语单点）
//!
//! 在 garnet 中的相对路径:
//! - `libs/client/ClientTcpNetworkSender.cs:SendResponse`
//! - `libs/client/ClientSession/GarnetClientSession.cs:InternalExecute`

use wresp::ext::RespVecExt;

/// 将一条命令编码为 RESP2 数组帧追加到 out（`*<n>\r\n` 数组头 + 逐元素
/// `$<len>\r\n<arg>\r\n`，均走 wresp 写原语，不再自推帧字节）
///
/// 在 garnet 中的相对路径:
/// - `libs/client/ClientTcpNetworkSender.cs:SendResponse`
/// - `libs/client/ClientSession/GarnetClientSession.cs:InternalExecute`
pub fn encode_command<T: AsRef<[u8]>>(out: &mut Vec<u8>, cmd: &[T]) {
  let est = cmd.iter().map(|s| s.as_ref().len() + 16).sum::<usize>() + 16;
  out.reserve(est);
  out.write_resp_array_len(cmd.len());
  for arg in cmd {
    out.write_resp_bulk_string(arg.as_ref());
  }
}
