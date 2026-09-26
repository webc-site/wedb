//! 客户端网络全双工读写双泵：命令帧编码 + 发送/接收解耦循环
//!
//! 在 garnet 中的相对路径:
//! - `libs/client/GarnetClient.cs`
//! - `libs/client/ClientSession/GarnetClientSession.cs`
//! - `libs/client/ClientTcpNetworkSender.cs`
//! - `libs/client/GarnetClientProcessReplies.cs`
//!
//! [`GarnetClient`](crate::GarnetClient) 与
//! [`GarnetClientSession`](crate::GarnetClientSession) 共用同一实现。
//! 子模块按 garnet libs/client 分文件拓扑拆分：
//! - [`encode`]：命令帧编码（ClientTcpNetworkSender.cs:Send /
//!   GarnetClientSession.cs:FormatRESP）；
//! - [`replies`]：帧解析与队列派发（GarnetClientProcessReplies.cs）；
//! - [`pump`]：读写双泵循环（ClientTcpNetworkSender.cs 发送流 +
//!   GarnetClientProcessReplies.cs 读循环）。
//!
//! 对标 libs/client 发送流（GarnetClientTcpNetworkSender 刷 socket）与接收流
//! （libs/client/GarnetClientProcessReplies.cs:ProcessReplies 认领应答）的
//! 解耦模型：[`TcpStream::into_split`] 拆分读写两半，写泵与读泵并发运行
//! （compio 单线程运行时内双任务，io_uring 并发 SQE），发送持续可推进，
//! 不被在途应答的认领进度串行阻滞；读泵独立读应答并按帧序认领回传
//! oneshot（C# TaskCompletionSource 的零锁等价物）。
//!
//! 退出闭环：读泵 EOF/协议错误退出 → 复位存活标志 → 写泵在限时收命令的
//! 超时窗内感知并退出、丢弃 rx，发送端经 `is_connected` 观测断连；调用方
//! 全部退场 → 写泵命令通道断连、shutdown 写半 → 读泵 EOF 收场，fd 两半
//! 全 drop 关闭连接（此路径属计划内关闭，不上抛错误）。

mod encode;
mod pump;
mod replies;
pub(crate) mod stream;

use crossfire::oneshot;
pub use encode::encode_command;
pub use pump::{READ_CHUNK, RECV_IDLE_PROBE, read_pump, write_pump};
pub(crate) use pump::{network_loop, resolve_network_pool};
pub use stream::OutStream;

use crate::{
  Result,
  types::{ChannelTx, CommandItem, ReplyTx, roundtrip},
};

/// 执行单条字符串命令往返
///
/// 在 garnet 中的相对路径: libs/client/GarnetClient.cs:ExecuteForStringResultAsync
async fn exec(tx: &ChannelTx, command: &[&str]) -> Result<String> {
  let (resp_tx, resp_rx) = oneshot::oneshot();
  let item = CommandItem::new(command, ReplyTx::Str(resp_tx));
  roundtrip(tx, item, resp_rx, None).await
}

/// 连接后握手序列：AUTH（用户名优先，缺省密码按空串补齐）+ CLIENT SETINFO/SETNAME
///
/// （C# ConnectAsync 内的 AUTH/CLIENT SETINFO 握手子步骤，客户端与会话共用同一口径，SETINFO/SETNAME 同以 clientName 非空为前提）
pub(super) async fn handshake(
  tx: &ChannelTx,
  lib_name: &str,
  auth_username: Option<&str>,
  auth_password: Option<&str>,
  client_name: Option<&str>,
) -> Result<()> {
  if auth_username.is_some() || auth_password.is_some() {
    let user = auth_username.unwrap_or("default");
    let pass = auth_password.unwrap_or("");
    exec(tx, &["AUTH", user, pass]).await?;
  }
  if let Some(client_name) = client_name {
    exec(tx, &["CLIENT", "SETINFO", "LIB-NAME", lib_name]).await?;
    exec(tx, &["CLIENT", "SETNAME", client_name]).await?;
  }
  Ok(())
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn encode_command_frame() {
    let mut out = Vec::new();
    encode_command(&mut out, &[b"GET".as_slice(), b"k1".as_slice()]);
    assert_eq!(&out, b"*2\r\n$3\r\nGET\r\n$2\r\nk1\r\n");

    let mut out_str = Vec::new();
    encode_command(&mut out_str, &["GET", "k1"]);
    assert_eq!(&out_str, b"*2\r\n$3\r\nGET\r\n$2\r\nk1\r\n");
  }
}
