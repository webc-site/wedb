use crossfire::{
  MAsyncTx, mpsc,
  oneshot::{RxOneshot, TxOneshot},
};

use crate::{
  Error, Result,
  network::{encode_bytes_command, encode_str_command},
};

/// 单连接在途命令通道容量上限（网络泵批量编码与 FIFO 派发的窗口大小）
pub(crate) const CHANNEL_CAP: usize = 1024;

/// 命令应答回传端口：按期望应答类型区分（标量 / 字符串数组），与网络泵的
/// 应答解析器一一对应；None 为发出即忘（协议约定无应答的命令）
pub(crate) enum ReplyTx {
  Str(TxOneshot<Result<String>>),
  Bytes(TxOneshot<Result<Vec<u8>>>),
  Array(TxOneshot<Result<Vec<String>>>),
  /// 发出即忘：不注册应答等待，写出即完成
  ///
  /// 对标 C# GarnetClientSession.ExecuteClusterAppendLog 的无 tcs 入队路径
  ///（集群 AOF 逐记录转发不注册 TaskCompletionSource，也不递增 numCommands，
  /// replica 侧对 APPENDLOG 记录帧不回写应答）
  None,
}

/// 在途命令：RESP 请求帧 + 应答回传端口
pub(crate) struct CommandItem {
  pub frame: Vec<u8>,
  pub resp_tx: ReplyTx,
}

impl CommandItem {
  #[inline]
  pub fn new_str(cmd: &[&str], resp_tx: ReplyTx) -> Self {
    let mut frame = Vec::new();
    encode_str_command(&mut frame, cmd);
    Self { frame, resp_tx }
  }

  #[inline]
  pub fn new_bytes(cmd: &[&[u8]], resp_tx: ReplyTx) -> Self {
    let mut frame = Vec::new();
    encode_bytes_command(&mut frame, cmd);
    Self { frame, resp_tx }
  }
}

/// 命令通道发送端类型别名（客户端与会话共用）
pub(crate) type ChannelTx = MAsyncTx<mpsc::Array<CommandItem>>;

/// 发送命令帧并等待应答回传：请求往返的公共路径（客户端与会话共用）
#[inline]
pub(crate) async fn roundtrip<T>(
  tx: &ChannelTx,
  item: CommandItem,
  resp_rx: RxOneshot<Result<T>>,
) -> Result<T> {
  tx.send(item)
    .await
    .map_err(|_| Error::Other("Network loop died".into()))?;
  resp_rx
    .await
    .map_err(|_| Error::Other("Response channel closed".into()))?
}
