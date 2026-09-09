use crossfire::{
  MAsyncTx,
  mpsc,
  oneshot::{RxOneshot, TxOneshot},
};

use crate::{Error, Result};

/// 单连接在途命令通道容量上限（网络泵批量编码与 FIFO 派发的窗口大小）
pub(crate) const CHANNEL_CAP: usize = 1024;

/// 命令应答回传端口：按期望应答类型区分（标量 / 字符串数组），与网络泵的
/// 应答解析器一一对应
pub(crate) enum ReplyTx {
  Str(TxOneshot<Result<String>>),
  Array(TxOneshot<Result<Vec<String>>>),
}

/// 在途命令：RESP 请求帧参数 + 应答回传端口
pub(crate) struct CommandItem {
  pub cmd: Vec<String>,
  pub resp_tx: ReplyTx,
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
