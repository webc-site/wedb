use std::sync::Arc;
use webc_cmd::{Cmd, ConnectionContext};
use wedb_embed::{Error, Result, WeDb};
use wedb_resp::RespValue;

/// 处理所有发布订阅 (Pub/Sub) 相关命令
pub async fn handle_pubsub(
  _db: &Arc<WeDb>,
  _ctx: &mut ConnectionContext,
  cmd: Cmd,
) -> Result<RespValue> {
  match cmd {
    Cmd::Publish(_, _) => Ok(RespValue::Int(0)),
    Cmd::MPublish(pairs) => Ok(RespValue::Arr(vec![RespValue::Int(0); pairs.len()])),
    Cmd::Subscribe(channels) => {
      let mut arr = Vec::with_capacity(channels.len());
      for (idx, ch) in channels.into_iter().enumerate() {
        arr.push(RespValue::Arr(vec![
          RespValue::Blob(b"subscribe".to_vec()),
          RespValue::Blob(ch.into_bytes()),
          RespValue::Int((idx + 1) as i64),
        ]));
      }
      Ok(RespValue::Arr(arr))
    }
    Cmd::Unsubscribe(channels) => {
      let mut arr = Vec::with_capacity(channels.len());
      for (idx, ch) in channels.into_iter().enumerate() {
        arr.push(RespValue::Arr(vec![
          RespValue::Blob(b"unsubscribe".to_vec()),
          RespValue::Blob(ch.into_bytes()),
          RespValue::Int(idx as i64),
        ]));
      }
      Ok(RespValue::Arr(arr))
    }
    Cmd::PSubscribe(patterns) => {
      let mut arr = Vec::with_capacity(patterns.len());
      for (idx, pat) in patterns.into_iter().enumerate() {
        arr.push(RespValue::Arr(vec![
          RespValue::Blob(b"psubscribe".to_vec()),
          RespValue::Blob(pat.into_bytes()),
          RespValue::Int((idx + 1) as i64),
        ]));
      }
      Ok(RespValue::Arr(arr))
    }
    Cmd::PUnsubscribe(patterns) => {
      let mut arr = Vec::with_capacity(patterns.len());
      for (idx, pat) in patterns.into_iter().enumerate() {
        arr.push(RespValue::Arr(vec![
          RespValue::Blob(b"punsubscribe".to_vec()),
          RespValue::Blob(pat.into_bytes()),
          RespValue::Int(idx as i64),
        ]));
      }
      Ok(RespValue::Arr(arr))
    }
    Cmd::SSubscribe(channels) => {
      let mut arr = Vec::with_capacity(channels.len());
      for (idx, ch) in channels.into_iter().enumerate() {
        arr.push(RespValue::Arr(vec![
          RespValue::Blob(b"ssubscribe".to_vec()),
          RespValue::Blob(ch.into_bytes()),
          RespValue::Int((idx + 1) as i64),
        ]));
      }
      Ok(RespValue::Arr(arr))
    }
    Cmd::SUnsubscribe(channels) => {
      let mut arr = Vec::with_capacity(channels.len());
      for (idx, ch) in channels.into_iter().enumerate() {
        arr.push(RespValue::Arr(vec![
          RespValue::Blob(b"sunsubscribe".to_vec()),
          RespValue::Blob(ch.into_bytes()),
          RespValue::Int(idx as i64),
        ]));
      }
      Ok(RespValue::Arr(arr))
    }
    Cmd::PubSub(_) => Ok(RespValue::Arr(Vec::new())),
    _ => Err(Error::internal("unsupported pubsub command")),
  }
}
