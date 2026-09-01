use crate::parse::util::*;
use crate::types::Cmd;
use wedb_embed::Result;

pub fn parse(cmd_name: &str, args: &[&[u8]]) -> Result<Option<Cmd>> {
  let res: Result<Cmd> = match cmd_name {
    // @cmd: PUBLISH channel message / SPUBLISH shardchannel message
    "publish" | "spublish" => {
      check_min_args(cmd_name, args, 3)?;
      Ok(Cmd::Publish(arg_string(args[1]), args[2].to_vec()))
    }
    // @cmd: MPUBLISH channel message [channel message ...]
    "mpublish" => {
      check_min_args(cmd_name, args, 3)?;
      let mut list = Vec::with_capacity((args.len() - 1) / 2);
      for chunk in args[1..].as_chunks::<2>().0 {
        list.push((arg_string(chunk[0]), chunk[1].to_vec()));
      }
      Ok(Cmd::MPublish(list))
    }
    // @cmd: SUBSCRIBE channel [channel ...]
    "subscribe" => {
      check_min_args(cmd_name, args, 2)?;
      let channels = args[1..].iter().map(|a| arg_string(a)).collect();
      Ok(Cmd::Subscribe(channels))
    }
    // @cmd: UNSUBSCRIBE [channel [channel ...]]
    "unsubscribe" => {
      let channels = args[1..].iter().map(|a| arg_string(a)).collect();
      Ok(Cmd::Unsubscribe(channels))
    }
    // @cmd: PSUBSCRIBE pattern [pattern ...]
    "psubscribe" => {
      check_min_args(cmd_name, args, 2)?;
      let pats = args[1..].iter().map(|a| arg_string(a)).collect();
      Ok(Cmd::PSubscribe(pats))
    }
    // @cmd: PUNSUBSCRIBE [pattern [pattern ...]]
    "punsubscribe" => {
      let pats = args[1..].iter().map(|a| arg_string(a)).collect();
      Ok(Cmd::PUnsubscribe(pats))
    }
    // @cmd: SSUBSCRIBE shardchannel [shardchannel ...]
    "ssubscribe" => {
      check_min_args(cmd_name, args, 2)?;
      let channels = args[1..].iter().map(|a| arg_string(a)).collect();
      Ok(Cmd::SSubscribe(channels))
    }
    // @cmd: SUNSUBSCRIBE [shardchannel [shardchannel ...]]
    "sunsubscribe" => {
      let channels = args[1..].iter().map(|a| arg_string(a)).collect();
      Ok(Cmd::SUnsubscribe(channels))
    }
    // @cmd: PUBSUB <CHANNELS | NUMSUB | NUMPAT | SHARDCHANNELS | SHARDNUMSUB> [argument ...]
    "pubsub" => {
      let list = args[1..].iter().map(|a| arg_string(a)).collect();
      Ok(Cmd::PubSub(list))
    }

    _ => return Ok(None),
  };
  res.map(Some)
}
