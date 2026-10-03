//! RESP 协议解析端到端集成测试（对标 libs/server/Resp/Parser/RespCommandParser.cs）

use wnode::resp::{
  RespServerSession,
  parser::command_table::{
    ACL_SUBTABLE, BITOP_SUBTABLE, CLIENT_SUBTABLE, CLUSTER_SUBTABLE, COMMAND_SUBTABLE,
    CONFIG_SUBTABLE, LATENCY_SUBTABLE, MEMORY_SUBTABLE, OBJECT_SUBTABLE, PRIMARY_TABLE,
    PUBSUB_SUBTABLE, SCRIPT_SUBTABLE, SLOWLOG_SUBTABLE, lookup_primary,
  },
};
use wresp::command::RespCommand;

/// 测试用父命令-子表三元组(父命令名, 父命令, 子命令表)
type ParentSubtable = (
  &'static str,
  RespCommand,
  &'static [(&'static str, RespCommand)],
);

fn parse_one(
  session: &mut RespServerSession,
  buffer: &[u8],
) -> (Option<RespCommand>, Vec<Vec<u8>>) {
  session.recv_buffer.clear();
  session.recv_buffer.extend_from_slice(buffer);
  session.bytes_read = session.recv_buffer.len();
  session.read_head = 0;
  let cmd = session.parse_command();
  let args = (0..session.parse_state.count)
    .map(|i| session.parse_state.arg_in(&session.recv_buffer, i).to_vec())
    .collect();
  (cmd, args)
}

/// 主表 + 子命令表全量端到端 round-trip:每一条表项都构造真实 RESP 帧
/// 解析,命中期望命令(哈希索引可达性 + 名称/枚举映射的全量回归,不止抽样)
#[test]
fn every_table_entry_round_trips_via_parse() {
  let mut s = RespServerSession::default();

  // 主表:非父命令 `*2 NAME k` → 命令本身(1 个参数)
  for (name, cmd, has_subcommands) in PRIMARY_TABLE {
    if *has_subcommands {
      continue;
    }
    let frame = format!("*2\r\n${}\r\n{name}\r\n$1\r\nk\r\n", name.len());
    let (parsed, _) = parse_one(&mut s, frame.as_bytes());
    assert_eq!(parsed, Some(*cmd), "主表 {name} 解析不可达");
  }

  // 父命令 + 子命令:`*2 PARENT SUB` → 子命令
  let subtables: [ParentSubtable; 12] = [
    ("CLIENT", RespCommand::Client, CLIENT_SUBTABLE),
    ("CONFIG", RespCommand::Config, CONFIG_SUBTABLE),
    ("COMMAND", RespCommand::Command, COMMAND_SUBTABLE),
    ("ACL", RespCommand::Acl, ACL_SUBTABLE),
    ("SCRIPT", RespCommand::Script, SCRIPT_SUBTABLE),
    ("PUBSUB", RespCommand::Pubsub, PUBSUB_SUBTABLE),
    ("LATENCY", RespCommand::Latency, LATENCY_SUBTABLE),
    ("SLOWLOG", RespCommand::Slowlog, SLOWLOG_SUBTABLE),
    ("MEMORY", RespCommand::Memory, MEMORY_SUBTABLE),
    ("OBJECT", RespCommand::Object, OBJECT_SUBTABLE),
    ("CLUSTER", RespCommand::Cluster, CLUSTER_SUBTABLE),
    ("BITOP", RespCommand::Bitop, BITOP_SUBTABLE),
  ];
  for (parent_name, parent_cmd, table) in subtables {
    // 父命令项须在主表且带子命令标记
    let entry = lookup_primary(parent_name.as_bytes());
    assert_eq!(
      entry,
      Some((parent_cmd, true)),
      "主表缺父命令 {parent_name}"
    );
    for (sub_name, sub_cmd) in table {
      let frame = format!(
        "*2\r\n${}\r\n{parent_name}\r\n${}\r\n{sub_name}\r\n",
        parent_name.len(),
        sub_name.len()
      );
      let (parsed, _) = parse_one(&mut s, frame.as_bytes());
      assert_eq!(
        parsed,
        Some(*sub_cmd),
        "{parent_name} {sub_name} 解析不可达"
      );
    }
  }
}
