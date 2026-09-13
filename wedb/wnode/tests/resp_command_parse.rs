//! RESP 命令解析端到端测试（自 src/resp/parser/resp_command.rs 内嵌测试迁出）
//!
//! 对标 libs/server/Resp/Parser/RespCommand.cs 的解析分级：16 字节模式表、
//! 单数字帧标量快路径、MRU 缓存、慢路径查表与子命令分派、协议违规哨兵、
//! AOF 提交模式与内联命令跳过。私有命令表（PRIMARY_TABLE / 子表）的不变式
//! 单元测试仍留在 src。

use wnode::resp::parser::resp_command::{is_aof_independent, is_allowed_in_subscription_mode};
use wnode::resp::resp_server_session::RespServerSession;
use wresp::RespCommand;
use wtxn::TxnState;

#[ctor::ctor(unsafe)]
fn _log_init() {
  log_init::init();
}

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
    .map(|i| {
      session
        .parse_state
        .get_arg_slice_by_ref(i)
        .as_slice()
        .to_vec()
    })
    .collect();
  (cmd, args)
}

/// 热命令 16 字节模式表 + 内联 + 小写慢路径
#[test]
fn fast_paths_parse_hot_commands() {
  let mut s = RespServerSession::default();
  // 恰 16 字节的帧首（*2 GET + 键头）命中模式表
  let (cmd, args) = parse_one(&mut s, b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
  assert_eq!(cmd, Some(RespCommand::Get));
  assert_eq!(args, vec![b"foo".to_vec()]);

  let (cmd, args) = parse_one(&mut s, b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$2\r\nv1\r\n");
  assert_eq!(cmd, Some(RespCommand::Set));
  assert_eq!(args, vec![b"k".to_vec(), b"v1".to_vec()]);

  // *1 PING = 无参（数组元素仅命令名）
  let (cmd, args) = parse_one(&mut s, b"*1\r\n$4\r\nPING\r\n");
  assert_eq!(cmd, Some(RespCommand::Ping));
  assert!(args.is_empty());

  // *2 PING msg → 慢路径解析（模式表不命中）
  let (cmd, args) = parse_one(&mut s, b"*2\r\n$4\r\nPING\r\n$3\r\nhey\r\n");
  assert_eq!(cmd, Some(RespCommand::Ping));
  assert_eq!(args, vec![b"hey".to_vec()]);

  // 小写命令走慢路径大写化
  let (cmd, args) = parse_one(&mut s, b"*2\r\n$3\r\nget\r\n$3\r\nfoo\r\n");
  assert_eq!(cmd, Some(RespCommand::Get));
  assert_eq!(args, vec![b"foo".to_vec()]);

  // 内联 PING
  let (cmd, _) = parse_one(&mut s, b"PING\r\n");
  assert_eq!(cmd, Some(RespCommand::Ping));

  // EXISTS 16 字节全等模式
  let (cmd, args) = parse_one(&mut s, b"*2\r\n$6\r\nEXISTS\r\n$1\r\nk\r\n");
  assert_eq!(cmd, Some(RespCommand::Exists));
  assert_eq!(args, vec![b"k".to_vec()]);
}

/// 单数字帧标量快路径：变参二级表（PUBLISH/SPUBLISH/SETRANGE/GETRANGE/
/// SET NX/GETEX/EXPIRE/PEXPIRE）
#[test]
fn scalar_fast_path_varargs_table() {
  let mut s = RespServerSession::default();
  // PUBLISH（长度 7，count 2）：二级表
  let (cmd, args) = parse_one(&mut s, b"*3\r\n$7\r\nPUBLISH\r\n$3\r\nfoo\r\n$1\r\nb\r\n");
  assert_eq!(cmd, Some(RespCommand::Publish));
  assert_eq!(args, vec![b"foo".to_vec(), b"b".to_vec()]);

  // SPUBLISH（长度 8，count 2，名首 SP）
  let (cmd, _) = parse_one(&mut s, b"*3\r\n$8\r\nSPUBLISH\r\n$1\r\na\r\n$1\r\nb\r\n");
  assert_eq!(cmd, Some(RespCommand::Spublish));

  // SETRANGE / GETRANGE（长度 8，count 3，名首 SE/GE）
  let (cmd, _) = parse_one(
    &mut s,
    b"*4\r\n$8\r\nSETRANGE\r\n$1\r\nk\r\n$1\r\n0\r\n$1\r\nv\r\n",
  );
  assert_eq!(cmd, Some(RespCommand::Setrange));
  let (cmd, _) = parse_one(
    &mut s,
    b"*4\r\n$8\r\nGETRANGE\r\n$1\r\nk\r\n$1\r\n0\r\n$1\r\nv\r\n",
  );
  assert_eq!(cmd, Some(RespCommand::Getrange));

  // 带选项的 SET（count 3..7，名 SET）→ SETEXNX（C# 嵌套标量表）
  let (cmd, args) = parse_one(&mut s, b"*4\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$2\r\nNX\r\n");
  assert_eq!(cmd, Some(RespCommand::Setexnx));
  assert_eq!(args, vec![b"k".to_vec(), b"v".to_vec(), b"NX".to_vec()]);

  // GETEX key PERSIST（长度 5，count 2）
  let (cmd, args) = parse_one(&mut s, b"*3\r\n$5\r\nGETEX\r\n$1\r\nk\r\n$7\r\nPERSIST\r\n");
  assert_eq!(cmd, Some(RespCommand::Getex));
  assert_eq!(args, vec![b"k".to_vec(), b"PERSIST".to_vec()]);

  // EXPIRE key 100（长度 6，count 2）/ PEXPIRE（长度 7，名首 P）
  let (cmd, _) = parse_one(&mut s, b"*3\r\n$6\r\nEXPIRE\r\n$1\r\nk\r\n$3\r\n100\r\n");
  assert_eq!(cmd, Some(RespCommand::Expire));
  let (cmd, _) = parse_one(&mut s, b"*3\r\n$7\r\nPEXPIRE\r\n$1\r\nk\r\n$3\r\n100\r\n");
  assert_eq!(cmd, Some(RespCommand::Pexpire));
}

/// MRU 双槽缓存：首查入槽，二次同帧槽命中
#[test]
fn mru_cache_captures_hash_lookup_commands() {
  let mut s = RespServerSession::default();
  // LPUSH 不在固定模式表 → 首次走慢路径（哈希查表），帧 15 字节进 MRU
  let (cmd, _) = parse_one(&mut s, b"*2\r\n$5\r\nLPUSH\r\n$1\r\nk\r\n");
  assert_eq!(cmd, Some(RespCommand::Lpush));
  // 第二次同帧 → MRU 槽 0 命中
  let (cmd, args) = parse_one(&mut s, b"*2\r\n$5\r\nLPUSH\r\n$1\r\nz\r\n");
  assert_eq!(cmd, Some(RespCommand::Lpush));
  assert_eq!(args, vec![b"z".to_vec()]);
  // HSET 帧入槽 0，LPUSH 降级槽 1；再发 LPUSH → 槽 1 命中并晋升
  let (cmd, _) = parse_one(&mut s, b"*4\r\n$4\r\nHSET\r\n$1\r\nk\r\n$1\r\nf\r\n$1\r\nv\r\n");
  assert_eq!(cmd, Some(RespCommand::Hset));
  let (cmd, _) = parse_one(&mut s, b"*2\r\n$5\r\nLPUSH\r\n$1\r\nq\r\n");
  assert_eq!(cmd, Some(RespCommand::Lpush));
}

/// 子命令分派 + 未知命令 / 子命令错误文案（逐字节对齐 C# CmdStrings）
#[test]
fn subcommand_dispatch_and_unknown() {
  let mut s = RespServerSession::default();
  let (cmd, args) = parse_one(&mut s, b"*2\r\n$6\r\nclient\r\n$2\r\nid\r\n");
  assert_eq!(cmd, Some(RespCommand::ClientId));
  assert!(args.is_empty());

  let (cmd, _) = parse_one(&mut s, b"*2\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n");
  assert_eq!(cmd, Some(RespCommand::ConfigGet));

  // 未知子命令 → Invalid + 无提示版文案（C# 文案自带句号）
  let (cmd, _) = parse_one(&mut s, b"*2\r\n$6\r\nCLIENT\r\n$4\r\nNOPE\r\n");
  assert_eq!(cmd, Some(RespCommand::Invalid));
  let out = s.take_output();
  assert_eq!(out, b"-ERR unknown subcommand 'NOPE'.\r\n");

  // CLUSTER 未知子命令 → 带帮助提示文案
  let (cmd, _) = parse_one(&mut s, b"*2\r\n$7\r\nCLUSTER\r\n$4\r\nNOPE\r\n");
  assert_eq!(cmd, Some(RespCommand::Invalid));
  let out = s.take_output();
  assert_eq!(out, b"-ERR unknown subcommand 'NOPE'. Try CLUSTER HELP\r\n");

  // 父命令无参（非 COMMAND）→ wrong number of arguments（父名大写对齐 C# 枚举名）
  let (cmd, _) = parse_one(&mut s, b"*1\r\n$6\r\nCLIENT\r\n");
  assert_eq!(cmd, Some(RespCommand::Invalid));
  let out = s.take_output();
  assert_eq!(out, b"-ERR wrong number of arguments for 'CLIENT' command\r\n");

  // 完全未知命令 → Invalid + unknown command
  let (cmd, _) = parse_one(&mut s, b"*1\r\n$4\r\nnope\r\n");
  assert_eq!(cmd, Some(RespCommand::Invalid));
  let out = s.take_output();
  assert_eq!(out, b"-ERR unknown command\r\n");
}

/// 主表二分检索回归：HELLO/HDEL 曾乱序致 HDEL 漏查；父命令与子命令表
/// 补齐 ACL/MODULE/BITOP/CLUSTER/MEMORY/OBJECT 后应全部分派成功
#[test]
fn primary_table_order_and_full_dispatch() {
  let mut s = RespServerSession::default();

  // 乱序回归：HDEL 位于 HELLO 之后曾不可达
  let (cmd, args) = parse_one(&mut s, b"*3\r\n$4\r\nHDEL\r\n$1\r\nk\r\n$1\r\nf\r\n");
  assert_eq!(cmd, Some(RespCommand::Hdel));
  assert_eq!(args, vec![b"k".to_vec(), b"f".to_vec()]);

  // 曾缺失的根命令（EXPIREAT / ZREVRANK / SUBSTR / LMOVE）
  for (frame, expect) in [
    (
      &b"*3\r\n$8\r\nEXPIREAT\r\n$1\r\nk\r\n$1\r\n1\r\n"[..],
      RespCommand::Expireat,
    ),
    (
      &b"*3\r\n$8\r\nZREVRANK\r\n$1\r\nk\r\n$1\r\nm\r\n"[..],
      RespCommand::Zrevrank,
    ),
    (
      &b"*2\r\n$6\r\nSUBSTR\r\n$1\r\nk\r\n"[..],
      RespCommand::Substr,
    ),
    (
      &b"*5\r\n$5\r\nLMOVE\r\n$1\r\na\r\n$1\r\nb\r\n$4\r\nLEFT\r\n$5\r\nRIGHT\r\n"[..],
      RespCommand::Lmove,
    ),
  ] {
    let (cmd, _) = parse_one(&mut s, frame);
    assert_eq!(cmd, Some(expect), "{frame:?}");
  }

  // 父命令分派补齐：ACL / MODULE / BITOP / OBJECT / MEMORY / CLUSTER
  for (frame, expect) in [
    (
      &b"*2\r\n$3\r\nACL\r\n$3\r\nCAT\r\n"[..],
      RespCommand::AclCat,
    ),
    (
      &b"*2\r\n$6\r\nMODULE\r\n$6\r\nLOADCS\r\n"[..],
      RespCommand::ModuleLoadcs,
    ),
    (
      &b"*3\r\n$6\r\nMEMORY\r\n$5\r\nUSAGE\r\n$1\r\nk\r\n"[..],
      RespCommand::MemoryUsage,
    ),
    (
      &b"*3\r\n$6\r\nOBJECT\r\n$8\r\nENCODING\r\n$1\r\nk\r\n"[..],
      RespCommand::ObjectEncoding,
    ),
    (
      &b"*2\r\n$7\r\nCLUSTER\r\n$4\r\nMYID\r\n"[..],
      RespCommand::ClusterMyid,
    ),
    (
      &b"*3\r\n$7\r\nCLUSTER\r\n$16\r\nSET-CONFIG-EPOCH\r\n$1\r\n0\r\n"[..],
      RespCommand::ClusterSetconfigepoch,
    ),
  ] {
    let (cmd, _) = parse_one(&mut s, frame);
    assert_eq!(cmd, Some(expect), "{frame:?}");
  }

  // BITOP NOT（连字段子命令 + 负载参数）
  let (cmd, args) = parse_one(
    &mut s,
    b"*4\r\n$5\r\nBITOP\r\n$3\r\nNOT\r\n$3\r\ndst\r\n$3\r\nsrc\r\n",
  );
  assert_eq!(cmd, Some(RespCommand::BitopNot));
  assert_eq!(args, vec![b"dst".to_vec(), b"src".to_vec()]);
}

/// 半截命令 / 半截数组头：等待更多字节（None），不误置违规哨兵
#[test]
fn incomplete_command_returns_none() {
  let mut s = RespServerSession::default();
  // 半截命令
  let (cmd, _) = parse_one(&mut s, b"*2\r\n$3\r\nSET\r\n$1\r\nk");
  assert_eq!(cmd, None);

  // 数组头不完整
  let (cmd, _) = parse_one(&mut s, b"*2\r");
  assert_eq!(cmd, None);
  // 非法 token 缺失时不得误置协议违规哨兵
  assert!(!s.parse_violation);
}

/// 畸形数组头 → 协议违规哨兵（C# RespParsingException → 断连）；
/// 缓冲不足的临界形态按不完整等待（C# ptr+3 > end / readHead+4 > end）
#[test]
fn malformed_array_header_raises_violation() {
  let mut s = RespServerSession::default();

  // '*' 后首字符非数字（≥3 字节）→ C# ThrowUnexpectedToken
  let (cmd, _) = parse_one(&mut s, b"*A\r\n$3\r\nGET\r\n");
  assert_eq!(cmd, None);
  assert!(s.parse_violation);
  // 消费方视角：try_consume_messages 以 None 表达致命错误
  let mut s2 = RespServerSession::default();
  assert_eq!(s2.try_consume_messages(b"*A\r\nGET\r\n"), None);
  assert!(!s2.parse_violation, "哨兵应被消费并复位");

  // 负长度（≥5 字节）→ C# ThrowInvalidStringLength
  s.parse_violation = false;
  let (cmd, _) = parse_one(&mut s, b"*-1\r\n");
  assert_eq!(cmd, None);
  assert!(s.parse_violation);

  // 终止符不符 → C# ThrowUnexpectedToken
  s.parse_violation = false;
  let (cmd, _) = parse_one(&mut s, b"*1X\r\n$3\r\nGET\r\n");
  assert_eq!(cmd, None);
  assert!(s.parse_violation);

  // 临界不完整形态（非违规）：
  // "*x"（<3 字节）、"*-1"（<5 字节）、"*12"（终止符未到齐）
  for frame in [&b"*x"[..], b"*-1", b"*12"] {
    s.parse_violation = false;
    let (cmd, _) = parse_one(&mut s, frame);
    assert_eq!(cmd, None, "{frame:?}");
    assert!(!s.parse_violation, "{frame:?} 不应置违规哨兵");
  }
}

/// ParseRespCommandBuffer / FuzzParseCommandBuffer 恢复接收状态快照
#[test]
fn parse_resp_command_buffer_restores_state() {
  let mut s = RespServerSession::default();
  let read_head_before = s.read_head;
  let cmd = s.parse_resp_command_buffer(b"*2\r\n$6\r\nclient\r\n$4\r\ninfo\r\n");
  assert_eq!(cmd, Some(RespCommand::ClientInfo));
  assert_eq!(s.read_head, read_head_before);
  // 模糊入口容忍半截
  let (ok, cmd) = s.fuzz_parse_command_buffer(b"*1\r\n$3\r\nGE");
  assert!(!ok);
  assert_eq!(cmd, RespCommand::Invalid);
  let (ok, cmd) = s.fuzz_parse_command_buffer(b"*1\r\n$3\r\nGET\r\n");
  assert!(ok);
  assert_eq!(cmd, RespCommand::Get);
}

/// AOF 提交模式：SET 置位阻塞标记，PING 按待发数据决定是否复位
#[test]
fn aof_commit_mode_marks_dependent_commands() {
  let mut s = RespServerSession::default();
  s.handle_aof_commit_mode(RespCommand::Set);
  assert!(s.wait_for_aof_blocking);
  // C#：无待发数据时每条命令先重置标记，PING（AOF 无关）不重新置位
  s.handle_aof_commit_mode(RespCommand::Ping);
  assert!(!s.wait_for_aof_blocking);
  // 有待发数据（dcurr > head）不重置：SET 置位保持到 PING 之后
  s.handle_aof_commit_mode(RespCommand::Set);
  s.write_direct_large(b"+OK\r\n");
  s.handle_aof_commit_mode(RespCommand::Ping);
  assert!(s.wait_for_aof_blocking);
  // 缓冲清空 → 重置
  s.output.clear();
  s.handle_aof_commit_mode(RespCommand::Ping);
  assert!(!s.wait_for_aof_blocking);
  // 事务 Started 态不改标记
  s.txn_state = TxnState::Started;
  s.handle_aof_commit_mode(RespCommand::Set);
  assert!(!s.wait_for_aof_blocking);
}

/// 内联畸形行被跳过后仍解析到后续命令
#[test]
fn inline_malformed_skips_line() {
  let mut s = RespServerSession::default();
  s.recv_buffer
    .extend_from_slice(b"garbage\r\n*1\r\n$4\r\nPING\r\n");
  s.bytes_read = s.recv_buffer.len();
  s.read_head = 0;
  let consumed = s.try_consume_messages(b"garbage\r\n*1\r\n$4\r\nPING\r\n");
  // 畸形行被跳过后仍解析到 PING
  assert!(consumed.is_some());
}

/// 逐项对标 RespCommand.cs:AofIndependentCommands 的 47 项全集
///（is_aof_independent 为 matches! 跳转表，全集对齐由此处正/负向校验）
#[test]
fn is_aof_independent_matches_csharp_set() {
  let csharp_set: &[RespCommand] = &[
    RespCommand::Async,
    RespCommand::Ping,
    RespCommand::Select,
    RespCommand::Swapdb,
    RespCommand::Echo,
    RespCommand::Monitor,
    RespCommand::ModuleLoadcs,
    RespCommand::Registercs,
    RespCommand::Info,
    RespCommand::Time,
    RespCommand::Lastsave,
    RespCommand::AclCat,
    RespCommand::AclDeluser,
    RespCommand::AclGenpass,
    RespCommand::AclGetuser,
    RespCommand::AclList,
    RespCommand::AclLoad,
    RespCommand::AclSave,
    RespCommand::AclSetuser,
    RespCommand::AclUsers,
    RespCommand::AclWhoami,
    RespCommand::ClientId,
    RespCommand::ClientInfo,
    RespCommand::ClientList,
    RespCommand::ClientKill,
    RespCommand::ClientGetname,
    RespCommand::ClientSetname,
    RespCommand::ClientSetinfo,
    RespCommand::ClientUnblock,
    RespCommand::Command,
    RespCommand::CommandCount,
    RespCommand::CommandDocs,
    RespCommand::CommandInfo,
    RespCommand::CommandGetkeys,
    RespCommand::CommandGetkeysandflags,
    RespCommand::MemoryUsage,
    RespCommand::ConfigGet,
    RespCommand::ConfigRewrite,
    RespCommand::ConfigSet,
    RespCommand::LatencyHelp,
    RespCommand::LatencyHistogram,
    RespCommand::LatencyReset,
    RespCommand::SlowlogHelp,
    RespCommand::SlowlogLen,
    RespCommand::SlowlogGet,
    RespCommand::SlowlogReset,
    RespCommand::Multi,
  ];
  // 集合相等（不多、不少、不重复）：matches! 正向逐项命中
  for cmd in csharp_set {
    assert!(is_aof_independent(*cmd), "{cmd:?} 应为 AOF 无关");
  }
  // AOF 相关命令不置独立位
  assert!(!is_aof_independent(RespCommand::Set));
  assert!(!is_aof_independent(RespCommand::Get));
  assert!(!is_aof_independent(RespCommand::Invalid));
  assert!(!is_aof_independent(RespCommand::None));
  assert!(!is_aof_independent(RespCommand::Latency));
  assert!(!is_aof_independent(RespCommand::Slowlog));
}

/// 订阅模式下仅订阅族 + PING/QUIT 可执行（对标 C# IsAllowedInSubscriptionMode）
#[test]
fn test_is_allowed_in_subscription_mode() {
  for cmd in [
    RespCommand::Subscribe,
    RespCommand::Ssubscribe,
    RespCommand::Psubscribe,
    RespCommand::Unsubscribe,
    RespCommand::Punsubscribe,
    RespCommand::Ping,
    RespCommand::Quit,
  ] {
    assert!(
      is_allowed_in_subscription_mode(cmd),
      "{cmd:?} 应允许在订阅模式下执行"
    );
  }

  for cmd in [
    RespCommand::Get,
    RespCommand::Set,
    RespCommand::Del,
    RespCommand::Publish,
    RespCommand::Spublish,
    RespCommand::Invalid,
    RespCommand::None,
  ] {
    assert!(
      !is_allowed_in_subscription_mode(cmd),
      "{cmd:?} 不应允许在订阅模式下执行"
    );
  }
}
