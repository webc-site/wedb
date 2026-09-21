//! RESP 命令解析端到端测试（自 src/resp/parser/resp_command.rs 内嵌测试迁出）
//!
//! 对标 libs/server/Resp/Parser/RespCommand.cs 的解析分级：16 字节模式表、
//! 单数字帧标量快路径、MRU 缓存、慢路径查表与子命令分派、协议违规哨兵、
//! AOF 提交模式与内联命令跳过。私有命令表（PRIMARY_TABLE / 子表）的不变式
//! 单元测试仍留在 src。

use wnode::resp::{
  parser::resp_command::{is_allowed_in_subscription_mode, is_aof_independent},
  resp_server_session::{RespServerSession, RespServerSessionOptions},
};
use wnode_test::drain_output;
use wresp::command::RespCommand;
use wtxn::TxnState;

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

/// 热命令 16 字节模式表 + 内联 + 小写慢路径
/// 模拟泵直填一批字节（take → extend → return → consume 的会话侧等价）
fn feed(s: &mut RespServerSession, bytes: &[u8]) -> Option<usize> {
  s.recv_buffer.extend_from_slice(bytes);
  s.try_consume_messages()
}

/// 直呼快速解析（不经慢路径与命令分派），回读命中命令、参数个数与前移量
fn fast_parse(s: &mut RespServerSession, frame: &[u8]) -> (RespCommand, isize, usize) {
  s.recv_buffer.clear();
  s.recv_buffer.extend_from_slice(frame);
  s.bytes_read = s.recv_buffer.len();
  s.read_head = 0;
  let mut count = -1isize;
  let cmd = s.fast_parse_command(&mut count);
  (cmd, count, s.read_head)
}

/// C# RespCommandSimdPatterns.cs:RespPattern 的同构帧构造：`*N\r\n$L\r\nCMD\r\n`
fn resp_pattern(elements: u8, name: &[u8]) -> Vec<u8> {
  let mut frame = Vec::new();
  frame.extend([b'*', b'0' + elements, b'\r', b'\n']);
  frame.extend([b'$', b'0' + name.len() as u8, b'\r', b'\n']);
  frame.extend_from_slice(name);
  frame.extend(b"\r\n");
  frame
}

/// 热命令固定形状表（测试侧独立真值）：`(数组元素总数, 命令名, 命中命令, 参数个数)`
///
/// 逐项对标 RespCommandSimdPatterns.cs 的 s_GET..s_PSETEX 与表内同序；
/// 帧字节由 [`resp_pattern`]（C# RespPattern 同构）现构，不手抄字面量
const HOT: [(u8, &[u8], RespCommand, isize); 18] = [
  (2, b"GET", RespCommand::Get, 1),
  (3, b"SET", RespCommand::Set, 2),
  (2, b"DEL", RespCommand::Del, 1),
  (2, b"TTL", RespCommand::Ttl, 1),
  (1, b"PING", RespCommand::Ping, 0),
  (2, b"INCR", RespCommand::Incr, 1),
  (2, b"DECR", RespCommand::Decr, 1),
  (1, b"EXEC", RespCommand::Exec, 0),
  (2, b"PTTL", RespCommand::Pttl, 1),
  (1, b"MULTI", RespCommand::Multi, 0),
  (3, b"SETNX", RespCommand::Setnx, 2),
  (4, b"SETEX", RespCommand::Setex, 3),
  (2, b"EXISTS", RespCommand::Exists, 1),
  (2, b"GETDEL", RespCommand::Getdel, 1),
  (3, b"APPEND", RespCommand::Append, 2),
  (3, b"INCRBY", RespCommand::Incrby, 2),
  (3, b"DECRBY", RespCommand::Decrby, 2),
  (4, b"PSETEX", RespCommand::Psetex, 3),
];

/// 模式表臂改向量核后的逐字节等价：18 项（帧由 C# RespPattern 同构独立构造）
/// 命中命令、count 赋值与 `read_head += 模式长度`（13/14/15/16，非恒 16）
/// 全部保持；模式长度之后的输入字节不参与判定
#[test]
fn fast_pattern_table_vectorized_equivalence() {
  let mut s = RespServerSession::default();
  for (elements, name, cmd, arg_count) in HOT {
    let pattern = resp_pattern(elements, name);
    assert_eq!(pattern.len(), name.len() + 10, "{name:?} 帧长");
    // 模式之后填满并越过 16 字节窗口的诱饵字节：掩码失效即失配
    let mut frame = pattern.clone();
    frame.extend_from_slice(b"\r\n$9\r\nZZZZZZZZZZZZZZZZ");
    let (parsed, count, advanced) = fast_parse(&mut s, &frame);
    assert_eq!(parsed, cmd, "{name:?} 命中命令");
    assert_eq!(count, arg_count, "{name:?} 参数个数");
    assert_eq!(advanced, pattern.len(), "{name:?} 前移量应为模式长度");
    // 同一帧逐字节全等形态（16 字节整窗即模式）同样命中
    let exact = &frame[..pattern.len()];
    let mut padded = exact.to_vec();
    padded.resize(16, b'\0');
    let (parsed, _, advanced) = fast_parse(&mut s, &padded);
    assert_eq!(parsed, cmd, "{name:?} 零填尾部");
    assert_eq!(advanced, pattern.len(), "{name:?} 零填尾部前移量");
    // 名尾一字节差异（掩码宽度内）不得命中
    let mut broken = pattern.clone();
    let last = broken.len() - 3;
    broken[last] ^= b'X';
    let mut frame = broken;
    frame.extend_from_slice(b"\r\n$1\r\nk\r\n");
    assert_eq!(
      fast_parse(&mut s, &frame).0,
      RespCommand::None,
      "{name:?} 单字节差异不得命中"
    );
    // 小写帧不属固定形状表（表为大小写敏感的字面量）
    let lower = resp_pattern(elements, &name.to_ascii_lowercase());
    let mut frame = lower;
    frame.extend_from_slice(b"\r\n$1\r\nk\r\n");
    assert_eq!(
      fast_parse(&mut s, &frame).0,
      RespCommand::None,
      "{name:?} 小写不得命中模式表"
    );
  }
}

/// 向量核与转写前标量表臂的逐用例对拍（tests/ 侧参照，独立于生产派生数组）
///
/// 参照 = 本票落地前的形态：18 项变长帧按表序逐项 `buffer.len() >= start +
/// pattern.len() && &buffer[start..start + pattern.len()] == pattern` 取首中。
/// 帧由 [`resp_pattern`]（C# RespPattern 同构）现构、掩码与定长承载在测试侧
/// 独立派生，故与生产 `FAST_PATTERN_GROUPS` 互为之证；两侧在同一窗口上须给出
/// 同一命中序位。覆盖 18 项 × 尾部填充、18 × 16 × 8 逐字节位翻转与伪随机语料
#[test]
fn vector_kernel_matches_legacy_scalar_table_scan() {
  use wbase::simd::{MaskedGroup, first_masked_eq};

  const fn mask_for_len(len: usize) -> [u8; 16] {
    let mut mask = [0u8; 16];
    let mut i = 0;
    while i < len {
      mask[i] = 0xFF;
      i += 1;
    }
    mask
  }

  let frames: Vec<Vec<u8>> = HOT
    .iter()
    .map(|(elements, name, ..)| resp_pattern(*elements, name))
    .collect();
  let padded: Vec<[u8; 16]> = frames
    .iter()
    .map(|frame| {
      let mut bytes = [0u8; 16];
      bytes[..frame.len()].copy_from_slice(frame);
      bytes
    })
    .collect();
  // 按长度档成组（表内 13 → 14 → 15 → 16 连续同序，与 C# 判定序一致）
  let mut groups: Vec<MaskedGroup<'_>> = Vec::new();
  let mut start = 0;
  while start < padded.len() {
    let frame_len = frames[start].len();
    let mut end = start;
    while end < padded.len() && frames[end].len() == frame_len {
      end += 1;
    }
    groups.push(MaskedGroup {
      mask: (frame_len < 16).then(|| mask_for_len(frame_len)),
      candidates: &padded[start..end],
      base: start,
    });
    start = end;
  }
  assert_eq!(groups.len(), 4, "四档长度组");

  // 转写前标量表臂（16 字节窗口内、start = 0 形态）
  let legacy_scan = |window: &[u8; 16]| -> Option<usize> {
    frames
      .iter()
      .position(|frame| window.len() >= frame.len() && &window[..frame.len()] == frame)
  };
  let cross_check = |window: &[u8; 16]| {
    assert_eq!(
      first_masked_eq(window, &groups),
      legacy_scan(window),
      "窗口 {window:?} 向量核与标量表臂不同判"
    );
  };

  for (idx, frame) in frames.iter().enumerate() {
    for fill in [0x00u8, 0xFF, b'Z', b'\r'] {
      let mut window = [fill; 16];
      window[..frame.len()].copy_from_slice(frame);
      assert_eq!(
        first_masked_eq(&window, &groups),
        Some(idx),
        "第 {idx} 项 fill={fill:#04x} 应命中表序位"
      );
      cross_check(&window);
    }
    for position in 0..16 {
      for bit in 0..8 {
        let mut window = [0u8; 16];
        window[..frame.len()].copy_from_slice(frame);
        window[position] ^= 1 << bit;
        cross_check(&window);
      }
    }
  }

  let mut state = 0x9E37_79B9_7F4A_7C15u64;
  let mut next = move || {
    state = state
      .wrapping_mul(6364136223846793005)
      .wrapping_add(1442695040888963407);
    (state >> 33) as u8
  };
  for _ in 0..2048 {
    let mut window = [0u8; 16];
    for byte in &mut window {
      *byte = next();
    }
    cross_check(&window);
  }
}

/// MRU 双槽掩码：命中判定只覆盖消费长度之内的字节（C# `_cachedMaskN` 同形）
#[test]
fn mru_mask_ignores_bytes_after_consumed_length() {
  let mut s = RespServerSession::default();
  // ECHO 不在固定模式表 → 首帧走慢路径入槽，消费长度 14（窗口第 15、16 字节掩码清零）
  let (cmd, _) = parse_one(&mut s, b"*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n");
  assert_eq!(cmd, Some(RespCommand::Echo));
  // 同前 14 字节、16 字节窗口内第 16 字节由 '3' 变 '1' → 直呼快速解析仍命中槽 0
  let (parsed, count, advanced) = fast_parse(&mut s, b"*2\r\n$4\r\nECHO\r\n$10\r\nabcdefghij\r\n");
  assert_eq!(parsed, RespCommand::Echo, "掩码外的窗口字节差异不应失配");
  assert_eq!(count, 1);
  assert_eq!(advanced, 14, "MRU 命中前移量应为入槽消费长度");
  // 掩码宽度内（第 9 字节命令名首字符）差异 → 槽位失配，快速解析落 None
  let (parsed, _, advanced) = fast_parse(&mut s, b"*2\r\n$4\r\nWCHO\r\n$3\r\nhey\r\n");
  assert_eq!(parsed, RespCommand::None);
  assert_eq!(advanced, 0);
}

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
  let (cmd, args) = parse_one(
    &mut s,
    b"*4\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$2\r\nNX\r\n",
  );
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
  let (cmd, _) = parse_one(
    &mut s,
    b"*4\r\n$4\r\nHSET\r\n$1\r\nk\r\n$1\r\nf\r\n$1\r\nv\r\n",
  );
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
  let out = drain_output(&mut s);
  assert_eq!(out, b"-ERR unknown subcommand 'NOPE'.\r\n");

  // CLUSTER 未知子命令 → 带帮助提示文案
  let (cmd, _) = parse_one(&mut s, b"*2\r\n$7\r\nCLUSTER\r\n$4\r\nNOPE\r\n");
  assert_eq!(cmd, Some(RespCommand::Invalid));
  let out = drain_output(&mut s);
  assert_eq!(out, b"-ERR unknown subcommand 'NOPE'. Try CLUSTER HELP\r\n");

  // 父命令无参（非 COMMAND）→ wrong number of arguments（父名大写对齐 C# 枚举名）
  let (cmd, _) = parse_one(&mut s, b"*1\r\n$6\r\nCLIENT\r\n");
  assert_eq!(cmd, Some(RespCommand::Invalid));
  let out = drain_output(&mut s);
  assert_eq!(
    out,
    b"-ERR wrong number of arguments for 'CLIENT' command\r\n"
  );

  // 完全未知命令 → Invalid + unknown command
  let (cmd, _) = parse_one(&mut s, b"*1\r\n$4\r\nnope\r\n");
  assert_eq!(cmd, Some(RespCommand::Invalid));
  let out = drain_output(&mut s);
  assert_eq!(out, b"-ERR unknown command\r\n");
}

/// 主表二分检索回归：HELLO/HDEL 曾乱序致 HDEL 漏查；父命令与子命令表
/// 补齐 ACL/BITOP/CLUSTER/MEMORY/OBJECT 后应全部分派成功
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

  // 父命令分派补齐：ACL / BITOP / OBJECT / MEMORY / CLUSTER
  for (frame, expect) in [
    (
      &b"*2\r\n$3\r\nACL\r\n$3\r\nCAT\r\n"[..],
      RespCommand::AclCat,
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
  assert!(s.parse_violation.is_none());
}

/// 畸形数组头 → 协议违规哨兵携带 C# RespParsingException 文案（断连）；
/// 缓冲不足的临界形态按不完整等待（C# ptr+3 > end / readHead+4 > end）
#[test]
fn malformed_array_header_raises_violation() {
  let mut s = RespServerSession::default();

  // '*' 后首字符非数字（≥3 字节）→ C# ThrowUnexpectedToken
  let (cmd, _) = parse_one(&mut s, b"*A\r\n$3\r\nGET\r\n");
  assert_eq!(cmd, None);
  assert_eq!(
    s.parse_violation.as_deref(),
    Some("Unexpected character 'A'.")
  );
  // 消费方视角：try_consume_messages 以 None 表达致命错误，协议错误
  // 已落输出（C# catch 块 ERR Protocol Error 文案对标）
  let mut s2 = RespServerSession::default();
  assert_eq!(feed(&mut s2, b"*A\r\nGET\r\n"), None);
  assert!(s2.parse_violation.is_none(), "哨兵应被消费并复位");
  assert_eq!(
    drain_output(&mut s2),
    b"-ERR Protocol Error: Unexpected character 'A'.\r\n"
  );

  // 负长度（≥5 字节，含 '-1' 特例）→ C# ThrowInvalidStringLength
  s.parse_violation = None;
  let (cmd, _) = parse_one(&mut s, b"*-1\r\n");
  assert_eq!(cmd, None);
  assert_eq!(
    s.parse_violation.as_deref(),
    Some("Invalid string length '-1'.")
  );

  // 负长度多位数值 → 文案携带实际数值
  s.parse_violation = None;
  let (cmd, _) = parse_one(&mut s, b"*-25\r\n");
  assert_eq!(cmd, None);
  assert_eq!(
    s.parse_violation.as_deref(),
    Some("Invalid string length '-25'.")
  );

  // '-' 后非数字 → C# ThrowUnexpectedToken（入口 MakeUpperCase 就地大写化，
  // 报大写 'X'，与 C# 同源）
  s.parse_violation = None;
  let (cmd, _) = parse_one(&mut s, b"*-x\r\n");
  assert_eq!(cmd, None);
  assert_eq!(
    s.parse_violation.as_deref(),
    Some("Unexpected character 'X'.")
  );

  // 终止符不符 → C# ThrowUnexpectedToken
  s.parse_violation = None;
  let (cmd, _) = parse_one(&mut s, b"*1X\r\n$3\r\nGET\r\n");
  assert_eq!(cmd, None);
  assert_eq!(
    s.parse_violation.as_deref(),
    Some("Unexpected character 'X'.")
  );

  // 临界不完整形态（非违规）：
  // "*x"（<3 字节）、"*-1"（<5 字节）、"*12"（终止符未到齐）
  for frame in [&b"*x"[..], b"*-1", b"*12"] {
    s.parse_violation = None;
    let (cmd, _) = parse_one(&mut s, frame);
    assert_eq!(cmd, None, "{frame:?}");
    assert!(s.parse_violation.is_none(), "{frame:?} 不应置违规哨兵");
  }
}

/// 协议违规时同批此前命令应答不丢、错误应答最后落位
///（C# RespServerSession.cs:522-537 catch 块：TryWriteError 追加在累积
/// 应答之后 → Send 整体发出 → DisposeNetworkSender 断连）
#[test]
fn protocol_violation_writes_error_after_prior_replies() {
  // None = 断连哨兵（泵发尽应答后关闭连接），违规字节驻留缓冲不会
  // 被同会话续消费——每个违规形态独立会话验证

  // 合法 PING + 违规数组头同批：应答顺序 = +PONG → 协议错误
  let mut s = RespServerSession::default();
  assert_eq!(feed(&mut s, b"*1\r\n$4\r\nPING\r\n*A\r\n"), None);
  assert_eq!(
    drain_output(&mut s),
    b"+PONG\r\n-ERR Protocol Error: Unexpected character 'A'.\r\n"
  );
  // 哨兵已消费复位
  assert!(s.parse_violation.is_none());

  // 违规形态多样性：负长度 / 终止符不符
  let mut s = RespServerSession::default();
  assert_eq!(feed(&mut s, b"*1\r\n$4\r\nPING\r\n*-2\r\n"), None);
  assert_eq!(
    drain_output(&mut s),
    b"+PONG\r\n-ERR Protocol Error: Invalid string length '-2'.\r\n"
  );

  let mut s = RespServerSession::default();
  assert_eq!(feed(&mut s, b"PING\r\n*1X\r\n"), None);
  assert_eq!(
    drain_output(&mut s),
    b"+PONG\r\n-ERR Protocol Error: Unexpected character 'X'.\r\n"
  );
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
  s.output.extend_from_slice(b"+OK\r\n");
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

/// EnableAOF && WaitForCommit 门控（RespCommand.cs:ParseCommand 尾部）：
/// 门关时解析不维护阻塞标记，门开后 AOF 相关命令置位
#[test]
fn aof_commit_mode_gate_controls_flag_maintenance() {
  // 门关（默认 EnableAOF = false）：解析 SET 不置位
  let mut s = RespServerSession::default();
  let (cmd, _) = parse_one(&mut s, b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n");
  assert_eq!(cmd, Some(RespCommand::Set));
  assert!(!s.wait_for_aof_blocking);

  // 门开（EnableAOF && WaitForCommit）：解析 SET 置位
  let mut s = RespServerSession::new(
    1,
    RespServerSessionOptions {
      enable_aof: true,
      wait_for_commit: true,
      ..RespServerSessionOptions::default()
    },
  );
  let (cmd, _) = parse_one(&mut s, b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n");
  assert_eq!(cmd, Some(RespCommand::Set));
  assert!(s.wait_for_aof_blocking);
}

/// 内联畸形行被跳过后仍解析到后续命令
#[test]
fn inline_malformed_skips_line() {
  let mut s = RespServerSession::default();
  let consumed = feed(&mut s, b"garbage\r\n*1\r\n$4\r\nPING\r\n");
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
    RespCommand::Sunsubscribe,
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

/// 模式表臂改前/改后同进程 A/B 吞吐探针（判据 8 取证用，不进门禁）
///
/// 改前形态 = 本票落地前的 18 项变长帧逐项带长度门的标量切片比较
///（逐行照抄 git 上前一版的 fast_parse_command 模式表臂），改后形态 = 会话现路径
///（一次 16B 载入 + 分组掩码整向量全等）。另跑一路纯循环基线，剥离会话结构体
/// 读写等共同开销，只看表臂净耗时。
///
/// 跑法（不能用 `--release`：本文件既有用例 `fuzz_parse_command_buffer` 依赖
/// `#[cfg(debug_assertions)]` 门，release 下该测试目标在 dev 现尖即无法编译，
/// 故以 opt-level=3 + 显式开回 debug-assertions 的等价优化档跑）：
/// `RUSTFLAGS="-C opt-level=3 -C debug-assertions=on" cargo test -p wnode --test resp_command_parse -- --ignored --nocapture --test-threads=1`
#[test]
#[ignore = "吞吐探针：优化档手工跑，见函数内跑法"]
fn bench_fast_pattern_table_arm_throughput() {
  use std::{hint::black_box, time::Instant};

  /// 表臂容器：定长 16B 承载原变长帧 + 真实帧长（尾部字节不参与比较）
  type LegacyTable = [([u8; 16], usize, RespCommand, isize)];

  /// 改前形态（与转写前实现逐句同构）
  #[inline(never)]
  fn legacy_table_arm(s: &mut RespServerSession, table: &LegacyTable, count: &mut isize) {
    let start = s.read_head;
    let remaining = s.bytes_read.saturating_sub(start);
    if remaining >= 16 && s.recv_buffer[start] == b'*' {
      for (bytes, len, cmd, arg_count) in table {
        let pattern = &bytes[..*len];
        if s.recv_buffer.len() >= start + pattern.len()
          && &s.recv_buffer[start..start + pattern.len()] == pattern
        {
          s.read_head += pattern.len();
          *count = *arg_count;
          black_box(*cmd);
          return;
        }
      }
    }
    black_box(RespCommand::None);
  }

  /// 单轮 ITERS 次表臂调用，回传均值纳秒（f64 保精度，避免整除截断把差异抹平）
  ///
  /// `which`：0 = 纯循环基线（两臂共同开销，用于剥离表臂净耗时），
  /// 1 = 改前形态，2 = 改后形态（会话现路径）。
  #[inline(never)]
  fn run(iters: usize, s: &mut RespServerSession, table: &LegacyTable, which: u8) -> f64 {
    let started = Instant::now();
    for _ in 0..iters {
      s.read_head = 0;
      let mut count = -1isize;
      match which {
        0 => {
          black_box((s.read_head, count));
        }
        1 => legacy_table_arm(s, table, &mut count),
        _ => {
          black_box(s.fast_parse_command(&mut count));
        }
      }
      black_box((s.read_head, count));
    }
    started.elapsed().as_nanos() as f64 / iters as f64
  }

  let table: Vec<([u8; 16], usize, RespCommand, isize)> = HOT
    .iter()
    .map(|(elements, name, cmd, arg_count)| {
      let frame = resp_pattern(*elements, name);
      let mut bytes = [0u8; 16];
      bytes[..frame.len()].copy_from_slice(&frame);
      (bytes, frame.len(), *cmd, *arg_count)
    })
    .collect();
  assert_eq!(table.len(), 18);

  const ITERS: usize = 1 << 20;
  const ROUNDS: usize = 5;
  let mut s = RespServerSession::default();
  println!("模式表臂吞吐（iters={ITERS}，{ROUNDS} 轮取最优，单位 ns/op）");
  for (label, frame) in [
    ("GET", &b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n"[..]),
    // PING 帧本体仅 14B，须带后续命令凑满 16B 视窗门，两臂才同走模式表臂
    ("PING", &b"*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n"[..]),
    ("EXISTS", &b"*2\r\n$6\r\nEXISTS\r\n$1\r\nk\r\n"[..]),
  ] {
    s.recv_buffer.clear();
    s.recv_buffer.extend_from_slice(frame);
    s.bytes_read = s.recv_buffer.len();
    let mut legacy = f64::MAX;
    let mut vector = f64::MAX;
    let mut baseline = f64::MAX;
    for _ in 0..ROUNDS {
      baseline = baseline.min(run(ITERS, &mut s, &table, 0));
      legacy = legacy.min(run(ITERS, &mut s, &table, 1));
      vector = vector.min(run(ITERS, &mut s, &table, 2));
    }
    // 命中自检：两臂都必须由模式表臂吃掉帧长，且吃掉同一字节数，
    // 否则测到的是标量慢路径，数字无意义
    let mut c = -1isize;
    s.read_head = 0;
    legacy_table_arm(&mut s, &table, &mut c);
    let legacy_consumed = s.read_head;
    s.read_head = 0;
    let cmd = s.fast_parse_command(&mut c);
    assert!(
      legacy_consumed > 0 && s.read_head == legacy_consumed && c >= 0,
      "{label} 未命中模式表臂（改前吃 {legacy_consumed}，改后吃 {}，count {c}，cmd {cmd:?}）",
      s.read_head
    );
    let net_legacy = legacy - baseline;
    let net_vector = vector - baseline;
    println!(
      "{label}: 基线 {baseline:.2} ns/op，改前 {legacy:.2}（净 {net_legacy:.2}） -> 改后 {vector:.2}（净 {net_vector:.2}），净提速 {:.2}x（省 {} ns/op）",
      net_legacy / net_vector,
      (net_legacy - net_vector).max(0.0) as u64
    );
  }
}

/// test/standalone/Garnet.test/Resp/RespCommandCacheTests.cs:CachedCommandDoesNotTruncateArgumentCount
#[test]
fn cached_command_does_not_truncate_argument_count() {
  let mut s = RespServerSession::default();

  for arg_count in [255, 256, 259, 600] {
    let mut frame = Vec::new();
    let total_elements = arg_count + 1;
    frame.extend(format!("*{total_elements}\r\n$3\r\nDEL\r\n").into_bytes());
    for i in 0..arg_count {
      let arg = format!("arg{i}");
      frame.extend(format!("${}\r\n{}\r\n", arg.len(), arg).into_bytes());
    }

    let (cmd, args) = parse_one(&mut s, &frame);
    assert_eq!(cmd, Some(RespCommand::Del));
    assert_eq!(args.len(), arg_count);

    // 二次解析验证：超过 255 参数的命令不能被 MRU 缓存截断为 u8
    let (cmd2, args2) = parse_one(&mut s, &frame);
    assert_eq!(cmd2, Some(RespCommand::Del));
    assert_eq!(args2.len(), arg_count, "参数个数不能被命令缓存截断");
  }
}
