//! AOF 条目重放输入编解码（C# StringInput 的 AOF 承载形态）。
//!
//! C# 的 StringInput（libs/server/InputHeader.cs:309）被
//! AofProcessor（libs/server/AOF/AofProcessor.cs）与 RangeIndexManager.Replication
//! （libs/server/Resp/RangeIndex/RangeIndexManager.Replication.cs）共用；
//! rust 侧同构：本模块被 aof_processor（重放反序列化）与 rangeindex 复制面
//! （直写入队编码）共同引用，单点定义消除跨域重复。

use waof::{arg_sequence_len, decode_arg_slices, encode_arg_sequence};
use wcol::RespInputFlags;
use wresp::command::RespCommand;
use wval::GarnetObjectType;

/// 单条回放负载的头部（C# RespInputHeader + StringInput 参数区的组合形态）：
/// `[cmd u16][flags u8][sub_id u8][obj_type u8][pad 3B][arg1 i64][arg2 i64][arg3 i64]
///  [args_count u32][args 原文字节...]`。
///
/// `obj_type` 对标 C# RespInputHeader 的判别联合：C# 对象输入以
/// `{type: GarnetObjectType, subId}`（header byte0/byte1）替代字符串输入的
/// `cmd`；rust 侧布局显式分列（cmd 与 obj_type 独立字段），对象 RMW 条目
/// 恒写 obj_type，字符串条目恒 0（Null）。
pub const REPLAY_INPUT_HEADER_SIZE: usize = 32;

/// 空命令输入序列化预存（ReplayInput 默认形态无参数，32B 头部 + 4B 参数计数 0）。
///
/// 与编码器的绑定由测试 `empty_replay_input_bytes_bound_to_encoder` 钉死：
/// 头布局或参数区编码演化即该断言断裂，禁止常量与编码器静默脱钩。
pub const EMPTY_REPLAY_INPUT_BYTES: [u8; 36] = [0u8; 36];

/// 重放输入（StringInput 的反序列化形态）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayInput {
  /// 命令（判别值与 wresp::command::RespCommand 一致）。
  pub cmd: RespCommand,
  /// 标志位。
  pub flags: u8,
  /// 对象子操作 id。
  pub sub_id: u8,
  /// 对象类型（GarnetObjectType 判别值；仅对象条目非 0）。
  pub obj_type: u8,
  /// arg1（INCR 族增量 / SETRANGE 偏移等）。
  pub arg1: i64,
  /// arg2。
  pub arg2: i64,
  /// arg3。
  pub arg3: i64,
  /// parseState 参数序列化（APPEND/SETRANGE 数据等）。
  pub args: Vec<Vec<u8>>,
}

/// 零拷贝借用型重放输入（反序列化零拷贝形态，参数切片借用底层载荷）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayInputRef<'a> {
  /// 命令（判别值与 wresp::command::RespCommand 一致）。
  pub cmd: RespCommand,
  /// 标志位。
  pub flags: u8,
  /// 对象子操作 id。
  pub sub_id: u8,
  /// 对象类型（GarnetObjectType 判别值；仅对象条目非 0）。
  pub obj_type: u8,
  /// arg1（INCR 族增量 / SETRANGE 偏移等）。
  pub arg1: i64,
  /// arg2。
  pub arg2: i64,
  /// arg3。
  pub arg3: i64,
  /// 切片参数集合（零拷贝借用）。
  pub args: Vec<&'a [u8]>,
}

impl<'a> ReplayInputRef<'a> {
  /// 零拷贝反序列化（参数切片借用底层 bytes）。
  pub fn deserialize(bytes: &'a [u8]) -> Option<Self> {
    if bytes.len() < REPLAY_INPUT_HEADER_SIZE {
      return None;
    }
    let cmd = RespCommand::from_repr(u16::from_le_bytes([bytes[0], bytes[1]]))?;
    // 参数序列区：waof 单点零拷贝解码
    let args = decode_arg_slices(&bytes[REPLAY_INPUT_HEADER_SIZE..])?;
    Some(Self {
      cmd,
      flags: bytes[2],
      sub_id: bytes[3],
      obj_type: bytes[4],
      arg1: i64::from_le_bytes(bytes[8..16].try_into().ok()?),
      arg2: i64::from_le_bytes(bytes[16..24].try_into().ok()?),
      arg3: i64::from_le_bytes(bytes[24..32].try_into().ok()?),
      args,
    })
  }
}

/// 零分配条目入队元数据借用体（对标 C# ReplayInput）
#[derive(Debug, Clone, Copy)]
pub struct ReplayInputSlice<'a, T: AsRef<[u8]> = &'a [u8]> {
  /// 命令
  pub cmd: RespCommand,
  /// 标志位
  pub flags: u8,
  /// 对象子操作 id
  pub sub_id: u8,
  /// 对象类型（GarnetObjectType 判别值）
  pub obj_type: u8,
  /// arg1
  pub arg1: i64,
  /// arg2
  pub arg2: i64,
  /// arg3
  pub arg3: i64,
  /// 切片参数集合
  pub args: &'a [T],
}

impl<'a, T: AsRef<[u8]>> ReplayInputSlice<'a, T> {
  #[inline]
  pub const fn new(cmd: RespCommand, args: &'a [T]) -> Self {
    Self {
      cmd,
      flags: 0,
      sub_id: 0,
      obj_type: 0,
      arg1: 0,
      arg2: 0,
      arg3: 0,
      args,
    }
  }

  #[inline]
  pub const fn with_flags(mut self, flags: u8) -> Self {
    self.flags = flags;
    self
  }

  /// 附加确定性标志（对标 C# RespInputFlags.Deterministic）
  #[inline]
  pub const fn with_deterministic(mut self) -> Self {
    self.flags |= RespInputFlags::DETERMINISTIC.bits();
    self
  }

  /// 附加对象子操作 id（对象条目 sub_id 判别，对标 C# ObjectInput.header.subId）
  #[inline]
  pub const fn with_sub_id(mut self, sub_id: u8) -> Self {
    self.sub_id = sub_id;
    self
  }

  /// 附加对象类型判别（对标 C# ObjectInput.header.type）
  #[inline]
  pub const fn with_obj_type(mut self, obj_type: GarnetObjectType) -> Self {
    self.obj_type = obj_type as u8;
    self
  }

  /// 构造带确定性标志的切片输入（对标 C# 输入构造侧置位：
  /// libs/server/Resp/RangeIndex/RangeIndexManager.Replication.cs 的
  /// `flags |= RespInputFlags.Deterministic`）
  #[inline]
  pub const fn new_deterministic(cmd: RespCommand, args: &'a [T]) -> Self {
    Self::new(cmd, args).with_deterministic()
  }

  #[inline]
  pub const fn with_args_num(mut self, arg1: i64, arg2: i64, arg3: i64) -> Self {
    self.arg1 = arg1;
    self.arg2 = arg2;
    self.arg3 = arg3;
    self
  }
}

impl Default for ReplayInput {
  /// 默认形态（写侧空输入的预存字节形态）：cmd 为 None、全字段零、无参数。
  fn default() -> Self {
    Self {
      cmd: RespCommand::None,
      flags: 0,
      sub_id: 0,
      obj_type: 0,
      arg1: 0,
      arg2: 0,
      arg3: 0,
      args: Vec::new(),
    }
  }
}

impl ReplayInput {
  /// 计算切片序列化字节数（32B 固定头 + 参数序列区）
  #[inline]
  pub fn encoded_len_for_slices(args: &[impl AsRef<[u8]>]) -> usize {
    REPLAY_INPUT_HEADER_SIZE + arg_sequence_len(args)
  }

  /// 序列化到缓冲切片，返回已写入切片（若缓冲不足则返回 None）
  pub fn encode_to_slice<'a, T: AsRef<[u8]>>(
    input: &ReplayInputSlice<'_, T>,
    buf: &'a mut [u8],
  ) -> Option<&'a [u8]> {
    let total_len = Self::encoded_len_for_slices(input.args);
    if buf.len() < total_len {
      return None;
    }
    let raw: u16 = input.cmd.into();
    buf[0..2].copy_from_slice(&raw.to_le_bytes());
    buf[2] = input.flags;
    buf[3] = input.sub_id;
    buf[4] = input.obj_type;
    buf[5..8].fill(0);
    buf[8..16].copy_from_slice(&input.arg1.to_le_bytes());
    buf[16..24].copy_from_slice(&input.arg2.to_le_bytes());
    buf[24..32].copy_from_slice(&input.arg3.to_le_bytes());
    let args_len = encode_arg_sequence(input.args, &mut buf[REPLAY_INPUT_HEADER_SIZE..]);
    Some(&buf[..REPLAY_INPUT_HEADER_SIZE + args_len])
  }

  /// 统一零分配/低分配写入助手：优先使用 512B 栈缓冲，超大载荷自动回落堆缓冲
  pub fn with_encoded_slices<R, T: AsRef<[u8]>>(
    input: &ReplayInputSlice<'_, T>,
    f: impl FnOnce(&[u8]) -> R,
  ) -> R {
    let total_len = Self::encoded_len_for_slices(input.args);
    if total_len <= 512 {
      let mut stack_buf = [0u8; 512];
      let slice = Self::encode_to_slice(input, &mut stack_buf).expect("stack buffer sufficient");
      f(slice)
    } else {
      let mut heap_buf = vec![0u8; total_len];
      let slice = Self::encode_to_slice(input, &mut heap_buf).expect("heap buffer sufficient");
      f(slice)
    }
  }

  /// 序列化（AOF 写入侧共用编码）。
  pub fn serialize(&self, into: &mut Vec<u8>) {
    let needed = Self::encoded_len_for_slices(&self.args);
    into.reserve(needed);
    let start = into.len();
    into.resize(start + needed, 0);
    let slice_input = ReplayInputSlice {
      cmd: self.cmd,
      flags: self.flags,
      sub_id: self.sub_id,
      obj_type: self.obj_type,
      arg1: self.arg1,
      arg2: self.arg2,
      arg3: self.arg3,
      args: &self.args,
    };
    Self::encode_to_slice(&slice_input, &mut into[start..]);
  }

  /// 反序列化（C# StringInput.DeserializeFrom 的组合形态；参数序列区
  /// 经 waof 单点解码）。
  pub fn deserialize(bytes: &[u8]) -> Option<Self> {
    let r = ReplayInputRef::deserialize(bytes)?;
    Some(Self {
      cmd: r.cmd,
      flags: r.flags,
      sub_id: r.sub_id,
      obj_type: r.obj_type,
      arg1: r.arg1,
      arg2: r.arg2,
      arg3: r.arg3,
      args: r.args.into_iter().map(<[u8]>::to_vec).collect(),
    })
  }
}

#[cfg(test)]
mod tests {
  use wval::GarnetObjectType;

  use super::*;

  /// `EMPTY_REPLAY_INPUT_BYTES` 与编码器绑定：常量必须恒等于
  /// `ReplayInput::default()` 的编码产物（32B 头 + 4B 参数计数 0）。
  /// 头布局或参数区编码一旦演化，此处即断，写侧预存字节不可能静默失配。
  #[test]
  fn empty_replay_input_bytes_bound_to_encoder() {
    let mut bytes = Vec::new();
    ReplayInput::default().serialize(&mut bytes);
    assert_eq!(bytes.as_slice(), &EMPTY_REPLAY_INPUT_BYTES[..]);

    // 入队热路径的借用切片形态与预存常量逐字节一致
    ReplayInput::with_encoded_slices(
      &ReplayInputSlice::<&[u8]>::new(RespCommand::None, &[]),
      |slice| {
        assert_eq!(slice, &EMPTY_REPLAY_INPUT_BYTES[..]);
      },
    );
  }

  #[test]
  fn replay_input_parses_integration_bytes() {
    let bytes: Vec<u8> = [
      0x4a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x6b, 0x02, 0x00, 0x00, 0x00,
      0x76, 0x31,
    ]
    .to_vec();
    let parsed_ref = ReplayInputRef::deserialize(&bytes).expect("集成字节借用应可解析");
    assert_eq!(parsed_ref.cmd, RespCommand::Set);
    assert_eq!(parsed_ref.args, vec![&b"k"[..], &b"v1"[..]]);

    let parsed = ReplayInput::deserialize(&bytes).expect("集成字节应可解析");
    assert_eq!(parsed.cmd, RespCommand::Set);
    assert_eq!(parsed.args, vec![b"k".to_vec(), b"v1".to_vec()]);
  }

  #[test]
  fn replay_input_roundtrip() {
    let input = ReplayInput {
      cmd: RespCommand::Set,
      flags: 0,
      sub_id: 0,
      obj_type: 0,
      arg1: 32,
      arg2: 0,
      arg3: 0,
      args: vec![b"cnt".to_vec(), b"32".to_vec()],
    };
    let mut bytes = Vec::new();
    input.serialize(&mut bytes);
    // 头 8B 元信息 + 24B 参数区 + 4B 计数 + 逐参长度前缀
    assert_eq!(bytes.len(), 8 + 24 + 4 + (4 + 3) + (4 + 2));
    let parsed = ReplayInput::deserialize(&bytes).expect("应可反序列化");
    assert_eq!(parsed.cmd, RespCommand::Set);
    assert_eq!(parsed.arg1, 32);
    assert_eq!(parsed.args, vec![b"cnt".to_vec(), b"32".to_vec()]);

    let parsed_ref = ReplayInputRef::deserialize(&bytes).expect("应可反序列化借用");
    assert_eq!(parsed_ref.cmd, RespCommand::Set);
    assert_eq!(parsed_ref.arg1, 32);
    assert_eq!(parsed_ref.args, vec![&b"cnt"[..], &b"32"[..]]);

    // 对象形态：obj_type 显式判别（对标 C# ObjectInput.header.type）
    let object_input = ReplayInput {
      cmd: RespCommand::None,
      flags: 0,
      sub_id: 6,
      obj_type: GarnetObjectType::Hash as u8,
      arg1: 0,
      arg2: 0,
      arg3: 0,
      args: vec![b"f".to_vec(), b"v".to_vec()],
    };
    let mut object_bytes = Vec::new();
    object_input.serialize(&mut object_bytes);
    let parsed = ReplayInput::deserialize(&object_bytes).expect("应可反序列化");
    assert_eq!(parsed.obj_type, GarnetObjectType::Hash as u8);
    assert_eq!(parsed.sub_id, 6);
  }
}
