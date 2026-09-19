//! 位图命令（SETBIT/GETBIT/BITCOUNT/BITPOS/BITOP/BITFIELD/BITFIELD_RO）
//!
//! 同步快路径：字符串域直读直写，磁盘候选等须异步裁决时返回 `Ok(false)`
//! 交调用方降级。位序对标 Redis/C#：bit 0 为首字节最高位。
//! C# 侧会话层经 `StringInput` 把参数传给存储回调；Rust 侧存储接口收敛到
//! 会话层直连，故 BITFIELD 子命令在解析期即固化为类型化参数。

use wbitmap::{
  BitFieldCmdArgs, BitFieldOverflow, BitFieldSecondaryCommand, BitOpAccumulator, BitmapOperation,
  bit_count_driver, bit_field_execute, bit_pos_driver, is_valid_bit_offset,
  new_block_alloc_length_from_type, parse_bitfield_encoding, parse_bitfield_overflow_slice,
  parse_bitfield_type_offset, try_validate_bit_pos_offsets,
};
use wresp::{
  check_args::check_arg_count,
  cmd_strings::{
    self as cs, RESP_ERR_BITOP_DIFF_TWO_SOURCE_KEYS_REQUIRED, RESP_ERR_BITOP_KEY_LIMIT,
    RESP_ERR_BITOP_NOT_SINGLE_SOURCE_KEY, RESP_ERR_GENERIC, RESP_ERR_INVALID_BITFIELD_TYPE,
    RESP_ERR_INVALID_OVERFLOW_TYPE, RESP_ERR_WRONG_NUMBER_OF_ARGUMENTS, abort_with_error_message,
  },
  ext::{RespSliceExt, RespVecExt},
};

use super::super::{
  basic_commands::{RiWriteGate, ri_write_gate},
  resp_server_session::RespServerSession,
  vector::vector_manager::VectorManager,
};
use crate::storage::session::common::{UserRead, read_user_sync};

/// SETBIT/GETBIT 的 offset 参数校验（对标 C# IsValidBitOffset 口径，
/// 边界单点 wbitmap::is_valid_bit_offset）
fn parse_bit_offset(raw: &[u8]) -> Option<i64> {
  let offset = raw.try_parse_i64()?;
  is_valid_bit_offset(offset).then_some(offset)
}

/// 写位域 nil 应答（随会话协议分派；C# functionsState.nilResp）
///
/// 版本二选一单源在 wresp::ext::RespVecExt::write_resp_null_ver
#[inline]
fn write_bitfield_nil(session: &RespServerSession, output: &mut Vec<u8>) {
  output.write_resp_null_ver(session.resp_protocol_version);
}

impl RespServerSession {
  /// libs/server/Resp/Bitmap/BitmapCommands.cs:NetworkStringSetBit
  /// libs/server/Storage/Session/MainStore/BitmapOps.cs:StringSetBit
  ///
  /// 键不存在时按需增长建值；应答改写前的原 bit 值。
  /// 对象键拦截（C# RMW ValueIsObject → RMWAction.WrongType）：回
  /// WRONGTYPE 且不写——若放行将按空串扩写经信封覆写清退墓碑化集合
  pub fn network_string_set_bit<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 3, output, "SETBIT");
    let key = parse_state[0];
    let Some(offset) = parse_bit_offset(parse_state[1]) else {
      abort_with_error_message(output, cs::RESP_ERR_GENERIC_BITOFFSET_IS_NOT_INTEGER);
      return Ok(true);
    };
    // C#：bit 参数须为单字符 '0'/'1'
    if !matches!(parse_state[2], b"0" | b"1") {
      abort_with_error_message(output, cs::RESP_ERR_GENERIC_BIT_IS_NOT_INTEGER);
      return Ok(true);
    }
    let bit = parse_state[2][0] - b'0';

    let byte_idx = (offset / 8) as usize;
    let bit_idx = 7 - (offset % 8) as u32;

    // 读改写原子窗口：跨「读位图—置位—写回」全程持本键桶排他闩，杜绝同键并发
    // 丢更新（对标 C# BasicSessionLocker 的 ephemeral 闩跨 InternalRMW 全程）
    let Some(window) = store.try_rmw_window(key) else {
      return Ok(false);
    };
    let mut val = match read_user_sync(store, key, |v| {
      let mut vec = Vec::with_capacity(v.len().max(byte_idx + 1));
      vec.extend_from_slice(v);
      vec
    }) {
      Ok(UserRead::Hit(v)) => v,
      Ok(UserRead::Missing) => Vec::with_capacity(byte_idx + 1),
      Ok(UserRead::WrongType) => {
        output.write_resp_error(cs::RESP_ERR_WRONG_TYPE);
        return Ok(true);
      }
      // 磁盘候选：降级
      Ok(UserRead::Deferred) => return Ok(false),
      Err(_) => {
        output.write_resp_error(RESP_ERR_GENERIC);
        return Ok(true);
      }
    };

    if val.len() <= byte_idx {
      val.resize(byte_idx + 1, 0);
    }
    let old_bit = (val[byte_idx] >> bit_idx) & 1;
    if bit == 1 {
      val[byte_idx] |= 1 << bit_idx;
    } else {
      val[byte_idx] &= !(1 << bit_idx);
    }

    match window.try_rmw_sync(&val) {
      Ok(Ok(_)) => output.write_resp_int(old_bit as i64),
      Ok(Err(_)) => return Ok(false),
      Err(_) => output.write_resp_error(RESP_ERR_GENERIC),
    }
    Ok(true)
  }

  /// libs/server/Resp/Bitmap/BitmapCommands.cs:NetworkStringGetBit
  /// libs/server/Storage/Session/MainStore/BitmapOps.cs:StringGetBit
  pub fn network_string_get_bit<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 2, output, "GETBIT");
    let key = parse_state[0];
    let Some(offset) = parse_bit_offset(parse_state[1]) else {
      abort_with_error_message(output, cs::RESP_ERR_GENERIC_BITOFFSET_IS_NOT_INTEGER);
      return Ok(true);
    };

    // 闭包内按偏移直接取位：跳过全值拷贝，零分配
    let byte_idx = (offset / 8) as usize;
    let bit_idx = 7 - (offset % 8) as u32;
    let bit = match read_user_sync(store, key, |val| {
      if byte_idx < val.len() {
        (val[byte_idx] >> bit_idx) & 1
      } else {
        0
      }
    }) {
      Ok(UserRead::Hit(bit)) => bit,
      // C# NOTFOUND → :0
      Ok(UserRead::Missing) => 0,
      // C# Read ValueIsObject → WrongType
      Ok(UserRead::WrongType) => {
        output.write_resp_error(cs::RESP_ERR_WRONG_TYPE);
        return Ok(true);
      }
      // 磁盘候选：降级
      Ok(UserRead::Deferred) => return Ok(false),
      Err(_) => {
        output.write_resp_error(RESP_ERR_GENERIC);
        return Ok(true);
      }
    };
    output.write_resp_int(i64::from(bit));
    Ok(true)
  }

  /// libs/server/Resp/Bitmap/BitmapCommands.cs:NetworkStringBitCount
  /// libs/server/Storage/Session/MainStore/BitmapOps.cs:StringBitCount
  ///
  /// 形态：BITCOUNT key [start end [BYTE|BIT]]；缺省与 BYTE 均为字节区间，
  /// BIT 为位区间（对标 C# 存储侧 arg1 仅在带第 4 参时生效）
  pub fn network_string_bit_count<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some((key, start, end, offset_type)) = parse_bit_count_args(parse_state, output) else {
      return Ok(true);
    };

    // 闭包内直接统计区间：跳过全值拷贝，零分配
    let total = match read_user_sync(store, key, |val| {
      bit_count_driver(start, end, offset_type, val, val.len() as i64)
    }) {
      Ok(UserRead::Hit(total)) => total,
      // C# NOTFOUND → :0
      Ok(UserRead::Missing) => 0,
      // C# Read ValueIsObject → WrongType
      Ok(UserRead::WrongType) => {
        output.write_resp_error(cs::RESP_ERR_WRONG_TYPE);
        return Ok(true);
      }
      // 磁盘候选：降级
      Ok(UserRead::Deferred) => return Ok(false),
      Err(_) => {
        output.write_resp_error(RESP_ERR_GENERIC);
        return Ok(true);
      }
    };
    output.write_resp_int(total);
    Ok(true)
  }

  /// libs/server/Resp/Bitmap/BitmapCommands.cs:NetworkStringBitPosition
  /// libs/server/Storage/Session/MainStore/BitmapOps.cs:StringBitPosition
  pub fn network_string_bit_position<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some(BitPosArgs {
      key,
      search_for,
      start_offset,
      end_offset,
      offset_type,
      has_start_offset,
      has_end_offset,
    }) = parse_bit_pos_args(parse_state, output)
    else {
      return Ok(true);
    };

    // 区间越界直接 -1
    if try_validate_bit_pos_offsets(
      start_offset,
      end_offset,
      offset_type,
      has_start_offset,
      has_end_offset,
    ) {
      output.write_resp_int(-1);
      return Ok(true);
    }

    // 闭包内直接查找：跳过全值拷贝，零分配
    match read_user_sync(store, key, |val| {
      bit_pos_driver(
        val,
        val.len() as i64,
        start_offset,
        end_offset,
        search_for,
        offset_type,
      )
    }) {
      Ok(UserRead::Hit(pos)) => output.write_resp_int(pos),
      // C# NOTFOUND：找 0 回 0，找 1 回 -1
      Ok(UserRead::Missing) => {
        let resp = if search_for == 0 {
          cs::RESP_RETURN_VAL_0
        } else {
          cs::RESP_RETURN_VAL_N1
        };
        output.extend_from_slice(resp);
      }
      // C# Read ValueIsObject → WrongType
      Ok(UserRead::WrongType) => {
        output.write_resp_error(cs::RESP_ERR_WRONG_TYPE);
      }
      // 磁盘候选：降级
      Ok(UserRead::Deferred) => return Ok(false),
      Err(_) => {
        output.write_resp_error(RESP_ERR_GENERIC);
      }
    }
    Ok(true)
  }

  /// libs/server/Resp/Bitmap/BitmapCommands.cs:NetworkStringBitOperation
  /// libs/server/Storage/Session/MainStore/BitmapOps.cs:StringBitOperation
  ///
  /// C# 由分派器按 BITOP AND/OR/XOR/NOT/DIFF 传入 [`BitmapOperation`]。
  ///
  /// 向量登记键在位点内分流裁决（派发层 BITOP 族豁免，见 wresp
  /// is_vector_gate_exempt）：源键命中登记整体 -WRONGTYPE 零写（C# 源循环
  /// WRONGTYPE abort，dest 未触）；目的键命中登记仅在折叠值将落盘时
  /// （C# keysFound/maxBitmapLen>0 同位）预清退再覆写（C# dest 走
  /// DELETE+SET 重试臂），全源缺失零写、登记保留。目的键写共同体前置
  /// [`ri_write_gate`]：存活 RangeIndex 上拒写，杜绝 String+Meta 双域幽灵
  /// （SETNX/RESTORE 的 RI 半边由 probe_alive Meta 折叠承接，本位点是
  /// 该族最后一个盲写口）。
  pub fn network_string_bit_operation<'a, D: wdev::Device>(
    &mut self,
    bit_op: BitmapOperation,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    vector: Option<&VectorManager>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let count = parse_state.len();
    // 参数过少（parse_state = [destkey, srckey...]）
    if count < 2 {
      abort_with_error_message(output, RESP_ERR_WRONG_NUMBER_OF_ARGUMENTS);
      return Ok(true);
    }
    // DIFF 至少两个源
    if bit_op == BitmapOperation::Diff && count < 3 {
      abort_with_error_message(output, RESP_ERR_BITOP_DIFF_TWO_SOURCE_KEYS_REQUIRED);
      return Ok(true);
    }
    // NOT 为一元：恰一个源键
    if bit_op == BitmapOperation::Not && count > 2 {
      abort_with_error_message(output, RESP_ERR_BITOP_NOT_SINGLE_SOURCE_KEY);
      return Ok(true);
    }
    // 源键上限（含 destkey 共 64）
    if count > 64 {
      abort_with_error_message(output, RESP_ERR_BITOP_KEY_LIMIT);
      return Ok(true);
    }

    let dest_key = parse_state[0];

    // 逐源回调折叠：源切片借用进闭包即弃，仅持有一份最长源长度的目标缓冲
    // （C# PinnedSpanByte 源指针收集 + 单次 InvokeBitOperationUnsafe 的同
    // 语义流式化，不逐源克隆）；缺失键跳过（C# NOTFOUND continue）；任一
    // 源键命中向量登记或为对象键即整体 WRONGTYPE 短路（C# StringBitOperation
    // 逐键 RecordType 判定：dest 尚未写入，无副作用）
    let mut acc = BitOpAccumulator::new(bit_op);
    for src_key in &parse_state[1..] {
      if vector.is_some_and(|vm| {
        vm.read_stored_index(store.session_prefix().as_slice(), src_key)
          .is_some()
      }) {
        output.write_resp_error(cs::RESP_ERR_WRONG_TYPE);
        return Ok(true);
      }
      match read_user_sync(store, src_key, |v| acc.fold(v)) {
        Ok(UserRead::Hit(()) | UserRead::Missing) => {}
        Ok(UserRead::WrongType) => {
          output.write_resp_error(cs::RESP_ERR_WRONG_TYPE);
          return Ok(true);
        }
        Ok(UserRead::Deferred) => return Ok(false),
        Err(_) => {
          output.write_resp_error(RESP_ERR_GENERIC);
          return Ok(true);
        }
      }
    }

    let result = match acc.finish() {
      Ok(dst) => {
        // C# maxBitmapLen > 0 才 SET；全缺失/全空源回 0 不写（dest 为存活
        // 向量键时同臂保留登记）
        let longest = dst.len();
        if longest > 0 {
          // dest 写共同体前置：RI 门先判（Blocked 已应答），过门后登记命中
          // 预清退再覆写（C# dest DELETE+SET 重试臂同终态）
          match ri_write_gate(store, dest_key, output) {
            RiWriteGate::Pass => {}
            RiWriteGate::Blocked => return Ok(true),
            RiWriteGate::Deferred => return Ok(false),
          }
          if let Some(vm) = vector {
            vm.delete_vector_set(store.session_prefix().as_slice(), dest_key);
          }
          match store.try_upsert_sync(dest_key, &dst) {
            Ok(Ok(_)) => longest as i64,
            Ok(Err(_)) => return Ok(false),
            Err(_) => {
              output.write_resp_error(RESP_ERR_GENERIC);
              return Ok(true);
            }
          }
        } else {
          0
        }
      }
      // C# GarnetException（源被吞并后 DIFF 单源）→ 通用错误应答
      Err(_) => {
        output.write_resp_error(RESP_ERR_GENERIC);
        return Ok(true);
      }
    };
    output.write_resp_int(result);
    Ok(true)
  }

  /// libs/server/Resp/Bitmap/BitmapCommands.cs:StringBitField
  /// libs/server/Storage/Session/MainStore/BitmapOps.cs:StringBitField
  ///
  /// BITFIELD key [GET e o] [SET e o v] [INCRBY e o inc] [OVERFLOW WRAP|SAT|FAIL]
  pub fn string_bit_field<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some((key, secondary_command_args, has_write_sub_commands)) =
      parse_bitfield_args(parse_state, output)
    else {
      return Ok(true);
    };

    if secondary_command_args.is_empty() {
      output.write_resp_array_len(0);
      return Ok(true);
    }

    if secondary_command_args.is_empty() {
      output.write_resp_array_len(0);
      return Ok(true);
    }

    self.string_bit_field_action(
      key,
      secondary_command_args,
      has_write_sub_commands,
      store,
      output,
    )
  }

  /// libs/server/Resp/Bitmap/BitmapCommands.cs:StringBitFieldReadOnly
  /// libs/server/Storage/Session/MainStore/BitmapOps.cs:StringBitFieldReadOnly
  ///
  /// BITFIELD_RO key [GET encoding offset [GET encoding offset] ...]
  pub fn string_bit_field_read_only<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some((key, secondary_command_args)) = parse_bitfield_ro_args(parse_state, output) else {
      return Ok(true);
    };

    if secondary_command_args.is_empty() {
      output.write_resp_array_len(0);
      return Ok(true);
    }

    self.string_bit_field_action(key, secondary_command_args, false, store, output)
  }

  /// libs/server/Resp/Bitmap/BitmapCommands.cs:StringBitFieldAction
  ///
  /// 执行已解析的位域子命令序列并按数组应答。C# 多子命令时经事务
  /// （写子命令排他锁）逐条 RMW；Rust 侧单会话直连，读一次、就地依次
  /// 应用、有写子命令时统一回写一次。对象键在数组长度写出前拦截，
  /// 整条命令仅回 WRONGTYPE（C# HandleFirstSubCommand 回卷 dcurr 覆写
  /// 数组长度的净效果）；首子命令错误短路语义经
  /// [`Self::handle_first_sub_command`] 返回值保留。
  /// 执行 BITFIELD 复合子命令；保留 _has_write_commands 以匹配 Garnet BitmapCommands.StringBitFieldAction 签名规范
  pub fn string_bit_field_action<'a, D: wdev::Device>(
    &mut self,
    key: &[u8],
    secondary_command_args: Vec<BitFieldCmdArgs>,
    _has_write_commands: bool,
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if secondary_command_args.is_empty() {
      output.write_resp_array_len(0);
      return Ok(true);
    }

    // 读改写原子窗口：跨「读位图快照—逐子命令算新值—写回」全程持本键桶排他闩
    //（BITFIELD 多子命令更须整段原子，杜绝同键并发丢更新；对标 C# ephemeral 闩
    // 跨 InternalRMW 全程）
    let Some(window) = store.try_rmw_window(key) else {
      return Ok(false);
    };
    // 初始值快照（C# 首子命令经事务 API 读；缺失键记 None）。双域判型
    // 先于数组长度写出：对象键（C# Read/RMW ValueIsObject → WrongType）
    // 整条命令只回错误帧
    let mut value: Option<Vec<u8>> = match read_user_sync(store, key, |v| v.to_vec()) {
      Ok(UserRead::Hit(v)) => Some(v),
      Ok(UserRead::Missing) => None,
      Ok(UserRead::WrongType) => {
        output.write_resp_error(cs::RESP_ERR_WRONG_TYPE);
        return Ok(true);
      }
      // 磁盘候选：降级
      Ok(UserRead::Deferred) => return Ok(false),
      Err(_) => {
        output.write_resp_error(RESP_ERR_GENERIC);
        return Ok(true);
      }
    };

    // 应答数组长度
    output.write_resp_array_len(secondary_command_args.len());

    let mut dirty = false;
    for (i, args) in secondary_command_args.iter().enumerate() {
      // 首子命令：数组长度已写、错误即整体短路（C# HandleFirstSubCommand）
      if i == 0 {
        if self.handle_first_sub_command(args, &mut value, &mut dirty, output)? {
          return Ok(true);
        }
        continue;
      }

      let is_get = args.secondary_command == BitFieldSecondaryCommand::Get;
      if is_get {
        match value.as_mut() {
          // NOTFOUND + GET → :0
          None => output.write_resp_int(0),
          Some(buf) => match bit_field_execute(args, buf) {
            Some((v, false)) => output.write_resp_int(v),
            // 只读 GET 不产生溢出
            _ => write_bitfield_nil(self, output),
          },
        }
      } else {
        // 写子命令：增长到位域所需长度后执行
        let need = new_block_alloc_length_from_type(args, 0) as usize;
        let buf = value.get_or_insert_with(|| Vec::with_capacity(need));
        if buf.len() < need {
          buf.resize(need, 0);
        }
        match bit_field_execute(args, buf) {
          Some((v, false)) => output.write_resp_int(v),
          Some((_, true)) => write_bitfield_nil(self, output),
          None => output.write_resp_error(RESP_ERR_GENERIC),
        }
        dirty = true;
      }
    }

    if let Some(buf) = dirty.then_some(value).flatten() {
      // RMW 语义写回（保留既有 key 级 TTL，对标 C# GetRMWModifiedFieldInfo）
      match window.try_rmw_sync(&buf) {
        Ok(Ok(_)) => {}
        // 回写降级
        Ok(Err(_)) => return Ok(false),
        Err(_) => output.write_resp_error(RESP_ERR_GENERIC),
      }
    }
    Ok(true)
  }

  /// libs/server/Resp/Bitmap/BitmapCommands.cs:HandleFirstSubCommand
  ///
  /// 首子命令特判：C# 须处理"数组长度已写但 WRONGTYPE 需回卷输出"；
  /// Rust 侧 WRONGTYPE 已在 [`Self::string_bit_field_action`] 读阶段拦下，
  /// 本层恒返回 false（保留签名以对齐调用形态）。
  pub fn handle_first_sub_command(
    &mut self,
    args: &BitFieldCmdArgs,
    value: &mut Option<Vec<u8>>,
    dirty: &mut bool,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let is_get = args.secondary_command == BitFieldSecondaryCommand::Get;
    if is_get {
      match value.as_mut() {
        // NOTFOUND + GET → :0
        None => output.write_resp_int(0),
        Some(buf) => match bit_field_execute(args, buf) {
          Some((v, false)) => output.write_resp_int(v),
          _ => write_bitfield_nil(self, output),
        },
      }
    } else {
      let need = new_block_alloc_length_from_type(args, 0) as usize;
      let buf = value.get_or_insert_with(|| Vec::with_capacity(need));
      if buf.len() < need {
        buf.resize(need, 0);
      }
      match bit_field_execute(args, buf) {
        Some((v, false)) => output.write_resp_int(v),
        Some((_, true)) => write_bitfield_nil(self, output),
        None => output.write_resp_error(RESP_ERR_GENERIC),
      }
      *dirty = true;
    }
    Ok(false)
  }
}

/// NetworkStringBitCount 的区间推导单源（快慢路径共用；解析失败时已写出错误
/// 应答并返回 None，返回 `(key, start, end, offset_type)`；缺省 0/-1/BYTE）
pub(crate) fn parse_bit_count_args<'p>(
  parse_state: &[&'p [u8]],
  output: &mut Vec<u8>,
) -> Option<(&'p [u8], i64, i64, u8)> {
  let count = parse_state.len();
  check_arg_count!(
    count == 1 || count == 3 || count == 4,
    output,
    "BITCOUNT",
    return None
  );
  let key = parse_state[0];

  // 缺省 start=0 / end=-1（全量）
  let mut start = 0i64;
  let mut end = -1i64;
  let mut use_bit_index = false;
  if count > 1 {
    let (Some(s), Some(e)) = (
      parse_state[1].try_parse_i64(),
      parse_state[2].try_parse_i64(),
    ) else {
      abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
      return None;
    };
    start = s;
    end = e;
    if count > 3 {
      let flag = parse_state[3];
      if flag.eq_ignore_ascii_case(b"BIT") {
        use_bit_index = true;
      } else if !flag.eq_ignore_ascii_case(b"BYTE") {
        abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
        return None;
      }
    }
  }
  Some((key, start, end, if use_bit_index { 0x1 } else { 0x0 }))
}

/// NetworkStringBitPosition 的推导产物（快慢路径共用）
pub(crate) struct BitPosArgs<'p> {
  pub(crate) key: &'p [u8],
  /// 查找位（0/1）
  pub(crate) search_for: u8,
  pub(crate) start_offset: i64,
  pub(crate) end_offset: i64,
  /// 0x0 BYTE / 0x1 BIT
  pub(crate) offset_type: u8,
  pub(crate) has_start_offset: bool,
  pub(crate) has_end_offset: bool,
}

/// NetworkStringBitPosition 的推导单源（快慢路径共用；解析失败时已写出错误
/// 应答并返回 None；bit 参数须单字符 '0'/'1'，缺省 start=0 / end=-1 / BYTE）
pub(crate) fn parse_bit_pos_args<'p>(
  parse_state: &[&'p [u8]],
  output: &mut Vec<u8>,
) -> Option<BitPosArgs<'p>> {
  check_arg_count!(parse_state, 2..=5, output, "BITPOS", return None);
  let count = parse_state.len();
  let key = parse_state[0];

  // bit 参数须为单字符 '0'/'1'
  let bit_slice = parse_state[1];
  if bit_slice.len() != 1 || (bit_slice[0] != b'0' && bit_slice[0] != b'1') {
    abort_with_error_message(output, cs::RESP_ERR_GENERIC_BIT_IS_NOT_INTEGER);
    return None;
  }
  let search_for = bit_slice[0] - b'0';

  // 依次为 start、end、[BIT|BYTE]（缺省 start=0 / end=-1 / BYTE）
  let mut start_offset = 0i64;
  let mut end_offset = -1i64;
  let mut offset_type = 0x0u8;
  let mut has_start_offset = false;
  let mut has_end_offset = false;
  if count > 2 {
    let Some(start) = parse_state[2].try_parse_i64() else {
      abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
      return None;
    };
    start_offset = start;
    has_start_offset = true;

    if count > 3 {
      let Some(end) = parse_state[3].try_parse_i64() else {
        abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
        return None;
      };
      end_offset = end;
      has_end_offset = true;

      if count > 4 {
        let flag = parse_state[4];
        if flag.eq_ignore_ascii_case(b"BIT") {
          offset_type = 0x1;
        } else if !flag.eq_ignore_ascii_case(b"BYTE") {
          abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
          return None;
        }
      }
    }
  }
  Some(BitPosArgs {
    key,
    search_for,
    start_offset,
    end_offset,
    offset_type,
    has_start_offset,
    has_end_offset,
  })
}

/// NetworkStringSetBit / NetworkStringGetBit 的推导单源（快慢路径共用；
/// 返回 `(key, offset, bit)`，bit 仅 SETBIT 语义有效）
pub(crate) fn parse_bit_args<'p>(
  cmd_name: &str,
  parse_state: &[&'p [u8]],
  output: &mut Vec<u8>,
  with_bit: bool,
) -> Option<(&'p [u8], i64, u8)> {
  if with_bit {
    check_arg_count!(parse_state, 3, output, cmd_name, return None);
  } else {
    check_arg_count!(parse_state, 2, output, cmd_name, return None);
  }
  let key = parse_state[0];
  let Some(offset) = parse_bit_offset(parse_state[1]) else {
    abort_with_error_message(output, cs::RESP_ERR_GENERIC_BITOFFSET_IS_NOT_INTEGER);
    return None;
  };
  let mut bit = 0u8;
  if with_bit {
    // C#：bit 参数须为单字符 '0'/'1'
    if !matches!(parse_state[2], b"0" | b"1") {
      abort_with_error_message(output, cs::RESP_ERR_GENERIC_BIT_IS_NOT_INTEGER);
      return None;
    }
    bit = parse_state[2][0] - b'0';
  }
  Some((key, offset, bit))
}

/// StringBitField 的子命令序列推导单源（快慢路径共用；OVERFLOW 末值后置全局
/// 生效，写子命令存在时返回 has_write=true）
pub(crate) fn parse_bitfield_args<'p>(
  parse_state: &[&'p [u8]],
  output: &mut Vec<u8>,
) -> Option<(&'p [u8], Vec<BitFieldCmdArgs>, bool)> {
  check_arg_count!(parse_state, 1.., output, "BITFIELD", return None);
  let key = parse_state[0];

  let mut is_overflow_type_set = false;
  let mut overflow_type = BitFieldOverflow::Wrap;
  let mut secondary_command_args: Vec<BitFieldCmdArgs> = Vec::new();
  let mut has_write_sub_commands = false;

  let mut curr_token_idx = 1usize;
  while curr_token_idx < parse_state.len() {
    let command = parse_state[curr_token_idx];
    curr_token_idx += 1;

    // OVERFLOW：校验并记录（覆盖既有策略，末值全局生效）
    if command.eq_ignore_ascii_case(b"OVERFLOW") {
      let Some(next) = parse_state.get(curr_token_idx) else {
        abort_with_error_message(output, RESP_ERR_INVALID_OVERFLOW_TYPE);
        return None;
      };
      let Some(parsed) = parse_bitfield_overflow_slice(next) else {
        abort_with_error_message(output, RESP_ERR_INVALID_OVERFLOW_TYPE);
        return None;
      };
      curr_token_idx += 1;
      overflow_type = parsed;
      is_overflow_type_set = true;
      continue;
    }

    // encoding（u<位宽> / i<位宽>）
    let Some(encoding_slice) = parse_state.get(curr_token_idx).copied() else {
      abort_with_error_message(output, RESP_ERR_INVALID_BITFIELD_TYPE);
      return None;
    };
    if parse_bitfield_encoding(encoding_slice).is_none() {
      abort_with_error_message(output, RESP_ERR_INVALID_BITFIELD_TYPE);
      return None;
    }
    curr_token_idx += 1;

    // offset（`#<n>` 倍乘或裸位偏移）
    let Some(offset_raw) = parse_state.get(curr_token_idx).copied() else {
      abort_with_error_message(output, cs::RESP_ERR_GENERIC_BITOFFSET_IS_NOT_INTEGER);
      return None;
    };
    let Some((type_info, offset)) = parse_bitfield_type_offset(encoding_slice, offset_raw) else {
      abort_with_error_message(output, cs::RESP_ERR_GENERIC_BITOFFSET_IS_NOT_INTEGER);
      return None;
    };
    curr_token_idx += 1;

    // GET 子命令取 encoding + offset
    if command.eq_ignore_ascii_case(b"GET") {
      secondary_command_args.push(BitFieldCmdArgs::new(
        BitFieldSecondaryCommand::Get,
        type_info,
        offset,
        0,
        BitFieldOverflow::Wrap as u8,
      ));
      continue;
    }

    // SET / INCRBY 再取 value/increment
    let op = if command.eq_ignore_ascii_case(b"SET") {
      BitFieldSecondaryCommand::Set
    } else if command.eq_ignore_ascii_case(b"INCRBY") {
      BitFieldSecondaryCommand::IncrBy
    } else {
      let err = format!(
        "ERR Bitfield command {} not supported",
        command.as_str_safe()
      );
      abort_with_error_message(output, &err);
      return None;
    };
    has_write_sub_commands = true;

    let Some(value_slice) = parse_state.get(curr_token_idx).copied() else {
      abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
      return None;
    };
    let Some(value) = value_slice.try_parse_i64() else {
      abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
      return None;
    };
    curr_token_idx += 1;

    secondary_command_args.push(BitFieldCmdArgs::new(
      op,
      type_info,
      offset,
      value,
      BitFieldOverflow::Wrap as u8,
    ));
  }

  // OVERFLOW 末值后置全局生效（含 OVERFLOW 之前入列的子命令）
  if is_overflow_type_set {
    for args in &mut secondary_command_args {
      args.overflow_type = overflow_type as u8;
    }
  }
  Some((key, secondary_command_args, has_write_sub_commands))
}

/// StringBitFieldReadOnly 的子命令序列推导单源（快慢路径共用；只读变体仅
/// 支持 GET）
pub(crate) fn parse_bitfield_ro_args<'p>(
  parse_state: &[&'p [u8]],
  output: &mut Vec<u8>,
) -> Option<(&'p [u8], Vec<BitFieldCmdArgs>)> {
  check_arg_count!(parse_state, 1.., output, "BITFIELD_RO", return None);
  let key = parse_state[0];
  let mut secondary_command_args: Vec<BitFieldCmdArgs> = Vec::new();

  let mut curr_token_idx = 1usize;
  while curr_token_idx < parse_state.len() {
    let command = parse_state[curr_token_idx];
    curr_token_idx += 1;

    // 只读变体仅支持 GET
    if !command.eq_ignore_ascii_case(b"GET") {
      abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
      return None;
    }

    // encoding
    let Some(encoding_slice) = parse_state.get(curr_token_idx).copied() else {
      abort_with_error_message(output, RESP_ERR_INVALID_BITFIELD_TYPE);
      return None;
    };
    if parse_bitfield_encoding(encoding_slice).is_none() {
      abort_with_error_message(output, RESP_ERR_INVALID_BITFIELD_TYPE);
      return None;
    }
    curr_token_idx += 1;

    // offset
    let Some(offset_raw) = parse_state.get(curr_token_idx).copied() else {
      abort_with_error_message(output, cs::RESP_ERR_GENERIC_BITOFFSET_IS_NOT_INTEGER);
      return None;
    };
    let Some((type_info, offset)) = parse_bitfield_type_offset(encoding_slice, offset_raw) else {
      abort_with_error_message(output, cs::RESP_ERR_GENERIC_BITOFFSET_IS_NOT_INTEGER);
      return None;
    };
    curr_token_idx += 1;

    secondary_command_args.push(BitFieldCmdArgs::new(
      BitFieldSecondaryCommand::Get,
      type_info,
      offset,
      0,
      BitFieldOverflow::Wrap as u8,
    ));
  }
  Some((key, secondary_command_args))
}
