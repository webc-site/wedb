//! 位图命令（SETBIT/GETBIT/BITCOUNT/BITPOS/BITOP/BITFIELD/BITFIELD_RO）
//!
//! 同步快路径：字符串域直读直写，磁盘候选等须异步裁决时返回 `Ok(false)`
//! 交调用方降级。位序对标 Redis/C#：bit 0 为首字节最高位。
//! C# 侧会话层经 `StringInput` 把参数传给存储回调；Rust 侧存储接口收敛到
//! 会话层直连，故 BITFIELD 子命令在解析期即固化为类型化参数。

use wbitmap::{
  BitFieldCmdArgs, BitFieldOverflow, BitFieldSecondaryCommand, BitOpAccumulator, BitmapOperation,
  bit_count_driver, bit_field_execute, bit_field_execute_ro, bit_pos_driver, get_bit,
  is_valid_bit_offset, length_in_bytes, new_block_alloc_length_from_type, parse_bitfield_encoding,
  parse_bitfield_overflow_slice, parse_bitfield_type_offset, parse_bitmap_offset_type,
  try_get_bitfield_secondary_command, try_validate_bit_pos_offsets, update_bitmap,
};
use wdev::Device;
use wkv::RmwGrow;
use wresp::{
  check_args::{check_arg_count, parse_i64_arg},
  cmd_strings::{
    self as cs, RESP_ERR_BITOP_DIFF_TWO_SOURCE_KEYS_REQUIRED, RESP_ERR_BITOP_KEY_LIMIT,
    RESP_ERR_BITOP_NOT_SINGLE_SOURCE_KEY, RESP_ERR_GENERIC, RESP_ERR_INVALID_BITFIELD_TYPE,
    RESP_ERR_INVALID_OVERFLOW_TYPE, RESP_ERR_WRONG_NUMBER_OF_ARGUMENTS, abort_with_error_message,
  },
  ext::{RespSliceExt, RespVecExt},
};

use super::super::{
  basic_commands::{RiWriteGate, ri_write_gate, string_record_fits_page},
  resp_server_session::RespServerSession,
  vector::vector_manager::VectorManager,
};
use crate::{
  read_user_or_bail,
  storage::session::common::{UserRead, read_user_sync, read_user_sync_with_prefix},
};

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
  pub fn network_string_set_bit<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // 参数推导走快慢路径共用单源 parse_bit_args（arity/offset/bit 校验与错误帧不变）
    let Some((key, offset, bit)) = parse_bit_args("SETBIT", parse_state, output, true) else {
      return Ok(true);
    };

    // 覆盖 offset 所需字节数（C# TrySetContentLengths(BitmapManager.Length(bOffset))；
    // parse_bit_args 已保证 offset 合法，None 不可达）
    let need = length_in_bytes(offset).unwrap() as usize;

    // 单页容量前置门（与 network_set_range 同一判据源 [`string_record_fits_page`]）：
    // need 超页即终值注定撞写侧 RecordTooLarge——取窗前按现终态同帧形收口，杜绝
    // 持闩期至多 512MB 盲目堆分配与全量零填充（票 zcode-r163c-bitopdst，对位 C#
    // BasicCommands.cs:NetworkSetRange :460-466 超大尺寸前置拒之动机扩展到位图族）
    if !string_record_fits_page(store, key, need) {
      output.write_resp_error(RESP_ERR_GENERIC);
      return Ok(true);
    }

    // 读改写原子窗口：跨「读位图—置位—写回」全程持本键桶排他闩，杜绝同键并发
    // 丢更新（对标 C# BasicSessionLocker 的 ephemeral 闩跨 InternalRMW 全程）
    let Some(window) = store.try_rmw_window(key) else {
      return Ok(false);
    };

    // 原位增长臂（对标 C# MainStore/RMWMethods.cs:InPlaceUpdater SETBIT 分支
    // :568-609）：byte_idx 未越槽时按需 cap[old_len..byte_idx+1].fill(0) 零初始化增长、
    // 置位并返回新长；槽位富余不足 / 记录非活 / 引擎侧错损一律回落全量臂
    let mut in_place_old_bit = 0u8;
    match window.try_grow_in_place(|cap, old_len| {
      let new_len = old_len.max(need);
      if new_len > cap.len() {
        return None;
      }
      if need > old_len {
        cap[old_len..need].fill(0);
      }
      in_place_old_bit = update_bitmap(&mut cap[..new_len], offset, bit);
      Some(new_len)
    }) {
      Ok(RmwGrow::InPlace(_)) => {
        output.write_resp_int(in_place_old_bit as i64);
        return Ok(true);
      }
      Ok(RmwGrow::Degrade) => return Ok(false),
      Ok(RmwGrow::Fallback) | Err(_) => {}
    }

    let mut val = read_user_or_bail!(
      read_user_sync(store, key, None, |v| {
        let mut vec = Vec::with_capacity(v.len().max(need));
        vec.extend_from_slice(v);
        vec
      }),
      output,
      Vec::with_capacity(need)
    );

    if val.len() < need {
      val.resize(need, 0);
    }
    // 单点位写单源（libs/server/Resp/Bitmap/BitmapManager.cs:UpdateBitmap）
    let old_bit = update_bitmap(&mut val, offset, bit);

    match window.try_rmw_sync(&val) {
      Ok(Ok(_)) => output.write_resp_int(old_bit as i64),
      Ok(Err(_)) => return Ok(false),
      Err(_) => output.write_resp_error(RESP_ERR_GENERIC),
    }
    Ok(true)
  }

  /// libs/server/Resp/Bitmap/BitmapCommands.cs:NetworkStringGetBit
  /// libs/server/Storage/Session/MainStore/BitmapOps.cs:StringGetBit
  pub fn network_string_get_bit<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // 参数推导走快慢路径共用单源 parse_bit_args（with_bit=false，bit 恒 0 不使用）
    let Some((key, offset, _)) = parse_bit_args("GETBIT", parse_state, output, false) else {
      return Ok(true);
    };

    // 闭包内按偏移直接取位（单点 wbitmap::get_bit，对标 BitmapManager.GetBit）：
    // 跳过全值拷贝，零分配
    let bit = read_user_or_bail!(
      read_user_sync(store, key, None, |val| get_bit(offset, val)),
      output,
      0
    );
    output.write_resp_int(i64::from(bit));
    Ok(true)
  }

  /// libs/server/Resp/Bitmap/BitmapCommands.cs:NetworkStringBitCount
  /// libs/server/Storage/Session/MainStore/BitmapOps.cs:StringBitCount
  ///
  /// 形态：BITCOUNT key [start end [BYTE|BIT]]；缺省与 BYTE 均为字节区间，
  /// BIT 为位区间（对标 C# 存储侧 arg1 仅在带第 4 参时生效）
  pub fn network_string_bit_count<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some((key, start, end, offset_type)) = parse_bit_count_args(parse_state, output) else {
      return Ok(true);
    };

    // 闭包内直接统计区间：跳过全值拷贝，零分配
    let total = read_user_or_bail!(
      read_user_sync(store, key, None, |val| {
        bit_count_driver(start, end, offset_type, val, val.len() as i64)
      }),
      output,
      0
    );
    output.write_resp_int(total);
    Ok(true)
  }

  /// libs/server/Resp/Bitmap/BitmapCommands.cs:NetworkStringBitPosition
  /// libs/server/Storage/Session/MainStore/BitmapOps.cs:StringBitPosition
  pub fn network_string_bit_position<'a, D: Device>(
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
    let pos = read_user_or_bail!(
      read_user_sync(store, key, None, |val| {
        bit_pos_driver(
          val,
          val.len() as i64,
          start_offset,
          end_offset,
          search_for,
          offset_type,
        )
      }),
      output,
      if search_for == 0 { 0 } else { -1 }
    );
    output.write_resp_int(pos);
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
  ///（C# keysFound/maxBitmapLen>0 同位）预清退再覆写（C# dest 走
  /// DELETE+SET 重试臂），全源缺失零写、登记保留。目的键写共同体前置
  /// [`ri_write_gate`]：存活 RangeIndex 上拒写，杜绝 String+Meta 双域幽灵
  ///（SETNX/RESTORE 的 RI 半边由 probe_alive Meta 折叠承接）；dest 本键读改写
  /// 窗口先于逐源折叠建立、跨「折叠读 → 求值 → 落笔」全程持有（对标 C#
  /// StringBitOperation keys[0] Exclusive 自读起罩至 dest SET，杜绝 dest∈srcs
  /// 自指形折叠读游离窗外被并发已提交写顶掉的非可串行化）。
  pub fn network_string_bit_operation<'a, D: Device>(
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

    // dest 读改写窗口先于逐源折叠建立，跨「折叠读 → 求值 → 落笔」全程持有：
    // 对标 C# BitmapOps.cs:StringBitOperation keys[0] 的 Exclusive 记录闩自任何
    // 读发生前即建立、罩至 dest SET 落笔全程（dest∈srcs 自指形窗内自读与 SETBIT
    // 臂 :81 同型先例）。折叠读若游离窗外，dest 上的并发已提交写会被旧折叠视图
    // 的尾段盲写顶掉（非可串行化）；失闩沿既有 Ok(false) 降级通道走慢路径同段
    // 持窗重放，不新建第二张锁表
    let Some(_window) = store.try_rmw_window(dest_key) else {
      return Ok(false);
    };
    // 逐源回调折叠：源切片借用进闭包即弃，仅持有一份最长源长度的目标缓冲
    // （C# PinnedSpanByte 源指针收集 + 单次 InvokeBitOperationUnsafe 的同
    // 语义流式化，不逐源克隆）；会话前缀循环外单次外提交带前缀读口，逐源
    // 零前缀重算；缺失键跳过（C# NOTFOUND continue）；任一源键命中向量登记
    // 或为对象键即整体 WRONGTYPE 短路（C# StringBitOperation 逐键 RecordType
    // 判定：dest 尚未写入，无副作用）
    //
    // 逐源「恰一帧」本地累加、收尾一次入账（票
    // wnode-string-bitmap-found-notfound-accounting-matrix，沿 do_network_mget
    // 先例 resp/array_commands.rs 逐键 None 静默 + 本地累加 + 收尾单点）：
    // 循环读口传 None 静默，命中 found / 缺席 notfound 本地累计，成功终态
    // 出帧前一次入账（对位 C# ReadWithUnsafeContext 逐源恰一帧，
    // MainStoreOps.cs:76/:81；dest 不计，C# 仅逐源计数）；WRONGTYPE 与一切
    // 降级出口（Ok(false)/Deferred/存储错误帧臂）零入账不出帧不清账——
    // 降级臂交慢臂逐源 read_user_with_prefix 唯一出口重放补账防双计
    // （票面裁定），存储错误帧臂零入账沿 getex「异常臂无 incr」先例
    let prefix = store.session_prefix();
    let mut acc = BitOpAccumulator::new(bit_op);
    let (mut found, mut notfound) = (0u64, 0u64);
    for src_key in &parse_state[1..] {
      if vector.is_some_and(|vm| vm.read_stored_index(prefix.as_slice(), src_key).is_some()) {
        output.write_resp_error(cs::RESP_ERR_WRONG_TYPE);
        return Ok(true);
      }
      match read_user_sync_with_prefix(store, prefix.as_slice(), src_key, None, |v| acc.fold(v)) {
        Ok(UserRead::Hit(())) => found += 1,
        Ok(UserRead::Missing) => notfound += 1,
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
          // 整体降级慢路径真异步清退再覆写（C# dest DELETE+SET 重试臂同终态；
          // 登记写透 async 化后快路径不再同步清退）
          match ri_write_gate(store, dest_key, output) {
            RiWriteGate::Pass => {}
            RiWriteGate::Blocked => return Ok(true),
            RiWriteGate::Deferred => return Ok(false),
          }
          if vector.is_some_and(|vm| vm.read_stored_index(prefix.as_slice(), dest_key).is_some()) {
            return Ok(false);
          }
          // dest 落笔续用折叠前已建立的本键读改写窗口（`_window` 在手至本函数
          // 返回），盲写与折叠读同闩域互斥（票 zcode-r32-rmwmatrix 立项一 +
          // 本票 dest 窗罩折叠读算全程，对位 C# dest SET 记录闩内落笔）
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
    // 成功终态收尾一次入账（逐源恰一帧合计，do_network_mget 同机制；
    // 上方一切降级/错误帧臂已先行零入账收口，本点后不再有零入账出口）
    if let Some(metrics) = self.session_metrics.as_deref() {
      metrics.incr_total_found(found);
      metrics.incr_total_notfound(notfound);
    }
    output.write_resp_int(result);
    Ok(true)
  }

  /// libs/server/Resp/Bitmap/BitmapCommands.cs:StringBitField
  /// libs/server/Storage/Session/MainStore/BitmapOps.cs:StringBitField
  ///
  /// BITFIELD key [GET e o] [SET e o v] [INCRBY e o inc] [OVERFLOW WRAP|SAT|FAIL]
  pub fn string_bit_field<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // 空子命令序列的 `*0` 应答由 string_bit_field_action 首段守卫统一承接，此处不重复
    let Some((key, secondary_command_args, has_write_sub_commands)) =
      parse_bitfield_args(parse_state, output)
    else {
      return Ok(true);
    };
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
  pub fn string_bit_field_read_only<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // 空子命令序列的 `*0` 应答由 string_bit_field_action 首段守卫统一承接，此处不重复
    let Some((key, secondary_command_args)) = parse_bitfield_ro_args(parse_state, output) else {
      return Ok(true);
    };
    self.string_bit_field_action(key, secondary_command_args, false, store, output)
  }

  /// libs/server/Resp/Bitmap/BitmapCommands.cs:StringBitFieldAction
  ///
  /// 执行已解析的位域子命令序列并按数组应答。C# 多子命令时经事务
  ///（写子命令 LockType.Exclusive、纯读 LockType.Shared）逐条 RMW，单子命令直连
  /// storageApi 不提升事务；Rust 侧单会话直连，读一次快照、就地依次应用、有写子
  /// 命令时统一回写一次。对象键在数组长度写出前拦截，整条命令仅回 WRONGTYPE。
  /// `has_write_commands` 对标 C# 同名形参（BitmapCommands.cs:StringBitFieldAction
  /// 的 `hasWriteCommands ? LockType.Exclusive : LockType.Shared`）：纯读形态（BITFIELD_RO /
  /// 全 GET）走上方一次性快照读分支、执行核为只读签名 [`bit_field_execute_ro`]
  ///（对标 PrivateMethods InternalRead 的 RespCommand.BITFIELD_RO 臂 →
  /// BitmapManager.BitFieldExecute_RO）；含写子命令才持排他闩、执行核统一为写签名
  /// [`bit_field_execute`]（对标 PrivateMethods InternalRMW/InternalRead 的
  /// RespCommand.BITFIELD 臂）。
  pub fn string_bit_field_action<'a, D: Device>(
    &mut self,
    key: &[u8],
    secondary_command_args: Vec<BitFieldCmdArgs>,
    has_write_commands: bool,
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if secondary_command_args.is_empty() {
      output.write_resp_array_len(0);
      return Ok(true);
    }

    if !has_write_commands {
      // 纯读形态（BITFIELD_RO / 全 GET 选项）：零分配，闭包内按偏移直接取位
      let hit = read_user_or_bail!(
        read_user_sync(store, key, None, |v| {
          output.write_resp_array_len(secondary_command_args.len());
          for args in secondary_command_args.iter() {
            match bit_field_execute_ro(args, v) {
              Some(val) => output.write_resp_int(val),
              None => write_bitfield_nil(self, output),
            }
          }
          true
        }),
        output,
        false
      );

      if !hit {
        output.write_resp_array_len(secondary_command_args.len());
        for _ in &secondary_command_args {
          output.write_resp_int(0);
        }
      }
      return Ok(true);
    }

    // 单页容量前置门（与 network_set_range 臂同一判据源 [`string_record_fits_page`]）：
    // 写子命令终值长上界超页即整条命令注定撞写侧 RecordTooLarge——取窗前按
    // 现终态同帧形收口，杜绝持闩期至多 512MB 盲目堆分配与全量零填充
    if !string_record_fits_page(store, key, bitfield_write_need(&secondary_command_args)) {
      output.write_resp_error(RESP_ERR_GENERIC);
      return Ok(true);
    }

    // 读改写原子窗口：纯读形态已在上方分支返回，此处必然含写子命令，跨「读位图
    // 快照—逐子命令算新值—写回」全程持本键桶排他闩（对标 C# LockType.Exclusive），
    // 杜绝同键并发丢更新；取闩失败按既有降级通道转异步
    let Some(window) = store.try_rmw_window(key) else {
      return Ok(false);
    };
    // 初始值快照（C# 首子命令经事务 API 读；缺失键记 None）。双域判型
    // 先于数组长度写出：对象键（C# Read/RMW ValueIsObject → WrongType）
    // 整条命令只回错误帧
    let mut value: Option<Vec<u8>> = read_user_or_bail!(
      read_user_sync(store, key, None, |v| Some(v.to_vec())),
      output,
      None
    );

    // 应答数组长度
    output.write_resp_array_len(secondary_command_args.len());

    let mut dirty = false;
    for args in secondary_command_args.iter() {
      let is_get = args.secondary_command == BitFieldSecondaryCommand::Get;
      if is_get {
        match value.as_mut() {
          // NOTFOUND + GET → :0
          None => output.write_resp_int(0),
          Some(buf) => {
            // 写形态统一走 bit_field_execute（对标 C# BITFIELD → BitFieldExecute；
            // 纯读 RO 形态的 bit_field_execute_ro 臂在上方分支已返回）
            match bit_field_execute(args, buf) {
              Some((v, false)) => output.write_resp_int(v),
              // 只读 GET 不产生溢出
              _ => write_bitfield_nil(self, output),
            }
          }
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
      // dirty 仅在写子命令分支置位，此时必然已持窗（上方取闩为无条件）；
      // RMW 语义写回保留既有 key 级 TTL（对标 C# GetRMWModifiedFieldInfo）
      match window.try_rmw_sync(&buf) {
        Ok(Ok(_)) => {}
        // 回写降级
        Ok(Err(_)) => return Ok(false),
        Err(_) => output.write_resp_error(RESP_ERR_GENERIC),
      }
    }
    Ok(true)
  }
}

/// BITFIELD 写子命令终值长上界（快慢双臂单页门前置判据共用单源）：逐写子命令
/// [`new_block_alloc_length_from_type`] 取最大；旧值必已单页容纳，终值长
/// max(old_len, need) 超页当且仅当 need 超页。纯读 GET 子命令无增长义务，
/// 不参与计算（全 GET 形返回 0，恒过门）
pub(crate) fn bitfield_write_need(secondary_command_args: &[BitFieldCmdArgs]) -> usize {
  secondary_command_args
    .iter()
    .filter(|args| args.secondary_command != BitFieldSecondaryCommand::Get)
    .map(|args| new_block_alloc_length_from_type(args, 0) as usize)
    .max()
    .unwrap_or(0)
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

  // 缺省 start=0 / end=-1（全量）/ 单位 BYTE（0x0，与 BITPOS 同型直传）
  let mut start = 0i64;
  let mut end = -1i64;
  let mut offset_type = 0x0u8;
  if count > 1 {
    start = parse_i64_arg(parse_state[1], output)?;
    end = parse_i64_arg(parse_state[2], output)?;
    if count > 3 {
      let Some(t) = parse_bitmap_offset_type(parse_state[3]) else {
        abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
        return None;
      };
      offset_type = t;
    }
  }
  Some((key, start, end, offset_type))
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
    start_offset = parse_i64_arg(parse_state[2], output)?;
    has_start_offset = true;

    if count > 3 {
      end_offset = parse_i64_arg(parse_state[3], output)?;
      has_end_offset = true;

      if count > 4 {
        let Some(t) = parse_bitmap_offset_type(parse_state[4]) else {
          abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
          return None;
        };
        offset_type = t;
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

/// BITFIELD/BITFIELD_RO 子命令 `'encoding offset'` 词元组共段解析（两种命令形态
/// 同形单源）：成功时前进 `idx` 越过两词元并返回 `(type_info, offset)`；失败时
/// 写出对应错误帧（编码 → INVALID_BITFIELD_TYPE，偏移 → BITOFFSET_NOT_INTEGER）
/// 并返回 None
fn parse_bitfield_enc_offset(
  parse_state: &[&[u8]],
  idx: &mut usize,
  output: &mut Vec<u8>,
) -> Option<(u8, i64)> {
  // encoding（u<位宽> / i<位宽>）
  let Some(encoding_slice) = parse_state.get(*idx).copied() else {
    abort_with_error_message(output, RESP_ERR_INVALID_BITFIELD_TYPE);
    return None;
  };
  if parse_bitfield_encoding(encoding_slice).is_none() {
    abort_with_error_message(output, RESP_ERR_INVALID_BITFIELD_TYPE);
    return None;
  }
  *idx += 1;

  // offset（`#<n>` 倍乘或裸位偏移）
  let Some(offset_raw) = parse_state.get(*idx).copied() else {
    abort_with_error_message(output, cs::RESP_ERR_GENERIC_BITOFFSET_IS_NOT_INTEGER);
    return None;
  };
  let Some((type_info, offset)) = parse_bitfield_type_offset(encoding_slice, offset_raw) else {
    abort_with_error_message(output, cs::RESP_ERR_GENERIC_BITOFFSET_IS_NOT_INTEGER);
    return None;
  };
  *idx += 1;
  Some((type_info, offset))
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

    // encoding + offset（与 BITFIELD_RO 共段单源）
    let (type_info, offset) = parse_bitfield_enc_offset(parse_state, &mut curr_token_idx, output)?;

    let Some(op) = try_get_bitfield_secondary_command(command) else {
      let err = format!(
        "ERR Bitfield command {} not supported",
        command.as_str_safe()
      );
      abort_with_error_message(output, &err);
      return None;
    };

    if op == BitFieldSecondaryCommand::Get {
      secondary_command_args.push(BitFieldCmdArgs::new(
        op,
        type_info,
        offset,
        0,
        BitFieldOverflow::Wrap as u8,
      ));
      continue;
    }
    has_write_sub_commands = true;

    let value_slice = parse_state.get(curr_token_idx).copied().unwrap_or(&[]);
    let value = parse_i64_arg(value_slice, output)?;
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

    // encoding + offset（与 BITFIELD 共段单源）
    let (type_info, offset) = parse_bitfield_enc_offset(parse_state, &mut curr_token_idx, output)?;

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
