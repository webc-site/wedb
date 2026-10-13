//! 位图族慢路径执行段（SETBIT/GETBIT/BITCOUNT/BITPOS/BITOP/BITFIELD；
//! 解析一律转调 bitmap_commands 的推导单源，应答形态与快路径逐字节一致）

use wbitmap::{
  BitFieldSecondaryCommand, BitOpAccumulator, BitmapOperation, bit_count_driver, bit_field_execute,
  bit_field_execute_ro, bit_pos_driver, get_bit, length_in_bytes, new_block_alloc_length_from_type,
  try_validate_bit_pos_offsets, update_bitmap,
};
use wdev::Device;
use wresp::{
  cmd_strings::{self as cs, RESP_ERR_GENERIC, RESP_ERR_WRONG_TYPE, abort_with_error_message},
  command::RespCommand,
  ext::RespVecExt,
};

use super::common::{clear_vector_registry, read_and_frame_quiet, read_cold_quiet};
use crate::{
  resp::{
    basic_commands::set::string_record_fits_page,
    bitmap::bitmap_commands::{parse_bit_args, parse_bit_count_args, parse_bit_pos_args},
    vector::vector_manager::VectorManager,
  },
  storage::session::{common::UserReadAsync, storage_session::StorageSession},
};

/// BITOP 键数上限（含 destkey，对齐 C# BitmapOps MaxKeys = 64）
const BITOP_KEYS_MAX: usize = 64;

/// 位图族慢路径执行段入口（快路径降级承接；解析一律转调 bitmap_commands
/// 的推导单源，应答形态与快路径逐字节一致）
pub(crate) async fn bitmap_slow(
  storage: &StorageSession<'_, impl Device>,
  cmd: RespCommand,
  parse_state: &[&[u8]],
  vector: Option<&VectorManager>,
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  use RespCommand as C;

  use crate::resp::bitmap::bitmap_commands::{
    bitfield_write_need, parse_bitfield_args, parse_bitfield_ro_args,
  };
  match cmd {
    C::Setbit | C::Getbit => {
      let (cmd_name, with_bit) = if cmd == C::Setbit {
        ("SETBIT", true)
      } else {
        ("GETBIT", false)
      };
      let Some((key, offset, bit)) = parse_bit_args(cmd_name, parse_state, output, with_bit) else {
        return Ok(());
      };
      // SETBIT 写臂：冷读与写回全程持本键桶排他闩（GETBIT 纯读不取窗）。
      // 增长字节数单点 wbitmap::length_in_bytes（C# BitmapManager.Length；
      // parse_bit_args 已保证 offset 合法，None 不可达）
      if with_bit {
        let need = length_in_bytes(offset).unwrap() as usize;
        // 与快臂 network_string_set_bit 同一前置判据源（[`string_record_fits_page`]）：
        // 超窗 need 在取窗与整值冷读回之前即按快臂 generic 同帧形收口，杜绝持闩期
        // 至多 512MB 盲目零填充分配与 RecordTooLarge 经 ? 上抛的次级落点分叉
        if !string_record_fits_page(&storage.batch, key, need) {
          output.write_resp_error(RESP_ERR_GENERIC);
          return Ok(());
        }
        let window = storage.batch.rmw_window(key).await.map_err(|_| ())?;
        // 位图写臂零入账（票 wnode-string-bitmap-found-notfound-accounting-
        // matrix：C# SETBIT 走 RMW_MainStore 零计数，与快臂 None 静默同账）
        let Some(old) = read_cold_quiet(storage, key, output).await? else {
          return Ok(());
        };
        // 增长补零到位（缺失键空旧值同形折叠，C# BitmapManager.Length 补零同构）
        let mut val = old.unwrap_or_default();
        if val.len() < need {
          val.resize(need, 0);
        }
        // 单点位写单源（libs/server/Resp/Bitmap/BitmapManager.cs:UpdateBitmap）
        let old_bit = update_bitmap(&mut val, offset, bit);
        storage.rmw_string(&window, &val).await.map_err(|_| ())?;
        output.write_resp_int(i64::from(old_bit));
      } else {
        // 纯读臂（GETBIT）：闭包内按偏移直接取位（单点 wbitmap::get_bit，
        // 对标 BitmapManager.GetBit）；C# NOTFOUND → :0；零入账（C# 走
        // Read_MainStore 零计数，票 wnode-string-bitmap-found-notfound-
        // accounting-matrix）
        read_and_frame_quiet(
          storage,
          key,
          |val| get_bit(offset, val),
          output,
          |out, bit| out.write_resp_int(i64::from(bit)),
          |out| out.write_resp_int(0),
        )
        .await?;
      }
      Ok(())
    }
    C::Bitcount => {
      let Some((key, start, end, offset_type)) = parse_bit_count_args(parse_state, output) else {
        return Ok(());
      };
      // C# NOTFOUND → :0；零入账（C# 走 Read_MainStore 零计数，票
      // wnode-string-bitmap-found-notfound-accounting-matrix）
      read_and_frame_quiet(
        storage,
        key,
        |val| bit_count_driver(start, end, offset_type, val, val.len() as i64),
        output,
        |out, total| out.write_resp_int(total),
        |out| out.write_resp_int(0),
      )
      .await?;
      Ok(())
    }
    C::Bitpos => {
      let Some(args) = parse_bit_pos_args(parse_state, output) else {
        return Ok(());
      };
      // 区间越界直接 -1（快路径同判，单点 try_validate_bit_pos_offsets）
      if try_validate_bit_pos_offsets(
        args.start_offset,
        args.end_offset,
        args.offset_type,
        args.has_start_offset,
        args.has_end_offset,
      ) {
        output.write_resp_int(-1);
        return Ok(());
      }
      let search_for = args.search_for;
      // 零入账（C# 走 Read_MainStore 零计数，票 wnode-string-bitmap-found-
      // notfound-accounting-matrix）
      read_and_frame_quiet(
        storage,
        args.key,
        |val| {
          bit_pos_driver(
            val,
            val.len() as i64,
            args.start_offset,
            args.end_offset,
            search_for,
            args.offset_type,
            // Redis end_given 唯一真源透传（与快臂同源 parse_bit_pos_args）
            args.has_end_offset,
          )
        },
        output,
        |out, pos| out.write_resp_int(pos),
        // C# NOTFOUND：找 0 回 0，找 1 回 -1
        |out| {
          out.extend_from_slice(if search_for == 0 {
            cs::RESP_RETURN_VAL_0
          } else {
            cs::RESP_RETURN_VAL_N1
          });
        },
      )
      .await?;
      Ok(())
    }
    C::BitopAnd | C::BitopOr | C::BitopXor | C::BitopNot | C::BitopDiff => {
      let bit_op = match cmd {
        C::BitopAnd => BitmapOperation::And,
        C::BitopOr => BitmapOperation::Or,
        C::BitopXor => BitmapOperation::Xor,
        C::BitopNot => BitmapOperation::Not,
        _ => BitmapOperation::Diff,
      };
      slow_bit_operation(storage, bit_op, parse_state, vector, output).await
    }
    C::Bitfield | C::BitfieldRo => {
      // has_write 对标 C# StringBitFieldAction 的 hasWriteCommands：BITFIELD_RO 恒
      // false；BITFIELD 依解析出的写子命令标志（SET / INCRBY 才有副作用）
      let (key, secondary_command_args, has_write) = if cmd == C::Bitfield {
        let Some((key, args, has_write)) = parse_bitfield_args(parse_state, output) else {
          return Ok(());
        };
        (key, args, has_write)
      } else {
        let Some((key, args)) = parse_bitfield_ro_args(parse_state, output) else {
          return Ok(());
        };
        (key, args, false)
      };
      if secondary_command_args.is_empty() {
        output.write_resp_array_len(0);
        return Ok(());
      }
      let resp_version = storage.resp_version;
      // 与快臂 string_bit_field_action 同一前置判据源（[`string_record_fits_page`]）：
      // 写子命令终值长上界超页即在取窗与整值冷读回之前按快臂 generic 同帧形收口，
      // 杜绝持闩期至多 512MB 盲目零填充分配与 RecordTooLarge 经 ? 上抛的次级落点
      // 分叉；纯读形态（全 GET 与 BITFIELD_RO）不取窗无增长义务，不入本门射程
      if has_write
        && !string_record_fits_page(
          &storage.batch,
          key,
          bitfield_write_need(&secondary_command_args),
        )
      {
        output.write_resp_error(RESP_ERR_GENERIC);
        return Ok(());
      }
      // 仅含写子命令时才持本键桶排他闩贯穿「冷读快照—逐子命令算新值—写回」全程
      //（与同步臂 string_bit_field_action 同窗，对标 C# LockType.Exclusive）；纯读
      //（全 GET 的 BITFIELD 及 BITFIELD_RO）走一次性快照读，不取排他闩（对标
      // C# LockType.Shared），消除并发只读互抢排他闩的串行化与误排队
      let window = if has_write {
        Some(storage.batch.rmw_window(key).await.map_err(|_| ())?)
      } else {
        None
      };
      // 零入账（C# BITFIELD/BITFIELD_RO 走 RMW_MainStore / Read_MainStore
      // 零计数，与快臂 None 静默同账，票 wnode-string-bitmap-found-notfound-
      // accounting-matrix）
      let Some(mut value) = read_cold_quiet(storage, key, output).await? else {
        return Ok(());
      };
      // 应答数组段起点（存储硬失败撤帧锚，快臂 reply_start 同款撤回形态）
      let reply_start = output.len();
      output.write_resp_array_len(secondary_command_args.len());
      let mut dirty = false;
      for args in &secondary_command_args {
        let is_get = args.secondary_command == BitFieldSecondaryCommand::Get;
        if is_get {
          match value.as_mut() {
            // NOTFOUND + GET → :0
            None => output.write_resp_int(0),
            Some(buf) => {
              // 执行核随命令形态二选一（对标 C# BITFIELD → BitFieldExecute、
              // BITFIELD_RO → BitFieldExecute_RO）
              let ans = if has_write {
                bit_field_execute(args, buf)
              } else {
                bit_field_execute_ro(args, buf).map(|v| (v, false))
              };
              match ans {
                Some((v, false)) => output.write_resp_int(v),
                // 只读 GET 不产生溢出
                _ => output.write_resp_null_ver(resp_version),
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
            Some((_, true)) => output.write_resp_null_ver(resp_version),
            None => output.write_resp_error(RESP_ERR_GENERIC),
          }
          dirty = true;
        }
      }
      // RMW 语义写回（保留既有 key 级 TTL，有写子命令时统一一次）。
      // 存储硬失败先整段撤回数组应答再上抛（慢臂外壳补单条存储错误帧），
      // 杜绝完整数组后悬挂第二应答
      if let Some(buf) = dirty.then_some(value).flatten() {
        match window {
          Some(window) => {
            if storage.rmw_string(&window, &buf).await.is_err() {
              output.truncate(reply_start);
              return Err(());
            }
          }
          // dirty 只在写子命令下置位，写子命令只属 BITFIELD 臂，缺窗即接线缺陷，
          // 宁回错误帧也不走无窗盲写
          None => {
            output.truncate(reply_start);
            output.write_resp_error(RESP_ERR_GENERIC);
          }
        }
      }
      Ok(())
    }
    _ => {
      // 分派表漏接线信号：本臂只应承接位图族，其余落此即缺陷
      cs::write_error_raw(output, cs::RESP_ERR_ASYNC_REQUIRED);
      Ok(())
    }
  }
}

/// NetworkStringBitOperation 的异步对偶（BITOP AND/OR/XOR/NOT/DIFF；
/// 逐源异步读折叠 + dest 写共同体，语义对齐快路径
/// `network_string_bit_operation`：源键命中向量登记或对象域即整体
/// WRONGTYPE 零写，dest 命中登记在有源命中时预清退再写）
async fn slow_bit_operation(
  storage: &StorageSession<'_, impl Device>,
  bit_op: BitmapOperation,
  parse_state: &[&[u8]],
  vector: Option<&VectorManager>,
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  use wresp::cmd_strings::{
    RESP_ERR_BITOP_DIFF_TWO_SOURCE_KEYS_REQUIRED, RESP_ERR_BITOP_KEY_LIMIT,
    RESP_ERR_BITOP_NOT_SINGLE_SOURCE_KEY, RESP_ERR_WRONG_NUMBER_OF_ARGUMENTS,
  };
  let count = parse_state.len();
  // 参数过少（parse_state = [destkey, srckey...]）
  if count < 2 {
    abort_with_error_message(output, RESP_ERR_WRONG_NUMBER_OF_ARGUMENTS);
    return Ok(());
  }
  // DIFF 至少两个源
  if bit_op == BitmapOperation::Diff && count < 3 {
    abort_with_error_message(output, RESP_ERR_BITOP_DIFF_TWO_SOURCE_KEYS_REQUIRED);
    return Ok(());
  }
  // NOT 为一元：恰一个源键
  if bit_op == BitmapOperation::Not && count > 2 {
    abort_with_error_message(output, RESP_ERR_BITOP_NOT_SINGLE_SOURCE_KEY);
    return Ok(());
  }
  // 源键上限（含 destkey 共 [`BITOP_KEYS_MAX`]）
  if count > BITOP_KEYS_MAX {
    abort_with_error_message(output, RESP_ERR_BITOP_KEY_LIMIT);
    return Ok(());
  }
  let dest_key = parse_state[0];

  // dest 读改写窗口先于逐源折叠建立、跨「折叠读 → 求值 → 落笔」全程持有（与
  // 快路径 network_string_bit_operation 同一窗口契约，对标 C# BitmapOps.cs
  // StringBitOperation keys[0] Exclusive 自任何读前即建立罩至 dest SET 全程）：
  // 慢路径折叠逐源 await 为结构化让出面，dest∈srcs 自指形的窗内自读须与并发
  // 持窗写者互斥，否则旧折叠视图尾段盲写顶掉窗内已提交写（非可串行化）
  let _window = storage.batch.rmw_window(dest_key).await.map_err(|_| ())?;

  // 逐源回调折叠（快路径同步段命中即弃切片，与快路径 network_string_bit_operation
  // 同构零克隆）：会话前缀循环外单次外提交带前缀读口，逐源零前缀重算；缺失键
  // 以零长切片参与折叠（Redis bitops.c:1294-1301 缺失=零长串；C#
  // BitmapOps.cs:131-132 NOTFOUND continue 把缺失键丢出折叠数组系上游缺形，
  // 不采）；源键命中向量登记或对象域（信封 / Meta 升阶）
  // 即整体 WRONGTYPE 短路（dest 尚未写入，无副作用）
  let prefix = storage.batch.session_prefix();
  let mut acc = BitOpAccumulator::new(bit_op);
  for src_key in &parse_state[1..] {
    if vector.is_some_and(|vm| vm.read_stored_index(prefix.as_slice(), src_key).is_some()) {
      output.write_resp_error(RESP_ERR_WRONG_TYPE);
      return Ok(());
    }
    match storage
      .read_user_with_prefix(prefix.as_slice(), src_key, |v| acc.fold(v))
      .await
      .map_err(|_| ())?
    {
      UserReadAsync::Hit(()) => {}
      // 缺失源以零长切片参与折叠（Redis bitops.c:1294-1301）：AND 零串定短
      // 使 finish 清尾臂产出全零结果、OR/XOR 零恒等、DIFF 保住参数位次；
      // 簿记面零改动（逐源入账在读口内，票
      // wnode-string-bitmap-found-notfound-accounting-matrix 收口形）
      UserReadAsync::Missing => acc.fold(&[]),
      UserReadAsync::WrongType => {
        output.write_resp_error(RESP_ERR_WRONG_TYPE);
        return Ok(());
      }
    }
  }

  let result = match acc.finish() {
    Ok(dst) => {
      // Redis bitops.c:1607-1618 尾部：maxlen>0 才 setKey，否则 dbDelete
      // 删 dest，应答恒 maxlen；C# BitmapOps.cs:131-132 NOTFOUND continue
      // + keysFound==0 臂（:204-209）回 0 不触 dest 系上游缺形，本票按
      // Redis 标准回改（deviations 不登记）
      let longest = dst.len();
      if longest > 0 {
        // dest 写共同体前置 RI 门（异步对偶：存活 RangeIndex 上拒写）+
        // 登记预清退（C# dest DELETE+SET 重试臂同终态）；静默口——dest
        // 非计数对象（C# ReadWithUnsafeContext 仅逐源计数，BitmapOps.cs:
        // 44），簿记档 gate 读虚增 1 帧会使本臂逐源 N 帧变 N+1、与快臂
        // 逐源补账失联（票 wnode-string-bitmap-found-notfound-accounting-
        // matrix）
        if storage
          .ri_write_gate_quiet(dest_key)
          .await
          .map_err(|_| ())?
        {
          output.write_resp_error(RESP_ERR_WRONG_TYPE);
          return Ok(());
        }
        clear_vector_registry(storage, vector, dest_key).await;
        // dest 落笔续用折叠前已建立的本键读改写窗口（`_window` 在手至本函数
        // 返回），盲写与折叠读同闩域互斥（票 zcode-r32-rmwmatrix 立项一 + 本票
        // dest 窗罩折叠读算全程，对位 C# BitmapOps.cs dest SET 记录闩内落笔）
        storage
          .upsert_string(dest_key, &dst)
          .await
          .map_err(|_| ())?;
        longest as i64
      } else {
        // 空结果删 dest（Redis bitops.c:1613-1618 dbDelete）：delete_string
        // 用户键删除单点（本文件 SET_Conditional 对象键臂同口先例）同步快删
        // + 异步降级一体闭环，不开第二张锁表；dest 为存活向量登记键经降级臂
        // batch.delete → delete_miss_hook 清退（wkv collection 层删除单点，
        // 命中登记视同删除成功）；dest 本不存在 Ok(false) 仍回 0（Redis
        // dbDelete 缺席键无副作用）
        storage.delete_string(dest_key).await.map_err(|_| ())?;
        0
      }
    }
    // DIFF 单命中 Err 防御位（wbitmap finish 臂，缺失源入折叠后此形
    // 不可达）→ 通用错误应答
    Err(_) => {
      output.write_resp_error(RESP_ERR_GENERIC);
      return Ok(());
    }
  };
  output.write_resp_int(result);
  Ok(())
}
