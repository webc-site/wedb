//! ZSCAN 族扫描输入解析（对标 libs/server/Objects/Types/GarnetObjectBase.cs
//! 的 ReadScanInput 段与 out 参数组；C# 承载于 GarnetObjectBase 抽象基类，
//! rust 侧解析不依赖对象实例，落为自由函数 + 借用结构）
//!
//! 自研依据: 扫描输入游标（C# 对应 test/standalone/Garnet.test/RespScanCommandsTests.cs 游标面）

use wbase::num::{strict_i32, strict_i64};
use wresp::{
  cmd_strings::{
    COUNT, MATCH, NOVALUES, RESP_ERR_GENERIC_INVALIDCURSOR, RESP_ERR_GENERIC_SYNTAX_ERROR,
    RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER,
  },
  ext::{RespVecExt, backfill_resp_frame_head, reserve_resp_frame_head, resp_frame_head_len},
};

/// ZSCAN 族扫描输入参数（ReadScanInput 解析产物，pattern 零拷贝借用）
///
/// libs/server/Objects/Types/GarnetObjectBase.cs:ReadScanInput out 参数组
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ScanInput<'a> {
  pub cursor: i64,
  pub pattern: &'a [u8],
  pub count: i64,
  pub is_no_value: bool,
}

/// 解析 ZSCAN 族输入单点（HSCAN/SSCAN/ZSCAN 三对象共用）：光标 /
/// MATCH pattern / COUNT n / NOVALUES
///
/// libs/server/Objects/Types/GarnetObjectBase.cs:ReadScanInput
/// （COUNT 无条件钳制到 limit_count_in_output，对标 C# countInInput >
/// limitCountInOutput；解析失败返回错误文本，由调用方写 RESP 错误）
pub fn read_scan_input<'a>(
  args: &[&'a [u8]],
  limit_count_in_output: i32,
) -> Result<ScanInput<'a>, &'static [u8]> {
  let mut result = ScanInput {
    cursor: 0,
    pattern: &[],
    count: 10,
    is_no_value: false,
  };

  let Some(cursor) = args
    .first()
    .copied()
    .and_then(strict_i64)
    .filter(|c| *c >= 0)
  else {
    return Err(RESP_ERR_GENERIC_INVALIDCURSOR.as_bytes());
  };
  result.cursor = cursor;

  let token_count = args.len();
  let mut curr_token_idx = 1;
  while curr_token_idx < token_count {
    let param = args[curr_token_idx];
    curr_token_idx += 1;

    if param.eq_ignore_ascii_case(MATCH) {
      if curr_token_idx >= token_count {
        return Err(RESP_ERR_GENERIC_SYNTAX_ERROR.as_bytes());
      }
      result.pattern = args[curr_token_idx];
      curr_token_idx += 1;
    } else if param.eq_ignore_ascii_case(COUNT) {
      if curr_token_idx >= token_count {
        return Err(RESP_ERR_GENERIC_SYNTAX_ERROR.as_bytes());
      }
      match strict_i32(args[curr_token_idx]) {
        Some(c) => {
          curr_token_idx += 1;
          // 无条件钳制单轮数量（对标 C# countInInput > limitCountInOutput）
          result.count = i64::from(c).min(i64::from(limit_count_in_output));
        }
        None => return Err(RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER.as_bytes()),
      }
    } else if param.eq_ignore_ascii_case(NOVALUES) {
      result.is_no_value = true;
    }
    // C# 三支链无 else：未识别词元已在上文整词消费并自增下标，直接跳过继续
  }

  Ok(result)
}

use wresp::resp_memory_writer::RespWriter;

use crate::resp::output::ObjectOutput;

/// Scan 输入解析 + 输出回写：HSCAN/SSCAN 共用（对应 C# GarnetObjectBase 的
/// 基类角色，抽象 Scan 以闭包注入；sortedset 因分值可空项走独立实现）
///
/// libs/server/Objects/Types/GarnetObjectBase.cs:Scan
///
/// 落帧序（帧头预留-回填直写，同族先例 SMEMBERS/HGETALL/LRANGE）：外层
/// `*2` 头直写 → 游标帧位预留（游标终值 <= 集合总量 `total` 恒成立，位宽按
/// `digits(total)` 上界估算）→ 条目数组头预留 → 扫描 emit 回调内
/// `write_resp_bulk_string` 逐条直写 → [`backfill_resp_frame_head`] 以实际
/// 出帧计数回填条目头、以游标终值回填游标帧位（回填序先条目头后游标位：
/// 后者搬移的是已定型的条目帧整段；write_head 复用
/// [`RespVecExt::write_resp_int_as_bulk_string`] 与
/// [`RespVecExt::write_resp_array_len`] 既有单源，位宽不等走 `copy_within`
/// 单次搬移，不另写第二套预留-回填）。空集臂 n=0 回填写 `*0\r\n` 与现状
/// `RESP_EMPTYLIST` 逐字节同。对位 C# out List 引用收集的零拷贝直写形态
pub fn scan_operate_shared(
  args: &[&[u8]],
  limit_count_in_output: i32,
  output: &mut ObjectOutput<'_>,
  total: usize,
  do_scan: impl FnOnce(i64, i64, &[u8], bool, &mut dyn FnMut(&[u8])) -> i64,
) {
  // 参数解析走 GarnetObjectBase::ReadScanInput 单点（错误直接写 RESP 错误）
  let params = match read_scan_input(args, limit_count_in_output) {
    Ok(params) => params,
    Err(msg) => {
      RespWriter::new_ref(output.payload).write_error_bytes(msg);
      return;
    }
  };

  let payload = &mut *output.payload;
  payload.write_resp_array_len(2);
  // 游标帧位预留：游标终值 <= 集合总量恒成立（分页截断恒 cursor+expired <
  // total，收敛臂归零），位宽按 digits(total) 上界估算
  let write_cursor = |buf: &mut Vec<u8>, n: usize| buf.write_resp_int_as_bulk_string(n);
  let cursor_reserved = resp_frame_head_len(total, write_cursor);
  let cursor_base = reserve_resp_frame_head(payload, cursor_reserved);
  // 条目数组头预留：成对形态（hash 非 NOVALUES）至多 2×total 项
  let items_upper = if params.is_no_value {
    total
  } else {
    total.saturating_mul(2)
  };
  let write_items = |buf: &mut Vec<u8>, n: usize| buf.write_resp_array_len(n);
  let items_reserved = resp_frame_head_len(items_upper, write_items);
  let items_base = reserve_resp_frame_head(payload, items_reserved);

  // 扫描 emit 回调：借条目切片直写出帧（零逐条目堆物化），计数供条目头回填
  let mut n = 0usize;
  let cursor_output = do_scan(
    params.cursor,
    params.count,
    params.pattern,
    params.is_no_value,
    &mut |item| {
      payload.write_resp_bulk_string(item);
      n += 1;
    },
  );

  backfill_resp_frame_head(payload, items_base, items_reserved, n, write_items);
  backfill_resp_frame_head(
    payload,
    cursor_base,
    cursor_reserved,
    cursor_output as usize,
    write_cursor,
  );

  output.result1 = n as i64;
}

/// 自定义对象 COSCAN 组帧内核（同步/冷两臂共用单点）
///
/// libs/server/Objects/Types/GarnetObjectBase.cs:Scan（protected 组帧段）：
/// [`read_scan_input`] 解析（错误直接落 RESP 错误帧）→ 调对象抽象成员扫描
///（[`wcustom::CustomScanMembersFn`] 同构契约，经闭包注入解耦依赖方向）
/// 收集成员与出页游标 → `*2` + 游标 bulk + 条目数组（空集 `*0\r\n`，对标
/// C# WriteEmptyArray）；成员扫描 Err（C# NotImplementedException 的错误帧
/// 裁量，见 doc/zh/deviations.md）→ 错误帧收口，绝无空成功帧
///
/// C# 组帧无帧头预留技巧（RespMemoryWriter 直写），rust 同序直写；
/// result1 记发出成员数（C# output.result1 = items.Count）
pub fn custom_scan_operate(
  args: &[&[u8]],
  limit_count_in_output: i32,
  output: &mut ObjectOutput<'_>,
  scan_members: impl FnOnce(i64, i64, &[u8], bool) -> Result<(Vec<Vec<u8>>, i64), &'static [u8]>,
) {
  let params = match read_scan_input(args, limit_count_in_output) {
    Ok(params) => params,
    Err(msg) => {
      RespWriter::new_ref(output.payload).write_error_bytes(msg);
      return;
    }
  };
  let (items, cursor) = match scan_members(
    params.cursor,
    params.count,
    params.pattern,
    params.is_no_value,
  ) {
    Ok(out) => out,
    Err(msg) => {
      RespWriter::new_ref(output.payload).write_error_bytes(msg);
      return;
    }
  };
  let payload = &mut *output.payload;
  payload.write_resp_array_len(2);
  // 游标语义非负（各成员扫描实现的出页游标契约，C# WriteInt64AsBulkString 同）
  payload.write_resp_int_as_bulk_string(cursor);
  payload.write_resp_array_len(items.len());
  for item in &items {
    payload.write_resp_bulk_string(item);
  }
  output.result1 = items.len() as i64;
}

/// 扫描游标收敛单点判定（内存态 hash/zset 与分层态 `exec_tiered_scan` 共用）：
/// 本轮结束游标 `cursor`（起始游标 + 本轮已扫存活数）叠加到期垫数
/// `expired_keys_count` 后越过集合总量 `total`（含到期垫数）即归零收敛，
/// 否则维持续页游标。
///
/// 上游原型 libs/server/Objects/Hash/HashObject.cs 的 Scan 与
/// libs/server/Objects/SortedSet/SortedSetObject.cs 的 Scan 尾段为相等判定
/// `if (cursor + expiredKeysCount == hash.Count) cursor = 0;`。Scan 系纯只读路径
/// （C# Storage/Session/ObjectStore/Common.cs:ReadObjectStoreOperation 直入，
/// 绝不调用 DeleteExpiredItems），到期成员持续滞留并垫高 `Count`；当存活数 L <
/// 起始游标 start <= 含到期总数 N 时，全部存活条目下标恒 < start 被跳过、无产出，
/// cursor 停在 start，尾判定 `start + E == L + E` 退化为 `start == L`（恒假，
/// 因 start > L），游标无法归零 → 向客户端返回原游标死循环挂死。本仓将该固有
/// 死锁死角修复为 `>=`（数学完备性：正常未截断遍历恒 cursor + expired == total，
/// 两判等价；分页 COUNT 截断恒 cursor + expired < total，两判均不命中维持续页；
/// 唯死锁死角下 `>=` 补足归零），内存态与分层态据此收敛同口径。
///
/// 属偏离上游继承缺陷的刻意修复，已登记 doc/zh/deviations.md。
#[inline]
pub const fn scan_converge_cursor(cursor: i64, expired_keys_count: i64, total: i64) -> i64 {
  if cursor + expired_keys_count >= total {
    0
  } else {
    cursor
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_read_scan_input() {
    // 正常混合大小写解析
    let scan = read_scan_input(
      &[
        b"10".as_slice(),
        b"mAtCh",
        b"abc*",
        b"cOuNt",
        b"20",
        b"nOvAlUeS",
      ],
      100,
    )
    .unwrap();
    assert_eq!(scan.cursor, 10);
    assert_eq!(scan.pattern, b"abc*");
    assert_eq!(scan.count, 20);
    assert!(scan.is_no_value);

    // 未知项被整词跳过（C# ReadScanInput 三支链无 else）
    let scan = read_scan_input(&[b"0".as_slice(), b"UNKNOWN_OPTION"], 100).unwrap();
    assert_eq!(scan.cursor, 0);
    assert!(scan.pattern.is_empty());
    assert_eq!(scan.count, 10);

    // 跳过未知项不影响其后选项解析
    let scan = read_scan_input(
      &[b"0".as_slice(), b"FOO", b"MATCH", b"h*", b"COUNT", b"5"],
      100,
    )
    .unwrap();
    assert_eq!(scan.pattern, b"h*");
    assert_eq!(scan.count, 5);

    // MATCH 缺参
    assert_eq!(
      read_scan_input(&[b"0".as_slice(), b"match"], 100).unwrap_err(),
      RESP_ERR_GENERIC_SYNTAX_ERROR.as_bytes()
    );

    // 未知项后接缺参选项仍报语法错误
    assert_eq!(
      read_scan_input(&[b"0".as_slice(), b"FOO", b"COUNT"], 100).unwrap_err(),
      RESP_ERR_GENERIC_SYNTAX_ERROR.as_bytes()
    );

    // COUNT 缺参
    assert_eq!(
      read_scan_input(&[b"0".as_slice(), b"count"], 100).unwrap_err(),
      RESP_ERR_GENERIC_SYNTAX_ERROR.as_bytes()
    );

    // COUNT 非整数
    assert_eq!(
      read_scan_input(&[b"0".as_slice(), b"count", b"xyz"], 100).unwrap_err(),
      RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER.as_bytes()
    );

    // 负游标或非整数游标
    assert_eq!(
      read_scan_input(&[b"-5".as_slice()], 100).unwrap_err(),
      RESP_ERR_GENERIC_INVALIDCURSOR.as_bytes()
    );

    assert_eq!(
      read_scan_input(&[b"not_a_num".as_slice()], 100).unwrap_err(),
      RESP_ERR_GENERIC_INVALIDCURSOR.as_bytes()
    );
  }
}
