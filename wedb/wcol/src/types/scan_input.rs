//! ZSCAN 族扫描输入解析（对标 libs/server/Objects/Types/GarnetObjectBase.cs
//! 的 ReadScanInput 段与 out 参数组；C# 承载于 GarnetObjectBase 抽象基类，
//! rust 侧解析不依赖对象实例，落为自由函数 + 借用结构）
//!
//! 自研依据: 扫描输入游标（C# 对应 test/standalone/Garnet.test/RespScanCommandsTests.cs 游标面）

use wbase::{
  eq_ascii_case_const,
  glob::glob_match,
  num::{strict_i32, strict_i64},
};
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

/// 缺省单轮扫描页大小（C# ReadScanInput 未给 COUNT 时的缺省 10）
const DEFAULT_SCAN_COUNT: i64 = 10;

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
    count: DEFAULT_SCAN_COUNT,
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

    // 词元按长度分派（房内范式 parse_utils.rs / bitfield/parse.rs）：MATCH 与
    // COUNT 同长同臂内序判，NOVALUES 独臂；C# 三支链无 else——未识别词元已在
    // 上文整词消费并自增下标，直接跳过继续
    match param.len() {
      5 => {
        if eq_ascii_case_const(param, MATCH) {
          if curr_token_idx >= token_count {
            return Err(RESP_ERR_GENERIC_SYNTAX_ERROR.as_bytes());
          }
          result.pattern = args[curr_token_idx];
          curr_token_idx += 1;
        } else if eq_ascii_case_const(param, COUNT) {
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
        }
      }
      8 if eq_ascii_case_const(param, NOVALUES) => result.is_no_value = true,
      _ => {}
    }
  }

  Ok(result)
}

use wresp::resp_memory_writer::RespWriter;

use crate::resp::output::ObjectOutput;

/// Scan 条目直写出帧器（sink 单点：bulk 直写 + 实际出帧计数，供条目头回填）。
/// 具体类型承接（无 dyn trait 对象），SCAN 每条目零间接
pub struct ScanEmitter<'a> {
  /// 应答帧负载缓冲（直写目标）
  payload: &'a mut Vec<u8>,
  /// 已发出条目计数
  n: usize,
}

impl ScanEmitter<'_> {
  #[inline]
  pub fn emit(&mut self, item: &[u8]) {
    self.payload.write_resp_bulk_string(item);
    self.n += 1;
  }
}

/// Scan 输入解析 + 输出回写：HSCAN/SSCAN 共用（对应 C# GarnetObjectBase 的
/// 基类角色，抽象 Scan 以闭包注入；sortedset 因分值可空项走独立实现）
///
/// libs/server/Objects/Types/GarnetObjectBase.cs:Scan
///
/// `do_scan` 的出帧回调为具体 [`ScanEmitter`]（sink 在本函数构造、由扫描闭包
/// 消费，闭包逐条目直调，无 trait 对象间接）。
///
/// 落帧序（帧头预留-回填直写，同族先例 SMEMBERS/HGETALL/LRANGE）：外层
/// `*2` 头直写 → 游标帧位预留（游标终值 <= 集合总量 `total` 恒成立，位宽按
/// `digits(total)` 上界估算）→ 条目数组头预留 → 扫描 emit 回调内
/// `write_resp_bulk_string` 逐条直写 → [`backfill_resp_frame_head`] 以实际
/// 出帧计数回填条目头、以游标终值回填游标帧位（回填序先条目头后游标位：
/// 后者搬移的是已定型的条目帧整段；write_head 复用
/// [`RespWriter::write_integer_as_bulk_string`] 与
/// [`RespVecExt::write_resp_array_len`] 既有单源，位宽不等走 `copy_within`
/// 单次搬移，不另写第二套预留-回填）。空集臂 n=0 回填写 `*0\r\n` 与现状
/// `RESP_EMPTYLIST` 逐字节同。对位 C# out List 引用收集的零拷贝直写形态
pub fn scan_operate_shared(
  args: &[&[u8]],
  limit_count_in_output: i32,
  output: &mut ObjectOutput<'_>,
  total: usize,
  do_scan: impl FnOnce(i64, i64, &[u8], bool, &mut ScanEmitter<'_>) -> i64,
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
  let write_cursor =
    |buf: &mut Vec<u8>, n: usize| RespWriter::new_ref(buf).write_integer_as_bulk_string(n);
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

  // 扫描 emit 出帧器：借条目切片直写出帧（零逐条目堆物化），计数供条目头回填
  let mut emitter = ScanEmitter { payload, n: 0 };
  let cursor_output = do_scan(
    params.cursor,
    params.count,
    params.pattern,
    params.is_no_value,
    &mut emitter,
  );
  let n = emitter.n;

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
/// GarnetObjectBase 的 Scan 面（protected 组帧段；主体对位锚留 [`scan_operate_shared`] 一处）：
/// [`read_scan_input`] 解析（错误直接落 RESP 错误帧）→ 调对象抽象成员扫描
///（`wcustom::CustomScanMembersFn` 同构契约，经闭包注入解耦依赖方向）
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
  RespWriter::new_ref(payload).write_integer_as_bulk_string(cursor);
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
/// （C# libs/server/Storage/Session/ObjectStore/Common.cs:ReadObjectStoreOperation 直入，
/// 绝不调用 DeleteExpiredItems），到期成员持续滞留并垫高 `Count`；当存活数 L <
/// 起始游标 start <= 含到期总数 N 时，全部存活条目下标恒 < start 被跳过、无产出，
/// cursor 停在 start，尾判定 `start + E == L + E` 退化为 `start == L`（恒假，
/// 因 start > L），游标无法归零 → 向客户端返回原游标死循环挂死。本仓将该固有
/// 死锁死角修复为 `>=`（数学完备性：正常未截断遍历恒 cursor + expired == total，
/// 两判等价；分页 COUNT 截断恒 cursor + expired < total，两判均不命中维持续页；
/// 唯死锁死角下 `>=` 补足归零），内存态与分层态据此收敛同口径。
///
/// 属偏离上游继承缺陷的刻意修复，已登记 doc/zh/deviations.md §201。
#[inline]
pub const fn scan_converge_cursor(cursor: i64, expired_keys_count: i64, total: i64) -> i64 {
  if cursor + expired_keys_count >= total {
    0
  } else {
    cursor
  }
}

/// HSCAN/SSCAN/ZSCAN 共享扫描内核（hash_object / set_object / sorted_set_object
/// 三 scan 的同构全链收敛单点）：迭代序遍历 → 过期垫数 → 起始
/// 游标跳过 → glob 匹配 → emitted 相等截断 → [`scan_converge_cursor`] 收敛。
///
/// `total` 为含到期总数；`count_limit` 为截断阈值——hash 侧成对形态先翻倍
///（NOVALUES 不翻倍），zset 侧恒 `count * 2`（成员 + 分值恒成对），set 侧单
/// 条目形态不翻倍且以恒存活判定 `|_| false` 接入（无成员级过期，垫数恒 0）；
/// `emit` 返回本条目发出项数（hash 1/2、zset 恒 2、set 恒 1）。相等判定（负
/// COUNT 恒不命中 → 全量遍历；count=0 首个未命中条目即停的上游怪癖）1:1 保留；
/// 极值点 i32 回绕不复刻（登记见 doc/zh/deviations.md §185，勿按 C# 回改）
pub fn scan_kernel<'a, V>(
  total: i64,
  start: i64,
  count_limit: i64,
  pattern: &[u8],
  entries: impl Iterator<Item = (&'a [u8], V)>,
  is_expired: impl Fn(&[u8]) -> bool,
  mut emit: impl FnMut(&[u8], V) -> usize,
) -> i64 {
  let mut cursor = start;

  if total < start {
    return 0;
  }

  let mut index = 0_i64;
  let mut expired_keys_count = 0_i64;
  // 发出条目计数（对位 C# items.Count 的截断比较口径）
  let mut emitted = 0_i64;

  for (member, value) in entries {
    if is_expired(member) {
      expired_keys_count += 1;
      continue;
    }

    if index < start {
      index += 1;
      continue;
    }

    if pattern.is_empty() || glob_match(pattern, member) {
      emitted += emit(member, value) as i64;
    }

    cursor += 1;

    // 相等判定截断（语义与怪癖契约见函数头）
    if emitted == count_limit {
      break;
    }
  }

  // 到达集合末尾则光标归零：经单点判定收敛（内存态与分层态同口径），
  // 修复上游 `==` 判定在存活数 < 起始游标 <= 含到期总数死角的原游标死锁
  scan_converge_cursor(cursor, expired_keys_count, total)
}
