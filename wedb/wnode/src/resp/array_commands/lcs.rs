//! LCS 选项解析与出帧单源（快路径校验段与慢路径执行段共用）
//!
//! 对标 libs/server/Resp/ArrayCommands.cs:NetworkLCS 选项校验段与
//! libs/server/Storage/Session/MainStore/MainStoreOps.cs:LCSInternal 的
//! LEN/IDX/默认三形态应答装配。

use wbase::num::strict_i32;
use wdev::Device;
use wresp::{cmd_strings as cs, ext::RespVecExt};

use crate::storage::session::storage_session::StorageSession;

/// libs/server/Resp/CmdStrings.cs:RESP_ERR_LENGTH_AND_INDEXES
///
/// C# 常量不带 `ERR` 前缀（RespWriteUtils.TryWriteError 直加 `-` 成帧），
/// 1:1 保留 garnet quirk：应答为 `-If you want ... IDX.\r\n`
const RESP_ERR_LENGTH_AND_INDEXES: &str =
  "If you want both the length and indexes, please just use IDX.";

/// LCS 选项（C# NetworkLCS 局部变量组的结构化承接）
///
/// 快路径校验段与慢路径执行段共用同一解析单源 [`parse_lcs_options`]
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct LcsOptions {
  /// `LEN`（只回长度）
  pub len_only: bool,
  /// `IDX`（回匹配段索引矩阵）
  pub with_idx: bool,
  /// `MINMATCHLEN`（段长下限；C# TryGetInt 有符号，负值钳 0）
  pub min_match_len: usize,
  /// `WITHMATCHLEN`（IDX 矩阵每段附段长）
  pub with_match_len: bool,
}

/// 解析 LCS 选项（LEN / IDX / MINMATCHLEN <n> / WITHMATCHLEN）
///
/// 校验口径 1:1 对标 C# NetworkLCS：选项名大小写不敏感；未知选项与
/// MINMATCHLEN 缺参回语法错；MINMATCHLEN 非整数回 value-not-integer；
/// LEN 与 IDX 互斥（[`RESP_ERR_LENGTH_AND_INDEXES`]）
pub(crate) fn parse_lcs_options(rest: &[&[u8]]) -> Result<LcsOptions, &'static str> {
  let mut opts = LcsOptions::default();
  let mut idx = 0;
  while idx < rest.len() {
    let token = rest[idx];
    if token.eq_ignore_ascii_case(b"LEN") {
      opts.len_only = true;
    } else if token.eq_ignore_ascii_case(b"IDX") {
      opts.with_idx = true;
    } else if token.eq_ignore_ascii_case(b"MINMATCHLEN") {
      idx += 1;
      if idx >= rest.len() {
        return Err(cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
      }
      // C# TryGetInt（有符号）：负值钳 0 而非报错；前导零拒收系 rust 严格收口，见 doc/zh/deviations.md §32
      let Some(min_len) = strict_i32(rest[idx]) else {
        return Err(cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
      };
      opts.min_match_len = min_len.max(0) as usize;
    } else if token.eq_ignore_ascii_case(b"WITHMATCHLEN") {
      opts.with_match_len = true;
    } else {
      return Err(cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
    }
    idx += 1;
  }
  if opts.len_only && opts.with_idx {
    return Err(RESP_ERR_LENGTH_AND_INDEXES);
  }
  Ok(opts)
}

/// LCS 应答整形单点（[`crate::resp::RespServerSession::network_lcs`] 快路径与
/// [`super::slow::lcs`] 慢路径共漏斗，LEN/IDX/默认三形态与缺键空帧逐字节一致）
pub(crate) fn write_lcs_output<D: Device>(
  v1: Option<&[u8]>,
  v2: Option<&[u8]>,
  opts: &LcsOptions,
  resp3: bool,
  output: &mut Vec<u8>,
) {
  match (v1, v2) {
    (Some(v1), Some(v2)) => {
      if opts.len_only {
        let len = StorageSession::<D>::compute_lcs_length(v1, v2, opts.min_match_len);
        output.write_resp_int(len as i64);
      } else if opts.with_idx {
        let (total, matches) =
          StorageSession::<D>::compute_lcs_with_indices(v1, v2, opts.min_match_len);
        StorageSession::<D>::write_lcs_matches(&matches, opts.with_match_len, total, output, resp3);
      } else {
        let lcs = StorageSession::<D>::compute_lcs(v1, v2, opts.min_match_len);
        output.write_resp_bulk_string(&lcs);
      }
    }
    _ => {
      if opts.len_only {
        output.write_resp_int(0);
      } else if opts.with_idx {
        StorageSession::<D>::write_lcs_matches(&[], opts.with_match_len, 0, output, resp3);
      } else {
        output.write_resp_bulk_string(b"");
      }
    }
  }
}
