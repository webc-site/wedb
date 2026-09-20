//! 主存（字符串）操作面（对标 libs/server/Storage/Session/MainStore/MainStoreOps.cs，C# 为 StorageSession partial）
//!
//! 全部落在本域 [`StorageSession`] 上：
//! 同步快路径优先、磁盘候选/环形缓冲翻转降级 wkv 异步路径（对标 C#
//! CompletePendingForSession），命中/未命中/pending 计数与 C# 逐点对应。

use std::mem;

use wdev::Device;
use wresp::resp_memory_writer::{Resp2, Resp3, RespProtocol, RespWriter};

use super::super::storage_session::StorageSession;
use crate::types::GarnetStatus;

/// LCS 动态规划返回的三元组：公共子序列长度、回溯得到的匹配段列表
///
/// 匹配段为 (a 起始, b 起始, 段长)，升序排列（对标 C# LCSMatchData.Start1/Start2/Length）
pub type LcsResult = (usize, Vec<(usize, usize, usize)>);

impl<'a, D: Device> StorageSession<'a, D> {
  /// SETEX：写值并设置相对过期（TimeSpan 口径 ticks，对标 C# SETEX 以
  /// `UtcNow.Ticks + expiry.Ticks` 构造输入）
  ///
  /// libs/server/Storage/Session/MainStore/MainStoreOps.cs:SETEX
  pub async fn setex(&self, key: &[u8], val: &[u8], ttl_ticks: i64) -> wkv::Result<GarnetStatus> {
    self.upsert_string(key, val).await?;
    self.expire_in_ticks(key, ttl_ticks).await?;
    Ok(GarnetStatus::Ok)
  }

  /// 计算两串 LCS 长度（纯函数，O(min(M, N)) 空间滚动行优化）
  ///
  /// libs/server/Storage/Session/MainStore/MainStoreOps.cs:ComputeLCSLength
  pub fn compute_lcs_length(str1: &[u8], str2: &[u8], min_match_len: usize) -> usize {
    let (mut s1, mut s2) = (str1, str2);
    if s1.is_empty() || s2.is_empty() {
      return 0;
    }
    // 确保 s2 为较短切片，将空间占用最小化至 O(min(M, N))
    if s1.len() < s2.len() {
      mem::swap(&mut s1, &mut s2);
    }
    let n = s2.len();
    let mut prev = vec![0i32; n + 1];
    let mut curr = vec![0i32; n + 1];
    for &b1 in s1 {
      for (j, &b2) in s2.iter().enumerate() {
        curr[j + 1] = if b1 == b2 {
          prev[j] + 1
        } else {
          prev[j + 1].max(curr[j])
        };
      }
      mem::swap(&mut prev, &mut curr);
    }
    let len = prev[n] as usize;
    if len >= min_match_len { len } else { 0 }
  }

  /// 计算两串 LCS 及匹配段索引（纯函数）
  ///
  /// libs/server/Storage/Session/MainStore/MainStoreOps.cs:ComputeLCSWithIndices
  pub fn compute_lcs_with_indices(str1: &[u8], str2: &[u8], min_match_len: usize) -> LcsResult {
    let (m, n) = (str1.len(), str2.len());
    if m == 0 || n == 0 {
      return (0, Vec::new());
    }
    let stride = n + 1;
    let dp = Self::get_lcs_dp_table(str1, str2);
    let lcs_length = dp[m * stride + n] as usize;
    let mut matches = Vec::new();

    if lcs_length >= min_match_len {
      let (mut i, mut j) = (m, n);
      let mut current_match: Vec<(usize, usize)> = Vec::new();

      while i > 0 && j > 0 {
        if str1[i - 1] == str2[j - 1] {
          current_match.push((i - 1, j - 1));
          i -= 1;
          j -= 1;
        } else if dp[(i - 1) * stride + j] > dp[i * stride + j - 1] {
          i -= 1;
        } else {
          j -= 1;
        }
      }

      current_match.reverse();

      if !current_match.is_empty() {
        let mut start = 0;
        for k in 1..=current_match.len() {
          if k == current_match.len()
            || current_match[k].0 != current_match[k - 1].0 + 1
            || current_match[k].1 != current_match[k - 1].1 + 1
          {
            let length = k - start;
            if length >= min_match_len {
              matches.push((current_match[start].0, current_match[start].1, length));
            }
            start = k;
          }
        }
      }
    }

    matches.reverse();
    (lcs_length, matches)
  }

  /// 将匹配段序列化为 RESP 格式（纯函数，对标 C# WriteLCSMatches）
  ///
  /// libs/server/Storage/Session/MainStore/MainStoreOps.cs:WriteLCSMatches
  pub fn write_lcs_matches(
    matches: &[(usize, usize, usize)],
    with_match_len: bool,
    lcs_length: usize,
    output: &mut Vec<u8>,
    resp3: bool,
  ) {
    if resp3 {
      Self::write_lcs_matches_p::<Resp3>(matches, with_match_len, lcs_length, output);
    } else {
      Self::write_lcs_matches_p::<Resp2>(matches, with_match_len, lcs_length, output);
    }
  }

  fn write_lcs_matches_p<P: RespProtocol>(
    matches: &[(usize, usize, usize)],
    with_match_len: bool,
    lcs_length: usize,
    output: &mut Vec<u8>,
  ) {
    let mut writer = RespWriter::<_, P>::new_ref_p(output);
    writer.write_map_length(2);
    writer.write_bulk_string(b"matches");
    writer.write_array_length(matches.len());
    for &(s1, s2, len) in matches {
      writer.write_array_length(if with_match_len { 3 } else { 2 });
      writer.write_array_length(2);
      writer.write_int32(s1 as i32);
      writer.write_int32((s1 + len - 1) as i32);
      writer.write_array_length(2);
      writer.write_int32(s2 as i32);
      writer.write_int32((s2 + len - 1) as i32);
      if with_match_len {
        writer.write_int32(len as i32);
      }
    }
    writer.write_bulk_string(b"len");
    writer.write_int32(lcs_length as i32);
  }

  /// LCS DP 表（纯函数；返回 (len+1) x (width+1) 行主序扁平表）
  ///
  /// libs/server/Storage/Session/MainStore/MainStoreOps.cs:GetLcsDpTable
  pub fn get_lcs_dp_table(str1: &[u8], str2: &[u8]) -> Vec<i32> {
    let (m, n) = (str1.len(), str2.len());
    let stride = n + 1;
    let mut dp = vec![0i32; (m + 1) * stride];
    for (i, &b1) in str1.iter().enumerate() {
      let row = (i + 1) * stride;
      let prev_row = i * stride;
      for (j, &b2) in str2.iter().enumerate() {
        dp[row + j + 1] = if b1 == b2 {
          dp[prev_row + j] + 1
        } else {
          dp[prev_row + j + 1].max(dp[row + j])
        };
      }
    }
    dp
  }

  /// 依据 DP 表计算完整 LCS 字节切片（纯函数，Redis LCS 命令底层）
  ///
  /// libs/server/Storage/Session/MainStore/MainStoreOps.cs:ComputeLCS
  pub fn compute_lcs(str1: &[u8], str2: &[u8], min_match_len: usize) -> Vec<u8> {
    let (m, n) = (str1.len(), str2.len());
    if m == 0 || n == 0 {
      return Vec::new();
    }
    let stride = n + 1;
    let dp = Self::get_lcs_dp_table(str1, str2);
    let len = dp[m * stride + n] as usize;
    if len < min_match_len || len == 0 {
      return Vec::new();
    }
    let mut result = vec![0u8; len];
    let mut index = len;
    let (mut k, mut l) = (m, n);
    while k > 0 && l > 0 {
      if str1[k - 1] == str2[l - 1] {
        index -= 1;
        result[index] = str1[k - 1];
        k -= 1;
        l -= 1;
      } else if dp[(k - 1) * stride + l] > dp[k * stride + l - 1] {
        k -= 1;
      } else {
        l -= 1;
      }
    }
    result
  }
}
