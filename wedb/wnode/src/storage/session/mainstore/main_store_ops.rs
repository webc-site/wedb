//! 主存（字符串）操作面（对标 libs/server/Storage/Session/MainStore/MainStoreOps.cs，C# 为 StorageSession partial）
//!
//! 全部落在本域 [`StorageSession`] 上：
//! 同步快路径优先、磁盘候选/环形缓冲翻转降级 wkv 异步路径（对标 C#
//! CompletePendingForSession），命中/未命中/pending 计数与 C# 逐点对应。
//! 另含主存字符串读写回会话面：read_string 族（GET）、upsert_string（SET）、
//! rmw_string（RMW 写回）、delete_string / take_string（DELETE / GETDEL）。

use std::mem;
// 删除故障注入（io/Ordering）仅 debug 装配，release 剔除防 unused imports
#[cfg(debug_assertions)]
use std::{io, sync::atomic::Ordering};

use wdev::Device;
use wkv::RmwWindow;
use wresp::resp_memory_writer::{Resp2, Resp3, RespProtocol, RespWriter};
use wval::KeyTag;

#[cfg(debug_assertions)]
use super::super::storage_session::DELETE_FAIL_INJECT;
use super::super::storage_session::StorageSession;

/// LCS 动态规划返回的三元组：公共子序列长度、回溯得到的匹配段列表
///
/// 匹配段为 (a 起始, b 起始, 段长)，升序排列（对标 C# LCSMatchData.Start1/Start2/Length）
pub(crate) type LcsResult = (usize, Vec<(usize, usize, usize)>);

impl<'a, D: Device> StorageSession<'a, D> {
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
  fn get_lcs_dp_table(str1: &[u8], str2: &[u8]) -> Vec<i32> {
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

impl<'a, D: Device> StorageSession<'a, D> {
  /// 读字符串键值（零拷贝闭包版：快路径内存直读，磁盘候选异步闭环）
  ///
  /// 对标 C# StringBasicContext.Read 内部 CompletePending：`Ok(None)`（磁盘候选）
  /// 时降级 wkv 异步 `read_with`，对调用方呈现同步闭环语义。
  pub async fn read_string_with<R>(
    &self,
    key: &[u8],
    f: impl Fn(&[u8]) -> R,
  ) -> wkv::Result<Option<R>> {
    self.read_tag_with(key, KeyTag::String, f).await
  }

  /// 读字符串键值（拷贝版）——测试钩子（#[doc(hidden)]：生产读族一律走
  /// [`Self::read_string_with`] 零拷贝闭包版，本口仅供 wnode/wedb 集成测试
  /// 断言直用，60+ 调用点不改写）
  ///
  /// libs/server/Storage/Session/MainStore/MainStoreOps.cs:GET
  #[doc(hidden)]
  pub async fn read_string(&self, key: &[u8]) -> wkv::Result<Option<Vec<u8>>> {
    self.read_string_with(key, |v| v.to_vec()).await
  }

  /// 写字符串键值（SET 语义：同步快路径优先，环形缓冲翻转 / TTL 清除异步闭环）
  ///
  /// libs/server/Storage/Session/MainStore/MainStoreOps.cs:SET
  pub async fn upsert_string(&self, key: &[u8], val: &[u8]) -> wkv::Result<()> {
    self.upsert_tag(key, KeyTag::String, val).await
  }

  /// RMW 写回字符串键值（未过期键保留既有 key 级 TTL，已过期键清退残留 TTL
  /// 后重建无 TTL：同步快路径优先，环形页翻转 / TTL 记录磁盘候选降级
  /// wkv `upsert_rmw` 异步完整闭环；WATCH 推进由 wkv 用户键写入口收口）
  ///
  /// libs/server/Storage/Functions/MainStore/RMWMethods
  ///
  /// INCR/DECR 族、INCRBYFLOAT、APPEND、SETRANGE、SETBIT、BITFIELD 写子命令、
  /// PFADD/PFMERGE 的读改写回写面（lua/事务/AOF 重放共用），对标 C#
  /// UnifiedStore/VarLenInputMethods 的 HasExpiration 保留语义与
  /// UnifiedStore/RMWMethods.cs CopyUpdater 的 CheckExpiry → ExpireAndResume
  /// （过期转 InitialUpdater 重建，初始记录无 Expiration）
  ///
  /// 写回目标键由 [`RmwWindow`] 承载：调用方须在装载旧值之前取窗（同步域
  /// `BatchStoreSession::try_rmw_window`、异步域 `rmw_window`），本入口只在窗口
  /// 内落笔，故「无锁读旧值 → 盲写绝对值」的两步式在类型面上不可表达
  pub async fn rmw_string<'k, 'w>(
    &self,
    window: &RmwWindow<'w, 'k, D>,
    val: &[u8],
  ) -> wkv::Result<()> {
    match window.try_rmw_sync(val)? {
      Ok(_) => {}
      // 降级 wkv 异步闭环（等价于退出批处理纪元后重写；upsert_rmw 内含
      // 过期残留完整裁决，先 purge 后重建）
      Err(_) => {
        self
          .with_pending_metrics(|| window.upsert_rmw(val))
          .await
          .map(|_| ())?;
      }
    }
    Ok(())
  }

  /// 删除键（同步快路径优先，磁盘异步闭环）
  ///
  /// C# 统一存 DELETE（unifiedContext.Delete → Found 判 OK/NOTFOUND）的
  /// rust 单点：libs/server/Storage/Session/UnifiedStore/UnifiedStoreOps.cs:DELETE
  pub async fn delete_string(&self, key: &[u8]) -> wkv::Result<bool> {
    // 故障注入门（测试钩子，一次性）：模拟底层设备写故障，与真实
    // wdev::Error::OutOfBounds 同型上抛，杜绝测试绕过本统一删除入口
    #[cfg(debug_assertions)]
    if DELETE_FAIL_INJECT.swap(false, Ordering::AcqRel) {
      return Err(wkv::Error::Io(io::Error::other("删除故障注入（测试钩子）")));
    }
    match self.batch.try_delete_sync(key)? {
      // 快路径闭环：版本推进由 wkv 用户键删除入口统一收口（对齐 C#
      // InitialDeleter 无条件 IncrementVersion，缺席键墓碑同向计入）
      Ok(deleted) => Ok(deleted),
      // 降级异步闭环（复合对象元数据 / 环形页翻转 / 冷数据确认）：WATCH
      // 版本推进已由 wkv collection 层 delete 无条件收口，本层零重复推进
      Err(_) => self.with_pending_metrics(|| self.batch.delete(key)).await,
    }
  }

  /// 取删字符串域键并回传被摘值（GETDEL 读删一体：应答值 = 实际摘除记录的值）
  ///
  /// [`Self::delete_string`] 的取值对位：快路径闭环答摘除值（捕获与摘除同一
  /// 临界区）；降级异步闭环由 wkv `take_string` 同级联收口（WATCH 版本推进
  /// 单点不重复）
  pub async fn take_string(&self, key: &[u8]) -> wkv::Result<Option<Vec<u8>>> {
    match self.batch.try_take_sync(key)? {
      Ok(taken) => Ok(taken),
      Err(_) => {
        self
          .with_pending_metrics(|| self.batch.take_string(key))
          .await
      }
    }
  }
}
