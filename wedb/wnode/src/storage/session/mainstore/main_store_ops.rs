//! 主存（字符串）操作面（对标 libs/server/Storage/Session/MainStore/MainStoreOps.cs，C# 为 StorageSession partial）
//!
//! 全部落在本域 [`StorageSession`] 上：
//! 同步快路径优先、磁盘候选/环形缓冲翻转降级 wkv 异步路径（对标 C#
//! CompletePendingForSession），命中/未命中/pending 计数与 C# 逐点对应。

use std::{mem, str, sync::atomic::Ordering::Relaxed};

use wdev::Device;

use super::super::storage_session::{StorageSession, StoreType};
use crate::{storage::resp_memory_writer::RespMemoryWriter, types::GarnetStatus};

/// LCS 动态规划返回的三元组：公共子序列长度、回溯得到的匹配段列表
///
/// 匹配段为 (a 起始, b 起始, 段长)，升序排列（对标 C# LCSMatchData.Start1/Start2/Length）
pub type LcsResult = (usize, Vec<(usize, usize, usize)>);

impl<'a, D: Device, CR: wkv::ConsistentReadFunctions> StorageSession<'a, D, CR> {
  /// 读取字符串值（批处理上下文版：内存直读 + 磁盘候选异步闭环）
  ///
  /// libs/server/Storage/Session/MainStore/MainStoreOps.cs:ReadWithUnsafeContext
  pub async fn read_with_unsafe_context(
    &self,
    key: &[u8],
  ) -> wkv::Result<(GarnetStatus, Option<Vec<u8>>)> {
    match self.read_string(key).await? {
      Some(v) => Ok((GarnetStatus::Ok, Some(v))),
      None => Ok((GarnetStatus::NotFound, None)),
    }
  }

  /// GETEX：读值并按参数续期（`ttl_ticks` 相对 ticks 续期，`persist` 移除 TTL）
  ///
  /// libs/server/Storage/Session/MainStore/MainStoreOps.cs:GETEX
  pub async fn getex(
    &self,
    key: &[u8],
    ttl_ticks: Option<i64>,
    persist: bool,
  ) -> wkv::Result<(GarnetStatus, Option<Vec<u8>>)> {
    let val = self.read_string(key).await?;
    if val.is_none() {
      return Ok((GarnetStatus::NotFound, None));
    }
    if let Some(ttl) = ttl_ticks {
      self.expire_in_ticks(key, ttl).await?;
    } else if persist {
      self.persist_key(key).await?;
    }
    Ok((GarnetStatus::Ok, val))
  }

  /// GETDEL：原子读后删除
  ///
  /// libs/server/Storage/Session/MainStore/MainStoreOps.cs:GETDEL
  pub async fn getdel(&self, key: &[u8]) -> wkv::Result<(GarnetStatus, Option<Vec<u8>>)> {
    match self.read_string(key).await? {
      Some(v) => {
        let _ = self.delete_string(key).await?;
        Ok((GarnetStatus::Ok, Some(v)))
      }
      None => Ok((GarnetStatus::NotFound, None)),
    }
  }

  /// GETRANGE：取值子串（Redis 负索引语义，闭区间 [start, end]）
  ///
  /// libs/server/Storage/Session/MainStore/MainStoreOps.cs:GETRANGE
  pub async fn getrange(&self, key: &[u8], start: i64, end: i64) -> wkv::Result<Vec<u8>> {
    let res = self
      .read_string_with(key, |val| {
        let len = val.len() as i64;
        // Redis 语义：负索引自尾部计数；夹取后空区间返回空串
        let s = if start < 0 { len + start } else { start }.max(0);
        let e = if end < 0 { len + end } else { end }.min(len - 1);
        if s > e || s >= len {
          return Vec::new();
        }
        let s = s as usize;
        let e = (e.max(0) as usize).min(val.len() - 1);
        val[s..=e].to_vec()
      })
      .await?;
    Ok(res.unwrap_or_default())
  }

  /// SET 条件写（NX 仅不存在时写 / XX 仅存在时写；`get_old` 返回旧值支撑 SET GET）
  ///
  /// libs/server/Storage/Session/MainStore/MainStoreOps.cs:SET_Conditional
  pub async fn set_conditional(
    &self,
    key: &[u8],
    val: &[u8],
    nx: bool,
    xx: bool,
    get_old: bool,
  ) -> wkv::Result<(GarnetStatus, Option<Vec<u8>>)> {
    let (old, exists) = if get_old {
      let v = self.read_string(key).await?;
      let exists = v.is_some();
      (v, exists)
    } else if nx || xx {
      let exists = self.read_string_with(key, |_| ()).await?.is_some();
      (None, exists)
    } else {
      (None, false)
    };
    if (nx && exists) || (xx && !exists) {
      // 条件不满足：对齐 C# RMW 失败 → NOTFOUND
      self.session_notfound.fetch_add(1, Relaxed);
      return Ok((GarnetStatus::NotFound, if get_old { old } else { None }));
    }
    self.upsert_string(key, val).await?;
    Ok((GarnetStatus::Ok, if get_old { old } else { None }))
  }

  /// 条件删除（DELIIFGREATER 语义：记录 etag 小于给定值才删除）
  ///
  /// 缺口：C# etag 存于 Tsavorite 记录扩展字段（RMWMethods.Etags），wkv 记录无
  /// etag 通道，本实现以当前字符串值按 u64 解析充当 etag
  /// （"值为整数文本 = etag"约定），语义方向一致。
  ///
  /// libs/server/Storage/Session/MainStore/MainStoreOps.cs:DEL_Conditional
  pub async fn del_conditional(&self, key: &[u8], etag: u64) -> wkv::Result<GarnetStatus> {
    let should_del = self
      .read_string_with(key, |val| {
        let current = str::from_utf8(val)
          .ok()
          .and_then(|s| s.parse::<u64>().ok())
          .unwrap_or(u64::MAX);
        current < etag
      })
      .await?;
    match should_del {
      Some(true) => {
        let _ = self.delete_string(key).await?;
        self.session_found.fetch_add(1, Relaxed);
        Ok(GarnetStatus::Ok)
      }
      Some(false) => Ok(GarnetStatus::NotFound),
      None => {
        self.session_notfound.fetch_add(1, Relaxed);
        Ok(GarnetStatus::NotFound)
      }
    }
  }

  /// MSET 条件批量写（`nx` 为真时任一键已存在则整批不写）
  ///
  /// libs/server/Storage/Session/MainStore/MainStoreOps.cs:MSET_Conditional
  pub async fn mset_conditional(
    &self,
    keys: &[&[u8]],
    values: &[&[u8]],
    nx: bool,
  ) -> wkv::Result<GarnetStatus> {
    if keys.len() != values.len() {
      return Ok(GarnetStatus::NotFound);
    }
    if nx {
      for key in keys {
        if self.read_string_with(key, |_| ()).await?.is_some() {
          return Ok(GarnetStatus::NotFound);
        }
      }
    }
    for (key, val) in keys.iter().zip(values.iter()) {
      self.upsert_string(key, val).await?;
    }
    Ok(GarnetStatus::Ok)
  }

  /// SETEX：写值并设置相对过期（TimeSpan 口径 ticks，对标 C# SETEX 以
  /// `UtcNow.Ticks + expiry.Ticks` 构造输入）
  ///
  /// libs/server/Storage/Session/MainStore/MainStoreOps.cs:SETEX
  pub async fn setex(&self, key: &[u8], val: &[u8], ttl_ticks: i64) -> wkv::Result<GarnetStatus> {
    self.upsert_string(key, val).await?;
    self.expire_in_ticks(key, ttl_ticks).await?;
    Ok(GarnetStatus::Ok)
  }

  /// APPEND：尾部追加，返回追加后的总长度
  ///
  /// libs/server/Storage/Session/MainStore/MainStoreOps.cs:APPEND
  pub async fn append(&self, key: &[u8], val: &[u8]) -> wkv::Result<(GarnetStatus, usize)> {
    let mut buf = self.read_string(key).await?.unwrap_or_default();
    buf.extend_from_slice(val);
    let len = buf.len();
    self.upsert_string(key, &buf).await?;
    Ok((GarnetStatus::Ok, len))
  }

  /// 主存删除键
  ///
  /// libs/server/Storage/Session/MainStore/MainStoreOps.cs:DELETE_MainStore
  pub async fn delete_main_store(&self, key: &[u8]) -> wkv::Result<GarnetStatus> {
    if self.delete_string(key).await? {
      Ok(GarnetStatus::Ok)
    } else {
      self.session_notfound.fetch_add(1, Relaxed);
      Ok(GarnetStatus::NotFound)
    }
  }

  /// SETRANGE：自偏移覆写（不足处补 '\0'），返回写入后的总长度
  ///
  /// libs/server/Storage/Session/MainStore/MainStoreOps.cs:SETRANGE
  pub async fn setrange(
    &self,
    key: &[u8],
    offset: usize,
    val: &[u8],
  ) -> wkv::Result<(GarnetStatus, usize)> {
    let mut buf = self.read_string(key).await?.unwrap_or_default();
    if buf.len() < offset {
      buf.resize(offset, 0);
    }
    let end = offset + val.len();
    if buf.len() < end {
      buf.resize(end, 0);
    }
    buf[offset..end].copy_from_slice(val);
    let len = buf.len();
    self.upsert_string(key, &buf).await?;
    Ok((GarnetStatus::Ok, len))
  }

  /// WATCH：登记键与当前写日志尾地址（版本代理，见 storage_session 域注释）
  ///
  /// libs/server/Storage/Session/MainStore/MainStoreOps.cs:WATCH
  /// 保留 _store_type 形参以对标 Garnet MainStoreOps.WATCH 接口签名
  pub fn watch(&self, key: &[u8], _store_type: StoreType) {
    self.watch_key(key);
  }

  /// LCS：两串最长公共子序列入口（`len_only` 仅返回长度）
  ///
  /// 任一键缺失：返回 OK + 空结果（总长 0、无匹配段）——C# LCSInternal 在
  /// NOTFOUND 时照常写出空应答（lenOnly 写 0 / withIndices 写空表 /
  /// 默认写空 bulk string），不返回 NOTFOUND。
  ///
  /// libs/server/Storage/Session/MainStore/MainStoreOps.cs:LCS
  /// （工作体：LCSInternal 纯函数）
  pub async fn lcs(
    &self,
    key1: &[u8],
    key2: &[u8],
  ) -> wkv::Result<(GarnetStatus, Option<LcsResult>)> {
    // 任一键缺失：OK + 空输出（对齐 C# status != OK 分支的空应答写出）
    let result = match (self.read_string(key1).await?, self.read_string(key2).await?) {
      (Some(v1), Some(v2)) => Some(Self::lcs_internal(&v1, &v2)),
      _ => Some((0, Vec::new())),
    };
    Ok((GarnetStatus::Ok, result))
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
    let mut writer = RespMemoryWriter::new(resp3);
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
    output.extend_from_slice(&writer.out);
  }

  /// LCS 计算内部实现（纯函数版，供存储层与 RESP 层复用）
  ///
  /// libs/server/Storage/Session/MainStore/MainStoreOps.cs:LCSInternal
  pub fn lcs_internal(a: &[u8], b: &[u8]) -> LcsResult {
    Self::compute_lcs_with_indices(a, b, 0)
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
