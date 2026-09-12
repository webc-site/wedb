//! 哈希对象操作（对标 libs/server/Storage/Session/ObjectStore/HashOps.cs，C# 为 StorageSession partial）
//!
//! 全部经 [`StorageSession`] 对象信封读写 [`HashObject`]，字段级 TTL
//! 缺口见 `hash_time_to_live`。哈希对象为空时删除键（对齐 Redis 对象生命周期）。

use wbase::num::try_parse_i64;
use wdev::Device;

use super::{
  super::storage_session::StorageSession,
  common::{GarnetObjectPayload, RmwOutcome},
};
use crate::{
  objects::{
    hash::hash_object::HashObject, parse_utils::strict_f64, types::object_output::ObjectOutput,
  },
  types::GarnetStatus,
};

impl<'a, D: Device, CR: wkv::ConsistentReadFunctions> StorageSession<'a, D, CR> {
  /// HDEL：删除字段，返回删除个数（删空时整键回收）
  ///
  /// libs/server/Storage/Session/ObjectStore/HashOps.cs:HashDelete
  pub async fn hash_delete(
    &self,
    key: &[u8],
    fields: &[&[u8]],
  ) -> wkv::Result<(GarnetStatus, i64)> {
    self
      .typed_remove_with_status::<HashObject, i64>(key, GarnetStatus::Ok, 0, |obj| {
        let mut n = 0i64;
        for field in fields {
          if obj.remove(field).is_some() {
            n += 1;
          }
        }
        Some(n)
      })
      .await
  }

  /// HGET：取单字段
  ///
  /// 键缺失返回 NOTFOUND（C# ReadObjectStoreOperation 缺键三态口径）。
  ///
  /// libs/server/Storage/Session/ObjectStore/HashOps.cs:HashGet
  pub async fn hash_get(
    &self,
    key: &[u8],
    field: &[u8],
  ) -> wkv::Result<(GarnetStatus, Option<Vec<u8>>)> {
    self
      .typed_query::<HashObject, _>(key, None, |obj| obj.try_get_value(field).cloned())
      .await
  }

  /// HMGET：取多字段（缺失字段占位 None）
  ///
  /// 键缺失返回 NOTFOUND（载荷仍按字段数占位 None，对齐 C# RESP 层
  /// NOTFOUND 分支的逐字段 nil 数组渲染）。
  ///
  /// libs/server/Storage/Session/ObjectStore/HashOps.cs:HashGetMultiple
  pub async fn hash_get_multiple(
    &self,
    key: &[u8],
    fields: &[&[u8]],
  ) -> wkv::Result<(GarnetStatus, Vec<Option<Vec<u8>>>)> {
    match self
      .typed_read_with::<HashObject, _>(key, |obj| {
        let mut out = Vec::with_capacity(fields.len());
        for field in fields {
          out.push(obj.try_get_value(field).cloned());
        }
        out
      })
      .await?
    {
      Err(s) => Ok((s, Vec::new())),
      Ok(None) => Ok((GarnetStatus::NotFound, vec![None; fields.len()])),
      Ok(Some(out)) => Ok((GarnetStatus::Ok, out)),
    }
  }

  /// HGETALL：全量键值对
  ///
  /// 键缺失返回 NOTFOUND（RESP 层据此写空数组）。
  ///
  /// libs/server/Storage/Session/ObjectStore/HashOps.cs:HashGetAll
  pub async fn hash_get_all(
    &self,
    key: &[u8],
  ) -> wkv::Result<(GarnetStatus, Vec<(Vec<u8>, Vec<u8>)>)> {
    self
      .typed_query::<HashObject, _>(key, Vec::new(), |obj| obj.to_pairs())
      .await
  }

  /// HLEN：字段数
  ///
  /// 键缺失返回 NOTFOUND（RESP 层同答 :0）。
  ///
  /// libs/server/Storage/Session/ObjectStore/HashOps.cs:HashLength
  pub async fn hash_length(&self, key: &[u8]) -> wkv::Result<(GarnetStatus, usize)> {
    self
      .typed_query::<HashObject, _>(key, 0, |obj| obj.hash.len())
      .await
  }

  /// HEXISTS：字段存在性
  ///
  /// 键缺失返回 NOTFOUND（对齐 C# ReadObjectStoreOperation 三态）。
  ///
  /// libs/server/Storage/Session/ObjectStore/HashOps.cs:HashExists
  pub async fn hash_exists(&self, key: &[u8], field: &[u8]) -> wkv::Result<(GarnetStatus, bool)> {
    self
      .typed_query::<HashObject, _>(key, false, |obj| obj.contains_key(field))
      .await
  }

  /// HRANDFIELD：随机返回字段（`count` 为负返回 |count| 个、允许重复；
  /// 为正返回至多 `count` 个不重复字段；`with_values` 两种计数下均附带值，
  /// 对齐 C# HashObjectImpl.HashRandomField / Redis WITHVALUES 口径）
  ///
  /// 键缺失返回 NOTFOUND（RESP 层据此写空数组或 nil）。
  ///
  /// libs/server/Storage/Session/ObjectStore/HashOps.cs:HashRandomField
  pub async fn hash_random_field(
    &self,
    key: &[u8],
    count: i64,
    with_values: bool,
  ) -> wkv::Result<(GarnetStatus, Vec<(Vec<u8>, Option<Vec<u8>>)>)> {
    match self
      .typed_read_with::<HashObject, _>(key, |obj| {
        let keys = obj.get_keys();
        // 空哈希（删空竞态窗口内的残留信封）：负计数随机下标无处可采，
        // 与键缺失同应答空列表，杜绝 fastrand 空区间 panic
        if keys.is_empty() {
          return Vec::new();
        }
        let pick = |k: Vec<u8>| {
          let v = obj.try_get_value(&k).cloned();
          (k, with_values.then_some(v).flatten())
        };
        if count < 0 {
          let n = count.unsigned_abs() as usize;
          (0..n)
            .map(|_| {
              let idx = fastrand::usize(..keys.len());
              pick(keys[idx].clone())
            })
            .collect()
        } else {
          let n = (count as usize).min(keys.len());
          let mut pool = keys;
          let mut out = Vec::with_capacity(n);
          for _ in 0..n {
            let idx = fastrand::usize(..pool.len());
            let k = pool.swap_remove(idx);
            out.push(pick(k));
          }
          out
        }
      })
      .await?
    {
      Err(s) => Ok((s, Vec::new())),
      Ok(None) => Ok((GarnetStatus::NotFound, Vec::new())),
      Ok(Some(res)) => Ok((GarnetStatus::Ok, res)),
    }
  }

  /// HSTRLEN：字段值字节数（字段缺失返回 None）
  ///
  /// libs/server/Storage/Session/ObjectStore/HashOps.cs:HashStrLength
  pub async fn hash_str_length(
    &self,
    key: &[u8],
    field: &[u8],
  ) -> wkv::Result<(GarnetStatus, Option<usize>)> {
    Ok(match self.hash_get(key, field).await? {
      (GarnetStatus::WrongType, _) => (GarnetStatus::WrongType, None),
      (s, v) => (s, v.map(|b| b.len())),
    })
  }

  /// HKEYS：全部字段
  ///
  /// libs/server/Storage/Session/ObjectStore/HashOps.cs:HashKeys
  pub async fn hash_keys(&self, key: &[u8]) -> wkv::Result<(GarnetStatus, Vec<Vec<u8>>)> {
    self
      .typed_query::<HashObject, _>(key, Vec::new(), |obj| obj.get_keys())
      .await
  }

  /// HVALS：全部字段值
  ///
  /// libs/server/Storage/Session/ObjectStore/HashOps.cs:HashVals
  pub async fn hash_vals(&self, key: &[u8]) -> wkv::Result<(GarnetStatus, Vec<Vec<u8>>)> {
    self
      .typed_query::<HashObject, _>(key, Vec::new(), |obj| obj.get_values())
      .await
  }

  /// HSET/HSETNX：写入字段（`nx` 为真时仅新字段生效），返回新增字段数
  ///
  /// 错误类型键（非哈希信封）传播 WRONGTYPE，不再被 RMW 放弃路径吞掉。
  ///
  /// libs/server/Storage/Session/ObjectStore/HashOps.cs:HashSet
  pub async fn hash_set(
    &self,
    key: &[u8],
    fields: &[(&[u8], &[u8])],
    nx: bool,
  ) -> wkv::Result<(GarnetStatus, i64)> {
    let added = self
      .typed_rmw::<HashObject, i64>(key, true, |obj| {
        let mut n = 0i64;
        for (field, value) in fields {
          let existed = obj.contains_key(field);
          if nx && existed {
            continue;
          }
          let old = obj.hash.insert(field.to_vec(), value.to_vec());
          if let Some(old_val) = old {
            obj.update_size(field, &old_val, false);
          } else {
            n += 1;
          }
          obj.update_size(field, value, true);
        }
        Some(n)
      })
      .await?;
    match added {
      RmwOutcome::WrongType => Ok((GarnetStatus::WrongType, 0)),
      outcome => Ok((GarnetStatus::Ok, outcome.unwrap_or(0))),
    }
  }

  /// HINCRBY/HINCRBYFLOAT：字段数值增减（`float` 走 f64 口径），回吐新值文本
  ///
  /// 字段存在但非数值 → WRONGTYPE（不覆盖写）。对齐 C# HashIncrement /
  /// HashIncrementFloat：字段缺失时原样存增量实参文本（incrSlice.ToArray()）；
  /// 浮点增量 NaN 字面量拒绝、±INF 先解析后报错；结果以最短往返文本落存
  /// （ObjectOutput::format_double 一处定义）；整数溢出按 C# unchecked 回绕。
  ///
  /// libs/server/Storage/Session/ObjectStore/HashOps.cs:HashIncrement
  pub async fn hash_increment(
    &self,
    key: &[u8],
    field: &[u8],
    delta: &[u8],
    float: bool,
  ) -> wkv::Result<(GarnetStatus, Option<Vec<u8>>)> {
    let outcome = self
      .typed_rmw::<HashObject, Vec<u8>>(key, true, |obj| {
        let current = obj.try_get_value(field).cloned();
        if float {
          // 浮点分支（HashIncrementFloat）：增量按 TryGetDouble 严格解析
          //（NaN 拒绝，±INF 白名单接受），随后 IsInfinity 检查报错
          let d = strict_f64(delta, true)?;
          if d.is_infinite() {
            return None; // RESP_ERR_GENERIC_NAN_INFINITY
          }
          match current.as_deref() {
            // 字段缺失：原样存增量文本（C# incrSlice.ToArray()），不归零计算
            None => {
              obj.hash.insert(field.to_vec(), delta.to_vec());
              obj.update_size(field, delta, true);
              Some(delta.to_vec())
            }
            Some(b) => {
              // 现值按 TryParseWithInfinity 同口径解析；∞ 现值拒绝增减
              let cur = strict_f64(b, true)?;
              if cur.is_infinite() {
                return None; // RESP_ERR_GENERIC_NAN_INFINITY_INCR
              }
              let new = cur + d;
              // 最短往返文本落存（ObjectOutput::format_double 一处定义）
              let text = ObjectOutput::format_double(new);
              let bytes = text.into_bytes();
              obj.update_size(field, b, false);
              obj.update_size(field, &bytes, true);
              obj.hash.insert(field.to_vec(), bytes.clone());
              Some(bytes)
            }
          }
        } else {
          // 整数分支（HashIncrement）：增量按 NumUtils.TryParse 整体消费解析
          //（接受前导零，不容空白；单一实现位于 wbase::num）
          let parse_i64 = |b: &[u8]| -> Option<i64> {
            let mut value = 0_i64;
            try_parse_i64(b, &mut value).then_some(value)
          };
          let d = parse_i64(delta)?;
          match current.as_deref() {
            // 字段缺失：原样存增量文本（C# incrSlice.ToArray()）
            None => {
              obj.hash.insert(field.to_vec(), delta.to_vec());
              obj.update_size(field, delta, true);
              Some(delta.to_vec())
            }
            Some(b) => {
              let cur = parse_i64(b)?;
              let mut buf = itoa::Buffer::new();
              let text = buf.format(cur.wrapping_add(d));
              let bytes = text.as_bytes().to_vec();
              obj.update_size(field, b, false);
              obj.update_size(field, &bytes, true);
              obj.hash.insert(field.to_vec(), bytes.clone());
              Some(bytes)
            }
          }
        }
      })
      .await?;
    match outcome {
      RmwOutcome::Written(v) => Ok((GarnetStatus::Ok, Some(v))),
      _ => Ok((GarnetStatus::WrongType, None)),
    }
  }

  /// HEXPIRE/HPEXPIRE：相对时长过期（整键级，TimeSpan 口径 ticks）
  ///
  /// libs/server/Storage/Session/ObjectStore/HashOps.cs:HashExpire
  pub async fn hash_expire(&self, key: &[u8], ttl_ticks: i64) -> wkv::Result<(GarnetStatus, bool)> {
    let set = self.expire_in_ticks(key, ttl_ticks).await?;
    Ok(if set == 1 {
      (GarnetStatus::Ok, true)
    } else {
      (GarnetStatus::NotFound, false)
    })
  }

  /// HPERSIST：移除过期
  ///
  /// libs/server/Storage/Session/ObjectStore/HashOps.cs:HashPersist
  pub async fn hash_persist(&self, key: &[u8]) -> wkv::Result<(GarnetStatus, bool)> {
    if self.pttl_ms(key).await? == -2 {
      return Ok((GarnetStatus::NotFound, false));
    }
    let removed = self.persist_key(key).await?;
    Ok((GarnetStatus::Ok, removed == 1))
  }

  /// 对象回收统计（键内字段数）
  ///
  /// libs/server/Storage/Session/ObjectStore/HashOps.cs:HashCollect
  pub async fn hash_collect(&self, key: &[u8]) -> wkv::Result<usize> {
    Ok(self.hash_length(key).await?.1)
  }

  /// 哈希对象键 TTL（整键级）
  ///
  /// 缺口说明：C# 侧 HEXPIRE/HTTL 字段级过期由 HashObject.expirationTimes 驱动；
  /// 存储层由 wkv 键级 TTL 统一承担，本方法仅提供整键 TTL 语义。
  ///
  /// libs/server/Storage/Session/ObjectStore/HashOps.cs:HashTimeToLive
  pub async fn hash_time_to_live(&self, key: &[u8]) -> wkv::Result<(GarnetStatus, i64)> {
    let pttl = self.pttl_ms(key).await?;
    if pttl == -2 {
      Ok((GarnetStatus::NotFound, pttl))
    } else {
      Ok((GarnetStatus::Ok, pttl))
    }
  }

  /// HSCAN：遍历哈希键字段
  ///
  /// libs/server/Storage/Session/ObjectStore/HashOps.cs:HashScan
  pub async fn hash_scan(
    &self,
    key: &[u8],
    cursor: &[u8],
    pattern: &[u8],
    count: usize,
  ) -> wkv::Result<(GarnetStatus, Vec<u8>, Vec<Vec<u8>>)> {
    let extract = |payload: &[u8]| -> Option<Vec<Vec<u8>>> {
      Some(HashObject::deserialize_from_slice(payload).get_keys())
    };
    self
      .object_scan(
        key,
        wval::GarnetObjectType::Hash as u8,
        pattern,
        cursor,
        count,
        extract,
      )
      .await
  }
}
