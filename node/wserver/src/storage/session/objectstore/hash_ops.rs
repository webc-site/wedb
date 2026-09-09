//! 哈希对象操作（对标 libs/server/Storage/Session/ObjectStore/HashOps.cs，C# 为 StorageSession partial）
//!
//! 全部经 [`StorageSession`] 对象信封读写 wobject [`HashObject`]，字段级 TTL
//! 缺口见 `hash_time_to_live`。哈希对象为空时删除键（对齐 Redis 对象生命周期）。

use std::{io::Cursor, str, sync::atomic::Ordering::Relaxed};

use wdev::Device;
use wobject::hash::hash_object::{HashObject, HashOperation};

use super::{super::storage_session::StorageSession, common::ObjState};
use crate::{
  api::garnet_status::GarnetStatus, objects::types::object_output::ObjectOutput,
  resp::parser::session_parse_state::strict_f64,
};

impl<'a, D: Device> StorageSession<'a, D> {
  /// 哈希对象读-改-写：载荷解码为 HashObject 后交闭包变更，返回前自动回写
  async fn hash_rmw<R>(
    &self,
    key: &[u8],
    f: impl FnOnce(&mut HashObject) -> R,
  ) -> wkv::Result<Option<R>> {
    self
      .rmw_object_store_operation(key, super::common::OBJ_TAG_HASH, |payload| {
        let mut obj = match payload {
          Some(bytes) => HashObject::deserialize(&mut Cursor::new(bytes)).unwrap_or_default(),
          None => HashObject::new(),
        };
        let r = f(&mut obj);
        let mut out = Vec::new();
        obj.serialize(&mut out).ok()?;
        Some((out, r))
      })
      .await
  }

  /// HDEL：删除字段，返回删除个数
  ///
  /// libs/server/Storage/Session/ObjectStore/HashOps.cs:HashDelete
  pub async fn hash_delete(
    &self,
    key: &[u8],
    fields: &[&[u8]],
  ) -> wkv::Result<(GarnetStatus, i64)> {
    match self.obj_load(key, super::common::OBJ_TAG_HASH).await? {
      ObjState::Absent => Ok((GarnetStatus::Ok, 0)),
      ObjState::WrongType => Ok((GarnetStatus::WrongType, 0)),
      _ => {
        let removed = self
          .hash_rmw(key, |obj| {
            let mut n = 0i64;
            for field in fields {
              if obj.operate(HashOperation::HDEL, field, b"").is_some() {
                n += 1;
              }
            }
            n
          })
          .await?
          .unwrap_or(0);
        self.hash_gc_if_empty(key).await?;
        Ok((GarnetStatus::Ok, removed))
      }
    }
  }

  /// HGET：取单字段
  ///
  /// libs/server/Storage/Session/ObjectStore/HashOps.cs:HashGet
  pub async fn hash_get(
    &self,
    key: &[u8],
    field: &[u8],
  ) -> wkv::Result<(GarnetStatus, Option<Vec<u8>>)> {
    match self.obj_load(key, super::common::OBJ_TAG_HASH).await? {
      ObjState::Absent => Ok((GarnetStatus::Ok, None)),
      ObjState::WrongType => Ok((GarnetStatus::WrongType, None)),
      ObjState::Present(payload) => {
        let obj = HashObject::deserialize(&mut Cursor::new(payload)).unwrap_or_default();
        Ok((
          GarnetStatus::Ok,
          obj.operate(HashOperation::HGET, field, b""),
        ))
      }
    }
  }

  /// HMGET：取多字段（缺失字段占位 None）
  ///
  /// libs/server/Storage/Session/ObjectStore/HashOps.cs:HashGetMultiple
  pub async fn hash_get_multiple(
    &self,
    key: &[u8],
    fields: &[&[u8]],
  ) -> wkv::Result<(GarnetStatus, Vec<Option<Vec<u8>>>)> {
    match self.obj_load(key, super::common::OBJ_TAG_HASH).await? {
      ObjState::Absent => Ok((GarnetStatus::Ok, fields.iter().map(|_| None).collect())),
      ObjState::WrongType => Ok((GarnetStatus::WrongType, Vec::new())),
      ObjState::Present(payload) => {
        let obj = HashObject::deserialize(&mut Cursor::new(payload)).unwrap_or_default();
        let pin = obj.hash.pin();
        let mut out = Vec::with_capacity(fields.len());
        for field in fields {
          out.push(pin.get(*field).cloned());
        }
        Ok((GarnetStatus::Ok, out))
      }
    }
  }

  /// HGETALL：全量键值对
  ///
  /// libs/server/Storage/Session/ObjectStore/HashOps.cs:HashGetAll
  pub async fn hash_get_all(
    &self,
    key: &[u8],
  ) -> wkv::Result<(GarnetStatus, Vec<(Vec<u8>, Vec<u8>)>)> {
    match self.obj_load(key, super::common::OBJ_TAG_HASH).await? {
      ObjState::Absent => Ok((GarnetStatus::Ok, Vec::new())),
      ObjState::WrongType => Ok((GarnetStatus::WrongType, Vec::new())),
      ObjState::Present(payload) => {
        let obj = HashObject::deserialize(&mut Cursor::new(payload)).unwrap_or_default();
        Ok((GarnetStatus::Ok, obj.hash_get_all()))
      }
    }
  }

  /// HLEN：字段数
  ///
  /// libs/server/Storage/Session/ObjectStore/HashOps.cs:HashLength
  pub async fn hash_length(&self, key: &[u8]) -> wkv::Result<(GarnetStatus, usize)> {
    match self.obj_load(key, super::common::OBJ_TAG_HASH).await? {
      ObjState::Absent => Ok((GarnetStatus::Ok, 0)),
      ObjState::WrongType => Ok((GarnetStatus::WrongType, 0)),
      ObjState::Present(payload) => {
        let obj = HashObject::deserialize(&mut Cursor::new(payload)).unwrap_or_default();
        Ok((GarnetStatus::Ok, obj.hash.pin().len()))
      }
    }
  }

  /// HEXISTS：字段存在性
  ///
  /// libs/server/Storage/Session/ObjectStore/HashOps.cs:HashExists
  pub async fn hash_exists(&self, key: &[u8], field: &[u8]) -> wkv::Result<(GarnetStatus, bool)> {
    match self.obj_load(key, super::common::OBJ_TAG_HASH).await? {
      ObjState::Absent => Ok((GarnetStatus::Ok, false)),
      ObjState::WrongType => Ok((GarnetStatus::WrongType, false)),
      ObjState::Present(payload) => {
        let obj = HashObject::deserialize(&mut Cursor::new(payload)).unwrap_or_default();
        Ok((GarnetStatus::Ok, obj.hash.pin().contains_key(field)))
      }
    }
  }

  /// HRANDFIELD：随机返回字段（`count` 为负返回 |count| 个、可重复；为正返回
  /// 至多 `count` 个不重复字段；`with_values` 不分正负一律附带值——对齐 C#
  /// HashObjectImpl.HashRandomField：负计数路径同样写 pair.Value，不忽略 WITHVALUES）
  ///
  /// libs/server/Storage/Session/ObjectStore/HashOps.cs:HashRandomField
  /// （工作体：libs/server/Objects/Hash/HashObjectImpl.cs:HashRandomField）
  pub async fn hash_random_field(
    &self,
    key: &[u8],
    count: i64,
    with_values: bool,
  ) -> wkv::Result<(GarnetStatus, Vec<(Vec<u8>, Option<Vec<u8>>)>)> {
    match self.obj_load(key, super::common::OBJ_TAG_HASH).await? {
      ObjState::Absent => Ok((GarnetStatus::Ok, Vec::new())),
      ObjState::WrongType => Ok((GarnetStatus::WrongType, Vec::new())),
      ObjState::Present(payload) => {
        let obj = HashObject::deserialize(&mut Cursor::new(payload)).unwrap_or_default();
        let keys = obj.get_keys();
        // 空哈希（删空竞态窗口内的残留信封）：负计数随机下标无处可采，
        // 与键缺失同应答空列表，杜绝 fastrand 空区间 panic
        if keys.is_empty() {
          return Ok((GarnetStatus::Ok, Vec::new()));
        }
        let pin = obj.hash.pin();
        if count < 0 {
          // 负计数：允许重复取样 |count| 个；WITHVALUES 照常带值（C# 语义，
          // 非 Redis CLI 口径——Garnet 不因计数为负而忽略 withValues）
          let n = count.unsigned_abs() as usize;
          let out = (0..n)
            .map(|_| {
              let k = keys[fastrand::usize(..keys.len())].clone();
              let v = with_values.then(|| pin.get(&k).cloned()).flatten();
              (k, v)
            })
            .collect();
          return Ok((GarnetStatus::Ok, out));
        }
        // 正计数：交换洗牌取不重复字段
        let want = (count as usize).min(keys.len());
        let mut pool = keys;
        let mut out = Vec::with_capacity(want);
        for _ in 0..want {
          let last = pool.len() - 1;
          let idx = fastrand::usize(..pool.len());
          pool.swap(idx, last);
          let k = pool.pop().unwrap_or_default();
          let v = pin.get(&k).cloned();
          out.push((k, with_values.then_some(v).flatten()));
        }
        Ok((GarnetStatus::Ok, out))
      }
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
    Ok(match self.hash_get_all(key).await? {
      (GarnetStatus::WrongType, _) => (GarnetStatus::WrongType, Vec::new()),
      (s, all) => (s, all.into_iter().map(|(k, _)| k).collect()),
    })
  }

  /// HVALS：全部字段值
  ///
  /// libs/server/Storage/Session/ObjectStore/HashOps.cs:HashVals
  pub async fn hash_vals(&self, key: &[u8]) -> wkv::Result<(GarnetStatus, Vec<Vec<u8>>)> {
    Ok(match self.hash_get_all(key).await? {
      (GarnetStatus::WrongType, _) => (GarnetStatus::WrongType, Vec::new()),
      (s, all) => (s, all.into_iter().map(|(_, v)| v).collect()),
    })
  }

  /// HSET/HSETNX：写入字段（`nx` 为真时仅新字段生效），返回新增字段数
  ///
  /// 错误类型键（非哈希信封）传播 WRONGTYPE，不再被 RMW 放弃路径吞掉。
  ///
  /// libs/server/Storage/Session/ObjectStore/HashOps.cs:HashSetWhenNotExists（IGarnetApi.HashSet 同链路）
  pub async fn hash_set(
    &self,
    key: &[u8],
    fields: &[(&[u8], &[u8])],
    nx: bool,
  ) -> wkv::Result<(GarnetStatus, i64)> {
    if let ObjState::WrongType = self.obj_load(key, super::common::OBJ_TAG_HASH).await? {
      return Ok((GarnetStatus::WrongType, 0));
    }
    let added = self
      .hash_rmw(key, |obj| {
        let mut n = 0i64;
        for (field, value) in fields {
          // 单次查询判定新增：HSET 返回 None 即字段原先不存在
          let existed = obj.operate(HashOperation::HGET, field, b"").is_some();
          if nx && existed {
            continue;
          }
          obj.operate(HashOperation::HSET, field, value);
          if !existed {
            n += 1;
          }
        }
        n
      })
      .await?;
    Ok((GarnetStatus::Ok, added.unwrap_or(0)))
  }

  /// HINCRBY/HINCRBYFLOAT：字段数值增减（`float` 走 f64 口径），回吐新值文本
  ///
  /// 字段存在但非数值 → WRONGTYPE（不覆盖写）。对齐 C# HashIncrement /
  /// HashIncrementFloat：字段缺失时原样存增量实参文本（incrSlice.ToArray()）；
  /// 浮点增量 NaN 字面量拒绝、±INF 先解析后报错；结果以最短往返文本落存
  /// （TryFormat 默认格式，非 17 位定点）。
  ///
  /// libs/server/Storage/Session/ObjectStore/HashOps.cs:HashIncrement
  /// （工作体：libs/server/Objects/Hash/HashObjectImpl.cs:HashIncrement /
  /// HashIncrementFloat）
  pub async fn hash_increment(
    &self,
    key: &[u8],
    field: &[u8],
    delta: &[u8],
    float: bool,
  ) -> wkv::Result<(GarnetStatus, Option<Vec<u8>>)> {
    if let ObjState::WrongType = self.obj_load(key, super::common::OBJ_TAG_HASH).await? {
      return Ok((GarnetStatus::WrongType, None));
    }
    match self
      .hash_rmw(key, |obj| {
        let current = obj.operate(HashOperation::HGET, field, b"");
        if float {
          // 浮点分支（HashIncrementFloat）：增量按 C# TryGetDouble 严格解析
          //（NaN 拒绝，±INF 白名单接受），随后 IsInfinity 检查报错
          let Some(d) = strict_f64(delta, true) else {
            return (false, None);
          };
          if d.is_infinite() {
            return (false, None); // RESP_ERR_GENERIC_NAN_INFINITY
          }
          match current.as_deref() {
            // 字段缺失：原样存增量文本（C# incrSlice.ToArray()），不归零计算
            None => {
              obj.operate(HashOperation::HSET, field, delta);
              (true, Some(delta.to_vec()))
            }
            Some(b) => {
              // 现值按 TryParseWithInfinity 同口径解析；∞ 现值拒绝增减
              let Some(cur) = strict_f64(b, true) else {
                return (false, None); // RESP_ERR_HASH_VALUE_IS_NOT_FLOAT
              };
              if cur.is_infinite() {
                return (false, None); // RESP_ERR_GENERIC_NAN_INFINITY_INCR
              }
              let new = cur + d;
              // 最短往返文本落存（C# double.TryFormat 默认格式）
              let text = ObjectOutput::format_double(new);
              obj.operate(HashOperation::HSET, field, text.as_bytes());
              (true, Some(text.into_bytes()))
            }
          }
        } else {
          // 整数分支（HashIncrement）：增量按 NumUtils.TryParse 整体消费解析
          //（Utf8Parser 默认口径：接受前导零，不容空白，非 strict 族）
          let parse_i64 = |b: &[u8]| -> Option<i64> {
            str::from_utf8(b).ok().and_then(|s| s.parse::<i64>().ok())
          };
          let Some(d) = parse_i64(delta) else {
            return (false, None); // RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER
          };
          match current.as_deref() {
            // 字段缺失：原样存增量文本（C# incrSlice.ToArray()）
            None => {
              obj.operate(HashOperation::HSET, field, delta);
              (true, Some(delta.to_vec()))
            }
            Some(b) => {
              let Some(cur) = parse_i64(b) else {
                return (false, None); // RESP_ERR_HASH_VALUE_IS_NOT_INTEGER
              };
              // C# `result += incr` 默认 unchecked：溢出回绕，非拒绝
              let text = cur.wrapping_add(d).to_string();
              obj.operate(HashOperation::HSET, field, text.as_bytes());
              (true, Some(text.into_bytes()))
            }
          }
        }
      })
      .await?
    {
      Some((true, v)) => Ok((GarnetStatus::Ok, v)),
      Some((false, _)) => Ok((GarnetStatus::WrongType, None)),
      None => Ok((GarnetStatus::WrongType, None)),
    }
  }

  /// 哈希对象键 TTL（整键级）
  ///
  /// 缺口说明：C# 侧 HEXPIRE/HTTL 字段级过期由 HashObject.expirationTimes 驱动；
  /// wobject Rust 侧明确移除字段级过期容器（由 wkv 键级 TTL 承担），本方法仅
  /// 提供整键 TTL 语义。
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

  /// 空哈希回收：字段清空后删除整键（对齐 Redis 对象生命周期）
  async fn hash_gc_if_empty(&self, key: &[u8]) -> wkv::Result<()> {
    if let Some(payload) = self
      .obj_load(key, super::common::OBJ_TAG_HASH)
      .await?
      .into_payload()
      && HashObject::deserialize(&mut Cursor::new(payload))
        .map(|o| o.hash.pin().is_empty())
        .unwrap_or(false)
    {
      let _ = self.delete_string(key).await?;
      self.session_notfound.fetch_add(1, Relaxed);
    }
    Ok(())
  }
}
