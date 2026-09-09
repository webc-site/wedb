//! 哈希对象操作（对标 libs/server/Storage/Session/ObjectStore/HashOps.cs，C# 为 StorageSession partial）
//!
//! 全部经 [`StorageSession`] 对象信封读写 wobject [`HashObject`]，字段级 TTL
//! 缺口见 `hash_time_to_live`。哈希对象为空时删除键（对齐 Redis 对象生命周期）。

use std::{io::Cursor, str, sync::atomic::Ordering::Relaxed};

use wdev::Device;
use wobject::hash::hash_object::HashObject;

use super::{super::storage_session::StorageSession, common::ObjState};
use crate::api::garnet_status::GarnetStatus;

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
              if obj.operate(5 /* HDEL */, field, b"").is_some() {
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
        Ok((GarnetStatus::Ok, obj.operate(2 /* HGET */, field, b"")))
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

  /// HRANDFIELD：随机返回字段（`count` 为负取绝对值个不重复字段；`with_values` 附带值）
  ///
  /// libs/server/Storage/Session/ObjectStore/HashOps.cs:HashRandomField
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
        let mut keys = obj.get_keys();
        let want = if count < 0 {
          // 负数：允许少于字段数（不重复取样至 |count|）
          (count.unsigned_abs() as usize).min(keys.len())
        } else {
          (count as usize).min(keys.len())
        };
        // 洗牌取样（fastrand 不可用域，用 papaya 迭代序 + 简单交换洗牌）
        let mut out = Vec::with_capacity(want);
        for _ in 0..want {
          let last = keys.len() - 1;
          let idx = fastrand::usize(..keys.len());
          keys.swap(idx, last);
          let k = keys.pop().unwrap_or_default();
          let v = obj.hash.pin().get(&k).cloned();
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
  /// libs/server/Storage/Session/ObjectStore/HashOps.cs:HashSetWhenNotExists（IGarnetApi.HashSet 同链路）
  pub async fn hash_set(
    &self,
    key: &[u8],
    fields: &[(&[u8], &[u8])],
    nx: bool,
  ) -> wkv::Result<(GarnetStatus, i64)> {
    let added = self
      .hash_rmw(key, |obj| {
        let mut n = 0i64;
        for (field, value) in fields {
          if nx && obj.hash.pin().contains_key(*field) {
            continue;
          }
          let existed = obj.hash.pin().contains_key(*field);
          obj.operate(0 /* HSET */, field, value);
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
  /// libs/server/Storage/Session/ObjectStore/HashOps.cs:HashIncrement
  pub async fn hash_increment(
    &self,
    key: &[u8],
    field: &[u8],
    delta: &[u8],
    float: bool,
  ) -> wkv::Result<(GarnetStatus, Option<Vec<u8>>)> {
    let delta_str = match str::from_utf8(delta) {
      Ok(s) => s,
      Err(_) => return Ok((GarnetStatus::WrongType, None)),
    };
    match self
      .hash_rmw(key, |obj| {
        let current = obj.operate(2 /* HGET */, field, b"");
        if float {
          let cur = current
            .as_deref()
            .and_then(|b| str::from_utf8(b).ok())
            .and_then(|s| s.trim().parse::<f64>().ok())
            .unwrap_or(0.0);
          let Some(d) = delta_str.trim().parse::<f64>().ok() else {
            return (false, None);
          };
          let new = cur + d;
          let text = format!("{new:.17}");
          obj.operate(0 /* HSET */, field, text.as_bytes());
          (true, Some(text.into_bytes()))
        } else {
          let cur = current
            .as_deref()
            .and_then(|b| str::from_utf8(b).ok())
            .and_then(|s| s.parse::<i64>().ok());
          let Ok(d) = delta_str.parse::<i64>() else {
            return (false, None);
          };
          match cur {
            Some(c) => match c.checked_add(d) {
              Some(n) => {
                let text = n.to_string();
                obj.operate(0 /* HSET */, field, text.as_bytes());
                (true, Some(text.into_bytes()))
              }
              None => (false, None), // 溢出：不写入
            },
            None => {
              let text = d.to_string();
              obj.operate(0 /* HSET */, field, text.as_bytes());
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
