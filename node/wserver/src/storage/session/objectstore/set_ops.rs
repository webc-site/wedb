//! 集合对象操作（对标 libs/server/Storage/Session/ObjectStore/SetOps.cs，C# 为 StorageSession partial）
//!
//! 全部经 [`StorageSession`] 对象信封读写 wobject [`SetObject`]；集合为空时
//! 删除键（对齐 Redis 对象生命周期）。并/交/差以调用方键序单遍折叠。

use std::io::Cursor;

use wdev::Device;
use wobject::set::set_object::{SetObject, SetOperation};

use super::{super::storage_session::StorageSession, common::ObjState};
use crate::api::garnet_status::GarnetStatus;

impl<'a, D: Device> StorageSession<'a, D> {
  /// 集合对象读-改-写：载荷解码为 SetObject 后交闭包变更，返回前自动回写
  async fn set_rmw<R>(
    &self,
    key: &[u8],
    f: impl FnOnce(&mut SetObject) -> Option<R>,
  ) -> wkv::Result<Option<R>> {
    self
      .rmw_object_store_operation(key, super::common::OBJ_TAG_SET, |payload| {
        let mut obj = match payload {
          Some(bytes) => SetObject::deserialize(&mut Cursor::new(bytes)).unwrap_or_default(),
          None => SetObject::new(),
        };
        let r = f(&mut obj)?;
        let mut out = Vec::new();
        obj.serialize(&mut out).ok()?;
        Some((out, r))
      })
      .await
  }

  /// 装载集合（缺失/类型不符快速出口）
  async fn set_load(&self, key: &[u8]) -> wkv::Result<Result<Option<SetObject>, GarnetStatus>> {
    Ok(
      match self.obj_load(key, super::common::OBJ_TAG_SET).await? {
        ObjState::Absent => Ok(None),
        ObjState::WrongType => Err(GarnetStatus::WrongType),
        ObjState::Present(p) => Ok(Some(
          SetObject::deserialize(&mut Cursor::new(p)).unwrap_or_default(),
        )),
      },
    )
  }

  /// SADD：批量添加成员，返回新增个数
  ///
  /// libs/server/Storage/Session/ObjectStore/SetOps.cs:SetAdd
  pub async fn set_add(&self, key: &[u8], members: &[&[u8]]) -> wkv::Result<(GarnetStatus, i64)> {
    match self.set_load(key).await? {
      Err(s) => Ok((s, 0)),
      Ok(_) => {
        let added = self
          .set_rmw(key, |obj| {
            let mut n = 0i64;
            for m in members {
              if !obj.operate(SetOperation::Sadd, m) {
                n += 1;
              }
            }
            Some(n)
          })
          .await?
          .unwrap_or(0);
        Ok((GarnetStatus::Ok, added))
      }
    }
  }

  /// SREM：批量移除成员，返回移除个数
  ///
  /// libs/server/Storage/Session/ObjectStore/SetOps.cs:SetRemove
  pub async fn set_remove(
    &self,
    key: &[u8],
    members: &[&[u8]],
  ) -> wkv::Result<(GarnetStatus, i64)> {
    match self.set_load(key).await? {
      Err(s) => Ok((s, 0)),
      Ok(None) => Ok((GarnetStatus::Ok, 0)),
      Ok(Some(_)) => {
        let removed = self
          .set_rmw(key, |obj| {
            let mut n = 0i64;
            for m in members {
              if obj.operate(SetOperation::Srem, m) {
                n += 1;
              }
            }
            if obj.set.pin().is_empty() {
              None // 空集合：放弃写回，由调用方删键
            } else {
              Some(n)
            }
          })
          .await?;
        match removed {
          Some(n) => Ok((GarnetStatus::Ok, n)),
          None => {
            let _ = self.delete_string(key).await?;
            Ok((GarnetStatus::Ok, 0))
          }
        }
      }
    }
  }

  /// SCARD：成员数
  ///
  /// libs/server/Storage/Session/ObjectStore/SetOps.cs:SetLength
  pub async fn set_length(&self, key: &[u8]) -> wkv::Result<(GarnetStatus, usize)> {
    match self.set_load(key).await? {
      Err(s) => Ok((s, 0)),
      Ok(None) => Ok((GarnetStatus::Ok, 0)),
      Ok(Some(obj)) => Ok((GarnetStatus::Ok, obj.count())),
    }
  }

  /// SMEMBERS：全部成员
  ///
  /// libs/server/Storage/Session/ObjectStore/SetOps.cs:SetMembers
  pub async fn set_members(&self, key: &[u8]) -> wkv::Result<(GarnetStatus, Vec<Vec<u8>>)> {
    match self.set_load(key).await? {
      Err(s) => Ok((s, Vec::new())),
      Ok(None) => Ok((GarnetStatus::Ok, Vec::new())),
      Ok(Some(obj)) => Ok((GarnetStatus::Ok, obj.get_keys())),
    }
  }

  /// SPOP：弹出成员（`count` 上限），返回被弹成员
  ///
  /// libs/server/Storage/Session/ObjectStore/SetOps.cs:SetPop
  pub async fn set_pop(
    &self,
    key: &[u8],
    count: usize,
  ) -> wkv::Result<(GarnetStatus, Vec<Vec<u8>>)> {
    match self.set_load(key).await? {
      Err(s) => Ok((s, Vec::new())),
      Ok(None) => Ok((GarnetStatus::Ok, Vec::new())),
      Ok(Some(_)) => {
        let popped = self
          .set_rmw(key, |obj| {
            let mut out = Vec::new();
            for _ in 0..count {
              match obj.pop() {
                Some(m) => out.push(m),
                None => break,
              }
            }
            Some(out)
          })
          .await?
          .unwrap_or_default();
        // 弹空后整键回收
        if !popped.is_empty() {
          let _ = self.delete_if_set_empty(key).await;
        }
        Ok((GarnetStatus::Ok, popped))
      }
    }
  }

  /// SMOVE：跨集合搬移成员，返回是否成功
  ///
  /// libs/server/Storage/Session/ObjectStore/SetOps.cs:SetMove
  pub async fn set_move(
    &self,
    src: &[u8],
    dest: &[u8],
    member: &[u8],
  ) -> wkv::Result<(GarnetStatus, bool)> {
    match self.set_load(src).await? {
      Err(s) => Ok((s, false)),
      Ok(None) => Ok((GarnetStatus::Ok, false)),
      Ok(Some(s)) if s.set.pin().get(member).is_none() => Ok((GarnetStatus::Ok, false)),
      Ok(Some(_)) => {
        if src == dest {
          return Ok((GarnetStatus::Ok, true));
        }
        // 先从源集合摘除（摘除后为空则整键回收），再写入目标集合
        let removed = self
          .set_rmw(src, |obj| {
            obj
              .operate(SetOperation::Srem, member)
              .then(|| obj.set.pin().is_empty())
          })
          .await?
          .unwrap_or(false);
        if removed && let (_, 0) = self.set_length(src).await? {
          let _ = self.delete_string(src).await?;
        }
        self.set_add(dest, &[member]).await?;
        Ok((GarnetStatus::Ok, true))
      }
    }
  }

  /// SINTER：多集合交集（以首键为基底折叠）
  ///
  /// libs/server/Storage/Session/ObjectStore/SetOps.cs:SetIntersect
  pub async fn set_intersect(&self, keys: &[&[u8]]) -> wkv::Result<(GarnetStatus, Vec<Vec<u8>>)> {
    let Some(first) = keys.first() else {
      return Ok((GarnetStatus::Ok, Vec::new()));
    };
    let Ok(Some(base)) = self.set_load(first).await? else {
      return Ok((GarnetStatus::Ok, Vec::new()));
    };
    let mut result: Vec<Vec<u8>> = base.get_keys();
    for key in &keys[1..] {
      let Ok(Some(other)) = self.set_load(key).await? else {
        return Ok((GarnetStatus::Ok, Vec::new()));
      };
      let pin = other.set.pin();
      result.retain(|m| pin.contains(m));
      if result.is_empty() {
        break;
      }
    }
    Ok((GarnetStatus::Ok, result))
  }

  /// SINTERSTORE：交集写入目标键，返回基数
  ///
  /// libs/server/Storage/Session/ObjectStore/SetOps.cs:SetIntersectStore
  pub async fn set_intersect_store(
    &self,
    dest: &[u8],
    keys: &[&[u8]],
  ) -> wkv::Result<(GarnetStatus, usize)> {
    let (status, members) = self.set_intersect(keys).await?;
    if status != GarnetStatus::Ok {
      return Ok((status, 0));
    }
    let refs: Vec<&[u8]> = members.iter().map(Vec::as_slice).collect();
    let _ = self.delete_string(dest).await?;
    let (_, n) = self.set_add(dest, &refs).await?;
    Ok((GarnetStatus::Ok, n as usize))
  }

  /// SUNION：多集合并集
  ///
  /// libs/server/Storage/Session/ObjectStore/SetOps.cs:SetUnion
  pub async fn set_union(&self, keys: &[&[u8]]) -> wkv::Result<(GarnetStatus, Vec<Vec<u8>>)> {
    // 去重折叠：以 papaya 集合承接，最终整体导出
    let merged = SetObject::new();
    let pin = merged.set.pin();
    for key in keys {
      if let Ok(Some(obj)) = self.set_load(key).await? {
        for m in obj.get_keys() {
          pin.insert(m);
        }
      }
    }
    drop(pin);
    Ok((GarnetStatus::Ok, merged.get_keys()))
  }

  /// SUNIONSTORE：并集写入目标键，返回基数
  ///
  /// libs/server/Storage/Session/ObjectStore/SetOps.cs:SetUnionStore
  pub async fn set_union_store(
    &self,
    dest: &[u8],
    keys: &[&[u8]],
  ) -> wkv::Result<(GarnetStatus, usize)> {
    let (status, members) = self.set_union(keys).await?;
    if status != GarnetStatus::Ok {
      return Ok((status, 0));
    }
    let refs: Vec<&[u8]> = members.iter().map(Vec::as_slice).collect();
    let _ = self.delete_string(dest).await?;
    let (_, n) = self.set_add(dest, &refs).await?;
    Ok((GarnetStatus::Ok, n as usize))
  }

  /// SISMEMBER：成员存在性
  ///
  /// libs/server/Storage/Session/ObjectStore/SetOps.cs:SetIsMember
  pub async fn set_is_member(
    &self,
    key: &[u8],
    member: &[u8],
  ) -> wkv::Result<(GarnetStatus, bool)> {
    match self.set_load(key).await? {
      Err(s) => Ok((s, false)),
      Ok(None) => Ok((GarnetStatus::Ok, false)),
      Ok(Some(obj)) => Ok((GarnetStatus::Ok, obj.set.pin().contains(member))),
    }
  }

  /// SRANDMEMBER：随机成员（`count` 负数允许重复）
  ///
  /// libs/server/Storage/Session/ObjectStore/SetOps.cs:SetRandomMember
  pub async fn set_random_member(
    &self,
    key: &[u8],
    count: i64,
  ) -> wkv::Result<(GarnetStatus, Vec<Vec<u8>>)> {
    let (status, members) = self.set_members(key).await?;
    if status != GarnetStatus::Ok || members.is_empty() {
      return Ok((status, Vec::new()));
    }
    let out = if count < 0 {
      let n = count.unsigned_abs() as usize;
      (0..n)
        .map(|_| members[fastrand::usize(..members.len())].clone())
        .collect()
    } else {
      let n = (count as usize).min(members.len());
      let mut pool = members;
      let mut out = Vec::with_capacity(n);
      for _ in 0..n {
        let last = pool.len() - 1;
        let idx = fastrand::usize(..pool.len());
        pool.swap(idx, last);
        out.push(pool.pop().unwrap_or_default());
      }
      out
    };
    Ok((GarnetStatus::Ok, out))
  }

  /// SDIFF：多集合差集（首键减其余）
  ///
  /// libs/server/Storage/Session/ObjectStore/SetOps.cs:SetDiff
  pub async fn set_diff(&self, keys: &[&[u8]]) -> wkv::Result<(GarnetStatus, Vec<Vec<u8>>)> {
    let Some(first) = keys.first() else {
      return Ok((GarnetStatus::Ok, Vec::new()));
    };
    let Ok(Some(base)) = self.set_load(first).await? else {
      return Ok((GarnetStatus::Ok, Vec::new()));
    };
    let mut result = base.get_keys();
    for key in &keys[1..] {
      if let Ok(Some(other)) = self.set_load(key).await? {
        let pin = other.set.pin();
        result.retain(|m| !pin.contains(m));
      }
    }
    Ok((GarnetStatus::Ok, result))
  }

  /// SDIFFSTORE：差集写入目标键，返回基数
  ///
  /// libs/server/Storage/Session/ObjectStore/SetOps.cs:SetDiffStore
  pub async fn set_diff_store(
    &self,
    dest: &[u8],
    keys: &[&[u8]],
  ) -> wkv::Result<(GarnetStatus, usize)> {
    let (status, members) = self.set_diff(keys).await?;
    if status != GarnetStatus::Ok {
      return Ok((status, 0));
    }
    let refs: Vec<&[u8]> = members.iter().map(Vec::as_slice).collect();
    let _ = self.delete_string(dest).await?;
    let (_, n) = self.set_add(dest, &refs).await?;
    Ok((GarnetStatus::Ok, n as usize))
  }

  /// SINTERCARD：交集基数（不物化成员列表）
  ///
  /// libs/server/Storage/Session/ObjectStore/SetOps.cs:SetIntersectLength
  pub async fn set_intersect_length(&self, keys: &[&[u8]]) -> wkv::Result<(GarnetStatus, usize)> {
    let (_, members) = self.set_intersect(keys).await?;
    Ok((GarnetStatus::Ok, members.len()))
  }

  /// 空集合键回收
  async fn delete_if_set_empty(&self, key: &[u8]) -> wkv::Result<()> {
    if let Ok(Some(obj)) = self.set_load(key).await?
      && obj.set.pin().is_empty()
    {
      let _ = self.delete_string(key).await?;
    }
    Ok(())
  }
}
