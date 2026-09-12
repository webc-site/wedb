//! 集合对象操作（对标 libs/server/Storage/Session/ObjectStore/SetOps.cs，C# 为 StorageSession partial）
//!
//! 全部经 [`StorageSession`] 对象信封读写 [`SetObject`]；集合为空时
//! 删除键（对齐 Redis 对象生命周期）。并/交/差以调用方键序单遍折叠。

use wdev::Device;

use super::{
  super::storage_session::StorageSession,
  common::{GarnetObjectPayload, RmwOutcome},
};
use crate::{objects::set::set_object::SetObject, types::GarnetStatus};

impl<'a, D: Device, CR: wkv::ConsistentReadFunctions> StorageSession<'a, D, CR> {
  /// 装载集合（缺失/类型不符快速出口；读路径专用）
  async fn set_load(&self, key: &[u8]) -> wkv::Result<Result<Option<SetObject>, GarnetStatus>> {
    self.typed_load::<SetObject>(key).await
  }

  /// SADD：批量添加成员，返回新增个数
  ///
  /// libs/server/Storage/Session/ObjectStore/SetOps.cs:SetAdd
  pub async fn set_add(&self, key: &[u8], members: &[&[u8]]) -> wkv::Result<(GarnetStatus, i64)> {
    let added = self
      .typed_rmw::<SetObject, i64>(key, true, |obj| {
        let mut n = 0i64;
        for m in members {
          if obj.add(m) {
            n += 1;
          }
        }
        Some(n)
      })
      .await?;
    match added {
      RmwOutcome::WrongType => Ok((GarnetStatus::WrongType, 0)),
      outcome => Ok((GarnetStatus::Ok, outcome.unwrap_or(0))),
    }
  }

  /// SREM：批量移除成员，返回移除个数（删空时整键回收但保留真实计数）
  ///
  /// 键缺失返回 NOTFOUND（C# NeedToCreate(SREM)=false，不物化空集合）。
  ///
  /// libs/server/Storage/Session/ObjectStore/SetOps.cs:SetRemove
  pub async fn set_remove(
    &self,
    key: &[u8],
    members: &[&[u8]],
  ) -> wkv::Result<(GarnetStatus, i64)> {
    self
      .typed_remove::<SetObject, i64>(key, 0, |obj| {
        let mut n = 0i64;
        for m in members {
          if obj.remove(m) {
            n += 1;
          }
        }
        Some(n)
      })
      .await
  }

  /// SCARD：成员数
  ///
  /// 键缺失返回 NOTFOUND（C# SetLength → ReadObjectStoreOperation，RESP 层同答 :0）。
  ///
  /// libs/server/Storage/Session/ObjectStore/SetOps.cs:SetLength
  pub async fn set_length(&self, key: &[u8]) -> wkv::Result<(GarnetStatus, usize)> {
    self
      .typed_query::<SetObject, _>(key, 0, |obj| obj.count())
      .await
  }

  /// SMEMBERS：全部成员
  ///
  /// 键缺失返回 NOTFOUND（C# SetMembers → ReadObjectStoreOperation，RESP 层同答空数组）。
  ///
  /// libs/server/Storage/Session/ObjectStore/SetOps.cs:SetMembers
  pub async fn set_members(&self, key: &[u8]) -> wkv::Result<(GarnetStatus, Vec<Vec<u8>>)> {
    self
      .typed_query::<SetObject, _>(key, Vec::new(), |obj| obj.get_keys())
      .await
  }

  /// SPOP：弹出成员（`count` 上限），返回被弹成员
  ///
  /// 键缺失返回 NOTFOUND（C# NeedToCreate(SPOP)=false，RESP 层据此写空数组）。
  ///
  /// libs/server/Storage/Session/ObjectStore/SetOps.cs:SetPop
  pub async fn set_pop(
    &self,
    key: &[u8],
    count: usize,
  ) -> wkv::Result<(GarnetStatus, Vec<Vec<u8>>)> {
    self
      .typed_remove::<SetObject, Vec<Vec<u8>>>(key, Vec::new(), |obj| {
        let mut out = Vec::new();
        for _ in 0..count {
          match obj.pop() {
            Some(m) => out.push(m),
            None => break,
          }
        }
        Some(out)
      })
      .await
  }

  /// SMOVE：跨集合搬移成员，返回是否成功
  ///
  /// 判定序对齐 C# SetMove：源缺失 → NOTFOUND；同键先于目标读取与成员
  /// 检查恒返 0；目标类型错先于成员摘除传播（避免误删后写入失败）。
  ///
  /// libs/server/Storage/Session/ObjectStore/SetOps.cs:SetMove
  pub async fn set_move(
    &self,
    src: &[u8],
    dest: &[u8],
    member: &[u8],
  ) -> wkv::Result<(GarnetStatus, bool)> {
    let s = match self.set_load(src).await? {
      Err(st) => return Ok((st, false)),
      // C# srcGetStatus == NOTFOUND → 返回 NOTFOUND（非 OK/0）
      Ok(None) => return Ok((GarnetStatus::NotFound, false)),
      Ok(Some(s)) => s,
    };
    // 同键：不做任何操作即返回（先于目标读取与成员检查，恒返 0）
    if src == dest {
      return Ok((GarnetStatus::Ok, false));
    }
    // 目标类型错：传播 WRONGTYPE（先于成员摘除，避免误删后写入失败）
    if let Err(st) = self.set_load(dest).await? {
      return Ok((st, false));
    }
    if !s.set.contains(member) {
      return Ok((GarnetStatus::Ok, false));
    }
    // 先从源集合摘除（摘除后为空则整键回收），再写入目标集合
    let (status, removed) = self
      .typed_remove::<SetObject, bool>(src, false, |obj| Some(obj.remove(member)))
      .await?;
    if !removed {
      return Ok((status, false));
    }
    self.set_add(dest, &[member]).await?;
    Ok((GarnetStatus::Ok, true))
  }

  /// SINTER：多集合交集（以首键为基底折叠；错误类型键传播 WRONGTYPE，缺键视为空集）
  ///
  /// libs/server/Storage/Session/ObjectStore/SetOps.cs:SetIntersect
  pub async fn set_intersect(&self, keys: &[&[u8]]) -> wkv::Result<(GarnetStatus, Vec<Vec<u8>>)> {
    let Some(first) = keys.first() else {
      return Ok((GarnetStatus::Ok, Vec::new()));
    };
    let base = match self.set_load(first).await? {
      Err(s) => return Ok((s, Vec::new())),
      Ok(None) => return Ok((GarnetStatus::Ok, Vec::new())),
      Ok(Some(o)) => o,
    };
    let mut result: Vec<Vec<u8>> = base.get_keys();
    for key in &keys[1..] {
      match self.set_load(key).await? {
        Err(s) => return Ok((s, Vec::new())),
        Ok(None) => return Ok((GarnetStatus::Ok, Vec::new())),
        Ok(Some(other)) => {
          result.retain(|m| other.set.contains(m));
        }
      }
      if result.is_empty() {
        break;
      }
    }
    Ok((GarnetStatus::Ok, result))
  }

  /// SINTERSTORE：交集写入目标键（空交集回收目标键），返回基数
  ///
  /// 键列表为空时提前返回 OK、不动目标键（对齐 C# keys.Length == 0 守卫）。
  ///
  /// libs/server/Storage/Session/ObjectStore/SetOps.cs:SetIntersectStore
  pub async fn set_intersect_store(
    &self,
    dest: &[u8],
    keys: &[&[u8]],
  ) -> wkv::Result<(GarnetStatus, usize)> {
    if keys.is_empty() {
      return Ok((GarnetStatus::Ok, 0));
    }
    let (status, members) = self.set_intersect(keys).await?;
    if status != GarnetStatus::Ok {
      return Ok((status, 0));
    }
    let _ = self.delete_string(dest).await?;
    if members.is_empty() {
      return Ok((GarnetStatus::Ok, 0));
    }
    let refs: Vec<&[u8]> = members.iter().map(Vec::as_slice).collect();
    let (_, n) = self.set_add(dest, &refs).await?;
    Ok((GarnetStatus::Ok, n as usize))
  }

  /// SUNION：多集合并集（错误类型键传播 WRONGTYPE，缺键视为空集）
  ///
  /// libs/server/Storage/Session/ObjectStore/SetOps.cs:SetUnion
  pub async fn set_union(&self, keys: &[&[u8]]) -> wkv::Result<(GarnetStatus, Vec<Vec<u8>>)> {
    let mut merged: gxhash::HashSet<Vec<u8>> = gxhash::HashSet::default();
    for key in keys {
      match self.set_load(key).await? {
        Err(s) => return Ok((s, Vec::new())),
        Ok(Some(obj)) => {
          for m in &obj.set {
            merged.insert(m.clone());
          }
        }
        Ok(None) => {}
      }
    }
    Ok((GarnetStatus::Ok, merged.into_iter().collect()))
  }

  /// SUNIONSTORE：并集写入目标键（空并集回收目标键），返回基数
  ///
  /// 键列表为空时提前返回 OK、不动目标键（对齐 C# keys.Length == 0 守卫）。
  ///
  /// libs/server/Storage/Session/ObjectStore/SetOps.cs:SetUnionStore
  pub async fn set_union_store(
    &self,
    dest: &[u8],
    keys: &[&[u8]],
  ) -> wkv::Result<(GarnetStatus, usize)> {
    if keys.is_empty() {
      return Ok((GarnetStatus::Ok, 0));
    }
    let (status, members) = self.set_union(keys).await?;
    if status != GarnetStatus::Ok {
      return Ok((status, 0));
    }
    let _ = self.delete_string(dest).await?;
    if members.is_empty() {
      return Ok((GarnetStatus::Ok, 0));
    }
    let refs: Vec<&[u8]> = members.iter().map(Vec::as_slice).collect();
    let (_, n) = self.set_add(dest, &refs).await?;
    Ok((GarnetStatus::Ok, n as usize))
  }

  /// SISMEMBER：成员存在性
  ///
  /// 键缺失返回 NOTFOUND（对齐 C# ReadObjectStoreOperation 三态）。
  ///
  /// libs/server/Storage/Session/ObjectStore/SetOps.cs:SetIsMember
  pub async fn set_is_member(
    &self,
    key: &[u8],
    member: &[u8],
  ) -> wkv::Result<(GarnetStatus, bool)> {
    self
      .typed_query::<SetObject, _>(key, false, |obj| obj.set.contains(member))
      .await
  }

  /// SRANDMEMBER：随机成员（`count` 负数允许重复）
  ///
  /// 键缺失随 SMEMBERS 返回 NOTFOUND。
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
        let idx = fastrand::usize(..pool.len());
        out.push(pool.swap_remove(idx));
      }
      out
    };
    Ok((GarnetStatus::Ok, out))
  }

  /// SDIFF：多集合差集（首键减其余；错误类型键传播 WRONGTYPE，缺键视为空集）
  ///
  /// libs/server/Storage/Session/ObjectStore/SetOps.cs:SetDiff
  pub async fn set_diff(&self, keys: &[&[u8]]) -> wkv::Result<(GarnetStatus, Vec<Vec<u8>>)> {
    let Some(first) = keys.first() else {
      return Ok((GarnetStatus::Ok, Vec::new()));
    };
    let base = match self.set_load(first).await? {
      Err(s) => return Ok((s, Vec::new())),
      Ok(None) => return Ok((GarnetStatus::Ok, Vec::new())),
      Ok(Some(o)) => o,
    };
    let mut result = base.get_keys();
    for key in &keys[1..] {
      match self.set_load(key).await? {
        Err(s) => return Ok((s, Vec::new())),
        Ok(Some(other)) => {
          result.retain(|m| !other.set.contains(m));
        }
        Ok(None) => {}
      }
      if result.is_empty() {
        break;
      }
    }
    Ok((GarnetStatus::Ok, result))
  }

  /// SDIFFSTORE：差集写入目标键（空差集回收目标键），返回基数
  ///
  /// 键列表为空时提前返回 OK、不动目标键（对齐 C# keys.Length == 0 守卫）。
  ///
  /// libs/server/Storage/Session/ObjectStore/SetOps.cs:SetDiffStore
  pub async fn set_diff_store(
    &self,
    dest: &[u8],
    keys: &[&[u8]],
  ) -> wkv::Result<(GarnetStatus, usize)> {
    if keys.is_empty() {
      return Ok((GarnetStatus::Ok, 0));
    }
    let (status, members) = self.set_diff(keys).await?;
    if status != GarnetStatus::Ok {
      return Ok((status, 0));
    }
    let _ = self.delete_string(dest).await?;
    if members.is_empty() {
      return Ok((GarnetStatus::Ok, 0));
    }
    let refs: Vec<&[u8]> = members.iter().map(Vec::as_slice).collect();
    let (_, n) = self.set_add(dest, &refs).await?;
    Ok((GarnetStatus::Ok, n as usize))
  }

  /// SINTERCARD：交集基数（不物化成员列表）
  ///
  /// libs/server/Storage/Session/ObjectStore/SetOps.cs:SetIntersectLength
  pub async fn set_intersect_length(&self, keys: &[&[u8]]) -> wkv::Result<(GarnetStatus, usize)> {
    let (status, members) = self.set_intersect(keys).await?;
    if status != GarnetStatus::Ok {
      return Ok((status, 0));
    }
    Ok((GarnetStatus::Ok, members.len()))
  }

  /// SSCAN：遍历集合成员
  ///
  /// libs/server/Storage/Session/ObjectStore/SetOps.cs:SetScan
  pub async fn set_scan(
    &self,
    key: &[u8],
    cursor: &[u8],
    pattern: &[u8],
    count: usize,
  ) -> wkv::Result<(GarnetStatus, Vec<u8>, Vec<Vec<u8>>)> {
    let extract = |payload: &[u8]| -> Option<Vec<Vec<u8>>> {
      Some(SetObject::deserialize_from_slice(payload).members())
    };
    self
      .object_scan(
        key,
        wval::GarnetObjectType::Set as u8,
        pattern,
        cursor,
        count,
        extract,
      )
      .await
  }
}
