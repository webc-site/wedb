//! 列表对象操作（对标 libs/server/Storage/Session/ObjectStore/ListOps.cs，C# 为 StorageSession partial）
//!
//! 全部经 [`StorageSession`] 对象信封读写 wobject [`ListObject`]（VecDeque 载体）；
//! 列表为空时删除键（对齐 Redis 对象生命周期）。

use std::io::Cursor;

use wdev::Device;
use wobject::list::list_object::{ListObject, ListOperation, OperationDirection};

use super::{super::storage_session::StorageSession, common::ObjState};
use crate::api::garnet_status::GarnetStatus;

impl<'a, D: Device> StorageSession<'a, D> {
  /// 列表对象读-改-写：载荷解码为 ListObject 后交闭包变更，返回前自动回写
  async fn list_rmw<R>(
    &self,
    key: &[u8],
    f: impl FnOnce(&mut ListObject) -> Option<R>,
  ) -> wkv::Result<Option<R>> {
    self
      .rmw_object_store_operation(key, super::common::OBJ_TAG_LIST, |payload| {
        let mut obj = match payload {
          Some(bytes) => ListObject::deserialize(&mut Cursor::new(bytes)).unwrap_or_default(),
          None => ListObject::new(),
        };
        let r = f(&mut obj)?;
        let mut out = Vec::new();
        obj.serialize(&mut out).ok()?;
        Some((out, r))
      })
      .await
  }

  /// 装载列表（缺失/类型不符快速出口）
  async fn list_load(&self, key: &[u8]) -> wkv::Result<Result<Option<ListObject>, GarnetStatus>> {
    Ok(
      match self.obj_load(key, super::common::OBJ_TAG_LIST).await? {
        ObjState::Absent => Ok(None),
        ObjState::WrongType => Err(GarnetStatus::WrongType),
        ObjState::Present(p) => Ok(Some(
          ListObject::deserialize(&mut Cursor::new(p)).unwrap_or_default(),
        )),
      },
    )
  }

  /// LPUSH/RPUSH/LPUSHX/RPUSHX：方向推入，返回推入后长度
  ///
  /// libs/server/Storage/Session/ObjectStore/ListOps.cs:ListPush
  pub async fn list_push(
    &self,
    key: &[u8],
    values: &[&[u8]],
    direction: OperationDirection,
    only_if_exists: bool,
  ) -> wkv::Result<(GarnetStatus, Option<usize>)> {
    match self.list_load(key).await? {
      Err(s) => Ok((s, None)),
      Ok(None) if only_if_exists => Ok((GarnetStatus::Ok, None)),
      Ok(_) => {
        let len = self
          .list_rmw(key, |obj| {
            for v in values {
              obj.operate(
                if direction == OperationDirection::Left {
                  ListOperation::Lpush
                } else {
                  ListOperation::Rpush
                },
                v,
              );
            }
            Some(obj.count())
          })
          .await?
          .unwrap_or(0);
        Ok((GarnetStatus::Ok, Some(len)))
      }
    }
  }

  /// LPOP/RPOP：单端弹出
  ///
  /// libs/server/Storage/Session/ObjectStore/ListOps.cs:ListPop
  pub async fn list_pop(
    &self,
    key: &[u8],
    direction: OperationDirection,
  ) -> wkv::Result<(GarnetStatus, Option<Vec<u8>>)> {
    self
      .list_pop_multiple(key, 1, direction)
      .await
      .map(|(s, mut v)| {
        (
          s,
          if v.is_empty() {
            None
          } else {
            Some(v.remove(0))
          },
        )
      })
  }

  /// LPOP/RPOP count 语义批量弹出
  ///
  /// libs/server/Storage/Session/ObjectStore/ListOps.cs:ListPopMultiple
  pub async fn list_pop_multiple(
    &self,
    key: &[u8],
    count: usize,
    direction: OperationDirection,
  ) -> wkv::Result<(GarnetStatus, Vec<Vec<u8>>)> {
    match self.list_load(key).await? {
      Err(s) => Ok((s, Vec::new())),
      Ok(None) => Ok((GarnetStatus::Ok, Vec::new())),
      Ok(Some(_)) => {
        let popped = self
          .list_rmw(key, |obj| {
            let op = if direction == OperationDirection::Left {
              ListOperation::Lpop
            } else {
              ListOperation::Rpop
            };
            let mut out = Vec::new();
            for _ in 0..count {
              match obj.operate(op, b"") {
                Some(v) => out.push(v),
                None => break,
              }
            }
            Some((out, obj.list.lock().is_empty()))
          })
          .await?;
        let out = self.finalize_removal(key, popped, Vec::new()).await?;
        Ok((GarnetStatus::Ok, out))
      }
    }
  }

  /// LLEN：列表长度
  ///
  /// libs/server/Storage/Session/ObjectStore/ListOps.cs:ListLength
  pub async fn list_length(&self, key: &[u8]) -> wkv::Result<(GarnetStatus, usize)> {
    match self.list_load(key).await? {
      Err(s) => Ok((s, 0)),
      Ok(None) => Ok((GarnetStatus::Ok, 0)),
      Ok(Some(obj)) => Ok((GarnetStatus::Ok, obj.count())),
    }
  }

  /// LMOVE/RPOPLPUSH：跨列表搬移（源弹出 + 目标推入）
  ///
  /// libs/server/Storage/Session/ObjectStore/ListOps.cs:ListMove
  pub async fn list_move(
    &self,
    src: &[u8],
    dest: &[u8],
    src_dir: OperationDirection,
    dest_dir: OperationDirection,
  ) -> wkv::Result<(GarnetStatus, Option<Vec<u8>>)> {
    let (status, popped) = self.list_pop(src, src_dir).await?;
    let Some(value) = popped else {
      return Ok((status, None));
    };
    self.list_push(dest, &[&value], dest_dir, false).await?;
    Ok((GarnetStatus::Ok, Some(value)))
  }

  /// LTRIM：区间裁剪（Redis 闭区间负索引语义；裁剪至空时整键回收并返回 OK）
  ///
  /// libs/server/Storage/Session/ObjectStore/ListOps.cs:ListTrim
  pub async fn list_trim(&self, key: &[u8], start: i64, stop: i64) -> wkv::Result<GarnetStatus> {
    match self.list_load(key).await? {
      Err(s) => Ok(s),
      Ok(None) => Ok(GarnetStatus::NotFound),
      Ok(Some(_)) => {
        let trimmed = self
          .list_rmw(key, |obj| {
            obj.trim(start as isize, stop as isize);
            Some(((), obj.list.lock().is_empty()))
          })
          .await?;
        let () = self.finalize_removal(key, trimmed, ()).await?;
        Ok(GarnetStatus::Ok)
      }
    }
  }

  /// LPOS：定位成员第 `rank` 次出现（`maxlen` 限制扫描长度），None 表示未命中
  ///
  /// libs/server/Storage/Session/ObjectStore/ListOps.cs:ListPosition
  pub async fn list_position(
    &self,
    key: &[u8],
    element: &[u8],
    rank: i64,
    maxlen: Option<usize>,
  ) -> wkv::Result<(GarnetStatus, Option<usize>)> {
    match self.list_load(key).await? {
      Err(_) | Ok(None) => Ok((GarnetStatus::Ok, None)),
      Ok(Some(obj)) => {
        let guard = obj.list.lock();
        let rank = if rank == 0 { 1 } else { rank };
        let (start, step, skip): (i64, i64, u64) = if rank > 0 {
          (0, 1, (rank - 1) as u64)
        } else {
          (guard.len() as i64 - 1, -1, rank.unsigned_abs() - 1)
        };
        let mut hits = 0u64;
        let mut i = start;
        let mut scanned = 0usize;
        while i >= 0 && (i as usize) < guard.len() {
          if maxlen.is_some_and(|m| scanned >= m) {
            break;
          }
          if guard[i as usize] == element {
            hits += 1;
            if hits > skip {
              return Ok((GarnetStatus::Ok, Some(i as usize)));
            }
          }
          i += step;
          scanned += 1;
        }
        Ok((GarnetStatus::Ok, None))
      }
    }
  }

  /// LRANGE：区间切片（Redis 闭区间负索引语义）
  ///
  /// libs/server/Storage/Session/ObjectStore/ListOps.cs:ListRange
  pub async fn list_range(
    &self,
    key: &[u8],
    start: i64,
    stop: i64,
  ) -> wkv::Result<(GarnetStatus, Vec<Vec<u8>>)> {
    match self.list_load(key).await? {
      Err(s) => Ok((s, Vec::new())),
      Ok(None) => Ok((GarnetStatus::Ok, Vec::new())),
      Ok(Some(obj)) => Ok((GarnetStatus::Ok, obj.range(start as isize, stop as isize))),
    }
  }

  /// LINSERT：在基准元素前/后插入，返回插入后长度（基准缺失返回 None）
  ///
  /// libs/server/Storage/Session/ObjectStore/ListOps.cs:ListInsert
  pub async fn list_insert(
    &self,
    key: &[u8],
    pivot: &[u8],
    element: &[u8],
    before: bool,
  ) -> wkv::Result<(GarnetStatus, Option<usize>)> {
    match self.list_load(key).await? {
      Err(s) => Ok((s, None)),
      Ok(None) => Ok((GarnetStatus::Ok, None)),
      Ok(Some(_)) => {
        let len = self
          .list_rmw(key, |obj| {
            let mut guard = obj.list.lock();
            let pos = guard.iter().position(|v| *v == *pivot)?;
            let idx = if before { pos } else { pos + 1 };
            guard.insert(idx, element.to_vec());
            Some(guard.len())
          })
          .await?;
        Ok((GarnetStatus::Ok, len))
      }
    }
  }

  /// LINDEX：按索引取元素（负索引自尾计数）
  ///
  /// libs/server/Storage/Session/ObjectStore/ListOps.cs:ListIndex
  pub async fn list_index(
    &self,
    key: &[u8],
    index: i64,
  ) -> wkv::Result<(GarnetStatus, Option<Vec<u8>>)> {
    match self.list_load(key).await? {
      Err(s) => Ok((s, None)),
      Ok(None) => Ok((GarnetStatus::Ok, None)),
      Ok(Some(obj)) => Ok((GarnetStatus::Ok, obj.index(index as isize))),
    }
  }

  /// LREM：移除 `count` 个匹配元素（正数自头、负数自尾、0 全部），返回移除数
  ///
  /// libs/server/Storage/Session/ObjectStore/ListOps.cs:ListRemove
  pub async fn list_remove(
    &self,
    key: &[u8],
    element: &[u8],
    count: i64,
  ) -> wkv::Result<(GarnetStatus, i64)> {
    match self.list_load(key).await? {
      Err(s) => Ok((s, 0)),
      Ok(None) => Ok((GarnetStatus::Ok, 0)),
      Ok(Some(_)) => {
        let removed = self
          .list_rmw(key, |obj| {
            let mut guard = obj.list.lock();
            let mut n = 0i64;
            if count >= 0 {
              let mut remaining = if count == 0 { i64::MAX } else { count };
              let mut i = 0usize;
              while i < guard.len() && remaining > 0 {
                if *guard[i].as_slice() == *element {
                  guard.remove(i);
                  remaining -= 1;
                  n += 1;
                } else {
                  i += 1;
                }
              }
            } else {
              let mut remaining = count.unsigned_abs() as i64;
              let mut i = guard.len();
              while i > 0 && remaining > 0 {
                i -= 1;
                if *guard[i].as_slice() == *element {
                  guard.remove(i);
                  remaining -= 1;
                  n += 1;
                }
              }
            }
            Some((n, guard.is_empty()))
          })
          .await?;
        let n = self.finalize_removal(key, removed, 0).await?;
        Ok((GarnetStatus::Ok, n))
      }
    }
  }

  /// LSET：按索引覆写元素
  ///
  /// libs/server/Storage/Session/ObjectStore/ListOps.cs:ListSet
  pub async fn list_set(
    &self,
    key: &[u8],
    index: i64,
    element: &[u8],
  ) -> wkv::Result<GarnetStatus> {
    match self.list_load(key).await? {
      Err(s) => Ok(s),
      Ok(None) => Ok(GarnetStatus::NotFound),
      Ok(Some(_)) => {
        let done = self
          .list_rmw(key, |obj| {
            let mut guard = obj.list.lock();
            let len = guard.len() as i64;
            let idx = if index < 0 { len + index } else { index };
            if idx < 0 || idx >= len {
              return Some(false);
            }
            guard[idx as usize] = element.to_vec();
            Some(true)
          })
          .await?
          .unwrap_or(false);
        Ok(if done {
          GarnetStatus::Ok
        } else {
          GarnetStatus::NotFound
        })
      }
    }
  }
}
