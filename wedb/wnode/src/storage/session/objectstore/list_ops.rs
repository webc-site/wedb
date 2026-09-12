//! 列表对象操作（对标 libs/server/Storage/Session/ObjectStore/ListOps.cs，C# 为 StorageSession partial）
//!
//! 全部经 [`StorageSession`] 对象信封读写 [`ListObject`]（VecDeque 载体）；
//! 列表为空时删除键（对齐 Redis 对象生命周期）。

use std::num::NonZeroI64;

use wdev::Device;

use super::{super::storage_session::StorageSession, common::RmwOutcome};
use crate::{
  objects::list::list_object::{ListObject, ListOperation, OperationDirection},
  types::GarnetStatus,
};

impl<'a, D: Device, CR: wkv::ConsistentReadFunctions> StorageSession<'a, D, CR> {
  /// 装载列表（缺失/类型不符快速出口；读路径专用）
  async fn list_load(&self, key: &[u8]) -> wkv::Result<Result<Option<ListObject>, GarnetStatus>> {
    self.typed_load::<ListObject>(key).await
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
    let outcome = self
      .typed_rmw::<ListObject, usize>(key, !only_if_exists, |obj| {
        for v in values {
          obj.operate_basic(
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
      .await?;
    match outcome {
      RmwOutcome::WrongType => Ok((GarnetStatus::WrongType, None)),
      // LPUSHX/RPUSHX 键缺失：C# NeedToCreate=false，RMW 直返 NOTFOUND（不物化）
      RmwOutcome::Aborted => Ok((GarnetStatus::NotFound, None)),
      RmwOutcome::Written(len) => Ok((GarnetStatus::Ok, Some(len))),
    }
  }

  /// LPOP/RPOP：单端弹出
  ///
  /// 键缺失返回 NOTFOUND（C# NeedToCreate(LPOP/RPOP)=false，不物化空列表），
  /// RESP 层据此写 nil，外显与 Redis 一致。
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
      .map(|(s, mut v)| (s, v.pop()))
  }

  /// LPOP/RPOP count 语义批量弹出（键缺失返回 NOTFOUND，对齐 C# NeedToCreate）
  ///
  /// libs/server/Storage/Session/ObjectStore/ListOps.cs:ListPopMultiple
  pub async fn list_pop_multiple(
    &self,
    key: &[u8],
    count: usize,
    direction: OperationDirection,
  ) -> wkv::Result<(GarnetStatus, Vec<Vec<u8>>)> {
    self
      .typed_remove::<ListObject, Vec<Vec<u8>>>(key, Vec::new(), |obj| {
        let op = if direction == OperationDirection::Left {
          ListOperation::Lpop
        } else {
          ListOperation::Rpop
        };
        let mut out = Vec::new();
        for _ in 0..count {
          match obj.operate_basic(op, b"") {
            Some(v) => out.push(v),
            None => break,
          }
        }
        Some(out)
      })
      .await
  }

  /// LLEN：列表长度
  ///
  /// 键缺失返回 NOTFOUND（C# ListLength → ReadObjectStoreOperation，RESP 层同答 :0）。
  ///
  /// libs/server/Storage/Session/ObjectStore/ListOps.cs:ListLength
  pub async fn list_length(&self, key: &[u8]) -> wkv::Result<(GarnetStatus, usize)> {
    self
      .typed_query::<ListObject, _>(key, 0, |obj| obj.count())
      .await
  }

  /// LMOVE/RPOPLPUSH：跨列表搬移（源弹出 + 目标推入）
  ///
  /// 源键缺失显式折算为 OK（C# ListMove 对 GET NOTFOUND 直接返回 OK、
  /// element 为空）；WRONGTYPE 原样传播。
  ///
  /// libs/server/Storage/Session/ObjectStore/ListOps.cs:ListMove
  pub async fn list_move(
    &self,
    src: &[u8],
    dest: &[u8],
    src_dir: OperationDirection,
    dest_dir: OperationDirection,
  ) -> wkv::Result<(GarnetStatus, Option<Vec<u8>>)> {
    if src == dest {
      // 同键原地旋转（对标 C# ListMove:sameKey）
      // 同向移动或单元素列表旋转等价于 no-op，直接窥视元素返回，
      // 严禁先 pop 再 push（否则会触发列表暂空导致 TTL 丢失）。
      let (status, val) = self
        .typed_query::<ListObject, Option<Option<Vec<u8>>>>(src, None, |obj| {
          let count = obj.list.len();
          if count == 0 {
            Some(None)
          } else if src_dir == dest_dir || count == 1 {
            let elem = if src_dir == OperationDirection::Left {
              obj.list.front().cloned()
            } else {
              obj.list.back().cloned()
            };
            Some(elem)
          } else {
            None
          }
        })
        .await?;

      if status != GarnetStatus::Ok && status != GarnetStatus::NotFound {
        return Ok((status, None));
      }
      if status == GarnetStatus::NotFound {
        return Ok((GarnetStatus::Ok, None));
      }
      if let Some(fast_element) = val {
        return Ok((GarnetStatus::Ok, fast_element));
      }

      let outcome = self
        .typed_rmw::<ListObject, Vec<u8>>(src, false, |obj| {
          let val = if src_dir == OperationDirection::Left {
            obj.list.pop_front()?
          } else {
            obj.list.pop_back()?
          };
          if dest_dir == OperationDirection::Left {
            obj.list.push_front(val.clone());
          } else {
            obj.list.push_back(val.clone());
          }
          Some(val)
        })
        .await?;
      return match outcome {
        RmwOutcome::WrongType => Ok((GarnetStatus::WrongType, None)),
        RmwOutcome::Aborted => Ok((GarnetStatus::Ok, None)),
        RmwOutcome::Written(val) => Ok((GarnetStatus::Ok, Some(val))),
      };
    }

    // 异键移动：先预检目标键类型（对标 C# GET(destinationKey) WRONGTYPE 拦截，防误删源列表）
    if let Err(status) = self.list_load(dest).await? {
      return Ok((status, None));
    }

    let (status, popped) = self.list_pop(src, src_dir).await?;
    let Some(value) = popped else {
      return Ok((
        if status == GarnetStatus::NotFound {
          GarnetStatus::Ok
        } else {
          status
        },
        None,
      ));
    };

    let (push_status, _) = self.list_push(dest, &[&value], dest_dir, false).await?;
    if push_status != GarnetStatus::Ok {
      return Ok((push_status, None));
    }
    Ok((GarnetStatus::Ok, Some(value)))
  }

  /// LTRIM：区间裁剪（Redis 闭区间负索引语义；裁剪至空时整键回收并返回 OK）
  ///
  /// libs/server/Storage/Session/ObjectStore/ListOps.cs:ListTrim
  pub async fn list_trim(&self, key: &[u8], start: i64, stop: i64) -> wkv::Result<GarnetStatus> {
    let (status, ()) = self
      .typed_remove::<ListObject, ()>(key, (), |obj| {
        obj.trim(start as isize, stop as isize);
        Some(())
      })
      .await?;
    Ok(status)
  }

  /// LPOS：定位成员第 `rank` 次出现（`maxlen` 限制扫描长度），None 表示未命中
  ///
  /// 键缺失返回 NOTFOUND（C# ListPosition → ReadObjectStoreOperation；RESP 层
  /// 据此写 null / 空数组）；错误类型键传播 WRONGTYPE。`rank` 以 NonZeroI64
  /// 承载（C# 对象层对 RANK 0 报错，typed 面以类型约束根治，杜绝静默改写）。
  ///
  /// libs/server/Storage/Session/ObjectStore/ListOps.cs:ListPosition
  pub async fn list_position(
    &self,
    key: &[u8],
    element: &[u8],
    rank: NonZeroI64,
    maxlen: Option<usize>,
  ) -> wkv::Result<(GarnetStatus, Option<usize>)> {
    match self.list_load(key).await? {
      Err(s) => Ok((s, None)),
      Ok(None) => Ok((GarnetStatus::NotFound, None)),
      Ok(Some(obj)) => {
        let guard = &obj.list;
        let rank = rank.get();
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
  /// 键缺失返回 NOTFOUND（C# ListRange → ReadObjectStoreOperation，RESP 层同答空数组）。
  ///
  /// libs/server/Storage/Session/ObjectStore/ListOps.cs:ListRange
  pub async fn list_range(
    &self,
    key: &[u8],
    start: i64,
    stop: i64,
  ) -> wkv::Result<(GarnetStatus, Vec<Vec<u8>>)> {
    self
      .typed_query::<ListObject, _>(key, Vec::new(), |obj| {
        obj.range(start as isize, stop as isize)
      })
      .await
  }

  /// LINSERT：在基准元素前/后插入，返回插入后长度（基准缺失返回 None）
  ///
  /// 键缺失返回 NOTFOUND（C# NeedToCreate(LINSERT)=false）；键存在但基准
  /// 缺失返回 OK+None（C# result1=-1，无写回）。
  ///
  /// libs/server/Storage/Session/ObjectStore/ListOps.cs:ListInsert
  pub async fn list_insert(
    &self,
    key: &[u8],
    pivot: &[u8],
    element: &[u8],
    before: bool,
  ) -> wkv::Result<(GarnetStatus, Option<usize>)> {
    // 闭包是否执行过：区分"键缺失"（闭包未跑，Aborted）与"键在但基准缺失"
    //（闭包拒绝写回，Aborted）
    let mut entered = false;
    let len = self
      .typed_rmw::<ListObject, usize>(key, false, |obj| {
        entered = true;
        let guard = &mut obj.list;
        let pos = guard.iter().position(|v| v.as_slice() == pivot)?;
        let idx = if before { pos } else { pos + 1 };
        guard.insert(idx, element.to_vec());
        Some(guard.len())
      })
      .await?;
    match len {
      RmwOutcome::WrongType => Ok((GarnetStatus::WrongType, None)),
      // 键在但基准缺失：OK+None（无变更不回写）
      RmwOutcome::Aborted if entered => Ok((GarnetStatus::Ok, None)),
      // 键缺失：NOTFOUND
      RmwOutcome::Aborted => Ok((GarnetStatus::NotFound, None)),
      RmwOutcome::Written(n) => Ok((GarnetStatus::Ok, Some(n))),
    }
  }

  /// LINDEX：按索引取元素（负索引自尾计数）
  ///
  /// 键缺失返回 NOTFOUND（RESP 层同答 null）。
  ///
  /// libs/server/Storage/Session/ObjectStore/ListOps.cs:ListIndex
  pub async fn list_index(
    &self,
    key: &[u8],
    index: i64,
  ) -> wkv::Result<(GarnetStatus, Option<Vec<u8>>)> {
    self
      .typed_query::<ListObject, _>(key, None, |obj| obj.index(index as isize))
      .await
  }

  /// LREM：移除 `count` 个匹配元素（正数自头、负数自尾、0 全部），返回移除数
  ///
  /// 键缺失返回 NOTFOUND（C# NeedToCreate(LREM)=false）。
  ///
  /// libs/server/Storage/Session/ObjectStore/ListOps.cs:ListRemove
  pub async fn list_remove(
    &self,
    key: &[u8],
    element: &[u8],
    count: i64,
  ) -> wkv::Result<(GarnetStatus, i64)> {
    self
      .typed_remove::<ListObject, i64>(key, 0, |obj| {
        let guard = &mut obj.list;
        let n = if count == 0 {
          let old_len = guard.len();
          guard.retain(|x| x.as_slice() != element);
          (old_len - guard.len()) as i64
        } else if count > 0 {
          let mut remaining = count as usize;
          guard.retain(|x| {
            if remaining > 0 && x.as_slice() == element {
              remaining -= 1;
              false
            } else {
              true
            }
          });
          count - (remaining as i64)
        } else {
          let to_remove = count.unsigned_abs() as usize;
          let total_matches = guard.iter().filter(|x| x.as_slice() == element).count();
          let actual_remove = to_remove.min(total_matches);
          let skip_matches = total_matches - actual_remove;
          let mut seen = 0usize;
          guard.retain(|x| {
            if x.as_slice() == element {
              seen += 1;
              seen <= skip_matches
            } else {
              true
            }
          });
          actual_remove as i64
        };
        Some(n)
      })
      .await
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
    let done = self
      .typed_rmw::<ListObject, bool>(key, false, |obj| {
        let guard = &mut obj.list;
        let len = guard.len() as i64;
        let idx = if index < 0 { len + index } else { index };
        if idx < 0 || idx >= len {
          return Some(false);
        }
        guard[idx as usize] = element.to_vec();
        Some(true)
      })
      .await?;
    match done {
      RmwOutcome::WrongType => Ok(GarnetStatus::WrongType),
      RmwOutcome::Written(true) => Ok(GarnetStatus::Ok),
      // Aborted（键缺失，不物化空列表）/ Written(false)（索引越界）
      _ => Ok(GarnetStatus::NotFound),
    }
  }
}
