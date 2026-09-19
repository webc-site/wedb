//! 自定义对象命令执行面（对标 libs/server/Custom/CustomRespCommands.cs）
//!
//! C# `TryCustomObjectCommand`：按命令类型分流 RMW_ObjectStore /
//! Read_ObjectStore，框架负责对象装载/回写，命令类四接口
//! （NeedInitialUpdate / Updater / Reader / NotFound）经
//! `customObjectCommand.functions` 触发；rust 侧对象以
//! `[类型标签][载荷]` 信封落库，装载/回写内聚本文件：
//! - RMW：NeedInitialUpdate 防空墓碑校验 → Updater 就地改写 →
//!   空对象整键回收（wedb 严格删空公理，对照 C# 工厂初建空对象常驻的
//!   刻意差异：空载荷不落库，杜绝幽灵空信封）
//! - Read：Reader 命中只读借用零拷贝直喂（杜绝整载荷堆分配）；NotFound 缺键应答（读不建键）
//!
//! WRONGTYPE 语义：键存在但信封标签不符（含字符串键），与 C# 统一
//! 存储上自定义对象命令作用于字符串键的 WRONGTYPE 口径一致。
//!
//! 分流 Read 与 RMW 两条求值路径：
//! - Read 臂：经 try_read_tag_sync / read_tag_with 闭包内借用切片直喂 reader，消除 ObjLoad<Vec<u8>> 中转
//! - RMW 臂：装载 owned Vec 供 Updater 就地改写

use core::cell::RefCell;

use wcol::object_payload::{obj_decode_custom, obj_encode_custom};
use wcustom::{CommandType, CustomObjectFns};
use wdev::Device;
use wkv::{BatchStoreSession, StoreResult};
use wresp::{
  cmd_strings::{RESP_ERR_ASYNC_REQUIRED, RESP_ERR_GENERIC, RESP_ERR_WRONG_TYPE, write_error_raw},
  ext::RespVecExt,
};
use wval::KeyTag;

use super::object_store_utils::{
  ObjLoad, obj_load_custom_async, obj_load_custom_sync, obj_save_custom_notified,
};
use crate::storage::session::storage_session::StorageSession;

/// 自定义对象命令同步执行结果
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CustomObjOutcome {
  /// 已闭环（应答/错误帧已写 output）
  Done,
  /// 磁盘候选降级：须异步重放（未写任何输出）
  Degrade,
}

/// 自定义对象命令修改动作
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CustomObjMutation {
  /// 删空回收键
  Delete,
  /// 回写新载荷
  Save(Vec<u8>),
}

/// 自定义对象命令 RMW 分派求值阶段
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CustomObjStep {
  /// 已闭环（成功帧或错误帧已写 output）
  Done,
  /// 磁盘候选降级：须异步重放（未写任何输出）
  Degrade,
  /// 产生变更动作（删空或写回）
  Mutate(CustomObjMutation),
}

/// 自定义对象只读命令分派求值原语（直接借用底层存储切片，零堆分配）
#[inline]
pub(crate) fn dispatch_custom_object_read(
  fns: &CustomObjectFns,
  payload: Option<&[u8]>,
  args: &[&[u8]],
  output: &mut Vec<u8>,
  resp_version: u8,
) {
  match payload {
    Some(p) => {
      let _ = (fns.reader)(p, args, output, resp_version);
    }
    None => {
      (fns.not_found)(args, output, resp_version);
    }
  }
}

/// 自定义对象读写改（RMW）求值原语
///
/// 对标 libs/server/Custom/CustomRespCommands.cs:TryCustomObjectCommand (RMW_ObjectStore)
///
/// 集中承载 NeedInitialUpdate 校验、Updater 变更、is_empty 判定与删空回收/防空墓碑逻辑
pub(crate) fn dispatch_custom_object_rmw(
  fns: &CustomObjectFns,
  loaded: ObjLoad<Vec<u8>>,
  args: &[&[u8]],
  output: &mut Vec<u8>,
  resp_version: u8,
) -> CustomObjStep {
  let (mut payload, existed) = match loaded {
    ObjLoad::Degrade => return CustomObjStep::Degrade,
    ObjLoad::WrongType => return CustomObjStep::Done,
    ObjLoad::Missing => {
      if !(fns.need_initial_update)(args, output, resp_version) {
        return CustomObjStep::Done;
      }
      (Vec::new(), false)
    }
    ObjLoad::Present(p) => (p, true),
  };

  if !(fns.updater)(&mut payload, args, output, resp_version) {
    return CustomObjStep::Done;
  }

  if (fns.is_empty)(&payload) {
    if existed {
      CustomObjStep::Mutate(CustomObjMutation::Delete)
    } else {
      CustomObjStep::Done
    }
  } else {
    CustomObjStep::Mutate(CustomObjMutation::Save(payload))
  }
}

/// 同步执行自定义对象命令的调用入参结构体（收敛入参，消除 too-many-arguments）
pub(crate) struct CustomObjectCall<'a> {
  pub cmd_type: CommandType,
  pub tag: u8,
  pub fns: &'a CustomObjectFns,
  pub key: &'a [u8],
  pub args: &'a [&'a [u8]],
  pub resp_version: u8,
}

/// 同步执行一条自定义对象命令（按 CommandType 分流 Read / RMW）；`key` 为用户键，`args` 为命令参数（不含 key）
pub(crate) fn try_custom_object_command<D: Device>(
  store: &BatchStoreSession<'_, D>,
  call: CustomObjectCall<'_>,
  output: &mut Vec<u8>,
) -> CustomObjOutcome {
  match call.cmd_type {
    CommandType::Read => try_custom_object_read_sync(store, &call, output),
    CommandType::ReadModifyWrite => try_custom_object_rmw_sync(store, &call, output),
  }
}

/// 同步 Read 臂：经 try_read_tag_sync 借用底层存储切片直喂 reader（零堆分配）
fn try_custom_object_read_sync<D: Device>(
  store: &BatchStoreSession<'_, D>,
  call: &CustomObjectCall<'_>,
  output: &mut Vec<u8>,
) -> CustomObjOutcome {
  // 读侧降级约定：RecordOnDisk 磁盘候选 / TTL 待裁决；NotFound 域内键缺失
  let res = store.try_read_tag_sync(call.key, KeyTag::ObjectEnvelope, |raw| {
    match obj_decode_custom(raw, call.tag) {
      None => Err(()), // 信封标签不符 -> WrongType
      Some(payload) => {
        // 零拷贝直喂 fns.reader
        dispatch_custom_object_read(
          call.fns,
          Some(payload),
          call.args,
          output,
          call.resp_version,
        );
        Ok(())
      }
    }
  });

  match res {
    // 命中且成功读取
    Ok(StoreResult::Success(Ok(()))) => CustomObjOutcome::Done,
    // 命中但信封标签不符 -> WRONGTYPE
    Ok(StoreResult::Success(Err(()))) => {
      write_error_raw(output, RESP_ERR_WRONG_TYPE);
      CustomObjOutcome::Done
    }
    // 信封域确认缺失（内存墓碑/无候选）：反探 String 域
    Ok(StoreResult::NotFound) => {
      match store.try_read_tag_sync(call.key, KeyTag::String, |raw| raw.first().copied()) {
        Ok(StoreResult::Success(_)) => {
          write_error_raw(output, RESP_ERR_WRONG_TYPE);
          CustomObjOutcome::Done
        }
        Ok(StoreResult::NotFound) => {
          dispatch_custom_object_read(call.fns, None, call.args, output, call.resp_version);
          CustomObjOutcome::Done
        }
        Ok(StoreResult::RecordOnDisk) | Err(_) => CustomObjOutcome::Degrade,
      }
    }
    Ok(StoreResult::RecordOnDisk) | Err(_) => CustomObjOutcome::Degrade,
  }
}

/// 同步 RMW 臂：装载 owned Vec<u8> 供 Updater 就地改写并按需回写或删空
fn try_custom_object_rmw_sync<D: Device>(
  store: &BatchStoreSession<'_, D>,
  call: &CustomObjectCall<'_>,
  output: &mut Vec<u8>,
) -> CustomObjOutcome {
  let loaded = obj_load_custom_sync(store, call.key, call.tag, output, |p| Some(p.to_vec()));
  let step = dispatch_custom_object_rmw(call.fns, loaded, call.args, output, call.resp_version);
  match step {
    CustomObjStep::Done => CustomObjOutcome::Done,
    CustomObjStep::Degrade => CustomObjOutcome::Degrade,
    CustomObjStep::Mutate(CustomObjMutation::Delete) => match store.try_delete_sync(call.key) {
      Ok(Ok(_)) => CustomObjOutcome::Done,
      Ok(Err(_)) => CustomObjOutcome::Degrade,
      Err(_) => {
        output.write_resp_error(RESP_ERR_GENERIC);
        CustomObjOutcome::Done
      }
    },
    CustomObjStep::Mutate(CustomObjMutation::Save(payload)) => {
      match obj_save_custom_notified(store, call.key, call.tag, &payload) {
        Ok(true) => CustomObjOutcome::Done,
        Ok(false) => CustomObjOutcome::Degrade,
        Err(_) => {
          output.write_resp_error(RESP_ERR_GENERIC);
          CustomObjOutcome::Done
        }
      }
    }
  }
}

/// 异步执行一条自定义对象命令（冷键降级后执行，按 CommandType 分流 Read / RMW）；
/// `cmd_refs` 为 [key, args...]，Err = 存储 IO 失败由调用方写 RESP_ERR_SLOW_PATH_STORAGE
pub(crate) async fn custom_object_slow<D: Device>(
  storage: &StorageSession<'_, D>,
  cmd_type: CommandType,
  tag: u8,
  fns: &CustomObjectFns,
  cmd_refs: &[&[u8]],
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  let Some((&key, args)) = cmd_refs.split_first() else {
    write_error_raw(output, RESP_ERR_ASYNC_REQUIRED);
    return Ok(());
  };
  match cmd_type {
    CommandType::Read => custom_object_read_async(storage, tag, fns, key, args, output).await,
    CommandType::ReadModifyWrite => {
      custom_object_rmw_async(storage, tag, fns, key, args, output).await
    }
  }
}

/// 异步 Read 臂：经 read_tag_with 在闭包内借用切片直喂 reader（零堆分配）
async fn custom_object_read_async<D: Device>(
  storage: &StorageSession<'_, D>,
  tag: u8,
  fns: &CustomObjectFns,
  key: &[u8],
  args: &[&[u8]],
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  let resp_version = storage.resp_protocol_version();
  let read = {
    let output_cell = RefCell::new(&mut *output);
    storage
      .read_tag_with(key, KeyTag::ObjectEnvelope, |raw| {
        match obj_decode_custom(raw, tag) {
          None => Err(()),
          Some(payload) => {
            let mut out_ref = output_cell.borrow_mut();
            dispatch_custom_object_read(fns, Some(payload), args, &mut out_ref, resp_version);
            Ok(())
          }
        }
      })
      .await
      .map_err(|_| ())?
  };

  match read {
    Some(Ok(())) => Ok(()),
    Some(Err(())) => {
      write_error_raw(output, RESP_ERR_WRONG_TYPE);
      Ok(())
    }
    None => {
      let str_hit = storage
        .read_tag_with(key, KeyTag::String, |raw| raw.first().copied())
        .await
        .map_err(|_| ())?;
      if str_hit.is_some() {
        write_error_raw(output, RESP_ERR_WRONG_TYPE);
      } else {
        dispatch_custom_object_read(fns, None, args, output, resp_version);
      }
      Ok(())
    }
  }
}

/// 异步 RMW 臂：装载 owned Vec<u8> 供 Updater 就地改写并回写或删空
async fn custom_object_rmw_async<D: Device>(
  storage: &StorageSession<'_, D>,
  tag: u8,
  fns: &CustomObjectFns,
  key: &[u8],
  args: &[&[u8]],
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  let loaded = obj_load_custom_async(storage, key, tag, output, |p| Some(p.to_vec()))
    .await
    .map_err(|_| ())?;
  let step = dispatch_custom_object_rmw(fns, loaded, args, output, storage.resp_protocol_version());
  match step {
    CustomObjStep::Done | CustomObjStep::Degrade => Ok(()),
    CustomObjStep::Mutate(CustomObjMutation::Delete) => {
      storage.delete_string(key).await.map(|_| ()).map_err(|_| ())
    }
    CustomObjStep::Mutate(CustomObjMutation::Save(payload)) => {
      match obj_save_custom_notified(&storage.batch, key, tag, &payload) {
        Ok(true) => Ok(()),
        Ok(false) => storage
          .upsert_tag(
            key,
            KeyTag::ObjectEnvelope,
            &obj_encode_custom(tag, &payload),
          )
          .await
          .map_err(|_| ()),
        Err(_) => Err(()),
      }
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn dummy_fns() -> CustomObjectFns {
    CustomObjectFns {
      need_initial_update: |_args, output, _resp_version| {
        if _args.first().copied() == Some(b"bad") {
          output.extend_from_slice(b"-ERR bad\r\n");
          false
        } else {
          true
        }
      },
      updater: |payload, args, output, _resp_version| {
        if args.first().copied() == Some(b"fail") {
          output.extend_from_slice(b"-ERR fail\r\n");
          false
        } else {
          payload.extend_from_slice(args.first().copied().unwrap_or(&[]));
          true
        }
      },
      reader: |payload, _args, output, _resp_version| {
        output.extend_from_slice(payload);
        true
      },
      not_found: |_args, output, resp_version| {
        output.write_resp_null_ver(resp_version);
      },
      is_empty: |payload| payload.is_empty(),
    }
  }

  #[test]
  fn test_dispatch_read() {
    let fns = dummy_fns();
    let mut out = Vec::new();

    // Missing -> NotFound
    dispatch_custom_object_read(&fns, None, &[], &mut out, 2);
    assert_eq!(out, b"$-1\r\n");

    // Present -> Reader (借用切片零拷贝)
    out.clear();
    dispatch_custom_object_read(&fns, Some(b"hello"), &[], &mut out, 2);
    assert_eq!(out, b"hello");
  }

  #[test]
  fn test_dispatch_rmw() {
    let fns = dummy_fns();
    let mut out = Vec::new();

    // Degrade
    let step = dispatch_custom_object_rmw(&fns, ObjLoad::Degrade, &[], &mut out, 2);
    assert_eq!(step, CustomObjStep::Degrade);

    // WrongType
    let step = dispatch_custom_object_rmw(&fns, ObjLoad::WrongType, &[], &mut out, 2);
    assert_eq!(step, CustomObjStep::Done);

    // Missing + need_initial_update returns false
    out.clear();
    let step = dispatch_custom_object_rmw(&fns, ObjLoad::Missing, &[b"bad"], &mut out, 2);
    assert_eq!(step, CustomObjStep::Done);
    assert_eq!(out, b"-ERR bad\r\n");

    // Missing + need_initial_update ok + updater returns false
    out.clear();
    let step = dispatch_custom_object_rmw(&fns, ObjLoad::Missing, &[b"fail"], &mut out, 2);
    assert_eq!(step, CustomObjStep::Done);
    assert_eq!(out, b"-ERR fail\r\n");

    // Missing + need_initial_update ok + updater ok + empty payload -> Done (no ghost tombstone)
    out.clear();
    let step = dispatch_custom_object_rmw(&fns, ObjLoad::Missing, &[], &mut out, 2);
    assert_eq!(step, CustomObjStep::Done);

    // Missing + need_initial_update ok + updater ok + non-empty payload -> Mutate(Save)
    out.clear();
    let step = dispatch_custom_object_rmw(&fns, ObjLoad::Missing, &[b"v1"], &mut out, 2);
    assert_eq!(
      step,
      CustomObjStep::Mutate(CustomObjMutation::Save(b"v1".to_vec()))
    );

    // Present + updater ok + non-empty payload -> Mutate(Save)
    out.clear();
    let step = dispatch_custom_object_rmw(
      &fns,
      ObjLoad::Present(b"p0".to_vec()),
      &[b"v1"],
      &mut out,
      2,
    );
    assert_eq!(
      step,
      CustomObjStep::Mutate(CustomObjMutation::Save(b"p0v1".to_vec()))
    );

    // Present + emptied payload -> Mutate(Delete) (strict empty deletion)
    let mut empty_fns = dummy_fns();
    empty_fns.updater = |payload, _args, _out, _resp_version| {
      payload.clear();
      true
    };
    let step = dispatch_custom_object_rmw(
      &empty_fns,
      ObjLoad::Present(b"old".to_vec()),
      &[],
      &mut out,
      2,
    );
    assert_eq!(step, CustomObjStep::Mutate(CustomObjMutation::Delete));
  }
}
