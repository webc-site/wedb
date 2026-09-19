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
//! - MultiKeyRead：静态清单 [`wcustom::KeyScope::MultiRead`] 形态位驱动的
//!   逐键 Read 循环（RedisJSON JSON.MGET 兼容面，C# 无对位——
//!   `modules/GarnetJSON/JsonModule.cs` 只注册 JSON.SET / JSON.GET）；与
//!   单键读共用同一探测面与同一 `reader` / `not_found` 执行体，会话执行臂
//!   不再按命令名比串特判（转写规范：键形态知识入编译期静态清单）
//!
//! WRONGTYPE 语义：键存在但信封标签不符（含字符串键），与 C# 统一
//! 存储上自定义对象命令作用于字符串键的 WRONGTYPE 口径一致。
//!
//! 分流三条求值路径（同步快路与冷键降级异步重放同款分型）：
//! - Read 臂：经 try_read_tag_sync / read_tag_with 闭包内借用切片直喂 reader，消除 ObjLoad<Vec<u8>> 中转
//! - RMW 臂：装载 owned Vec 供 Updater 就地改写
//! - 多键读臂：数组头 + 逐键 Read 探测；任一键为磁盘候选即整体降级重放

use core::cell::RefCell;

use wcol::object_payload::{obj_decode_custom, obj_encode_custom};
use wcustom::{CommandType, CustomArgs, CustomCommandMeta, CustomObjectFns, RespVersion};
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

/// 自定义对象命令单键只读探测的键级结局（单键臂与多键循环臂共用）
enum ReadProbe {
  /// 已产出应答帧（命中喂 reader，键缺失走 not_found）
  Handled,
  /// 键存在但类型不符（信封标签非本类型 / 落在 String 域）
  WrongType,
  /// 磁盘候选 / 存储故障：须降级异步重放（未写任何字节）
  Degrade,
}

/// 自定义对象命令执行上下文（信封标签 + 静态执行体 + 会话协议版本）
///
/// 三条臂共用同一组不变入参，免逐臂重复传参；标签在信封编解码边界
/// 收窄为 u8 线域（`obj_decode_custom` 同收标准段与扩展段标签）。
#[derive(Clone, Copy)]
struct CustomObjCtx<'a> {
  /// 信封内层类型标签（wval::CustomObjectType 分配单点）
  tag: u8,
  fns: &'a CustomObjectFns,
  resp_version: RespVersion,
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
  /// 命令类型（Read / ReadModifyWrite；多键形态下不参与分派，见 [`CustomArgs::Multi`]）
  pub cmd_type: CommandType,
  /// 键与命令入参（会话侧按静态清单键作用域 [`CustomArgs`] 拆定的形态）
  pub args: CustomArgs<'a>,
  /// 信封内层类型标签
  pub tag: u8,
  /// 静态执行体
  pub fns: &'a CustomObjectFns,
  /// 会话 RESP 协议版本
  pub resp_version: RespVersion,
}

/// 同步执行一条自定义对象命令（按静态清单键作用域与 CommandType 分派）
pub(crate) fn try_custom_object_command<D: Device>(
  store: &BatchStoreSession<'_, D>,
  call: CustomObjectCall<'_>,
  output: &mut Vec<u8>,
) -> CustomObjOutcome {
  let ctx = CustomObjCtx {
    tag: call.tag,
    fns: call.fns,
    resp_version: call.resp_version,
  };
  match call.args {
    CustomArgs::Single { key, args } => match call.cmd_type {
      CommandType::Read => try_custom_object_read_sync(store, &ctx, key, args, output),
      CommandType::ReadModifyWrite => try_custom_object_rmw_sync(store, &ctx, key, args, output),
    },
    CustomArgs::Multi { keys, args } => {
      try_custom_object_multi_read_sync(store, &ctx, keys, args, output)
    }
  }
}

/// 同步单键只读探测（唯一读探测面，多键循环臂逐键复用）：信封命中直喂
/// reader（零堆分配），信封域确认缺失反探 String 域判类型不符/键缺失，
/// 磁盘候选回 Degrade
fn probe_custom_read_sync<D: Device>(
  store: &BatchStoreSession<'_, D>,
  ctx: &CustomObjCtx<'_>,
  key: &[u8],
  args: &[&[u8]],
  out: &mut Vec<u8>,
) -> ReadProbe {
  // 读侧降级约定：RecordOnDisk 磁盘候选 / TTL 待裁决；NotFound 域内键缺失
  let envelope = store.try_read_tag_sync(key, KeyTag::ObjectEnvelope, |raw| {
    match obj_decode_custom(raw, ctx.tag) {
      None => Err(()), // 信封标签不符 -> WrongType
      Some(payload) => {
        // 零拷贝直喂 fns.reader
        dispatch_custom_object_read(ctx.fns, Some(payload), args, out, ctx.resp_version);
        Ok(())
      }
    }
  });
  match envelope {
    // 命中且成功读取
    Ok(StoreResult::Success(Ok(()))) => ReadProbe::Handled,
    // 命中但信封标签不符
    Ok(StoreResult::Success(Err(()))) => ReadProbe::WrongType,
    // 信封域确认缺失（内存墓碑/无候选）：反探 String 域
    Ok(StoreResult::NotFound) => {
      match store.try_read_tag_sync(key, KeyTag::String, |raw| raw.first().copied()) {
        Ok(StoreResult::Success(_)) => ReadProbe::WrongType,
        Ok(StoreResult::NotFound) => {
          dispatch_custom_object_read(ctx.fns, None, args, out, ctx.resp_version);
          ReadProbe::Handled
        }
        Ok(StoreResult::RecordOnDisk) | Err(_) => ReadProbe::Degrade,
      }
    }
    Ok(StoreResult::RecordOnDisk) | Err(_) => ReadProbe::Degrade,
  }
}

/// 同步 Read 臂：单键只读探测，类型不符按 WRONGTYPE 报错
fn try_custom_object_read_sync<D: Device>(
  store: &BatchStoreSession<'_, D>,
  ctx: &CustomObjCtx<'_>,
  key: &[u8],
  args: &[&[u8]],
  output: &mut Vec<u8>,
) -> CustomObjOutcome {
  match probe_custom_read_sync(store, ctx, key, args, output) {
    ReadProbe::Handled => CustomObjOutcome::Done,
    ReadProbe::WrongType => {
      write_error_raw(output, RESP_ERR_WRONG_TYPE);
      CustomObjOutcome::Done
    }
    ReadProbe::Degrade => CustomObjOutcome::Degrade,
  }
}

/// 同步多键读臂（[`wcustom::KeyScope::MultiRead`] 形态）：数组头 + 逐键只读
/// 探测，键缺失与类型不符均按逐元素口径回 nil（单元素失败不拖垮整条批量读）；
/// 任一键为磁盘候选即丢弃本次输出整体降级异步重放（降级契约：零字节残留）
fn try_custom_object_multi_read_sync<D: Device>(
  store: &BatchStoreSession<'_, D>,
  ctx: &CustomObjCtx<'_>,
  keys: &[&[u8]],
  args: &[&[u8]],
  output: &mut Vec<u8>,
) -> CustomObjOutcome {
  // 本条应答起点（同批可含前序命令输出，降级时按此回退而非另立草稿缓冲）
  let base = output.len();
  output.write_resp_array_len(keys.len());
  for &key in keys {
    match probe_custom_read_sync(store, ctx, key, args, output) {
      ReadProbe::Handled => {}
      ReadProbe::WrongType => output.write_resp_null_ver(ctx.resp_version),
      ReadProbe::Degrade => {
        output.truncate(base);
        return CustomObjOutcome::Degrade;
      }
    }
  }
  CustomObjOutcome::Done
}

/// 同步 RMW 臂：装载 owned Vec<u8> 供 Updater 就地改写并按需回写或删空
fn try_custom_object_rmw_sync<D: Device>(
  store: &BatchStoreSession<'_, D>,
  ctx: &CustomObjCtx<'_>,
  key: &[u8],
  args: &[&[u8]],
  output: &mut Vec<u8>,
) -> CustomObjOutcome {
  let loaded = obj_load_custom_sync(store, key, ctx.tag, output, |p| Some(p.to_vec()));
  let step = dispatch_custom_object_rmw(ctx.fns, loaded, args, output, ctx.resp_version);
  match step {
    CustomObjStep::Done => CustomObjOutcome::Done,
    CustomObjStep::Degrade => CustomObjOutcome::Degrade,
    CustomObjStep::Mutate(CustomObjMutation::Delete) => match store.try_delete_sync(key) {
      Ok(Ok(_)) => CustomObjOutcome::Done,
      Ok(Err(_)) => CustomObjOutcome::Degrade,
      Err(_) => {
        output.write_resp_error(RESP_ERR_GENERIC);
        CustomObjOutcome::Done
      }
    },
    CustomObjStep::Mutate(CustomObjMutation::Save(payload)) => {
      match obj_save_custom_notified(store, key, ctx.tag, &payload) {
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

/// 异步执行一条自定义对象命令（冷键降级后执行，与会话快路径同一
/// [`CustomArgs`] 拆分与同一臂分型）；`cmd_refs` 为完整参数域
/// [key..., 命令入参...]，Err = 存储 IO 失败由调用方写
/// RESP_ERR_SLOW_PATH_STORAGE
pub(crate) async fn custom_object_slow<D: Device>(
  storage: &StorageSession<'_, D>,
  tag: u8,
  meta: &CustomCommandMeta,
  cmd_refs: &[&[u8]],
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  let ctx = CustomObjCtx {
    tag,
    fns: &meta.fns,
    resp_version: storage.resp_protocol_version(),
  };
  // 键与入参的布局判定与会话快路径同一单点（KeyScope::split）；拆不出键
  // = 回放帧不成形，按慢路径既有口径回明确错误帧，绝不静默
  let Some(args) = meta.key_scope.split(cmd_refs) else {
    write_error_raw(output, RESP_ERR_ASYNC_REQUIRED);
    return Ok(());
  };
  match args {
    CustomArgs::Single { key, args } => match meta.command_type {
      CommandType::Read => custom_object_read_async(storage, &ctx, key, args, output).await,
      CommandType::ReadModifyWrite => {
        custom_object_rmw_async(storage, &ctx, key, args, output).await
      }
    },
    CustomArgs::Multi { keys, args } => {
      custom_object_multi_read_async(storage, &ctx, keys, args, output).await
    }
  }
}

/// 异步单键只读探测（异步重放段与多键循环共用）：闭包内借用切片直喂
/// reader（零堆分配）
async fn probe_custom_read_async<D: Device>(
  storage: &StorageSession<'_, D>,
  ctx: &CustomObjCtx<'_>,
  key: &[u8],
  args: &[&[u8]],
  out: &mut Vec<u8>,
) -> Result<ReadProbe, ()> {
  let envelope = {
    let output_cell = RefCell::new(&mut *out);
    storage
      .read_tag_with(key, KeyTag::ObjectEnvelope, |raw| {
        match obj_decode_custom(raw, ctx.tag) {
          None => Err(()),
          Some(payload) => {
            let mut out_ref = output_cell.borrow_mut();
            dispatch_custom_object_read(
              ctx.fns,
              Some(payload),
              args,
              &mut out_ref,
              ctx.resp_version,
            );
            Ok(())
          }
        }
      })
      .await
      .map_err(|_| ())?
  };
  match envelope {
    Some(Ok(())) => Ok(ReadProbe::Handled),
    Some(Err(())) => Ok(ReadProbe::WrongType),
    None => {
      let str_hit = storage
        .read_tag_with(key, KeyTag::String, |raw| raw.first().copied())
        .await
        .map_err(|_| ())?;
      if str_hit.is_some() {
        return Ok(ReadProbe::WrongType);
      }
      dispatch_custom_object_read(ctx.fns, None, args, out, ctx.resp_version);
      Ok(ReadProbe::Handled)
    }
  }
}

/// 异步 Read 臂
async fn custom_object_read_async<D: Device>(
  storage: &StorageSession<'_, D>,
  ctx: &CustomObjCtx<'_>,
  key: &[u8],
  args: &[&[u8]],
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  match probe_custom_read_async(storage, ctx, key, args, output).await? {
    ReadProbe::WrongType => write_error_raw(output, RESP_ERR_WRONG_TYPE),
    // 异步重放段已无更冷层可降级（存储失败以 Err 上抛），Degrade 不可达
    ReadProbe::Handled | ReadProbe::Degrade => {}
  }
  Ok(())
}

/// 异步多键读臂：数组头 + 逐键只读探测，非命中元素（类型不符/键缺失/
/// 已无更冷层可读的磁盘候选）回 nil
async fn custom_object_multi_read_async<D: Device>(
  storage: &StorageSession<'_, D>,
  ctx: &CustomObjCtx<'_>,
  keys: &[&[u8]],
  args: &[&[u8]],
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  output.write_resp_array_len(keys.len());
  for &key in keys {
    match probe_custom_read_async(storage, ctx, key, args, output).await? {
      ReadProbe::Handled => {}
      ReadProbe::WrongType | ReadProbe::Degrade => output.write_resp_null_ver(ctx.resp_version),
    }
  }
  Ok(())
}

/// 异步 RMW 臂：装载 owned Vec<u8> 供 Updater 就地改写并回写或删空
async fn custom_object_rmw_async<D: Device>(
  storage: &StorageSession<'_, D>,
  ctx: &CustomObjCtx<'_>,
  key: &[u8],
  args: &[&[u8]],
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  let loaded = obj_load_custom_async(storage, key, ctx.tag, output, |p| Some(p.to_vec()))
    .await
    .map_err(|_| ())?;
  let step = dispatch_custom_object_rmw(ctx.fns, loaded, args, output, ctx.resp_version);
  match step {
    CustomObjStep::Done | CustomObjStep::Degrade => Ok(()),
    CustomObjStep::Mutate(CustomObjMutation::Delete) => {
      storage.delete_string(key).await.map(|_| ()).map_err(|_| ())
    }
    CustomObjStep::Mutate(CustomObjMutation::Save(payload)) => {
      match obj_save_custom_notified(&storage.batch, key, ctx.tag, &payload) {
        Ok(true) => Ok(()),
        Ok(false) => storage
          .upsert_tag(
            key,
            KeyTag::ObjectEnvelope,
            &obj_encode_custom(ctx.tag, &payload),
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
