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
//! - Read：Reader 命中只读；NotFound 缺键应答（读不建键）
//!
//! WRONGTYPE 语义：键存在但信封标签不符（含字符串键），与 C# 统一
//! 存储上自定义对象命令作用于字符串键的 WRONGTYPE 口径一致。

use wcustom::{CommandType, CustomObjectCommand, CustomObjectFns};
use wdev::Device;
use wkv::BatchStoreSession;
use wresp::{RespVecExt, cmd_strings::RESP_ERR_GENERIC};

use super::object_store_utils::{ObjLoad, obj_load_typed_sync, obj_save_notified};

/// 自定义对象命令同步执行结果
pub(crate) enum CustomObjOutcome {
  /// 已闭环（应答/错误帧已写 output）
  Done,
  /// 磁盘候选降级：须异步重放（未写任何输出）
  Degrade,
}

/// libs/server/Custom/CustomRespCommands.cs:TryCustomObjectCommand
///
/// 同步执行一条自定义对象命令（信封装载 + 四接口分派 + 回写）。
/// `key` 为用户键，`args` 为命令参数（不含 key）
pub(crate) fn try_custom_object_command<D: Device>(
  store: &BatchStoreSession<'_, D>,
  obj_cmd: &CustomObjectCommand,
  tag: u8,
  fns: &CustomObjectFns,
  key: &[u8],
  args: &[&[u8]],
  output: &mut Vec<u8>,
) -> CustomObjOutcome {
  match obj_cmd.command_type {
    CommandType::ReadModifyWrite => try_rmw(store, fns, tag, key, args, output),
    CommandType::Read => try_read(store, fns, tag, key, args, output),
  }
}

/// 同步回写三态（RMW 收尾共用）
enum SaveOutcome {
  Done,
  Degrade,
  Failed,
}

/// RMW 路径（C# RMW_ObjectStore 分流）
fn try_rmw<D: Device>(
  store: &BatchStoreSession<'_, D>,
  fns: &CustomObjectFns,
  tag: u8,
  key: &[u8],
  args: &[&[u8]],
  output: &mut Vec<u8>,
) -> CustomObjOutcome {
  let (mut payload, existed) = match obj_load_typed_sync(store, key, tag, output, |p| p.to_vec()) {
    ObjLoad::Degrade => return CustomObjOutcome::Degrade,
    ObjLoad::Error => return CustomObjOutcome::Done,
    // C# NeedInitialUpdate：建对象前校验（false = 错误帧已写，防空墓碑）
    ObjLoad::Missing => {
      if !(fns.need_initial_update)(args, output) {
        return CustomObjOutcome::Done;
      }
      // 新建对象零载荷初值（C# factory.Create 承接）
      (Vec::new(), false)
    }
    ObjLoad::Present(p) => (p, true),
  };

  if !(fns.updater)(&mut payload, args, output) {
    return CustomObjOutcome::Done;
  }

  // 空对象：命中键整键回收；缺失键不落库（防空墓碑，wedb 严格删空公理）
  let save = if (fns.is_empty)(&payload) {
    match existed {
      false => SaveOutcome::Done,
      true => match store.try_delete_sync(key) {
        Ok(Ok(_)) => SaveOutcome::Done,
        Ok(Err(_)) => SaveOutcome::Degrade,
        Err(_) => SaveOutcome::Failed,
      },
    }
  } else {
    // 非空写回走入账收口（ObjectStoreUpsert 全量条目，对标 C# WriteLogUpsert）
    match obj_save_notified(store, key, tag, &payload) {
      Ok(true) => SaveOutcome::Done,
      Ok(false) => SaveOutcome::Degrade,
      Err(_) => SaveOutcome::Failed,
    }
  };
  match save {
    SaveOutcome::Done => CustomObjOutcome::Done,
    SaveOutcome::Degrade => CustomObjOutcome::Degrade,
    SaveOutcome::Failed => {
      output.write_resp_error(RESP_ERR_GENERIC);
      CustomObjOutcome::Done
    }
  }
}

/// 只读路径（C# Read_ObjectStore 分流；NOTFOUND → functions.NotFound）
fn try_read<D: Device>(
  store: &BatchStoreSession<'_, D>,
  fns: &CustomObjectFns,
  tag: u8,
  key: &[u8],
  args: &[&[u8]],
  output: &mut Vec<u8>,
) -> CustomObjOutcome {
  match obj_load_typed_sync(store, key, tag, output, |p| p.to_vec()) {
    ObjLoad::Degrade => CustomObjOutcome::Degrade,
    ObjLoad::Error => CustomObjOutcome::Done,
    ObjLoad::Missing => {
      (fns.not_found)(args, output);
      CustomObjOutcome::Done
    }
    ObjLoad::Present(payload) => {
      let _ = (fns.reader)(&payload, args, output);
      CustomObjOutcome::Done
    }
  }
}
