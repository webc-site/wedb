//! 单键装载求值族慢臂（LTRIM / LRANGE / LINDEX / LPOS / LINSERT / LREM /
//! LSET：单键「装载 → 求值（→ 写回）」形，读臂走 slow_load_eval 非封窗
//! 通道，写臂走 load_eval_write 封窗通道；臂体逐行原样迁移）

use wbase::num::strict_i32;
use wcol::list::list_object::{ListObject, ListOperation};
use wdev::Device;
use wresp::{cmd_strings as cs, ext::RespVecExt};
use wval::GarnetObjectType;

use super::{
  super::{parse_i32_pair_args, run_operate},
  shared::{err_async, load_eval_write},
};
use crate::{
  resp::objects::object_store_utils::{
    GarnetObjectPayload, obj_writeback_rechecked_async, slow_load_eval,
  },
  storage::session::storage_session::StorageSession,
};

/// LTRIM 慢臂（libs/server/Resp/Objects/ListCommands.cs:ListTrim）
pub(super) async fn ltrim_cold(
  storage: &StorageSession<'_, impl Device>,
  key: &[u8],
  refs: &[&[u8]],
  resp_version: u8,
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  // start/stop 参数推导单源（快慢共用，失败帧已写出）
  let Some((start, stop)) = parse_i32_pair_args("LTRIM", refs, output) else {
    return Ok(());
  };
  load_eval_write(
    storage,
    key,
    output,
    // C# NOTFOUND → OK（无对象可裁剪，仍回 OK）
    |output| output.extend_from_slice(cs::RESP_OK),
    async move |mut obj: ListObject, sealed: bool, output: &mut Vec<u8>| {
      run_operate(
        &mut obj,
        ListOperation::Ltrim,
        &[],
        start,
        stop,
        resp_version,
        output,
      );
      obj_writeback_rechecked_async(storage, key, &obj, sealed, true).await?;
      output.extend_from_slice(cs::RESP_OK);
      Ok(())
    },
  )
  .await
}

/// LRANGE 慢臂（libs/server/Resp/Objects/ListCommands.cs:ListRange）
pub(super) async fn lrange_cold(
  storage: &StorageSession<'_, impl Device>,
  key: &[u8],
  refs: &[&[u8]],
  resp_version: u8,
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  // start/stop 参数推导单源（快慢共用，失败帧已写出）
  let Some((start, stop)) = parse_i32_pair_args("LRANGE", refs, output) else {
    return Ok(());
  };
  // 只读臂走非封窗纯物化通道（与 hash/set/zset 三族读臂同机制，
  // eval 零写回不封窗，读面忙拒面不扩大）
  slow_load_eval(
    storage,
    key,
    GarnetObjectType::List,
    output,
    ListObject::from_blob,
    |output| output.extend_from_slice(cs::RESP_EMPTYLIST),
    async move |obj: &mut ListObject, output: &mut Vec<u8>| {
      run_operate(
        obj,
        ListOperation::Lrange,
        &[],
        start,
        stop,
        resp_version,
        output,
      );
    },
  )
  .await
}

/// LINDEX 慢臂（libs/server/Resp/Objects/ListCommands.cs:ListIndex）
pub(super) async fn lindex_cold(
  storage: &StorageSession<'_, impl Device>,
  key: &[u8],
  refs: &[&[u8]],
  resp_version: u8,
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  let Some(index) = refs.get(1).and_then(|v| strict_i32(v)) else {
    return err_async(output, ());
  };
  slow_load_eval(
    storage,
    key,
    GarnetObjectType::List,
    output,
    ListObject::from_blob,
    |output| output.write_resp_null_ver(resp_version),
    async move |obj: &mut ListObject, output: &mut Vec<u8>| {
      // result1 == -1 时对象层未写负载（C# ProcessOutput + WriteNull）
      let result1 = run_operate(
        obj,
        ListOperation::Lindex,
        &[],
        index,
        0,
        resp_version,
        output,
      )
      .result1;
      if result1 == -1 {
        output.write_resp_null_ver(resp_version);
      }
    },
  )
  .await
}

/// LPOS 慢臂（libs/server/Resp/Objects/ListCommands.cs:ListPosition）
pub(super) async fn lpos_cold(
  storage: &StorageSession<'_, impl Device>,
  key: &[u8],
  args: &[&[u8]],
  refs: &[&[u8]],
  resp_version: u8,
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  // 只读臂走非封窗纯物化通道（与三族读臂同机制）：分层键已注册树内
  // 流式臂（tiered_list_arm Lpos），本臂仅承接信封态与装载竞态降级形；
  // 对象层 Lpos result1 = 命中数恒 ≥0，无 LINDEX 的 -1 未写负载形
  slow_load_eval(
    storage,
    key,
    GarnetObjectType::List,
    output,
    ListObject::from_blob,
    // C# NOTFOUND：参数含 COUNT → 空数组，否则 null
    |output| {
      if refs[2..].iter().any(|t| t.eq_ignore_ascii_case(cs::COUNT)) {
        output.extend_from_slice(cs::RESP_EMPTYLIST);
      } else {
        output.write_resp_null_ver(resp_version);
      }
    },
    async move |obj: &mut ListObject, output: &mut Vec<u8>| {
      run_operate(obj, ListOperation::Lpos, args, 0, 0, resp_version, output);
    },
  )
  .await
}

/// LINSERT 慢臂（libs/server/Resp/Objects/ListCommands.cs:ListInsert）
pub(super) async fn linsert_cold(
  storage: &StorageSession<'_, impl Device>,
  notify: &impl Fn(&[u8]),
  key: &[u8],
  args: &[&[u8]],
  resp_version: u8,
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  load_eval_write(
    storage,
    key,
    output,
    |output| output.extend_from_slice(cs::RESP_RETURN_VAL_0),
    async move |mut obj: ListObject, sealed: bool, output: &mut Vec<u8>| {
      let result1 = run_operate(
        &mut obj,
        ListOperation::Linsert,
        args,
        0,
        0,
        resp_version,
        output,
      )
      .result1;
      if result1 > 0 {
        obj_writeback_rechecked_async(storage, key, &obj, sealed, true).await?;
        notify(key);
      }
      output.write_resp_int(result1);
      Ok(())
    },
  )
  .await
}

/// LREM 慢臂（libs/server/Resp/Objects/ListCommands.cs:ListRemove）
pub(super) async fn lrem_cold(
  storage: &StorageSession<'_, impl Device>,
  key: &[u8],
  refs: &[&[u8]],
  resp_version: u8,
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  let Some(count) = refs.get(1).and_then(|v| strict_i32(v)) else {
    return err_async(output, ());
  };
  let element = refs.get(2).copied().unwrap_or(&[]);
  load_eval_write(
    storage,
    key,
    output,
    |output| output.extend_from_slice(cs::RESP_RETURN_VAL_0),
    async move |mut obj: ListObject, sealed: bool, output: &mut Vec<u8>| {
      let result1 = run_operate(
        &mut obj,
        ListOperation::Lrem,
        &[element],
        count,
        0,
        resp_version,
        output,
      )
      .result1;
      if result1 > 0 {
        obj_writeback_rechecked_async(storage, key, &obj, sealed, true).await?;
      }
      output.write_resp_int(result1);
      Ok(())
    },
  )
  .await
}

/// LSET 慢臂（libs/server/Resp/Objects/ListCommands.cs:ListSet）
pub(super) async fn lset_cold(
  storage: &StorageSession<'_, impl Device>,
  key: &[u8],
  refs: &[&[u8]],
  resp_version: u8,
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  let (Some(idx_val), Some(element)) = (refs.get(1).copied(), refs.get(2).copied()) else {
    return err_async(output, ());
  };
  load_eval_write(
    storage,
    key,
    output,
    // C# NOTFOUND → ERR no such key
    |output| cs::write_error_raw(output, cs::RESP_ERR_GENERIC_NOSUCHKEY),
    async move |mut obj: ListObject, sealed: bool, output: &mut Vec<u8>| {
      let mut obj_out = run_operate(
        &mut obj,
        ListOperation::Lset,
        &[idx_val, element],
        0,
        0,
        resp_version,
        output,
      );
      if obj_out.payload_view().first() == Some(&b'+')
        && let Err(e) = obj_writeback_rechecked_async(storage, key, &obj, sealed, true).await
      {
        // 回退挂载点清场后上抛（+OK 负载不得与忙帧拼帧，统一应答兜底）
        obj_out.reset();
        return Err(e);
      }
      // +OK / 对象层错误负载均已直写会话输出尾段
      Ok(())
    },
  )
  .await
}
