//! 推入/弹出族慢臂（LPUSH / RPUSH / LPUSHX / RPUSHX / LPOP / RPOP；入口
//! 分派门按 op_of 六臂联合筛定后进入，三段序与臂体原样保持）

use wbase::num::strict_i32;
use wcol::list::list_object::{ListObject, ListOperation};
use wdev::Device;
use wresp::{cmd_strings as cs, command::RespCommand, ext::RespVecExt};

use super::{
  super::{Rmw, run_operate},
  shared::{err_async, list_rmw_cold, load_eval_write, op_of},
};
use crate::{
  resp::objects::object_store_utils::{obj_writeback_rechecked_async, write_rmw_reply},
  storage::session::storage_session::StorageSession,
};

/// 推入/弹出族慢臂公共体（原 list() 前置三段 if 链整体迁移，段序/臂体
/// 逐行原样；LPOP/RPOP 的 count 词元经 args 首参取——args = refs[1..]，
/// 与原 refs.get(1) 同位）
pub(super) async fn push_pop_cold(
  storage: &StorageSession<'_, impl Device>,
  notify: &impl Fn(&[u8]),
  cmd: RespCommand,
  key: &[u8],
  args: &[&[u8]],
  resp_version: u8,
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  // LPUSH/RPUSH（RMW；写成功唤醒阻塞观察者，对标同步段 notify 点）
  if let Some(op @ (ListOperation::Lpush | ListOperation::Rpush)) = op_of(cmd) {
    let done = list_rmw_cold(storage, key, op, args, (0, 0), resp_version, output).await?;
    if let Rmw::Present(done) = done {
      write_rmw_reply(done, output);
      notify(key);
    }
    return Ok(());
  }
  // LPUSHX/RPUSHX（缺失不物化回 :0；写成功唤醒阻塞观察者；装载前取 rmw 窗
  // 跨全程由 load_eval_write 单源承担，对位同步臂同款双保护·异步档）
  if let Some(op @ (ListOperation::Lpushx | ListOperation::Rpushx)) = op_of(cmd) {
    return load_eval_write(
      storage,
      key,
      output,
      // MISSING：C# 键缺失不创建，回复 :0
      |output| output.extend_from_slice(cs::RESP_RETURN_VAL_0),
      async move |mut obj: ListObject, sealed: bool, output: &mut Vec<u8>| {
        // LPUSH 族仅回填 result1（无负载段），写回失败由外层统一落错
        let result1 = run_operate(&mut obj, op, args, 0, 0, resp_version, output).result1;
        obj_writeback_rechecked_async(storage, key, &obj, sealed, true).await?;
        output.write_resp_int(result1);
        notify(key);
        Ok(())
      },
    )
    .await;
  }
  // LPOP/RPOP（RMW，arg1 = pop_count）
  if let Some(op @ (ListOperation::Lpop | ListOperation::Rpop)) = op_of(cmd) {
    let pop_count = match args.first() {
      // 缺省 count = 1（无外层数组形态）
      None => 1,
      Some(c) => match strict_i32(c) {
        Some(v) if v >= 0 => v,
        _ => return err_async(output, ()),
      },
    };
    list_rmw_cold(storage, key, op, &[], (pop_count, 0), resp_version, output).await?;
    return Ok(());
  }
  // 入口分派门已按 op_of 六臂联合筛定后进入，三段必中其一，此行仅穷尽收尾
  Ok(())
}
