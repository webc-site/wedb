//! 多键弹出族慢臂（LMPOP / BLMPOP / BLPOP / BRPOP：逐键立即可取弹出与
//! 阻塞闭环；[`pop_first_cold`] 为四命令冷臂共用骨架，臂体逐行原样迁移）

use wcol::list::list_object::{ListObject, OperationDirection};
use wdev::Device;
use wresp::{command::RespCommand, ext::RespVecExt};

use super::{
  super::parse_lmpop_args,
  face::BlockWaitFace,
  shared::{face_with_timeout, pop_end, write_key_item_frame, write_key_items_frame},
};
use crate::{
  resp::objects::object_store_utils::{load_sealed_tri, obj_writeback_rechecked_async},
  storage::session::storage_session::StorageSession,
};

/// LMPOP / BLMPOP 慢臂（原 list() 的 Lmpop|Blmpop 臂整体迁移，臂体逐行
/// 原样）
pub(super) async fn mpop_cold(
  storage: &StorageSession<'_, impl Device>,
  block: Option<&BlockWaitFace<'_>>,
  cmd: RespCommand,
  refs: &[&[u8]],
  resp_version: u8,
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  // 参数推导单源（快慢共用，失败帧已写出；BLMPOP 的 timeout 词元由
  // 快路径先行校验，冷键降级时不再复检）
  let Some((keys, pop_direction, pop_count)) =
    parse_lmpop_args(refs, cmd == RespCommand::Blmpop, output)
  else {
    return Ok(());
  };
  let handled = pop_first_cold(
    storage,
    keys,
    pop_direction,
    pop_count as usize,
    output,
    write_key_items_frame,
  )
  .await?;
  if !handled {
    // LMPOP NOTFOUND → WriteNullArray（非阻塞语义恒立即回）；BLMPOP
    // 未取到 → 经等待面闭环（cmd_args = [popDir, popCount]，与快路径
    // park 同编码；C# ListBlockingPopMultiple 尾部 BlockingWait 同位），
    // 出件/超时经应答单源出帧；经纪未注入域回空值
    if cmd == RespCommand::Lmpop {
      output.write_resp_null_array_ver(resp_version);
    } else if let Some((face, timeout)) = face_with_timeout(block, refs.first().copied()) {
      face
        .wait(
          cmd,
          timeout,
          keys.iter().map(|k| k.to_vec()).collect(),
          vec![vec![pop_direction as u8], pop_count.to_le_bytes().to_vec()],
          resp_version,
          output,
        )
        .await;
    } else {
      output.write_resp_null_ver(resp_version);
    }
  }
  Ok(())
}

/// BLPOP / BRPOP 慢臂（原 list() 的 Blpop|Brpop 臂整体迁移，臂体逐行原样）
pub(super) async fn bpop_cold(
  storage: &StorageSession<'_, impl Device>,
  block: Option<&BlockWaitFace<'_>>,
  cmd: RespCommand,
  refs: &[&[u8]],
  resp_version: u8,
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  // 阻塞族冷臂：逐键立即可取（弹取出件复用 [`pop_first_cold`] 骨架，count
  // 恒 1、回复 [key, item]；对位同步段 blocking.rs list_blocking_pop 的三态
  // 匹配，C# ListBlockingPop 遇 IsTypeMismatch 写错误行即终止整条命令，后续
  // 键不被触碰）；全部键缺失或空列表即经等待面闭环（C# ListBlockingPop :284
  // 无条件 BlockingWait 的键态解耦语义），出件/超时经应答单源出帧
  let dir = if cmd == RespCommand::Blpop {
    OperationDirection::Left
  } else {
    OperationDirection::Right
  };
  let keys = &refs[..refs.len().saturating_sub(1)];
  if pop_first_cold(storage, keys, dir, 1, output, write_key_item_frame).await? {
    return Ok(());
  }
  // 未取到：经纪注入域等待闭环（timeout 词元快路径已校验）；未注入
  // 域回 C# !result.Found 同款空数组（会话版本分派，RESP3 为 `_\r\n`）
  if let Some((face, timeout)) = face_with_timeout(block, refs.last().copied()) {
    face
      .wait(
        cmd,
        timeout,
        keys.iter().map(|k| k.to_vec()).collect(),
        Vec::new(),
        resp_version,
        output,
      )
      .await;
  } else {
    output.write_resp_null_array_ver(resp_version);
  }
  Ok(())
}

/// 逐键弹出第一个非空列表的慢路径骨架（LMPOP/BLMPOP 与 BLPOP/BRPOP 冷臂
/// 共用，对标同步段 pop_first_nonempty 的 Some(true)/None 分派；出帧形态由
/// `frame` 决定，C# ListPopMultiple / ListBlockingPop 尾部应答差异）
///
/// WRONGTYPE 判定序单源：装载底层已写错误行即返回终结，严禁续扫后续键
/// （C# 遇 IsTypeMismatch 终止整条命令）；MISSING / 空列表续扫且不写回。
///
/// 返回 `true` = 已生成终结性应答（WRONGTYPE 错误行写出，或命中并出帧），
/// 调用方不得追加任何空值应答；`false` = 全部键缺失或空列表，由调用方补写
/// null 形态应答
async fn pop_first_cold(
  storage: &StorageSession<'_, impl Device>,
  keys: &[&[u8]],
  pop_direction: OperationDirection,
  pop_count: usize,
  output: &mut Vec<u8>,
  frame: impl Fn(&[u8], &[Vec<u8>], &mut Vec<u8>),
) -> Result<bool, ()> {
  for key in keys {
    // 装载型取件臂双保护·异步档：逐键装载前取 rmw 窗跨弹出与写回
    let _window = storage.batch.rmw_window(key).await.map_err(|_| ())?;
    let (mut obj, swap_in_window) =
      match load_sealed_tri::<ListObject, _>(storage, key, output).await? {
        // WRONGTYPE：错误帧已写出，返回 true 告知调用方应答已终结
        None => return Ok(true),
        // MISSING：键不存在，继续扫描下一键
        Some(None) => continue,
        Some(Some(loaded)) => loaded,
      };
    let mut popped = Vec::with_capacity(pop_count.min(obj.list.len()));
    while popped.len() < pop_count {
      let Some(item) = pop_end(&mut obj.list, pop_direction) else {
        break;
      };
      obj.update_size(&item, false);
      popped.push(item);
    }
    // 空列表（无可弹元素）→ 续扫，不写回
    if popped.is_empty() {
      continue;
    }
    obj_writeback_rechecked_async(storage, key, &obj, swap_in_window.is_some(), true).await?;
    frame(key, &popped, output);
    return Ok(true);
  }
  Ok(false)
}
