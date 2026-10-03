//! 双键移动族慢臂（LMOVE / RPOPLPUSH / BLMOVE / BRPOPLPUSH：参数解包臂
//! [`lmove_cold`] + 移动核 [`move_core_cold`] + 源未取到阻塞闭环
//! [`wait_src_or_null`]；臂体与核逐行原样迁移）

use wcol::list::list_object::{ListObject, OperationDirection};
use wdev::Device;
use wresp::{command::RespCommand, ext::RespVecExt};

use super::{
  face::BlockWaitFace,
  shared::{
    BLMOVE_TIMEOUT_IDX, BRPOPLPUSH_TIMEOUT_IDX, err_async, face_with_timeout, peek_end, pop_end,
    push_end,
  },
};
use crate::{
  resp::objects::object_store_utils::{
    load_sealed_tri, obj_writeback_rechecked_async, rmw_window_pair_async,
  },
  session_parse_state_extensions::operation_direction_from_token as parse_direction,
  storage::session::storage_session::StorageSession,
};

/// LMOVE 双键移动族慢臂入口（原 list() 的 LMOVE/RPOPLPUSH/BLMOVE/BRPOPLPUSH
/// 臂整体迁移，臂体逐行原样；首键经 split_refs 同式取——refs.first 兜底
/// 空串，与入口 `let (key, _) = split_refs(refs)` 同值）
pub(super) async fn lmove_cold(
  storage: &StorageSession<'_, impl Device>,
  notify: &impl Fn(&[u8]),
  block: Option<&BlockWaitFace<'_>>,
  cmd: RespCommand,
  refs: &[&[u8]],
  resp_version: u8,
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  let key = refs.first().copied().unwrap_or(&[]);
  // LMOVE/RPOPLPUSH 直取；BLMOVE/BRPOPLPUSH 为阻塞族（源未取到时经
  // 等待面闭环，方向定式对位同步段）
  let (src_key, dst_key, src_dir, dst_dir) = match cmd {
    RespCommand::Rpoplpush | RespCommand::Brpoplpush => (
      key,
      refs.get(1).copied().unwrap_or(&[]),
      OperationDirection::Right,
      OperationDirection::Left,
    ),
    _ => {
      // 方向词元成对判定（任一元缺失/非 LEFT|RIGHT 同报，对位原两处同形分支）
      let (Some(src_dir), Some(dst_dir)) = (
        refs.get(2).copied().and_then(parse_direction),
        refs.get(3).copied().and_then(parse_direction),
      ) else {
        return err_async(output, ());
      };
      (key, refs.get(1).copied().unwrap_or(&[]), src_dir, dst_dir)
    }
  };
  // 阻塞族超时词元（快路径已校验同源；非阻塞族恒无等待面）
  let block = match cmd {
    RespCommand::Blmove => face_with_timeout(block, refs.get(BLMOVE_TIMEOUT_IDX).copied()),
    RespCommand::Brpoplpush => face_with_timeout(block, refs.get(BRPOPLPUSH_TIMEOUT_IDX).copied()),
    _ => None,
  };
  move_core_cold(
    resp_version,
    storage,
    notify,
    block,
    MoveColdArgs {
      src_key,
      dst_key,
      src_dir,
      dst_dir,
    },
    output,
  )
  .await
}

/// LMOVE 双键移动慢路径入参（收敛入参，消除 too-many-arguments）
struct MoveColdArgs<'a> {
  src_key: &'a [u8],
  dst_key: &'a [u8],
  src_dir: OperationDirection,
  dst_dir: OperationDirection,
}

/// 源未取到的阻塞闭环（C# ListBlockingMove 尾部 BlockingWait 后
/// !result.Found → WriteNull：出件由经纪 BLMOVE 臂完整执行弹+推，超时经
/// 应答单源回空值；经纪未注入域维持立即可取回 null）。入参收敛为
/// [`MoveColdArgs`]，两个源不可出件分支（缺失 / 空列表）同形复用
async fn wait_src_or_null(
  block: Option<(&BlockWaitFace<'_>, f64)>,
  resp_version: u8,
  args: &MoveColdArgs<'_>,
  output: &mut Vec<u8>,
) {
  match block {
    Some((face, timeout)) => {
      face
        .wait(
          RespCommand::Blmove,
          timeout,
          vec![args.src_key.to_vec()],
          vec![
            args.dst_key.to_vec(),
            vec![args.src_dir as u8],
            vec![args.dst_dir as u8],
          ],
          resp_version,
          output,
        )
        .await;
    }
    None => output.write_resp_null_ver(resp_version),
  }
}

/// LMOVE 双键移动慢路径对位（对标同步段 list_move_core；写成功唤醒阻塞
/// 观察者。`block` 为阻塞族等待面：BLMOVE/BRPOPLPUSH 源未取到时不立即
/// 回空值，登记观察者在执行域内联等待出件——C# ListBlockingMove
/// :373-376 无条件 BlockingWait 的键态解耦语义；`timeout` 仅阻塞族消费）
async fn move_core_cold(
  resp_version: u8,
  storage: &StorageSession<'_, impl Device>,
  notify: &impl Fn(&[u8]),
  block: Option<(&BlockWaitFace<'_>, f64)>,
  args: MoveColdArgs<'_>,
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  let MoveColdArgs {
    src_key,
    dst_key,
    src_dir,
    dst_dir,
  } = args;
  // 双键装载型写臂双保护·异步档：键组桶升序单机制双窗杜绝环等待与双序对撞
  //（同键/同桶折叠仅持一窗，票 zcode-r135c-lockorder 案一），跨 src/dst 装载、
  // 求值与双写回全程；窗预算耗尽按存储忙统一应答（fail-closed）
  let windows = rmw_window_pair_async(storage, src_key, dst_key)
    .await
    .map_err(|_| ())?;
  // 源未取到的阻塞闭环（C# ListBlockingMove 尾部 BlockingWait 后
  // !result.Found → WriteNull：出件由经纪 BLMOVE 臂完整执行弹+推，超时经
  // 应答单源回空值；经纪未注入域维持立即可取回 null）。等待臂须先释放
  // 双窗：阻塞等待期间唤醒方（LPUSH 等写命令）须能取得窗写入源键，持窗
  // 等待即自我锁死至超时（装载快照已定，窗的保护使命随之结束）
  let (mut src, src_window) =
    match load_sealed_tri::<ListObject, _>(storage, src_key, output).await? {
      // WRONGTYPE：错误帧已由装载底层写出，终止整条命令
      None => return Ok(()),
      // C# src 缺失 → null element（与同步 list_move_core 的 Missing 分支同形；
      // 阻塞族转等待闭环）
      Some(None) => {
        drop(windows);
        wait_src_or_null(block, resp_version, &args, output).await;
        return Ok(());
      }
      Some(Some(loaded)) => loaded,
    };
  if src.list.is_empty() {
    // C# src 缺失/空 → OK + null element（会话版本分派，RESP3 为 `_\r\n`；
    // 阻塞族转等待闭环，同上先释放双窗）
    drop(windows);
    wait_src_or_null(block, resp_version, &args, output).await;
    return Ok(());
  }
  let _keep = windows;

  let same_key = src_key == dst_key;
  if same_key && (src_dir == dst_dir || src.list.len() == 1) {
    // C# 同键同向 或 单元素：窥视元素返回，严禁先 pop 再 push（防 TTL 丢失）
    match peek_end(&src.list, src_dir) {
      Some(item) => output.write_resp_bulk_string(item),
      None => output.write_resp_null_ver(resp_version),
    }
    return Ok(());
  }

  // 异键移动：先预检目标键类型（对标 C# GET(destinationKey) WRONGTYPE 拦截）
  let mut dst_loaded = ListObject::new();
  let mut dst_window = None;
  // dst 装载态快照（新建写回复验要求域仍缺席，票 load-type-rmw-window）
  let mut dst_existed = false;
  if !same_key {
    match load_sealed_tri(storage, dst_key, output).await? {
      None => return Ok(()),
      Some(None) => {}
      // 封窗守卫存活至 dst 写回收尾（函数尾），杜绝窗内并发写被顶替
      Some(Some((o, w))) => {
        dst_loaded = o;
        dst_window = w;
        dst_existed = true;
      }
    }
  }

  let Some(element) = pop_end(&mut src.list, src_dir) else {
    output.write_resp_null_ver(resp_version);
    return Ok(());
  };

  if same_key {
    push_end(&mut src.list, dst_dir, element);
    obj_writeback_rechecked_async(storage, src_key, &src, src_window.is_some(), true).await?;
    notify(src_key);
    if let Some(elem) = peek_end(&src.list, dst_dir) {
      output.write_resp_bulk_string(elem);
    }
    return Ok(());
  }

  src.update_size(&element, false);
  dst_loaded.update_size(&element, true);
  push_end(&mut dst_loaded.list, dst_dir, element);
  // 写回序先目标后源（与同步段 list_move_core 及 set_commands/slow.rs 同款补偿
  // 序，对齐 set_move 成文纪律）：目标写回失败（迁移窗忙错/升阶未成超页
  // fail-closed/存储 IO）时源零变异零写入，错误帧后数据无损，重试自完整初态
  // 收敛；目标成功而源失败时列表 push 非幂等，残留为「元素双份」可重试收敛
  // （集合侧 insert 幂等自收敛，差异注见同步段）
  obj_writeback_rechecked_async(
    storage,
    dst_key,
    &dst_loaded,
    dst_window.is_some(),
    dst_existed,
  )
  .await?;
  // dst 提交即唤醒，src 写回失败不回撤事件（对齐快臂 list_move_core dst save
  // Ok(true) 贴发位与 C# ListOps.cs:299 提交后唤醒之本仓投影：本仓两笔 save
  // 独立，唤醒随 dst 自身提交；notify 严禁漂回下方 src save `?` 之后——src
  // fail-closed 错误帧会把已持久 dst 元素落成「已提交漏发」永悬态，
  // timeout=0 观察者悬至该键下一写事件（票 zcode-r145c-lblpop2 案一）。
  // 观察者即时取走该元素落 §100 在册「元素双份」可重试容忍形，快臂现形
  // 即此口径运行，零新增险面
  notify(dst_key);
  // 源空 → 整键回收（C# EXPIRE TimeSpan.Zero）
  obj_writeback_rechecked_async(storage, src_key, &src, src_window.is_some(), true).await?;
  if let Some(elem) = peek_end(&dst_loaded.list, dst_dir) {
    output.write_resp_bulk_string(elem);
  }
  Ok(())
}
