use std::sync::Arc;

use wbase::num::strict_i32;
use wbftree::ScanReturnField;
use wcol::{SET_MEMBER_DUMMY_VALUE, set::set_object::SetOperation};
use wdev::Device;
use wkv::BatchStoreSession;
use wresp::{cmd_strings as cs, ext::RespVecExt};
use wval::GarnetObjectType;

use super::common::{
  OutFace, RespHead, SCAN_FROM_HEAD, TieredCollectionArgs, TieredCtx, emit_tiered_mirror,
  finish_tiered_arm, save_tiered_meta, scan_count, stream_scan_face, tiered_guard, tiered_precheck,
  tiered_write_mirror, tree_put_batch, tree_put_rejected,
};

/// 集合族写面判定（一处定义）：SADD 写臂取独占写锁；SREM / SPOP 与 SRANDMEMBER、
/// 纯读 / 穿透臂一律共享读锁（删除重族无树内臂，见本模块头注「树内零墓碑」）
pub(crate) fn set_needs_write(op: SetOperation) -> bool {
  matches!(op, SetOperation::Sadd)
}

/// 执行分层态集合命令（WATCH 栅栏由 [`finish_tiered_arm`] 统一收尾）
pub(crate) async fn exec_tiered_set<D: Device>(
  session: &BatchStoreSession<'_, D>,
  key: &[u8],
  ctx: &mut TieredCtx<'_>,
  call: TieredCollectionArgs<'_, SetOperation>,
  output: &mut Vec<u8>,
) -> Result<bool, ()> {
  let handled = tiered_set_arm(session, key, ctx, call, output).await;
  finish_tiered_arm(session, key, ctx, handled)
}

/// 分层态集合命令树内主体（读写臂分派，见 [`exec_tiered_set`]）
async fn tiered_set_arm<D: Device>(
  session: &BatchStoreSession<'_, D>,
  key: &[u8],
  ctx: &mut TieredCtx<'_>,
  call: TieredCollectionArgs<'_, SetOperation>,
  output: &mut Vec<u8>,
) -> Result<bool, ()> {
  let TieredCollectionArgs {
    op,
    args12: _,
    args,
    resp_protocol_version,
  } = call;
  // SetOperation 无 arg1/arg2 压缩字消费臂（信封通道同形），镜像载荷恒 (0, 0)
  let mirror = tiered_write_mirror(GarnetObjectType::Set, op, (0, 0), args, set_needs_write);
  // 本命令负载起点（臂尾镜像入账失败撤帧用，与 zset 臂 frame_base 同形）
  let frame_base = output.len();
  // 写臂独占互斥（锁内刷新元记录），读臂共享锁（判定一处定义）
  let Some(tree_guard) = tiered_guard(session, key, ctx, set_needs_write(op)).await? else {
    return Ok(false);
  };
  let tree = tree_guard.tree();

  let result = 'arm: {
    match op {
      SetOperation::Sadd => {
        // 预校验先于任何写入（RI 批量口径，Set 成员恒以 dummy 值裸编码落树，
        // 任一成员越契约即整体失败、零树内副作用）
        for &member in args {
          if !tiered_precheck(ctx, member, SET_MEMBER_DUMMY_VALUE.len(), None, output) {
            break 'arm Ok(true);
          }
        }
        // 批量折叠：dummy 值整批经排序批量 upsert 内核一次下刷（栈上排序集中
        // 命中叶页），返回值即真实新增数——同批重复成员去重只计一次，与逐条
        // contains_key 前探的「插成功才计数」判据等价（C#
        // SetObjectImpl.SetAdd 纯字典写计数的分层等价判据）
        let entries: Vec<(&[u8], &[u8])> = args
          .iter()
          .map(|&member| (member, SET_MEMBER_DUMMY_VALUE))
          .collect();
        let added = match tree_put_batch(tree, &entries) {
          Ok(n) => n,
          Err(_) => {
            tree_put_rejected(output);
            break 'arm Ok(true);
          }
        };
        // 置脏判据：仅真实新增才变更树内容——重复成员落树记录逐位相同（dummy
        // 值定长编码），C# SetObjectImpl.SetAdd 对已存成员零字典写、零变更
        ctx.dirty |= added > 0;
        if added > 0 {
          ctx.meta.size += added;
          // 落盘失败：树内写已生效，break 落臂尾补镜像再上抛（不变式见 save_tiered_meta）
          if save_tiered_meta(session, key, ctx).await.is_err() {
            break 'arm Err(());
          }
        }
        output.write_resp_int(added as i64);
        Ok(true)
      }

      SetOperation::Sismember => {
        if args.is_empty() {
          return Err(());
        }
        output.write_resp_int(i64::from(tree.contains_key(args[0])));
        Ok(true)
      }

      SetOperation::Smismember => {
        output.write_resp_array_len(args.len());
        for &member in args {
          output.write_resp_int(i64::from(tree.contains_key(member)));
        }
        Ok(true)
      }

      SetOperation::Scard => {
        output.write_resp_int(ctx.meta.size as i64);
        Ok(true)
      }

      SetOperation::Smembers => {
        // 协议感知 set 头（RESP3 `~<n>`、RESP2 `*<n>`，对标 C# SetObjectImpl
        // .SetMembers WriteSetLength(Set.Count)）：成员逐条直写最终 output，帧头
        // 按存活上界预留、以实际出帧计数回填（骨架见 [`stream_scan_face`]，
        // 预留位宽只是提示、Err 支撤帧，见其文注两条不变量）
        stream_scan_face(
          tree,
          output,
          OutFace::from_head(
            ScanReturnField::Key,
            ctx.meta.size as usize,
            RespHead::set(resp_protocol_version),
          ),
          |out, member, _| out.write_resp_bulk_string(member),
        )?;
        Ok(true)
      }

      SetOperation::Srandmember => {
        // count 解析（libs/server/Resp/Objects/SetCommands.cs:SetRandomMember：
        // 非整数 → RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER；负数合法取 |count| 可重复）
        let count_arg = args.first().map(|raw| (raw, strict_i32(raw)));
        if let Some((_, parsed)) = &count_arg
          && parsed.is_none()
        {
          cs::write_error_raw(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
          break 'arm Ok(true);
        }
        let count = count_arg.as_ref().and_then(|(_, v)| *v);

        // 随机起点：fastrand 随机起始键定位树内扫描位（对象层为随机索引抽取；
        // 树内顺序流式近似，返回集合无序契约下元素序与抽样域语义等价，长度契约
        // 另由下方回绕补扫收敛循环补足，|count| 恒满）；扫描 Err 经
        // [`scan_count`] 上抛（应答尚未落帧）
        let start_key = fastrand::u64(..).to_be_bytes();
        let scan = |tree: &Arc<wbftree::BfTreeService>,
                    start: &[u8],
                    n: usize,
                    out: &mut Vec<(Vec<u8>, Vec<u8>)>|
         -> Result<(), ()> {
          scan_count(tree.scan_with_count_callback(
            start,
            usize::MAX,
            ScanReturnField::Key,
            |k, _| {
              if out.len() < n {
                out.push((k.to_vec(), Vec::new()));
                true
              } else {
                false
              }
            },
          ))?;
          Ok(())
        };

        // 请求/可返条目基数（对标 C# SetObjectImpl.SetRandomMember 各分支）
        let n = match count {
          None => 1,
          Some(c) if c > 0 => (c as u64).min(ctx.meta.size) as usize,
          Some(0) => 0,
          Some(c) => c.unsigned_abs() as usize,
        };

        // 空集出口（结构性防御分支：MetaValue::is_live 门令 size == 0 的
        // 集合键判死，稳态不可达；语义仍严格对标 C# SetObjectImpl.cs:
        // SetRandomMember 空集三分支——count>0 → WriteSetLength(0)，无
        // count → WriteNull，负 count → WriteNull。count==0 在 C# 由
        // RESP 层键态无关拦截为空数组，分层臂先于慢路径 count==0 拦截点
        // 求值，是该形态的先见位点，同步 *0）
        if ctx.meta.size == 0 {
          match count {
            Some(c) if c > 0 => cs::write_set_len(output, 0, resp_protocol_version),
            Some(0) => output.extend_from_slice(cs::RESP_EMPTYLIST),
            _ => output.write_resp_null_ver(resp_protocol_version),
          }
          break 'arm Ok(true);
        }

        let mut members: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(n);
        scan(tree, &start_key, n, &mut members)?;
        // 回绕补扫收敛循环：首轮自随机起点只拿到树尾后缀 a 键，|count| > a 时
        // 单轮回绕（自树头扫满 n 预算）上限 a+d 仍可短供——长度是负 count 臂的
        // 唯一硬契约（内存态 set_object_impl 放回臂恒满 |count|，双态长度分叉违
        // collection.md §5/§8 与 review.md §5.1），故未达 n 即自重入树头续扫。
        // 负 count 可重复语义容许后缀段同键多轮重复入列，无需去重、不改诚实头形。
        // 收敛性：is_live 门已把 size==0 的键挡在空集出口外，读锁内树内容稳定，
        // 每轮至少新增一键 ⇒ 必有进展；本轮零新增（树被并发掏空的防御出口）
        // 即诚实短帧早退，无零进展死循环。正 count 臂 n ≤ 基数，首两扫已闭包，
        // 循环不感应。
        while members.len() < n {
          let before = members.len();
          scan(tree, SCAN_FROM_HEAD, n, &mut members)?;
          if members.len() == before {
            break;
          }
        }
        match count {
          None => {
            if let Some((m, _)) = members.first() {
              output.write_resp_bulk_string(m);
            } else {
              output.write_resp_null_ver(resp_protocol_version);
            }
          }
          // count>0 互异：set 头（C# WriteSetLength）
          Some(c) if c > 0 => {
            cs::write_set_len(output, members.len(), resp_protocol_version);
            for (m, _) in &members {
              output.write_resp_bulk_string(m);
            }
          }
          // count<=0（含 0 与负 count 可重复）：数组头（C# WriteArrayLength）
          Some(_) => {
            output.write_resp_array_len(members.len());
            for (m, _) in &members {
              output.write_resp_bulk_string(m);
            }
          }
        }
        Ok(true)
      }

      // 未支持操作一律穿透（Ok(false)）：由 run_async_rmw 物化降级通道接手，
      // 杜绝静默兜底输出与命令语义无关的应答。删除重的 SREM / SPOP 同在此穿透
      // （无树内逐成员删除臂），见本模块头注「树内零墓碑」
      SetOperation::Srem
      | SetOperation::Spop
      | SetOperation::Sscan
      | SetOperation::Smove
      | SetOperation::Sunion
      | SetOperation::Sunionstore
      | SetOperation::Sdiff
      | SetOperation::Sdiffstore
      | SetOperation::Sinter
      | SetOperation::Sinterstore => Ok(false),
    }
  };
  // 稳态写命令镜像收尾：树守卫存续窗口内入账，早退臂未置脏自然跳过，落盘失败臂
  // 经 save_tiered_meta 以 break 'arm 落本收尾（判据与不变式见 emit_tiered_mirror）；
  // 入队失败撤本命令已落成功帧后按 AofEnqueue 契约上抛拒绝（禁假成功冒答）
  if emit_tiered_mirror(session, key, ctx, mirror).is_err() {
    output.truncate(frame_base);
    return Err(());
  }
  result
}
