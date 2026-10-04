use wbase::num::strict_i32;
use wbftree::ScanReturnField;
use wcol::{SET_MEMBER_DUMMY_VALUE, set::set_object::SetOperation};
use wdev::Device;
use wkv::BatchStoreSession;
use wresp::{
  cmd_strings as cs,
  ext::{RespVecExt, backfill_resp_frame_head, reserve_resp_frame_head, resp_frame_head_len},
};
use wval::GarnetObjectType;

use super::common::{
  OutFace, RandomScanEmit, RespHead, SCAN_FROM_HEAD, TieredCollectionArgs, TieredCtx,
  TieredOpError, emit_tiered_mirror_finish, finish_tiered_arm, random_scan_round,
  random_stream_wraparound, save_tiered_meta, scan_count, stream_scan_face, tiered_guard,
  tiered_precheck, tiered_write_mirror, tree_put_batch, tree_put_rejected,
};
use crate::resp::objects::object_store_utils::MAX_RANDOM_MEMBER_COUNT;

/// 集合族写面判定（一处定义）：SADD 写臂取独占写锁；SREM / SPOP 与 SRANDMEMBER、
/// 纯读 / 穿透臂一律共享读锁（删除重族无树内臂，见本模块头注「树内零墓碑」）
pub(crate) fn set_needs_write(op: SetOperation) -> bool {
  matches!(op, SetOperation::Sadd)
}

/// 执行分层态集合命令（WATCH 栅栏由 [`finish_tiered_arm`] 统一收尾）
pub async fn exec_tiered_set<D: Device>(
  session: &BatchStoreSession<'_, D>,
  key: &[u8],
  ctx: &mut TieredCtx<'_>,
  call: TieredCollectionArgs<'_, SetOperation>,
  output: &mut Vec<u8>,
) -> Result<bool, TieredOpError> {
  let handled = tiered_set_arm(session, key, ctx, call, output).await;
  finish_tiered_arm(session, key, ctx, handled).map_err(|_| TieredOpError)
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
        if ctx.meta.size == 0 {
          cs::write_set_len(output, 0, resp_protocol_version);
          break 'arm Ok(true);
        }
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
        let count = match &count_arg {
          Some((_, Some(v))) if v.unsigned_abs() <= MAX_RANDOM_MEMBER_COUNT as u32 => Some(*v),
          Some(_) => {
            cs::write_error_raw(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
            break 'arm Ok(true);
          }
          None => None,
        };

        let start_key = fastrand::u64(..).to_be_bytes();

        let count_val = match count {
          None => {
            if ctx.meta.size == 0 {
              output.write_resp_null_ver(resp_protocol_version);
              break 'arm Ok(true);
            }
            let mut chosen: Option<Vec<u8>> = None;
            let mut live_seen = 0usize;
            scan_count(tree.scan_with_count_callback(
              SCAN_FROM_HEAD,
              usize::MAX,
              ScanReturnField::Key,
              |k, _| {
                live_seen += 1;
                if fastrand::usize(..live_seen) == 0 {
                  chosen = Some(k.to_vec());
                }
                true
              },
            ))?;
            match chosen {
              Some(field) => output.write_resp_bulk_string(&field),
              None => output.write_resp_null_ver(resp_protocol_version),
            }
            break 'arm Ok(true);
          }
          Some(c) => c,
        };

        let n = if count_val > 0 {
          (count_val as u64).min(ctx.meta.size) as usize
        } else {
          count_val.unsigned_abs() as usize
        };

        if ctx.meta.size == 0 {
          if count_val > 0 {
            cs::write_set_len(output, 0, resp_protocol_version);
          } else {
            output.extend_from_slice(cs::RESP_EMPTYLIST);
          }
          break 'arm Ok(true);
        }

        let write_head = |buf: &mut Vec<u8>, n| {
          if count_val > 0 {
            cs::write_set_len(buf, n, resp_protocol_version);
          } else {
            buf.write_resp_array_len(n);
          }
        };

        let reserved = resp_frame_head_len(n, write_head);
        let base = reserve_resp_frame_head(output, reserved);
        let streamed = random_stream_wraparound(
          output,
          &start_key,
          n,
          count_val > 0,
          |start, bound, budget, out| {
            random_scan_round(
              tree,
              start,
              bound,
              budget,
              ScanReturnField::Key,
              // Set 恒零字段级到期（写单源 0x00 无刻度帧），存活谓词恒真
              RandomScanEmit {
                alive: &mut |_, _| true,
                emit: &mut |k, _, out| out.write_resp_bulk_string(k),
              },
              out,
            )
          },
        );

        let total = match streamed {
          Ok(t) => t,
          Err(()) => {
            output.truncate(base);
            return Err(());
          }
        };
        backfill_resp_frame_head(output, base, reserved, total, write_head);
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
  // 稳态写命令镜像收尾单源（判据与不变式见 emit_tiered_mirror_finish）
  emit_tiered_mirror_finish(session, key, ctx, mirror, output, frame_base, result)
}
