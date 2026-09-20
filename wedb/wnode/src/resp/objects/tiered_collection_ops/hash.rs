use itoa::Buffer as ItoaBuffer;
use wbase::{
  num::{strict_f64, strict_i64},
  time::now_ticks,
};
use wbftree::{BfTreeReadResult, ScanReturnField};
use wcol::{
  hash::hash_object::HashOperation,
  types::member_ttl::{decode_member, member_expired_at},
};
use wdev::Device;
use wkv::BatchStoreSession;
use wresp::{
  cmd_strings as cs,
  ext::{RespVecExt, backfill_resp_frame_head, reserve_resp_frame_head, resp_frame_head_len},
  resp_memory_writer::format_double,
};
use wval::GarnetObjectType;
use zmij::Buffer as ZmijBuffer;

use super::common::{
  SweepOutcome, TieredCollectionArgs, TieredCtx, TieredMirror, emit_tiered_mirror,
  expire_sweep_or_rebuild, finish_tiered_arm, save_tiered_meta, scan_count, tiered_count,
  tiered_guard, tiered_precheck, tiered_write_mirror, tree_member_state, tree_put_batch,
  tree_put_ok, tree_put_rejected,
};

/// 哈希族写面判定（一处定义）：多步写臂与含 [`expire_sweep_or_rebuild`] 校正面
/// 的输出臂（Hgetall/Hkeys/Hvals 到期成员出账重灌）均取独占写锁；计数臂
/// Hlen 稳态共享读锁 O(1) 直读（水位越过经 [`tiered_count`] 升级写锁出账）；
/// 纯读臂（Hget/Hmget/Hexists/Hstrlen）与穿透臂（HDEL / HEXPIRE / HTTL /
/// HPERSIST / HRANDFIELD 等，经物化降级整值重灌）维持共享读锁
fn hash_needs_write(op: HashOperation) -> bool {
  matches!(
    op,
    HashOperation::Hset
      | HashOperation::Hmset
      | HashOperation::Hsetnx
      | HashOperation::Hincrby
      | HashOperation::Hincrbyfloat
      | HashOperation::Hgetall
      | HashOperation::Hkeys
      | HashOperation::Hvals
  )
}

/// 哈希族树内稳态写命令判定（AOF 命令镜像面一处定义，刻意窄于
/// [`hash_needs_write`] 锁面）：仅写语义命令产镜像事件；Hlen/Hgetall/Hkeys/
/// Hvals 是读校正臂，到期出账的树变更已由 promote 重灌流整树镜像，
/// 命令本身无写语义不产命令记录
fn tiered_hash_writes(op: HashOperation) -> bool {
  matches!(
    op,
    HashOperation::Hset
      | HashOperation::Hmset
      | HashOperation::Hsetnx
      | HashOperation::Hincrby
      | HashOperation::Hincrbyfloat
  )
}

/// 执行分层态哈希命令（WATCH 栅栏由 [`finish_tiered_arm`] 统一收尾）
pub(crate) async fn exec_tiered_hash<D: Device>(
  session: &BatchStoreSession<'_, D>,
  key: &[u8],
  ctx: &mut TieredCtx<'_>,
  call: TieredCollectionArgs<'_, HashOperation>,
  output: &mut Vec<u8>,
) -> Result<bool, ()> {
  let TieredCollectionArgs {
    op,
    args12,
    args,
    resp_protocol_version,
  } = call;
  let mirror = tiered_write_mirror(GarnetObjectType::Hash, op, args12, args, tiered_hash_writes);
  let handled = tiered_hash_arm(
    session,
    key,
    ctx,
    TieredCollectionArgs::new(op, args12, args, resp_protocol_version),
    output,
    mirror,
  )
  .await;
  finish_tiered_arm(session, key, ctx, handled)
}

/// 分层态哈希命令树内主体（读写臂分派，见 [`exec_tiered_hash`]）
async fn tiered_hash_arm<D: Device>(
  session: &BatchStoreSession<'_, D>,
  key: &[u8],
  ctx: &mut TieredCtx<'_>,
  call: TieredCollectionArgs<'_, HashOperation>,
  output: &mut Vec<u8>,
  mirror: Option<TieredMirror<'_>>,
) -> Result<bool, ()> {
  let TieredCollectionArgs {
    op,
    // arg1/arg2 压缩字已无树内消费臂（HEXPIRE 族穿透物化降级，压缩字由
    // run_async_rmw 的 run_op 闭包捕获透传对象层），显式弃绑防未用告警
    args12: _,
    args,
    resp_protocol_version,
  } = call;
  // 写臂独占互斥（锁内刷新元记录），读臂共享锁（判定一处定义）
  let Some(tree_guard) = tiered_guard(session, key, ctx, hash_needs_write(op)).await? else {
    return Ok(false);
  };
  let tree = tree_guard.tree();

  let result = 'arm: {
    match op {
      HashOperation::Hset | HashOperation::Hmset | HashOperation::Hsetnx => {
        let is_nx = op == HashOperation::Hsetnx;
        if is_nx {
          if args.len() != 2 {
            break 'arm Err(());
          }
          let field = args[0];
          let val = args[1];
          // C# contains_key 口径：到期字段视同不存在 → 插入（物理覆盖替换，size
          // 不变即死→活转换；存活字段命中则拒写）
          let now = now_ticks();
          if !tiered_precheck(ctx, field, val.len(), None, output) {
            break 'arm Ok(true);
          }
          match tree_member_state(tree, field, now) {
            Some((_, false)) => {
              output.write_resp_int(0);
              break 'arm Ok(true);
            }
            state => {
              // 插成功才计数、才回 1（libs/server/Objects/Hash/HashObjectImpl.cs
              // :SetIfNotExists 计数与写入天然同步的分层等价判据）
              if !tree_put_ok(ctx, tree, field, val, None) {
                tree_put_rejected(output);
                break 'arm Ok(true);
              }
              if state.is_none() {
                ctx.meta.size += 1;
              }
              // 落盘失败：树内写已生效，break 落臂尾补镜像再上抛（不变式见 save_tiered_meta）
              if save_tiered_meta(session, key, ctx).await.is_err() {
                break 'arm Err(());
              }
            }
          }
          output.write_resp_int(1);
          break 'arm Ok(true);
        }

        // 覆盖写清字段 TTL（C# HashSet 变更分支移除 expiration 条目，
        // libs/server/Objects/Hash/HashObjectImpl.cs:HashSet）；到期旧记录物理
        // 覆盖（size 不变，等价 C# 先 DeleteExpiredItems 再 Add 的净计数）
        // 预校验先于任何写入（RI 批量口径：任一字段越契约即整体失败，零树内副作用）
        for chunk in args.as_chunks::<2>().0 {
          if !tiered_precheck(ctx, chunk[0], chunk[1].len(), None, output) {
            break 'arm Ok(true);
          }
        }
        // 到期出账前置（对齐 C# HashSet 入口先 DeleteExpiredItems 再按缺席计 1，
        // libs/server/Objects/Hash/HashObjectImpl.cs:187/:206）：水位越过先经既有
        // 出账内核物理剔除到期成员再落折叠，批量 upsert 的树内 Found 两态前查才
        // 与「真缺席」同计（覆写到期字段回复 1，两态一致；水位内树内零到期成员，
        // 直落折叠计数天然精确）。Swept 臂元记录已换新（重灌换树 / 水位回写）：
        // 重装载存根快照重取守卫（锁内刷新，与穿透臂既有形态同）；全到期批出账
        // 删空自愈（键消亡）则 Ok(false) 穿透，物化降级段按键不存在新建承接
        //（C# 清空后 Add 的净字典语义）
        let tree_guard =
          match expire_sweep_or_rebuild(session, key, ctx, tree_guard, |_| {}).await? {
            SweepOutcome::Below(guard) => guard,
            SweepOutcome::Swept { .. } => {
              let Some((meta, stub)) = session.load_collection_stub(key).await.map_err(|_| ())?
              else {
                break 'arm Ok(false);
              };
              *ctx.meta = meta;
              *ctx.stub = stub;
              let Some(guard) = tiered_guard(session, key, ctx, true).await? else {
                break 'arm Ok(false);
              };
              guard
            }
          };
        let tree = tree_guard.tree();

        // 批量折叠：编码整批经排序批量 upsert 内核一次下刷（栈上排序集中命中
        // 叶页），返回值即真实新增键数——到期旧记录经上述出账前置后不在树，
        // 与逐条前探 tree_member_state 的「插成功才计数」判据等价（前查由内核
        // 单次借用承担）
        let entries: Vec<(&[u8], &[u8])> = args
          .as_chunks::<2>()
          .0
          .iter()
          .map(|chunk| (chunk[0], chunk[1]))
          .collect();
        let new_fields = match tree_put_batch(tree, &entries) {
          Ok(n) => n,
          Err(_) => {
            tree_put_rejected(output);
            break 'arm Ok(true);
          }
        };
        // 置脏判据：写成功即树内容变更（覆盖字段计数为 0 但值字节被替换）
        ctx.dirty |= !entries.is_empty();

        if new_fields > 0 {
          ctx.meta.size += new_fields;
          // 落盘失败：树内写已生效，break 落臂尾补镜像再上抛（不变式见 save_tiered_meta）
          if save_tiered_meta(session, key, ctx).await.is_err() {
            break 'arm Err(());
          }
        }

        if op == HashOperation::Hmset {
          output.extend_from_slice(cs::RESP_OK);
        } else {
          output.write_resp_int(new_fields as i64);
        }
        Ok(true)
      }

      HashOperation::Hget => {
        if args.is_empty() {
          break 'arm Err(());
        }
        let field = args[0];
        let now = now_ticks();
        // 到期字段视同不存在（C# TryGetValue 口径，
        // HashObject.ContainsKey/TryGetValue 语义）
        let mut alive = false;
        tree.read_callback(field, |res, raw| {
          if res == BfTreeReadResult::Found {
            if !member_expired_at(raw, now) {
              alive = true;
              output.write_resp_bulk_string(decode_member(raw).1);
            }
            true
          } else {
            false
          }
        });
        if !alive {
          output.write_resp_null_ver(resp_protocol_version);
        }
        Ok(true)
      }

      HashOperation::Hmget => {
        let now = now_ticks();
        output.write_resp_array_len(args.len());
        for &field in args {
          let mut alive = false;
          tree.read_callback(field, |res, raw| {
            if res == BfTreeReadResult::Found {
              if !member_expired_at(raw, now) {
                alive = true;
                output.write_resp_bulk_string(decode_member(raw).1);
              }
              true
            } else {
              false
            }
          });
          if !alive {
            output.write_resp_null_ver(resp_protocol_version);
          }
        }
        Ok(true)
      }

      HashOperation::Hexists => {
        if args.is_empty() {
          break 'arm Err(());
        }
        let now = now_ticks();
        let exists = matches!(tree_member_state(tree, args[0], now), Some((_, false)));
        output.write_resp_int(if exists { 1 } else { 0 });
        Ok(true)
      }

      HashOperation::Hlen => {
        // 计数（同分层 Zcard 臂，共 [`tiered_count`] 内核）：稳态水位内共享读锁
        // 锁内重读元记录 O(1) 直读 size；水位越过升级写锁物理出账（树内零墓碑，
        // 有到期才重灌），O(N) 每到期纪元至多一次（collection.md 大键 O(1) 计数
        // 规约第 3 条分层态补则）
        let size = tiered_count(session, key, ctx, tree_guard).await?;
        output.write_resp_int(size as i64);
        Ok(true)
      }

      HashOperation::Hstrlen => {
        if args.is_empty() {
          break 'arm Err(());
        }
        let now = now_ticks();
        let mut len = 0usize;
        tree.read_callback(args[0], |res, raw| {
          if res == BfTreeReadResult::Found && !member_expired_at(raw, now) {
            len = decode_member(raw).1.len();
          }
          res == BfTreeReadResult::Found
        });
        output.write_resp_int(len as i64);
        Ok(true)
      }

      HashOperation::Hgetall => {
        // 帧头与实体同源：RESP3 写 `%<对数>`、RESP2 退化为 `*<2×对数>`（对标 C#
        // HashObjectImpl.HashGetAll WriteMapLength(Count())）。分层态对数不可先验
        //（成员级到期语义下 meta.size 与实际存活对数可不符），故走 wresp 单点
        // 预留-回填：字段对逐条直写最终 output，帧头位宽按 size 上界估算预留、
        // 扫完以**实际出帧对数**回填（size 只作移动量提示，严禁落头，见
        // wresp::ext::RESP_FRAME_HEAD_RESERVED 文注不变式 1）
        let write_head = |buf: &mut Vec<u8>, n| cs::write_map_len(buf, n, resp_protocol_version);
        let reserved = resp_frame_head_len(ctx.meta.size as usize, write_head);
        let base = reserve_resp_frame_head(output, reserved);
        let mut pairs = 0usize;
        let outcome = match expire_sweep_or_rebuild(session, key, ctx, tree_guard, |live| {
          // 水位命中：存活全集在重灌前经闭包交出（到期成员已被扫描剔除），零二次扫树
          for (field, record) in live {
            output.write_resp_bulk_string(field);
            output.write_resp_bulk_string(decode_member(record).1);
            pairs += 1;
          }
        })
        .await
        {
          Ok(outcome) => outcome,
          // 撤帧（连同预留头回到臂进入点，不变式 2）后按臂内既有 `?` 口径直接出
          // 函数（错误帧由上游漏斗闭环）：本臂是持写锁的读校正臂，镜像面判定
          // [`tiered_hash_writes`] 刻意不含它，臂尾 emit 本就是 no-op——出账重灌
          // 的树变更镜像由 promote 重灌流整树承接，两条 Err 出口（封窗被拒 / 重灌
          // 发布失败）树内容均未变更，落臂尾反而无据可入
          Err(()) => {
            output.truncate(base);
            return Err(());
          }
        };
        if let SweepOutcome::Below(guard) = outcome {
          // 水位未命中：守卫原样奉还，树内无到期成员，流式直出；扫描 Err 同口
          // 撤帧后上抛（应答尚未落帧，上游闭环成 RESP 错误帧）
          if scan_count(guard.tree().scan_with_count_callback(
            &[0u8],
            usize::MAX,
            ScanReturnField::KeyAndValue,
            |k, v| {
              output.write_resp_bulk_string(k);
              output.write_resp_bulk_string(decode_member(v).1);
              pairs += 1;
              true
            },
          ))
          .is_err()
          {
            output.truncate(base);
            return Err(());
          }
        }
        backfill_resp_frame_head(output, base, reserved, pairs, write_head);
        Ok(true)
      }

      HashOperation::Hkeys => {
        // 帧头与实体同源（数组语义，HKEYS RESP3 仍数组）：预留-回填与撤帧同
        // Hgetall 臂，一处做法一处口径
        let write_head = |buf: &mut Vec<u8>, n| buf.write_resp_array_len(n);
        let reserved = resp_frame_head_len(ctx.meta.size as usize, write_head);
        let base = reserve_resp_frame_head(output, reserved);
        let mut n = 0usize;
        let outcome = match expire_sweep_or_rebuild(session, key, ctx, tree_guard, |live| {
          for (field, _) in live {
            output.write_resp_bulk_string(field);
            n += 1;
          }
        })
        .await
        {
          Ok(outcome) => outcome,
          Err(()) => {
            output.truncate(base);
            return Err(());
          }
        };
        if let SweepOutcome::Below(guard) = outcome
          && scan_count(guard.tree().scan_with_count_callback(
            &[0u8],
            usize::MAX,
            ScanReturnField::Key,
            |k, _| {
              output.write_resp_bulk_string(k);
              n += 1;
              true
            },
          ))
          .is_err()
        {
          output.truncate(base);
          return Err(());
        }
        backfill_resp_frame_head(output, base, reserved, n, write_head);
        Ok(true)
      }

      HashOperation::Hvals => {
        // 帧头与实体同源（数组语义，HVALS RESP3 仍数组）：预留-回填与撤帧同
        // Hgetall 臂
        let write_head = |buf: &mut Vec<u8>, n| buf.write_resp_array_len(n);
        let reserved = resp_frame_head_len(ctx.meta.size as usize, write_head);
        let base = reserve_resp_frame_head(output, reserved);
        let mut n = 0usize;
        let outcome = match expire_sweep_or_rebuild(session, key, ctx, tree_guard, |live| {
          for (_, record) in live {
            output.write_resp_bulk_string(decode_member(record).1);
            n += 1;
          }
        })
        .await
        {
          Ok(outcome) => outcome,
          Err(()) => {
            output.truncate(base);
            return Err(());
          }
        };
        if let SweepOutcome::Below(guard) = outcome
          && scan_count(guard.tree().scan_with_count_callback(
            &[0u8],
            usize::MAX,
            ScanReturnField::Value,
            |_, v| {
              output.write_resp_bulk_string(decode_member(v).1);
              n += 1;
              true
            },
          ))
          .is_err()
        {
          output.truncate(base);
          return Err(());
        }
        backfill_resp_frame_head(output, base, reserved, n, write_head);
        Ok(true)
      }

      HashOperation::Hincrby => {
        if args.len() < 2 {
          break 'arm Err(());
        }
        let field = args[0];
        let incr_slice = args[1];
        // 入参增量解析失败（进树前前置校验）：回 C# 同款值域错误
        //（HashObjectImpl.cs HashIncrement 的 NumUtils.TryParse 失败 →
        // RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER，信封层 hash_increment 同面），
        // 严禁折叠成慢路径存储错误与信封臂分叉
        let Some(incr) = strict_i64(incr_slice) else {
          cs::write_error_raw(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
          break 'arm Ok(true);
        };
        let now = now_ticks();
        let mut cur_val = 0i64;
        let mut is_new = true;
        // 现存值非数字（Found 且未到期却解析失败）：对齐 C# HashIncrement /
        // 对象层 hash_increment，回 RESP_ERR_HASH_VALUE_IS_NOT_INTEGER 且不写
        // （不落库、不动 size、不推进 dirty，与 HSETNX 命中已存在同属未走写漏斗出口）
        let mut bad_value = false;
        // 到期命中独立标志（三态判据：真缺席 / 到期在树 / 存活，与 HSETNX·ZADD
        // 收敛同一判据面）：到期在树走物理覆盖零计数——成员已在 size 中（记账
        // 契约见 [`sweep_expired_members`]），覆盖后条目数不变，再 +1 即永久
        // 虚增且无收敛点（覆盖为存活后 sweep 不再将其数入出账）；C# 净零由
        // DeleteExpiredItems 先摘（size 减）再加（size 加）承担
        //（libs/server/Objects/Hash/HashObjectImpl.cs HashIncrement:303 /
        // DeleteExpiredItemsWorker），物理覆盖即其分层对偶
        let mut expired_hit = false;
        // 存活成员增量保留既有 TTL（C# HashIncrement 不动 expiration）；到期
        // 旧记录视同不存在（C# 增量入口先 DeleteExpiredItems）
        let mut old_expiry = None;
        tree.read_callback(field, |res, raw| {
          if res == BfTreeReadResult::Found {
            let (expiry, payload) = decode_member(raw);
            if member_expired_at(raw, now) {
              expired_hit = true;
              return true;
            }
            is_new = false;
            old_expiry = expiry;
            // 现存值用对象层同口径的 strict_i64（单源于 wbase::num::strict_i64）判定可解析性
            match strict_i64(payload) {
              Some(n) => cur_val = n,
              None => bad_value = true,
            }
          }
          res == BfTreeReadResult::Found
        });
        if bad_value {
          cs::write_error_raw(output, cs::RESP_ERR_HASH_VALUE_IS_NOT_INTEGER);
          break 'arm Ok(true);
        }
        // 新字段：存/回增量原文（对齐对象层 add(incr_slice)+write_integer_from_bytes，
        // 与内存态逐字节一致，如输入 "5" 存 "5"）；wrapping 溢出语义与对象层同
        if is_new {
          // 预校验先于写入（RI 单点口径，零树内副作用）；插成功才计数回值
          if !tiered_precheck(ctx, field, incr_slice.len(), old_expiry, output) {
            break 'arm Ok(true);
          }
          if !tree_put_ok(ctx, tree, field, incr_slice, old_expiry) {
            tree_put_rejected(output);
            break 'arm Ok(true);
          }
          // 真缺席才计数；到期命中物理覆盖零计数（C# 先摘后加净零的对偶）
          if !expired_hit {
            ctx.meta.size += 1;
            // 落盘失败：树内写已生效，break 落臂尾补镜像再上抛（不变式见 save_tiered_meta）
            if save_tiered_meta(session, key, ctx).await.is_err() {
              break 'arm Err(());
            }
          }
          output.resp_writer2().write_integer_from_bytes(incr_slice);
          break 'arm Ok(true);
        }
        let new_val = cur_val.wrapping_add(incr);
        let mut itoa_buf = ItoaBuffer::new();
        let formatted = itoa_buf.format(new_val);
        if !tree_put_ok(ctx, tree, field, formatted.as_bytes(), old_expiry) {
          tree_put_rejected(output);
          break 'arm Ok(true);
        }
        output.write_resp_int(new_val);
        Ok(true)
      }

      HashOperation::Hincrbyfloat => {
        if args.len() < 2 {
          break 'arm Err(());
        }
        let field = args[0];
        let incr_slice = args[1];
        // 入参增量两态值域门（对标 C# HashIncrementFloat：parseState.TryGetDouble
        // canBeInfinite 默认 true → 非浮点回 RESP_ERR_NOT_VALID_FLOAT；随后
        // double.IsInfinity 回 RESP_ERR_GENERIC_NAN_INFINITY，±inf 词形与纯数值
        // 溢出同归此门，libs/server/Objects/Hash/HashObjectImpl.cs:346-352）。
        // 进树前前置校验，严禁折叠成慢路径存储错误与信封臂分叉
        let Some(incr) = strict_f64(incr_slice, true) else {
          cs::write_error_raw(output, cs::RESP_ERR_NOT_VALID_FLOAT);
          break 'arm Ok(true);
        };
        if incr.is_infinite() {
          cs::write_error_raw(output, cs::RESP_ERR_GENERIC_NAN_INFINITY);
          break 'arm Ok(true);
        }
        let now = now_ticks();
        let mut cur_val = 0.0f64;
        let mut is_new = true;
        let mut old_expiry = None;
        // 到期命中独立标志（三态判据与 HINCRBY 同面，见其文注）：到期在树物理
        // 覆盖零计数，真缺席才 +1
        let mut expired_hit = false;
        // 现存值两态错误（对齐 C# HashIncrementFloat 双门）：非浮点 → NOT_FLOAT；
        // 可解析但为 ±inf → NAN_INFINITY_INCR；均回错且不写、不动 size、不推进 dirty
        let mut err: Option<&'static str> = None;
        tree.read_callback(field, |res, raw| {
          if res == BfTreeReadResult::Found {
            let (expiry, payload) = decode_member(raw);
            if member_expired_at(raw, now) {
              expired_hit = true;
              return true;
            }
            is_new = false;
            old_expiry = expiry;
            // strict_f64(.., true)（允许 inf 词形，
            // 与对象层 strict_f64(&hash_value, true) 单源一致），据此区分
            // 「非浮点」与「现存值为无穷」两态
            match strict_f64(payload, true) {
              Some(n) if n.is_infinite() => err = Some(cs::RESP_ERR_GENERIC_NAN_INFINITY_INCR),
              Some(n) => cur_val = n,
              None => err = Some(cs::RESP_ERR_HASH_VALUE_IS_NOT_FLOAT),
            }
          }
          res == BfTreeReadResult::Found
        });
        if let Some(msg) = err {
          cs::write_error_raw(output, msg);
          break 'arm Ok(true);
        }
        // 新字段：存/回增量原文（对齐对象层 add(incr_slice)+write_bulk_string，
        // 与内存态逐字节一致，如输入 "0.10" 存 "0.10"、回 bulk "0.10"）
        if is_new {
          // 预校验先于写入（RI 单点口径，零树内副作用）；插成功才计数回值
          if !tiered_precheck(ctx, field, incr_slice.len(), old_expiry, output) {
            break 'arm Ok(true);
          }
          if !tree_put_ok(ctx, tree, field, incr_slice, old_expiry) {
            tree_put_rejected(output);
            break 'arm Ok(true);
          }
          // 真缺席才计数；到期命中物理覆盖零计数（同 HINCRBY 三态判据）
          if !expired_hit {
            ctx.meta.size += 1;
            // 落盘失败：树内写已生效，break 落臂尾补镜像再上抛（不变式见 save_tiered_meta）
            if save_tiered_meta(session, key, ctx).await.is_err() {
              break 'arm Err(());
            }
          }
          output.write_resp_bulk_string(incr_slice);
          break 'arm Ok(true);
        }
        let new_val = cur_val + incr;
        let mut zmij_buf = ZmijBuffer::new();
        let formatted = format_double(new_val, &mut zmij_buf);
        if !tree_put_ok(ctx, tree, field, formatted.as_bytes(), old_expiry) {
          tree_put_rejected(output);
          break 'arm Ok(true);
        }
        output.write_resp_bulk_string(formatted.as_bytes());
        Ok(true)
      }

      // 未支持操作一律穿透（Ok(false)）：由 run_async_rmw 物化降级通道接手，
      // 杜绝静默兜底输出与命令语义无关的应答——HCOLLECT / HRANDFIELD 族经此
      // 落 wcol 对象层单源求值，WATCH 推进同臂由 apply_rmw_post_operate 承接。
      // HDEL 与成员级 TTL 面（HEXPIRE / HTTL / HPERSIST 族，原树内逐成员出账臂
      // 已删）亦在此穿透：删除与到期出账一律走「物化 → 对象层单源求值 →
      // 整值重灌（bulk_load 重建）」，树内零墓碑，见本模块头注
      _ => Ok(false),
    }
  };
  // 稳态写命令镜像收尾：树守卫存续窗口内入账（锁内 emit，AOF 序与树内提交
  // 序严格一致，见 emit_tiered_mirror 文注）；早退臂未走写漏斗不置脏自然跳过，
  // 落盘失败臂经 save_tiered_meta 以 break 'arm 落本收尾——镜像先行再上抛
  //（树已变更 ⟺ 镜像已入账，见 emit_tiered_mirror 头注不变式）
  emit_tiered_mirror(session, key, ctx, mirror);
  result
}

#[cfg(test)]
mod tests {
  use std::sync::Arc;

  use compio::runtime::Runtime;
  use tempfile::tempdir;
  use wcol::types::member_ttl::encode_member;
  use wdev::SegmentedDevice;
  use wkv::{StoreConfig, WedbStore};
  use wval::GarnetObjectType;

  use super::*;
  use crate::resp::objects::tiered_collection_ops::common::{TieredCollectionArgs, TieredCtx};

  #[test]
  fn test_tiered_hash_hincrby_strict_i64() {
    let dir = tempdir().unwrap();
    let config = StoreConfig::new(2048, 1024 * 1024, 16, 0.5).unwrap();
    let device =
      Arc::new(SegmentedDevice::single_file(dir.path().join("tiered_hash_hincrby.db")).unwrap());
    let store = Arc::new(WedbStore::open(config, device).unwrap());
    let rt = Runtime::new().unwrap();
    let sess = store.new_session().unwrap();
    let key = b"tiered_hash_key";

    // 灌入初始字段并升阶为分层态
    let entries: Vec<(Vec<u8>, Vec<u8>)> = vec![
      (b"f_valid".to_vec(), encode_member(b"10", None)),
      (b"f_str".to_vec(), encode_member(b"abc", None)),
      (b"f_lead0".to_vec(), encode_member(b"010", None)),
      (b"f_space".to_vec(), encode_member(b" 10", None)),
      (b"f_float".to_vec(), encode_member(b"10.5", None)),
    ];

    rt.block_on(sess.promote_collection_to_bftree(
      key,
      GarnetObjectType::Hash,
      entries,
      i64::MAX,
      false,
    ))
    .unwrap();

    let batch = sess.enter_batch();
    let (mut meta, mut stub) = rt
      .block_on(sess.load_collection_stub(key))
      .unwrap()
      .unwrap();

    let run_hincrby = |field: &[u8], incr: &[u8], meta: &mut _, stub: &mut _| {
      let mut ctx = TieredCtx::new(meta, stub);
      let mut output = Vec::new();
      let args: &[&[u8]] = &[field, incr];
      let call = TieredCollectionArgs::new(HashOperation::Hincrby, (0, 0), args, 2);
      let res = rt.block_on(exec_tiered_hash(&batch, key, &mut ctx, call, &mut output));
      (res, output)
    };

    // 1. 合法整数旧值：累加成功
    {
      let (res, output) = run_hincrby(b"f_valid", b"5", &mut meta, &mut stub);
      assert_eq!(res, Ok(true));
      assert_eq!(output, b":15\r\n");
    }

    // 2. 字符串旧值：解析失败，回 RESP_ERR_HASH_VALUE_IS_NOT_INTEGER
    {
      let (res, output) = run_hincrby(b"f_str", b"5", &mut meta, &mut stub);
      assert_eq!(res, Ok(true));
      assert_eq!(output, b"-ERR hash value is not an integer.\r\n");
    }

    // 3. 前导零旧值：strict_i64 拦截，回 RESP_ERR_HASH_VALUE_IS_NOT_INTEGER（原 parse::<i64> 会错误放行）
    {
      let (res, output) = run_hincrby(b"f_lead0", b"5", &mut meta, &mut stub);
      assert_eq!(res, Ok(true));
      assert_eq!(output, b"-ERR hash value is not an integer.\r\n");
    }

    // 4. 前导空格旧值：strict_i64 拦截，回 RESP_ERR_HASH_VALUE_IS_NOT_INTEGER
    {
      let (res, output) = run_hincrby(b"f_space", b"5", &mut meta, &mut stub);
      assert_eq!(res, Ok(true));
      assert_eq!(output, b"-ERR hash value is not an integer.\r\n");
    }

    // 5. 浮点数字符串旧值：回 RESP_ERR_HASH_VALUE_IS_NOT_INTEGER
    {
      let (res, output) = run_hincrby(b"f_float", b"5", &mut meta, &mut stub);
      assert_eq!(res, Ok(true));
      assert_eq!(output, b"-ERR hash value is not an integer.\r\n");
    }

    // 6. 入参增量非整数：回 RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER
    {
      let (res, output) = run_hincrby(b"f_valid", b"abc", &mut meta, &mut stub);
      assert_eq!(res, Ok(true));
      // 对位 C# RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER（CmdStrings.cs:246 原文带句点）
      assert_eq!(output, b"-ERR value is not an integer or out of range.\r\n");
    }

    // 7. 新字段：直接写入增量原文
    {
      let (res, output) = run_hincrby(b"f_new", b"7", &mut meta, &mut stub);
      assert_eq!(res, Ok(true));
      assert_eq!(output, b":7\r\n");
    }
  }
}
