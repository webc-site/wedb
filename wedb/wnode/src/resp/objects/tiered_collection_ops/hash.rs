use itoa::Buffer as ItoaBuffer;
use wbase::{map::HashSet, time::now_ticks};
use wbftree::{BfTreeReadResult, BfTreeService, ScanReturnField};
use wcol::{
  hash::{
    hash_object::HashOperation,
    hash_object_impl::{HashIncrStage, parse_hash_incr, parse_hash_incr_float},
  },
  types::member_ttl::{decode_member, member_expired_at},
};
use wdev::Device;
use wkv::{BatchStoreSession, TreeGuard};
use wresp::{
  cmd_strings as cs,
  ext::{
    RespVecExt, backfill_resp_frame_head, is_resp3, reserve_resp_frame_head, resp_frame_head_len,
  },
  resp_memory_writer::format_double,
};
use wval::GarnetObjectType;
use zmij::Buffer as ZmijBuffer;

use super::common::{
  OutFace, RandomScanEmit, RespHead, SCAN_FROM_HEAD, SweepOutcome, TieredCollectionArgs, TieredCtx,
  TieredOpError, emit_tiered_mirror_finish, expire_sweep_or_rebuild, finish_tiered_arm,
  member_expiry_probe, random_scan_round, random_stream_wraparound, save_tiered_meta, scan_count,
  sweep_output_face, tiered_guard, tiered_precheck, tiered_write_mirror, tree_member_state,
  tree_put_batch, tree_put_ok, tree_put_rejected,
};

/// 哈希族写面判定（一处定义）：多步写臂与含 [`expire_sweep_or_rebuild`] 校正面
/// 的输出臂（Hgetall/Hkeys/Hvals 到期成员出账重灌）均取独占写锁；纯读臂
///（Hget/Hmget/Hexists/Hstrlen/Hrandfield）与穿透臂（HDEL / HEXPIRE / HTTL /
/// HPERSIST 等，经物化降级整值重灌）维持共享读锁（HLEN 由长度族慢路径
/// [`super::exec_tiered_collect`] 承接唯一计数校正）
pub(crate) fn hash_needs_write(op: HashOperation) -> bool {
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
/// [`hash_needs_write`] 锁面）：仅写语义命令产镜像事件；Hgetall/Hkeys/
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
///
/// [`doc(hidden)`] 测试专用隐藏面：分层臂逐字段语义集成测
///（wnode/tests/tiered_hash_hincrby_parse_gates.rs——解析基座与无穷门
/// 直驱断言），生产入口为 [`super::exec_tiered_by_op_code`] 分派单点，
/// 非公共 API 契约
///
/// 错误面与 [`super::exec_tiered_list`] / [`super::exec_tiered_set`] 同形：
/// 内臂 `Err(())` 在边界折叠为 [`TieredOpError`] 单元错误（对齐既有 pub 口
/// 的 `result_unit_err` 门）
#[doc(hidden)]
pub async fn exec_tiered_hash<D: Device>(
  session: &BatchStoreSession<'_, D>,
  key: &[u8],
  ctx: &mut TieredCtx<'_>,
  call: TieredCollectionArgs<'_, HashOperation>,
  output: &mut Vec<u8>,
) -> Result<bool, TieredOpError> {
  let handled = tiered_hash_arm(session, key, ctx, call, output).await;
  finish_tiered_arm(session, key, ctx, handled).map_err(|_| TieredOpError)
}

/// 单字段值出帧（HGET / HMGET 共用）：存活字段直写 bulk、到期或不存在写版本感知
/// null（C# TryGetValue 口径：到期字段视同不存在）
#[inline]
fn field_reply(
  tree: &BfTreeService,
  field: &[u8],
  now: i64,
  resp_protocol_version: u8,
  output: &mut Vec<u8>,
) {
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

/// HGETALL / HKEYS / HVALS 三臂出帧形态包（投影域 + 帧头；打包压参，形参个数
/// 收在 too-many-arguments 门内）
struct HashSweepFace {
  field: ScanReturnField,
  head: RespHead,
}

/// HGETALL / HKEYS / HVALS 三臂共同出帧壳（`meta.size` 仅作预留位宽提示，
/// 分层态对数不可先验；出账-流式内核单源 [`sweep_output_face`]）
async fn sweep_hash_face<'s, D: Device>(
  session: &'s BatchStoreSession<'_, D>,
  key: &[u8],
  ctx: &mut TieredCtx<'_>,
  tree_guard: TreeGuard<'s>,
  output: &mut Vec<u8>,
  face: HashSweepFace,
  emit: impl FnMut(&mut Vec<u8>, &[u8], &[u8]),
) -> Result<(), ()> {
  sweep_output_face(
    session,
    key,
    ctx,
    tree_guard,
    output,
    OutFace::from_head(face.field, ctx.meta.size as usize, face.head),
    emit,
  )
  .await
}

/// 分层态哈希命令树内主体（读写臂分派，见 [`exec_tiered_hash`]）
async fn tiered_hash_arm<D: Device>(
  session: &BatchStoreSession<'_, D>,
  key: &[u8],
  ctx: &mut TieredCtx<'_>,
  call: TieredCollectionArgs<'_, HashOperation>,
  output: &mut Vec<u8>,
) -> Result<bool, ()> {
  let TieredCollectionArgs {
    op,
    // arg1 压缩字仅 HRANDFIELD 树内抽样臂消费（count/included/withValues 打包，
    // 上游单源解析喂入）；arg2 种子无树内消费臂（HEXPIRE 族穿透物化降级，
    // 压缩字由 run_async_rmw 的 run_op 闭包捕获透传对象层）
    args12,
    args,
    resp_protocol_version,
  } = call;
  let mirror = tiered_write_mirror(GarnetObjectType::Hash, op, args12, args, tiered_hash_writes);
  // 本命令负载起点（臂尾镜像入账失败撤帧用，与 zset 臂 frame_base 同形）
  let frame_base = output.len();
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
        //（C# 清空后 Add 的净字典语义）。Swept 携带位非空 = 页缓存预算耗尽推迟
        // 臂（树原样、到期成员仍在树、磁盘元记录未动）：到期在树字段在下方折叠
        // 被按键已存在覆盖不计 new_fields，推迟键集留待交集叠加补偿计数
        let (tree_guard, deferred_expired) =
          match expire_sweep_or_rebuild(session, key, ctx, tree_guard, |_| {}).await? {
            SweepOutcome::Below(guard) => (guard, Vec::new()),
            SweepOutcome::Swept(expired_keys) => {
              let Some((meta, stub)) = session.load_collection_stub(key).await.map_err(|_| ())?
              else {
                ctx.dirty = false;
                break 'arm Ok(false);
              };
              *ctx.meta = meta;
              *ctx.stub = stub;
              let Some(guard) = tiered_guard(session, key, ctx, true).await? else {
                ctx.dirty = false;
                break 'arm Ok(false);
              };
              (guard, expired_keys)
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
        let mut new_fields = match tree_put_batch(tree, &entries) {
          Ok(n) => n,
          Err(_) => {
            tree_put_rejected(output);
            break 'arm Ok(true);
          }
        };
        // 推迟重灌窗补偿计数：到期在树字段物理覆盖（键已存在）不入 new_fields，
        // 本批去重字段命中推迟键集者按「到期视同缺席」叠加计入——对齐 C#
        // DeleteExpiredItems 后 `!exists || IsExpired` 臂 set++
        //（HashObjectImpl.cs:206-212），与内存态信封臂 / 预算充足重灌路径三向
        // 全等（上文臂注「覆写到期字段回复 1，两态一致」不变量）。交集判定
        // 复用出账扫描产物单点，不引入第二套三态前查机制；应答与 meta.size
        // 增量同源同值，瞬态 size 虚高由下个命令出账 live.len() 实存直赋收敛
        //（到期成员仍由 size 承载的未出账口径，见 sweep_expired_members 文注）
        if !deferred_expired.is_empty() {
          let batch_fields: HashSet<&[u8]> = entries.iter().map(|(f, _)| *f).collect();
          new_fields += deferred_expired
            .iter()
            .filter(|k| batch_fields.contains(k.as_slice()))
            .count() as u64;
        }
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
        field_reply(tree, args[0], now_ticks(), resp_protocol_version, output);
        Ok(true)
      }

      HashOperation::Hmget => {
        let now = now_ticks();
        output.write_resp_array_len(args.len());
        for &field in args {
          field_reply(tree, field, now, resp_protocol_version, output);
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
        // 分层态对数不可先验（成员级到期语义下 meta.size 与实际存活对数可不符），
        // 出帧走 [`sweep_hash_face`] 预留-回填单点（对标 C#
        // HashObjectImpl.HashGetAll WriteMapLength(Count())）：field/value 成对直写
        sweep_hash_face(
          session,
          key,
          ctx,
          tree_guard,
          output,
          HashSweepFace {
            field: ScanReturnField::KeyAndValue,
            head: RespHead::map(resp_protocol_version),
          },
          |out, field, record| {
            out.write_resp_bulk_string(field);
            out.write_resp_bulk_string(decode_member(record).1);
          },
        )
        .await?;
        Ok(true)
      }

      HashOperation::Hkeys => {
        // 数组语义（HKEYS RESP3 仍数组），出帧/撤帧与 Hgetall 臂同一内核
        sweep_hash_face(
          session,
          key,
          ctx,
          tree_guard,
          output,
          HashSweepFace {
            field: ScanReturnField::Key,
            head: RespHead::array(resp_protocol_version),
          },
          |out, field, _| out.write_resp_bulk_string(field),
        )
        .await?;
        Ok(true)
      }

      HashOperation::Hvals => {
        // 数组语义（HVALS RESP3 仍数组），出帧/撤帧与 Hgetall 臂同一内核
        sweep_hash_face(
          session,
          key,
          ctx,
          tree_guard,
          output,
          HashSweepFace {
            field: ScanReturnField::Value,
            head: RespHead::array(resp_protocol_version),
          },
          |out, _, record| out.write_resp_bulk_string(decode_member(record).1),
        )
        .await?;
        Ok(true)
      }

      HashOperation::Hincrby => {
        if args.len() < 2 {
          break 'arm Err(());
        }
        let field = args[0];
        let incr_slice = args[1];
        // 入参增量解析失败（进树前前置校验）：走信封态与分层臂共用的单点判据
        // parse_hash_incr（基座 = C# HashObjectImpl.cs HashIncrement 两调用点的
        // NumUtils.TryParse 对位 wbase::num::try_parse，接受前导零与 + 号；勿锚成
        // INCR 族的 strict_i64 严格档），错误文案分态由判据函数返回，严禁折叠
        // 成慢路径存储错误与信封臂分叉
        let incr = match parse_hash_incr(incr_slice, HashIncrStage::Incr) {
          Ok(v) => v,
          Err(msg) => {
            cs::write_error_raw(output, msg);
            break 'arm Ok(true);
          }
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
        // ——与 HSET 覆写清期相反（C# HashSet:226-236 expirationTimes.Remove／rust 信封
        // hash_set 覆写臂 ledger.remove_expiration／本文件 Hset 折叠臂「覆盖写清字段 TTL」），
        // 增量族存期系家族内刻意不对称，deviations §66a 回指注在册，勿顺手统一；
        // 锁面 tiered_field_ttl.rs::tiered_hash_incr_preserves_live_field_ttl 与
        // hash_ttl.rs::incr_family_preserves_live_field_ttl_envelope（浮点臂 old_expiry 逐点对位同理）
        let mut old_expiry = None;
        tree.read_callback(field, |res, raw| {
          member_expiry_probe(
            res,
            raw,
            now,
            &mut expired_hit,
            &mut is_new,
            &mut old_expiry,
            // 现存值用同一单点判据（与信封态 hash_increment 共用 parse_hash_incr）
            |payload| match parse_hash_incr(payload, HashIncrStage::Stock) {
              Ok(n) => cur_val = n,
              Err(_) => bad_value = true,
            },
          )
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
        // 入参增量两态值域门（进树前前置校验）：走信封态与分层臂共用的单点判据
        // parse_hash_incr_float（基座 strict_f64(raw, true)，对位 C# HashIncrementFloat
        // 增量侧 TryGetDouble(canBeInfinite:true)——"inf" 词形与纯数值溢出同落无穷门），
        // 文案分态由判据函数返回，严禁折叠成慢路径存储错误与信封臂分叉
        let incr = match parse_hash_incr_float(incr_slice, HashIncrStage::Incr) {
          Ok(v) => v,
          Err(msg) => {
            cs::write_error_raw(output, msg);
            break 'arm Ok(true);
          }
        };
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
          member_expiry_probe(
            res,
            raw,
            now,
            &mut expired_hit,
            &mut is_new,
            &mut old_expiry,
            // 与信封态共用单点判据 parse_hash_incr_float（基座 strict_f64(raw, true)，
            // 允许 inf 词形落无穷门），据此区分「非浮点」与「现存值为无穷」两态
            |payload| match parse_hash_incr_float(payload, HashIncrStage::Stock) {
              Ok(n) => cur_val = n,
              Err(msg) => err = Some(msg),
            },
          )
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
        // 求和不设结果门（与信封臂 hash_increment_float 同注同向，deviations §1 尾回指
        // 注／§80 同族第二消费位）：和逾 DBL_MAX 恒经 format_double 单源落 "inf"/"-inf"
        // 三字节刻形并携 old_expiry 回树，禁按 C# "Infinity" 词形回改、禁补复检门；
        // 存量/增量无穷门均在上游 parse_hash_incr_float 既有档
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

      HashOperation::Hrandfield => {
        // 树内只读抽样臂（§8.5 SRANDMEMBER 之树内臂对偶，票 zcode-r151c-smembers
        // 案一）：无 count 单成员形为全树单扫 reservoir 均匀抽样（fastrand 随机
        // 源），count 形为随机起始键回绕扫位 + 流式直写出帧，内存随应答
        // 条数，消除全树三拷贝物化堆峰。时间恒 O(N) 树扫一次起步
        //（collection.md §8.3 无字段序索引之固有折损在册，勿按「点查零扫描」
        // 再提案）；随机源与信封态独立、§12 不承诺同 seed 同序，字段值域
        // 近似抽样在集合/映射无序契约下语义等价（§8.5 在册口径）。
        // 三词元解析不在本臂：与信封兜底臂单源共上游
        // parse_random_member_args（hash_commands/slow.rs 路由门喂 arg1 打包字，
        // 对位 C# ObjectInput.arg1）；param_count==0 不触后端已上游短路。
        // 成员级 TTL 读侧单次 now 采样剔除（scan.rs 物化通道同 §53 口径），
        // 剔除后单程流式出帧、预留-回填令声明数恒等实际出帧数
        //（wresp::ext 不变式 1），剔除不固化（read.rs 头注 §5 最小案不扩面）。
        // 帧形与对象层 hash_random_field 逐字节对位：RESP2 WITHVALUES 平铺 2n
        // 数组头、RESP3 每项 `*2` 对帧。扫描恒取 KeyAndValue——TTL 判据须见
        // 记录 TTL 头，Key 投影不可见（此与 Srandmember 树内臂之必然而异）
        let arg1 = args12.0;
        let count_param = arg1 >> 2;
        let included_count = (arg1 >> 1) & 1 == 1;
        let with_values = arg1 & 1 == 1;
        let now = now_ticks();
        let start_key = fastrand::u64(..).to_be_bytes();
        // count 形：帧头预留-回填（存活数不可先验，严禁落 meta.size 于头，
        // 见 RESP_FRAME_HEAD_RESERVED 不变式 1）
        // 请求/可返条目基数（对标 C# HashObjectImpl.HashRandomField 各分支）：
        // 正 count 互异域钳 min(count, size) 上界（实际抽样域经成员级 TTL 剔除
        // 后由短供收敛至存活数），负 count 可重复取 |count|，无 count 形恒 1
        let n = if !included_count {
          1
        } else if count_param > 0 {
          (count_param as u64).min(ctx.meta.size) as usize
        } else {
          count_param.unsigned_abs() as usize
        };
        // 空集出口（结构性防御：MetaValue::is_live 门令 size==0 键判死，
        // tiered_guard 锁内刷新键消亡亦清零 size；语义仍对标对象层
        // purge 归零形——count 形 *0，无 count 形 nil，与 set.rs 空集三形同口径）
        if ctx.meta.size == 0 {
          if included_count {
            output.write_resp_array_len(0);
          } else {
            output.write_resp_null_ver(resp_protocol_version);
          }
          break 'arm Ok(true);
        }
        if !included_count {
          // 单成员形：全树一次流扫 + reservoir 等概率替换（fastrand 随机源，
          // 与对象层 pick_random_index(count, seed) 的均匀抽样形对位，§12
          // 随机源独立面）。弃「随机起始键定位」：树键共享长前缀时随机 u64
          // 大端首字节几乎恒落键域之外（首字节小于公共前缀 → 命中即树头，
          // 大于全域 → 零命中回绕树头），连抽退化为定值字段（实测 f 前缀
          // 131072 字段树 32 连抽恒中同字段）——该定位形对随机散布是结构
          // 性失效面，非参数可调面；count 形保留回绕扫位不受本锁域影响。
          // 时间口径与臂注一致（§8.3 全树一次扫固有折损，禁「点查零扫描」
          // 再提案）；内存仅驻留一条候选字段；成员级 TTL 按 §53 口径在
          // 存活域上均匀；扫描 Err 直 `?` 上抛（候选未出帧，不违不变式 2）；
          // 存活耗尽 ⟺ nil（对象层 purge 归零同形）
          let mut chosen: Option<Vec<u8>> = None;
          let mut live_seen = 0usize;
          scan_count(tree.scan_with_count_callback(
            SCAN_FROM_HEAD,
            usize::MAX,
            ScanReturnField::KeyAndValue,
            |k, v| {
              if member_expired_at(v, now) {
                return true;
              }
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
        // count 形：帧头预留-回填（存活数不可先验，严禁落 meta.size 于头，
        // 见 RESP_FRAME_HEAD_RESERVED 不变式 1）
        let write_head = |buf: &mut Vec<u8>, n| {
          buf.write_resp_array_len(if with_values && resp_protocol_version == 2 {
            n * 2
          } else {
            n
          });
        };
        let reserved = resp_frame_head_len(n, write_head);
        let base = reserve_resp_frame_head(output, reserved);
        // count 形流式抽样（与 set.rs Srandmember 同判据的共享内核装配：
        // random_scan_round 部分应用承接字段级到期滤与 RESP3 对帧形态）
        let streamed = random_stream_wraparound(
          output,
          &start_key,
          n,
          count_param > 0,
          |start, bound, budget, out| {
            random_scan_round(
              tree,
              start,
              bound,
              budget,
              ScanReturnField::KeyAndValue,
              RandomScanEmit {
                alive: &mut |_, v| !member_expired_at(v, now),
                emit: &mut |k, v, out| {
                  if with_values && is_resp3(resp_protocol_version) {
                    out.write_resp_array_len(2);
                  }
                  out.write_resp_bulk_string(k);
                  if with_values {
                    out.write_resp_bulk_string(decode_member(v).1);
                  }
                },
              },
              out,
            )
          },
        );
        let total = match streamed {
          Ok(t) => t,
          // 撤帧（连同预留头回到臂进入点，应答未落帧，错误帧由上游漏斗闭环，
          // 不变式 2）后上抛，同 Hgetall 臂 Err 出口形
          Err(()) => {
            output.truncate(base);
            return Err(());
          }
        };
        backfill_resp_frame_head(output, base, reserved, total, write_head);
        Ok(true)
      }

      // 未支持操作一律穿透（Ok(false)）：由 run_async_rmw 物化降级通道接手，
      // 杜绝静默兜底输出与命令语义无关的应答——HCOLLECT 族经此落 wcol
      // 对象层单源求值，WATCH 推进同臂由 apply_rmw_post_operate 承接。
      // HDEL 与成员级 TTL 面（HEXPIRE / HTTL / HPERSIST 族，原树内逐成员出账臂
      // 已删）亦在此穿透：删除与到期出账一律走「物化 → 对象层单源求值 →
      // 整值重灌（bulk_load 重建）」，树内零墓碑，见本模块头注
      _ => Ok(false),
    }
  };
  // 稳态写命令镜像收尾单源（判据与不变式见 emit_tiered_mirror_finish）
  emit_tiered_mirror_finish(session, key, ctx, mirror, output, frame_base, result)
}
