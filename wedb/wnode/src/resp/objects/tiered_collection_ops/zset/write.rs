//! 分层 zset 树内写臂：ZADD / ZINCRBY（tiered_zset_arm 的写命令臂提取，
//! 语义与骨架分派见 [`super::tiered_zset_arm`]）

use wbase::{num::strict_f64, time::now_ticks};
use wbftree::BfTreeService;
use wcol::zset::sorted_set_object_impl::sorted_set_add_get_options;
use wdev::Device;
use wkv::BatchStoreSession;
use wresp::{cmd_strings as cs, ext::RespVecExt};

use super::{
  super::common::{
    TieredCtx, member_expiry_probe, save_tiered_meta, score_of_payload, tiered_precheck,
    tree_member_state, tree_put_ok, tree_put_rejected,
  },
  read_alive_score,
};

/// ZADD 树内写臂（选项段/数据段解析、预校验、主循环与收尾记账，原
/// `tiered_zset_arm` 的 `SortedSetOperation::Zadd` 臂逐字迁出）
pub(super) async fn zset_zadd_arm<D: Device>(
  session: &BatchStoreSession<'_, D>,
  key: &[u8],
  ctx: &mut TieredCtx<'_>,
  tree: &BfTreeService,
  args: &[&[u8]],
  resp_protocol_version: u8,
  output: &mut Vec<u8>,
) -> Result<bool, ()> {
  use wresp::options::SortedSetAddOption;

  // ---- 选项段与数据段解析（逐 token 惰性判定，与内存态
  // sorted_set_add / C# SortedSetAdd 主循环同构，选项段判定复用 wcol
  // 单源 sorted_set_add_get_options，杜绝第二套翻译）：首 token 即合法
  // 分值时选项段后置，中段选项词形（如 ZADD k 1 m NX）在选项段已解析后
  // 回 NOT_VALID_FLOAT——与内存态、C# 三方错误帧逐字节一致，不再随键
  // 形态（内存态/分层态）漂移
  //
  // 尾段检查经 vendored C# 行实核对为 1:1 同在（agy r6-data 条 4 复核：
  // SortedSetObjectImpl.cs:80-87）——前缀选项后剩余段为空/奇数在 C# 同回
  // syntax error，该校验单源位于 sorted_set_add_get_options 内
  let mut options = SortedSetAddOption::NONE;
  let mut curr = 0usize;
  let mut data_start = 0usize;
  let mut parsed_options = false;
  while curr < args.len() {
    if strict_f64(args[curr], true).is_some() {
      parsed_options = true;
      curr += 1;
      // member 缺失（奇数尾巴）防御性截断：内存态 sorted_set_add 同款，
      // 收尾按已消费对出账（C# 此形 GetArgSliceByRef 越界读 UB——
      // deviations §151，禁按 C# 形改写为越界读）
      if curr == args.len() {
        break;
      }
      curr += 1;
    } else if parsed_options {
      // 中段选项词形：C# SortedSetAdd Invalid Score → NOT_VALID_FLOAT
      cs::write_error_raw(output, cs::RESP_ERR_NOT_VALID_FLOAT);
      return Ok(true);
    } else {
      parsed_options = true;
      match sorted_set_add_get_options(args, &mut curr) {
        Ok(opts) => {
          options = opts;
          data_start = curr;
        }
        Err(frame) => {
          cs::write_error_raw(output, frame);
          return Ok(true);
        }
      }
    }
  }

  // 预校验先于任何写入（RI 批量口径：任一成员越契约即整体失败、零树内副作用；
  // (score, member) 成对排布，成员即 pair[1]，分值恒 8B 裸记录落树，成员长度
  // 决定契约；数据段内非分值词形已由上方解析扫描以 NOT_VALID_FLOAT 拦下，
  // 主循环 else 臂仅余防御口径）
  for pair in args[data_start..].as_chunks::<2>().0 {
    if !tiered_precheck(ctx, pair[1], size_of::<f64>(), None, output) {
      return Ok(true);
    }
  }

  // ---- SortedSetAdd 主循环（C# SortedSetObjectImpl 的 SortedSetAdd 臂：
  // 新增恒计数 / CH 计变更 / NX 过滤已存在 / GT/LT 分值比较 /
  // INCR 增量回 bulk string）；「插成功才计数」判据统一走 tree_put_ok
  let mut added_or_changed = 0_i64;
  let mut incr_result = 0_f64;
  let mut new_members = 0u64;
  let mut put_rejected = false;
  let now = now_ticks();
  // ZADD 收尾记账单点：新成员计数入账与元记录回写一处判定，提前出口与正常
  // 出口共用——保证任一出口处「计数与树内实存一一对应」（解析/NaN 等命令
  // 中途错误提前回包时，已落树的成员计数不得随早退丢失，否则 size 虚减
  // 反噬删空自愈判据）
  macro_rules! commit_new_members {
    () => {
      if new_members > 0 {
        ctx.meta.size += new_members;
        // 落盘失败：树内写已生效，break 落臂尾补镜像再上抛（不变式见
        // save_tiered_meta），早退臂已落树的成员计数不随失败丢失
        if save_tiered_meta(session, key, ctx).await.is_err() {
          return Err(());
        }
      }
    };
  }
  curr = data_start;
  while curr < args.len() {
    // 注：本臂 NOT_VALID_FLOAT 出帧恒不可达——数据段内非分值词形已由
    // 预扫（:132-159）在任何树写之前整体拦下（:142-145 早退），此臂仅余
    // 防御口径（:161-164 预校验注自陈同口）；勿按 C# 部分提交语义改写
    // （deviations §142 禁回改）
    let Some(score) = strict_f64(args[curr], true) else {
      commit_new_members!();
      cs::write_error_raw(output, cs::RESP_ERR_NOT_VALID_FLOAT);
      return Ok(true);
    };
    curr += 1;
    // member（前缀选项场景偶数校验已保证成对；首 token 即分值场景奇数
    // 尾巴防御性截断，内存态 sorted_set_add 同款——C# 此形主循环
    // GetArgSliceByRef 越界读 UB，deviations §151，禁按 C# 形改写）
    let Some(&member) = args.get(curr) else {
      break;
    };
    curr += 1;

    // 到期成员视同不存在（C# SortedSetAdd 入口先 DeleteExpiredItems）；
    // 物理覆盖对到期旧记录等价先删后加（size 不变），对存活旧记录任一
    // 写入分支都清字段 TTL（C# TryRemoveExpiration，含同分值分支）
    let state = tree_member_state(tree, member, now);
    let alive_score = if matches!(state, Some((_, false))) {
      read_alive_score(tree, member, now)
    } else {
      None
    };

    match alive_score {
      None => {
        // 真新成员 / 已到期旧记录：XX 置位则不新增（到期视同不存在）；INCR
        // 形态成员缺席直出 null 终止（对标 #2197，C# SortedSetAdd WriteNull
        // 分支——继续遍历会使 INCR 尾帧误报未消费的 incrResult 初值 0）
        if options.contains(SortedSetAddOption::XX) {
          if options.contains(SortedSetAddOption::INCR) {
            commit_new_members!();
            output.write_resp_null_ver(resp_protocol_version);
            return Ok(true);
          }
          continue;
        }
        incr_result = score;
        if tree_put_ok(ctx, tree, member, &score.to_be_bytes(), None) {
          if state.is_none() {
            new_members += 1;
          }
          added_or_changed += 1;
        } else {
          put_rejected = true;
        }
      }
      Some(old_score) => {
        let mut score = score;
        if options.contains(SortedSetAddOption::INCR) {
          score += old_score;
          incr_result = score;
          if score.is_nan() {
            commit_new_members!();
            cs::write_error_raw(output, cs::RESP_ERR_GENERIC_SCORE_NAN);
            return Ok(true);
          }
        }
        if score == old_score {
          // 同分值（IEEE 754 下 -0.0 == +0.0 判真）：必须保留树内存值位
          // 模式，严禁回写本次输入分值——C# 同分支 `_ = TryRemoveExpiration
          // (member); continue;`（libs/server/Objects/SortedSet/
          // SortedSetObjectImpl.cs:163）只清成员级 TTL、不动存储分值，
          // 内存态 sorted_set_object_impl.rs:sorted_set_add 同分支仅
          // remove_expiration。幂等零动作同形：旧记录挂 TTL 时以
          // (old_score, None) 重写树值清 TTL（树内容实际变更 → 置脏），
          // 无挂 TTL 时跳过树写（零写零脏零镜像，remove_expiration 对
          // 无过期成员即无操作）；任一形态均零计数变更
          let has_ttl = state.is_some_and(|(expiry, _)| expiry.is_some());
          if has_ttl && !tree_put_ok(ctx, tree, member, &old_score.to_be_bytes(), None) {
            put_rejected = true;
          }
          continue;
        }
        // NX 置位，或 GT/LT 置位且现存分值高于/低于新分值 → 不更新
        if options.contains(SortedSetAddOption::NX)
          || (options.contains(SortedSetAddOption::GT) && old_score > score)
          || (options.contains(SortedSetAddOption::LT) && old_score < score)
        {
          if options.contains(SortedSetAddOption::INCR) {
            commit_new_members!();
            output.write_resp_null_ver(resp_protocol_version);
            return Ok(true);
          }
          continue;
        }
        if tree_put_ok(ctx, tree, member, &score.to_be_bytes(), None) {
          if options.contains(SortedSetAddOption::CH) {
            added_or_changed += 1;
          }
        } else {
          put_rejected = true;
        }
      }
    }
  }

  commit_new_members!();
  if put_rejected {
    tree_put_rejected(output);
    return Ok(true);
  }
  if options.contains(SortedSetAddOption::INCR) {
    // 分值数值（对标 C# SortedSetObjectImpl.SortedSetAdd INCR 分支
    // WriteDoubleNumeric，RESP3 `,val`、RESP2 bulk）
    cs::write_double_numeric(output, incr_result, resp_protocol_version);
  } else {
    output.write_resp_int(added_or_changed);
  }
  Ok(true)
}

/// ZINCRBY 树内写臂（增量解析、三态判据点读与「插成功才计数」收尾，原
/// `tiered_zset_arm` 的 `SortedSetOperation::Zincrby` 臂逐字迁出）
pub(super) async fn zset_zincrby_arm<D: Device>(
  session: &BatchStoreSession<'_, D>,
  key: &[u8],
  ctx: &mut TieredCtx<'_>,
  tree: &BfTreeService,
  args: &[&[u8]],
  resp_protocol_version: u8,
  output: &mut Vec<u8>,
) -> Result<bool, ()> {
  if args.len() < 2 {
    return Err(());
  }
  // 入参增量解析失败（进树前前置校验）：C# SortedSetIncrement 的
  // parseState.TryGetDouble（canBeInfinite 默认 true，±inf 词形合法放行）
  // 失败 → RESP_ERR_NOT_VALID_FLOAT（libs/server/Objects/SortedSet/
  // SortedSetObjectImpl.cs:331-334，信封层 sorted_set_increment 同面），
  // 严禁折叠成慢路径存储错误与信封臂分叉
  let Some(incr) = strict_f64(args[0], true) else {
    cs::write_error_raw(output, cs::RESP_ERR_NOT_VALID_FLOAT);
    return Ok(true);
  };
  let member = args[1];
  let now = now_ticks();
  let mut cur_score = 0.0f64;
  let mut is_new = true;
  // 存活成员增量保留既有 TTL（C# SortedSetIncrby 不动 expiration）；到期
  // 旧记录视同不存在（C# 增量入口先 DeleteExpiredItems）
  let mut old_expiry = None;
  // 到期命中独立标志（三态判据：真缺席 / 到期在树 / 存活，与 ZADD None
  // 分支收敛同一判据面，见 Zincrby 尾部记账文注）
  let mut expired_hit = false;
  tree.read_callback(member, |res, raw| {
    member_expiry_probe(
      res,
      raw,
      now,
      &mut expired_hit,
      &mut is_new,
      &mut old_expiry,
      |payload| {
        if let Some(score) = score_of_payload(payload) {
          cur_score = score;
        }
      },
    )
  });
  // 新增/到期（expired_hit 恒维持 is_new 真，三态判据单标志）视同缺席臂
  // 直存增量原值（对位 C# SortedSetObjectImpl 的 SortedSetIncrement 缺席
  // 臂 `sortedSetDict.Add(member, incrValue)`、信封 sorted_set_increment
  // None 臂直插 incr_value、本文件 ZADD 臂 None 分支 `incr_result = score`
  // 直存正解形，三处同谱同一判据），严禁并入 0.0 基底折叠——IEEE 754
  // 就近舍入 0.0 + (-0.0) = +0.0 抹洗 ±0.0 符号，致 -0 词形建员双态应答
  // 帧字节与存储位模式分叉（本文件 ZADD 位模式保留注同形危害自证）；
  // 仅存活成员折叠增量。NaN 闸保持现位统一判：incr 已过 strict_f64 严格
  // 文法恒非 NaN，新增臂恒不触发，零成本零副作用，勿另立第二谓词
  let new_score = if is_new { incr } else { cur_score + incr };
  if new_score.is_nan() {
    cs::write_error_raw(output, cs::RESP_ERR_GENERIC_SCORE_NAN);
    return Ok(true);
  }
  let score_record = new_score.to_be_bytes();
  // 预校验先于写入（RI 单点口径，零副作用）；编码记录长度随 TTL 头形态换算
  if !tiered_precheck(ctx, member, score_record.len(), old_expiry, output) {
    return Ok(true);
  }
  // 「插成功才计数」：写成才计新成员并入账回分值（C#
  // SortedSetObjectImpl.SortedSetIncrement 的等价判据）
  if !tree_put_ok(ctx, tree, member, &score_record, old_expiry) {
    tree_put_rejected(output);
    return Ok(true);
  }
  // 真缺席才计数（is_new 初值真；到期命中已由物理覆盖承接零计数——成员
  // 已在 size 中，+1 即永久虚增且覆盖后 sweep 不再数入出账。C# 净零由
  // 入口 DeleteExpiredItems 先摘后加承担，三态判据与 ZADD None 分支同面）
  if is_new && !expired_hit {
    ctx.meta.size += 1;
    // 落盘失败：树内写已生效，break 落臂尾补镜像再上抛（不变式见 save_tiered_meta）
    if save_tiered_meta(session, key, ctx).await.is_err() {
      return Err(());
    }
  }
  // 分值数值（对标 C# SortedSetObjectImpl.SortedSetIncrement WriteDoubleNumeric）
  cs::write_double_numeric(output, new_score, resp_protocol_version);
  Ok(true)
}
