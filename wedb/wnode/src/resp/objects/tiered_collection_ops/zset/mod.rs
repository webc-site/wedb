//! 分层态有序集合命令：树内主体骨架与读臂分派（写臂见 [`write`]、计数/范围/
//! 排名臂见 [`range`]、扫描内核见 [`scan`]）

mod range;
mod scan;
mod write;

use wbase::time::now_ticks;
use wbftree::{BfTreeReadResult, BfTreeService};
use wcol::{
  types::member_ttl::{decode_member, member_expired_at},
  zset::sorted_set_object::SortedSetOperation,
};
use wdev::Device;
use wkv::BatchStoreSession;
use wresp::{cmd_strings as cs, ext::RespVecExt};
use wval::GarnetObjectType;

use super::common::{
  TieredCollectionArgs, TieredCtx, TieredOpError, emit_tiered_mirror_finish, finish_tiered_arm,
  score_of_payload, tiered_count, tiered_guard, tiered_write_mirror,
};

/// 有序集合族写面判定（一处定义）：树内稳态写命令取独占写锁；纯读
///（Zscore/Zmscore）、计数臂 Zcard（稳态共享读锁 O(1) 直读，水位越过经
/// [`tiered_count`] 升级写锁出账）、ZREM、ZEXPIRE / ZTTL / ZPERSIST 与其余
/// 穿透臂维持共享读锁（经物化降级整值重灌）
pub(crate) fn zset_needs_write(op: SortedSetOperation) -> bool {
  matches!(op, SortedSetOperation::Zadd | SortedSetOperation::Zincrby)
}

/// 执行分层态有序集合命令（WATCH 栅栏由 [`finish_tiered_arm`] 统一收尾）
///
/// [`doc(hidden)`] 测试专用隐藏面：byLex 块解析失败撤帧语义集成测
///（wnode/tests/tiered_zset_bylex_truncate_pipeline.rs——单命令 / 流水线 /
/// BYSCORE 并置三场景直驱断言），生产入口为 [`super::exec_tiered_by_op_code`]
/// 分派单点，非公共 API 契约
///
/// 错误面与 [`super::exec_tiered_list`] / [`super::exec_tiered_set`] 同形：
/// 内臂 `Err(())` 在边界折叠为 [`TieredOpError`] 单元错误（对齐既有 pub 口
/// 的 `result_unit_err` 门）；撤帧判定仍以内臂 `Err` 原形进行
#[doc(hidden)]
pub async fn exec_tiered_zset<D: Device>(
  session: &BatchStoreSession<'_, D>,
  key: &[u8],
  ctx: &mut TieredCtx<'_>,
  call: TieredCollectionArgs<'_, SortedSetOperation>,
  output: &mut Vec<u8>,
) -> Result<bool, TieredOpError> {
  // 本命令负载起点：ZRANGE 族 byLex 块解析失败须撤帧回到此处重写错误应答
  //（对标 C# writer.ResetPosition），Err 出口同样回退，杜绝半成品帧残留
  let frame_base = output.len();
  let handled = tiered_zset_arm(session, key, ctx, call, output, frame_base).await;
  let res = finish_tiered_arm(session, key, ctx, handled);
  if res.is_err() {
    output.truncate(frame_base);
  }
  res.map_err(|_| TieredOpError)
}

/// 存活成员分值点读（ZSCORE / ZMSCORE 臂与 ZADD 存在性分支共用）：缺席与已到期
/// 同归 `None`（到期视同不存在，对位 C# 入口 `DeleteExpiredItems`）
fn read_alive_score(tree: &BfTreeService, member: &[u8], now: i64) -> Option<f64> {
  let mut score = None;
  tree.read_callback(member, |res, raw| {
    if res == BfTreeReadResult::Found && !member_expired_at(raw, now) {
      score = score_of_payload(decode_member(raw).1);
    }
    res == BfTreeReadResult::Found
  });
  score
}

/// 分层态有序集合命令树内主体（读写臂分派，见 [`exec_tiered_zset`]）：
/// 写臂（ZADD / ZINCRBY）提取至 [`write`]，计数/范围/排名臂（ZCOUNT /
/// ZLEXCOUNT / ZRANGE 族 / ZRANK 族）提取至 [`range`]，点读与计数小臂
///（Zscore / Zmscore / Zcard）留驻本骨架
async fn tiered_zset_arm<D: Device>(
  session: &BatchStoreSession<'_, D>,
  key: &[u8],
  ctx: &mut TieredCtx<'_>,
  call: TieredCollectionArgs<'_, SortedSetOperation>,
  output: &mut Vec<u8>,
  frame_base: usize,
) -> Result<bool, ()> {
  let TieredCollectionArgs {
    op,
    // arg1 为 ZRANK / ZREVRANK 的 WITHSCORE 位、arg2 为 ZRANGE 族选项位（见下方
    // 范围与排名臂）；成员级 TTL 面（ZEXPIRE 族）已穿透物化降级，其压缩字仍由
    // run_async_rmw 的 run_op 闭包捕获透传对象层，本臂不再消费
    args12,
    args,
    resp_protocol_version,
  } = call;
  let mirror = tiered_write_mirror(
    GarnetObjectType::SortedSet,
    op,
    args12,
    args,
    // ZADD / ZINCRBY 两写臂即全部稳态写面，镜像面与锁面同域（Zcard 水位
    // 越过出账的树变更已由 promote 重灌流整树镜像，命令本身无写语义）
    zset_needs_write,
  );
  // 写臂独占互斥（锁内刷新元记录），读臂共享锁（判定一处定义）
  let Some(tree_guard) = tiered_guard(session, key, ctx, zset_needs_write(op)).await? else {
    return Ok(false);
  };
  let tree = tree_guard.tree();

  let result = 'arm: {
    match op {
      SortedSetOperation::Zadd => {
        write::zset_zadd_arm(session, key, ctx, tree, args, resp_protocol_version, output).await
      }

      SortedSetOperation::Zscore => {
        if args.is_empty() {
          break 'arm Err(());
        }
        let score_opt = read_alive_score(tree, args[0], now_ticks());
        if let Some(score) = score_opt {
          cs::write_double_numeric(output, score, resp_protocol_version);
        } else {
          output.write_resp_null_ver(resp_protocol_version);
        }
        Ok(true)
      }

      SortedSetOperation::Zmscore => {
        let now = now_ticks();
        output.write_resp_array_len(args.len());
        for &member in args {
          match read_alive_score(tree, member, now) {
            Some(score) => cs::write_double_numeric(output, score, resp_protocol_version),
            None => output.write_resp_null_ver(resp_protocol_version),
          }
        }
        Ok(true)
      }

      SortedSetOperation::Zcard => {
        // 计数（同分层 Hlen 臂，共 [`tiered_count`] 内核）：稳态水位内共享读锁
        // 锁内重读元记录 O(1) 直读 size；水位越过升级写锁物理出账（树内零墓碑，
        // 有到期才重灌），O(N) 每到期纪元至多一次
        let size = tiered_count(session, key, ctx, tree_guard).await?;
        output.write_resp_int(size as i64);
        Ok(true)
      }

      SortedSetOperation::Zincrby => {
        write::zset_zincrby_arm(session, key, ctx, tree, args, resp_protocol_version, output).await
      }

      SortedSetOperation::Zcount => range::zset_zcount_arm(tree, output, args),

      SortedSetOperation::Zlexcount => range::zset_zlexcount_arm(tree, output, args),

      SortedSetOperation::Zrange => range::zset_zrange_arm(
        tree,
        output,
        args,
        args12,
        resp_protocol_version,
        frame_base,
      ),

      SortedSetOperation::Zrank | SortedSetOperation::Zrevrank => {
        range::zset_zrank_arm(tree, output, args, args12, resp_protocol_version, op)
      }

      // 未支持操作一律穿透（Ok(false)）：由 run_async_rmw 物化降级通道接手，
      // 杜绝静默兜底输出与命令语义无关的应答——ZPOPMIN / ZPOPMAX / ZREMRANGEBY*
      // / GEOADD / ZRANGESTORE 等写族经此落 wcol 对象层单源真实删改，WATCH 推进
      // 同臂由 apply_rmw_post_operate 承接（旧兜底臂把它们应答成整表 ZRANGE 形态
      // 且零树删除，客户端见成功而数据未动，是比漏栅栏更重的语义缺陷）。
      // ZREM 与成员级 TTL 面（ZEXPIRE / ZTTL / ZPERSIST 族，原树内逐成员出账臂
      // 已删）同在此穿透：删除与到期出账一律走整值重灌，树内零墓碑（见模块头注）。
      // ZRANDMEMBER / ZDIFF / ZUNION / ZINTER / ZRANGESTORE 等多键与随机采样面
      // 亦维持物化通道（非本票射程，代价与限流口径见 doc/zh/collection.md）。
      _ => Ok(false),
    }
  };
  // 稳态写命令镜像收尾单源（判据与不变式见 emit_tiered_mirror_finish）
  emit_tiered_mirror_finish(session, key, ctx, mirror, output, frame_base, result)
}
