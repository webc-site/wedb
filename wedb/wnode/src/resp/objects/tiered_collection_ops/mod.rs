//! 自适应分层集合引擎：基于 wbftree 的分页分层态操作（Hash/Set/ZSet/List）
//!
//! 当集合规模达到升阶阈值（条目数 >= 65,536 或 体积 >= 4MB）时，
//! 自动就地升阶转换为基于 wbftree 的独立页级持久化树，基于 B+ 树页级冷热置换
//! 实现千万级海量存储，彻底消除全量反序列化读放大。
//!
//! 写形不变量「树内零墓碑」：本模块**没有**任何按成员往树里落删除记录的写臂
//! （旧 `tree_del` / `tree_del_batch` 漏斗已随成员级 TTL 面收口删净）。删除重的
//! 写命令（HDEL / SREM / SPOP / ZREM / LPOP / RPOP）与成员级 TTL 面
//! （HEXPIRE / HTTL / HPERSIST / ZEXPIRE / ZTTL / ZPERSIST 族）一律穿透至
//! `rmw_helpers::run_async_rmw` 的物化降级通道 → wcol 对象层单源求值 →
//! `apply_rmw_post_operate` 整值写回（删空即整键消亡，否则
//! `promote_collection_to_bftree` 先建快照后原子换入重灌，零墓碑）。该形与 C#
//! 一致：C# 集合对象整值常驻对象域、删即就地改内存对象、删空即整键消亡
//!（garnet/libs/server/Storage/Functions/ObjectStore/RMWMethods.cs:188-215
//! `PostCopyUpdater` → `value.Operate` → `HasRemoveKey`），既无按成员分记录的
//! 树、也就无墓碑连跑。
//!
//! 本不变量是硬要求而非优化：底层 bf-tree 的 `ScanIter::next` 对墓碑记录是
//! **尾递归自调**（rustc 无 TCO），跳过一条墓碑压一帧（实测 ≈680B/帧），且墓碑
//! 判定排在界键比较之前、`scan_cnt` 只在存活分支递减 ⇒ 游标后的连续墓碑必须在
//! 单次 `next()` 内跳完，COUNT / 上界键 / 早停回调一概截不断，8MiB 默认栈的安全
//! 边 ≈ 连续墓碑 8000 条。故任何一次扫描面调用（分层 HSCAN/SSCAN/ZSCAN 的
//! [`exec_tiered_scan`]、push 双臂用的 [`list_head_seq`]、后台降阶物化轮
//! [`tiered_materialize_blob`]）的栈深度都不由树内墓碑决定——树内根本没有墓碑
//!（机理锚点：wbftree `ScanIter::next` 墓碑尾递归自调与紧上界键不截断遍历的
//! 实测档案，见 `list_head_seq` 文注；裁决 B 收口命令级删除、本票收口成员级
//! TTL 残余源，两层收口后树内删除记录恒零）。
//!
//! 成员级 TTL 的惰性出账面（计数校正臂 HLEN / ZCARD、输出面校正臂 HGETALL /
//! HKEYS / HVALS、显式 HCOLLECT / ZCOLLECT 与周期对象收集任务）不穿透物化，
//! 而走本模块的到期重灌内核 [`expire_sweep_or_rebuild`]：水位命中时锁内单趟
//! 扫描收集存活全集，放守卫后整值重灌（先建快照后原子换入，与裁决 B 同一
//! 换入原语），树内同样零墓碑；水位未命中（`now <= next_expiry`，含水位刻度
//! 当刻）零树访问、直读恒精确。计数复杂度契约（已裁决口径，见
//! doc/zh/collection.md 大键 O(1) 计数规约第 3 条分层态补则）：稳态水位内
//! HLEN/ZCARD O(1) 直读 `MetaValue.size`；水位越过即首个计数命令 O(N) 物理
//! 出账一次（扫 + 有到期才重灌），水位前移后回归 O(1)——到期辅助索引会被
//! 索引侧墓碑与双写放大击穿「树内零墓碑」写形，单批限截断则必须有界删树，
//! 均不采；C# 同面亦非 O(1)（HashObject/SortedSetObject.Count 遍历
//! expirationTimes 字典，O(T) 内存只读）。

mod common;
mod hash;
mod list;
mod scan;
mod set;
mod zset;

pub(crate) use common::{TieredCollectionArgs, TieredCtx, earliest_expiry};
pub(crate) use hash::exec_tiered_hash;
pub(crate) use list::exec_tiered_list;
pub(crate) use scan::{
  exec_tiered_collect, exec_tiered_scan, tiered_materialize_blob, tiered_materialize_blob_sealed,
};
pub(crate) use set::exec_tiered_set;
use wcol::{
  hash::hash_object::HashOperation, list::list_object::ListOperation,
  set::set_object::SetOperation, zset::sorted_set_object::SortedSetOperation,
};
use wdev::Device;
use wkv::BatchStoreSession;
use wval::GarnetObjectType;
pub(crate) use zset::exec_tiered_zset;

/// 四族树内覆盖面共享分派单点（一处装配，杜绝第二形态）：判别类型 →
/// 操作码 `u8` 转族内树操作枚举 → [`TieredCollectionArgs`] 装配 → 转引对应
/// [`exec_tiered_hash`] / [`exec_tiered_set`] / [`exec_tiered_zset`] /
/// [`exec_tiered_list`]。入参 `call` 即参数包装配核，操作码形态以
/// `Op = u8` 表达（族内转换在本核一处完成）。
///
/// 主端生产面（rmw_helpers::run_async_rmw 的分层态原生臂）与重放面
///（aof_processor_object_replay::tiered_replay_arm）同转引本核，四族树内
/// 覆盖面自此一处维护，新增族内命令不再两处补臂（禁两处 match 漂移）。
/// 对标 C# AofProcessor.cs:ObjectStoreRMW<TObjectContext> 经泛型
/// objectContext 单通道应用对象，分派一处。
///
/// 返回 `Ok(None)` = 操作码不可转或判别类型越四族树内覆盖面（不含族内
/// 穿透臂的 `Ok(false)`）——主端按未支持操作穿透物化降级通道，重放端按
/// 发散残留留痕跳过，语义由消费方各自收口；`Err(())` 存储 IO 失败原样上抛
pub(crate) async fn exec_tiered_by_op_code<D: Device>(
  session: &BatchStoreSession<'_, D>,
  key: &[u8],
  tag: GarnetObjectType,
  ctx: &mut TieredCtx<'_>,
  call: TieredCollectionArgs<'_, u8>,
  output: &mut Vec<u8>,
) -> Result<Option<bool>, ()> {
  let TieredCollectionArgs {
    op: op_code,
    args12,
    args,
    resp_protocol_version,
  } = call;
  macro_rules! by_op {
    ($ty:ty, $exec:ident) => {
      match <$ty>::from_repr(op_code) {
        Some(op) => $exec(
          session,
          key,
          ctx,
          TieredCollectionArgs::new(op, args12, args, resp_protocol_version),
          output,
        )
        .await
        .map(Some),
        None => Ok(None),
      }
    };
  }
  match tag {
    GarnetObjectType::Hash => by_op!(HashOperation, exec_tiered_hash),
    GarnetObjectType::Set => by_op!(SetOperation, exec_tiered_set),
    GarnetObjectType::SortedSet => by_op!(SortedSetOperation, exec_tiered_zset),
    GarnetObjectType::List => by_op!(ListOperation, exec_tiered_list),
    _ => Ok(None),
  }
}
