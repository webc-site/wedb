//! VDB 换号清扫域：死亡账本墓碑注销（`pop_reclaimable` 截断线越界前缀）与
//! 空闲租户路由快照析构。由换号物理回收内核 [`GcManager::reclaim_physical`]
//! 驱动（见 [`crate::gc`] 门面件总述）。

use std::sync::Arc;

use log::{info, warn};
use wbase::time::{now_ms, now_ticks};
use wdev::Device;

use super::{GcManager, reclaim::RELEASE_BATCH};
use crate::{error::Result, store::WedbStore, vdb::DbMetaRecord};

impl<D: Device> GcManager<D> {
  pub(super) async fn sweep_vdb(&self, store: &Arc<WedbStore<D>>) -> Result<()> {
    let now_ticks = now_ticks();
    let begin_addr = store.begin_address();
    // 死亡账本按到期时间小根堆只弹「已到期且日志截断线已越界」的前缀，
    // 单轮至多弹出 RELEASE_BATCH 条（与 gc/reclaim.rs 的 RELEASE_BATCH 孪生臂同源立法，
    // 换号风暴下单轮注销有界推进，杜绝串行磁盘墓碑删除长期霸占常驻回收任务）。
    // 扫描成本与到期前缀成正比，与账本总量（历史租户数）无关
    let to_remove = store
      .vdb
      .gc_dead
      .pop_reclaimable(now_ticks, begin_addr, RELEASE_BATCH);
    for (vid, item) in to_remove {
      // 墓碑键载荷与值经 DbMetaRecord 单点编解码（退役角色由 vns 有无甄别：
      // Some = 库级 GcDeadDb，None = 命名空间级 GcDeadNs），杜绝双分支共享
      // 裸缓冲的错位隐患
      let rec = match item.vns {
        Some(vns) => DbMetaRecord::GcDeadDb {
          expired_at: item.expired_at,
          vns,
          old_vdb: vid,
          tail_address: item.tail_address,
        },
        None => DbMetaRecord::GcDeadNs {
          expired_at: item.expired_at,
          old_vns: vid,
          tail_address: item.tail_address,
        },
      };
      // 到期注销与 DbMeta 墓碑注销镜像回放共用 apply_dbmeta_tombstone 单点
      //（账本条目已离册，内部 remove 幂等空；命名空间级连带释放废弃租户路由
      // 快照表）。注销落盘失败 warn 留痕：盘上墓碑未注销则重启重建后下轮重扫
      // 重投（pop_reclaimable 双检幂等），绝不静默吞硬错
      if let Err(err) = store.apply_dbmeta_tombstone(rec.key().as_slice()).await {
        warn!("内置 GC 退役墓碑注销失败: vid={vid} err={err}");
      }
    }
    // 空闲析构：引用归零且空闲期限已满的租户路由快照摘除释放（连接断开 +
    // 长期空闲双触发；映射权威在磁盘 DbMeta，析构后访问点查装载回建）
    let evicted = store
      .vdb
      .pop_idle_candidates(now_ms())
      .into_iter()
      .filter(|&vns| store.vdb.evict_idle_route(vns))
      .count();
    if evicted > 0 {
      info!("内置 GC 空闲析构租户路由快照: 数量={evicted}（内存归零，磁盘权威可重载）");
    }
    Ok(())
  }
}
