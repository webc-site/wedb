//! 树清退面：bftree drain 清退内核与 STORE 族目标键分层残留清退

use wdev::Device;
use wkv::Error;

use super::load_stub;
use crate::storage::session::storage_session::StorageSession;

/// bftree drain 清退的 `Error::Swapped` 折算单点：Swapped 残留先显式推进
/// WATCH 恰一次再报忙，其余错误直接报忙（fail-closed 交既有拒写/重试通道）。
/// 仅供需要 Swapped 折算的两臂使用；obj_save 已入账的懒降阶臂维持裸
/// `map_err` 不重复推进（一命令一推进，见 [`apply_rmw_post_operate`] 头注）
#[inline]
pub(super) async fn bftree_drain<D: Device>(
  storage: &StorageSession<'_, D>,
  key: &[u8],
  keep_ttl: bool,
) -> Result<(), ()> {
  if let Err(e) = storage
    .batch
    .handle_bftree_drain_and_delete(key, keep_ttl)
    .await
  {
    if matches!(e, Error::Swapped(_)) {
      storage.bump_watch_version(key);
    }
    return Err(());
  }
  Ok(())
}

/// STORE 族目标键清退收尾：目标键若原为分层态，按接管形态分流清退残留树
///（SINTERSTORE / ZINTERSTORE 族统一漏斗）
///
/// 分流口径（票 zcode-r15-zset 发现一，对齐 [`apply_rmw_post_operate`] 两分支
/// 既有口径，一处定义）：非空结果信封写回已接管数据面、键换域存活 →
/// `keep_ttl=true` 只墓碑元记录 + 注销树，绝不触碰刚写回的信封（删键臂
/// `keep_ttl=false` 会先对信封域写幂等墓碑，把接管态数据一并抹掉）；删空
/// 回收整键消亡 → `keep_ttl=false` 两域齐清 + 随键 TTL/ETag 旁路级联清退，
/// 杜绝孤儿
pub(crate) async fn retire_tiered_dest<D: Device>(
  storage: &StorageSession<'_, D>,
  dst: &[u8],
  keep_ttl: bool,
) -> Result<(), ()> {
  if load_stub(&storage.batch, dst).await?.is_some() {
    bftree_drain(storage, dst, keep_ttl).await
  } else {
    Ok(())
  }
}
