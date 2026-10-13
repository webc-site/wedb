//! MSET 慢路径执行臂（exec_slow 冷键分派单臂独立成文件）
//!
//! 对标 libs/server/Resp/ArrayCommands.cs:NetworkMSET 在 Tsavorite pending
//! 写 / 环形页翻转后 CompletePending 重放的异步形态；其余慢路径臂见
//! [`super::slow`]。

use wdev::Device;
use wresp::{
  cmd_strings::{self as cs, RESP_ERR_WRONG_TYPE},
  ext::RespVecExt,
};

use crate::{
  resp::{basic_commands::slow::clear_vector_registry, vector::vector_manager::VectorManager},
  storage::session::storage_session::StorageSession,
};

/// MSET 慢路径执行臂
///
/// 批量折叠（与快路径 [`crate::resp::RespServerSession::network_mset`] 同一
/// 折叠入口 `try_upsert_batch_sync`，快慢路径性能契约一致）：会话前缀单次
/// 外提、借用对排序去重保末值（MSET 重复键后者胜）、单次折叠写。任一键
/// 存储错误时已写键保持、整臂报错；同步折叠降级（环形页翻转 / TTL 清退）
/// 时已写键保持，剩余键逐键 `upsert_string` 异步兜底重放全量（重放值与
/// 已写键同值幂等；SET 语义自动清新键残留 TTL，与快路径同口径）。
/// C# NetworkMSET 逐键 SET 亦无事务回滚，折叠为 transpile SKILL 批量接口
/// 单次折叠工程准则。
///
/// 向量登记清退（票 zcode-r163c-setguard 案一）：`set_vector_guard` 命中
/// Degrade 至此臂后，取窗成功且 RI 门全数通过时逐键复用
/// [`clear_vector_registry`] 单源摘除登记表（对标 C# ArrayCommands.cs:59-63
/// 排他锁内 DELETE+SET 重投——清退与覆写同一临界区），落毕键恒 string 域、
/// 登记零残骸；失败 arity / RI 拒 / 失窗早退于清退之前，零副作用契约不变
pub(crate) async fn mset(
  storage: &StorageSession<'_, impl Device>,
  vector: Option<&VectorManager>,
  refs: &[&[u8]],
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  // 键值对视图（取窗/预检/清退/折叠/兜底重放共用，免逐处重展 as_chunks）
  let chunks = refs.as_chunks::<2>().0;
  // RI 键门折叠前预检（异步对偶 [`StorageSession::ri_write_gate`]，
  // 与快路径同判据）：任一键为存活 RangeIndex 整命令拒 WRONGTYPE，零键落库
  // 全键读改写窗口（快路径 network_mset 同一窗口契约：对标 C#
  // MSET_Conditional 全键排他锁；桶序取闩无循环等待面，闩内完成折叠批写，
  // 杜绝批写跨键交叠他 worker 读算写间隙）。失闩预算耗尽按本臂存储错误应答
  let Ok(_windows) = storage
    .batch
    .rmw_window_sorted(chunks.iter().map(|c| c[0]))
    .await
  else {
    return Err(());
  };
  for chunk in chunks {
    if storage.ri_write_gate(chunk[0]).await.map_err(|_| ())? {
      output.write_resp_error(RESP_ERR_WRONG_TYPE);
      return Ok(());
    }
  }
  // 窗内逐键清退登记表（持窗临界区内、批量落笔之前；判据与清退全走既有
  // 单源壳，未命中零操作幂等），杜绝已答 +OK 的覆写留下双域幽灵残骸
  for chunk in chunks {
    clear_vector_registry(storage, vector, chunk[0]).await;
  }
  let pairs = chunks.iter().map(|c| (c[0], c[1]));
  match storage.batch.try_upsert_batch_sync(pairs) {
    Ok(Ok(())) => {}
    Ok(Err(_)) => {
      for [key, val] in chunks {
        storage.upsert_string(key, val).await.map_err(|_| ())?;
      }
    }
    Err(_) => return Err(()),
  }
  cs::write_raw(output, cs::RESP_OK);
  Ok(())
}
