//! 对象族收集与底层存储交互

use wcol::{
  HashOperation, object_payload::GarnetObjectPayload, types::ObjectOutput,
  zset::sorted_set_object::SortedSetOperation,
};
use wdev::Device;
use wkv::Error;
use wresp::cmd_strings::{RESP_ERR_SLOW_PATH_STORAGE, RESP_ERR_WRONG_TYPE};
use wval::GarnetObjectType;

use crate::{
  resp::objects::{
    hash_commands::hash_load_sync,
    object_store_utils::{ObjLoad, obj_length_sync, obj_save_or_gc},
    sorted_set_commands::{zset_load_sync, zset_save_or_gc},
    tiered_collection_ops::exec_tiered_collect,
  },
  storage::session::{
    common::array_key_iteration_functions::ScanTypeFilter, storage_session::StorageSession,
  },
  types::GarnetStatus,
};

/// 收集型 operate 的 RESP 协议版本（HCOLLECT/ZCOLLECT 无 RESP 输出面，C#
/// 特殊通道无应答负载，operate 不消费该参数；网络命令路径传会话
/// resp_protocol_version，收集执行体统一取 RESP2 下限，消除硬编码散点）
const COLLECT_RESP_VERSION: u8 = 2;

/// 单批待处理对象键上限（对标 C# ObjectCollect 的
/// `DbScan(searchKey, true, cursor, out storeCursor, out var hashKeys, 100,
/// typeObject)`——libs/server/Storage/Session/ObjectStore/Common.cs:820）：
/// 任意库规模下单批驻留清单恒定，收集轮内存与库总键数无关
const COLLECT_BATCH_KEYS: usize = 100;

/// HCOLLECT / ZCOLLECT `*` 全库收集公共体（网络命令路径与周期对象收集任务
/// [`crate::primary_tasks`] 共用的唯一收集机制）
///
/// 对标 C# ObjectCollect（libs/server/Storage/Session/ObjectStore/
/// Common.cs:807）的批式流式骨架：`do { DbScan(count = 100) ; foreach 逐键
/// RMW } while (storeCursor != 0)`（:820-827）——每批至多
/// [`COLLECT_BATCH_KEYS`] 键、批内即刻逐键收集过期条目并回收空键、游标续批，
/// 扫描与收集不再串行分相。分批拉取复用 RESP SCAN 同一地址游标内核
/// [`StorageSession::scan_cursor`]（其存活判定单点与 C#
/// `UnifiedStoreGetDBKeys.Reader` 的内部记录 + `CheckExpiry` 过滤链同口径，
/// libs/server/Storage/Session/Common/ArrayKeyIterationFunctions.cs:292），
/// 不另设第二套分页器；类型标签经 [`ScanTypeFilter::Object`] 下传，信封域
/// （值首字节内层标签）与分层态（Meta 元记录 collection_type）两域匹配键
/// 同批收齐，去重由游标内核的链首地址校验承担。
///
/// 两域分流由单键执行体自持，收集清单无须先分态：[`collect_hash_key`] /
/// [`collect_sorted_set_key`] 的 `ObjLoad::Degrade` 臂转树内收集执行体
/// [`exec_tiered_collect`]（到期成员物理出账 + meta 计数回写，同一
/// 到期重灌内核）。互斥单写位由调用方持有与释放（周期任务
/// 沿同款 CAS 单写位语义）。
///
/// 批间不冻结写入：每批尾地址取该批起点时刻的 tail，收集自身写回的新版本记
/// 录可被后续批次再访问一次，由单键零变更门控当场短路（不再落新记录），故
/// 无前台写入时轮次必然收敛；持续前台写入把窗口尾地址推后，与 C# 逐批推进
/// storeCursor 期间追逐 tail 的既有口径一致（互斥单写位已拦同族并发轮次）。
///
/// `Err` = 应答/日志错误文案，`Ok(())` = 收集完成（含 WrongType 计数）。
pub(crate) async fn object_collect_all<D: Device>(
  storage: &StorageSession<'_, D>,
  is_hash: bool,
) -> Result<(), &'static str> {
  let tag = if is_hash {
    GarnetObjectType::Hash
  } else {
    GarnetObjectType::SortedSet
  };
  let mut wrong_type = false;
  let mut cursor = 0u64;
  loop {
    // all_keys = true：C# ObjectCollect 传 allKeys: true，pattern 不参与匹配
    let (next, keys) = storage
      .scan_cursor(
        &[],
        true,
        cursor,
        COLLECT_BATCH_KEYS,
        Some(ScanTypeFilter::Object(tag)),
      )
      .await
      .map_err(|_| RESP_ERR_SLOW_PATH_STORAGE)?;
    for key in &keys {
      let res = if is_hash {
        collect_hash_key(storage, key).await
      } else {
        collect_sorted_set_key(storage, key).await
      };
      match res {
        Ok(GarnetStatus::WrongType) => wrong_type = true,
        Ok(_) => {}
        Err(_) => return Err(RESP_ERR_SLOW_PATH_STORAGE),
      }
    }
    // 游标内核的终态约定：0 = 扫尽（对标 C# storeCursor == 0 出循环）
    if next == 0 {
      break;
    }
    cursor = next;
  }
  if wrong_type {
    Err(RESP_ERR_WRONG_TYPE)
  } else {
    Ok(())
  }
}

/// 收集写回的同步降级异步闭环（环形缓冲翻转 / TTL 磁盘候选：wkv 契约约定
/// 同步快路径返回 Ok(Err) 时调用方降级全异步路径重放）——空对象整键回收，
/// 否则信封整值异步写回（与旧 collect_object_key 的 obj_save().await 同通道）
async fn collect_save_fallback<D: Device>(
  storage: &StorageSession<'_, D>,
  key: &[u8],
  tag: GarnetObjectType,
  empty: bool,
  payload: &[u8],
) -> wkv::Result<()> {
  if empty {
    storage.delete_string(key).await?;
  } else {
    storage.obj_save(key, tag, payload).await?;
  }
  Ok(())
}

/// 收集单个哈希键过期条目并回收空键（对标 C# HashCollect）
///
/// 与网络层 HCOLLECT 显式键（AdminCommands.cs:NetworkHCOLLECT）同一执行体：
/// [`obj_length_sync`] 信封头部存活计数直读 → [`hash_load_sync`] 跨域装载
/// （Meta 分层探测 + 内存信封）→ `HashOperation::Hcollect` → 变更门控回写
/// [`obj_save_or_gc`]。周期对象收集任务与命令路径共用此唯一收集执行，
/// 杜绝第二套收集逻辑。
pub(crate) async fn collect_hash_key<D: Device>(
  storage: &StorageSession<'_, D>,
  key: &[u8],
) -> wkv::Result<GarnetStatus> {
  // 信封头部存活计数直读（上次序列化「写时过滤」后的条目数，O(1) 零反序
  // 列化）；分层提升对象（BfTree 域，含字段级 TTL 水位命中降级）转树内
  // 收集执行体
  let mut scratch = Vec::new();
  let disk_count = match obj_length_sync(&storage.batch, key, GarnetObjectType::Hash, &mut scratch)
  {
    ObjLoad::Present(count) => count,
    ObjLoad::WrongType => return Ok(GarnetStatus::WrongType),
    ObjLoad::Missing => return Ok(GarnetStatus::NotFound),
    ObjLoad::Degrade => {
      return exec_tiered_collect(&storage.batch, key, GarnetObjectType::Hash)
        .await
        .map(|_| GarnetStatus::Ok)
        .map_err(|_| Error::InvalidConfig("tiered collect storage failure".into()));
    }
  };
  scratch.clear();
  let mut obj = match hash_load_sync(&storage.batch, key, &mut scratch) {
    // 分层提升对象（BfTree 域）：转树内收集执行体（到期成员物理出账 +
    // meta 计数回写，与 `*` 全库周期收集同一机制）
    ObjLoad::Degrade => {
      return exec_tiered_collect(&storage.batch, key, GarnetObjectType::Hash)
        .await
        .map(|_| GarnetStatus::Ok)
        .map_err(|_| Error::InvalidConfig("tiered collect storage failure".into()));
    }
    ObjLoad::WrongType => return Ok(GarnetStatus::WrongType),
    ObjLoad::Missing => return Ok(GarnetStatus::NotFound),
    ObjLoad::Present(o) => o,
  };
  obj.operate(
    HashOperation::Hcollect as u8,
    &[] as &[&[u8]],
    0,
    0,
    &mut ObjectOutput::new(),
    COLLECT_RESP_VERSION,
  );
  // 零变更门控——刻意差异（C# 无对应物，不得称等价）：C# HashObject.Operate
  // 尾段 RemoveKey 判定后恒 `return true`（libs/server/Objects/Hash/
  // HashObject.cs:301-304），InPlaceUpdater 成功臂对 appendOnlyFile 非空一律
  // 置 NeedAofLog（libs/server/Storage/Functions/ObjectStore/RMWMethods.cs:
  // 101-102），PostRMWOperation 据此 WriteLogRMW（:256-257）——即 C# 周期收集
  // 每轮把全库 hash 信封全量序列化进 AOF（副本侧同量流量）；rust 按头部存活
  // 计数 == 收集后条目数判零变更即短路跳过写与入账，消除该纯追加放大。
  // 不等 = 装载/收集已剔除过期项而信封陈旧，回写固化（空对象整键回收）；
  // Ok(false) = 环形缓冲翻转 / TTL 磁盘候选（同步臂未落写入）降级异步闭环，
  // Err 存储错误真实传播（命令路径应答失败文案，周期任务侧 warn 留痕）
  if disk_count == obj.len() {
    return Ok(GarnetStatus::Ok);
  }
  let empty = obj.is_empty();
  match obj_save_or_gc(
    &storage.batch,
    key,
    GarnetObjectType::Hash,
    &obj,
    empty,
    |o| o.to_blob(),
  ) {
    Ok(true) => Ok(GarnetStatus::Ok),
    Ok(false) => {
      let payload = if empty { Vec::new() } else { obj.to_blob() };
      collect_save_fallback(storage, key, GarnetObjectType::Hash, empty, &payload).await?;
      Ok(GarnetStatus::Ok)
    }
    Err(e) => Err(e),
  }
}

/// 收集单个有序集合键过期条目并回收空键（对标 C# SortedSetCollect）
///
/// 与网络层 ZCOLLECT 显式键同一执行体（见 [`collect_hash_key`]）
pub(crate) async fn collect_sorted_set_key<D: Device>(
  storage: &StorageSession<'_, D>,
  key: &[u8],
) -> wkv::Result<GarnetStatus> {
  let mut scratch = Vec::new();
  let disk_count = match obj_length_sync(
    &storage.batch,
    key,
    GarnetObjectType::SortedSet,
    &mut scratch,
  ) {
    ObjLoad::Present(count) => count,
    ObjLoad::WrongType => return Ok(GarnetStatus::WrongType),
    ObjLoad::Missing => return Ok(GarnetStatus::NotFound),
    ObjLoad::Degrade => {
      return exec_tiered_collect(&storage.batch, key, GarnetObjectType::SortedSet)
        .await
        .map(|_| GarnetStatus::Ok)
        .map_err(|_| Error::InvalidConfig("tiered collect storage failure".into()));
    }
  };
  scratch.clear();
  let mut obj = match zset_load_sync(&storage.batch, key, &mut scratch) {
    // 分层提升对象：转树内收集执行体（见 [`collect_hash_key`] Degrade 臂）
    ObjLoad::Degrade => {
      return exec_tiered_collect(&storage.batch, key, GarnetObjectType::SortedSet)
        .await
        .map(|_| GarnetStatus::Ok)
        .map_err(|_| Error::InvalidConfig("tiered collect storage failure".into()));
    }
    ObjLoad::WrongType => return Ok(GarnetStatus::WrongType),
    ObjLoad::Missing => return Ok(GarnetStatus::NotFound),
    ObjLoad::Present(o) => o,
  };
  obj.operate(
    SortedSetOperation::Zcollect as u8,
    &[] as &[&[u8]],
    0,
    0,
    &mut ObjectOutput::new(),
    COLLECT_RESP_VERSION,
  );
  // 零变更门控（含 [`collect_hash_key`] 处登记的「C# 无零变更门控」刻意差异，
  // 对应臂 libs/server/Objects/SortedSet/SortedSetObject.cs:452-455 Operate
  // 尾段恒 return true）与降级闭环同 [`collect_hash_key`]
  if disk_count == obj.len() {
    return Ok(GarnetStatus::Ok);
  }
  let empty = obj.is_empty();
  match zset_save_or_gc(&storage.batch, key, &obj) {
    Ok(true) => Ok(GarnetStatus::Ok),
    Ok(false) => {
      let payload = if empty { Vec::new() } else { obj.to_blob() };
      collect_save_fallback(storage, key, GarnetObjectType::SortedSet, empty, &payload).await?;
      Ok(GarnetStatus::Ok)
    }
    Err(e) => Err(e),
  }
}
