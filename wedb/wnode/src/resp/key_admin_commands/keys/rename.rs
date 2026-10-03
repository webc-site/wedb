//! RENAME / RENAMENX 双键跨命名空间搬移事务（对标 libs/server/Resp/KeyAdminCommands.cs
//! 的 NetworkRENAME / NetworkRENAMENX 与 libs/server/Storage/Session/UnifiedStore/
//! UnifiedStoreOps.cs 的 RENAME）

use wdev::Device;
use wkv::StoreResult;
use wresp::{
  check_args::unpack_args,
  cmd_strings as cs,
  cmd_strings::{RESP_ERR_GENERIC, abort_with_error_message, write_raw},
  ext::RespVecExt,
};
use wval::{KeyTag, NO_ETAG};

use super::super::super::resp_server_session::RespServerSession;
use crate::{
  resp::vector::vector_manager::VectorManager,
  storage::session::common::{
    TagRead,
    etag_sync::{del_etag_sync, etag_of_sync_with_prefix, put_etag_sync},
    read_tag_sync_with_prefix,
    ttl_sync::{
      del_ttl_sync, meta_collection_type_of, probe_alive_with_registry, put_ttl_sync,
      registry_alive, ttl_of_sync_with_prefix,
    },
  },
};

/// RENAME/RENAMENX 成应答帧（同键早退与尾部收尾两处共用）
#[inline]
pub(crate) fn reply_renamed(nx: bool, output: &mut Vec<u8>) {
  if nx {
    output.write_resp_int(1);
  } else {
    write_raw(output, cs::RESP_OK);
  }
}

impl RespServerSession {
  /// libs/server/Resp/KeyAdminCommands.cs:NetworkRENAME
  pub fn network_rename<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    vector: Option<&VectorManager>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some([old_key, new_key]) = unpack_args(parse_state, output, "RENAME") else {
      return Ok(true);
    };
    rename_sync(store, old_key, new_key, false, vector, output)
  }

  /// libs/server/Resp/KeyAdminCommands.cs:NetworkRENAMENX
  pub fn network_renamenx<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    vector: Option<&VectorManager>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some([old_key, new_key]) = unpack_args(parse_state, output, "RENAMENX") else {
      return Ok(true);
    };
    rename_sync(store, old_key, new_key, true, vector, output)
  }
}

/// RENAME/RENAMENX 共同内核（对标 libs/server/Storage/Session/UnifiedStore/
/// UnifiedStoreOps.cs 的 RENAME，C# 以 isNX 单实现双命令）
///
/// RENAMENX 命令级锚点（isNX=true 分支：新键存活 → result=0 不动旧键）：
/// libs/server/Storage/Session/UnifiedStore/UnifiedStoreOps.cs:RENAMENX
///
/// 序次对齐 C#：同键早退（首个检查，先于一切读取与 NX 判定）→ [NX] 新键存活判定（唯一 NX 判据）→ 双探旧键定
/// 物理域（String 命中 → 字符串；未命中探信封域，命中 → 对象键连同标签整体
/// 迁移；皆缺 → 向量登记表承接，未登记 →
/// NOSUCHKEY）→ 旧键 TTL 记录 → 对象域
/// 新键旁域预清 → 写新键（isNX/非 isNX 共用 SET（upsert）单写入口，对标 C#
/// :363 单条 `SET(newKey, logRecord)`）→ TTL 随键迁移
///（C# TryCopyFrom 连同 Expiration 拷入新记录）→ 清旧键 TTL → 删旧键。
///
/// 新键清退（C# needDeleteNewKey 与 isNX 无涉，UnifiedStoreOps.cs:338-347）：
/// 向量集项登记表命中先降级慢路径清退（快路径不做真异步登记摘除）；对象键
/// 迁移无条件经 [`wkv::BatchStoreSession::try_delete_sync`] 级联清退新键
/// String 残留与随键 TTL/ETag 旁域——与慢路径 `delete_string(new_key)`
/// （slow.rs 无条件臂）同一删除单点，杜绝探针判死的过期残留经信封写入后
/// 双域并存或新键随残 TTL 隐死。
///
/// 任一步遇异步闭环（磁盘候选/环形页翻转）即整体降级 `Ok(false)`：调用方
/// 重试整条命令，旧键未删时幂等重放。`Ok(true)` 已闭环（应答已写入 output）
fn rename_sync<'a, D: Device>(
  store: &wkv::BatchStoreSession<'a, D>,
  old_key: &[u8],
  new_key: &[u8],
  nx: bool,
  vector: Option<&VectorManager>,
  output: &mut Vec<u8>,
) -> wresp::Result<bool> {
  // C# 同键早退：RENAME → OK；RENAMENX → 1（result=1，先于 NX 存在性判定）
  if old_key == new_key {
    reply_renamed(nx, output);
    return Ok(true);
  }

  // 会话前缀单次外提（循环前缀外提对位：本函数三域探针、TTL/ETag 记录读、
  // registry 判定、NX 存活探测与向量集迁移统一复用该缓冲，消除逐探点重读
  // ns/db 原子变量与重算 Varint；对偶慢路径 rename_slow 同一外提口径）
  let prefix = store.session_prefix();

  // 双键读改写窗口（票 zcode-r15-generic 发现一，对标 C# UnifiedStoreOps.RENAME
  // 的 SaveKeyEntryToLock(oldKey, Exclusive) + SaveKeyEntryToLock(newKey,
  // Exclusive) 双键排他事务锁：UnifiedStoreOps.cs:241 起「探 new → GET old →
  // [NX] 判定 → DELETE(new) → SET(new) → DELETE(old)」全程锁内一体，并发
  // SET/DEL 与 RENAME 严格互斥）：桶序取闩（哈希升序定序防 RENAME a b /
  // RENAME b a 交叉死锁），闩内完成本函数全序列，杜绝三探旧键 → 写新删旧
  // 间隙的并发 SET new 覆写丢失与并发 DEL old 后键复活。失闩沿既有 Ok(false)
  // 降级慢路径同段持窗重放，绝不自旋等闩
  let Some(_windows) = store.try_rmw_window_sorted([old_key, new_key]) else {
    return Ok(false);
  };

  // RENAMENX：新键存活（含过期裁决，双域 + 向量登记表第四态）→ 0，不动旧键。
  // 存活判据接探针三态收尾单源（与 EXISTS 计数 / RESTORE NX 同一判序，
  // 删去内联 read_stored_index 手兜底与本处手抄四臂，去重）
  if nx && probe_alive_or_bail!(store, prefix.as_slice(), new_key, vector, output) {
    output.write_resp_int(0);
    return Ok(true);
  }

  // 三探旧键定物理域：String 域命中 → 字符串迁移；信封域命中 → 对象键迁移
  //（值首字节起即信封载荷，原样搬移不嗅探内容）；Meta 域命中（RangeIndex / 升阶键）
  // → 降级异步完整路由（树排空重建）；皆缺 → 向量登记表承接（C# 统一记录
  // RecordType=VectorManager.RecordType 的 rust 对偶，与 wkv 用户键删除单点的「双域未命中且登记表命中」缺席观测钩子同口径）；未登记 → NOSUCHKEY
  #[derive(Copy, PartialEq, Eq, Clone)]
  enum RenameDomain {
    Str,
    Obj,
  }
  impl RenameDomain {
    /// 本域对应的物理键标签（旧键双探与新键补偿读共用同一映射单源）
    const fn tag(self) -> KeyTag {
      match self {
        Self::Str => KeyTag::String,
        Self::Obj => KeyTag::ObjectEnvelope,
      }
    }
  }
  // String → ObjectEnvelope 顺序双探：两域共用同一读内核与同一三态收尾臂
  //（命中即定域早退，杜绝逐域抄一套 Deferred/Err 臂）
  let mut hit = None;
  for domain in [RenameDomain::Str, RenameDomain::Obj] {
    match read_tag_sync_with_prefix(store, prefix.as_slice(), old_key, domain.tag(), |v| {
      v.to_vec()
    }) {
      Ok(TagRead::Hit(val)) => {
        hit = Some((val, domain));
        break;
      }
      // 本域内存确认缺席 → 续探下一域
      Ok(TagRead::Missing) => {}
      // 旧键本域有磁盘候选 / TTL 待裁决：降级
      Ok(TagRead::Deferred) => return Ok(false),
      Err(_) => bail_err_frame!(output),
    }
  }
  let (old_val, domain) = match hit {
    Some(found) => found,
    // 双域皆缺：Meta 域探针 + 向量登记表第四态 + NOSUCHKEY（本分支各臂皆收尾）
    None => {
      match read_tag_sync_with_prefix(
        store,
        prefix.as_slice(),
        old_key,
        KeyTag::Meta,
        meta_collection_type_of,
      ) {
        // Meta 域命中（RangeIndex / 升阶键）：须走慢路径进行树排空与重建
        Ok(TagRead::Hit(Some(_))) | Ok(TagRead::Deferred) => return Ok(false),
        Ok(TagRead::Hit(None)) | Ok(TagRead::Missing) => {}
        Err(_) => bail_err_frame!(output),
      }
      // 登记表域键单次外提（会话域内寻址）；「三域皆缺 → 是否向量集」的
      // 第四态判定与存活折叠同一判据源（ttl_sync registry_alive 单点，
      // 已含「有登记表」前置），臂内不再手抄 read_stored_index 命中式。
      // 登记写透 async 化后快路径不再承接向量集迁移：登记命中即整体
      // 降级慢路径（rename_slow 臂真异步清退新键 + 迁移 + 合成写闭环）
      if registry_alive(vector, prefix.as_slice(), old_key) {
        return Ok(false);
      }
      abort_with_error_message(output, cs::RESP_ERR_GENERIC_NOSUCHKEY);
      return Ok(true);
    }
  };

  // 旧键 TTL 记录（RecordOnDisk：TTL 值在磁盘候选，降级）
  let old_ttl = match ttl_of_sync_with_prefix(store, prefix.as_slice(), old_key) {
    Ok(StoreResult::Success(ttl)) => ttl,
    Ok(StoreResult::NotFound) => None,
    Ok(StoreResult::RecordOnDisk) => return Ok(false),
    Err(_) => bail_err_frame!(output),
  };

  // 旧键 ETag 记录（对标 C# RENAME 搬迁记录连同可选 ETag 字段；
  // Ok(None)：磁盘候选降级）
  let old_etag = match etag_of_sync_with_prefix(store, prefix.as_slice(), old_key) {
    Ok(Some(etag)) => etag,
    Ok(None) => return Ok(false),
    Err(_) => bail_err_frame!(output),
  };

  // 覆写语义下先清退新键既有记录（新键向量集清退 + 对象键迁移清退既有记录与随键 TTL）。
  // 登记写透 async 化后快路径不再承接向量集清退：新键登记命中即整体降级
  // 慢路径（rename_slow 臂真异步清退 + 迁移闭环）；未命中零操作放行。
  // 向量半爿仍门 !nx：存活探针（上方 nx 臂）已折叠 registry_alive 第四态，
  // NX 通过即登记表不命中，无须二次判定（慢径臂为无条件幂等清退，同口径不
  // 分叉）。对象域旁域清退不门 !nx（对标 C# needDeleteNewKey 与 isNX 无涉，
  // UnifiedStoreOps.cs:338-347；位置对偶慢径 delete_string(new_key)，
  // 该句在 NX 判定之后、域分派之前无条件执行）：探针判死的过期 String 残留
  // 物理仍在，信封写入既不自带跨域清退、亦按 RMW 语义保留残留 TTL，缺此
  // 预清即出双域并存（读面 String 优先命中 → WRONGTYPE）或新键随残 TTL
  // 整键隐死。键不存在时本原语回 Ok(Ok(false)) 零副作用（墓碑与 WATCH 推进
  // 由 wkv 用户键删除单点统一收口，与 DEL 缺席形同向）
  if !nx && registry_alive(vector, prefix.as_slice(), new_key) {
    return Ok(false);
  }
  if domain == RenameDomain::Obj {
    let pre_clear = store.try_delete_sync(new_key);
    bail_store_step!(output, pre_clear, Ok(Ok(_)), Ok(Err(_)));
  }

  // 写新键：String / 信封域各取其域的单套 SET（upsert）写入口，isNX 不另起
  // 第二写原语（对标 C# 「GET(newKey) 存活判定即唯一 NX 裁决（:301-306，
  // CheckExpiry 令过期键判 NOTFOUND）→ SET(newKey) 整记录覆写（:363）两臂
  // 共用」，与慢径 Str 臂 upsert_string 同一原语对位）。纯物理 NX 插口在
  // 探针已放行后必不拒（同闩窗内无并发写者），残留过期记录即「探针判死、
  // 物理在场」，误用插口即出假 :0 应答（实际可 rename）
  let write_res = match domain {
    RenameDomain::Str => store.try_upsert_sync(new_key, old_val.as_slice()),
    RenameDomain::Obj => {
      store.try_upsert_tag_sync(new_key, KeyTag::ObjectEnvelope, old_val.as_slice())
    }
  };
  bail_store_step!(output, write_res, Ok(Ok(_)), Ok(Err(_)));
  // 对象域新键入账（对标 C# UnifiedStoreOps.RENAME 的 SET(newKey) →
  // WriteLogUpsert 全量条目：libs/server/Storage/Session/UnifiedStore/UnifiedStoreOps.cs:RENAME）
  // ：resp 层直写信封域不经 StorageSession::upsert_tag
  // 的通知漏斗，此处显式补发 ObjectStoreUpsert——漏记则重放端只删旧键、
  // 新键无从建立，集合键丢失
  if domain == RenameDomain::Obj {
    let raw_key = store.session_tag_key(KeyTag::ObjectEnvelope, new_key);
    // AOF 入队失败按 error.rs AofEnqueue 契约以错误拒绝本命令（票
    // wnode-objrmw-aof-enqueue-swallow-matrix，与 r167c-aoffail 同族收口）：
    // 新键写已生效不回滚，漏记即副本只删旧键、新键无从建立（本域注释自证
    // 危害），吞错冒答 +OK 是假成功，禁沿用；错误帧与本域其余存储失败臂同形
    if let Err(e) = store.notify_envelope_upsert(raw_key.as_slice(), old_val.as_slice()) {
      log::error!("RENAME 对象域 AOF 入队失败，命令拒绝: {e}");
      bail_err_frame!(output);
    }
  }
  if let Some(exp) = old_ttl {
    // RENAME 迁移裸写、不套粗化（对标 C# UnifiedStoreOps.cs:363 RENAME 用
    // `SET(newKey, in logRecord)` 把旧记录的 expiration optional 原样随记录
    // 迁移；MainStore/RMWMethods.cs:499-501 TrySetExpiration 亦收裸 ticks）：
    // old_ttl 取自 ttl_of_sync 的存量值，EXPIRE 族来源已在上游定域、SET 族
    // 本就裸值，此处二次粗化只会引入偏移，破坏逐位相等迁移语义
    let ttl_migrated = put_ttl_sync(store, new_key, exp);
    bail_store_step!(output, ttl_migrated, Ok(true), Ok(false));
  }
  // 新键同步旧 etag：旧键有 etag 则回填，无 etag 则清退新键残留 etag
  //（旧键标签由尾部 try_delete_sync 级联清理）
  let etag_sync_res = if old_etag > NO_ETAG {
    put_etag_sync(store, new_key, old_etag)
  } else {
    del_etag_sync(store, new_key)
  };
  bail_store_step!(output, etag_sync_res, Ok(true), Ok(false));
  // 旧键删除：wkv 删内核已按「记录墓碑先行、随键 TTL 后剥」序级联清退（票
  // wkv-ttl-sidecar-strip-before-record-crash-window-value-immortal）——前置
  // del_ttl 已移除：先剥旧键 TTL 而删记录途中失败/崩溃即留「旧值永生 +
  // TTL 已亡」窗，后置序残留收敛为无主 TTL 记录良性自愈态
  match store.try_delete_sync(old_key) {
    Ok(Ok(true)) => {}
    Ok(Ok(false)) => {
      // 补偿臂：确认待删内容确系本次所写再删，杜绝裸删吞并发写
      //（两域同一读内核、同一四态折叠，域标签经 RenameDomain::tag 单源）
      let is_our_val =
        match read_tag_sync_with_prefix(store, prefix.as_slice(), new_key, domain.tag(), |v| {
          v == old_val.as_slice()
        }) {
          Ok(TagRead::Hit(matches)) => matches,
          Ok(TagRead::Missing) | Err(_) => false,
          // 磁盘候选无从比对：沿用原判序保守视为本次所写
          Ok(TagRead::Deferred) => true,
        };
      if is_our_val {
        if old_ttl.is_some() {
          let _ = del_ttl_sync(store, new_key);
        }
        if old_etag > NO_ETAG {
          let _ = del_etag_sync(store, new_key);
        }
        let del_res = match domain {
          RenameDomain::Str => store.try_delete_sync(new_key),
          RenameDomain::Obj => store.try_delete_tag_sync(new_key, KeyTag::ObjectEnvelope),
        };
        bail_store_step!(output, del_res, Ok(Ok(_)), Ok(Err(_)));
      }
      abort_with_error_message(output, cs::RESP_ERR_GENERIC_NOSUCHKEY);
      return Ok(true);
    }
    Ok(Err(_)) => return Ok(false),
    Err(_) => bail_err_frame!(output),
  }

  reply_renamed(nx, output);
  Ok(true)
}
