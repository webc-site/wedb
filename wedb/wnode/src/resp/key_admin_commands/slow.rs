//! 键管理族慢路径执行段（快路径 `Ok(false)` 降级承接）
//!
//! 对标 C# KeyAdminCommands.cs 同步函数体内 CompletePending 就地闭环的
//! 应答形态：TTL / 存在性 / 迁移裁决降级后，本模块以存储会话异步口
//! （wkv `expire_at` / `pttl_ms` / `expiretime_ms` / `persist` 与三域探针）
//! 重放整条命令，应答与快路径逐字节一致；参数推导转调快侧同一纯函数。

use wbase::{convert::expire_after_to_ticks, time::now_ticks};
use wdev::Device;
use wresp::{
  check_args::unpack_args,
  cmd_strings::{self as cs, write_error_raw, write_raw},
  command::RespCommand,
  ext::RespVecExt,
};
use wval::KeyTag;

use super::{
  ExpireCmd,
  keys::{ExpireArgs, FLAG_FRAMES, parse_expire_args, reply_renamed},
  types::{
    ERR_DUMP_PAYLOAD_INVALID, parse_restore_args, restore_residual_rollback, write_dump_payload,
  },
};
use crate::{
  resp::{TtlResume, vector::vector_manager::VectorManager},
  storage::session::{
    common::{
      UserReadAsync,
      ttl_sync::{probe_alive_with_registry_async, registry_alive},
    },
    storage_session::StorageSession,
  },
};

/// 慢径存储步骤收尾单源：await → wkv 硬故障折 `Err(())` 交执行域统一应答
/// （判序与原逐处 `.await.map_err(|_| ())?` 完全一致，仅去样板；需按原口径
/// 告警的臂不入本宏，就地 `map_err` 保日志文案逐字）
macro_rules! stor {
  ($res:expr) => {
    $res.await.map_err(|_| ())?
  };
}

/// 慢径存活探针收尾单源：三域 + 向量登记表第四态折叠（判据唯一点
/// `probe_alive_with_registry_async`）→ 布尔，硬故障折 `Err(())`
async fn alive_probe(
  storage: &StorageSession<'_, impl Device>,
  prefix: &[u8],
  key: &[u8],
  vector: Option<&VectorManager>,
) -> Result<bool, ()> {
  probe_alive_with_registry_async(storage, prefix, key, vector)
    .await
    .map_err(|_| ())
}

/// TTL 族慢路径读侧（TTL/PTTL/EXPIRETIME/PEXPIRETIME 共用：wkv 异步读内核
/// 出参毫秒口径；向量键与写侧同源收敛三域口径，不接登记表第四态——同键
/// TTL -2 与 EXPIRE :0 应答一致，见 ttl_sync probe_alive_with_registry 头注）
///
/// 返回 RESP 出参：-2 无 key；-1 无 TTL；否则 Unix 毫秒（PTTL/PEXPIRETIME
/// 原样，TTL/EXPIRETIME 由调用方换秒）
async fn ttl_read_ms(
  storage: &StorageSession<'_, impl Device>,
  key: &[u8],
  absolute: bool,
) -> Result<i64, ()> {
  if absolute {
    storage.expiretime_ms(key).await.map_err(|_| ())
  } else {
    storage.pttl_ms(key).await.map_err(|_| ())
  }
}

/// RENAME 慢路径执行体（`rename_sync` 的异步对偶；序次对齐：同键早退 →
/// 三探旧键定物理域 → 旧键 TTL/ETag → [NX] 新键存活判定 → 新键向量清退 →
/// 对象域先删 → 写新键 → TTL/ETag 随键迁移 → 清旧键 TTL → 删旧键）。
/// Meta 域命中（RangeIndex / 升阶键）经 wkv `rename_range_index` 整树快照
/// 迁移（快路径降级注释所指的「树排空重建」唯一内核，不另起第三套迁移；
/// dst String/信封残留清退与旧键排空注销一并下沉该内核逐段保失败即原态）
/// RENAME 迁移物理域
#[derive(Clone, Copy, PartialEq, Eq)]
enum RenameDomain {
  Str,
  Obj,
  Meta,
}

impl RenameDomain {
  /// 本域对应的物理键标签（旧键三域探针与新键补偿复验共用同一映射单源）
  const fn tag(self) -> KeyTag {
    match self {
      Self::Str => KeyTag::String,
      Self::Obj => KeyTag::ObjectEnvelope,
      Self::Meta => KeyTag::Meta,
    }
  }
}

async fn rename_slow(
  storage: &StorageSession<'_, impl Device>,
  old_key: &[u8],
  new_key: &[u8],
  nx: bool,
  vector: Option<&VectorManager>,
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  // C# 同键早退：RENAME → OK；RENAMENX → 1（先于 NX 存在性判定）
  if old_key == new_key {
    reply_renamed(nx, output);
    return Ok(());
  }

  // 会话前缀单次外提（循环前缀外提对位：本函数内探针链与登记表判定零前缀派生）
  let prefix = storage.batch.session_prefix();
  let prefix = prefix.as_slice();

  // 双键读改写窗口（快路径 rename_sync 同一窗口契约，票 zcode-r15-generic
  // 发现一对标 C# SaveKeyEntryToLock 双键排他事务锁）：桶序取闩防 RENAME
  // a b / RENAME b a 交叉死锁，闩内完成全序列；慢路径逐段 await 间无锁
  // 窗口就此收口。Meta 域臂例外放闩（见分支内注释）
  let mut windows = Some(stor!(storage.batch.rmw_window_sorted([old_key, new_key])));

  // 三探旧键定物理域（异步闭环，磁盘候选不再降级）：String / ObjectEnvelope
  // 顺序双探共用同一读内核与同一收尾臂（命中即定域早退，杜绝逐域手抄一套）
  let mut hit = None;
  for domain in [RenameDomain::Str, RenameDomain::Obj] {
    if let Some(val) =
      stor!(storage.read_tag_with_prefix(prefix, old_key, domain.tag(), |v| v.to_vec()))
    {
      hit = Some((val, domain));
      break;
    }
  }
  let (old_val, domain) = match hit {
    Some(found) => found,
    // 双域皆缺：Meta 域探针 → 向量登记表第四态 → NOSUCHKEY（各臂皆收尾）
    None => {
      // Meta 域命中（RangeIndex / 升阶键）：整树快照迁移 + 新键元记录落盘
      let meta = stor!(storage.read_tag_with_prefix(prefix, old_key, KeyTag::Meta, |_| ()));
      if meta.is_some() {
        (Vec::new(), RenameDomain::Meta)
      // 皆缺 → 向量登记表承接（C# 统一记录 RecordType=VectorManager 的 rust
      // 对偶）；RENAMENX 先判新键存活（三域 + 第四态折叠，与快路径
      // rename_vector_set_sync 同一单点 probe_alive_with_registry_async）
      } else if registry_alive(vector, prefix, old_key) {
        if nx && alive_probe(storage, prefix, new_key, vector).await? {
          output.write_resp_int(0);
          return Ok(());
        }
        return rename_vector_set_slow(storage, old_key, new_key, nx, vector, output).await;
      } else {
        // 未登记 → NOSUCHKEY；第四态判据单点 registry_alive
        write_error_raw(output, cs::RESP_ERR_GENERIC_NOSUCHKEY);
        return Ok(());
      }
    }
  };

  // 旧键 TTL / ETag 记录（随键迁移；ticks 与 etag 均裸值，幂等重放）。
  // etag_of 的 None 即无记录（NoETag 0），与快路径 etag_of_sync 同值域。
  // old_ttl 快照仅供 Str/Obj 域臂消费（无迁移 claim 面）；Meta 域臂随迁
  // 下沉 rename_range_index 段五 claim 窗内现势读取，本快照不消费
  let old_ttl = stor!(storage.batch.ttl_of(old_key));
  let old_etag = stor!(storage.batch.etag_of(old_key)).unwrap_or(wval::NO_ETAG);

  // RENAMENX：新键存活（三域 + 向量登记表第四态）→ 0，不动旧键；折叠式与
  // 快路径同一单点（ttl_sync probe_alive_with_registry_async）
  if nx && alive_probe(storage, prefix, new_key, vector).await? {
    output.write_resp_int(0);
    return Ok(());
  }

  // 新键向量集清退（C# needDeleteNewKey 的显式 DELETE(new)；登记表未命中
  // 即无操作）
  if let Some(vm) = vector {
    vm.delete_vector_set(prefix, new_key).await;
  }
  // 旧键 TTL 记录在场标记（迁移尾部据此清旧键 TTL、补偿臂据此回滚）
  let had_ttl = old_ttl.is_some();

  match domain {
    RenameDomain::Meta => {
      // Meta 域迁移让闩：rename_range_index 整树快照含 fsync 与惰性恢复，属
      // 百毫秒级重 IO，桶闩纪律「持闩期只有读—算—写三段纯内存操作，微秒级
      // 即放闩」（wkv rmw_window 自旋预算即按此定容）不容持闩跨快照；Meta 域
      // 的「RENAME vs 并发 EXPIRE/PERSIST/写原语」互斥已由该内核段一 old/new
      // 双键迁移 claim 完整承接（wkv range_index/migration.rs 锁形取舍注释：
      // claim 窗即 C# RENAME 双键排他锁窗的完整对位，判点零副作用拒绝），
      // 桶闩让位于 claim，两机制同向叠加不冲突
      drop(windows.take());
      // RI / 升阶键：整树快照一物两用换入（promote 同款写序）。key 级 TTL
      // 随迁已下沉 rename_range_index 段五 claim 窗内（窗内 EXPIRE/PERSIST
      // 双键被 wkv 判点零副作用拒绝、排空前读旧键现势即随迁终态，见该内核
      // 段五注释）——调用方零快照消费（窗前陈旧快照回填与 claim 登记间隙
      // 的插入面交叠即「续期蒸发 / 已撤销 TTL 借尸还魂」，就此收口）；旧键
      // TTL 已随段五排空清退，had_ttl=false 免去幂等 persist。迁移尾部
      //（ETag 同步 + 旧键收尾删除）与 String / 对象域共用
      storage
        .batch
        .rename_range_index(old_key, new_key)
        .await
        .map_err(|e| {
          log::error!("rename_range_index error: {e:?}");
        })?;
      // Meta 域树迁移不可逆，不设条件补偿臂（rollback=None）
      finish_rename_move(storage, old_key, new_key, old_etag, false, None).await?;
    }
    RenameDomain::Str | RenameDomain::Obj => {
      // 对象键迁移：覆写语义下先清退新键既有记录（含 String 域残留与随键
      // TTL，信封写入不自带跨域清退），再整体搬移信封载荷（经
      // StorageSession::upsert_tag 的信封整值写通知漏斗，AOF 重放端新键
      // 建立不缺条目）；字符串域直写 SET（自动清新键残留 TTL）
      let val = old_val.as_slice();
      if domain == RenameDomain::Obj {
        stor!(storage.delete_string(new_key));
        stor!(storage.upsert_tag(new_key, KeyTag::ObjectEnvelope, val));
      } else {
        stor!(storage.upsert_string(new_key, val));
      }
      if let Some(exp) = old_ttl {
        // TTL 随键迁移（裸 ticks 逐位相等，对标 C# TryCopyFrom 连同
        // Expiration 拷入新记录；二次粗化只会引入偏移）
        stor!(storage.batch.put_ttl(new_key, exp));
      }
      let rollback = Some((val, domain));
      finish_rename_move(storage, old_key, new_key, old_etag, had_ttl, rollback).await?;
    }
  }

  reply_renamed(nx, output);
  Ok(())
}

/// RENAME 迁移尾部（String / 对象域共用）：新键同步旧 etag（旧键有则回填、
/// 无则清退新键残留）→ 清旧键 TTL → 删旧键（C# DELETE 连同 Expiration
/// 一并移除，先清 TTL 避免孤儿记录令后续读取长期走异步裁决）。
/// 补偿臂：确认待删内容确系本次所写再删，杜绝裸删吞并发写
async fn finish_rename_move(
  storage: &StorageSession<'_, impl Device>,
  old_key: &[u8],
  new_key: &[u8],
  old_etag: i64,
  had_ttl: bool,
  expected_rollback: Option<(&[u8], RenameDomain)>,
) -> Result<(), ()> {
  use wval::NO_ETAG;
  if old_etag > NO_ETAG {
    storage
      .batch
      .put_etag(new_key, old_etag)
      .await
      .map_err(|e| {
        log::error!("put_etag error: {e:?}");
      })?;
  } else {
    storage.batch.del_etag(new_key).await.map_err(|e| {
      log::error!("del_etag error: {e:?}");
    })?;
  }
  if had_ttl {
    // 窗内裸清退用 del_ttl（幂等探针+delete_raw 自持锁零取键桶闩）：本函数
    // 恒运行于 rename_slow 的 rmw_window_sorted 双键排他窗内，persist 的
    // ttl_write_gate! 单键独占桶闩与在窗自闩同桶非重入，调用必自致
    // LockTimeout（「窗/TTL/事务三面同键同桶互斥」纪律下窗即串行化凭据，
    // 无须也不得在窗内再取同闩）
    storage.batch.del_ttl(old_key).await.map_err(|e| {
      log::error!("del_ttl error: {e:?}");
    })?;
  }
  let deleted = storage.delete_string(old_key).await.map_err(|e| {
    log::error!("delete_string old error: {e:?}");
  })?;
  if let Some((expected_val, domain)) = expected_rollback
    && !deleted
  {
    log::error!("old_key not deleted, rolling back new_key conditionally");
    let is_our_val = matches!(
      storage
        .read_tag_with(new_key, domain.tag(), |v| v == expected_val)
        .await,
      Ok(Some(true))
    );
    if is_our_val {
      if had_ttl {
        let _ = storage.batch.del_ttl(new_key).await;
      }
      if old_etag > NO_ETAG {
        let _ = storage.batch.del_etag(new_key).await;
      }
      let del_res = match domain {
        RenameDomain::Str => storage.delete_string(new_key).await,
        RenameDomain::Obj => storage.delete_tag(new_key, KeyTag::ObjectEnvelope).await,
        RenameDomain::Meta => Ok(true),
      };
      if let Err(e) = del_res {
        log::error!("delete new_key rollback error: {e:?}");
      }
    }
    return Err(());
  }
  Ok(())
}

/// RENAME 向量集分支（`rename_vector_set_sync` 的异步对偶：新键 wkv 域
/// 残留清退 → 登记表迁移 → AOF 合成 RENAME 条目）
async fn rename_vector_set_slow(
  storage: &StorageSession<'_, impl Device>,
  old_key: &[u8],
  new_key: &[u8],
  nx: bool,
  vector: Option<&VectorManager>,
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  // 分支进入条件：旧键登记命中（三域皆缺）且 NX 判定已过（调用方分支内完成）
  // 新键 wkv 域残留清退（C# DELETE(newKey)；未命中即无操作）
  stor!(storage.delete_string(new_key));
  if let Some(vm) = vector {
    let prefix = storage.batch.session_prefix();
    let prefix = prefix.as_slice();
    // 新键向量集清退（C# case #3/#4：新键为向量集须显式 DELETE）
    vm.delete_vector_set(prefix, new_key).await;
    // 登记表迁移（C# MarkSuppressCleanup(old) → SET(new) →
    // UpdateHashSlot → DELETE(old) 的窗口序）
    vm.rename_vector_set(prefix, old_key, new_key).await;
    // AOF 合成条目：入队失败沿 error.rs AofEnqueue 契约上抛拒绝本命令
    //（已生效 + 镜像缺失经错误帧对客户端可见，enqueue 内已告警日志；
    // 禁吞错冒答 +OK 令副本/重放永缺 RENAME 条目而主从发散）
    vm.replicate_vector_set_rename(prefix, old_key, new_key)
      .map_err(|_| ())?;
  }
  reply_renamed(nx, output);
  Ok(())
}

/// 键管理族慢路径执行段入口（exec_slow 分派；`Err(())` 为存储错误，
/// 调用方统一应答）
pub(crate) async fn key_admin_slow(
  storage: &StorageSession<'_, impl Device>,
  cmd: RespCommand,
  parse_state: &[&[u8]],
  vector: Option<&VectorManager>,
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  use RespCommand as C;
  match cmd {
    C::Exists => {
      // 多键计数；三域存活 + 向量登记表第四态逐键裁决收敛到探针家族单点
      // probe_alive_with_registry_async：与快路径 probe_alive_with_registry 同
      // 一条「三域 || 第四态」折叠式、同一第四态判据源（ttl_sync registry_alive），
      // 两态只差三域取数通道（同步纪元内存直读 vs 异步读口闭环），臂内不再
      // 内联第四态
      // 会话前缀单次外提（循环前缀外提对位：探针链与登记表判定零前缀派生）
      // C# NetworkEXISTS 口径：循环内每键一次存储调用、存活本地累加、末尾
      // 一次写整型应答（重复键同计数，键数 0 由 arity 校验前置拦截）
      let prefix = storage.batch.session_prefix();
      let prefix = prefix.as_slice();
      let mut exists_count = 0i64;
      for key in parse_state {
        if alive_probe(storage, prefix, key, vector).await? {
          exists_count += 1;
        }
      }
      output.write_resp_int(exists_count);
      Ok(())
    }
    // TTL / PTTL / EXPIRETIME / PEXPIRETIME 四命令共同体：读侧同一 wkv 异步
    // 毫秒口（`ttl_read_ms`），秒域换算按「是否绝对戳」二选一；个数错误文案
    // 沿快路径（TTL/PTTL 各报本名，PEXPIRETIME 报 EXPIRETIME 系 C# quirk）
    C::Ttl | C::Pttl | C::Expiretime | C::Pexpiretime => {
      let absolute = matches!(cmd, C::Expiretime | C::Pexpiretime);
      let to_secs = matches!(cmd, C::Ttl | C::Expiretime);
      let name = match cmd {
        C::Ttl => "TTL",
        C::Pttl => "PTTL",
        _ => "EXPIRETIME",
      };
      let Some([key]) = unpack_args(parse_state, output, name) else {
        return Ok(());
      };
      // wkv pttl_ms 已按 ConvertUtils.MillisecondsFromDiffUtcNowTicks 口径
      // 换算毫秒；TTL 秒 = 毫秒域四舍五入 (ms + 500) / 1000，对标 C#
      // ConvertUtils.SecondsFromDiffUtcNowTicks 的 ticks 域
      // (diff + TPS/2) / TPS：ticks → 毫秒截断损失 r < 10_000 ticks（<1ms）
      // 且半秒余量 5_000_000 ticks 与 500ms 整对齐、r 恒不跨进位边界
      //（最贴边界处余 10_000 ticks），与快路径 seconds_from_diff_ticks 逐值
      // 等价（旧 div_euclid 双重向下取整在 1.5 秒余量处回 1、快路径与 C#
      // 回 2 的分叉就此收口）；负值 -1/-2 已在 pttl_ms 出口收敛，原样透传。
      // +500 用 saturating：EXPIRE 大值钳 i64::MAX ticks（deviations 登记面）
      // 时 pttl_ms 出参逼近 i64::MAX，裸加溢出即 debug panic / release 环回
      //
      // 绝对戳侧（EXPIRETIME）Unix 毫秒 → 秒同款嵌套截断恒等（快路径
      // unix_time_in_seconds_from_ticks）；哨兵 -2（缺失/过期）与 -1（无 TTL）
      // 原样透传，严禁无守卫 div_euclid 将 -2 折成 -1（对标 C#
      // NetworkEXPIRETIME 与 HandleExpireTime）
      let value = ttl_read_ms(storage, key, absolute).await?;
      output.write_resp_int(match (value > 0, to_secs, absolute) {
        (true, true, false) => value.saturating_add(500) / 1000,
        (true, true, true) => value.div_euclid(1000),
        _ => value,
      });
      Ok(())
    }
    C::Expire | C::Pexpire | C::Expireat | C::Pexpireat => {
      let expire_cmd = match cmd {
        C::Expire => ExpireCmd::Expire,
        C::Pexpire => ExpireCmd::Pexpire,
        C::Expireat => ExpireCmd::Expireat,
        _ => ExpireCmd::Pexpireat,
      };
      let Some(ExpireArgs {
        key,
        expire_at_ticks,
        opt,
      }) = parse_expire_args(expire_cmd, parse_state, output)
      else {
        return Ok(());
      };
      // StorageSession 带选项包装（快路径 expire_apply_sync 的镜像单点）：
      // -2 键缺失 / 0 条件不满足 → :0；1 已设置 / 2 过去时间戳已物理删除 →
      // :1（含 Redis 7.4 立即删除语义）；粗化已在上游 `parse_expire_args`
      // 命令边界单点施加，本会话入口恒等直通（§143）。
      // 经包装而非裸调 batch.expire_at：applied > 0 的 WATCH 版本推进尾巴
      // 与 AOF 重放端共用一处口径
      let applied = stor!(storage.expire_at_ticks_opt(key, expire_at_ticks, opt));
      // 判据 applied > 0（快慢路径同态应答的锚点）：wkv expire_at 的 -2
      // （键缺失 / TTL 已过期视同缺失）必须落 :0，与快路径
      // expire_apply_sync 的 Some(0) 臂及 C# status != OK 回 :0 三方对齐；
      // 误用 != 0 会把 -2 误答 :1（应答 :1 而键已消失的用户可见发散）
      write_raw(output, FLAG_FRAMES[usize::from(applied > 0)]);
      Ok(())
    }
    C::Persist => {
      let Some([key]) = unpack_args(parse_state, output, "PERSIST") else {
        return Ok(());
      };
      // persist_key 包装（PERSIST 快慢路径与 AOF 重放同一推进尾巴：applied > 0
      // 推进 WATCH 版本，裸调 batch.persist 缺席推进禁再直调）
      let removed = stor!(storage.persist_key(key));
      output.write_resp_int(i64::from(removed));
      Ok(())
    }
    C::Getdel => {
      let Some([key]) = unpack_args(parse_state, output, "GETDEL") else {
        return Ok(());
      };
      // 窗口 + 域判探针 + 取删一体（快路径 network_getdel 同一原子性契约）：
      // 应答值即本次实际摘除记录的值，杜绝读值与删除间隙内并发 SET 的新值被删
      // 而旧值被答出；摘除空手（并发盲 DEL 先行）沿串行序 DEL→GETDEL 答 nil；
      // 探针走 read_user_quiet 零入账口（RMW 前置读不入账纪律的慢臂对偶，
      // 对位快臂 keys.rs read_user_sync 传 None 与 C# GETDEL 全链零计数）
      let _window = stor!(storage.batch.rmw_window(key));
      match stor!(storage.read_user_quiet(key, |_| ())) {
        UserReadAsync::Hit(()) => match stor!(storage.take_string(key)) {
          Some(val) => output.write_resp_bulk_string(&val),
          None => output.write_resp_null_ver(storage.resp_version),
        },
        UserReadAsync::WrongType => {
          output.write_resp_error(cs::RESP_ERR_WRONG_TYPE);
        }
        UserReadAsync::Missing => {
          output.write_resp_null_ver(storage.resp_version);
        }
      }
      Ok(())
    }
    C::Rename | C::Renamenx => {
      let (cmd_name, nx) = if cmd == C::Rename {
        ("RENAME", false)
      } else {
        ("RENAMENX", true)
      };
      let Some([old_key, new_key]) = unpack_args(parse_state, output, cmd_name) else {
        return Ok(());
      };
      rename_slow(storage, old_key, new_key, nx, vector, output).await
    }
    C::Dump => {
      let Some([key]) = unpack_args(parse_state, output, "DUMP") else {
        return Ok(());
      };
      // 载荷组装与快路径同一单源（types::write_dump_payload：类型字节 + 长度
      // 前缀 + 值 + rdb 版本 + crc64，crc 含类型字节起算保证 DUMP→RESTORE
      // 往返成立），逐字节直写 output
      let dumped = stor!(storage.read_user(key, |value| write_dump_payload(value, output)));
      match dumped {
        UserReadAsync::Hit(true) => {}
        UserReadAsync::Hit(false) => {
          write_error_raw(output, ERR_DUMP_PAYLOAD_INVALID);
        }
        // 对象键（信封域命中）与键缺失一致：C# WRONGTYPE → nil 同口径
        UserReadAsync::WrongType | UserReadAsync::Missing => {
          output.write_resp_null_ver(storage.resp_version);
        }
      }
      Ok(())
    }
    C::Restore => {
      // 尾参为快路径「值已提交 + TTL 待投」续跑标记（[`TtlResume::from_tail`]
      // 逆解析，沿 MSETNX/DEL 尾参先例，exec 降级快照恒追加）：Pending = 值
      // 已同步提交、TTL 遭环形页翻转降级，剥尾参后跳过整命令重放自碰已提交
      // 值（本命令刚建的键被存活探针判存在即误回 BUSYKEY、TTL 永缺，票
      // wnode-nx-conditional-ttl-degrade-replay-selfhit），持窗仅补投 TTL 回 +OK
      let Some((tail, cmd_args)) = parse_state.split_last() else {
        cs::abort_with_wrong_number_of_arguments(output, "RESTORE");
        return Ok(());
      };
      let resume = TtlResume::from_tail(Some(tail));
      let Some((key, expiry, val)) = parse_restore_args(cmd_args, output) else {
        return Ok(());
      };
      // 单键读改写窗口（快路径 network_restore 同一窗口契约，票
      // zcode-r15-generic 发现一对标 C# SET_Conditional 原子条件写）：
      // 闩内完成「存活探测 → 写入 → TTL 落库」全序列，杜绝探测判不存在后
      // 并发 SET 落库再被覆写的 BUSYKEY 契约绕过
      let _window = stor!(storage.batch.rmw_window(key));
      if let TtlResume::Pending(ticks) = resume {
        // 快臂值已落库且降级零应答，本臂复取同窗补投裸 ticks（对标 C#
        // 单记录 CAS 值与过期一体落库的终态）后出 +OK
        if storage.batch.put_ttl(key, ticks).await.is_err() {
          // 补投硬故障：快臂已提交残值同窗补偿删（回滚单源，防「值无 TTL
          // 永存 + 应答错误帧 + 重试恒 BUSYKEY」分裂态）后交执行域统一应答
          restore_residual_rollback(&storage.batch, key);
          return Err(());
        }
        write_raw(output, cs::RESP_OK);
        return Ok(());
      }
      // SET_Conditional(SETEXNX)：仅键不存在时写入（NX 语义）。存活折叠
      // 收敛到单点 probe_alive_with_registry_async：与快路径
      // network_restore 同一「三域 || 登记表第四态」判据（票
      // zcode-r161c-msetnx 案一：快臂第四态半爿已收编闩窗内折叠，派发层
      // 窗外 BUSYKEY 位删除，本臂是该折叠的异步收尾面），删去两处手抄判定
      let prefix = storage.batch.session_prefix();
      if alive_probe(storage, prefix.as_slice(), key, vector).await? {
        write_error_raw(output, cs::RESP_ERR_BUSSYKEY);
        return Ok(());
      }
      stor!(storage.upsert_string(key, val));
      if expiry > 0 {
        // C#：UtcNow.Ticks + FromSeconds(expiry).Ticks；口径为秒（非 Redis
        // 的毫秒，见快路径头部差异说明）
        let expire_at_ticks = expire_after_to_ticks(now_ticks(), expiry);
        if storage.batch.put_ttl(key, expire_at_ticks).await.is_err() {
          // TTL 落库硬故障：本次 upsert_string 残值同窗补偿删（回滚单源）
          // 后交执行域统一应答
          restore_residual_rollback(&storage.batch, key);
          return Err(());
        }
      }
      write_raw(output, cs::RESP_OK);
      Ok(())
    }
    _ => {
      // 分派表漏接线信号：本臂只应承接键管理族，其余落此即缺陷
      write_error_raw(output, cs::RESP_ERR_ASYNC_REQUIRED);
      Ok(())
    }
  }
}
