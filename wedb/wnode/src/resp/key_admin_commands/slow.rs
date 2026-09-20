//! 键管理族慢路径执行段（快路径 `Ok(false)` 降级承接）
//!
//! 对标 C# KeyAdminCommands.cs 同步函数体内 CompletePending 就地闭环的
//! 应答形态：TTL / 存在性 / 迁移裁决降级后，本模块以存储会话异步口
//! （wkv `expire_at` / `pttl_ms` / `expiretime_ms` / `persist` 与三域探针）
//! 重放整条命令，应答与快路径逐字节一致；参数推导转调快侧同一纯函数。

use itoa::Buffer;
use wbase::{convert::expire_after_to_ticks, crc64::hash, time::now_ticks};
use wresp::{
  cmd_strings::{self as cs, write_error_raw, write_raw},
  command::RespCommand,
  ext::RespVecExt,
  length::try_write_length,
};
use wval::KeyTag;

use super::{
  ExpireCmd,
  keys::{ExpireArgs, parse_expire_args},
  types::{RDB_VERSION, parse_restore_args},
};
use crate::{
  resp::vector::vector_manager::VectorManager,
  storage::session::{
    common::{
      UserReadAsync,
      ttl_sync::{probe_alive_with_registry_async, registry_alive},
    },
    storage_session::StorageSession,
  },
};

/// TTL 族慢路径读侧（TTL/PTTL/EXPIRETIME/PEXPIRETIME 共用：wkv 异步读内核
/// 出参毫秒口径；向量键与写侧同源收敛三域口径，不接登记表第四态——同键
/// TTL -2 与 EXPIRE :0 应答一致，见 ttl_sync probe_alive_with_registry 头注）
///
/// 返回 RESP 出参：-2 无 key；-1 无 TTL；否则 Unix 毫秒（PTTL/PEXPIRETIME
/// 原样，TTL/EXPIRETIME 由调用方换秒）
async fn ttl_read_ms(
  storage: &StorageSession<'_, impl wdev::Device>,
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
async fn rename_slow(
  storage: &StorageSession<'_, impl wdev::Device>,
  old_key: &[u8],
  new_key: &[u8],
  nx: bool,
  vector: Option<&VectorManager>,
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  // C# 同键早退：RENAME → OK；RENAMENX → 1（先于 NX 存在性判定）
  if old_key == new_key {
    if nx {
      output.write_resp_int(1);
    } else {
      write_raw(output, cs::RESP_OK);
    }
    return Ok(());
  }

  // 会话前缀单次外提（循环前缀外提对位：本函数内探针链与登记表判定零前缀派生）
  let prefix = storage.batch.session_prefix();
  let prefix = prefix.as_slice();

  // 三探旧键定物理域（异步闭环，磁盘候选不再降级）
  enum RenameDomain {
    Str,
    Obj,
    Meta,
  }
  let old_val = match storage
    .read_tag_with(old_key, KeyTag::String, |v| v.to_vec())
    .await
    .map_err(|_| ())?
  {
    Some(val) => (val, RenameDomain::Str),
    None => match storage
      .read_tag_with(old_key, KeyTag::ObjectEnvelope, |v| v.to_vec())
      .await
      .map_err(|_| ())?
    {
      Some(val) => (val, RenameDomain::Obj),
      None => match storage
        .read_tag_with(old_key, KeyTag::Meta, |_| ())
        .await
        .map_err(|_| ())?
      {
        // Meta 域命中（RangeIndex / 升阶键）：整树快照迁移 + 新键元记录落盘
        Some(()) => (Vec::new(), RenameDomain::Meta),
        None => {
          // 皆缺 → 向量登记表承接（C# 统一记录 RecordType=VectorManager
          // 的 rust 对偶）；未登记 → NOSUCHKEY；第四态判据单点 registry_alive
          if registry_alive(vector, prefix, old_key) {
            // RENAMENX 先判新键存活（三域 + 第四态折叠，与快路径
            // rename_vector_set_sync 同一单点 probe_alive_with_registry_async）
            if nx
              && probe_alive_with_registry_async(storage, prefix, new_key, vector)
                .await
                .map_err(|_| ())?
            {
              output.write_resp_int(0);
              return Ok(());
            }
            return rename_vector_set_slow(storage, old_key, new_key, nx, vector, output).await;
          }
          write_error_raw(output, cs::RESP_ERR_GENERIC_NOSUCHKEY);
          return Ok(());
        }
      },
    },
  };
  let (old_val, domain) = old_val;

  // 旧键 TTL / ETag 记录（随键迁移；ticks 与 etag 均裸值，幂等重放）。
  // etag_of 的 None 即无记录（NoETag 0），与快路径 etag_of_sync 同值域
  let old_ttl = storage.batch.ttl_of(old_key).await.map_err(|_| ())?;
  let old_etag = storage
    .batch
    .etag_of(old_key)
    .await
    .map_err(|_| ())?
    .unwrap_or(wval::NO_ETAG);

  // RENAMENX：新键存活（三域 + 向量登记表第四态）→ 0，不动旧键；折叠式与
  // 快路径同一单点（ttl_sync probe_alive_with_registry_async）
  if nx
    && probe_alive_with_registry_async(storage, prefix, new_key, vector)
      .await
      .map_err(|_| ())?
  {
    output.write_resp_int(0);
    return Ok(());
  }

  // 新键向量集清退（C# needDeleteNewKey 的显式 DELETE(new)；登记表未命中
  // 即无操作）
  if let Some(vm) = vector {
    vm.delete_vector_set(prefix, new_key);
  }

  match domain {
    RenameDomain::Meta => {
      // RI / 升阶键：整树快照一物两用换入（promote 同款写序）+ 新键元记录
      //（集合类型与成员 TTL 水位透传旧元记录）；dst String/信封残留清退、
      // 旧键排空注销（handle_bftree_drain_and_delete）一并下沉该内核，段一/
      // 段二失败 dst 与旧键均保原态；随后 key 级 TTL 随迁，迁移尾部
      //（ETag 同步 + 旧键收尾删除）与 String / 对象域共用
      storage
        .batch
        .rename_range_index(old_key, new_key)
        .await
        .map_err(|_| ())?;
      // WATCH 版本栅栏单点在 wkv 内核段三（dst 残留清退触碰前恰一次推进，窗口
      // 覆盖 dst 清退 + 新键元记录裸原语 upsert_raw 全程，调用方显式补推即同键
      // 双计）；旧键由 finish_rename_move 的 delete_string 降级臂推进，勿重复
      match old_ttl {
        Some(exp) => storage.batch.put_ttl(new_key, exp).await.map_err(|_| ())?,
        // 源键无 TTL：清退 dst 旧 TTL（wkv 段三清退只触数据记录不触旁路，
        // 残留旧 TTL 会借尸还魂令改名后键提前蒸发；降级排空臂已清时本调用
        // 哈希探针落空零写，幂等）
        None => storage.batch.del_ttl(new_key).await.map_err(|_| ())?,
      }
      finish_rename_move(storage, old_key, new_key, old_etag, old_ttl.is_some()).await?;
    }
    RenameDomain::Str => {
      // 字符串域：写新键（SET 语义自动清新键残留 TTL）
      storage
        .upsert_string(new_key, old_val.as_slice())
        .await
        .map_err(|_| ())?;
      if let Some(exp) = old_ttl {
        // TTL 随键迁移（裸 ticks 逐位相等，对标 C# TryCopyFrom 连同
        // Expiration 拷入新记录；二次粗化只会引入偏移）
        storage.batch.put_ttl(new_key, exp).await.map_err(|_| ())?;
      }
      finish_rename_move(storage, old_key, new_key, old_etag, old_ttl.is_some()).await?;
    }
    RenameDomain::Obj => {
      // 对象键迁移：覆写语义下先清退新键既有记录（含 String 域残留与随键
      // TTL，信封写入不自带跨域清退），再整体搬移信封载荷（经
      // StorageSession::upsert_tag 的信封整值写通知漏斗，AOF 重放端新键
      // 建立不缺条目）
      storage.delete_string(new_key).await.map_err(|_| ())?;
      storage
        .upsert_tag(new_key, KeyTag::ObjectEnvelope, old_val.as_slice())
        .await
        .map_err(|_| ())?;
      if let Some(exp) = old_ttl {
        storage.batch.put_ttl(new_key, exp).await.map_err(|_| ())?;
      }
      finish_rename_move(storage, old_key, new_key, old_etag, old_ttl.is_some()).await?;
    }
  }

  if nx {
    output.write_resp_int(1);
  } else {
    write_raw(output, cs::RESP_OK);
  }
  Ok(())
}

/// RENAME 迁移尾部（String / 对象域共用）：新键同步旧 etag（旧键有则回填、
/// 无则清退新键残留）→ 清旧键 TTL → 删旧键（C# DELETE 连同 Expiration
/// 一并移除，先清 TTL 避免孤儿记录令后续读取长期走异步裁决）
async fn finish_rename_move(
  storage: &StorageSession<'_, impl wdev::Device>,
  old_key: &[u8],
  new_key: &[u8],
  old_etag: i64,
  had_ttl: bool,
) -> Result<(), ()> {
  use wval::NO_ETAG;
  if old_etag > NO_ETAG {
    storage
      .batch
      .put_etag(new_key, old_etag)
      .await
      .map_err(|_| ())?;
  } else {
    storage.batch.del_etag(new_key).await.map_err(|_| ())?;
  }
  if had_ttl {
    storage.batch.persist(old_key).await.map_err(|_| ())?;
  }
  storage.delete_string(old_key).await.map_err(|_| ())?;
  Ok(())
}

/// RENAME 向量集分支（`rename_vector_set_sync` 的异步对偶：新键 wkv 域
/// 残留清退 → 登记表迁移 → AOF 合成 RENAME 条目）
async fn rename_vector_set_slow(
  storage: &StorageSession<'_, impl wdev::Device>,
  old_key: &[u8],
  new_key: &[u8],
  nx: bool,
  vector: Option<&VectorManager>,
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  // 分支进入条件：旧键登记命中（三域皆缺）且 NX 判定已过（调用方分支内完成）
  // 新键 wkv 域残留清退（C# DELETE(newKey)；未命中即无操作）
  storage.delete_string(new_key).await.map_err(|_| ())?;
  if let Some(vm) = vector {
    let prefix = storage.batch.session_prefix();
    // 新键向量集清退（C# case #3/#4：新键为向量集须显式 DELETE）
    vm.delete_vector_set(prefix.as_slice(), new_key);
    // 登记表迁移（C# MarkSuppressCleanup(old) → SET(new) →
    // UpdateHashSlot → DELETE(old) 的窗口序）
    vm.rename_vector_set(prefix.as_slice(), old_key, new_key);
    // AOF 合成条目（主存先行语义，入队失败不回滚已生效的 RENAME）
    vm.replicate_vector_set_rename(prefix.as_slice(), old_key, new_key);
  }
  if nx {
    output.write_resp_int(1);
  } else {
    write_raw(output, cs::RESP_OK);
  }
  Ok(())
}

/// 键管理族慢路径执行段入口（exec_slow 分派；`Err(())` 为存储错误，
/// 调用方统一应答）
pub(crate) async fn key_admin_slow(
  storage: &StorageSession<'_, impl wdev::Device>,
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
        if probe_alive_with_registry_async(storage, prefix, key, vector)
          .await
          .map_err(|_| ())?
        {
          exists_count += 1;
        }
      }
      output.write_resp_int(exists_count);
      Ok(())
    }
    C::Ttl | C::Pttl => {
      let key = match parse_state {
        [key] => *key,
        _ => {
          cs::abort_with_wrong_number_of_arguments(
            output,
            if cmd == C::Ttl { "TTL" } else { "PTTL" },
          );
          return Ok(());
        }
      };
      // wkv pttl_ms 已按 ConvertUtils.MillisecondsFromDiffUtcNowTicks 口径
      // 换算毫秒；TTL 秒 = floor(毫秒/1000)，与快路径
      // seconds_from_diff_ticks 的截断嵌套恒等（.NET ticks 下
      // floor(floor(x/T_MS)/1000) == floor(x/T_S)）
      let value = ttl_read_ms(storage, key, false).await?;
      output.write_resp_int(if cmd == C::Ttl {
        value.div_euclid(1000)
      } else {
        value
      });
      Ok(())
    }
    C::Expiretime | C::Pexpiretime => {
      let key = match parse_state {
        [key] => *key,
        _ => {
          cs::abort_with_wrong_number_of_arguments(output, "EXPIRETIME");
          return Ok(());
        }
      };
      // 绝对 Unix 毫秒 → 秒同款嵌套截断恒等（快路径
      // unix_time_in_seconds_from_ticks）
      let value = ttl_read_ms(storage, key, true).await?;
      output.write_resp_int(if cmd == C::Expiretime {
        value.div_euclid(1000)
      } else {
        value
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
      // :1（含 Redis 7.4 立即删除语义）；头部粗化对命令边界粗化值幂等恒等。
      // 经包装而非裸调 batch.expire_at：applied > 0 的 WATCH 版本推进尾巴
      // 与 AOF 重放端共用一处口径
      let applied = storage
        .expire_at_ticks_opt(key, expire_at_ticks, opt)
        .await
        .map_err(|_| ())?;
      // 判据 applied > 0（快慢路径同态应答的锚点）：wkv expire_at 的 -2
      // （键缺失 / TTL 已过期视同缺失）必须落 :0，与快路径
      // expire_apply_sync 的 Some(0) 臂及 C# status != OK 回 :0 三方对齐；
      // 误用 != 0 会把 -2 误答 :1（应答 :1 而键已消失的用户可见发散）
      write_raw(
        output,
        if applied > 0 {
          cs::RESP_RETURN_VAL_1
        } else {
          cs::RESP_RETURN_VAL_0
        },
      );
      Ok(())
    }
    C::Persist => {
      let key = match parse_state {
        [key] => *key,
        _ => {
          cs::abort_with_wrong_number_of_arguments(output, "PERSIST");
          return Ok(());
        }
      };
      // persist_key 包装（PERSIST 快慢路径与 AOF 重放同一推进尾巴：applied > 0
      // 推进 WATCH 版本，裸调 batch.persist 缺席推进禁再直调）
      let removed = storage.persist_key(key).await.map_err(|_| ())?;
      output.write_resp_int(i64::from(removed));
      Ok(())
    }
    C::Getdel => {
      let key = match parse_state {
        [key] => *key,
        _ => {
          cs::abort_with_wrong_number_of_arguments(output, "GETDEL");
          return Ok(());
        }
      };
      match storage
        .read_user_async(key, |v| v.to_vec())
        .await
        .map_err(|_| ())?
      {
        UserReadAsync::Hit(val) => {
          // 先删后答：删除闭环后才出值，杜绝已答旧值而键未删成
          storage.delete_string(key).await.map_err(|_| ())?;
          output.write_resp_bulk_string(&val);
        }
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
      let (old_key, new_key) = match parse_state {
        [old_key, new_key] => (*old_key, *new_key),
        _ => {
          cs::abort_with_wrong_number_of_arguments(output, cmd_name);
          return Ok(());
        }
      };
      rename_slow(storage, old_key, new_key, nx, vector, output).await
    }
    C::Dump => {
      let key = match parse_state {
        [key] => *key,
        _ => {
          cs::abort_with_wrong_number_of_arguments(output, "DUMP");
          return Ok(());
        }
      };
      // 载荷组装与快路径同形（类型字节 + 长度前缀 + 值 + rdb 版本 + crc64，
      // crc 含类型字节起算保证 DUMP→RESTORE 往返成立）
      let dump = storage
        .read_user_async(key, |value| {
          let mut frame = Vec::with_capacity(value.len() + 16);
          let mut encoded_len = [0u8; 5];
          let Some(bytes_written) = try_write_length(value.len() as u32, &mut encoded_len) else {
            return Err(());
          };
          let encoded_len = &encoded_len[..bytes_written];
          let payload_len = 1 + encoded_len.len() + value.len() + 2 + 8;
          frame.push(b'$');
          let mut buf = Buffer::new();
          frame.extend_from_slice(buf.format(payload_len).as_bytes());
          frame.extend_from_slice(b"\r\n");
          frame.push(0x00);
          frame.extend_from_slice(encoded_len);
          frame.extend_from_slice(value);
          frame.extend_from_slice(&RDB_VERSION.to_le_bytes());
          let crc = hash(&frame[(frame.len() - (payload_len - 8))..]);
          frame.extend_from_slice(&crc);
          frame.extend_from_slice(b"\r\n");
          Ok(frame)
        })
        .await
        .map_err(|_| ())?;
      match dump {
        UserReadAsync::Hit(Ok(frame)) => output.extend_from_slice(&frame),
        UserReadAsync::Hit(Err(())) => {
          write_error_raw(output, "ERR DUMP payload length is invalid");
        }
        // 对象键（信封域命中）与键缺失一致：C# WRONGTYPE → nil 同口径
        UserReadAsync::WrongType | UserReadAsync::Missing => {
          output.write_resp_null_ver(storage.resp_version);
        }
      }
      Ok(())
    }
    C::Restore => {
      let Some((key, expiry, val)) = parse_restore_args(parse_state, output) else {
        return Ok(());
      };
      // SET_Conditional(SETEXNX)：仅键不存在时写入（NX 语义；C# 前置存在判定）。
      // 存活折叠收敛到单点 probe_alive_with_registry_async：与快路径同一
      // 「三域 || 登记表第四态」判据（快路径的第四态半爿由派发层
      // garnet_api::raw 的 vector_registry_gate 先行回 BUSYKEY，本臂是慢路径
      // 承接面的完整折叠），删去两处手抄判定
      if probe_alive_with_registry_async(
        storage,
        storage.batch.session_prefix().as_slice(),
        key,
        vector,
      )
      .await
      .map_err(|_| ())?
      {
        write_error_raw(output, cs::RESP_ERR_BUSSYKEY);
        return Ok(());
      }
      storage.upsert_string(key, val).await.map_err(|_| ())?;
      if expiry > 0 {
        // C#：UtcNow.Ticks + FromSeconds(expiry).Ticks；口径为秒（非 Redis
        // 的毫秒，见快路径头部差异说明）
        let expire_at_ticks = expire_after_to_ticks(now_ticks(), expiry);
        storage
          .batch
          .put_ttl(key, expire_at_ticks)
          .await
          .map_err(|_| ())?;
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
