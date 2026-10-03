//! 集合对象族慢路径承接（哈希 / 集合 / 列表 / 有序集合 / 地理 / 对象扫描 /
//! 对象长度直读 / 对象收集 / HyperLogLog / 自定义对象 / MEMORY USAGE）
//!
//! 各命令族快路径磁盘候选 / 异步 TTL 裁决 / 信封水位越线降级至此，
//! 应答形态与快路径逐字节一致

use wdev::Device;
// 仅扩展特性 Customobjcmd 慢路径臂消费（no-default 剔除防 unused imports）
#[cfg(any(feature = "roaring", feature = "json"))]
use wresp::cmd_strings::RESP_ERR_GENERIC_UNK_CMD;
use wresp::{
  cmd_strings::{
    RESP_ERR_ASYNC_REQUIRED, RESP_ERR_HCOLLECT_ALREADY_IN_PROGRESS, RESP_ERR_SLOW_PATH_STORAGE,
    RESP_ERR_WRONG_TYPE, RESP_ERR_ZCOLLECT_ALREADY_IN_PROGRESS, RESP_OK, RESP_RETURN_VAL_0,
    write_error_raw,
  },
  command::RespCommand,
  ext::RespVecExt,
};
use wval::{GarnetObjectType, KeyTag};

use super::StoreGarnetApi;
// 扩展对象慢路径执行体与静态清单回查：任一扩展特性启用即接线
//（与模块门、分派臂门同侧；清单空表即恒 None，无第二处门控轨）
#[cfg(any(feature = "roaring", feature = "json"))]
use crate::resp::{custom_objects, objects::custom_object_commands::custom_object_slow};
use crate::{
  resp::{
    garnet_api::{
      CollectLockGuard,
      objects::{collect_hash_key, collect_sorted_set_key, object_collect_all},
    },
    objects::{
      hash_commands, list_commands,
      object_store_utils::{ObjLoad, envelope_heap_estimate, obj_length_async},
      rmw_helpers::envelope_length_correct,
      set_commands, shared_object_commands, sorted_set_commands, sorted_set_geo_commands,
      tiered_collection_ops::exec_tiered_collect,
    },
  },
  storage::session::storage_session::StorageSession,
  types::GarnetStatus,
};

impl<D: Device> StoreGarnetApi<D> {
  /// 阻塞族慢路径等待面装配（列表 LBLPOP/BLMPOP 与有序集合 BZPOPMIN/
  /// BZPOPMAX/BZMPOP 两族同形骨架单源：经纪注入域 + 会话逻辑域快照，冷键
  /// 装载未取到时在执行域内联等待出件，C# BlockingWait 的 compio 投影；
  /// 未注入经纪即 None，各臂走非阻塞形态）
  #[inline]
  fn block_wait_face(&self) -> Option<list_commands::slow::BlockWaitFace<'_>> {
    let broker = self.item_broker_wait()?;
    Some(list_commands::slow::BlockWaitFace {
      broker,
      domain: (self.session.namespace(), self.session.active_db()),
    })
  }

  /// MEMORY USAGE 慢路径闭环执行段（C# BasicCommands.NetworkMemoryUsage 的
  /// RECORD_ON_DISK 分支：快路径磁盘候选/异步 TTL 裁决降级至此，双域尺寸
  /// 统计与 RespServerSession::network_memory_usage 同口径）
  pub(super) async fn memory_usage_command_slow(
    &self,
    storage: &StorageSession<'_, D>,
    refs: &[&[u8]],
    resp_version: u8,
  ) -> Vec<u8> {
    let mut output = Vec::new();
    let key = refs.first().copied().unwrap_or(&[]);
    match storage
      .read_tag_with_size(key, KeyTag::String, |_v, size| size)
      .await
    {
      Ok(Some(size)) => output.write_resp_int(size as i64),
      // String 域缺失：探对象信封域（尺寸 + 内层对象堆内存估算）
      Ok(None) => match storage
        .read_tag_with_size(key, KeyTag::ObjectEnvelope, |raw, size| {
          size as i64 + envelope_heap_estimate(raw)
        })
        .await
      {
        Ok(Some(total)) => output.write_resp_int(total),
        // 信封域缺失：探升阶 Meta 域（元记录物理尺寸 + 活跃树常驻页环
        // 容量，与快路径 network_memory_usage 的 Meta 臂同口径——页环整块
        // 常驻必须计入，冷态未打开回 0 不虚报）
        Ok(None) => match storage
          .read_tag_with_size(key, KeyTag::Meta, |_v, size| {
            size as i64 + storage.batch.live_tree_cache_bytes(key) as i64
          })
          .await
        {
          Ok(Some(total)) => output.write_resp_int(total),
          Ok(None) => output.write_resp_null_ver(resp_version),
          Err(_) => err_frame!(output),
        },
        Err(_) => err_frame!(output),
      },
      Err(_) => err_frame!(output),
    }
    output
  }

  /// 集合对象长度异步直读闭环执行段（HLEN/SCARD/ZCARD/LLEN 慢路径承接：
  /// 同步段磁盘候选/异步 TTL 裁决/信封水位越线降级至此，头部直读计数
  /// O(1) 零反序列化）
  pub(super) async fn objlen_command_slow(
    &self,
    storage: &StorageSession<'_, D>,
    cmd: RespCommand,
    refs: &[&[u8]],
  ) -> Vec<u8> {
    use RespCommand as C;

    let mut output = Vec::new();
    let key = refs.first().copied().unwrap_or(&[]);
    let tag = match cmd {
      C::Hlen => GarnetObjectType::Hash,
      C::Scard => GarnetObjectType::Set,
      C::Zcard => GarnetObjectType::SortedSet,
      C::Llen => GarnetObjectType::List,
      _ => unreachable!(),
    };
    match obj_length_async(storage, key, tag, &mut output).await {
      Ok(ObjLoad::Present(len)) => output.write_resp_int(len as i64),
      Ok(ObjLoad::Missing) => output.extend_from_slice(RESP_RETURN_VAL_0),
      Ok(ObjLoad::Degrade) => {
        // 分层键字段级 TTL 水位命中：树内收集执行体校正（到期成员物理
        // 出账 + meta 回写）后直读，两态计数同口径；信封键（Meta 缺位）
        // 水位越线落物化矫正臂（堆序惰性剔除 + 升格写回/删空自愈）
        match exec_tiered_collect(&storage.batch, key, tag).await {
          Ok(Some(len)) => output.write_resp_int(len as i64),
          Ok(None) => match envelope_length_correct(storage, key, tag, &mut output).await {
            Ok(Some(len)) => output.write_resp_int(len as i64),
            Ok(None) => output.extend_from_slice(RESP_RETURN_VAL_0),
            Err(()) => err_frame!(output),
          },
          Err(()) => err_frame!(output),
        }
      }
      Ok(ObjLoad::WrongType) => {}
      Err(_) => err_frame!(output),
    }
    output
  }

  /// 对象收集 `*` 全库族执行段（C# AdminCommands.NetworkHCOLLECT /
  /// NetworkZCOLLECT：C# ObjectCollect 游标分批 + 批内逐键 RMW 的慢路径承接；
  /// 信封域与分层态两域匹配键同批收齐，分层键经树内收集执行体物理出账）
  pub(super) async fn collect_command_slow(
    &self,
    storage: &StorageSession<'_, D>,
    cmd: RespCommand,
    refs: &[&[u8]],
  ) -> Vec<u8> {
    use RespCommand as C;

    let mut output = Vec::new();
    let is_all = refs.first().copied() == Some(b"*");
    let is_hash = matches!(cmd, C::Hcollect);
    // C# ObjectCollect("*") 互斥（Common.cs:810 collectLock.TryWriteLock 失败
    // 回 NOTFOUND → 网络层 default 分支回 already-in-progress）：CAS 抢占
    // 单写位，在途即拒绝；扫描段结束释放。C# StorageSession.Dispose 的
    // Thread.Yield 自旋等锁由 Arc 所有权天然承担（exec_slow future 持
    // Arc 克隆，扫描完成才释放）
    let in_progress = if is_hash {
      &self.hcollect_in_progress
    } else {
      &self.zcollect_in_progress
    };
    // None = OK，Some = 错误文案
    let scan: Result<(), &'static str> = if is_all {
      // C# ObjectCollect("*") 互斥（Common.cs:810 collectLock.TryWriteLock
      // 失败回 NOTFOUND → 网络层 default 分支回 already-in-progress）：
      // CAS 抢占单写位，在途即拒绝；RAII 守卫在扫描段结束或 panic unwind 时
      // 自动释放（对标 C# finally WriteUnlock）
      match CollectLockGuard::try_acquire(in_progress) {
        None => Err(if is_hash {
          RESP_ERR_HCOLLECT_ALREADY_IN_PROGRESS
        } else {
          RESP_ERR_ZCOLLECT_ALREADY_IN_PROGRESS
        }),
        Some(_guard) => {
          // 独立收集会话（域对齐观察者物理域快照；C# ObjectCollect 以独立
          // 扫描 StorageSession 承接）：收集轮逐批短批窗口在 object_collect_all
          // 内自管，连接会话批守卫不得横跨全库收集轮（整轮钉死会话公布纪元，
          // safe_head/closed_until 排空屏障停摆，前台写 evict 等待面连锁停摆）。
          // 快照域直设形态在轮内固着，并发 FLUSHDB 换号时本轮作用于旧域空集
          // 自然收敛，下轮重扫新域
          let (vns, vdb) = storage.batch.session.virtual_domain();
          let res = match storage.batch.session.store.new_session() {
            Ok(session) => {
              // 观察者会话持逻辑域真值，直设收集会话即显式透传（版本轨=
              // 逻辑域种子，无需换算——源即真值）
              let (lns, ldb) = (
                storage.batch.session.namespace(),
                storage.batch.session.active_db(),
              );
              session.set_virtual_context(vns, vdb, lns, ldb);
              object_collect_all(session, is_hash).await
            }
            Err(_) => Err(RESP_ERR_SLOW_PATH_STORAGE),
          };
          drop(_guard);
          res
        }
      }
    } else {
      let mut wrong_type = false;
      for &key in refs {
        let res = if is_hash {
          collect_hash_key(storage, key).await
        } else {
          collect_sorted_set_key(storage, key).await
        };
        match res {
          Ok(GarnetStatus::WrongType) => wrong_type = true,
          Ok(_) => {}
          // 显式键形态无单写位可释放，错误直接闭环应答
          Err(_) => bail_frame!(output),
        }
      }
      if wrong_type {
        Err(RESP_ERR_WRONG_TYPE)
      } else {
        Ok(())
      }
    };
    match scan {
      Ok(()) => output.extend_from_slice(RESP_OK),
      Err(err) => write_error_raw(&mut output, err),
    }
    output
  }

  /// 自定义对象命令族执行段（C# CustomRespCommands.TryCustomObjectCommand
  /// 的异步承接：同步段磁盘候选降级重放。快照尾参为命令名，经与快路径
  /// 解析、ACL 校验同一张编译期静态清单回查后按同款四接口执行；
  /// 未启用任一扩展时清单为空表 → 未知命令兜底臂）
  #[cfg(any(feature = "roaring", feature = "json"))]
  pub(super) async fn custom_object_command_slow(
    &self,
    storage: &StorageSession<'_, D>,
    refs: &[&[u8]],
  ) -> Vec<u8> {
    let mut output = Vec::new();
    let Some((name, cmd_refs)) = refs.split_last() else {
      bail_frame!(output, RESP_ERR_ASYNC_REQUIRED);
    };
    let Some((entry, meta)) = custom_objects::match_custom_object_command(name) else {
      bail_frame!(output, RESP_ERR_GENERIC_UNK_CMD);
    };
    slow_arm!(
      output,
      custom_object_slow,
      storage,
      entry.tag,
      &meta,
      cmd_refs
    );
    output
  }

  /// HyperLogLog 族慢路径承接执行段（C# HyperLogLogOps.HyperLogLogAdd/
  /// Length/Merge 的 RMW 语义：Tsavorite 磁盘候选挂起 pending 读后重放，
  /// NOTFOUND 才允许新建。快路径 load_hll 磁盘候选降级至此：异步
  /// read_tag_with 闭环冷区装载后再 RMW，杜绝把降级信号当缺失盲插覆盖）
  pub(super) async fn hll_command_slow(
    &self,
    storage: &StorageSession<'_, D>,
    cmd: RespCommand,
    refs: &[&[u8]],
  ) -> Vec<u8> {
    use RespCommand as C;

    use crate::resp::hyperloglog::hyper_log_log_commands::{
      slow_hll_add, slow_hll_count, slow_hll_merge,
    };

    let mut output = Vec::new();
    let closed = match cmd {
      C::Pfadd => slow_hll_add(storage, refs, &mut output).await,
      C::Pfcount => slow_hll_count(storage, refs, &mut output).await,
      _ => slow_hll_merge(storage, refs, &mut output).await,
    };
    if closed.is_err() {
      err_frame!(output);
    }
    output
  }

  /// 哈希族执行段（HashCommands.cs 慢路径承接；HSCAN 走对象扫描族；
  /// HLEN 由 O(1) 计数直读臂承接）
  pub(super) async fn hash_command_slow(
    &self,
    storage: &StorageSession<'_, D>,
    cmd: RespCommand,
    refs: &[&[u8]],
  ) -> Vec<u8> {
    let mut output = Vec::new();
    slow_arm!(output, hash_commands::slow::hash, storage, cmd, refs);
    output
  }

  /// 集合族执行段（SetCommands.cs 慢路径承接；SSCAN 走对象扫描族；
  /// SCARD 由 O(1) 计数直读臂承接）
  pub(super) async fn set_command_slow(
    &self,
    storage: &StorageSession<'_, D>,
    cmd: RespCommand,
    refs: &[&[u8]],
  ) -> Vec<u8> {
    let mut output = Vec::new();
    let vector = self.vector_mgr();
    slow_arm!(output, set_commands::slow::set, storage, vector, cmd, &refs);
    output
  }

  /// 列表族执行段（ListCommands.cs 慢路径承接；写回后唤醒阻塞观察者；
  /// LSCAN 形态走对象扫描族）
  pub(super) async fn list_command_slow(
    &self,
    storage: &StorageSession<'_, D>,
    cmd: RespCommand,
    refs: &[&[u8]],
  ) -> Vec<u8> {
    let mut output = Vec::new();
    let notify = |key: &[u8]| self.notify_collection_update(key);
    let block = self.block_wait_face();
    slow_arm!(
      output,
      list_commands::slow::list,
      storage,
      &notify,
      block.as_ref(),
      cmd,
      &refs,
    );
    output
  }

  /// 有序集合族执行段（SortedSetCommands.cs 慢路径承接；写回后唤醒阻塞
  /// 观察者；ZSCAN 走对象扫描族）
  pub(super) async fn sorted_set_command_slow(
    &self,
    storage: &StorageSession<'_, D>,
    cmd: RespCommand,
    refs: &[&[u8]],
  ) -> Vec<u8> {
    let mut output = Vec::new();
    let notify = |key: &[u8]| self.notify_collection_update(key);
    let block = self.block_wait_face();
    slow_arm!(
      output,
      sorted_set_commands::slow::sorted_set,
      storage,
      &notify,
      block.as_ref(),
      cmd,
      &refs,
    );
    output
  }

  /// 地理族执行段（SortedSetGeoCommands.cs 慢路径承接）
  pub(super) async fn geo_command_slow(
    &self,
    storage: &StorageSession<'_, D>,
    cmd: RespCommand,
    refs: &[&[u8]],
  ) -> Vec<u8> {
    let mut output = Vec::new();
    slow_arm!(
      output,
      sorted_set_geo_commands::slow::geo,
      storage,
      cmd,
      &refs
    );
    output
  }

  /// 对象扫描族执行段（C# SharedObjectCommands.ObjectScan 慢路径承接；
  /// 快照尾参为 4 字节 LE COUNT 上限，exec 降级快照追加）
  pub(super) async fn object_scan_command_slow(
    &self,
    storage: &StorageSession<'_, D>,
    cmd: RespCommand,
    refs: &[&[u8]],
  ) -> Vec<u8> {
    use RespCommand as C;

    let mut output = Vec::new();
    let (Some(limit_bytes), scan_args) =
      (refs.last().copied(), &refs[..refs.len().saturating_sub(1)])
    else {
      bail_frame!(output);
    };
    let Ok(limit_arr) = <[u8; 4]>::try_from(limit_bytes) else {
      bail_frame!(output);
    };
    let limit = i32::from_le_bytes(limit_arr);
    let proto = storage.resp_version;
    let res = match cmd {
      C::Coscan => {
        shared_object_commands::slow::coscan(storage, scan_args, limit, &mut output).await
      }
      _ => {
        let object_type = match cmd {
          C::Hscan => GarnetObjectType::Hash,
          C::Sscan => GarnetObjectType::Set,
          _ => GarnetObjectType::SortedSet,
        };
        shared_object_commands::slow::object_scan(
          storage,
          scan_args,
          object_type,
          limit,
          proto,
          &mut output,
        )
        .await
      }
    };
    if res.is_err() {
      err_frame!(output);
    }
    output
  }
}
