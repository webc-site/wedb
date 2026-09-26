//! 慢路径异步分派与通道调度

use std::{
  io,
  sync::{Arc, atomic::Ordering},
};

use compio::runtime::spawn;
use itoa::Buffer;
use wbase::{glob::glob_match_nocase, num::parse_db_index};
use wdev::Device;
use wresp::{
  cmd_strings::{
    RESP_ERR_ASYNC_REQUIRED, RESP_ERR_CHECKPOINT_ALREADY_IN_PROGRESS,
    RESP_ERR_DEVICE_CONTAMINATED_REFUSING_CHECKPOINT, RESP_ERR_DEVICE_CONTAMINATED_REFUSING_FLUSH,
    RESP_ERR_FLUSH_TRUNCATE_LOG_NS0, RESP_ERR_GENERIC_SYNTAX_ERROR, RESP_ERR_GENERIC_UNK_CMD,
    RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, RESP_ERR_HCOLLECT_ALREADY_IN_PROGRESS,
    RESP_ERR_SLOW_PATH_STORAGE, RESP_ERR_SWAPDB_UNSUPPORTED, RESP_ERR_WRONG_TYPE,
    RESP_ERR_ZCOLLECT_ALREADY_IN_PROGRESS, RESP_OK, RESP_RETURN_VAL_0, write_error_raw,
  },
  command::RespCommand,
  ext::RespVecExt,
  metrics::InfoMetricsType,
};
use wval::{GarnetObjectType, KeyTag};

use super::{
  StoreGarnetApi,
  objects::{collect_hash_key, collect_sorted_set_key, object_collect_all},
};
// 扩展对象慢路径执行体与静态清单回查：任一扩展特性启用即接线
//（与模块门、分派臂门同侧；清单空表即恒 None，无第二处门控轨）
#[cfg(any(feature = "roaring", feature = "json"))]
use crate::resp::{custom_objects, objects::custom_object_commands::custom_object_slow};
use crate::{
  database::IDatabaseManager,
  resp::{
    MsetnxResume, RespServerSession, array_commands as array_cmds,
    basic_commands::{
      parse_flush_options,
      slow::{bitmap_slow, string_slow},
    },
    basic_etag_commands::etag_slow,
    info_provider::InfoScanResult,
    key_admin_commands::slow::key_admin_slow,
    objects::{
      hash_commands, list_commands,
      object_store_utils::{ObjLoad, envelope_heap_estimate, obj_length_async},
      rmw_helpers::envelope_length_correct,
      set_commands, shared_object_commands, sorted_set_commands, sorted_set_geo_commands,
      tiered_collection_ops::exec_tiered_collect,
    },
    range_index::resp_server_session_range_index as ri_cmds,
    vector::vector_manager::VectorManager,
  },
  servers::consumer_registry::ConsumerRegistry,
  storage::session::{
    common::{
      array_key_iteration_functions::try_push_key,
      ttl_sync::{probe_alive_with_registry, probe_alive_with_registry_async},
    },
    storage_session::StorageSession,
  },
  types::GarnetStatus,
};

/// 检查点通道未装配（宿主未注入 [`super::CheckpointCtx`]）时的显式拒绝文案
const RESP_ERR_CHECKPOINT_UNWIRED: &str = "ERR checkpoint channel not configured";

/// 慢路径臂错误帧单源成帧：`$out` 为本臂应答缓冲局部名（hygiene 显式传入），
/// `$frame` 缺省为本域同源存储错误帧——帧字节与文案与既有各臂逐字节一致
macro_rules! err_frame {
  ($out:ident) => {
    write_error_raw(&mut $out, RESP_ERR_SLOW_PATH_STORAGE)
  };
  ($out:ident, $frame:expr) => {
    write_error_raw(&mut $out, $frame)
  };
}

/// 补写错误帧并立即出帧（本臂应答已定，不再续跑后续段）
macro_rules! bail_frame {
  ($out:ident) => {{
    err_frame!($out);
    return $out;
  }};
  ($out:ident, $frame:expr) => {{
    err_frame!($out, $frame);
    return $out;
  }};
}

/// 慢路径执行臂统一收尾：各臂执行体一律以 `(执行域, …参数…, 应答缓冲)` 同形
/// 签名收末参，本宏补齐 `&mut $out` 实参并 await——执行体以 `Err`（存储降级 /
/// 落盘失败）收场即补同源存储错误帧，即「调执行体 → 写回帧」骨架的单源
macro_rules! slow_arm {
  ($out:ident, $exec:path, $($arg:expr),+ $(,)?) => {{
    if $exec($($arg),+, &mut $out).await.is_err() {
      err_frame!($out);
    }
  }};
}

/// 活跃会话数（对标 C# `GarnetServerBase.ActiveConsumers()` 计数，注册表未
/// 装配的无服务器形态即 0）。SWAPDB 换库门控与管理命令同频读取，非热路径
fn active_session_count() -> usize {
  ConsumerRegistry::global().map_or(0, |r| r.active_consumers().len())
}

impl<D: Device> StoreGarnetApi<D> {
  /// 向量登记表句柄（存在性四态探针与各对象族慢路径臂共用的单点取用形态，
  /// 未装配向量域的嵌入式形态即 None）
  #[inline]
  pub(super) fn vector_mgr(&self) -> Option<&VectorManager> {
    self.vector_session.as_ref().map(|v| v.manager.as_ref())
  }

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

  /// 登记表域内用户键投影合并（本库向量键；剥域后与存储键同域直推，天然互斥免判重）
  ///
  /// 页上界单源（票 zcode-r147c-hscanmt 案二收敛口径）：`remaining` 为本页
  /// 剩余额度（SCAN 首页为 `limit` 页上界与存储域已收数之差、KEYS 全量臂为
  /// `usize::MAX`），只补投至满额即止不加码——向量登记表域不经游标递进，
  /// 首页满额后的余项按截断收场，不为续投另立第二套游标位段（跨页续投无
  /// 损形不可达成，禁加码为既定裁决，doc/zh/db.md 剥前缀段在册）。
  ///
  /// 分配收口单源：逐键经 [`try_push_key`] 平滑追加（`try_reserve` 前置，
  /// 失败沿 Err 走两臂既有 `RESP_ERR_SLOW_PATH_STORAGE` 单会话错误帧漏斗，
  /// 与 scan_cursor/db_keys 同纪律，杜绝裸 push 扩容触顶
  /// `handle_alloc_error` abort 全进程，票 zcode-r55-alloc）。
  #[inline]
  fn merge_vector_keys(
    &self,
    pattern: &[u8],
    keys: &mut Vec<Vec<u8>>,
    remaining: usize,
  ) -> io::Result<()> {
    let Some(vectors) = &self.vector_session else {
      return Ok(());
    };
    let prefix = self.session.session_prefix();
    let all_keys = pattern == b"*";
    let mut budget = remaining;
    let mut push_err: Option<io::Error> = None;
    vectors
      .manager
      .for_each_domain_user_key(prefix.as_slice(), |k| {
        // 额度耗尽或已折错即止投（登记域照常遍历，零分配零出帧）
        if budget == 0 {
          return;
        }
        if all_keys || glob_match_nocase(pattern, k) {
          match try_push_key(keys, k) {
            Ok(()) => budget -= 1,
            Err(e) => {
              push_err = Some(e);
              budget = 0;
            }
          }
        }
      });
    push_err.map_or(Ok(()), Err)
  }

  /// INFO 扫描族段异步产物（KEYSPACE 逐库键空间计数 / HLOGSCAN 混合日志
  /// 分布扫描 / STOREHASHTABLE 哈希分布诊断扫描 / STOREREVIV 复活统计
  /// 转储）——对标 C# PopulateKeyspaceInfo → GetKeyspaceStats 专用扫描
  /// 会话、PopulateHlogScanInfo → HybridLogDistributionScan、
  /// PopulateStoreHashDistribution → DumpDistribution、PopulateStoreRevivInfo
  /// → DumpRevivificationStats 的逐段实填；rust 存储域扫描须跨 await，扫描
  /// 行在此异步产出，非扫描面经 [`InfoSurface`] 调度点快照（exec_slow 无
  /// 会话可达面同渠道口径），两路由 render_info_slow_reply 合成全段集渲染。
  /// 段集判定单源在上游分派门与漏斗解析（本函数只按需取扫描行，虚库上界
  /// 按会话快照做越界防御）。
  ///
  /// KEYSPACE 段一律走引擎侧单内核 WedbStore::keyspace_stats——只读
  /// 遍历在册库、一趟分桶扫描，连接会话上下文零改动（切库既盲分配虚库
  /// 又落 DbMeta，冷库还会把上一库计数错贴到本库号上）。向量键登记表
  /// 域内增量在本消费点合并（引擎侧 keyspace_stats 纯物理扫描无
  /// VectorManager 访问面，wkv 层不持装配句柄；C# 向量元数据驻留主存随
  /// GetKeyspaceStats 扫描天然计入，libs/server/Storage/Functions/
  /// UnifiedStore/ReadMethods.cs:153 同一记录族——rust 登记表旁挂域经
  /// 逐在册库域前缀计数对位）。向量键 TTL 族 b 案不支持（ttl_read_sync
  /// 头注同口径），expires 栏不加
  pub(super) async fn info_scan_slow(
    &self,
    sections: &[InfoMetricsType],
    max_databases: u64,
  ) -> InfoScanResult {
    let mut scan = InfoScanResult::default();
    if sections.contains(&InfoMetricsType::Keyspace) {
      match self
        .session
        .store()
        .keyspace_stats(self.session.namespace())
        .await
      {
        Ok(rows) => {
          let vec_counts = self.vector_domain_counts();
          // C# PopulateKeyspaceInfo 仅列出至少持有一个键的库，零键库行与
          // 虚库号越界者一并剔除
          scan.keyspace = rows
            .into_iter()
            .map(|(db, keys, expires)| {
              let keys = keys + vec_counts.get(&db).copied().unwrap_or(0) as u64;
              (db, keys, expires)
            })
            .filter(|&(db, keys, _)| db < max_databases && keys > 0)
            .map(|(db, keys, expires)| (db as i32, keys, expires))
            .collect();
        }
        Err(_) => scan.storage_failed = true,
      }
    }
    if scan.storage_failed {
      return scan;
    }
    // HLOGSCAN 段：经数据库管理面拉取混合日志内存分布扫描
    //（C# PopulateHlogScanInfo → HybridLogDistributionScan；wedb 单
    // 物理日志 + wcol 信封统一值域，统计由 db 0 形态呈现，对象存储
    // 槽恒空——无独立对象存储域。管理面未装配的嵌入式形态转储为空，
    // 段按 wmetric 缺省形态呈现 Empty；扫描失败与 KEYSPACE 段同款
    // 置 storage_failed——不静默降级空转储，客户端可区分空日志与
    // 存储故障）
    if sections.contains(&InfoMetricsType::HlogScan)
      && let Some(mgr) = self
        .checkpoint
        .as_ref()
        .map(|c| Arc::clone(&c.database_manager))
    {
      match mgr.collect_hybrid_log_stats().await {
        Ok(stats) => {
          scan.hlog_dump = stats
            .into_iter()
            .map(|(_, m)| m.dump_scan_metrics_info())
            .collect();
        }
        Err(_) => scan.storage_failed = true,
      }
    }
    if scan.storage_failed {
      return scan;
    }
    // STOREHASHTABLE / STOREREVIV 段：哈希索引分布直方图（O(桶数)
    // 纯内存诊断扫描，C# PopulateStoreHashDistribution → DumpDistribution）
    // 与复活池四计数转储（O(1)，C# PopulateStoreRevivInfo →
    // DumpRevivificationStats）。wedb 单物理存储，db 0 形态呈现；
    // 纯内存读无失败面，不引入 KEYSPACE 段同款回错分支
    if sections.contains(&InfoMetricsType::StoreHashtable) {
      scan
        .hash_dump
        .push(self.session.store().hash_distribution_dump());
    }
    if sections.contains(&InfoMetricsType::StoreReviv) {
      scan
        .reviv_dump
        .push(self.session.store().revivification_dump());
    }
    scan
  }

  pub(crate) async fn exec_slow_impl(
    &self,
    cmd: RespCommand,
    args: Vec<Vec<u8>>,
    resp_version: u8,
  ) -> Vec<u8> {
    use RespCommand as C;

    use crate::resp::array_commands::parse_scan_filter;

    // 检查点 / AOF 提交族须在会话纪元保护区外发起（检查点 fail-fast 契约：
    // 批处理纪元守卫内自钉纪元会令排空屏障谓词永假；AOF 提交同理不得在
    // 存储写纪元内触发刷盘级联），先于 batch 域闭环
    if matches!(
      cmd,
      RespCommand::Save | RespCommand::Bgsave | RespCommand::Lastsave | RespCommand::Commitaof
    ) {
      return self.checkpoint_command_slow(cmd, &args).await;
    }

    // DEBUG 慢路径（FLUSHANDEVICT 涉及刷盘与日志地址推进，在 batch 纪元保护区外执行）
    if cmd == RespCommand::Debug {
      return self.debug_command_slow(&args).await;
    }

    // 清库族慢路径（FLUSHALL 物理截断与 AOF 截断涉及日志地址推进与纪元排空，在 batch 纪元保护区外执行）
    if matches!(cmd, RespCommand::Flushdb | RespCommand::Flushall) {
      return self.flush_command_slow(cmd, &args).await;
    }

    // 跨库交换慢路径（涉及新会话创建、DbMeta 写入与全局纪元刷新，在 batch 纪元保护区外执行）
    if cmd == RespCommand::Swapdb {
      return self.swap_command_slow(&args).await;
    }

    let refs: Vec<&[u8]> = args.iter().map(Vec::as_slice).collect();
    // 慢路径执行域：独立批处理纪元 + 只读扫描会话（慢命令不登记/推进
    // WATCH，独立版本表与 GarnetDatabase 的共享面语义等价）
    let batch = self.session.enter_batch();
    let storage = StorageSession::new_readonly(batch)
      // 会话指标共享句柄下传（对标 C# storageSession 共持 sessionMetrics 直写
      // 命中/pending 计数；采样关闭为 None 空条件跳过）
      .with_session_metrics(self.session_metrics.clone())
      // PENDING_LAT 计量槽下传（与 sessionMetrics 同轨的延迟注入轨：C#
      // storageSession 共持 LatencyMetrics，pending 闭环据此记 PENDING_LAT；
      // 会话未回挂/延迟监视关闭为 None，零取时零分配）
      .with_pending_latency(self.pending_latency())
      // 版本源单点流入：会话真实协议版本经慢路径入口快照带入存储会话，
      // 对象族慢路径各消费点经 resp_protocol_version() 取真值（修复恒 2）
      .with_resp_version(resp_version);
    let mut output = Vec::new();

    match cmd {
      // ---- 字符串族 / OBJECT 慢路径承接（BasicCommands.cs 各命令同步函数
      // 体内 CompletePending 就地闭环的 rust 慢路径对偶：快路径环形页翻转 /
      // RI 门 / 存在性探针降级至此，解析与快路径同一单源，应答形态逐字节
      // 一致）
      C::Set
      | C::Setex
      | C::Psetex
      | C::Setnx
      | C::Setexnx
      | C::Getset
      | C::Setrange
      | C::Append
      | C::Incr
      | C::Decr
      | C::Incrby
      | C::Decrby
      | C::Incrbyfloat
      | C::Getex
      | C::Getrange
      | C::Substr
      | C::Strlen
      | C::ObjectEncoding
      | C::ObjectFreq
      | C::ObjectIdletime
      | C::ObjectRefcount => {
        slow_arm!(output, string_slow, &storage, cmd, &refs, self.vector_mgr());
      }
      // ---- 位图族慢路径承接（BitmapCommands.cs 同型：读改写回异步闭环，
      // BITFIELD 子命令序列解析单源，BITOP 逐源折叠）
      C::Setbit
      | C::Getbit
      | C::Bitcount
      | C::Bitpos
      | C::BitopAnd
      | C::BitopOr
      | C::BitopXor
      | C::BitopNot
      | C::BitopDiff
      | C::Bitfield
      | C::BitfieldRo => {
        slow_arm!(output, bitmap_slow, &storage, cmd, &refs, self.vector_mgr());
      }
      // ---- 键管理族慢路径承接（KeyAdminCommands.cs：TTL / 存在性 / 迁移
      // 裁决降级至此，wkv 异步判定表与三域探针闭环；RENAME 的 RangeIndex /
      // 升阶键整树快照迁移经 wkv rename_range_index 单点）
      C::Exists
      | C::Expire
      | C::Pexpire
      | C::Expireat
      | C::Pexpireat
      | C::Persist
      | C::Ttl
      | C::Pttl
      | C::Expiretime
      | C::Pexpiretime
      | C::Getdel
      | C::Rename
      | C::Renamenx
      | C::Dump
      | C::Restore => {
        let vector = self.vector_mgr();
        slow_arm!(output, key_admin_slow, &storage, cmd, &refs, vector);
      }
      // ---- ETag 族慢路径承接（BasicEtagCommands.cs：六命令快路径失闩 /
      // 磁盘候选降级至此——写臂持本键读改写窗口重放（票 zcode-r32-rmwmatrix
      // 立项二），读共同体异步闭环，应答形态与快路径逐字节一致）
      C::Getwithetag
      | C::Getifnotmatch
      | C::Delifgreater
      | C::Setifmatch
      | C::Setifgreater
      | C::Setwithetag => {
        slow_arm!(output, etag_slow, &storage, cmd, &refs);
      }
      // ---- MSETNX 慢路径承接（C# MSET_Conditional 事务锁内全同步闭环的
      // rust 对偶：前缀外提 + 判定与批量写入折叠单窗口 + 中途失败条件回滚，
      // 全有或全无，应答与最终存储状态恒一致）
      C::Msetnx => {
        // 尾参为快路径续跑模式标记（[`MsetnxResume::from_tail`] 逆解析）：
        // Continue（b"1"）= NX 判定已整体通过、已写键保持，跳过判定续写
        // 全部键回 :1；Rollback（b"r"）= 快路径回滚存在删除降级残留，持窗
        // 条件回滚收尾回 :0；Replay（b"0"）= 判定段降级，先三域异步裁决
        // 存活——任一存活整体不写回 :0（C# EXISTS 非 NOTFOUND 即存在），
        // 全不存活才写入
        let mode = MsetnxResume::from_tail(args.last().map(Vec::as_slice));
        // 剥离尾参后的键值对序列（快路径已校验 arity 非空且偶数）
        let pairs = &refs[..refs.len() - 1];
        if mode == MsetnxResume::Rollback {
          // 回滚收尾模式（票 zcode-r37-lockfix 发现 B）：持全键窗逐键条件
          // 删除——仅删内容即本命令所写的键（并发盲写 SET 抢覆写键的已
          // 确认写保留不删），async 删除闭环无降级残留，回 :0（全有或全
          // 无失败面：残留清完后该语义成立；存储错误留痕可见）
          let Ok(_windows) = storage
            .batch
            .rmw_window_sorted(pairs.as_chunks::<2>().0.iter().map(|c| c[0]))
            .await
          else {
            // 取闩预算耗尽（LockTimeout）按本臂存储错误同源应答
            bail_frame!(output);
          };
          let prefix = storage.batch.session_prefix();
          let prefix_slice = prefix.as_slice();
          for [key, val] in pairs.as_chunks::<2>().0 {
            if matches!(
              storage
                .read_tag_with_prefix(prefix_slice, key, KeyTag::String, |cur| cur == &val[..])
                .await,
              Ok(Some(true))
            ) && let Err(e) = storage.delete_string(key).await
            {
              log::error!("MSETNX 回滚收尾删除失败: {e:?}");
            }
          }
          output.write_resp_int(0);
          return output;
        }
        let resume = mode == MsetnxResume::Continue;
        // 全键读改写窗口（快路径 network_msetnx 同一窗口契约，票
        // zcode-r15-generic 发现一对标 C# MSET_Conditional 全键排他锁）：
        // 桶序取闩无循环等待面，闩内完成判定（含逐键 await 闭环）与批量写
        // 全序列——快慢两路径「判定与写入一体」同一窗口，删去原「逐键 await
        // 窗口内的并发写入属顺序未定义」的自认窗口
        let Ok(_windows) = storage
          .batch
          .rmw_window_sorted(pairs.as_chunks::<2>().0.iter().map(|c| c[0]))
          .await
        else {
          // 取闩预算耗尽（LockTimeout）按本臂存储错误同源应答
          bail_frame!(output);
        };
        // 循环前缀外提（transpile SKILL 工程准则）：判定与写入两段共用同一
        // 外提前缀，零逐键 ns/db 原子变量重读与 Varint 重算
        let prefix = storage.batch.session_prefix();
        let prefix_slice = prefix.as_slice();
        // 登记表句柄：会话侧取用（本档 string_slow 臂同形装配）
        let vector = self.vector_mgr();
        if !resume {
          for key in pairs.iter().step_by(2) {
            // 同步折叠探针先行（快路径 network_msetnx 判定段同款：三域
            // String / ObjectEnvelope / Meta + 向量登记表第四态，票
            // zcode-r161c-msetnx 案一——快臂判定段磁盘候选降级后，派发与
            // 慢臂落笔之间另有 await 宽窗，其间他核 VADD 新提交登记须在本
            // 位判存在，否则 :1 覆写值被值域门永久遮蔽）；Deferred（磁盘
            // 候选 / TTL 待裁决）才逐键异步折叠闭环（TTL 过期键经异步读
            // 惰性清除后视同缺失）。逐键 await 全程持全键窗口，判定与写入一体
            match probe_alive_with_registry(&storage.batch, prefix_slice, key, vector) {
              // 任一键存活：整体零写入回 :0（C# MSET_Conditional error 短路）
              Ok(Some(true)) => {
                output.write_resp_int(0);
                return output;
              }
              Ok(Some(false)) => {}
              Ok(None) => {
                match probe_alive_with_registry_async(&storage, prefix_slice, key, vector).await {
                  Ok(true) => {
                    output.write_resp_int(0);
                    return output;
                  }
                  Ok(false) => {}
                  Err(_) => bail_frame!(output),
                }
              }
              Err(_) => bail_frame!(output),
            }
          }
        }
        // 写入段（全有或全无，与 C# 事务缓冲 + Commit 的可观测语义对齐）：
        // 先单次同步折叠（与快路径 network_msetnx 同一折叠入口，多数场景
        // 零 await 原子窗口闭环）；降级或存储错误时已写键保持，逐键异步兜底
        // 重放全量（重放值与已写键同值幂等；SET 语义自动清新键残留 TTL，
        // 补写模式重写快路径前缀键同值幂等）。任一键 Err 条件回滚涉及键
        //（票 zcode-r37-lockfix 发现 B：仅删内容即本命令所写的键，禁盲删吞
        // 并发已确认写；Continue 续写形态判定由快路径承担，回滚时点不重持
        // 「全键不存在」前提，条件化恰补此洞。删除自身失败留痕可见，错误
        // 同源属尽力极限）
        let fold = pairs.as_chunks::<2>().0.iter().map(|c| (c[0], c[1]));
        match storage.batch.try_upsert_batch_sync(fold) {
          Ok(Ok(())) => {}
          _ => {
            let mut failed = false;
            for [key, val] in pairs.as_chunks::<2>().0 {
              if storage.upsert_string(key, val).await.is_err() {
                failed = true;
                break;
              }
            }
            if failed {
              for [key, val] in pairs.as_chunks::<2>().0 {
                if matches!(
                  storage
                    .read_tag_with_prefix(prefix_slice, key, KeyTag::String, |cur| cur == &val[..])
                    .await,
                  Ok(Some(true))
                ) && let Err(e) = storage.delete_string(key).await
                {
                  log::error!("MSETNX 回滚删除失败: {e:?}");
                }
              }
              bail_frame!(output);
            }
          }
        }
        output.write_resp_int(1);
      }
      // ---- GET 慢路径承接（同步段磁盘候选降级至此：read_string_batch_into 走 wkv
      // 批量读口，TTL 门与逐条读口同源、在批量口内裁决，过期键视同不存在回 nil，
      // 冷命中键经磁盘冷读装载并惰性物理清除（对标 C# InternalRead 的
      // ReadCache/磁盘冷读异步完成路径与 Reader 内 CheckExpiry）；多键时触发流水线
      // 冷读 Scatter-Gather (SG) 异步批量 I/O，对标 C# NetworkGET_SG）
      C::Get => {
        if storage
          .read_string_batch_into(&refs, &mut output)
          .await
          .is_err()
        {
          // 批量读口以 Err 中止时 output 残留首个磁盘候选之前已先行交付的部分帧
          //（wkv `session/raw/batch.rs:read_batch_raw_with` 次序契约：调用方须
          // 整体丢弃本批部分结果）：先彻底丢弃残帧，再按挂起键数逐键各补一条错
          // 误帧——本臂一次承接的是 SG 流水线合并的 N 条 GET（garnet_api/mod.rs
          // 的 sg_batched_keys 快照），每键各是一条独立命令，应答须 N 键 N 帧
          // 严格对齐（对标 C# NetworkGET_SG 逐键独立成帧，BasicCommands.cs:244；
          // 单键 GET 即退化为单帧）
          output.clear();
          for _ in &refs {
            err_frame!(output);
          }
        }
      }
      // ---- HyperLogLog 族慢路径承接（C# HyperLogLogOps.HyperLogLogAdd/
      // Length/Merge 的 RMW 语义：Tsavorite 磁盘候选挂起 pending 读后重放，
      // NOTFOUND 才允许新建。快路径 load_hll 磁盘候选降级至此：异步
      // read_tag_with 闭环冷区装载后再 RMW，杜绝把降级信号当缺失盲插覆盖）
      C::Pfadd | C::Pfcount | C::Pfmerge => {
        use crate::resp::hyperloglog::hyper_log_log_commands::{
          slow_hll_add, slow_hll_count, slow_hll_merge,
        };
        let closed = match cmd {
          C::Pfadd => slow_hll_add(&storage, &refs, &mut output).await,
          C::Pfcount => slow_hll_count(&storage, &refs, &mut output).await,
          _ => slow_hll_merge(&storage, &refs, &mut output).await,
        };
        if closed.is_err() {
          err_frame!(output);
        }
      }
      // ---- 全库扫描族（同步段仅校验，异步段承载实际扫描）
      C::Dbsize => {
        self
          .session
          .set_context(self.session.namespace(), self.session.active_db());
        // 登记表域内计数（换号换库互不串扰；registry 域内单点承接）
        let prefix = self.session.session_prefix();
        match storage.db_size().await {
          Ok(n) => {
            let vec_count = self
              .vector_session
              .as_ref()
              .map_or(0, |v| v.manager.registry_domain_count(prefix.as_slice()));
            output.write_resp_int((n + vec_count) as i64);
          }
          Err(_) => err_frame!(output),
        }
      }
      C::Keys => {
        self
          .session
          .set_context(self.session.namespace(), self.session.active_db());
        let pattern = refs.first().copied().unwrap_or(b"*");
        match storage.db_keys(pattern).await {
          Ok(mut keys) => {
            // KEYS 无分页：全量投影（remaining 无界），仅分配纪律归一
            if self
              .merge_vector_keys(pattern, &mut keys, usize::MAX)
              .is_err()
            {
              bail_frame!(output);
            }
            output.write_resp_array_len(keys.len());
            for key in &keys {
              output.write_resp_bulk_string(key);
            }
          }
          Err(_) => err_frame!(output),
        }
      }
      C::Scan => {
        let filter = match parse_scan_filter(&refs) {
          Ok(f) => f,
          Err(err) => bail_frame!(output, err),
        };
        // 未知 TYPE 值：C# DbScan 对非空未知 typeObject 直接回空列表 +
        // 游标 0（ArrayKeyIterationFunctions.cs:82-84），不触达扫描
        if filter.type_unknown {
          RespServerSession::write_output_for_scan(0, &[], &mut output);
          return output;
        }
        // TYPE 参数出现时单页无上限（C# long.MaxValue 同口径）
        let count = if filter.type_given {
          usize::MAX
        } else {
          filter.count
        };
        match storage
          .scan_cursor(
            &filter.pattern,
            filter.all_keys,
            filter.cursor as u64,
            count,
            filter.type_filter,
          )
          .await
        {
          Ok((cursor, mut keys)) => {
            if filter.cursor == 0 && !filter.type_given {
              // 向量并页臂归 limit 页上界单源：只补投剩余额度，满额截断不加码
              let remaining = count.saturating_sub(keys.len());
              if self
                .merge_vector_keys(&filter.pattern, &mut keys, remaining)
                .is_err()
              {
                bail_frame!(output);
              }
            }
            let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
            RespServerSession::write_output_for_scan(cursor as i64, &key_refs, &mut output)
          }
          Err(_) => err_frame!(output),
        }
      }
      // ---- INFO 族不在此表：凡段集含扫描族段的 INFO 请求由会话分派门
      // 整请求降级，经 exec_slow_info 类型化调度参数直达组合源渲染
      //（见 garnet_api mod.rs 漏斗与 info_provider 组合数据源）
      // ---- 清库族（libs/server/Resp/BasicCommands.cs:ExecuteFlushDb：选项单源重解析 +
      // O(1) 虚拟 ID 换号秒清；FLUSHDB 清会话当前库——C# FlushDatabase(activeDbId)，
      // FLUSHALL 清全部库——C# FlushAllDatabases 逐活跃库 FlushDatabase）
      // ---- MEMORY USAGE 慢路径闭环（C# BasicCommands.NetworkMemoryUsage 的
      // RECORD_ON_DISK 分支：快路径磁盘候选/异步 TTL 裁决降级至此，双域尺寸
      // 统计与 RespServerSession::network_memory_usage 同口径）
      C::MemoryUsage => {
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
      }
      // ---- 集合对象长度异步直读闭环（HLEN/SCARD/ZCARD/LLEN 慢路径承接：
      // 同步段磁盘候选/异步 TTL 裁决/信封水位越线降级至此，头部直读计数
      // O(1) 零反序列化）
      C::Hlen | C::Scard | C::Zcard | C::Llen => {
        let key = refs.first().copied().unwrap_or(&[]);
        let tag = match cmd {
          C::Hlen => GarnetObjectType::Hash,
          C::Scard => GarnetObjectType::Set,
          C::Zcard => GarnetObjectType::SortedSet,
          C::Llen => GarnetObjectType::List,
          _ => unreachable!(),
        };
        match obj_length_async(&storage, key, tag, &mut output).await {
          Ok(ObjLoad::Present(len)) => output.write_resp_int(len as i64),
          Ok(ObjLoad::Missing) => output.extend_from_slice(RESP_RETURN_VAL_0),
          Ok(ObjLoad::Degrade) => {
            // 分层键字段级 TTL 水位命中：树内收集执行体校正（到期成员物理
            // 出账 + meta 回写）后直读，两态计数同口径；信封键（Meta 缺位）
            // 水位越线落物化矫正臂（堆序惰性剔除 + 升格写回/删空自愈）
            match exec_tiered_collect(&storage.batch, key, tag).await {
              Ok(Some(len)) => output.write_resp_int(len as i64),
              Ok(None) => match envelope_length_correct(&storage, key, tag, &mut output).await {
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
      }
      // ---- 对象收集 `*` 全库族（C# AdminCommands.NetworkHCOLLECT /
      // NetworkZCOLLECT：C# ObjectCollect 游标分批 + 批内逐键 RMW 的慢路径承接；
      // 信封域与分层态两域匹配键同批收齐，分层键经树内收集执行体物理出账）
      C::Hcollect | C::Zcollect => {
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
          // CAS 抢占单写位，在途即拒绝；扫描段结束释放（错误也不提前返回
          // 外层，保证释放）
          if in_progress
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
          {
            Err(if is_hash {
              RESP_ERR_HCOLLECT_ALREADY_IN_PROGRESS
            } else {
              RESP_ERR_ZCOLLECT_ALREADY_IN_PROGRESS
            })
          } else {
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
            in_progress.store(false, Ordering::Release);
            res
          }
        } else {
          let mut wrong_type = false;
          for &key in &refs {
            let res = if is_hash {
              collect_hash_key(&storage, key).await
            } else {
              collect_sorted_set_key(&storage, key).await
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
      }
      // ---- 自定义对象命令族（C# CustomRespCommands.TryCustomObjectCommand
      // 的异步承接：同步段磁盘候选降级重放。快照尾参为命令名，经与快路径
      // 解析、ACL 校验同一张编译期静态清单回查后按同款四接口执行；
      // 未启用任一扩展时清单为空表 → 未知命令兜底臂）
      #[cfg(any(feature = "roaring", feature = "json"))]
      C::Customobjcmd => {
        let Some((name, cmd_refs)) = refs.split_last() else {
          bail_frame!(output, RESP_ERR_ASYNC_REQUIRED);
        };
        let Some((entry, meta)) = custom_objects::match_custom_object_command(name) else {
          bail_frame!(output, RESP_ERR_GENERIC_UNK_CMD);
        };
        slow_arm!(
          output,
          custom_object_slow,
          &storage,
          entry.tag,
          &meta,
          cmd_refs
        );
      }
      // ---- RangeIndex 族（resp_server_session_range_index.rs：解析校验与
      // 执行一体在异步段闭环；ri 门取存储域共享范围索引管理器）
      C::Ricreate
      | C::Riset
      | C::Riget
      | C::Ridel
      | C::Riscan
      | C::Rirange
      | C::Riexists
      | C::Riconfig
      | C::Ricount
      | C::Rimetrics => {
        // 解析校验 + 执行一体闭环；应答直写 output（错误映射在处理器内）。
        // C# EnableRangeIndexPreview 预览门在 rust 恒开（deviations 登记）
        let _ = match cmd {
          C::Ricreate => ri_cmds::network_ricreate(&refs, &self.session, &mut output).await,
          C::Riset => ri_cmds::network_riset(&refs, &self.session, &mut output).await,
          C::Riget => ri_cmds::network_riget(&refs, &self.session, resp_version, &mut output).await,
          C::Ridel => ri_cmds::network_ridel(&refs, &self.session, &mut output).await,
          C::Riscan => ri_cmds::network_riscan(&refs, &self.session, &mut output).await,
          C::Rirange => ri_cmds::network_rirange(&refs, &self.session, &mut output).await,
          C::Riexists => ri_cmds::network_riexists(&refs, &self.session, &mut output).await,
          C::Riconfig => ri_cmds::network_riconfig(&refs, &self.session, &mut output).await,
          C::Ricount => ri_cmds::network_ricount(&refs, &self.session, &mut output).await,
          _ => ri_cmds::network_rimetrics(&refs, &self.session, &mut output).await,
        };
      }
      // ---- 过期键删除扫描（C# AdminCommands.NetworkEXPDELSCAN：C# 阻塞
      // 等待 storeWrapper.ExpiredKeyDeletionScan；应答 *2 计数对
      // `*2\r\n$N\r\n<expired>\r\n$N\r\n<scanned>\r\n`，DBID 已在快
      // 路径 try_parse_database_id 校验，此处防御性重解析）
      C::Expdelscan => {
        let db_id = match refs.first() {
          None => None,
          Some(arg) => match parse_db_index(arg) {
            Ok(idx) => Some(idx as u64),
            Err(_) => bail_frame!(output, RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER),
          },
        };
        match self.session.store.expired_key_deletion_scan(db_id).await {
          Ok((expired, scanned)) => {
            let mut buf = Buffer::new();
            output.write_resp_array_len(2);
            output.write_resp_bulk_string(buf.format(expired).as_bytes());
            output.write_resp_bulk_string(buf.format(scanned).as_bytes());
          }
          Err(_) => err_frame!(output),
        }
      }
      // ---- TYPE / LCS 慢路径承接（C# ArrayCommands.NetworkTYPE / NetworkLCS
      // 的 Read_UnifiedStore pending 就地闭环 rust 对偶：快路径三域判型 /
      // 双键读磁盘候选降级至此，应答形态与快路径逐字节一致）
      C::Type => {
        let vector = self.vector_mgr();
        slow_arm!(output, array_cmds::slow::type_cmd, &storage, vector, &refs);
      }
      C::Lcs => {
        slow_arm!(output, array_cmds::slow::lcs, &storage, &refs, resp_version);
      }
      // ---- 批量操作族（C# ArrayCommands.NetworkDEL/NetworkMGET/NetworkMSET
      // 的 Tsavorite pending 读/环形页翻转 CompletePending 重放承接）
      C::Del | C::Unlink => {
        let (keys, initial_count) = match refs.split_last() {
          Some((cnt_bytes, keys)) if cnt_bytes.len() == 8 => {
            let Ok(arr) = <[u8; 8]>::try_from(*cnt_bytes) else {
              bail_frame!(output);
            };
            (keys, i64::from_le_bytes(arr))
          }
          _ => (refs.as_slice(), 0i64),
        };
        slow_arm!(output, array_cmds::slow::del, &storage, keys, initial_count);
      }
      C::Mget => {
        let proto = storage.resp_version;
        slow_arm!(output, array_cmds::slow::mget, &storage, &refs, proto);
      }
      C::Mset => {
        // 登记表句柄：窗内清退用（string_slow 臂同形装配，票 zcode-r163c-setguard 案一）
        let vector = self.vector_mgr();
        slow_arm!(output, array_cmds::slow::mset, &storage, vector, &refs);
      }
      // ---- 哈希族（HashCommands.cs 慢路径承接；HSCAN 走对象扫描族；
      // HLEN 由 O(1) 计数直读臂承接）
      C::Hset
      | C::Hsetnx
      | C::Hmset
      | C::Hget
      | C::Hgetall
      | C::Hmget
      | C::Hdel
      | C::Hexists
      | C::Hkeys
      | C::Hvals
      | C::Hrandfield
      | C::Hstrlen
      | C::Hincrby
      | C::Hincrbyfloat
      | C::Hexpire
      | C::Hpexpire
      | C::Hexpireat
      | C::Hpexpireat
      | C::Httl
      | C::Hpttl
      | C::Hexpiretime
      | C::Hpexpiretime
      | C::Hpersist => {
        slow_arm!(output, hash_commands::slow::hash, &storage, cmd, &refs);
      }
      // ---- 集合族（SetCommands.cs 慢路径承接；SSCAN 走对象扫描族；
      // SCARD 由 O(1) 计数直读臂承接）
      C::Sadd
      | C::Srem
      | C::Smembers
      | C::Sismember
      | C::Smismember
      | C::Spop
      | C::Srandmember
      | C::Smove
      | C::Sinter
      | C::Sinterstore
      | C::Sintercard
      | C::Sunion
      | C::Sunionstore
      | C::Sdiff
      | C::Sdiffstore => {
        let vector = self.vector_mgr();
        slow_arm!(
          output,
          set_commands::slow::set,
          &storage,
          vector,
          cmd,
          &refs
        );
      }
      // ---- 列表族（ListCommands.cs 慢路径承接；写回后唤醒阻塞观察者；
      // LSCAN 形态走对象扫描族）
      C::Lpush
      | C::Rpush
      | C::Lpushx
      | C::Rpushx
      | C::Lpop
      | C::Rpop
      | C::Lpos
      | C::Lmpop
      | C::Blpop
      | C::Brpop
      | C::Blmove
      | C::Brpoplpush
      | C::Ltrim
      | C::Lrange
      | C::Lindex
      | C::Linsert
      | C::Lrem
      | C::Lmove
      | C::Rpoplpush
      | C::Lset
      | C::Blmpop => {
        let notify = |key: &[u8]| self.notify_collection_update(key);
        let block = self.block_wait_face();
        slow_arm!(
          output,
          list_commands::slow::list,
          &storage,
          &notify,
          block.as_ref(),
          cmd,
          &refs,
        );
      }
      // ---- 有序集合族（SortedSetCommands.cs 慢路径承接；写回后唤醒阻塞
      // 观察者；ZSCAN 走对象扫描族）
      C::Zadd
      | C::Zscore
      | C::Zrem
      | C::Zpopmin
      | C::Zpopmax
      | C::Zrange
      | C::Zrevrange
      | C::Zrangebylex
      | C::Zrevrangebylex
      | C::Zrangebyscore
      | C::Zrevrangebyscore
      | C::Zrangestore
      | C::Zmscore
      | C::Zmpop
      | C::Zcount
      | C::Zlexcount
      | C::Zincrby
      | C::Zrank
      | C::Zrevrank
      | C::Zremrangebyrank
      | C::Zremrangebyscore
      | C::Zremrangebylex
      | C::Zrandmember
      | C::Zdiff
      | C::Zdiffstore
      | C::Zinter
      | C::Zintercard
      | C::Zinterstore
      | C::Zunion
      | C::Zunionstore
      | C::Bzpopmin
      | C::Bzpopmax
      | C::Bzmpop
      | C::Zexpire
      | C::Zpexpire
      | C::Zexpireat
      | C::Zpexpireat
      | C::Zttl
      | C::Zpttl
      | C::Zexpiretime
      | C::Zpexpiretime
      | C::Zpersist => {
        let notify = |key: &[u8]| self.notify_collection_update(key);
        let block = self.block_wait_face();
        slow_arm!(
          output,
          sorted_set_commands::slow::sorted_set,
          &storage,
          &notify,
          block.as_ref(),
          cmd,
          &refs,
        );
      }
      // ---- 地理族（SortedSetGeoCommands.cs 慢路径承接）
      C::Geoadd
      | C::Geodist
      | C::Geohash
      | C::Geopos
      | C::Georadius
      | C::GeoradiusRo
      | C::Georadiusbymember
      | C::GeoradiusbymemberRo
      | C::Geosearch
      | C::Geosearchstore => {
        slow_arm!(
          output,
          sorted_set_geo_commands::slow::geo,
          &storage,
          cmd,
          &refs
        );
      }
      // ---- 对象扫描族（C# SharedObjectCommands.ObjectScan 慢路径承接；
      // 快照尾参为 4 字节 LE COUNT 上限，exec 降级快照追加）
      C::Hscan | C::Sscan | C::Zscan | C::Coscan => {
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
            shared_object_commands::slow::coscan(&storage, scan_args, limit, &mut output).await
          }
          _ => {
            let object_type = match cmd {
              C::Hscan => GarnetObjectType::Hash,
              C::Sscan => GarnetObjectType::Set,
              _ => GarnetObjectType::SortedSet,
            };
            shared_object_commands::slow::object_scan(
              &storage,
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
      }
      // ---- 向量只读族冷态真读裁决（C# 各 NetworkV* 的 res 三态分派：
      // VectorManager.Locking.cs:ReadVectorIndexCore 的 Read_MainStore 真读
      // 落盘裁决后 WRONGTYPE / NOTFOUND 族就地应答。快路径守卫遇磁盘候选 /
      // 存储错误降级至此；写命令保守拒不走本臂，见 doc/zh/deviations.md §22）
      C::Vsim
      | C::Vemb
      | C::Vcard
      | C::Vdim
      | C::Vgetattr
      | C::Vinfo
      | C::Vismember
      | C::Vlinks
      | C::Vrandmember => {
        let Some(vectors) = &self.vector_session else {
          bail_frame!(output, RESP_ERR_ASYNC_REQUIRED);
        };
        vectors
          .network_vector_read_slow(&storage, cmd, &refs, resp_version, &mut output)
          .await;
      }
      // ---- 向量写族挂起闭环（VADD / VSETATTR：插入/属性写链为 compio 存储
      // 异步操作，同步段 inline_wait 内联收割移除后 Allow 态参数快照挂起
      // SlowWait 转投本臂——对标 cluster 链 pending_slow 转挂先例；慢臂
      // network_vector_write_slow 真读复判键域后闭环，poll 边界的执行域
      // 绑定由入口 SlowPollSessionBound 包装承接）
      C::Vadd | C::Vsetattr | C::Vrem => {
        let Some(vectors) = &self.vector_session else {
          bail_frame!(output, RESP_ERR_ASYNC_REQUIRED);
        };
        vectors
          .network_vector_write_slow(&storage, cmd, &refs, resp_version, &mut output)
          .await;
      }
      // 未接入慢路径分派表的命令兜底：本臂出现的 RESP_ERR_ASYNC_REQUIRED
      // 即分派表漏接线的缺陷信号（内部哨兵文案，C# 零命中——同步存储上下文
      // 里 CompletePending 系列就地闭环，客户端任何路径不该看到）。
      // 字符串 / 键管理 / Bitmap / 数组（TYPE/LCS）族已全部接臂，常规命令落此即回归
      _ => write_error_raw(&mut output, RESP_ERR_ASYNC_REQUIRED),
    }
    output
  }

  /// 存储执行域 DEBUG 承接面（FLUSHANDEVICT 物理驱逐，命令入口映射见
  /// 会话侧 network_debug）
  ///
  /// DEBUG 慢路径执行段（FLUSHANDEVICT 刷盘并驱逐主存储全部页面至磁盘区）
  ///
  /// 独立方法承载以脱离调用方的批处理纪元保护区（落盘与日志地址推进需排空旧纪元，
  /// 不得在 batch 纪元守卫内触发）
  pub(crate) async fn debug_command_slow(&self, args: &[Vec<u8>]) -> Vec<u8> {
    let mut output = Vec::new();
    if let Some(subcommand) = args.first()
      && subcommand.eq_ignore_ascii_case(b"FLUSHANDEVICT")
    {
      match self.session.store.flush_and_evict_all().await {
        Ok(()) => {
          // 对标 AdminCommands.cs:788 同名子命令，应答 'OK head= tail=' 逐字节同形；
          // 帧头由 simple string 单点成帧，载荷经 itoa 零重复格式化
          let mut h = Buffer::new();
          let mut t = Buffer::new();
          output.extend_from_slice(b"+OK head=");
          output.extend_from_slice(h.format(self.session.store.head_address()).as_bytes());
          output.extend_from_slice(b" tail=");
          output.extend_from_slice(t.format(self.session.store.tail_address()).as_bytes());
          output.extend_from_slice(b"\r\n");
        }
        Err(_) => err_frame!(output),
      }
      return output;
    }
    write_error_raw(&mut output, RESP_ERR_ASYNC_REQUIRED);
    output
  }

  /// 清库族慢路径执行段（C# BasicCommands.ExecuteFlushDb →
  /// StoreWrapper.FlushDatabase/FlushAllDatabases → databaseManager）
  ///
  /// 独立方法承载以脱离调用方的批处理纪元保护区（FLUSHALL 物理截断推进
  /// begin_address 需排空旧纪元读者，不得在 batch 纪元守卫内触发）
  ///
  /// 清库唯一漏斗（C# 一处漏斗）：本函数不含 store 直调臂，换号 / 截断与
  /// FlushDb / FlushNs / FlushAll 广播条目的唯一入口 = databaseManager 换号
  /// 执行段 + SafeFlushAOF 广播条目（`safe_flush_aof` 对标 C#
  /// SingleDatabaseManager.SafeFlushAOF；仅主库入队，副本经回放条目承接
  /// 换号，主从读写域一致）。三支统一经常驻 SingleDatabaseManager，绝不绕过
  /// 包装直调 store——缺广播条目即主从换号域分叉；管理面未装配即显式回错
  pub(crate) async fn flush_command_slow(&self, cmd: RespCommand, args: &[Vec<u8>]) -> Vec<u8> {
    let refs: Vec<&[u8]> = args.iter().map(Vec::as_slice).collect();
    let mut output = Vec::new();
    let Ok(opts) = parse_flush_options(&refs) else {
      bail_frame!(output, RESP_ERR_GENERIC_SYNTAX_ERROR);
    };
    let _ = opts.async_flush;
    let caller_ns = self.session.namespace();

    // 门禁前置：物理截断日志属于全局破坏性运维动作，非 0 租户禁止执行
    if opts.unsafe_truncate_log && caller_ns != 0 {
      bail_frame!(output, RESP_ERR_FLUSH_TRUNCATE_LOG_NS0);
    }

    // 清库唯一漏斗：三条臂一律经常驻 SingleDatabaseManager（换号/截断执行段
    // + SafeFlushAOF 广播条目），对标 C# BasicCommands.ExecuteFlushDb →
    // C# StoreWrapper.FlushDatabase / C# StoreWrapper.FlushAllDatabases →
    // C# SingleDatabaseManager.FlushDatabase（内嵌 SafeFlushAOF(FlushDb)）。
    // 直调 store 换号虽会经 DbMeta 镜像条目同步映射（service.rs:on_aof_store_event
    // 放行 KeyTag::DbMeta），但绕过 SafeFlushAOF 即丢 FlushDb 广播条目：副本侧
    // 换号栅栏对齐与向量登记表域回收双双缺失。管理面未装配时显式回错，绝不
    // 静默退回 store 直调（宁可拒绝命令，不可制造主从发散）
    let Some(manager) = self.checkpoint.as_ref().map(|ctx| &ctx.database_manager) else {
      bail_frame!(output, RESP_ERR_CHECKPOINT_UNWIRED);
    };

    if manager.is_device_contaminated() {
      bail_frame!(output, RESP_ERR_DEVICE_CONTAMINATED_REFUSING_FLUSH);
    }

    let flushed = if cmd == RespCommand::Flushdb {
      // O(1) 虚拟数据库换号秒级清库 + FlushDb 广播条目（载荷 (vns, 旧 vdb)）
      manager
        .flush_database(
          caller_ns,
          self.session.active_db(),
          opts.unsafe_truncate_log,
        )
        .await
    } else if caller_ns != 0 {
      // 非 0 租户 FLUSHALL：仅清空当前命名空间下的全部库（FlushNs 广播条目），
      // 绝不触碰其他租户与全域物理截断
      manager
        .flush_namespace(caller_ns, opts.unsafe_truncate_log)
        .await
    } else {
      // 超管 ns 0：全域物理截断 O(1) + FlushAll 广播条目
      manager.flush_all_databases(opts.unsafe_truncate_log).await
    };
    if flushed.is_err() {
      bail_frame!(output);
    }
    // 清库成功后统一单点刷新会话活动库，防止会话本地 active_vdb 缓存陈旧代数
    self.session.set_active_db(self.session.active_db());
    // 超管执行 UNSAFETRUNCATELOG 物理截断历史段
    if opts.unsafe_truncate_log && self.session.store.truncate().await.is_err() {
      bail_frame!(output);
    }
    // 非 0 租户 FLUSHALL 集群总线换号广播（doc/zh/db.md 4.5）：本地换号
    // 仅完成协调者一区，须经总线收齐全部主节点 ack 方可应答；广播 future
    // 自带最终应答字节（全部 +OK 才 +OK，任一失败/超时回错误），单机门
    // 缺省时直落本地 +OK
    if cmd == RespCommand::Flushall
      && caller_ns != 0
      && let Some(bcast) = manager.flushall_broadcast(caller_ns)
    {
      output.extend_from_slice(&bcast.await);
      return output;
    }
    output.extend_from_slice(RESP_OK);
    output
  }

  /// SWAPDB 跨库交换慢路径执行段（在 batch 纪元保护区外执行，杜绝持有 EpochGuard 跨 await 导致死锁）
  ///
  /// 集群模式的按库归属门禁在同步校验段（array_commands network_swapdb 经集群
  /// 提供者 is_slot_local_stable 判定两库槽位均由本地掌管且处于 Stable 态）
  /// 放行后，与单机同流此异步换号路径（doc/zh/db.md SWAPDB 条款）
  pub(crate) async fn swap_command_slow(&self, args: &[Vec<u8>]) -> Vec<u8> {
    let mut output = Vec::new();
    let (Some(idx1), Some(idx2)) = (
      args.first().and_then(|a| parse_db_index(a).ok()),
      args.get(1).and_then(|a| parse_db_index(a).ok()),
    ) else {
      bail_frame!(output, RESP_ERR_ASYNC_REQUIRED);
    };
    if idx1 == idx2 {
      output.extend_from_slice(RESP_OK);
    } else if active_session_count() > 1 {
      // 活跃会话门控（对标 C# MultiDatabaseManager.TrySwapDatabases 的
      // activeSessions > 1 分支）：多会话在途读写换库会与其缓存的库上下文
      // 失步，故按 Garnet 契约拒换库。换号动作必须在门控之后，绝不半程搬移
      write_error_raw(&mut output, RESP_ERR_SWAPDB_UNSUPPORTED);
    } else {
      let swapped = self
        .session
        .swap_databases(idx1 as i64, idx2 as i64)
        .await
        .is_ok();
      if swapped {
        let (vns, _) = self.session.virtual_domain();
        let caller_ns = self.session.namespace();
        if let Some(ref vs) = self.vector_session {
          vs.manager
            .swap_database_slots(vns, caller_ns, idx1 as u64, idx2 as u64)
            .await;
        } else if let Some(ref ctx) = self.checkpoint
          && let Some(vm) = ctx.database_manager.try_vector_manager()
        {
          vm.swap_database_slots(vns, caller_ns, idx1 as u64, idx2 as u64)
            .await;
        }
        output.extend_from_slice(RESP_OK);
      } else {
        write_error_raw(&mut output, RESP_ERR_SWAPDB_UNSUPPORTED);
      }
    }
    output
  }

  /// 检查点 / AOF 提交族慢路径执行段（C# AdminCommands.NetworkSAVE/
  /// NetworkBGSAVE/NetworkLASTSAVE/NetworkCOMMITAOF；rust 映射为
  /// WedbStore::create_checkpoint（经 wkv 检查点通道）；LASTSAVE 为纯读取；
  /// COMMITAOF 为 AOF 物理刷盘提交）
  ///
  /// libs/server/Resp/AdminCommands.cs:CommitAofAsync 合并承接：C# 该接口方法
  /// 即纯转发 `=> storeWrapper.CommitAOFAsync(dbId)`，rust COMMITAOF 臂经常驻
  /// SingleDatabaseManager 内核 `IDatabaseManager::commit_to_aof_async` 直达
  /// 同一落点，转发层不另设。
  ///
  /// libs/server/StoreWrapper.cs:CommitAOFAsync
  ///（C# internal 转发（EnableAOF 门 + 多库兼容门 + 按/全库分派）；rust 共享
  /// 单 AOF 面下门与分派随双轨折叠，Commitaof 慢路径臂直调常驻管理器内核）
  ///
  /// SAVE 同步等待检查点完成（C# NetworkSAVE：AsyncUtils.BlockingWait）；
  /// BGSAVE 对标 C# SingleDatabaseManager.TakeCheckpointAsync(background=true)
  /// ——检查点后台任务承接（LastSaveTime 在任务完成时落定），命令即回
  /// "Background saving started"
  ///
  /// 独立方法承载以脱离调用方的批处理纪元保护区（检查点
  /// CheckpointWhileEpochProtected fail-fast 契约，实现在 wcpr 经 wkv 生效；
  /// AOF 提交同不得在存储写纪元内触发刷盘级联）
  pub(crate) async fn checkpoint_command_slow(
    &self,
    cmd: RespCommand,
    args: &[Vec<u8>],
  ) -> Vec<u8> {
    use RespCommand as C;

    let mut output = Vec::new();
    match cmd {
      C::Lastsave => {
        let secs = match &self.checkpoint {
          Some(ctx) => ctx.database_manager.last_save_ms() / 1000,
          None => 0,
        };
        output.write_resp_int(secs as i64);
      }
      C::Commitaof => {
        // C# NetworkCOMMITAOF → CommitAofAsync(dbId) → storeWrapper.CommitAOFAsync
        // → databaseManager.CommitToAofAsync → AppendOnlyFile.Log.CommitAsync
        // （物理刷盘推进 committed_until 至 safe_tail）：rust 经常驻
        // SingleDatabaseManager 内核（IDatabaseManager::commit_to_aof_async）
        // 承接。
        // C# !EnableAOF 直接回 false 不做 I/O，而 NetworkCOMMITAOF 无视提交
        // 结果恒回 "AOF file committed"——db.aof 缺位即该禁用态（内核空操作），
        // 应答文案不变。DBID 已在会话侧 try_parse_database_id 校验，此处防御
        // 重解析（缺省 -1 = 全部活跃库；rust 共享存储单 WAL，全库共用一条
        // 物理日志，提交即覆盖全部在途条目，与 C# SingleDatabaseManager
        // 忽略 dbId 同口径）
        let Some(ctx) = &self.checkpoint else {
          bail_frame!(output, RESP_ERR_CHECKPOINT_UNWIRED);
        };
        let _db_id = match args.first().map(Vec::as_slice) {
          None => -1,
          Some(arg) => match parse_db_index(arg) {
            Ok(idx) => idx as i64,
            Err(_) => bail_frame!(output, RESP_ERR_ASYNC_REQUIRED),
          },
        };
        if ctx.database_manager.commit_to_aof_async().await.is_err() {
          bail_frame!(output);
        }
        output.write_resp_simple_string("AOF file committed");
      }
      C::Save | C::Bgsave => {
        let Some(ctx) = &self.checkpoint else {
          bail_frame!(output, RESP_ERR_CHECKPOINT_UNWIRED);
        };
        // C# NetworkSAVE/NetworkBGSAVE → storeWrapper.TakeCheckpointAsync →
        // SingleDatabaseManager.TakeCheckpointAsync：full 判定 + 版本推进
        // + AOF 安全截断 + CheckpointingLock 互斥，对齐常驻单例。
        // 互斥走 take_checkpoint 收口入口（占闸失败即 false → already-
        // in-progress 应答 + finally 还闸），与副本重放钩子、集群按需重拍同闸
        if ctx.database_manager.is_device_contaminated() {
          bail_frame!(output, RESP_ERR_DEVICE_CONTAMINATED_REFUSING_CHECKPOINT);
        }

        if matches!(cmd, C::Bgsave) {
          // C# TakeCheckpointAsync(true)：占闸在同步段完成（false 即占用拒
          // 绝），成功才转后台任务承接推进段（background=true 不 await
          // helper 即返 true 同位），命令即回成功文案
          if ctx.database_manager.try_pause_checkpoints() {
            let mgr_bg = Arc::clone(&ctx.database_manager);
            spawn(async move {
              if let Err(e) = mgr_bg.take_checkpoint_within_gate().await {
                // 与同步 SAVE 臂、service.rs 自动检查点任务单级对齐 error
                //（C# DatabaseManagerBase.cs:202 检查点失败统一 LogError）
                log::error!("background checkpoint failed: {e}");
              }
            })
            .detach();
            // C# BGSAVE 成功应答文案
            output.write_resp_simple_string("Background saving started");
          } else {
            write_error_raw(&mut output, RESP_ERR_CHECKPOINT_ALREADY_IN_PROGRESS);
          }
        } else {
          // 同步 SAVE：C# BlockingWait(TakeCheckpointAsync(false)) 后
          // !success → already-in-progress 同位（占闸失败不排队——外部
          // TryPauseCheckpoints 持闸方还闸时点不受命令侧控制，排队即无上
          // 界死等）
          match ctx.database_manager.take_checkpoint(false).await {
            Ok(true) => {
              output.extend_from_slice(RESP_OK);
            }
            Ok(false) => err_frame!(output, RESP_ERR_CHECKPOINT_ALREADY_IN_PROGRESS),
            Err(e) => {
              log::error!("SAVE checkpoint failed: {e}");
              err_frame!(output);
            }
          }
        }
      }
      _ => unreachable!("checkpoint_command_slow 仅承接检查点族"),
    }
    output
  }
}
