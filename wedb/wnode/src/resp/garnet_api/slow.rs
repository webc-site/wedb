//! 慢路径异步分派与通道调度

use std::sync::{Arc, atomic::Ordering};

use compio::runtime::spawn;
use itoa::Buffer;
use wbase::{glob::glob_match_nocase, num::parse_db_index};
use wdev::Device;
use wmetric::GarnetInfoMetrics;
use wresp::{
  cmd_strings::{
    RESP_EMPTY, RESP_ERR_ASYNC_REQUIRED, RESP_ERR_CHECKPOINT_ALREADY_IN_PROGRESS,
    RESP_ERR_GENERIC_SYNTAX_ERROR, RESP_ERR_GENERIC_UNK_CMD, RESP_ERR_HCOLLECT_ALREADY_IN_PROGRESS,
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
    RespServerSession, array_commands as array_cmds,
    basic_commands::{
      parse_flush_options,
      slow::{bitmap_slow, string_slow},
    },
    info_provider::InfoSlowScanSource,
    key_admin_commands::slow::key_admin_slow,
    objects::{
      hash_commands, list_commands,
      object_store_utils::{ObjLoad, envelope_heap_estimate, obj_length_async},
      set_commands, shared_object_commands, sorted_set_commands, sorted_set_geo_commands,
      tiered_collection_ops::exec_tiered_collect,
    },
    rangeindex::resp_server_session_range_index as ri_cmds,
  },
  servers::consumer_registry::ConsumerRegistry,
  storage::session::storage_session::StorageSession,
  types::GarnetStatus,
};

/// C# TrySwapDatabases 活跃会话门控（MultiDatabaseManager.cs:695-709）：
/// 遍历 Servers 的 ActiveConsumers 统计 RespServerSession 数，> 1 拒绝换库
///
/// rust 会话注册表为进程级 [`ConsumerRegistry`]（主 RESP 服务网络泵建连/
/// 释放时注册注销，与 C# activeHandlers 同面）；未装配（嵌入式/测试）视为
/// 单会话放行
fn active_resp_sessions_exceeds_one() -> bool {
  ConsumerRegistry::global().is_some_and(|reg| reg.connection_totals().2 > 1)
}

/// 检查点通道未装配（宿主未注入 [`super::CheckpointCtx`]）时的显式拒绝文案
const RESP_ERR_CHECKPOINT_UNWIRED: &str = "ERR checkpoint channel not configured";

impl<D: Device> StoreGarnetApi<D> {
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
      // 会话延迟表下传（与 sessionMetrics 同轨的延迟注入轨：C# storageSession
      // 共持 LatencyMetrics，pending 闭环据此记 PENDING_LAT；会话未回挂/
      // 延迟监视关闭为 None，零取时零分配）
      .with_latency_metrics(self.latency_metrics())
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
        let vector = self.vector_session.as_ref().map(|v| v.manager.as_ref());
        if string_slow(&storage, cmd, &refs, vector, &mut output)
          .await
          .is_err()
        {
          write_error_raw(&mut output, RESP_ERR_SLOW_PATH_STORAGE);
        }
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
        let vector = self.vector_session.as_ref().map(|v| v.manager.as_ref());
        if bitmap_slow(&storage, cmd, &refs, vector, &mut output)
          .await
          .is_err()
        {
          write_error_raw(&mut output, RESP_ERR_SLOW_PATH_STORAGE);
        }
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
        let vector = self.vector_session.as_ref().map(|v| v.manager.as_ref());
        if key_admin_slow(&storage, cmd, &refs, vector, &mut output)
          .await
          .is_err()
        {
          write_error_raw(&mut output, RESP_ERR_SLOW_PATH_STORAGE);
        }
      }
      // ---- MSETNX 慢路径承接（C# MSET_Conditional 事务锁内全同步闭环，
      // 无此形态；rust 快路径写入段环形页翻转 / 判定段磁盘候选降级至此
      // 续跑，杜绝半提交误答——应答与最终存储状态恒一致）
      C::Msetnx => {
        // 尾参为快路径续跑模式标记（exec 降级快照追加）：b"1" = NX 判定
        // 已整体通过、前缀键已持久写入，直接续写回 :1；b"0" = 判定段降级，
        // 先双域异步裁决存活——任一存活整体不写回 :0（C# EXISTS 非
        // NOTFOUND 即存在），全不存活才写入
        let resume = matches!(args.last().map(Vec::as_slice), Some(b"1"));
        // 剥离尾参后的键值对序列（快路径已校验 arity 非空且偶数）
        let pairs = &refs[..refs.len() - 1];
        if !resume {
          for key in pairs.iter().step_by(2) {
            // String 域优先、未命中探对象信封域（对标 C# unified 域
            // EXISTS）；TTL 过期键经异步读惰性清除后视同缺失。逐键 await
            // 窗口内的并发写入与 C# 锁序差异属顺序未定义，非语义破坏
            let alive = match storage.read_tag_with(key, KeyTag::String, |_| ()).await {
              Ok(hit) => hit.is_some(),
              Err(_) => {
                write_error_raw(&mut output, RESP_ERR_SLOW_PATH_STORAGE);
                return output;
              }
            } || match storage
              .read_tag_with(key, KeyTag::ObjectEnvelope, |_| ())
              .await
            {
              Ok(hit) => hit.is_some(),
              Err(_) => {
                write_error_raw(&mut output, RESP_ERR_SLOW_PATH_STORAGE);
                return output;
              }
            };
            if alive {
              output.write_resp_int(0);
              return output;
            }
          }
        }
        // 逐键写入（SET 语义自动清新键残留 TTL，与快路径 try_upsert_sync
        // 同口径）；补写模式重写前缀键同值幂等
        for [key, val] in pairs.as_chunks::<2>().0 {
          if storage.upsert_string(key, val).await.is_err() {
            write_error_raw(&mut output, RESP_ERR_SLOW_PATH_STORAGE);
            return output;
          }
        }
        output.write_resp_int(1);
      }
      // ---- GET 慢路径承接（同步段磁盘候选降级至此：异步 read_string
      // 含惰性过期物理清除（purge 后视同不存在），对标 C# InternalRead 的
      // ReadCache/磁盘冷读异步完成路径；TTL 已到期键不再降级——同步段
      // ttl_gate 同栈裁决直接 NOTFOUND 闭环，对标 C# Reader 内 CheckExpiry；
      // 多键时触发流水线冷读 Scatter-Gather (SG) 异步批量 I/O，对标 C# NetworkGET_SG）
      C::Get => {
        if storage
          .read_string_batch_into(&refs, &mut output)
          .await
          .is_err()
        {
          write_error_raw(&mut output, RESP_ERR_SLOW_PATH_STORAGE);
        }
      }
      // ---- HyperLogLog 族慢路径承接（HyperLogLogOps.cs:HyperLogLogAdd/
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
          write_error_raw(&mut output, RESP_ERR_SLOW_PATH_STORAGE);
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
          Err(_) => write_error_raw(&mut output, RESP_ERR_SLOW_PATH_STORAGE),
        }
      }
      C::Keys => {
        self
          .session
          .set_context(self.session.namespace(), self.session.active_db());
        let pattern = refs.first().copied().unwrap_or(b"*");
        match storage.db_keys(pattern).await {
          Ok(mut keys) => {
            // 登记表域内用户键投影（本库向量键；剥域后与存储键同域比对）
            if let Some(vectors) = &self.vector_session {
              let prefix = self.session.session_prefix();
              let all_keys = pattern == b"*";
              vectors
                .manager
                .for_each_domain_user_key(prefix.as_slice(), |k| {
                  if (all_keys || glob_match_nocase(pattern, k))
                    && !keys.iter().any(|existing| existing == k)
                  {
                    keys.push(k.to_vec());
                  }
                });
            }
            output.write_resp_array_len(keys.len());
            for key in &keys {
              output.write_resp_bulk_string(key);
            }
          }
          Err(_) => write_error_raw(&mut output, RESP_ERR_SLOW_PATH_STORAGE),
        }
      }
      C::Scan => {
        let filter = match parse_scan_filter(&refs) {
          Ok(f) => f,
          Err(err) => {
            write_error_raw(&mut output, err);
            return output;
          }
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
            if filter.cursor == 0
              && !filter.type_given
              && let Some(vectors) = &self.vector_session
            {
              // 登记表域内用户键投影（同 KEYS 域内单点，剥域用户键口径）
              let prefix = self.session.session_prefix();
              vectors
                .manager
                .for_each_domain_user_key(prefix.as_slice(), |k| {
                  if (filter.all_keys || glob_match_nocase(&filter.pattern, k))
                    && !keys.iter().any(|existing| existing == k)
                  {
                    keys.push(k.to_vec());
                  }
                });
            }
            let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
            RespServerSession::write_output_for_scan(cursor as i64, &key_refs, &mut output)
          }
          Err(_) => write_error_raw(&mut output, RESP_ERR_SLOW_PATH_STORAGE),
        }
      }
      // ---- INFO 慢路径闭环（KEYSPACE 逐库扫描：C# PopulateKeyspaceInfo →
      // GetKeyspaceStats → GetDatabaseKeyspaceStats 的专用扫描会话统计；
      // HLOGSCAN 分布扫描：C# PopulateHlogScanInfo →
      // storeWrapper.HybridLogDistributionScan；STOREHASHTABLE 哈希分布：
      // PopulateStoreHashDistribution → DumpDistribution；STOREREVIV 复活
      // 统计：PopulateStoreRevivInfo → DumpRevivificationStats。存储域扫描
      // 须跨 await，同步段降级至此）。快照尾参 8 字节 LE 库数上限。
      // KEYSPACE 段一律走引擎侧单内核 WedbStore::keyspace_stats——只读
      // 遍历在册库、一趟分桶扫描，连接会话上下文零改动（切库既盲分配虚库
      // 又落 DbMeta，冷库还会把上一库计数错贴到本库号上），虚库上界按快照
      // 做越界防御。段文本经 wmetric 段填充器一处定义（数据源
      // InfoSlowScanSource）
      C::Info => {
        let Some(max_db_bytes) = args.last().map(Vec::as_slice) else {
          write_error_raw(&mut output, RESP_ERR_SLOW_PATH_STORAGE);
          return output;
        };
        let Ok(max_databases) = <[u8; 8]>::try_from(max_db_bytes).map(u64::from_le_bytes) else {
          write_error_raw(&mut output, RESP_ERR_SLOW_PATH_STORAGE);
          return output;
        };
        // 段集合解析（同步分派已保证全部段 ∈ 扫描族四段）
        let segments: Vec<InfoMetricsType> = args[..args.len() - 1]
          .iter()
          .filter_map(|a| InfoMetricsType::from_name(a))
          .collect();

        // KEYSPACE 段：本租户在册库键空间计数（C# PopulateKeyspaceInfo 仅列出
        // 至少持有一个键的库，零键库行与虚库号越界者一并剔除）
        let mut keyspace = Vec::new();
        if segments.contains(&InfoMetricsType::Keyspace) {
          match self
            .session
            .store()
            .keyspace_stats(self.session.namespace())
            .await
          {
            Ok(rows) => {
              keyspace = rows
                .into_iter()
                .filter(|&(db, keys, _)| db < max_databases && keys > 0)
                .map(|(db, keys, expires)| (db as i32, keys, expires))
                .collect();
            }
            Err(_) => {
              write_error_raw(&mut output, RESP_ERR_SLOW_PATH_STORAGE);
              return output;
            }
          }
        }

        // HLOGSCAN 段：经数据库管理面拉取混合日志内存分布扫描
        //（C# PopulateHlogScanInfo → HybridLogDistributionScan；wedb 单
        // 物理日志 + wcol 信封统一值域，统计由 db 0 形态呈现，对象存储
        // 槽恒空——无独立对象存储域。管理面未装配的嵌入式形态转储为空，
        // 段按 wmetric 缺省形态呈现 Empty；扫描失败与 KEYSPACE 段同款
        // 回错——不静默降级空转储，客户端可区分空日志与存储故障）
        let mut hlog_dump = Vec::new();
        if segments.contains(&InfoMetricsType::HlogScan)
          && let Some(mgr) = self
            .checkpoint
            .as_ref()
            .map(|c| Arc::clone(&c.database_manager))
        {
          match mgr.collect_hybrid_log_stats().await {
            Ok(stats) => {
              hlog_dump = stats
                .into_iter()
                .map(|(_, m)| m.dump_scan_metrics_info())
                .collect();
            }
            Err(_) => {
              write_error_raw(&mut output, RESP_ERR_SLOW_PATH_STORAGE);
              return output;
            }
          }
        }

        // STOREHASHTABLE / STOREREVIV 段：哈希索引分布直方图（O(桶数)
        // 纯内存诊断扫描，C# PopulateStoreHashDistribution → DumpDistribution）
        // 与复活池四计数转储（O(1)，C# PopulateStoreRevivInfo →
        // DumpRevivificationStats）。wedb 单物理存储，db 0 形态呈现；
        // 纯内存读无失败面，不引入 KEYSPACE 段同款回错分支
        let mut hash_dump = Vec::new();
        if segments.contains(&InfoMetricsType::StoreHashtable) {
          hash_dump.push(self.session.store().hash_distribution_dump());
        }
        let mut reviv_dump = Vec::new();
        if segments.contains(&InfoMetricsType::StoreReviv) {
          reviv_dump.push(self.session.store().revivification_dump());
        }

        let provider = InfoSlowScanSource::new(keyspace, hlog_dump, hash_dump, reviv_dump);
        let text = GarnetInfoMetrics::new().get_resp_info(
          &segments,
          self.session.active_db() as i32,
          &provider,
        );
        if text.is_empty() {
          // C# InfoCommand.cs:NetworkINFO 空段回 CmdStrings.RESP_EMPTY（
          // 协议恒定的空批量串，非 nil）
          output.extend_from_slice(RESP_EMPTY);
        } else {
          output.write_resp_bulk_string(text.as_bytes());
        }
      }
      // ---- 清库族（BasicCommands.cs:ExecuteFlushDb：选项单源重解析 +
      // O(1) 虚拟 ID 换号秒清；FLUSHDB 清会话当前库——C# FlushDatabase(activeDbId)，
      // FLUSHALL 清全部库——C# FlushAllDatabases 逐活跃库 FlushDatabase）
      // ---- MEMORY USAGE 慢路径闭环（BasicCommands.cs:NetworkMemoryUsage 的
      // RECORD_ON_DISK 分支：快路径磁盘候选/异步 TTL 裁决降级至此，双域尺寸
      // 统计与 RespServerSession::network_memory_usage 同口径）
      C::MemoryUsage => {
        let key = refs.first().copied().unwrap_or(&[]);
        let resp_err = |output: &mut Vec<u8>| write_error_raw(output, RESP_ERR_SLOW_PATH_STORAGE);
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
            Ok(None) => output.write_resp_null_ver(resp_version),
            Err(_) => resp_err(&mut output),
          },
          Err(_) => resp_err(&mut output),
        }
      }
      // ---- 集合对象长度异步直读闭环（HLEN/SCARD/ZCARD/LLEN 慢路径承接：
      // 同步段磁盘候选/异步 TTL 裁决降级至此，4B 头部直读计数 O(1) 零反序列化）
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
          // 分层键字段级 TTL 水位命中：树内收集执行体校正（到期成员物理
          // 出账 + meta 回写）后直读，两态计数同口径
          Ok(ObjLoad::Degrade) => match exec_tiered_collect(&storage.batch, key, tag).await {
            Ok(Some(len)) => output.write_resp_int(len as i64),
            Ok(None) => output.extend_from_slice(RESP_RETURN_VAL_0),
            Err(()) => write_error_raw(&mut output, RESP_ERR_SLOW_PATH_STORAGE),
          },
          Ok(ObjLoad::WrongType) => {}
          Err(_) => write_error_raw(&mut output, RESP_ERR_SLOW_PATH_STORAGE),
        }
      }
      // ---- 对象收集 `*` 全库族（AdminCommands.cs:NetworkHCOLLECT /
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
            let res = object_collect_all(&storage, is_hash).await;
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
              Err(_) => {
                write_error_raw(&mut output, RESP_ERR_SLOW_PATH_STORAGE);
                return output;
              }
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
      // ---- 自定义对象命令族（CustomRespCommands.cs:TryCustomObjectCommand
      // 的异步承接：同步段磁盘候选降级重放。快照尾参为命令名，经与快路径
      // 解析、ACL 校验同一张编译期静态清单回查后按同款四接口执行；
      // 未启用任一扩展时清单为空表 → 未知命令兜底臂）
      #[cfg(any(feature = "roaring", feature = "json"))]
      C::Customobjcmd => {
        let Some((name, cmd_refs)) = refs.split_last() else {
          write_error_raw(&mut output, RESP_ERR_ASYNC_REQUIRED);
          return output;
        };
        let Some((entry, meta)) = custom_objects::match_custom_object_command(name) else {
          write_error_raw(&mut output, RESP_ERR_GENERIC_UNK_CMD);
          return output;
        };
        if let Err(()) = custom_object_slow(
          &storage,
          meta.command_type,
          entry.tag.as_u8(),
          &meta.fns,
          cmd_refs,
          &mut output,
        )
        .await
        {
          write_error_raw(&mut output, RESP_ERR_SLOW_PATH_STORAGE);
        }
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
        let ri = Some(self.session.store.range_index().as_ref());
        // 解析校验 + 执行一体闭环；应答直写 output（错误映射在处理器内）
        let _ = match cmd {
          C::Ricreate => ri_cmds::network_ricreate(&refs, ri, &self.session, &mut output).await,
          C::Riset => ri_cmds::network_riset(&refs, ri, &self.session, &mut output).await,
          C::Riget => {
            ri_cmds::network_riget(&refs, ri, &self.session, resp_version, &mut output).await
          }
          C::Ridel => ri_cmds::network_ridel(&refs, ri, &self.session, &mut output).await,
          C::Riscan => ri_cmds::network_riscan(&refs, ri, &self.session, &mut output).await,
          C::Rirange => ri_cmds::network_rirange(&refs, ri, &self.session, &mut output).await,
          C::Riexists => ri_cmds::network_riexists(&refs, ri, &self.session, &mut output).await,
          C::Riconfig => ri_cmds::network_riconfig(&refs, ri, &self.session, &mut output).await,
          C::Ricount => ri_cmds::network_ricount(&refs, ri, &self.session, &mut output).await,
          _ => ri_cmds::network_rimetrics(&refs, ri, &self.session, &mut output).await,
        };
      }
      // ---- 过期键删除扫描（AdminCommands.cs:NetworkEXPDELSCAN：C# 阻塞
      // 等待 storeWrapper.ExpiredKeyDeletionScan；应答 *2 计数对
      // `*2\r\n$N\r\n<expired>\r\n$N\r\n<scanned>\r\n`，DBID 已在快
      // 路径 try_parse_database_id 校验，此处防御性重解析）
      C::Expdelscan => {
        let db_id = match refs.first() {
          None => None,
          Some(arg) => match parse_db_index(arg) {
            Ok(idx) => Some(idx as u64),
            Err(_) => {
              write_error_raw(&mut output, RESP_ERR_ASYNC_REQUIRED);
              return output;
            }
          },
        };
        match self.session.store.expired_key_deletion_scan(db_id).await {
          Ok((expired, scanned)) => {
            let mut buf = Buffer::new();
            output.write_resp_array_len(2);
            output.write_resp_bulk_string(buf.format(expired).as_bytes());
            output.write_resp_bulk_string(buf.format(scanned).as_bytes());
          }
          Err(_) => write_error_raw(&mut output, RESP_ERR_SLOW_PATH_STORAGE),
        }
      }
      // ---- 批量操作族（ArrayCommands.cs:NetworkDEL/NetworkMGET/NetworkMSET
      // 的 Tsavorite pending 读/环形页翻转 CompletePending 重放承接）
      C::Del | C::Unlink => {
        if array_cmds::slow::del(&storage, &refs, &mut output)
          .await
          .is_err()
        {
          write_error_raw(&mut output, RESP_ERR_SLOW_PATH_STORAGE);
        }
      }
      C::Mget => {
        if array_cmds::slow::mget(
          &storage,
          &refs,
          storage.resp_protocol_version(),
          &mut output,
        )
        .await
        .is_err()
        {
          write_error_raw(&mut output, RESP_ERR_SLOW_PATH_STORAGE);
        }
      }
      C::Mset => {
        if array_cmds::slow::mset(&storage, &refs, &mut output)
          .await
          .is_err()
        {
          write_error_raw(&mut output, RESP_ERR_SLOW_PATH_STORAGE);
        }
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
        if hash_commands::slow::hash(&storage, cmd, &refs, &mut output)
          .await
          .is_err()
        {
          write_error_raw(&mut output, RESP_ERR_SLOW_PATH_STORAGE);
        }
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
        if set_commands::slow::set(&storage, cmd, &refs, &mut output)
          .await
          .is_err()
        {
          write_error_raw(&mut output, RESP_ERR_SLOW_PATH_STORAGE);
        }
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
        if list_commands::slow::list(&storage, &notify, cmd, &refs, &mut output)
          .await
          .is_err()
        {
          write_error_raw(&mut output, RESP_ERR_SLOW_PATH_STORAGE);
        }
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
        if sorted_set_commands::slow::sorted_set(&storage, &notify, cmd, &refs, &mut output)
          .await
          .is_err()
        {
          write_error_raw(&mut output, RESP_ERR_SLOW_PATH_STORAGE);
        }
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
        if sorted_set_geo_commands::slow::geo(&storage, cmd, &refs, &mut output)
          .await
          .is_err()
        {
          write_error_raw(&mut output, RESP_ERR_SLOW_PATH_STORAGE);
        }
      }
      // ---- 对象扫描族（SharedObjectCommands.cs:ObjectScan 慢路径承接；
      // 快照尾参为 4 字节 LE COUNT 上限，exec 降级快照追加）
      C::Hscan | C::Sscan | C::Zscan | C::Coscan => {
        let (Some(limit_bytes), scan_args) =
          (refs.last().copied(), &refs[..refs.len().saturating_sub(1)])
        else {
          write_error_raw(&mut output, RESP_ERR_SLOW_PATH_STORAGE);
          return output;
        };
        let Ok(limit_arr) = <[u8; 4]>::try_from(limit_bytes) else {
          write_error_raw(&mut output, RESP_ERR_SLOW_PATH_STORAGE);
          return output;
        };
        let limit = i32::from_le_bytes(limit_arr);
        let proto = storage.resp_protocol_version();
        let res = match cmd {
          C::Coscan => {
            shared_object_commands::slow::coscan(&storage, scan_args, limit, proto, &mut output)
              .await
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
          write_error_raw(&mut output, RESP_ERR_SLOW_PATH_STORAGE);
        }
      }
      // 未接入慢路径分派表的命令兜底：本臂出现的 RESP_ERR_ASYNC_REQUIRED
      // 即分派表漏接线的缺陷信号（内部哨兵文案，C# 零命中——同步存储上下文
      // 里 CompletePending 系列就地闭环，客户端任何路径不该看到）。
      // 字符串 / 键管理 / Bitmap 族已全部接臂，常规命令落此即回归
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
          let mut buf = Buffer::new();
          output.extend_from_slice(b"+OK head=");
          output.extend_from_slice(buf.format(self.session.store.head_address()).as_bytes());
          output.extend_from_slice(b" tail=");
          output.extend_from_slice(buf.format(self.session.store.tail_address()).as_bytes());
          output.extend_from_slice(b"\r\n");
        }
        Err(_) => write_error_raw(&mut output, RESP_ERR_SLOW_PATH_STORAGE),
      }
      return output;
    }
    write_error_raw(&mut output, RESP_ERR_ASYNC_REQUIRED);
    output
  }

  /// 清库族慢路径执行段（AdminCommands.cs:ExecuteFlushDb →
  /// StoreWrapper.FlushDatabase/FlushAllDatabases → databaseManager）
  ///
  /// 独立方法承载以脱离调用方的批处理纪元保护区（FLUSHALL 物理截断推进
  /// begin_address 需排空旧纪元读者，不得在 batch 纪元守卫内触发）
  ///
  /// 清库唯一漏斗（C# 一处漏斗）：本函数不含 store 直调臂，换号 / 截断与
  /// FlushDb / FlushNs / FlushAll 广播条目的唯一入口 = databaseManager 换号
  /// 执行段 + SafeFlushAOF 广播条目（`safe_flush_aof` 对标 C#
  /// SingleDatabaseManager.cs:SafeFlushAOF；仅主库入队，副本经回放条目承接
  /// 换号，主从读写域一致）。三支统一经常驻 SingleDatabaseManager，绝不绕过
  /// 包装直调 store——缺广播条目即主从换号域分叉；管理面未装配即显式回错
  pub(crate) async fn flush_command_slow(&self, cmd: RespCommand, args: &[Vec<u8>]) -> Vec<u8> {
    let refs: Vec<&[u8]> = args.iter().map(Vec::as_slice).collect();
    let mut output = Vec::new();
    let Ok(opts) = parse_flush_options(&refs) else {
      write_error_raw(&mut output, RESP_ERR_GENERIC_SYNTAX_ERROR);
      return output;
    };
    let _ = opts.async_flush;
    let caller_ns = self.session.namespace();

    // 门禁前置：物理截断日志属于全局破坏性运维动作，非 0 租户禁止执行
    if opts.unsafe_truncate_log && caller_ns != 0 {
      write_error_raw(
        &mut output,
        "-ERR permission denied: only namespace 0 can truncate log\r\n",
      );
      return output;
    }

    // 清库唯一漏斗：三条臂一律经常驻 SingleDatabaseManager（换号/截断执行段
    // + SafeFlushAOF 广播条目），对标 C# BasicCommands.cs:ExecuteFlushDb →
    // StoreWrapper.cs:FlushDatabase / StoreWrapper.cs:FlushAllDatabases →
    // SingleDatabaseManager.cs:FlushDatabase（内嵌 SafeFlushAOF(FlushDb)）。
    // 直调 store 换号只写 KeyTag::DbMeta 映射记录，而 AOF 写端口按标签只镜像
    // String / Acl（与信封墓碑）域（service.rs:on_aof_store_event），故该路径
    // 在日志里连条目都不留：副本侧无换号可回放，主从读写域自本次清库起分叉。
    // 管理面未装配时显式回错，绝不静默退回 store 直调
    // （宁可拒绝命令，不可制造主从发散）
    let Some(manager) = self.checkpoint.as_ref().map(|ctx| &ctx.database_manager) else {
      write_error_raw(&mut output, RESP_ERR_CHECKPOINT_UNWIRED);
      return output;
    };

    let flushed = if cmd == RespCommand::Flushdb {
      // O(1) 虚拟数据库换号秒级清库 + FlushDb 广播条目（载荷 (vns, 旧 vdb)）
      manager
        .flush_database(
          caller_ns,
          self.session.active_db(),
          opts.unsafe_truncate_log,
        )
        .await
        .map(|_| {
          self.session.set_active_db(self.session.active_db());
        })
    } else if caller_ns != 0 {
      // 非 0 租户 FLUSHALL：仅清空当前命名空间下的全部库（FlushNs 广播条目），
      // 绝不触碰其他租户与全域物理截断
      manager
        .flush_namespace(caller_ns, opts.unsafe_truncate_log)
        .await
        .map(|_| {
          self.session.set_active_db(self.session.active_db());
        })
    } else {
      // 超管 ns 0：全域物理截断 O(1) + FlushAll 广播条目
      manager.flush_all_databases(opts.unsafe_truncate_log).await
    };
    if flushed.is_err() {
      write_error_raw(&mut output, RESP_ERR_SLOW_PATH_STORAGE);
      return output;
    }
    // 超管执行 UNSAFETRUNCATELOG 物理截断历史段
    if opts.unsafe_truncate_log && self.session.store.truncate().await.is_err() {
      write_error_raw(&mut output, RESP_ERR_SLOW_PATH_STORAGE);
      return output;
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
  /// 提供者 is_slot_local 判定两库槽位均由本地掌管）放行后，与单机同流此异步
  /// 换号路径（doc/zh/db.md SWAPDB 条款）
  pub(crate) async fn swap_command_slow(&self, args: &[Vec<u8>]) -> Vec<u8> {
    let mut output = Vec::new();
    let (Some(idx1), Some(idx2)) = (
      args.first().and_then(|a| parse_db_index(a).ok()),
      args.get(1).and_then(|a| parse_db_index(a).ok()),
    ) else {
      write_error_raw(&mut output, RESP_ERR_ASYNC_REQUIRED);
      return output;
    };
    if idx1 == idx2 {
      output.extend_from_slice(RESP_OK);
    } else if active_resp_sessions_exceeds_one() {
      // C# TrySwapDatabases 活跃会话门控：RespServerSession 数 > 1 时拒绝换库
      write_error_raw(&mut output, RESP_ERR_SWAPDB_UNSUPPORTED);
    } else {
      let swapped = self
        .session
        .swap_databases(idx1 as i64, idx2 as i64)
        .await
        .is_ok();
      if swapped {
        output.extend_from_slice(RESP_OK);
      } else {
        write_error_raw(&mut output, RESP_ERR_SWAPDB_UNSUPPORTED);
      }
    }
    output
  }

  /// 检查点 / AOF 提交族慢路径执行段（AdminCommands.cs:NetworkSAVE/
  /// NetworkBGSAVE/NetworkLASTSAVE/NetworkCOMMITAOF；rust 映射为
  /// WedbStore::create_checkpoint（经 wkv 检查点通道）；LASTSAVE 为纯读取；
  /// COMMITAOF 为 AOF 物理刷盘提交）
  ///
  /// SAVE 同步等待检查点完成（C# NetworkSAVE：AsyncUtils.BlockingWait）；
  /// BGSAVE 对标 SingleDatabaseManager.cs:TakeCheckpointAsync(background=true)
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
          write_error_raw(&mut output, RESP_ERR_CHECKPOINT_UNWIRED);
          return output;
        };
        let _db_id = match args.first().map(Vec::as_slice) {
          None => -1,
          Some(arg) => match parse_db_index(arg) {
            Ok(idx) => idx as i64,
            Err(_) => {
              write_error_raw(&mut output, RESP_ERR_ASYNC_REQUIRED);
              return output;
            }
          },
        };
        if ctx.database_manager.commit_to_aof_async().await.is_err() {
          write_error_raw(&mut output, RESP_ERR_SLOW_PATH_STORAGE);
          return output;
        }
        output.write_resp_simple_string("AOF file committed");
      }
      C::Save | C::Bgsave => {
        let Some(ctx) = &self.checkpoint else {
          write_error_raw(&mut output, RESP_ERR_CHECKPOINT_UNWIRED);
          return output;
        };
        // C# NetworkSAVE/NetworkBGSAVE → storeWrapper.TakeCheckpointAsync →
        // SingleDatabaseManager.TakeCheckpointAsync：full 判定 + 版本推进
        // + AOF 安全截断 + CheckpointingLock 互斥，对齐常驻单例
        let Some(guard) = ctx.database_manager.try_acquire_checkpoint_guard() else {
          write_error_raw(&mut output, RESP_ERR_CHECKPOINT_ALREADY_IN_PROGRESS);
          return output;
        };
        if matches!(cmd, C::Bgsave) {
          let mgr_bg = Arc::clone(&ctx.database_manager);
          spawn(async move {
            let _guard = guard;
            match mgr_bg.take_checkpoint(false).await {
              Ok(true) => {}
              Ok(false) => log::warn!("background checkpoint skipped: paused"),
              Err(e) => log::warn!("background checkpoint failed: {e}"),
            }
          })
          .detach();
          // C# BGSAVE 成功应答文案
          output.write_resp_simple_string("Background saving started");
        } else {
          // 同步 SAVE：常驻 SingleDatabaseManager 占锁互斥 + RAII 自动还锁
          let res = ctx.database_manager.take_checkpoint(false).await;
          drop(guard);
          match res {
            Ok(true) => {
              output.extend_from_slice(RESP_OK);
            }
            Ok(false) => {
              write_error_raw(&mut output, RESP_ERR_CHECKPOINT_ALREADY_IN_PROGRESS);
            }
            Err(e) => {
              log::error!("SAVE checkpoint failed: {e}");
              write_error_raw(&mut output, RESP_ERR_SLOW_PATH_STORAGE);
            }
          }
        }
      }
      _ => unreachable!("checkpoint_command_slow 仅承接检查点族"),
    }
    output
  }
}
