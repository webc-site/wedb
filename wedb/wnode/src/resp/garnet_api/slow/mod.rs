//! 慢路径异步分派与通道调度

use std::sync::Arc;

use wdev::Device;
use wkv::SessionLocking;
use wresp::{
  cmd_strings::{RESP_ERR_ASYNC_REQUIRED, write_error_raw},
  command::RespCommand,
  metrics::InfoMetricsType,
};

use super::StoreGarnetApi;
use crate::{
  resp::{info_provider::InfoScanResult, vector::vector_manager::VectorManager},
  storage::session::storage_session::StorageSession,
};

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
/// 落盘失败）收场即补同源存储错误帧，即「调执行体 → 写回帧」骨架的单源。
/// 契约：执行体 Err 前必须自律撤回本命令已产出的部分应答段（先出帧后写回
/// 形态以 reply_start 锚 truncate，如 BITFIELD 慢臂），本宏仅追加不撤帧——
/// 后续新增慢臂严禁依赖本宏代撤
macro_rules! slow_arm {
  ($out:ident, $exec:path, $($arg:expr),+ $(,)?) => {{
    if $exec($($arg),+, &mut $out).await.is_err() {
      err_frame!($out);
    }
  }};
}

// 子模块声明置于宏定义之后：macro_rules 文本作用域覆盖此后声明的子模块，
// 各臂族子文件直接使用 err_frame! / bail_frame! / slow_arm!
mod admin;
mod array;
mod objects;
mod range_index;
mod scan;
mod string_bitmap;
mod vector;

impl<D: Device> StoreGarnetApi<D> {
  /// 向量登记表句柄（存在性四态探针与各对象族慢路径臂共用的单点取用形态，
  /// 未装配向量域的嵌入式形态即 None）
  #[inline]
  pub(super) fn vector_mgr(&self) -> Option<&VectorManager> {
    self.vector_session.as_ref().map(|v| v.manager.as_ref())
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
    session_locking: SessionLocking,
  ) -> Vec<u8> {
    use RespCommand as C;

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
    // 慢路径执行域：以 push_session_locking 守卫罩住整个慢体 await 段，
    // 事务重放段降级命令全程保持 Transactional 让闩复用，守卫退出即还原
    let _locking = self.session.push_session_locking(session_locking);
    let batch = self.session.enter_batch();
    let storage = StorageSession::new(batch)
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
        return self.string_command_slow(&storage, cmd, &refs).await;
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
        return self.bitmap_command_slow(&storage, cmd, &refs).await;
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
        return self.key_admin_command_slow(&storage, cmd, &refs).await;
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
        return self.etag_command_slow(&storage, cmd, &refs).await;
      }
      // ---- MSETNX 慢路径承接（C# MSET_Conditional 事务锁内全同步闭环的
      // rust 对偶：前缀外提 + 判定与批量写入折叠单窗口 + 中途失败条件回滚，
      // 全有或全无，应答与最终存储状态恒一致）
      C::Msetnx => {
        return self.msetnx_command_slow(&storage, &args, &refs).await;
      }
      // ---- GET 慢路径承接（同步段磁盘候选降级至此：read_user_batch_into 走
      // wkv 批量读口取 String 域值，TTL 门与逐条读口同源、在批量口内裁决，
      // 过期键视同不存在回 nil，冷命中键经磁盘冷读装载并惰性物理清除（对标 C#
      // InternalRead 的 ReadCache/磁盘冷读异步完成路径与 Reader 内
      // CheckExpiry）；多键时触发流水线冷读 Scatter-Gather (SG) 异步批量 I/O，
      // 对标 C# NetworkGET_SG。批量口确认缺失的键经三域判型续探（信封 / Meta
      // 域命中即集合对象键）改出 WRONGTYPE 错误帧且簿记静默——与快臂
      // read_user_sync 三域折叠同源，修复冷对象键回 nil+notfound 的双臂分叉
      //（对位 C# NetworkGET pending 收割后同一 Reader 判型，快慢无第二形态）
      C::Get => {
        return self.get_command_slow(&storage, &refs).await;
      }
      // ---- HyperLogLog 族慢路径承接（C# HyperLogLogOps.HyperLogLogAdd/
      // Length/Merge 的 RMW 语义：Tsavorite 磁盘候选挂起 pending 读后重放，
      // NOTFOUND 才允许新建。快路径 load_hll 磁盘候选降级至此：异步
      // read_tag_with 闭环冷区装载后再 RMW，杜绝把降级信号当缺失盲插覆盖）
      C::Pfadd | C::Pfcount | C::Pfmerge => {
        return self.hll_command_slow(&storage, cmd, &refs).await;
      }
      // ---- 全库扫描族（同步段仅校验，异步段承载实际扫描）
      C::Dbsize => {
        return self.dbsize_command_slow(&storage).await;
      }
      C::Keys => {
        return self.keys_command_slow(&storage, &refs).await;
      }
      C::Scan => {
        return self.scan_command_slow(&storage, &refs).await;
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
        return self
          .memory_usage_command_slow(&storage, &refs, resp_version)
          .await;
      }
      // ---- 集合对象长度异步直读闭环（HLEN/SCARD/ZCARD/LLEN 慢路径承接：
      // 同步段磁盘候选/异步 TTL 裁决/信封水位越线降级至此，头部直读计数
      // O(1) 零反序列化）
      C::Hlen | C::Scard | C::Zcard | C::Llen => {
        return self.objlen_command_slow(&storage, cmd, &refs).await;
      }
      // ---- 对象收集 `*` 全库族（C# AdminCommands.NetworkHCOLLECT /
      // NetworkZCOLLECT：C# ObjectCollect 游标分批 + 批内逐键 RMW 的慢路径承接；
      // 信封域与分层态两域匹配键同批收齐，分层键经树内收集执行体物理出账）
      C::Hcollect | C::Zcollect => {
        return self.collect_command_slow(&storage, cmd, &refs).await;
      }
      // ---- 自定义对象命令族（C# CustomRespCommands.TryCustomObjectCommand
      // 的异步承接：同步段磁盘候选降级重放。快照尾参为命令名，经与快路径
      // 解析、ACL 校验同一张编译期静态清单回查后按同款四接口执行；
      // 未启用任一扩展时清单为空表 → 未知命令兜底臂）
      #[cfg(any(feature = "roaring", feature = "json"))]
      C::Customobjcmd => {
        return self.custom_object_command_slow(&storage, &refs).await;
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
        return self
          .range_index_command_slow(cmd, &refs, resp_version)
          .await;
      }
      // ---- 过期键删除扫描（C# AdminCommands.NetworkEXPDELSCAN：C# 阻塞
      // 等待 storeWrapper.ExpiredKeyDeletionScan；应答 *2 计数对
      // `*2\r\n$N\r\n<expired>\r\n$N\r\n<scanned>\r\n`，DBID 已在快
      // 路径 try_parse_database_id 校验，此处防御性重解析）
      C::Expdelscan => {
        return self.expdelscan_command_slow(&refs).await;
      }
      // ---- TYPE / LCS 慢路径承接（C# ArrayCommands.NetworkTYPE / NetworkLCS
      // 的 Read_UnifiedStore pending 就地闭环 rust 对偶：快路径三域判型 /
      // 双键读磁盘候选降级至此，应答形态与快路径逐字节一致）
      C::Type => {
        return self.type_command_slow(&storage, &refs).await;
      }
      C::Lcs => {
        return self.lcs_command_slow(&storage, &refs, resp_version).await;
      }
      // ---- 批量操作族（C# ArrayCommands.NetworkDEL/NetworkMGET/NetworkMSET
      // 的 Tsavorite pending 读/环形页翻转 CompletePending 重放承接）
      C::Del | C::Unlink => {
        return self.del_command_slow(&storage, &refs).await;
      }
      C::Mget => {
        return self.mget_command_slow(&storage, &refs).await;
      }
      C::Mset => {
        return self.mset_command_slow(&storage, &refs).await;
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
        return self.hash_command_slow(&storage, cmd, &refs).await;
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
        return self.set_command_slow(&storage, cmd, &refs).await;
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
        return self.list_command_slow(&storage, cmd, &refs).await;
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
        return self.sorted_set_command_slow(&storage, cmd, &refs).await;
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
        return self.geo_command_slow(&storage, cmd, &refs).await;
      }
      // ---- 对象扫描族（C# SharedObjectCommands.ObjectScan 慢路径承接；
      // 快照尾参为 4 字节 LE COUNT 上限，exec 降级快照追加）
      C::Hscan | C::Sscan | C::Zscan | C::Coscan => {
        return self.object_scan_command_slow(&storage, cmd, &refs).await;
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
        return self
          .vector_read_command_slow(&storage, cmd, &refs, resp_version)
          .await;
      }
      // ---- 向量写族挂起闭环（VADD / VSETATTR：插入/属性写链为 compio 存储
      // 异步操作，同步段 inline_wait 内联收割移除后 Allow 态参数快照挂起
      // SlowWait 转投本臂——对标 cluster 链 pending_slow 转挂先例；慢臂
      // network_vector_write_slow 真读复判键域后闭环，poll 边界的执行域
      // 绑定由入口 SlowPollSessionBound 包装承接）
      C::Vadd | C::Vsetattr | C::Vrem => {
        return self
          .vector_write_command_slow(&storage, cmd, &refs, resp_version)
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
}
