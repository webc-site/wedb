//! 主端 diskless 快照共享迭代器与逐会话锁步扇出 (SnapshotIteratorManager)
//!
//! 在 garnet 中的相对路径: libs/cluster/Server/Replication/PrimaryOps/DisklessReplication/ReplicationSnapshotIterator.cs
//!
//! 对标 C# `SnapshotIteratorManager`：单遍存储迭代产记录，逐记录经
//! FanOutRecordSpan / FanOutChunk 同步锁步写入本批全部会话（C# 逐会话迭代
//! 缓冲 + 缓冲满全批冲刷等齐重试；rust 停等批模型收敛为单共享攒批——锁步
//! 不变量「同一字节流写全部会话、任一满即全体冲刷」使各会话缓冲内容恒同，
//! 攒批与编码每批一次、发送逐会话并发停等，序列化一次广播 N 副本）。
//! C# `StoreSnapshotIterator`（同文件 :264 的 IStreamingSnapshotIterator-
//! Functions 消费者壳）在本模块收敛为 [`run_snapshot_fanout`] 的扫描主体；
//! OnStart 初始化逐会话迭代缓冲对应共享攒批的构造复位，OnStop 统一收尾
//! 对应末批冲刷 + 完成哨兵，OnException 失败收敛对应逐扇出点判败摘除。
//!
//! rust 快照源为活存储 live scan（get_keys_in_slot 库级单遍 + read_live_value
//! 活值分类），非 C# Tsavorite StreamingSnapshot 检查点迭代。活扫描 + 起点锚
//! 的组合以扫描键门收口锚点窗口：扇出扫描逐批置键门（Blocking 相全阻 →
//! 纪元静止排空 → 门内取锚 → Scanning 相按未读键集栅放、读值装帧即释门），
//! 使「记录效果进快照 ⟺ 记录地址 ≤ 锚」对增量语义记录（ObjectStoreRMW 的
//! ReplayInput 载荷，重放非幂等）结构成立——锚后并发写绝不既进快照又经 AOF
//! 续推被双重应用，窗口语义详见 [`super::scan_key_gate`] 模块头。wbftree
//! 页存储集合树（RangeIndex 与升阶分层集合）与向量集带外流为本仓相对 C#
//! 的扩展面：树分块帧副本无关可整帧广播（树快照字节流类型无关，对象类型
//! 与 TTL 随流元携载）；向量集上下文按副本预留、索引重映射逐副本各异，故
//! 源端导出单遍、逐副本各自编码发送（不共享载荷）。
//!
//! 跨全量活跃域扇出（C# 整 store 流式检查点天然全库覆盖；rust 活扫描按域
//! 编排补齐同覆盖面，对标 demote 后台轮的跨域扫描先例）：扫描前经
//! [`wkv::WedbStore::active_domains`] 枚举全部活跃域（在册租户 × 在册库，
//! 冷租户窗口外预热路由回建，死亡域剔除），逐域切上下文复用单域路径枚举
//! 活键并域钉至枚举时物理域（见下「域钉」），向量段按覆盖域展开 db_slots；
//! 快照链不重放锚前 AOF，副本域映射体系经 DbMeta 映射帧（[`wkv::DbMetaRecord`]
//! 单点编码，接收端
//! apply_dbmeta_record 单点应用）与逐载荷落域上下文帧收敛——映射帧/上下文
//! 帧为本仓多租户扩展（C# 单租户单库无此面），杜绝「副本报成功却静默永久
//! 缺域」。摘要日志携带覆盖域清单，杜绝静默缺域。
//!
//! 域钉＝窗内禁换绑的执行面承接：换号族（FLUSHDB/FLUSHNS/SWAPDB）为域重绑
//! 本体、无用户键可栅、不经扫描键门（门谓词只挂在键位判定核，见
//! [`super::scan_key_gate`] 模块头），键门 Blocking 相「域清单与逐域键集即
//! 锚点时刻的完整快照态」的成立前提须由本件承接——装载与读值的逐域切上下文
//! 在逻辑物化后一律经 [`wkv::StoreSession::set_virtual_context`] 紧随域钉
//! 枚举时物理对 `(vns, vdb)`：`is_virtual` 旁路代数守卫后，窗内任何换代/
//! 换指对本会话键集枚举、逐键读值与记录前缀结构无感，帧戳、装载、读值、
//! 释门四点恒等枚举时刻物理域，快照即锚时物理域投影，换号族效果改由「锚时
//! 物理域投影 + 锚后绝对值记录续推」单律收敛（FLUSHDB 形旧域全量入帧随
//! GcDeadDb 整域退役；SWAPDB 形读回锚时真域，随 DbSwap 成对回放扳指后主从
//! 两域内容逐键全等）——本律系向量段按枚举物理域直配的既有正确形态向
//! 数据段的推广。

use std::{
  collections::BTreeSet,
  sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
  },
};

use compio::time::timeout;
use futures_util::future::join_all;
use parking_lot::Mutex;
use waof::AofAddress;
use wbase::{hash_slot::slot_of, hex::hex_str_u128, map::HashMap as GxHashMap};
use wbftree::DEFAULT_MIGRATION_CHUNK_SIZE;
use wconn::record::{
  BatchItem, MigrateVectorElement, MigrationDomainContext, encode_dbmeta_frame,
  encode_frame_payload, encode_migration_payload, encode_vector_set_element_payload,
  encode_vector_set_index_payload, prepend_migration_frame, send_chunked_record,
};
use wkv::DbMetaRecord;
use wnode::{
  StorageSession,
  resp::vector::{
    vector_manager::{INDEX_SIZE_BYTES, VectorManager},
    vector_manager_index::Index,
    vector_manager_locking::split_registry_key,
  },
};

use super::scan_key_gate::{ScanGateGuard, ScanKeyGate};
use crate::{
  client::GarnetClient,
  error::Error,
  server::{
    cluster_provider::{ClusterProvider, PrimaryReplicationAssets},
    migration::migrate_driver::{LiveValue, read_live_value},
    replication::{
      diskless_replication::{replica_sync_session::DisklessSyncSession, sync_status::SyncStatus},
      replica_wire::REPLICA_SYNC_TIMEOUT,
    },
    sync_transport::{
      MAX_MIGRATION_BATCH_COUNT, export_vector_set_elements, transmit_range_index_stream,
    },
  },
};

/// 单遍快照迭代 + 逐会话锁步扇出管理器
struct SnapshotIteratorManager {
  /// 扇出帧源节点 id（协议面渲染 hex，同原单副本路径）
  source_node_hex: String,
  /// 本批参与快照扇出的活跃全量会话（判败即摘除，对标 C# IsActive 收敛）
  sessions: Mutex<Vec<Arc<DisklessSyncSession>>>,
}

impl SnapshotIteratorManager {
  fn new(local_node_id: u128, sessions: Vec<Arc<DisklessSyncSession>>) -> Self {
    Self {
      source_node_hex: hex_str_u128(local_node_id),
      sessions: Mutex::new(sessions),
    }
  }

  /// 活跃会话快照（终态会话摘除后返回，对标 C# IsActive 逐位检查）
  fn active_sessions(&self) -> Vec<Arc<DisklessSyncSession>> {
    let mut guard = self.sessions.lock();
    guard.retain(|s| !s.is_terminal());
    guard.clone()
  }

  /// 同一载荷广播全部活跃会话（rust 停等形态：逐会话并发发送、全批等齐
  /// 返回；任一会话发送失败/超时/被拒即判败摘除，其余会话继续——对标 C#
  /// SetFlushTask 失败收敛 + WaitForFlushAsync 内
  /// `if (Sessions[i].Failed) Sessions[i] = null`）。
  /// libs/cluster/Server/Replication/PrimaryOps/DisklessReplication/ReplicationSnapshotIterator.cs:FanOutRecordSpan
  ///
  /// 整记录一次广播本口；分块扇出（C# FanOutChunk）由 run 的超大记录分支
  /// 逐块回调本口，锚点落该分支注释，两枚不叠挂同一函数
  ///
  /// `domain` 为 Some 时载荷首帧前插落域上下文帧——接收会话逐命令重建，
  /// 跨域扇出载荷必须逐载荷携带落域上下文（DbMeta 映射帧与完成哨兵除外）
  async fn fan_out_send(
    &self,
    domain: Option<MigrationDomainContext>,
    payload: &[u8],
  ) -> Result<(), String> {
    let prefixed: Vec<u8>;
    let payload = match domain {
      Some(ctx) => {
        prefixed = prepend_migration_frame(&ctx.encode_frame(), payload);
        &prefixed
      }
      None => payload,
    };
    let targets = self.active_sessions();
    if targets.is_empty() {
      return Err(ALL_SESSIONS_FAILED.to_string());
    }
    let hex = &self.source_node_hex;
    join_all(targets.iter().map(|s| async {
      let client = s.client().ok_or("snapshot session client not connected");
      let outcome: Result<(), String> = match client {
        Err(msg) => Err(msg.to_string()),
        Ok(client) => {
          match timeout(
            REPLICA_SYNC_TIMEOUT,
            client.execute_cluster_sync(hex, payload),
          )
          .await
          {
            Err(_) => Err("CLUSTER SYNC timeout".to_string()),
            Ok(Err(e)) => Err(format!("CLUSTER SYNC failed: {e}")),
            Ok(Ok(false)) => Err("CLUSTER SYNC rejected by replica".to_string()),
            Ok(Ok(true)) => Ok(()),
          }
        }
      };
      if let Err(msg) = outcome {
        s.set_status(SyncStatus::Failed, Some(msg));
      }
    }))
    .await;
    if self.active_sessions().is_empty() {
      return Err(ALL_SESSIONS_FAILED.to_string());
    }
    Ok(())
  }

  /// 单遍跨域活扫描快照扇出主体（对标 C# WriteRecord 消费循环 +
  /// MainStreamingSnapshotDriverAsync 内 TakeStreamingCheckpointAsync 触发；
  /// C# 整 store 流式检查点天然全库覆盖，rust 以活跃域枚举 + 逐域单域路径
  /// 编排补齐同覆盖面）；任一键无法承载即上抛中止全量同步——半快照副本
  /// 绝不放行转入增量，杜绝「同步成功却永久缺键」的主从无痕发散
  async fn run(
    &self,
    provider: &Arc<ClusterProvider>,
    assets: &PrimaryReplicationAssets,
  ) -> Result<(), String> {
    let store = provider
      .try_store()
      .ok_or_else(|| "store not initialized".to_string())?;
    let session = store
      .new_session()
      .map_err(|e| format!("session error: {e}"))?;
    let batch = session.enter_batch();
    let storage = StorageSession::new_readonly(batch);
    // 单块/装批内容上限：与迁移共用同一真源（对标 C#
    // NetworkBufferSettings.MaxSendBufferContentSize = sendBufferSize -
    // SendBufferOverheadReserve 公式；rust 只建一份发送缓冲规格，故走
    // cluster_provider 单点读取，migration_manager 未装配时回退同源缺省）
    let max_chunk = provider.max_send_buffer_content_size();

    // ===== 扫描键门开窗（Blocking 相：全部键写命令一律挂起；换号族无用户键
    // 可栅不经门，窗内域重绑由读值域钉承接，见模块头「域钉」）=====
    // 活扫描 + 起点锚的收敛保证要求「记录效果进快照 ⟺ 记录地址 ≤ 锚」对
    // 增量语义记录（ObjectStoreRMW 的 ReplayInput 载荷，重放非幂等）结构
    // 成立：锚点后、扫描读值前的并发写若落库，其效果既进快照、其记录（地址
    // 恒 > 锚）又被重放，副本双重应用永久发散。键门两相栅写（窗口语义见
    // [`super::scan_key_gate`] 模块头）：Blocking 相全阻排空在途写后取锚，
    // 锚后按覆盖域未读键集栅放、读值装帧即释门——被门键的写效果绝不进快照，
    // 恰经 AOF 续推应用一次
    let rm = provider
      .replication_manager()
      .ok_or_else(|| "replication manager not initialized".to_string())?;
    let gate = Arc::new(ScanKeyGate::new());
    let _gate_guard = ScanGateGuard::register(&rm.replication_sync_manager, Arc::clone(&gate))?;
    // 读窗留钩计数按轮复位（臂位仅定向时序用例设置，见模块尾静态注）
    TEST_SNAPSHOT_READ_COUNT.store(0, Ordering::Relaxed);
    // 覆盖域路由预热（窗口外）：冷租户磁盘路由回建的全部 I/O 挪出键门
    // Blocking 窗，窗口内重取即纯内存命中
    store
      .active_domains()
      .await
      .map_err(|e| format!("domain route warmup failed: {e}"))?;
    // 纪元静止等待：等 Blocking 门对全会话生效、批内在途写排空（对标迁移
    // 传输窗 MigrateSessionSlots.cs:236 WaitForConfigPropagationAsync 同款
    // 原语；返值承判：C# 无限自旋必达，rust 以 cluster_node_timeout 有界，
    // false = 静止未达成——键门窗口不变量「记录效果进快照 ⟺ 记录地址 ≤
    // 锚」以排空达成为前提，未达成即锚照样起算、批内写会话后续提交记录
    // 地址恒 > 锚而其效果可被读值折叠进快照，非幂等记录双重应用静默发散，
    // 故判败重拍：上抛经既有 Err 收敛臂全员 FAILED、守卫 Drop 注销整门
    // 放行挂起写，副本沿节流重连整备跑下一批窗）
    if !provider.bump_and_wait_for_epoch_transition_async().await {
      return Err(
        "diskless scan gate: epoch drain not settled within cluster-node-timeout".to_string(),
      );
    }
    // 快照覆盖锚 = 键门闭窗排空点的日志尾（对标 C# SnapshotIteratorManager
    // 构造期 CheckpointCoveredAddress = Log.TailAddress，ReplicationSnapshot-
    // Iterator.cs:56-61；rust 取锚自 stream_sync 扇出前挪入门窗之内——锚点
    // 必须后于键门闭窗、先于任何读值），逐会话赋同值：锚前记录效果已含于
    // 快照，AOF 推流恰从锚起续推（授予位点 = 锚）
    let PrimaryReplicationAssets { wal, .. } = assets;
    let snapshot_anchor = AofAddress::create(rm.sublog_count() as i32, wal.tail_address() as i64);
    for s in self.active_sessions() {
      s.set_checkpoint_covered_aof_address(snapshot_anchor);
    }

    // ===== 全量活跃域枚举（Blocking 相下无并发键写可完成：域清单与逐域键集
    // 即锚点时刻的完整快照态；换号族窗内换绑由后续读值域钉承接，模块头）=====
    // 在册租户 × 在册库逐域展开（冷租户已窗口外预热，此处零磁盘 I/O），
    // 死亡域剔除；覆盖清单含仅存向量集登记、数据已清空的空域（空域枚举
    // 零键零扇出成本，映射帧仍携带——副本映射体系须与主端一致）。库级定槽
    // doc/zh/db.md 4.1：会话库全部键恒共会话槽位，单次列举覆盖整域
    let domains = store
      .active_domains()
      .await
      .map_err(|e| format!("domain enumeration failed: {e}"))?;

    // 域映射收敛帧（先行单载荷）：快照链不重放锚前 AOF，副本映射体系经
    // DbMeta 映射帧以 apply_dbmeta_record 单点收敛（映射装载 + 水位抬升 +
    // 本节点落盘，与 AOF 镜像条目回放同一应用口）——FLUSHALL 清库复位帧
    // 已重置副本映射，锚前全部域映射唯此一途可达；根域 (0, 0) 内建免映射
    // 帧，分配水位（NextId）恒随载荷下发杜绝副本本地新号撞号
    let mut meta_frames: Vec<Vec<u8>> = Vec::new();
    for d in &domains {
      if d.ns != 0 {
        let rec = DbMetaRecord::NsMap {
          logic_ns: d.ns,
          vns: d.vns,
        };
        meta_frames.push(encode_dbmeta_frame(
          rec.key().as_slice(),
          rec.value().as_slice(),
        ));
      }
      if d.ns != 0 || d.db != 0 {
        let rec = DbMetaRecord::DbMap {
          vns: d.vns,
          logic_db: d.db,
          vdb: d.vdb,
        };
        meta_frames.push(encode_dbmeta_frame(
          rec.key().as_slice(),
          rec.value().as_slice(),
        ));
      }
    }
    let next_id = DbMetaRecord::NextId {
      next_virtual_id: store.vdb.next_virtual_id.load(Ordering::Relaxed),
    };
    meta_frames.push(encode_dbmeta_frame(
      next_id.key().as_slice(),
      next_id.value().as_slice(),
    ));

    // 逐域装载未读键集（Blocking 相内逐域切上下文复用单域枚举路径；全部
    // 域装载后始可切相，杜绝「已切 Scanning 而键集未装载」的空窗放行）
    // 切上下文=逻辑物化（冷检/路由重绑语义原样）+ 紧随域钉枚举时物理对：
    // is_virtual 旁路代数守卫后，窗内换号族换指/换代对本会话键集枚举与
    // 后续读值结构无感（域钉律见模块头）
    // 装载窗留钩单点拾取：活跃域枚举已成、首域切上下文之前（测试定序注入，
    // 生产恒 None 一次无争锁读，见 TEST_SNAPSHOT_LOAD_HOOK 注）
    let load_hook = TEST_SNAPSHOT_LOAD_HOOK.lock().take();
    if let Some(load_hook) = load_hook {
      load_hook();
    }
    let mut covered: Vec<DomainKeys> = Vec::with_capacity(domains.len());
    for d in &domains {
      if !session.set_context(d.ns, d.db) {
        return Err(format!(
          "cold db context switch failed: ns {}/db {}",
          d.ns, d.db
        ));
      }
      // 域钉直设臂显式携逻辑域真值（d.ns/d.db 即载荷记录域，版本轨=逻辑域
      // 种子的透传点，禁经映射反查）
      session.set_virtual_context(d.vns, d.vdb, d.ns, d.db);
      let slot = slot_of(d.ns, d.db);
      let keys = storage
        .get_keys_in_slot(slot, usize::MAX)
        .await
        .map_err(|e| format!("snapshot scan failed: {e}"))?;
      gate.admit_domain(slot, &keys);
      covered.push(DomainKeys {
        ctx: MigrationDomainContext {
          vns: d.vns,
          vdb: d.vdb,
          ns: d.ns,
          db: d.db,
        },
        slot,
        keys,
      });
    }
    gate.begin_scan();

    // 映射收敛载荷先行发送（帧与落域上下文无关，无上下文前插）
    self
      .fan_out_send(None, &encode_frame_payload(&meta_frames))
      .await?;

    let mut cur_batch: Vec<BatchItem<'_>> = Vec::new();
    let mut cur_batch_bytes = 0usize;

    // 装批冲刷：整批一次编码广播全部活跃会话（序列化一次、发送逐会话；
    // 批字节计数复位由调用点按需跟进；载荷随落域上下文前插）
    macro_rules! flush_batch {
      ($ctx:expr) => {
        if !cur_batch.is_empty() {
          let payload = encode_migration_payload(&cur_batch);
          self.fan_out_send(Some($ctx), &payload).await?;
          cur_batch.clear();
        }
      };
    }

    // 逐域扫描扇出（Scanning 相：读值装帧即逐域逐键释门）；切上下文同
    // 装载侧——逻辑物化后紧随域钉枚举时物理对，帧戳/键集/读值/释门四点
    // 恒等枚举时刻物理域（域钉律见模块头）
    for dk in &covered {
      let DomainKeys { ctx, slot, keys } = dk;
      if !session.set_context(ctx.ns, ctx.db) {
        return Err(format!(
          "cold db context switch failed: ns {}/db {}",
          ctx.ns, ctx.db
        ));
      }
      // 扫描侧域钉与装载侧同源：ctx.ns/ctx.db 即枚举时刻逻辑域真值透传
      session.set_virtual_context(ctx.vns, ctx.vdb, ctx.ns, ctx.db);
      for key in keys {
        // 读窗留钩（测试定序注入，生产恒零负担，见 TEST_SNAPSHOT_READ_HOOK 注；
        // 取钩即释锁再回调，杜绝钩体经 wkv 会话路径重入本锁自锁）
        if TEST_SNAPSHOT_READ_AT.load(Ordering::Relaxed) != usize::MAX
          && TEST_SNAPSHOT_READ_COUNT.fetch_add(1, Ordering::Relaxed)
            == TEST_SNAPSHOT_READ_AT.load(Ordering::Relaxed)
        {
          let hook = TEST_SNAPSHOT_READ_HOOK.lock().take();
          if let Some(hook) = hook {
            hook();
          }
        }
        // 活值分类读取：string/合规信封域自带惰性过期裁决，真实过期时间戳
        // 一并提取装帧（对标 C# 快照迭代整记录搬运含 RecordDataHeader.expiration）
        match read_live_value(&storage, None, key).await {
          Ok(LiveValue::Migratable(val, expire_unix_ms)) => {
            // 读值装帧即释门：整值已物化（MigrateVal 全量持有），该键锚后并发
            // 写放行落库——效果不入快照（读值已定）、记录地址恒 > 锚，副本
            // 恰经 AOF 续推应用一次
            gate.release_key(*slot, key);
            let item = BatchItem {
              key,
              val,
              expire_unix_ms,
            };
            let frame_len = item.frame_len();
            if frame_len > max_chunk {
              // 超大记录切块广播（分块帧副本无关，逐块锁步等齐）
              // libs/cluster/Server/Replication/PrimaryOps/DisklessReplication/ReplicationSnapshotIterator.cs:FanOutChunk
              // 的 moreChunksFollow 续块语义：每块经 fan_out_send 广播
              flush_batch!(*ctx);
              cur_batch_bytes = 0;
              send_chunked_record(&item, max_chunk, async |payload| {
                self
                  .fan_out_send(Some(*ctx), payload)
                  .await
                  .map_err(Error::InvalidArgument)
              })
              .await
              .map_err(|e| e.to_string())?;
              continue;
            }
            if !cur_batch.is_empty()
              && (cur_batch.len() >= MAX_MIGRATION_BATCH_COUNT
                || cur_batch_bytes + frame_len > max_chunk)
            {
              flush_batch!(*ctx);
              cur_batch_bytes = 0;
            }
            cur_batch_bytes += frame_len;
            cur_batch.push(item);
          }
          // wbftree 页存储集合键（RangeIndex 与升阶分层集合共用）：树快照走
          // RangeIndexStream 带外分块帧，副本端接收态流式落盘按流元携载的
          // 真实对象类型与 TTL 原子发布（对标 C# 快照迭代 RangeIndexRecordType
          // 分流 + RangeIndexMigrationReceiveSession 承接，rust 泛化到全部
          // 页存储树）；分块帧副本无关，整帧广播锁步发送，失败即中止全量
          // 同步，半快照副本绝不放行转入增量
          Ok(LiveValue::TieredTree) => {
            flush_batch!(*ctx);
            cur_batch_bytes = 0;
            let ok = transmit_range_index_stream(
              &session,
              key,
              DEFAULT_MIGRATION_CHUNK_SIZE,
              async |payload| {
                self
                  .fan_out_send(Some(*ctx), payload)
                  .await
                  .map_err(Error::InvalidArgument)
              },
            )
            .await
            .map_err(|e| e.to_string())?;
            if !ok {
              return Err(format!(
                "failed to snapshot tree for key {}",
                String::from_utf8_lossy(key)
              ));
            }
            // 树快照流式读树非单点物化，门护至整流送达后释门（失败臂由守卫
            // Drop 整门注销收口）
            gate.release_key(*slot, key);
          }
          // 未知信封内层类型（仅数据损坏可达，诚实写入端不产此类信封）：
          // 无法判定载荷语义，上抛中止本轮扇出——跳键即副本永久缺键，
          // 与读失败同口径（诚实发送端经 read_live_value 门控不触达此臂）
          Ok(LiveValue::Unsupported(kind_label)) => {
            return Err(format!(
              "failed to snapshot key {}: unsupported envelope type {kind_label}",
              String::from_utf8_lossy(key)
            ));
          }
          // 存储读失败：上抛中止本轮扇出（读失败跳键=静默丢数据，比缺不支持
          // 键更不可容忍；沿 RI 臂 `!ok return Err` 通道，半快照副本绝不放行
          // 转入增量）
          Err(e) => {
            return Err(format!(
              "failed to read live value for key {}: {e}",
              String::from_utf8_lossy(key)
            ));
          }
          // 竞态消亡（扫描后被并发删除/过期/改写，无帧可发）与向量集（本扫描
          // vm 恒 None 不可达，向量集走独立段发现面）：无快照内容，释门放行
          Ok(LiveValue::Gone | LiveValue::VectorSet) => gate.release_key(*slot, key),
        }
      }
      flush_batch!(*ctx);
      cur_batch_bytes = 0;
    }

    // 向量集段：索引记录驻留向量管理器登记表、存储槽扫描不可见，按覆盖域
    // 槽位集发现面独立装帧（对标 C# MigrateOperation.EncounteredVectorSet
    // 传输面分发）。上下文按副本预留、索引重映射逐副本各异——源端导出单遍，
    // 逐副本编码发送并发等齐；全体判败即中止
    if let Some(vm) = provider.try_vector_manager().as_deref() {
      let db_slots: BTreeSet<i32> = covered.iter().map(|dk| i32::from(dk.slot)).collect();
      let vector_sets = vm.get_vector_set_keys_for_slots_with(&db_slots, |vns, vdb| {
        covered
          .iter()
          .find(|dk| (dk.ctx.vns, dk.ctx.vdb) == (vns, vdb))
          .map(|dk| dk.slot)
      });
      // 按物理域分组（登记表复合键单点解域）：逐域一组，落域上下文随组前插
      // 逐载荷携带；登记域不在覆盖清单（换号退役域滞留登记）即跳过留痕——
      // 死亡域数据对副本不可见是正确行为，但静默跳过必须留痕
      let mut by_domain: Vec<(MigrationDomainContext, Vec<VectorSetSnapshot>)> = Vec::new();
      for (rk, src_index) in &vector_sets {
        let (domain, user_key) = split_registry_key(rk);
        let Some(dk) = covered
          .iter()
          .find(|dk| (dk.ctx.vns, dk.ctx.vdb) == (domain.vns, domain.vdb))
        else {
          log::warn!(
            "无盘全量同步快照向量集登记域不在活跃覆盖清单，跳过: vns={} vdb={}",
            domain.vns,
            domain.vdb
          );
          continue;
        };
        let entry = match by_domain
          .iter_mut()
          .find(|(ctx, _)| (ctx.vns, ctx.vdb) == (domain.vns, domain.vdb))
        {
          Some((_, snaps)) => snaps,
          None => {
            by_domain.push((dk.ctx, Vec::new()));
            &mut by_domain.last_mut().unwrap().1
          }
        };
        entry.push(VectorSetSnapshot {
          key: user_key.to_vec(),
          src_index: *src_index,
          // 源端导出经传输件单臂自备专用会话（本核跑在快照后台线程，不经命令
          // 面绑定段；元素记录键前缀随绑定会话域落位，故逐集按登记条目自带域
          // 落位——见 sync_transport::export_vector_set_elements）
          elements: export_vector_set_elements(&store, vm, rk, src_index)
            .await
            .map_err(|e| format!("vector set export session error: {e}"))?,
        });
      }
      for (ctx, snapshots) in &by_domain {
        if snapshots.is_empty() {
          continue;
        }
        let targets = self.active_sessions();
        join_all(targets.iter().map(|s| async {
          let outcome = transmit_vector_sets_to_session(
            s,
            vm,
            snapshots,
            *ctx,
            &self.source_node_hex,
            max_chunk,
          )
          .await;
          if let Err(msg) = outcome {
            s.set_status(SyncStatus::Failed, Some(msg));
          }
        }))
        .await;
        if self.active_sessions().is_empty() {
          return Err(ALL_SESSIONS_FAILED.to_string());
        }
      }
    }

    // 发送完成哨兵空载荷 (recordCount = 0)（对标 C# OnStop 统一收尾冲刷）
    let sentinel_payload = encode_migration_payload(&[]);
    self.fan_out_send(None, &sentinel_payload).await?;

    // 覆盖域清单摘要（杜绝静默缺域：同步成功即意味着清单内全部域的锚前
    // 存量已扇出）
    log::info!(
      "无盘全量同步快照完成，覆盖 {} 个活跃域: [{}]",
      covered.len(),
      covered
        .iter()
        .map(|dk| format!("ns{}/db{}", dk.ctx.ns, dk.ctx.db))
        .collect::<Vec<_>>()
        .join(", ")
    );
    Ok(())
  }
}

/// 单域扫描产物（落域上下文 + 库级槽位 + 锚点时刻的域内活键集）
struct DomainKeys {
  /// 落域上下文（物理 + 逻辑域四标量，随载荷前插）
  ctx: MigrationDomainContext,
  /// 库级槽位（库级定槽 doc/zh/db.md 4.1，键门分域登记与向量发现共用）
  slot: u16,
  /// 域内活键集（键门 Blocking 相下枚举，即锚点时刻快照态）
  keys: Vec<Vec<u8>>,
}

/// 全员判败统一文案
const ALL_SESSIONS_FAILED: &str = "all diskless sync snapshot sessions failed";

/// 快照读窗留钩（一次性，[`wkv::TEST_COLD_WINDOW_HOOK`] 同族，先例
/// wkv/src/session/mod.rs:64）：本轮逐键读值计数触 [`TEST_SNAPSHOT_READ_AT`]
/// 定臂位时于该键读值前回调，供「换号族介入快照窗」三点定向时序用例定序
/// 注入（落位甲走本模块 [`TEST_SNAPSHOT_LOAD_HOOK`]，落位乙/丙走本钩：臂位
/// 0 = 装载后首键读值前、臂位 1 = 前两键读值之间）。生产路径臂位恒
/// [`usize::MAX`]、钩槽恒 None，判位仅一次 Relaxed 原子读，零负担；业务
/// 代码禁止触碰
#[doc(hidden)]
pub static TEST_SNAPSHOT_READ_HOOK: Mutex<Option<Box<dyn FnOnce() + Send>>> = Mutex::new(None);

/// 快照装载窗留钩（一次性，[`TEST_SNAPSHOT_READ_HOOK`] 同族）：全量活跃域
/// 枚举已成、首域切上下文装载前的间隙内回调，供「换号族介入快照窗」落位甲
/// 用例定序注入——「枚举后、装载前」本即 [`wkv::TEST_COLD_WINDOW_HOOK`]
/// 借道 set_context 冷检窗的原始落点，但该槽任意会话创建皆可消费（副本
/// 宿主 get_session 新建会话即先抢），且钩体经会话创建重入 set_context 持槽
/// 锁回调必自锁，定序无保证；改由本模块装载窗单点拾取，快照任务栈内同步
/// 触发无竞态。生产路径恒 None，判位仅一次无争锁读；业务代码禁止触碰
#[doc(hidden)]
pub static TEST_SNAPSHOT_LOAD_HOOK: Mutex<Option<Box<dyn FnOnce() + Send>>> = Mutex::new(None);

/// 留钩触发读序位（`usize::MAX` = 关闭；[`SnapshotIteratorManager::run`]
/// 起点复位计数，跨轮不累积）
#[doc(hidden)]
pub static TEST_SNAPSHOT_READ_AT: AtomicUsize = AtomicUsize::new(usize::MAX);

/// 本轮快照逐键读值计数（仅留钩臂位非关闭时递增）
static TEST_SNAPSHOT_READ_COUNT: AtomicUsize = AtomicUsize::new(0);

/// 向量集源端单遍导出快照（键 + 源索引字节 + 导出元素三段）
struct VectorSetSnapshot {
  key: Vec<u8>,
  src_index: [u8; INDEX_SIZE_BYTES],
  elements: Vec<(Vec<u8>, Vec<u8>, Vec<u8>)>,
}

/// 逐副本向量集装帧发送：目标端上下文预留 → 源→目标重映射 → 索引帧 +
/// 元素批帧停等（口径同 sync_transport::transmit_vector_set_frames，差异
/// 在源端导出已由调用方单遍完成，本函数只做逐副本编码与发送）；每帧载荷
/// 首帧前插落域上下文——接收会话逐命令重建，向量登记前缀与登记槽均随
/// 落域上下文收敛
async fn transmit_vector_sets_to_session(
  session: &Arc<DisklessSyncSession>,
  vm: &VectorManager,
  snapshots: &[VectorSetSnapshot],
  ctx: MigrationDomainContext,
  source_node_hex: &str,
  max_chunk: usize,
) -> Result<(), String> {
  let client = session
    .client()
    .ok_or("snapshot session client not connected")?;
  // 目标端上下文预留 + 源→目标重映射表（一键一上下文；预留应答序与键序对齐）
  let reserved: Vec<u64> = match timeout(
    REPLICA_SYNC_TIMEOUT,
    client.reserve_vector_set_contexts_async(snapshots.len()),
  )
  .await
  {
    Err(_) => return Err("CLUSTER RESERVE timeout".to_string()),
    Ok(Err(e)) => return Err(format!("reserve destination vector sets failed: {e}")),
    Ok(Ok(r)) => r,
  };
  if reserved.len() != snapshots.len() {
    return Err(format!(
      "vector set context reserve mismatch (need {}, got {})",
      snapshots.len(),
      reserved.len()
    ));
  }
  let mut namespace_map: GxHashMap<u64, u64> = GxHashMap::default();
  for (snap, dst_ctx) in snapshots.iter().zip(&reserved) {
    if let Some(index) = Index::from_bytes(&snap.src_index) {
      namespace_map.insert(index.context, *dst_ctx);
    }
  }

  // 逐键：索引帧（停等）→ 元素分批帧（停等）
  for snap in snapshots {
    let Some(dst_index) = vm.remap_index_for_migration(&snap.src_index, &namespace_map) else {
      return Err(format!(
        "向量集键 {} 上下文无预留映射，快照中止",
        String::from_utf8_lossy(&snap.key)
      ));
    };
    let index_payload = encode_vector_set_index_payload(&snap.key, &dst_index);
    send_sync_frame(&client, source_node_hex, &ctx, &index_payload).await?;

    let mut items: Vec<MigrateVectorElement> = Vec::new();
    let mut batch_bytes = 0usize;
    for (element, values, attributes) in &snap.elements {
      let item = MigrateVectorElement {
        key: snap.key.clone(),
        element: element.clone(),
        values: values.clone(),
        attributes: attributes.clone(),
      };
      if !items.is_empty()
        && (items.len() >= MAX_MIGRATION_BATCH_COUNT || batch_bytes + item.frame_len() > max_chunk)
      {
        let payload = encode_vector_set_element_payload(&items);
        send_sync_frame(&client, source_node_hex, &ctx, &payload).await?;
        items.clear();
        batch_bytes = 0;
      }
      batch_bytes += item.frame_len();
      items.push(item);
    }
    if !items.is_empty() {
      let payload = encode_vector_set_element_payload(&items);
      send_sync_frame(&client, source_node_hex, &ctx, &payload).await?;
    }
  }
  Ok(())
}

/// 单副本 CLUSTER SYNC 帧停等发送（vector 逐副本链专用；扇出广播链走
/// fan_out_send，停等口径一致）；载荷首帧前插落域上下文帧
async fn send_sync_frame(
  client: &GarnetClient,
  source_node_hex: &str,
  ctx: &MigrationDomainContext,
  payload: &[u8],
) -> Result<(), String> {
  let prefixed = prepend_migration_frame(&ctx.encode_frame(), payload);
  let ok = match timeout(
    REPLICA_SYNC_TIMEOUT,
    client.execute_cluster_sync(source_node_hex, &prefixed),
  )
  .await
  {
    Err(_) => return Err("CLUSTER SYNC timeout".to_string()),
    Ok(Err(e)) => return Err(format!("CLUSTER SYNC failed: {e}")),
    Ok(Ok(ok)) => ok,
  };
  if !ok {
    return Err("CLUSTER SYNC rejected by replica".to_string());
  }
  Ok(())
}

/// 主端 diskless 快照扇出入口（批内单遍扫描；会话需已带快照客户端连接）。
/// 扫描窗口内扫描键门栅写 + 门内取锚（见模块头窗口语义）；全部活键要么入
/// 快照要么上抛中止，不存在留痕跳键通道
///
/// 在 garnet 中的相对路径: libs/cluster/Server/Replication/PrimaryOps/DisklessReplication/ReplicationSyncManager.cs:TakeStreamingCheckpointAsync
pub(crate) async fn run_snapshot_fanout(
  provider: &Arc<ClusterProvider>,
  local_node_id: u128,
  sessions: Vec<Arc<DisklessSyncSession>>,
  assets: &PrimaryReplicationAssets,
) -> Result<(), String> {
  SnapshotIteratorManager::new(local_node_id, sessions)
    .run(provider, assets)
    .await
}
