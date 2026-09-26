//! AOF 主存储操作重放（对标 libs/server/AOF/AofProcessor.cs 的
//! StoreUpsert / StoreRMW / StoreDelete / replay_dbmeta 段）。

use waof::AofEntryType;
use wbase::hash_slot::slot_of;
use wdev::Device;
use wkv::DbMetaRecord;
use wresp::command::RespCommand;
use wval::{KeyTag, NO_ETAG};

use super::{
  aof_processor::{AofProcessor, AofReplayError, ReplayTarget},
  replay_input::ReplayInput,
};
use crate::{
  resp::vector::vector_manager::{RECORD_TYPE, VSETINDEX_APPEND_LOG_ARG, VectorManager},
  storage::session::storage_session::StorageSession,
};

/// DbMeta 镜像条目回放应用（主从映射体系同步的唯一通道）
///
/// 主库全部 DbMeta 落盘经存储事件镜像为 StoreUpsert / StoreDelete 条目
///（service.rs:on_aof_store_event 放行 KeyTag::DbMeta）：upsert 复原完整
/// 记录交 [`wkv::WedbStore::apply_dbmeta_record`]（映射装载 + 水位抬升 + 判死
/// 联动回收 + 本节点落盘）；墓碑删除只需键自足信息，交
/// [`wkv::WedbStore::apply_dbmeta_tombstone`] 注销。条目先于同事务的 FlushDb /
/// FlushNs 广播条目入队（落盘先于条目入队），回放按序到达即映射已就位。
/// 回放全程 pause_aof_listeners，应用落盘不再镜像，无自激放大
pub async fn replay_dbmeta<D: Device>(
  target: &ReplayTarget<'_, '_, D>,
  op_type: AofEntryType,
  key: &[u8],
  payload: &[u8],
  vector_manager: Option<&VectorManager>,
) -> Result<(), AofReplayError> {
  match op_type {
    AofEntryType::StoreUpsert => {
      let (value, _) = AofProcessor::split_value_input(payload).ok_or("StoreUpsert 负载损坏")?;
      let rec = DbMetaRecord::decode(key, value)
        .ok_or("DbMeta 镜像条目记录损坏（布局错位或未知子类型）")?;
      let swap_info = match &rec {
        DbMetaRecord::DbSwap {
          vns,
          logic_db1,
          logic_db2,
          ..
        } => Some((*vns, *logic_db1, *logic_db2)),
        _ => None,
      };
      target
        .store
        .apply_dbmeta_record(rec)
        .await
        .map_err(AofReplayError::Store)?;
      if let (Some(vm), Some((vns, db1, db2))) = (vector_manager, swap_info) {
        // 向量联动臂与下方向量重放臂同教义：load_routes_of_vns 前置回建 +
        // 反查未命中显式上抛，绝不静默回退 ns=0 定槽（错槽一经登记盖章即
        // 不可自愈；退役租户已摘出 active_vns 逆表，unwrap_or(0) 恰把
        // 残留死亡域映射静默盖进 0 槽）
        target
          .store
          .load_routes_of_vns(vns)
          .await
          .map_err(AofReplayError::Store)?;
        let ns = target.store.vdb.logic_ns_of(vns).ok_or_else(|| {
          format!("DbSwap replay failed: 租户 {vns} 在本节点无逻辑入口（退役后滞留映射）")
        })?;
        vm.swap_database_slots(vns, ns, db1, db2).await;
      }
      Ok(())
    }
    AofEntryType::StoreDelete => target
      .store
      .apply_dbmeta_tombstone(key)
      .await
      .map_err(AofReplayError::Store),
    // 写侧镜像端口只产这两种形态，其余条目类型携 DbMeta 键即损坏流
    _ => Err(format!("DbMeta 条目非法操作类型 {op_type:?}").into()),
  }
}

/// libs/server/AOF/AofProcessor.cs:StoreUpsert
/// libs/server/AOF/AofProcessor.cs:UnifiedStoreStringUpsert
///
/// 条目物理键标签决定值域：ACL 旁路标签（0x0D）走标签直写（String 域记录
/// 写入会误伤旁路记录）；String 域为纯值镜像，不携带任何 SET 语义随动清退。
///
/// 契约声明（与 [`wkv::RmwWindow::try_rmw_sync`] 头注 §17 恒保留裁决首尾
/// 呼应，票 zcode-r123c-hllchain1）：条目流为唯一真值源，回放臂禁随动清退。
/// 主端写效果与其全部随动效应均已在主端各自自述为独立镜像条目——随键 TTL
/// 经 TtlWrite 分流为 Pexpireat/Persist 条目（含 SET 快臂环形页翻转降级形态：
/// 异步 upsert_tag 的 TTL 清除腿同样经 TtlWrite(None) 镜像为 Persist 条目，
/// 主端只摘 TTL 腿、由条目承接清除事实）、信封墓碑与 Meta 域排空经
/// StoreDelete 条目（先例即 tiered_demote 票「逐域收敛」裁决）、RMW 终值
/// 写回（INCR/APPEND/SETRANGE/SETBIT/BITFIELD/PFADD/PFMERGE 经 try_rmw_sync
/// 零 TTL 触碰）随键 TTL 根本不出条目。回放臂对 String 记录键只镜像写效果
/// （复用 [`wkv::RmwWindow::upsert_rmw`] 纯值写腿同形：upsert_raw +
/// bump_watch_version 单点，禁第二机制），严禁触发第二次 SET 语义本地裁决：
/// 旧形态直调 upsert_string 令「EXPIRE → RMW 终值 StoreUpsert」条目流在副本
/// 与重启恢复臂恒清掉刚落的 TTL 记录（副本幽灵带读、升主永续化、重启键
/// 永生），系统性击穿 §17 恒保留裁决。
/// C# UnifiedStoreStringUpsert（统一存上下文 String 覆载）在 wkv 单一存储面
/// 下与主存 upsert 同形，统一存重放臂（replay_op / replay_chunk 的
/// UnifiedStoreStringUpsert 支）以 `KeyTag::String` 直调本口
pub async fn store_upsert<D: Device>(
  session: &StorageSession<'_, D>,
  tag: KeyTag,
  key: &[u8],
  value: &[u8],
) -> Result<(), AofReplayError> {
  // 条件写形态（EX/NX 等经 StoreRMW 路径回放）；upsert 直写
  if tag == KeyTag::Acl {
    session
      .upsert_tag(key, KeyTag::Acl, value)
      .await
      .map_err(AofReplayError::Store)?;
    // 回放/复制链路改的是同一份 ACL 真源：代数一并推进，本节点在途会话
    // 下一次鉴权即收敛（与 resp::acl_store::AclStore::write 同判据）
    session.batch.store().bump_acl_generation();
    return Ok(());
  }
  // String 域纯值盲写（条目流唯一真值源，禁随动清退，见函数头注契约声明）：
  // 物理键直写既有值写漏斗，环形页翻转由 upsert_raw 内部异步闭环，无 SET
  // 语义随动腿可降级；WATCH 栅栏与主存用户键写入口同收口恰一次推进
  let rec_k = session.batch.session_tag_key(KeyTag::String, key);
  session
    .batch
    .upsert_raw(&rec_k, value)
    .await
    .map_err(AofReplayError::Store)?;
  session.bump_watch_version(key);
  Ok(())
}

/// libs/server/AOF/AofProcessor.cs:StoreRMW
/// libs/server/AOF/AofProcessor.cs:UnifiedStoreRMW
/// libs/server/API/GarnetApiUnifiedCommands.cs:DELIFEXPIM
///（C# GarnetApi.DELIFEXPIM(key) → storageSession.DELIFEXPIM → UnifiedStore
/// RMW Expired|Deterministic 删除臂；rust 该存储操作的全链为「主端 GC 判定
/// 到期 → StoreEvent::TtlPurge 产 Delifexpim 条目（service.rs on_aof_store_event）
/// → 本口 Delifexpim 臂确定性删除」，重放面即本函数该臂）
///
/// RMW 命令重放：RI 族交范围索引重放面实际执行（C#
/// RangeIndexManager.HandleRangeIndex*Replay）；DELIFEXPIM 为 TTL 物理清除
/// 确定性单条目（C# ExpireAndResume + Expired|Deterministic 标志的回放面）；
/// 键管理族只保留写侧真实产出的 Pexpireat / Persist 两形态，其余命令形态
/// 写侧不可产，按 C# MainStore/RMWMethods default 尾部口径显式失败。
/// C# UnifiedStoreRMW（统一存上下文覆载）在 wkv 单一存储面下与主存 RMW
/// 同形，统一存重放臂直调本口
pub async fn store_rmw<D: Device>(
  processor: &AofProcessor,
  session: &StorageSession<'_, D>,
  key: &[u8],
  input: &[u8],
) -> Result<(), AofReplayError> {
  let input = ReplayInput::deserialize(input).ok_or("StoreRMW input 损坏")?;
  // 向量族 RMW 子分派（VADD/VREM/VSETATTR 与 RENAME 向量哨兵条目）交向量
  // 域真实重放（对标 C# AofProcessor.StoreRMW 的 VADD/VREM/VSETATTR 分派 +
  // UnifiedStoreStringUpsert 的 RENAME+RecordType 特判：经 VectorManager
  // 重建索引/增删元素/改属性/迁移登记项；context 已由 KeyContextGuard 切至
  // 条目域）
  if matches!(
    input.cmd,
    RespCommand::Vadd | RespCommand::Vrem | RespCommand::Vsetattr
  ) || (input.cmd == RespCommand::Rename && input.arg1 == i64::from(RECORD_TYPE))
  {
    let Some(vm) = processor.append_only_file().vector_manager() else {
      return Err(
        "Vector Set (preview) commands are not enabled; Replay failed"
          .to_string()
          .into(),
      );
    };
    // 库级定槽（doc/zh/db.md 4.1）：槽位口径与在线面同源——`slot_of` 取
    // **逻辑域** (namespace, active_db)。回放会话逻辑槽属本机连接面、与条目
    // 无关，守卫又绝不重解析映射（否则即从库本地二次映射），故按条目物理域
    // (vns, vdb) 经 vdb 管理面逆向表反查真逻辑域，与在线面逐值同值；
    // 登记表域随条目域收敛（session_prefix 与条目物理键前缀同域）
    //
    // 反查前置装载：冷租户 0 内存常驻条款下，重启 / 检查点基线后面的非根域
    // 库级路由表刻意不装载（`store/mod.rs:rebuild_apply_record` DbMap / DbSwap
    // 臂仅根域执行），而早于基线的 DbMeta 镜像条目又被上面的版本闸挡在应用面
    // 之外，故此处 DB 腿必无格可查。以磁盘 DbMeta 为权威经装载单点
    // [`wkv::WedbStore::load_routes_of_vns`] 点查回建该租户快照（零本地取号、零落盘，
    // 与 `resolve_context` 首访回建同一内核）；回建后仍反查未命中即本节点对该
    // 域确无逻辑入口（映射已随退役注销的残留条目），**显式上抛留痕**——绝不
    // 静默回退物理号定槽：错槽一经 `create_index_locked` 盖章进 context 即不可
    // 自愈，按槽迁移枚举从此取不到该向量集（整键漏发），宁可回放现场暴露
    let store = session.batch.store();
    let (vns, vdb) = session.batch.virtual_domain();
    store
      .load_routes_of_vns(vns)
      .await
      .map_err(AofReplayError::Store)?;
    let (slot_ns, slot_db) = store.vdb.logic_domain_of(vns, vdb).ok_or_else(|| {
      format!("Vector replay failed: 条目物理域 ({vns}, {vdb}) 在磁盘 DbMeta 权威中无逻辑映射")
    })?;
    let repl_slot = slot_of(slot_ns, slot_db);
    let repl_prefix = session.batch.session_prefix();
    let repl_prefix = repl_prefix.as_slice();
    let result = match input.cmd {
      // 迁移索引条目（arg1 哨兵区分）：按几何参补建登记表与内存索引，
      // 零元素插入（空向量集重启重建面）
      RespCommand::Vadd if input.arg1 == VSETINDEX_APPEND_LOG_ARG => {
        vm.replay_vector_set_index(repl_prefix, key, repl_slot, &input)
          .await
      }
      RespCommand::Vadd => {
        vm.replay_vector_set_add(repl_prefix, key, repl_slot, &input)
          .await
      }
      RespCommand::Vrem => vm.replay_vector_set_remove(repl_prefix, key, &input).await,
      RespCommand::Vsetattr => {
        vm.replay_vector_set_set_attribute(repl_prefix, key, &input)
          .await
      }
      // 条目键=新名，参数=[旧名, 新名]（C# parseState 同布局）
      RespCommand::Rename => vm.replay_vector_set_rename(repl_prefix, key, &input).await,
      _ => unreachable!("向量族判别已收敛"),
    };
    // 重放族内已按单源自足成文（含因由），直传不套第二段 stringify
    // （票 zcode-r135c 案二：双段 format 令 Replay(String) 持续吸收
    // 本应归位文案，与 load_routes 等 map_err(Store) 正例口径统一）
    return result;
  }
  // 范围索引族须实际执行（C# AofProcessor.StoreRMW 的 RICREATE/RISET/RIDEL
  // 分派）：context 已由 KeyContextGuard 切至条目域，直接交 RI 复制面
  if matches!(
    input.cmd,
    RespCommand::Ricreate | RespCommand::Riset | RespCommand::Ridel
  ) {
    let Some(ri) = processor.range_index_manager() else {
      return Err(
        "RangeIndexPreview disabled; Replay failed"
          .to_string()
          .into(),
      );
    };
    let result = match input.cmd {
      RespCommand::Ricreate => {
        ri.handle_range_index_create_replay(&session.batch, key, &input)
          .await
      }
      RespCommand::Riset => {
        ri.handle_range_index_set_replay(&session.batch, key, &input)
          .await
      }
      RespCommand::Ridel => {
        ri.handle_range_index_del_replay(&session.batch, key, &input)
          .await
      }
      _ => unreachable!("RI 族判别已收敛"),
    };
    // 类型化错误经 From<ReplicationError> 归位既有 Store/Log 透明通道，
    // 天然无类型源落 Replay（票 zcode-r135c 案二：删双段 stringify）
    return result.map_err(AofReplayError::from);
  }
  // TTL 物理清除确定性重放（C# DELIFEXPIM：Expired|Deterministic 标志下
  // CheckExpiry 恒真 → 删除）；主端已判定到期，重放端幂等执行统一 DEL
  //（先清随键 TTL 再清数据，context 已切至条目域）
  if input.cmd == RespCommand::Delifexpim {
    session
      .batch
      .delete(key)
      .await
      .map_err(|e| format!("Delifexpim replay failed: {e}"))?;
    return Ok(());
  }
  // ETag 旁路记录确定性直设/清除（wnode ETag 写监听端口入队的两跳等价
  // 条目；对标 C# RMWMethods.Etags 族 etag 内嵌记录随 RMW input 重放
  // 确定性重现——rust 侧 etag 为独立旁路记录，条目 arg1 携带主端线性化
  // 后的绝对 etag 值直设，不重算）：arg1 > NO_ETAG 直设旁路记录；
  // arg1 == NO_ETAG(0) 为清除墓碑（合法 etag 恒 >= 1，0 无歧义），物理
  // 删除旁路记录，杜绝 DEL 级联后盘上残留令恢复复活旧 etag。
  //
  // 原子性边界：C# etag 与值同记录一体；rust 拆「值记录 + etag 旁路
  // 记录」两记录，写入端 apply 序恒为「值 upsert → etag 推进」，AOF 全序
  // 保证值条目先于 etag 条目，恢复同序重放即收敛（检查点基线过滤对两
  // 记录同代生效，不产生跨代错序）
  if input.cmd == RespCommand::Setwithetag {
    if input.arg1 > NO_ETAG {
      session
        .batch
        .put_etag(key, input.arg1)
        .await
        .map_err(|e| format!("Setwithetag replay failed: {e}"))?;
    } else {
      session
        .batch
        .del_etag(key)
        .await
        .map_err(|e| format!("Setwithetag replay failed: {e}"))?;
    }
    return Ok(());
  }
  // 主存 RMW 的逐命令手写臂已整族删除（C# 无这一层：AofProcessor.cs:StoreRMW 只对
  // VADD/VREM/VSETATTR 与 RICREATE/RISET/RIDEL 特判，其余 input 直通
  // stringContext.RMW，按 cmd 的分发只存在于 MainStore/RMWMethods 算子内部）。
  // rust 侧 StoreRMW 条目的生产者全集是 service.rs:on_aof_store_event 的七个镜像臂
  //（Pexpireat/Persist/Setwithetag/Riset/Ridel/Ricreate/Delifexpim）加
  // range_index_manager_replication.rs 与 vector_manager_replication.rs 的 RI/Vector
  // 族入队点，全部已在上面各臂落位；主存字符串 RMW 的语义在命令端就地折成终值
  //（resp/basic_commands/incr.rs 的 network_increment、set.rs 的 network_append /
  // network_set_range 走 read_user_sync + try_rmw_sync），AOF 只经 wkv 物理写漏斗
  // 镜像写效果（StoreUpsert 终值 / StoreDelete 墓碑），故
  // Incr/Incrby/Decr/Decrby/Incrbyfloat/Append/Setrange 形态条目恒不可产，
  // rmw_main_store 收口同批删除。Expireat/Getdel/Setex/Psetex 四臂同批删除，
  // 口径逐条如下：
  // - Expireat：写侧唯一 TTL 事件源 TtlWrite 恒以 Pexpireat + 主端线性化绝对
  //   .NET Ticks 原值（或 Persist）入条目，客户端 EXPIRE/PEXPIRE/EXPIREAT/
  //   PEXPIREAT 的相对秒/毫秒与绝对时间戳在命令端即换算为物理 TTL 记录；
  //   C# AOF 亦从不存在相对时长条目形态（主端 KeyAdminCommands.cs:423-432
  //   统一线性化为绝对 ticks 打包 ExpirationWithOption.Word）
  // - Setex/Psetex：C# NetworkSETEX 在命令端把时长折成绝对 ticks 随条目下发
  //   （BasicCommands.cs:552-559 的 valMetadata = UtcNow.Ticks + 时长），rust 命令
  //   端折成「值 upsert + Pexpireat 绝对 ticks」两跳（set.rs:network_setex_impl）；
  //   被删臂却按相对时长解释 arg1（session.setex → expire_in_ticks），与写侧唯一
  //   TTL 条目源口径正好相反——条目一旦出现即按错误语义执行，相对重算叠加复制
  //   延迟使从库 TTL 系统性偏短，严禁静默执行
  // - Getdel：命令端 read_user_sync 取值 + try_delete_sync 删除
  //   （key_admin_commands/keys.rs:network_getdel），净效果由 StoreDelete 承载
  // 遇上述任一形态即条目损坏或写/放两端演化失配，一律落 default 臂显式失败
  match input.cmd {
    RespCommand::Pexpireat => {
      // arg1 即主端线性化后的绝对 .NET Ticks 原值，直设不折算（TtlWrite 镜像
      // 已不经 Unix 毫秒中转，主从/恢复后 PTTL 与主端逐位一致；原毫秒往返
      // 恒向下截断的重放端系统性前移至多 1ms 分叉就此消灭）。wkv `expire_at`
      // 会话入口恒等裸写（对标 C# 存储侧 word 形恒等装载，粗化唯 EXPIRE 族
      // 命令边界单点）：SET/GETEX/RENAME 族非 16 对齐裸 ticks 在副本/恢复端
      // 全精度保真，与主端存值逐位一致（历史头部粗化门致主副存值偏 ≤15
      // ticks＝1.5μs 方向恒早的形态收口登记见 doc/zh/deviations.md §143；
      // 1 tick＝100ns，旧注「<16μs」系标度错 10 倍，已随批订正）
      // 刻意差异声明：C# EXPIRE 族条件（NX/XX/GT/LT）经 ExpirationWithOption
      // word 低 4 位随 AOF 完整携带、副本端重评估（UnifiedStore/PrivateMethods.cs:
      // 108 WriteLogRMW 置 Deterministic 后条件语义仍在）；rust AOF 条目由
      // TtlWrite 镜像产出，仅携带主端线性化后的绝对 ticks、不携带 ExpireOption，
      // 故重放按无条件绝对过期执行（TtlOpt::NONE）——主端写事件已按条件裁决，
      // 副本端丢失条件不影响镜像一致性，但携带面弱于 C# word 编码。
      // AOF TTL 口径恒为本臂（绝对 ticks）+ Persist 双形态，Expire/Pexpire/
      // Expireat/Setex/Psetex 五臂已删（重放按接收时刻重算会叠加复制延迟致
      // TTL 漂移）；未来若写入端补相对形态条目，必须同步改回绝对口径后再入
      // 重放面
      session
        .expire_at_ticks(key, input.arg1)
        .await
        .map_err(|e| format!("Pexpireat replay failed: {e}"))?;
      Ok(())
    }
    RespCommand::Persist => {
      session
        .persist_key(key)
        .await
        .map_err(|e| format!("Persist replay failed: {e}"))?;
      Ok(())
    }
    _ => {
      // C# RMW 重放面对未覆盖命令抛 GarnetException("Unsupported
      // operation on input")（MainStore/RMWMethods.cs InPlaceUpdaterWorker
      // default 尾部）——恢复显式失败，杜绝未知命令静默吞没恢复数据
      //（写入端新增 RMW 编码而重放端漏配时立即暴露）
      Err(
        format!(
          "StoreRMW replay failed: unsupported cmd {:?} (key length {})",
          input.cmd,
          key.len()
        )
        .into(),
      )
    }
  }
}

/// libs/server/AOF/AofProcessor.cs:StoreDelete
/// libs/server/AOF/AofProcessor.cs:UnifiedStoreDelete
///
/// 逐域删除、域内收敛（对位 C# `StoreDelete(preparedParameters, stringContext)`
/// 与 `ObjectStoreDelete(preparedParameters, objectContext)`——两存储域各删自域，
/// 从无跨域级联）：信封/ACL 走标签墓碑，String 域条目仅摘字符串物理记录，Meta
/// 域条目（RangeIndexDrop 镜像）仅排空 Meta 域——元记录 + 树实例注销 + 数据
/// 文件 + 换号旁表回收，直调主端同一排空内核
/// [`wkv::StoreSession::handle_bftree_drain_and_delete`] 的迁移臂（keep_ttl =
/// true 形），绝不连带销毁信封域与随键 TTL。
/// C# UnifiedStoreDelete（统一存上下文覆载）在 wkv 单一存储面下与主存
/// delete 同形，统一存重放臂直调本口
///
/// 各物理域的消亡均由主端各自的镜像条目承接（元记录删除必经
/// handle_bftree_drain_and_delete 单点入账 RangeIndexDrop，信封域另由
/// ObjectEnvelope 墓碑条目承接），副本逐域回放即确定性收敛。跨域级联在此
/// 是冗余而非必需，且两处必错：懒降阶（obj_save 写回信封 + drain(keep_ttl
/// = true) 清退树）的 StoreDelete(Meta) 紧随 ObjectStoreUpsert 入账，级联
/// 即把上一条目刚落的信封连同随键 TTL 一并抹掉，主端存活而副本整键消失；
/// 「数据流 publish 先行入账」的流序（升阶 / RENAME 换入）下级联亦会连坐
/// 抹掉副本刚重建的树态元记录
pub async fn store_delete<D: Device>(
  session: &StorageSession<'_, D>,
  tag: KeyTag,
  key: &[u8],
) -> Result<(), AofReplayError> {
  if tag == KeyTag::Meta {
    // 元记录墓碑条目（RangeIndexDrop 镜像）：Meta 域内排空——与主端降阶臂
    // 同一内核同一形（keep_ttl = true），副本随键 TTL 与信封域全程不受波及；
    // 排空臂纯物理键原语，不经用户键写入口，故本层显式恰一次推进栅栏
    session
      .batch
      .handle_bftree_drain_and_delete(key, true)
      .await
      .map_err(AofReplayError::Store)?;
    session.bump_watch_version(key);
    return Ok(());
  }
  // String 域（含其余旁路标签）：仅摘字符串物理记录
  let tag = if tag == KeyTag::ObjectEnvelope || tag == KeyTag::Acl {
    tag
  } else {
    KeyTag::String
  };
  session
    .delete_tag(key, tag)
    .await
    .map_err(|e| format!("StoreDelete replay failed: {e}"))?;
  // 墓碑即撤权：代数推进口径与 store_upsert 的 ACL 臂同源
  if tag == KeyTag::Acl {
    session.batch.store().bump_acl_generation();
  }
  Ok(())
}
