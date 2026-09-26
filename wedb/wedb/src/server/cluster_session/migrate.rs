//! 集群迁移命令实现（对标 libs/cluster/Session/RespClusterMigrateCommands.cs 与 MigrateCommand.cs）

use std::{str::from_utf8, sync::Arc};

use async_lock::Mutex as AsyncLockMutex;
use compio::runtime::spawn;
use parking_lot::Mutex;
use wbase::{
  hash_slot::CLUSTER_SLOT_COUNT, hex::hex_u128, map::HashSet as GxHashSet, num::strict_i32,
};
use wconn::record::parse_migration_payload;
use wkv::WedbStore;
use wnode::{
  StorageSession, range_index::RangeIndexMigrationReceiveState, resp::slow_path::SlowWait,
};
use wresp::{
  cmd_strings::{
    NOKEY, RESP_ERR_GENERIC_SYNTAX_ERROR, RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER,
    RESP_ERR_SLOW_PATH_STORAGE, RESP_OK, abort_with_wrong_number_of_arguments,
    cluster::{
      ERR_GENERIC_SLOT_OUT_OFF_RANGE, ERR_GENERIC_UNKNOWN_ENDPOINT, ERR_IOERR,
      migration_slot_domain_error,
    },
    write_error_raw,
  },
  command::RespCommand,
  ext::RespVecExt,
};

use super::{
  ClusterSession, ERR_CLUSTER_NOT_INITIALIZED, cluster_sub_name, migration_slot_supported,
  reject_wrong_arity,
};
use crate::server::{
  cluster_config::ClusterConfig,
  cluster_provider::ClusterProvider,
  migration::{
    chunk_reassembler::ChunkReassembler,
    frame_import::{FrameImport, import_migration_frames},
    migrate_driver::{
      run_keys_migration_driver, run_slots_migration_task, try_add_slots_migration_task,
    },
    migrate_session::MigrateTaskSpec,
    transfer_option::TransferOption,
  },
  worker::NodeRole,
};

/// MIGRATE 解析错误（对标 libs/cluster/Session/MigrateCommand.cs:
/// MigrateCmdParseState 可达子集；HOSTNAME_RESOLUTION_FAILED 无 DNS 解析
/// 基建不适用、MULTI_TRANSFER_OPTION 归并 Parsing、FAILEDTOADDKEY 由驱动
/// 内注册失败 IOERR 承接。NOTMIGRATING 在 KEYS 路径生效：手工迁移前置
/// 要求目标槽已置 MIGRATING，自动编排路径经
/// [`MigrateSession::try_prepare_local_for_migration`] 内部翻转、不经过
/// 本解析门）
#[derive(Debug)]
enum MigrateParseErr {
  UnknownTarget,
  MultiSlotRef(i32),
  SlotNotLocal(i32),
  NotMigrating,
  TargetNodeNotMaster,
  IncompleteSlotsRange,
  SlotOutOfRange(i32),
  Parsing,
  /// 库级分片迁移域门禁：槽位不属于迁移可承接的默认域
  ///（见 [`super::migration_slot_supported`]，驱动域上下文全量落地后移除）
  SlotDomain(i32),
}

impl MigrateParseErr {
  /// 应答文案（对标 MigrateCommand.cs:HandleCommandParsingErrors）
  fn err_text(&self, target_address: &str, target_port: i32) -> String {
    match *self {
      Self::UnknownTarget => ERR_GENERIC_UNKNOWN_ENDPOINT.to_string(),
      Self::MultiSlotRef(slot) => format!("ERR Slot {slot} specified multiple times."),
      Self::SlotNotLocal(slot) => format!("ERR slot {slot} not owned by current node."),
      // libs/cluster/CmdStrings.cs:RESP_ERR_GENERIC_SLOTNOTMIGRATING
      Self::NotMigrating => "ERR slot state not set to MIGRATING state".to_string(),
      Self::TargetNodeNotMaster => format!(
        "ERR Cannot initiate migration, target node ({target_address}:{target_port}) is not a primary."
      ),
      Self::IncompleteSlotsRange => "ERR incomplete slotrange".to_string(),
      Self::SlotOutOfRange(slot) => format!("ERR Slot {slot} out of range."),
      Self::Parsing => "ERR Parsing error".to_string(),
      Self::SlotDomain(slot) => migration_slot_domain_error(i64::from(slot)),
    }
  }
}

/// libs/cluster/Session/RespClusterMigrateCommands.cs:TrackImportProgress
/// libs/cluster/Server/Migration/MigrateSession.cs:GetLocalSession
///
/// CLUSTER MIGRATE 慢路径壳（帧导入实体收归
/// [`import_migration_frames`]，对标 C# Process 单一实体）。本壳负责协议面
/// 差异：载荷解析与按链文案前缀、写入会话 + 共享版本表构造、头声明槽位
/// 一次性导入门禁、核心错误的按链渲染。
///
/// 头门禁（库级定槽 doc/zh/db.md 4.1：槽位由 CLUSTER MIGRATE 头显式携带，
/// 一次一判整批生效；C# 逐键 HashSlot + IsImportingSlot 探测随键级哈希
/// 废除）：任一声名槽未导入即整体拒绝、绝不部分写入，并同复位双接收态
///（半途残段不得污染本会话后续迁移流）。
///
/// 会话构造（对标 C# basicGarnetApi.SET(in diskLogRecord) 的正常存储会话
/// 语义，UnifiedStore/UpsertMethods.cs:68-90 PostInitialWriter 推进
/// functionsState.watchVersionMap = db.VersionMap 全会话共享表）：导入写入
/// 必须用 `StorageSession::new` + provider 共享版本表——`new_readonly` 的
/// 独立表会使 `bump_watch_version` 推进失联，resp 会话侧 WATCH 迁入键的
/// 读数不变、事务失效判定出错。提交链与 resp 写命令同源闭环：wkv
/// hlog.append 成功即 notify_write_listener → StoreEvent::Write/TtlWrite →
/// service.rs `on_aof_store_event` 镜像入 WalLog → aof_replication_pump
/// attach_wake 驱动副本推流，分片内主从经复制链路收敛一致；重放侧防环由
/// `pause_aof_listeners` 承担，本路径不绕行。
async fn cluster_migrate_slow(
  provider: Arc<ClusterProvider>,
  store: Arc<WedbStore<wdev::SegmentedDevice>>,
  chunk_reassembler: Arc<Mutex<ChunkReassembler>>,
  ri_receive_state: Option<Arc<AsyncLockMutex<RangeIndexMigrationReceiveState>>>,
  replace: bool,
  slots: Vec<u16>,
  payload: Vec<u8>,
) -> Vec<u8> {
  let mut out = Vec::new();
  let (record_count, frames) = match parse_migration_payload(&payload) {
    Ok(res) => res,
    Err(e) => {
      out.write_resp_error(&format!("ERR Invalid migration payload: {e:?}"));
      return out;
    }
  };
  if record_count == 0 {
    out.write_resp_simple_string("OK");
    return out;
  }

  let Ok(session) = store.new_session() else {
    out.write_resp_error(RESP_ERR_SLOW_PATH_STORAGE);
    return out;
  };
  let batch = session.enter_batch();
  let storage = StorageSession::new(batch);
  let import = FrameImport {
    provider: &provider,
    session: &session,
    storage: &storage,
    chunks: &chunk_reassembler,
    ri: &ri_receive_state,
    replace,
    // 向量帧登记槽取头声明槽（库级定槽下迁移流单会话单槽；逐帧判槽已废除）
    vector_slot: slots[0],
    // 导槽链拒绝跨域扩展帧：头声明槽位一次性门禁后导入，落域上下文帧会把
    // 写入换到声明槽之外未门禁的域（见 frame_import 字段文档）
    accept_domain_frames: false,
  };
  // 槽门判败系修复型分叉（§160 登记条回指）：C# 置 migrateState 吞错后
  // 主命令臂仍无条件回 +OK，rust 此处显式 ERR 判败，严禁按 C# 形态回改
  if let Some(cm) = provider.cluster_manager() {
    let cfg = cm.current_config.read();
    if let Some(slot) = slots.iter().find(|&&s| !cfg.is_importing_slot(s)) {
      import.reset_receive_states();
      out.write_resp_error(&format!("ERR Slot {slot} is not in importing state"));
      return out;
    }
  }
  // 错误收场与 SYNC 链同口径：核心显式拒绝即复位两接收态后应答
  match import_migration_frames(frames, &import).await {
    Ok(()) => out.write_resp_simple_string("OK"),
    Err(err) => {
      import.reset_receive_states();
      out.write_resp_error(&err);
    }
  }
  out
}

impl ClusterSession {
  /// MIGRATE SLOTS/SLOTSRANGE 槽位收录校验（对标 MigrateCommand.cs 选项
  /// 循环的 OutOfRange / IsLocal / MULTISLOTREF 三查，首错保留）
  fn collect_migrate_slot(
    config: &ClusterConfig,
    slot: i32,
    slots: &mut GxHashSet<i32>,
    parse_err: &mut Option<MigrateParseErr>,
  ) {
    if parse_err.is_some() {
      return;
    }
    if !(0..CLUSTER_SLOT_COUNT as i32).contains(&slot) {
      *parse_err = Some(MigrateParseErr::SlotOutOfRange(slot));
      return;
    }
    if !config.is_local(slot as u16, false) {
      *parse_err = Some(MigrateParseErr::SlotNotLocal(slot));
      return;
    }
    // 迁移域门禁（SLOTS/SLOTSRANGE 臂）：驱动扫描只覆盖默认域槽位，非默认域
    // 取键恒空却照常交权，显式拒绝（判据单点见 cluster_session::migration_slot_supported）
    if !migration_slot_supported(i64::from(slot)) {
      *parse_err = Some(MigrateParseErr::SlotDomain(slot));
      return;
    }
    if !slots.insert(slot) {
      *parse_err = Some(MigrateParseErr::MultiSlotRef(slot));
    }
  }

  /// libs/cluster/Session/RespClusterMigrateCommands.cs:NetworkClusterMigrate
  ///
  /// 头格式（库级定槽 doc/zh/db.md 4.1 偏离声明）：`CLUSTER MIGRATE
  /// <sourceNodeId> <replace T/F> <slot-list> <payload>`，
  /// `slot-list` 为逗号分隔槽位（发送端会话槽集显式携带，C# 头无此参数——
  /// 接收端逐键 HashSlot 门禁随之废除，改头级一次性判槽）。
  /// C# 头的 vectorSets 位（SetClusterMigrateHeader isVectorSets，接收端
  /// 据此分派向量集专用导入路径）在本仓废除：向量集帧自描述
  /// kind=5/6 是唯一判据，[`import_migration_frames`] 按帧种类直接分派，
  /// 头不再承载向量集信息
  pub(super) fn network_cluster_migrate(
    &self,
    cmd: RespCommand,
    args: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> bool {
    let &[source_node_raw, replace_raw, slots_raw, payload_raw] = args else {
      abort_with_wrong_number_of_arguments(output, cluster_sub_name(cmd));
      return true;
    };
    // 头声明槽位集（逗号分隔，非空；解析失败或越界整批拒绝）
    let slots: Vec<u16> = if slots_raw.is_empty() {
      Vec::new()
    } else {
      slots_raw
        .split(|&b| b == b',')
        .map(|seg| {
          strict_i32(seg)
            .filter(|s| (0..CLUSTER_SLOT_COUNT as i32).contains(s))
            .map(|s| s as u16)
        })
        .collect::<Option<Vec<_>>>()
        .unwrap_or_default()
    };
    if slots.is_empty() {
      output.write_resp_error(ERR_GENERIC_SLOT_OUT_OFF_RANGE);
      return true;
    }
    let Some(store) = self.cluster_provider.try_store() else {
      output.write_resp_error(ERR_CLUSTER_NOT_INITIALIZED);
      return true;
    };
    let replace = replace_raw.eq_ignore_ascii_case(b"T");
    // 协议入口：32 字符 hex 节点 id 解析为内部 u128 身份（接收态已会话化，
    // 源节点 id 不再参与接收态路由，仅作语法校验与应答语义保留）
    if hex_u128(source_node_raw).is_none() {
      output.write_resp_error(RESP_ERR_GENERIC_SYNTAX_ERROR);
      return true;
    };
    let payload = payload_raw.to_vec();
    let ri_receive_state = self.ensure_range_index_receive_state();
    let chunk_reassembler = Arc::clone(&self.chunk_reassembler);
    let provider = self.cluster_provider.clone();
    *self.pending_slow.lock() = Some(SlowWait::new(async move {
      cluster_migrate_slow(
        provider,
        store,
        chunk_reassembler,
        ri_receive_state,
        replace,
        slots,
        payload,
      )
      .await
    }));
    true
  }

  /// libs/cluster/Session/MigrateCommand.cs:NetworkTryMIGRATE（顶层 MIGRATE，
  /// C# 顶层分派见 ClusterSession.cs:110）
  ///
  /// 参数形状对标 Redis MIGRATE host port <key | ""> destination-db timeout
  /// [COPY] [REPLACE] [[AUTH password] | [AUTH2 username password]]
  /// [KEYS key [key ...]] [SLOTS slot [slot ...]] [SLOTSRANGE start end ...]
  ///
  /// 与 C# 行为对标与显式差异说明：
  /// 1. C# 通过 AsyncUtils.BlockingWait 阻塞当前网络线程等待迁移闭环，rust
  ///    KEYS 同步形态挂慢路径异步驱动（单调用窗口独立阻塞），SLOTS 形态以
  ///    后台异步任务运行；
  /// 2. 地址不做 DNS 解析重试（无解析基建），集群配置精确匹配失败即
  ///    UNKNOWNTARGET；
  /// 3. 库级定槽（doc/zh/db.md 4.1）：`slot` 为会话库级槽位，KEYS 臂不再
  ///    逐键 HashSlot 校验（越界/归属/跨槽三查随键级哈希废除），收敛为
  ///    会话槽位的 IsLocal + IsMigratingSlot 两查；Redis 单键形态（args[2]
  ///    非空）与 KEYS 选项臂同口径做两查并收录 key_slots。
  pub(super) fn network_try_migrate(
    &self,
    args: &[&[u8]],
    output: &mut Vec<u8>,
    slot: u16,
  ) -> bool {
    reject_wrong_arity!(args.len() < 5, RespCommand::Migrate, output);
    let target_address = String::from_utf8_lossy(args[0]).into_owned();
    let (target_port, _db_id, timeout) = match (
      strict_i32(args[1]),
      strict_i32(args[3]),
      strict_i32(args[4]),
    ) {
      (Some(port), Some(db), Some(timeout)) => (port, db, timeout),
      _ => {
        output.write_resp_error(RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
        return true;
      }
    };
    // timeout 哨兵收敛（对标 C# TimeSpan.FromMilliseconds 三档：>0 限时、
    // 0 立即超时、-1 即 Timeout.InfiniteTimeSpan 免超时；其余负值 C# 在首个
    // WaitAsync 抛 ArgumentOutOfRangeException 致迁移运行期失败，本仓提前到
    // 解析期显式拒收，偏差登记 doc/zh/deviations.md 86）
    if timeout < -1 {
      output.write_resp_error("ERR MIGRATE timeout is invalid: must be -1, 0, or positive");
      return true;
    }

    let mut copy_option = false;
    let mut replace_option = false;
    let mut username = "";
    let mut passwd = "";
    // 解析期产出：KEYS 形态收录键清单与键槽集合，SLOTS/SLOTSRANGE 收录槽集合
    let mut keys: Vec<Vec<u8>> = Vec::new();
    let mut key_slots: GxHashSet<i32> = GxHashSet::default();
    let mut slots: GxHashSet<i32> = GxHashSet::default();
    let mut transfer = TransferOption::None;
    // 首个解析错误（C# pstate 口径：出错后继续收集但不覆盖首错）
    let mut parse_err: Option<MigrateParseErr> = None;

    // 单键形态（C# keySlice.Length > 0：收录进 sketch）；库级定槽
    //（doc/zh/db.md 4.1）下槽位校验依 config 解析后补做，见下方单键两查
    if !args[2].is_empty() {
      transfer = TransferOption::Keys;
      keys.push(args[2].to_vec());
    }

    let Some(cm) = self.cluster_manager() else {
      output.write_resp_error(ERR_CLUSTER_NOT_INITIALIZED);
      return true;
    };
    let config = cm.current_config();

    // 目标端点解析先行（对标 C# MigrateCommand.cs：
    // GetWorkerNodeIdFromAddressOrHostname 在选项循环之前，UNKNOWNTARGET 即
    // 首错，循环内各槽校验依「首错保留」跳过，不再覆盖未知端点）
    let target_node_id =
      config.get_worker_node_id_from_address_or_hostname(&target_address, target_port);
    if target_node_id.is_none() {
      parse_err = Some(MigrateParseErr::UnknownTarget);
    }

    // 单键形态（Redis MIGRATE host port <key> …）与 KEYS 选项臂同口径：库级
    // 定槽（doc/zh/db.md 4.1）下键内容不定槽，会话槽位单点做迁移域门禁 +
    // IsLocal + IsMigratingSlot 校验并收录 key_slots（C# 单键经 KEYS 循环逐键
    // 哈希三查，本仓单键无后续选项循环兜底）。漏收录则载荷头槽位列表为空串，
    // 目标端 slots.is_empty() 整批拒收，且本端 MIGRATING 门控因空集 `.all`
    // 恒真被整体跳过。首错保留：get_or_insert 不覆盖 UnknownTarget
    if transfer == TransferOption::Keys {
      // 迁移域门禁先行（判据单点见 migration_slot_supported，与 SLOTS 臂
      // collect_migrate_slot 同序）：非默认域签发单键迁移显式拒绝
      if !migration_slot_supported(i64::from(slot)) {
        parse_err.get_or_insert(MigrateParseErr::SlotDomain(i32::from(slot)));
      } else if !config.is_local(slot, false) {
        parse_err.get_or_insert(MigrateParseErr::SlotNotLocal(i32::from(slot)));
      } else if !config.is_migrating_slot(slot) {
        // 手工单键迁移前置：目标槽必须已置 MIGRATING
        //（C# MigrateCommand.cs:212 IsMigratingSlot → NOTMIGRATING）
        parse_err.get_or_insert(MigrateParseErr::NotMigrating);
      }
      key_slots.insert(i32::from(slot));
    }

    let mut idx = 5;
    while idx < args.len() {
      let option = args[idx];
      idx += 1;
      if option.eq_ignore_ascii_case(b"COPY") {
        copy_option = true;
      } else if option.eq_ignore_ascii_case(b"REPLACE") {
        replace_option = true;
      } else if option.eq_ignore_ascii_case(b"AUTH") {
        let Some(pw) = args.get(idx) else {
          output.write_resp_error(RESP_ERR_GENERIC_SYNTAX_ERROR);
          return true;
        };
        idx += 1;
        passwd = from_utf8(pw).unwrap_or_default();
      } else if option.eq_ignore_ascii_case(b"AUTH2") {
        let (Some(u), Some(pw)) = (args.get(idx), args.get(idx + 1)) else {
          output.write_resp_error(RESP_ERR_GENERIC_SYNTAX_ERROR);
          return true;
        };
        idx += 2;
        username = from_utf8(u).unwrap_or_default();
        passwd = from_utf8(pw).unwrap_or_default();
      } else if option.eq_ignore_ascii_case(b"KEYS") {
        if transfer == TransferOption::Slots && parse_err.is_none() {
          parse_err = Some(MigrateParseErr::Parsing);
        }
        transfer = TransferOption::Keys;
        // 库级定槽（doc/zh/db.md 4.1）：会话库内所有键恒共会话槽位，键内容
        // 不参与定槽，C# 逐键 IsLocal / 单槽约束 / IsMigratingSlot 收敛为
        // 会话槽位两查，键全数收录（跨槽键在架构层不存在）
        if !config.is_local(slot, false) {
          parse_err.get_or_insert(MigrateParseErr::SlotNotLocal(i32::from(slot)));
        } else if !config.is_migrating_slot(slot) {
          // 手工 KEYS 迁移前置：目标槽必须已置 MIGRATING
          //（C# MigrateCommand.cs:212 IsMigratingSlot → NOTMIGRATING）
          parse_err.get_or_insert(MigrateParseErr::NotMigrating);
        }
        key_slots.insert(i32::from(slot));
        keys.extend(args[idx..].iter().map(|k| k.to_vec()));
        idx = args.len();
      } else if option.eq_ignore_ascii_case(b"SLOTS") {
        if transfer == TransferOption::Keys && parse_err.is_none() {
          parse_err = Some(MigrateParseErr::Parsing);
        }
        transfer = TransferOption::Slots;
        while idx < args.len() {
          let Some(slot) = strict_i32(args[idx]) else {
            output.write_resp_error(RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
            return true;
          };
          idx += 1;
          Self::collect_migrate_slot(&config, slot, &mut slots, &mut parse_err);
        }
      } else if option.eq_ignore_ascii_case(b"SLOTSRANGE") {
        if transfer == TransferOption::Keys && parse_err.is_none() {
          parse_err = Some(MigrateParseErr::Parsing);
        }
        transfer = TransferOption::Slots;
        let rest = args.len() - idx;
        if rest == 0 || (rest & 1) == 1 {
          parse_err = Some(MigrateParseErr::IncompleteSlotsRange);
          break;
        }
        while idx < args.len() {
          let (Some(start), Some(end)) = (strict_i32(args[idx]), strict_i32(args[idx + 1])) else {
            output.write_resp_error(RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
            return true;
          };
          idx += 2;
          for slot in start..=end {
            Self::collect_migrate_slot(&config, slot, &mut slots, &mut parse_err);
          }
        }
      }
      // 未知选项忽略（C# 选项循环无 else 分支）
    }

    // 迁移域门禁（KEYS 形态：含 Redis 单键形态与 KEYS 选项形态）：本臂触及的
    // 槽位即签发会话库槽位，非默认域驱动读值必然 miss（键被登记不可迁移而滞留
    // 源端）。置于选项循环之后，按首错保留让更具体的归属/状态错优先
    if transfer == TransferOption::Keys && !migration_slot_supported(i64::from(slot)) {
      parse_err.get_or_insert(MigrateParseErr::SlotDomain(i32::from(slot)));
    }

    // 解析错误统一应答（对标 HandleCommandParsingErrors）
    if let Some(err) = parse_err {
      output.write_resp_error(&err.err_text(&target_address, target_port));
      return true;
    }

    // 目标节点角色校验（对标 GetNodeRoleFromNodeId 判定；端点归属已在
    // 选项循环前解析并首错保留）
    let Some(target_node_id) = target_node_id else {
      output
        .write_resp_error(&MigrateParseErr::UnknownTarget.err_text(&target_address, target_port));
      return true;
    };
    if config.get_node_role_from_node_id(target_node_id) != NodeRole::Primary {
      output.write_resp_error(
        &MigrateParseErr::TargetNodeNotMaster.err_text(&target_address, target_port),
      );
      return true;
    }
    let source_node_id = config.local_node_id().unwrap_or_default();
    drop(config);

    let spec = MigrateTaskSpec {
      source_node_id,
      target_address,
      target_port,
      target_node_id,
      username: username.to_string(),
      passwd: passwd.to_string(),
      copy_option,
      replace_option,
      timeout,
      transfer_option: transfer,
    };

    match transfer {
      // SLOTS/SLOTSRANGE：注册任务后 fire-and-forget，命令立即 +OK
      TransferOption::Slots => {
        match try_add_slots_migration_task(&self.cluster_provider, spec.clone(), &slots) {
          Ok(session) => {
            let store = self.cluster_provider.try_store();
            spawn(async move {
              let Some(store) = store else {
                log::error!("MIGRATE SLOTS 后台驱动无可用存储");
                return;
              };
              if let Err(err) = run_slots_migration_task(store, spec, session).await {
                log::error!("MIGRATE SLOTS 后台驱动失败: {err}");
              }
            })
            .detach();
            output.extend_from_slice(RESP_OK);
          }
          Err(err) => {
            log::error!("MIGRATE 注册迁移任务失败: {err}");
            output.write_resp_error(ERR_IOERR);
          }
        }
        true
      }
      // KEYS：同步驱动挂慢路径（对标 BlockingWait 阻塞网络线程）
      TransferOption::Keys if !keys.is_empty() => {
        let provider = Arc::clone(&self.cluster_provider);
        let store = self.cluster_provider.try_store();
        *self.pending_slow.lock() = Some(SlowWait::new(async move {
          let mut out = Vec::new();
          let Some(store) = store else {
            out.write_resp_error(ERR_CLUSTER_NOT_INITIALIZED);
            return out;
          };
          match run_keys_migration_driver(provider, store, spec, &key_slots, &keys).await {
            Ok(count) => {
              log::info!("MIGRATE KEYS 完成: {count} 键");
              out.write_resp_simple_string("OK");
            }
            Err(err) => {
              log::error!("MIGRATE KEYS 失败: {err}");
              out.write_resp_error(ERR_IOERR);
            }
          }
          out
        }));
        true
      }
      // 单键占位为空且无 KEYS/SLOTS 选项（NONE 形态）、或 KEYS 后零后继键：
      // 显式拒绝（对标 Redis migrateCommand 同形态 addReplyError "NOKEY"）。
      // C# NONE 形态 slots=null 直调 TryAddMigrationTask，而
      // MigrateSessionTaskStore.TryAddMigrateSession 的 new MigrateSession
      // 位于 try 块之外（MigrateSessionTaskStore.cs:94 构造、:107 才进 try），
      // 构造器 GetRanges（MigrateSession.cs:230）对 null _sslots 取 Count 抛
      // NRE 异常逃逸——绝非 +OK，旧「空任务空跑投影」断言失实已订正
      _ => {
        write_error_raw(output, NOKEY);
        true
      }
    }
  }

  /// libs/cluster/Session/RespClusterMigrateCommands.cs:NetworkClusterMTasks
  pub(super) fn network_cluster_mtasks(
    &self,
    cmd: RespCommand,
    args: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> bool {
    reject_wrong_arity!(!args.is_empty(), cmd, output);
    let mtasks = self
      .cluster_provider
      .migration_manager()
      .map(|mm| mm.get_migration_task_count())
      .unwrap_or(0);
    output.write_resp_int(mtasks as i64);
    true
  }
}
