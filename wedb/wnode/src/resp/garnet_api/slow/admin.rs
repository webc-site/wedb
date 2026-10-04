//! 管理族慢路径执行段（检查点 / AOF 提交 / DEBUG / 清库 / 跨库交换）
//!
//! 四族均独立方法承载以脱离调用方的批处理纪元保护区：检查点 fail-fast
//! 契约与落盘级联不得在存储写纪元内触发（见各方法注释）

use std::sync::Arc;

use compio::runtime::spawn;
use itoa::Buffer;
use wbase::num::parse_db_index;
use wdev::Device;
use wresp::{
  cmd_strings::{
    RESP_ERR_ASYNC_REQUIRED, RESP_ERR_CHECKPOINT_ALREADY_IN_PROGRESS,
    RESP_ERR_DEVICE_CONTAMINATED_REFUSING_CHECKPOINT, RESP_ERR_DEVICE_CONTAMINATED_REFUSING_FLUSH,
    RESP_ERR_FLUSH_TRUNCATE_LOG_NS0, RESP_ERR_GENERIC_SYNTAX_ERROR, RESP_ERR_SLOW_PATH_STORAGE,
    RESP_ERR_SWAPDB_UNSUPPORTED, RESP_OK, write_error_raw,
  },
  command::RespCommand,
  ext::RespVecExt,
};

use super::StoreGarnetApi;
use crate::{
  resp::basic_commands::parse_flush_options, servers::consumer_registry::ConsumerRegistry,
};

/// 检查点通道未装配（宿主未注入 [`super::super::CheckpointCtx`]）时的显式拒绝文案
const RESP_ERR_CHECKPOINT_UNWIRED: &str = "ERR checkpoint channel not configured";

/// 活跃会话数（对标 C# `GarnetServerBase.ActiveConsumers()` 计数，注册表未
/// 装配的无服务器形态即 0）。SWAPDB 换库门控与管理命令同频读取，非热路径
fn active_session_count() -> usize {
  ConsumerRegistry::global().map_or(0, |r| r.active_consumers().len())
}

impl<D: Device> StoreGarnetApi<D> {
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
  /// SingleDatabaseManager 内核 `commit_to_aof_async` 直达
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
        // C# NetworkCOMMITAOF → CommitAofAsync(dbId) → storeWrapper.CommitAofAsync
        // → databaseManager.CommitToAofAsync → AppendOnlyFile.Log.CommitAsync
        // （物理刷盘推进 committed_until 至 safe_tail）：rust 经常驻
        // SingleDatabaseManager 内核（commit_to_aof_async）
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
