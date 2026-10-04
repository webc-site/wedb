//! [`StoreGarnetApi`] 的 [`GarnetApiFace`] 实现（`exec` 同步分派大臂 +
//! 慢路径 `exec_slow` 族异步臂 + AUTH/ACL 异步装箱臂 + 上下文/快照臂）
//!
//! 对标 C# ProcessBasicCommands / ProcessArrayCommands 的 switch 承接与
//! Tsavorite pending 重放的异步形态；trait 面定义与执行域结构体见 [`super`]。

use std::{future::Future, mem, pin::Pin, sync::Arc};

use wbase::hash_slot::slot_of;
use wconf::ServerConfigType;
use wdev::Device;
use wkv::SessionLocking;
use wmetric::{DbSnapshot, InfoCommand, PendingLatencyMeter};
use wresp::{
  cmd_strings::{RESP_ERR_ASYNC_REQUIRED, write_error_raw},
  command::{RespCommand, is_vector_set_command},
  metrics::InfoMetricsType,
};
use wtxn::TxnState;
use wval::SessionPrefixBuf;

use super::{
  AclUserRecordFut, GarnetApiFace, SlowPollSessionBound, StoreGarnetApi, project_db_snapshot, raw,
};
use crate::{
  resp::{
    EtagResume, MsetnxResume, RespServerSession, TtlResume,
    info_provider::{InfoSurface, SessionInfoSource, render_info_slow_reply},
    slow_path::{SlowFuture, SlowWait},
    vector::{
      resp_server_session_vectors::VectorGuardVerdict,
      vector_store_callbacks::ActiveVectorSessionGuard,
    },
  },
  storage::session::common::ttl_sync::purge_expired_residue_sync,
};

impl<D: Device> GarnetApiFace for StoreGarnetApi<D> {
  fn exec_auth_acl<'a>(
    &'a self,
    session: &'a mut RespServerSession,
    cmd: RespCommand,
    args: &'a [&'a [u8]],
  ) -> Pin<Box<dyn Future<Output = bool> + 'a>> {
    Box::pin(StoreGarnetApi::exec_auth_acl(self, session, cmd, args))
  }

  fn exec_acl_refresh<'a>(
    &'a self,
    session: &'a mut RespServerSession,
  ) -> Pin<Box<dyn Future<Output = ()> + 'a>> {
    Box::pin(StoreGarnetApi::exec_acl_refresh(self, session))
  }

  fn acl_user_record<'a>(&'a self, ns: u64, username: &'a [u8]) -> AclUserRecordFut<'a> {
    Box::pin(StoreGarnetApi::acl_user_record(self, ns, username))
  }

  fn exec(&self, session: &mut RespServerSession, cmd: RespCommand, args: &[&[u8]]) {
    // 命令分派同步段整段绑定向量存储会话执行域（对标 C# `[ThreadStatic]
    // ActiveThreadSession` 在 NetworkVADD / VSIM 期间的连接线程绑定，且把
    // 缺席删除钩子（DEL 向量键 → 登记表摘除写透）一并纳入绑定段；本段为
    // 纯同步段，段尾自动解绑还原，绝不跨 `.await` 存活）
    let _vector_domain = ActiveVectorSessionGuard::bind(&self.session);
    // 当处于事务 Running 态且当前会话物理前缀异于展开期锚前缀时：
    // 严禁降级为 SessionLocking::Basic！
    // 改按既定执行方案：通过对新增桶增量 try 闩（旧代已持桶不重复取），
    // 若争用走既有 ExecRun::Contended 慢臂让步重驱，保证整个事务重放期间始终持桶闩，杜绝他连接在多条重放命令间穿插破坏原子性。
    if session.txn_prefix_needs_rearm() && !session.rearm_txn_locks_for_generation_swap() {
      return;
    }
    // 会话锁器模式选型点（对标 C# RespServerSession.ProcessMessages 依
    // `txnManager.state == TxnState.Running` 在 basicApi 与 transactionalApi 间
    // 派发）：事务重放段的本键桶排他闩已由本会话事务在 windex 同一份锁内存上
    // 同一主桶持有（wtxn 锁登记与本窗口寻桶同经 whasher::scoped_hash 前缀种子
    // 单点，票 wtxn-wkv-keybucket-hash-scope-desync），键窗全落持锁桶域时读改
    // 写窗口让闩复用；非事务遍窗口自取闩。RAII 守卫在本分派段退出即还原，而
    // 同步段 `Ok(false)` 纯降级的命令体改由网络泵在段外 await——判定结果必随
    // 挂起体同渠道下传（[`SlowWait::for_command_with_locking`]），慢臂体罩在
    // 同一 RAII 守卫内，杜绝段外回落 Basic 后向自家事务已持的桶闩重新入册
    // （桶闩非重入、唯一放闩者要到 EXEC 收口 → 让核预算环烧尽成 LockTimeout
    // 错误元素）；C# 对位是事务视图内 `TransactionalSessionLocker
    // .TryLockEphemeralExclusive` 恒 true，慢路径重投仍处 transactionalApi 派发
    // （doc/zh/deviations.md §139 同族补登）。脚本重入段不
    // 无条件下传 Running：C# 内嵌 processor 自带独立会话、脚本内命令恒走
    // basicApi ephemeral 自取闩，rust 无内嵌 processor 而重入共享会话（事务镜像
    // 滞留 Running），故按本命令键窗对本事务持锁桶域的会合判定选型——在域内
    // 让闩（复用已持闩，杜绝非重入桶闩自等死锁），在域外 Basic 自取闩（他连接
    // 同键并发经同一锁内存互斥，杜绝无闩盲写丢更新；判定单点见
    // `RespServerSession::txn_locks_cover_cmd`，登记 doc/zh/deviations.md §139）
    let locking =
      if session.txn_state == TxnState::Running && session.txn_locks_cover_cmd(cmd, args) {
        SessionLocking::Transactional
      } else {
        SessionLocking::Basic
      };
    let _locking = self.session.push_session_locking(locking);
    // AUTH / HELLO / ACL 族已上提至泵侧 [`Self::exec_auth_acl`] /
    // [`Self::exec_acl_refresh`] 异步臂（分派漏斗 dispatch_via_garnet_api
    // 预筛停车），同步段 exec 只承接其余命令
    if is_vector_set_command(cmd)
      && let Some(vectors) = &self.vector_session
    {
      let resp3 = session.resp_protocol_version == 3;
      // 向量集命令前置守卫（读写分臂，对标 C# RespServerSessionVectors.cs:
      // 501/944/1360/1362/1417/1453/1489/1534/1573/1664/1717/1786 等）：
      // 键驻留 wkv 值域即拒绝，杜绝与既有非向量键并行建向量集产生双域键或非向量键读命令误报
      if let Some(key) = args.first() {
        let batch = self.session.enter_batch();
        match vectors.vector_key_guard(key, &batch) {
          VectorGuardVerdict::Reject(reply) => {
            reply.encode_resp(&mut session.output, resp3);
            return;
          }
          // Allow / 只读族 Degrade：一律落穿下方 raw::dispatch Ok(false) 臂
          // 挂起 SlowWait（向量十二臂全量挂起化，见下注）
          VectorGuardVerdict::Allow => {
            // VADD 登记创建位过期残留清退快臂（案 zcode-r151c-exwatch 案二）：
            // 守卫对「过期未清退死残留」判缺失放行，窗内同步清退 wkv 值域
            // 过去代残留（TTL 旁路记录 + 判死值记录），清退 bump 与命令体
            // 自身 bump 合并同命令、恒先于任何后续 WATCH 登记，WATCH 假弃
            // 面归零；降级形（失闩/磁盘候选/页翻转）零副作用静默落穿，由
            // network_vector_write_slow VADD 臂同源口经既有 delete 级联承接
            if cmd == RespCommand::Vadd {
              let _ = purge_expired_residue_sync(&batch, key);
            }
          }
          VectorGuardVerdict::Degrade if cmd.is_vector_read_command() => {}
          // 磁盘候选待裁决 / 存储错误（同步段读不准）：写命令保守拒（冷态
          // 不确定键拒写，误拒可 DEL 后重试，双域键一经写即成幽灵——取舍
          // 登记 doc/zh/deviations.md §22）
          VectorGuardVerdict::Degrade => {
            vectors
              .wrong_type_reply()
              .encode_resp(&mut session.output, resp3);
            return;
          }
        }
      }
      // 十二臂全量挂起化（对标 C# VectorStoreOps.cs 十四锁点 using 锁域
      // 全程罩住命令体的 rust 异步承接）：写族（VADD / VSETATTR / VREM：
      // 插入/删除/属性写链为 compio 存储异步操作）+ 读族全部经锁定读面
      // read_vector_index（VSIM / VEMB / VCARD / VDIM / VGETATTR / VINFO /
      // VISMEMBER / VLINKS / VRANDMEMBER：共享守卫跨命令体 await 存活，
      // ptr=0 冷记录在锁内独占重建后降级共享——重建挂起面归一，同步段
      // 无就地应答形态）——Allow 态直接落穿下方 raw::dispatch Ok(false)
      // 臂（与只读 Degrade 同路），参数快照登记 SlowWait 停车，网络泵
      // await exec_slow 向量读/写臂（network_vector_read_slow /
      // network_vector_write_slow）异步闭环。对标 cluster 链 pending_slow
      // 转挂 + acl 链泵侧异步臂先例，不发明新机制；挂起窗口的键域竞态由
      // 慢臂真读复判收口（见 network_vector_write_slow 文档）。
    }
    let batch = self.session.enter_batch();
    let mut output = mem::take(&mut session.output);
    // 命令层约定：Ok(false) = 须异步闭环且本次不残留输出
    let vector = self.vector_mgr();
    if raw::dispatch(session, cmd, args, &batch, vector, &mut output) == Ok(false) {
      // 慢路径分派（单次实现，多命令复用）：挂起 SlowWait 停止本批消费，
      // 网络泵 await 闭环后写回应答；参数快照脱离接收缓冲生命周期。
      // 句柄克隆保 Arc 存活，future 借用的执行域（self.session）在网络泵
      // await 期间有效（消费串行驱动，无并发进入）
      if let Some(api) = &session.garnet_api {
        // INFO 慢路径（凡段集含扫描族段的整请求降级）：类型化调度参数直挂
        // exec_slow_info 单源——非扫描面在此调度点经 InfoSurface 同步快照
        //（exec_slow 无会话可达面，与命令名 / resp_version 快照同渠道同
        // 口径，取代旧 8 字节 LE 库数上限尾参裸字节通道），扫描行由慢臂
        // 异步产出，两路合成组合数据源单源渲染（对标 C# 逐段实填）
        if cmd == RespCommand::Info {
          let parsed = InfoCommand::parse_sections(args);
          let surface = InfoSurface::capture(&SessionInfoSource::new(session), &parsed.sections);
          let max_databases = session.max_databases;
          let active_db = session.active_db_id as i32;
          let resp_version = session.resp_protocol_version;
          session.pending_slow = Some(api.exec_slow_info(
            parsed.sections,
            surface,
            max_databases,
            active_db,
            resp_version,
          ));
          session.output = output;
          return;
        }
        let mut snapshot = if cmd == RespCommand::Get
          && let Some(sg_keys) = session.sg_batched_keys.take()
        {
          sg_keys
        } else {
          args.iter().map(|a| a.to_vec()).collect()
        };
        // 自定义对象命令快照尾参追加命令名（C# currentCustomObjectCommand
        // 的慢路径承接：exec_slow 无会话可达面，经名回查注册表）
        if cmd == RespCommand::Customobjcmd
          && let Some((_, custom)) = session.current_custom_command.take()
        {
          snapshot.push(custom.name.as_bytes().to_vec());
        }
        // HSCAN/SSCAN/ZSCAN/COSCAN 慢路径快照尾参追加 COUNT 上限（4 字节
        // LE；同款先例；OBJECT_SCAN_COUNT_LIMIT 运行时配置热更即时生效）
        if matches!(
          cmd,
          RespCommand::Hscan | RespCommand::Sscan | RespCommand::Zscan | RespCommand::Coscan
        ) {
          snapshot.push(
            session
              .runtime_config()
              .get_int(ServerConfigType::ObjectScanCountLimit)
              .to_le_bytes()
              .to_vec(),
          );
        }
        // MSETNX 慢路径快照尾参追加续跑模式标记（[`MsetnxResume::tail_byte`]：
        // b"1" = NX 判定已整体通过、已写键保持，慢路径补写续跑；b"r" = 回滚
        // 存在删除降级残留，慢路径持窗条件回滚收尾；b"0" = 判定段降级，慢
        // 路径须先完整异步裁决存活），消费即复位
        if cmd == RespCommand::Msetnx {
          snapshot.push(vec![session.msetnx_resume.tail_byte()]);
          session.msetnx_resume = MsetnxResume::Replay;
        }
        // DEL / UNLINK 慢路径快照尾参追加已删键计数（8 字节 LE；
        // 沿 MSETNX resume 尾参先例），慢路径继承起始计数，消费即复位
        if matches!(cmd, RespCommand::Del | RespCommand::Unlink) {
          snapshot.push(session.del_deleted_count.to_le_bytes().to_vec());
          session.del_deleted_count = 0;
        }
        // RESTORE / SET 条件写族慢路径快照尾参追加「值已提交 + TTL 待投」
        // 续跑标记（[`TtlResume::tail_bytes`]，25 字节：模式字节 + 8 字节 LE
        // 过期刻度 + 16 字节 LE 域锚 `(vns, vdb)`；沿 MSETNX/DEL resume 尾参
        // 先例经同一 pending_slow 通道承载，票 wnode-nx-conditional-ttl-
        // degrade-replay-selfhit：快臂值先落库、put_ttl 遭环形页翻转降级时
        // 慢臂跳过整命令重放自碰已提交值——RESTORE 误回 BUSYKEY / SET NX
        // 误回 nil / KEEPTTL 丢 TTL，仅补投 TTL 出成功帧；域锚随刻度同点
        // 捕获，慢臂跨换号域比对失配即弃刻度跳补投，杜绝死域 TTL 盖进新域
        // 成幽灵 TTL——票 wnode-set-keepttl-resume-tail-no-domain-anchor-
        // ghost-ttl-across-swap），消费即复位
        if matches!(
          cmd,
          RespCommand::Restore | RespCommand::Set | RespCommand::Setexnx
        ) {
          snapshot.push(session.ttl_resume.tail_bytes());
          session.ttl_resume = TtlResume::Full;
        }
        // ETag 写族慢路径快照尾参追加「值腿已提交 + 余腿待投」续跑标记
        // （[`EtagResume::tail_bytes`]，33 字节：模式字节 + 8 字节 LE 新 etag +
        // 8 字节 LE 过期刻度 + 16 字节 LE 域锚；与 RESTORE/SET 同一
        // pending_slow 通道、同一尾参纪律，票 zcode-r139c-etag2 案一情形 B：
        // 快臂值已同步落库后 TTL / etag 余腿遭环形页翻转降级时，慢臂不得整
        // 命令重放自碰已提交值——Missing / WrongType 臂读态翻成 Hit 后条件
        // 重判失配回 [0, 新值] 且 etag 侧写永缺、KEEPTTL 形回填读已被
        // upsert 清退的 None 静默丢 TTL，剥尾参后持窗补投余腿出与快臂逐字节
        // 一致的成功帧；跨换号域比对失配即跳余腿只出成功帧），消费即复位
        if matches!(
          cmd,
          RespCommand::Setifmatch | RespCommand::Setifgreater | RespCommand::Setwithetag
        ) {
          snapshot.push(session.etag_resume.tail_bytes());
          session.etag_resume = EtagResume::Full;
        }
        // VADD 慢路径快照尾参追加库级定槽（2 字节 LE；同款先例：
        // exec_slow 无会话可达面，槽位随调度点快照带入，慢臂剥除后交
        // network_vadd；对标 slot_of 的快速臂取值同源）
        if cmd == RespCommand::Vadd {
          snapshot.push(
            slot_of(self.session.namespace(), self.session.active_db())
              .to_le_bytes()
              .to_vec(),
          );
        }
        session.pending_slow = Some(SlowWait::for_command(
          api,
          cmd,
          snapshot,
          session.resp_protocol_version,
          locking,
        ));
      } else {
        // 执行域未挂载的装配缺口：写明错误，绝不静默吞命令
        write_error_raw(&mut output, RESP_ERR_ASYNC_REQUIRED);
      }
    }
    session.output = output;
  }

  fn exec_slow(
    self: Arc<Self>,
    cmd: RespCommand,
    args: Vec<Vec<u8>>,
    resp_version: u8,
  ) -> SlowFuture {
    // 无调度点判定的直调形态：锁器快照取 Basic（慢臂自取闩，与非事务遍同形）
    self.exec_slow_locked(cmd, args, resp_version, SessionLocking::Basic)
  }

  fn exec_slow_locked(
    self: Arc<Self>,
    cmd: RespCommand,
    args: Vec<Vec<u8>>,
    resp_version: u8,
    locking: SessionLocking,
  ) -> SlowFuture {
    // poll 边界绑定包装（FLUSHDB 慢路径的登记表域回收、DEL 系慢路径的缺席
    // 删除钩子等向量臂须见当前执行域会话，见 [`SlowPollSessionBound`]）
    let api = Arc::clone(&self);
    SlowFuture::new(SlowPollSessionBound {
      api,
      inner: async move { self.exec_slow_impl(cmd, args, resp_version, locking).await },
    })
  }

  /// INFO 慢路径覆写臂：扫描行经存储域 [`Self::info_scan_slow`] 异步产出，
  /// 与调度点快照的非扫描面合成组合数据源后单源渲染出帧；poll 边界执行域
  /// 绑定包装与 [`Self::exec_slow`] 同款（存储段 TLS 句柄线程绑定）
  fn exec_slow_info(
    self: Arc<Self>,
    sections: Vec<InfoMetricsType>,
    surface: InfoSurface,
    max_databases: u64,
    active_db: i32,
    resp_version: u8,
  ) -> SlowWait {
    let api = Arc::clone(&self);
    SlowWait::new(SlowPollSessionBound {
      api,
      inner: async move {
        let scan = self.info_scan_slow(&sections, max_databases).await;
        render_info_slow_reply(&sections, &scan, surface, active_db, resp_version)
      },
    })
  }

  #[inline]
  fn set_context(&self, ns: u64, db: u64) -> bool {
    self.session.set_context(ns, db)
  }

  /// 执行域会话物理前缀直取（锁轨种子与物理寻址的 `StoreSession` 单点）
  #[inline]
  fn session_prefix(&self) -> SessionPrefixBuf {
    self.session.session_prefix()
  }

  /// 执行域会话逻辑前缀直取（版本轨种子 `StoreSession` 单点，与写面
  /// bump_watch_version 的逻辑投影同源）
  #[inline]
  fn session_logical_prefix(&self) -> SessionPrefixBuf {
    self.session.session_logical_prefix()
  }

  #[inline]
  fn refresh_active_db(&self) {
    self.session.set_active_db(self.session.active_db());
  }

  /// 引擎级 ACL 变更代数（`Arc<WedbStore>` 单标量，同引擎各连接共视同一源）
  #[inline]
  fn acl_generation(&self) -> Option<u64> {
    Some(self.session.store().acl_generation())
  }

  /// 装配期回挂 PENDING_LAT 计量槽（[`RespServerSession::set_garnet_api`] 挂入
  /// 会话时调用，执行域持有的永远与会话是同一槽；重复回挂保留首次，装配
  /// 单点在类型层面固化）
  #[inline]
  fn attach_pending_latency(&self, meter: Arc<PendingLatencyMeter>) {
    let _ = self.pending_latency.set(meter);
  }

  /// 库快照逐库投影（经检查点通道的数据库管理面枚举；通道未注入的
  /// 嵌入式形态回空集）
  ///
  ///（C# 对位 StoreWrapper.GetDatabasesSnapshot 的转发面，引擎真实现锚在
  /// wkv `WedbStore::store_snapshot`）
  fn store_snapshots(&self) -> Vec<DbSnapshot> {
    self.checkpoint.as_ref().map_or_else(Vec::new, |ctx| {
      ctx
        .database_manager
        .get_databases_snapshot()
        .iter()
        .map(|db| project_db_snapshot(db))
        .collect()
    })
  }
}
