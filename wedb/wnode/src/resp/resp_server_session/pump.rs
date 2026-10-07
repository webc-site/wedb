//! 网络泵驱动面（对标 libs/server/Resp/RespServerSession.cs 的 Send /
//! SendAndReset 冲出与记账段，及 C# 网络线程 BlockingWait 挂起内联的
//! compio 投影：阻塞等待 / 慢路径执行体 / 冷上下文装载的登记与取出，
//! 应答并入口单点）。

use std::{borrow::Cow, mem::take, sync::Arc};

use wcol::itembroker::collection_item_observer::CollectionItemResult;
use wdev::Device;
use wkv::WedbStore;
use wresp::{cmd_strings as cs, command::RespCommand};
use wtxn::TxnState;

use super::core::{DEFAULT_OUTPUT_BUFFER_CAPACITY, RespServerSession};
use crate::resp::{
  BlockedWait, objects::list_commands::write_collection_item_result, slow_path::SlowWait,
};

impl RespServerSession {
  /// 集合更新唤醒（C# StorageSession ListOps/SortedSetOps 写成功后
  /// `itemBroker?.HandleCollectionUpdate(key)`——阻塞观察者经经纪主循环
  /// 试取指派；无经纪或键无观察者均为无害空操作）。裸键随本会话域
  /// (ns, db) 传入，观察表命中由经纪域折叠承担
  pub(crate) fn notify_collection_update(&self, key: &[u8]) {
    if let Some(broker) = &self.item_broker {
      broker.handle_collection_update((self.namespace, self.active_db_id), key);
    }
  }

  /// 读冷上下文挂起摘要（应答组装点消费；None = 上下文已物化，直接应答。
  /// 只读不取——暂存载荷留在 [`RespServerSession::cold_ctx`] 待 SlowWait
  /// 完成应答回写时于 [`Self::resolve_slow_wait_into`] 物化或弃置）
  #[inline]
  pub fn cold_pending_ctx(&self) -> Option<(u64, u64)> {
    self.cold_ctx.as_ref().map(|p| (p.ns, p.db))
  }

  /// 弃置冷上下文挂起暂存载荷（事务窗禁停泊等拒绝/异常路径）
  #[inline]
  pub fn discard_cold_pending_ctx(&mut self) {
    self.cold_ctx = None;
  }

  /// 挂起冷上下文点查装载（严格会话上下文切换的异步闭环）
  ///
  /// 严格会话 `set_context` 报告映射未装载时：预组应答字节交由 SlowWait，
  /// future 点查磁盘 DbMeta 装载既有映射后经 api 重放上下文物化，再原样
  /// 产出应答——应答按流水线序写回，挂起期间本批停止消费，后续命令看到的
  /// 一定是装载后的上下文（磁盘为映射权威，装载不改任何既有映射）。
  ///
  /// `txn_reject` 为事务窗禁停泊围栏的拒绝文案，由调用方按本命令域传入
  /// （SELECT 臂回 SELECT_IN_TXN 族帧、AUTH/HELLO 臂各回本文案，杜绝三域
  /// 共用 HELLO 文案错位，deviations §58b/§58d）；`output` 为调用方应答
  /// 缓冲——存储分派段会话 output 处于 take/restore 移出态，围栏帧直写
  /// 会话缓冲必被还原覆没（EXEC 重放窗空元素即该事故形）
  ///
  /// 重放臂接收 [`wkv::StoreSession::set_context`] 的 bool 契约（与 core.rs
  /// 对照臂 [`RespServerSession::try_switch_active_database_session`] 同口径，
  /// 对标 C# `TryGetOrSetDatabaseSession` success 门）：装载成功不蕴含重放
  /// 物化成功——装载回建的路由快照无绑定方（重放臂会话仅持旧租绑定），空闲
  /// 析构引擎（`route_idle_evict_secs=0` 即引用归零即期的合法稳态）可在装载
  /// 完成至重放判点之间摘除快照，重放 `set_context` 报告 false 且先于一切
  /// 标量存储返回（零盲分配、零半物化）。此刻成功应答会令外层镜像
  ///（namespace / acl_user_handle / HELLO 元数据）经 core.rs 的
  /// `ColdContextPending::materialize_into` 物化出新租户，而内层物理域仍
  /// 锚旧租——外层报新租、读写穿透旧租存储域的跨租户撕裂，且析构不推进
  /// 换代纪元、快路径缓存恒命中旧代，撕裂持续至连接结束。故 false 即产出
  /// '-' 存储错误帧，交 [`Self::resolve_slow_wait_into`] 既有失败通道弃置
  /// 暂存、外层旧值原样保持（零新机制、零第二套判据）
  pub(crate) fn park_cold_context_load<D: Device>(
    &mut self,
    store: &Arc<WedbStore<D>>,
    ns: u64,
    db: u64,
    reply: Cow<'static, [u8]>,
    output: &mut Vec<u8>,
    txn_reject: &'static str,
  ) {
    if self.txn_state != TxnState::None {
      // 事务窗禁停泊围栏：会话在途事务窗（Started/Running/Aborted）内严禁停泊挂起，
      // 弃置暂存并直接写出错误帧，保持会话上下文零撕裂
      self.cold_ctx = None;
      cs::write_error_raw(output, txn_reject);
      return;
    }
    let Some(api) = self.garnet_api.clone() else {
      // 无存储执行域（理论不可达：严格会话必经 garnet_api 装配）：直回应答防挂死
      output.extend_from_slice(&reply);
      return;
    };
    let store = Arc::clone(store);
    self.pending_slow = Some(SlowWait::new(async move {
      match store.resolve_context(ns, db).await {
        // guard 内重放即契约消费点：真物化才原样产出成功应答（SELECT/AUTH/
        // HELLO 三域应答字节不受影响）；Err 臂 guard 短路不执行，重放零触碰
        Ok(_) if api.set_context(ns, db) => reply.into_owned(),
        // 磁盘点查装载失败 / 重放物化未完成（物理域未切、标量未动）：与
        // Err 臂同走 '-' 存储错误帧，失败通道原样承接
        _ => {
          let mut out = Vec::with_capacity(cs::RESP_ERR_SLOW_PATH_STORAGE.len() + 5);
          cs::write_error_raw(&mut out, cs::RESP_ERR_SLOW_PATH_STORAGE);
          out
        }
      }
    }));
  }

  /// 经纪注入时挂起阻塞命令（登记观察者 + pending_block，由网络泵驱动；
  /// cmd_args 懒求值，键段仅在经纪在挂时才物化 to_vec——六阻塞弹臂的
  /// pop_keys 闭包单源收口于此）
  pub(crate) fn park_broker_wait(
    &mut self,
    command: RespCommand,
    timeout: f64,
    keys: &[&[u8]],
    cmd_args: impl FnOnce() -> Vec<Vec<u8>>,
  ) -> bool {
    let Some(broker) = &self.item_broker else {
      return false;
    };
    let observer = broker.start_wait(
      command,
      keys.iter().map(|k| k.to_vec()).collect(),
      self.id as usize,
      cmd_args(),
      (self.namespace, self.active_db_id),
    );
    self.pending_block = Some(BlockedWait::new(
      Arc::clone(broker),
      observer,
      command,
      timeout,
    ));
    true
  }

  /// 取走挂起中的阻塞等待（网络泵专属：await 驱动至完成后经
  /// [`Self::resolve_blocked_wait_into`] 写回应答）
  pub fn take_blocked_wait(&mut self) -> Option<BlockedWait> {
    self.pending_block.take()
  }

  /// 取走挂起中的慢路径执行体（网络泵专属：await 驱动至完成后把应答
  /// 字节按流水线顺序写回；对照 C# 慢命令在网络线程内同步执行的整段语义）
  pub fn take_slow_wait(&mut self) -> Option<SlowWait> {
    self.pending_slow.take()
  }

  /// 阻塞等待完成后的应答写出：直接追加到目标写缓冲（零中间堆分配与二次拷贝）
  ///（C# 各阻塞命令尾部 BlockingWait 之后的 switch 应答段）
  ///
  /// 先经 [`Self::take_output_into`] 冲出会话缓冲内已累积的应答，再把本条
  /// 阻塞命令的应答直写目标缓冲：该段字节绕过 `output`，故出向量另经
  /// [`Self::account_output`] 单点入账（与冲出口同一份实现，两处量取口径）
  ///
  /// `account` 为出账判别：外层泵路径（drive/race.rs 阻塞挂起竞速胜出臂）传
  /// true 入本会话活跃面；脚本内挂起续跑（resume_suspended_script）传
  /// false——C# 该形态应答字节只进内嵌 processor 的私有计数句柄
  /// （SessionScriptCache.cs:64，随 Dispose 入 history 不入外层活跃面），
  /// rust 无内嵌独立句柄，不入账即活跃面对位形
  pub fn resolve_blocked_wait_into(
    &mut self,
    cmd: RespCommand,
    result: CollectionItemResult,
    resp_buf: &mut Vec<u8>,
    account: bool,
  ) {
    self.take_output_into(resp_buf, account);
    let start_len = resp_buf.len();
    let resp_version = self.resp_protocol_version;
    write_collection_item_result(cmd, &result, resp_version, resp_buf);
    if account {
      self.account_output((resp_buf.len() - start_len) as u64);
    }
  }

  /// 慢路径完成后的应答写出（C# 慢命令在网络线程同步执行段的应答产出点）：
  /// 与 [`Self::resolve_blocked_wait_into`] 同一收尾形态——先冲出会话已累积
  /// 应答，再把挂起体产出的应答字节按流水线顺序并入目标写缓冲，绕过
  /// `output` 的这段同样经 [`Self::account_output`] 单点入账
  ///
  /// 网络泵与脚本重入两处承接点共用此一枚并入口，杜绝「挂起应答如何落
  /// 缓冲」的第二份实现
  ///
  /// `account` 出账判别与 [`Self::resolve_blocked_wait_into`] 同形：外层泵
  /// 传 true 入活跃面，脚本内挂起续跑传 false（内嵌字节不入外层活跃面）
  ///
  /// 冷上下文挂起闭环单点（对标 C# `TryGetOrSetDatabaseSession` success 门：
  /// 底层就绪才会话标量才切换）：挂起体产出的应答为错误帧（磁盘点查装载
  /// 失败或重放物化未完成，物理域未切）即弃暂存载荷——会话 `active_db_id` /
  /// `namespace` / `acl_user_handle` 与 HELLO 元数据严格保持旧值，外层镜像与
  /// 底层物理域零撕裂；非错误应答（装载完成且物理域已切）即把暂存标量一次
  /// 物化。失败通道唯一判据即应答首字节 '-'，重放臂（
  /// [`Self::park_cold_context_load`]）的 false 臂与装载 Err 臂同走该通道，
  /// 无第二套判据
  pub fn resolve_slow_wait_into(&mut self, reply: &[u8], resp_buf: &mut Vec<u8>, account: bool) {
    self.take_output_into(resp_buf, account);
    if let Some(pending) = self.cold_ctx.take() {
      // 仅在非事务窗（txn_state == None）消费 cold_ctx：
      // 事务在途时即使挂起体完成也严禁物化（弃置暂存），杜绝跨租户 reset 撕裂事务态
      if self.txn_state == TxnState::None && !reply.first().is_some_and(|&b| b == b'-') {
        pending.materialize_into(self);
      }
    }
    let start_len = resp_buf.len();
    resp_buf.extend_from_slice(reply);
    if account {
      self.account_output((resp_buf.len() - start_len) as u64);
    }
  }

  /// 待发送字节（C# dcurr - GetResponseObjectHead）
  #[inline]
  pub fn pending_output_len(&self) -> usize {
    self.output.len()
  }

  /// 出向字节记账单点（C# 唯一出向记账点 Send 内的会话指标出向累计，
  /// `sessionMetrics?.` 空即跳过；C# 侧既无第二枚冲出口，也无会话级
  /// 出向累计字段）。脚本窗余量等绕过冲出口的直写段经本单点补账，
  /// 全仓出向入账只此一处落账形态
  #[inline]
  pub(super) fn account_output(&self, bytes: u64) {
    if let Some(metrics) = &self.session_metrics {
      metrics.incr_total_net_output_bytes(bytes);
    }
  }

  /// libs/server/Resp/RespServerSession.cs:SendAndReset
  /// libs/server/Resp/RespServerSession.cs:Send
  ///
  /// 冲取出面单点：把会话累积应答并入目标写缓冲并复位
  ///
  /// C# 契约：会话在网络线程直接向网络发送器的池化响应缓冲写出应答
  ///（GarnetTcpNetworkSender.EnterAndGetResponseObject 借出定长块，
  /// LimitedFixedBufferPool.Return 发送完毕无损回池），私有块与池化块
  /// 永不置换身份；本投影保持唯一一份追加复位形态——目标缓冲（通常为
  /// 网络泵池化响应块）与会话 output 各持独立生命周期，块随应答超额至多
  /// 被泵复位点整体换弃重借，绝不流入会话私有域。冲出量为零即无应答可出网，
  /// C# 「写超响应缓冲仍无进展即抛」探针在可扩容 Vec 下无从成立，故无残留
  ///
  /// `account` 为出账判别（入账仍收敛于 [`Self::account_output`] 单点）：
  /// 外层连接出向路径恒传 true；脚本窗内 redis.call 的应答冲出
  /// （lua.rs `RespScriptingApi::dispatch_resp` 两臂）传 false——C# 该形态
  /// 字节只进内嵌 processor 的私有计数句柄不入外层活跃面
  /// （SessionScriptCache.cs:24-26/:64，ScratchBufferNetworkSender 承接），
  /// rust 无内嵌独立句柄，不入账即活跃面契约对位形
  pub fn take_output_into(&mut self, out: &mut Vec<u8>, account: bool) {
    if self.output.is_empty() {
      return;
    }
    let len = self.output.len();
    out.extend_from_slice(&self.output);
    self.output.clear();
    // 单应答峰值容量收敛（接收侧 DEFAULT_RECV_BUFFER_CAPACITY 收敛单点的出向
    // 对偶）：大值应答（如整值 GET 回帧）后峰值容量驻留该连接直至关闭，超
    // 默认驻留水位即换缓冲实例回归——C# WriteDirectLarge 经定长块分片流过
    // 峰值 O(bufferSize)（RespServerSession.cs:WriteDirectLarge/SendAndReset），
    // 平铺 Vec 无分片面，以显式收敛承接同款驻留上界
    if self.output.capacity() > DEFAULT_OUTPUT_BUFFER_CAPACITY {
      self.output = Vec::with_capacity(DEFAULT_OUTPUT_BUFFER_CAPACITY);
    }
    if account {
      self.account_output(len as u64);
    }
  }

  /// 取走会话待释放哨兵（C# ProcessMessages 尾部 `if (toDispose)
  /// DisposeNetworkSender(true)` 的信号通道：QUIT 置位，网络泵发尽本轮
  /// 累积应答后据此断连；取走即复位）
  pub fn take_dispose_request(&mut self) -> bool {
    take(&mut self.to_dispose)
  }

  /// 取走批内输出水位让渡哨兵（网络泵专属：置位表示本批因累计应答达
  /// OUTPUT_WATERMARK_BYTES 在命令边界停住，接收缓冲尚有完整帧待续消费；
  /// 泵实写本轮应答后立即重入消费，不等下一批网络字节。取走即复位）
  pub fn take_output_watermark_yield(&mut self) -> bool {
    take(&mut self.output_watermark_yield)
  }
}
