//! 网络泵驱动面（对标 libs/server/Resp/RespServerSession.cs 的 Send /
//! SendAndReset 冲出与记账段，及 C# 网络线程 BlockingWait 挂起内联的
//! compio 投影：阻塞等待 / 慢路径执行体 / 冷上下文装载的登记与取出，
//! 应答并入口单点）。

use std::{mem::take, sync::Arc};

use wcol::itembroker::collection_item_observer::CollectionItemResult;
use wdev::Device;
use wkv::WedbStore;
use wresp::{cmd_strings as cs, command::RespCommand};
use wtxn::TxnState;

use super::core::RespServerSession;
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
  pub(crate) fn park_cold_context_load<D: Device>(
    &mut self,
    store: &Arc<WedbStore<D>>,
    ns: u64,
    db: u64,
    reply: Vec<u8>,
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
        Ok(_) => {
          api.set_context(ns, db);
          reply
        }
        Err(_) => {
          let mut out = Vec::with_capacity(cs::RESP_ERR_SLOW_PATH_STORAGE.len() + 5);
          cs::write_error_raw(&mut out, cs::RESP_ERR_SLOW_PATH_STORAGE);
          out
        }
      }
    }));
  }

  /// 经纪注入时挂起阻塞命令（登记观察者 + pending_block，由网络泵驱动；懒求值入参）
  pub(crate) fn park_broker_wait(
    &mut self,
    command: RespCommand,
    timeout: f64,
    keys: impl FnOnce() -> Vec<Vec<u8>>,
    cmd_args: impl FnOnce() -> Vec<Vec<u8>>,
  ) -> bool {
    let Some(broker) = &self.item_broker else {
      return false;
    };
    let observer = broker.start_wait(
      command,
      keys(),
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
  pub fn resolve_blocked_wait_into(
    &mut self,
    cmd: RespCommand,
    result: CollectionItemResult,
    resp_buf: &mut Vec<u8>,
  ) {
    self.take_output_into(resp_buf);
    let start_len = resp_buf.len();
    let resp_version = self.resp_protocol_version;
    write_collection_item_result(cmd, &result, resp_version, resp_buf);
    self.account_output((resp_buf.len() - start_len) as u64);
  }

  /// 慢路径完成后的应答写出（C# 慢命令在网络线程同步执行段的应答产出点）：
  /// 与 [`Self::resolve_blocked_wait_into`] 同一收尾形态——先冲出会话已累积
  /// 应答，再把挂起体产出的应答字节按流水线顺序并入目标写缓冲，绕过
  /// `output` 的这段同样经 [`Self::account_output`] 单点入账
  ///
  /// 网络泵与脚本重入两处承接点共用此一枚并入口，杜绝「挂起应答如何落
  /// 缓冲」的第二份实现
  ///
  /// 冷上下文挂起闭环单点（对标 C# `TryGetOrSetDatabaseSession` success 门：
  /// 底层就绪才会话标量才切换）：挂起体产出的应答为错误帧（磁盘点查装载
  /// 失败，物理域未切）即弃暂存载荷——会话 `active_db_id` / `namespace` /
  /// `acl_user_handle` 与 HELLO 元数据严格保持旧值，外层镜像与底层物理域
  /// 零撕裂；非错误应答（装载完成且物理域已切）即把暂存标量一次物化
  pub fn resolve_slow_wait_into(&mut self, reply: &[u8], resp_buf: &mut Vec<u8>) {
    self.take_output_into(resp_buf);
    if let Some(pending) = self.cold_ctx.take() {
      // 仅在非事务窗（txn_state == None）消费 cold_ctx：
      // 事务在途时即使挂起体完成也严禁物化（弃置暂存），杜绝跨租户 reset 撕裂事务态
      if self.txn_state == TxnState::None && !reply.first().is_some_and(|&b| b == b'-') {
        pending.materialize_into(self);
      }
    }
    let start_len = resp_buf.len();
    resp_buf.extend_from_slice(reply);
    self.account_output((resp_buf.len() - start_len) as u64);
  }

  /// 待发送字节（C# dcurr - GetResponseObjectHead）
  #[inline]
  pub fn pending_output_len(&self) -> usize {
    self.output.len()
  }

  /// 出向字节记账单点（C# 唯一出向记账点 Send 内的会话指标出向累计，
  /// `sessionMetrics?.` 空即跳过；C# 侧既无第二枚冲出口，也无会话级
  /// 出向累计字段）
  #[inline]
  fn account_output(&self, bytes: u64) {
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
  pub fn take_output_into(&mut self, out: &mut Vec<u8>) {
    if self.output.is_empty() {
      return;
    }
    let len = self.output.len();
    out.extend_from_slice(&self.output);
    self.output.clear();
    self.account_output(len as u64);
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
