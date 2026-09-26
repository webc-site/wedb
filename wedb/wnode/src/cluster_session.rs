//! 集群会话切面（对标 libs/server/Cluster/IClusterSession.cs 与 IClusterProvider.cs
//! 的会话侧子集）
//!
//! C# `RespServerSession` 构造期经 `clusterProvider?.CreateClusterSession(...)`
//! 持有 `IClusterSession`，主消费循环、READONLY/READWRITE、CLUSTER 命令与
//! ROLE/HELLO 集群分支均经该切面外达集群域。Rust 依赖方向反转（wnode 不感知
//! 集群实现），由宿主（wedb）以 [`ClusterSession`]（`Arc<dyn ClusterSessionFace>`
//! trait 对象，对标 C# 直接持接口引用的虚分派）注入，会话侧零集群实现耦合：
//! 单机形态注入 None，命令路径与 C# clusterSession == null 分支一致。

use std::sync::Arc;

use wresp::{catalog::SimpleRespKeySpec, command::RespCommand};

use crate::resp::slow_path::SlowWait;

/// 槽位校验门裁决（C# CanServeSlot 门在 compio 协作调度下的三态投影：
/// C# 的 CanOperateOnKey / WaitForSlotToStabalize 在网络线程内联自旋，
/// rust 存储域为 compio 异步、迁移驱动同池协作，内联自旋会饿死推进方，
/// 不可判定时以 [`SlotVerifyGate::Wait`] 挂起重评）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotVerifyGate {
  /// 放行，执行命令分派
  Serve,
  /// 已向 `output` 写出 MOVED/ASK 等重定向或错误应答，跳过分派
  Redirected,
  /// 切面已登记挂起等待体（[`SlowWait`]）：调用方须取走挂起体、回退消费
  /// 游标并停止消费本批；等待体由网络泵驱动至迁移推进/超时后重评本命令
  Wait,
}

/// 集群槽位验证输入（借阅键规格，命令热路径零克隆）
///
/// libs/server/Cluster/ClusterSlotVerificationInput.cs:ClusterSlotVerificationInput
#[derive(Debug, Clone, Copy)]
pub struct ClusterSlotVerificationInput<'a> {
  /// 会话库级槽位：`Mixer(namespace, active_db)`（doc/zh/db.md 4.1，全仓唯一
  /// 定槽真值源 wbase::hash_slot::slot_of）；命令内各键恒共此槽，
  /// 键内容不参与定槽
  pub slot: u16,
  /// 简化键规格（C# keySpecs）
  pub key_specs: &'a [SimpleRespKeySpec],
  /// 是否子命令（BITOP 解析器吞参补偿 -2 偏移同此置位）
  pub is_sub_command: bool,
  /// 命令是否只读（C# readOnly = cmd.IsReadOnly()）
  pub read_only: bool,
  /// 会话 ASKING 剩余计数（C# sessionAsking）
  pub session_asking: u8,
  /// 是否等待槽位迁移稳定（向量集写命令）
  pub wait_for_stable_slot: bool,
}

/// 集群会话能力抽象切面（会话侧集群能力外达接口）
///
/// 各方法与 C# 的接口声明一一对应；集群域实现方（wedb ClusterSession）另行
/// 对标 libs/cluster 下的具体实现。
pub trait ClusterSessionFace: Send + Sync {
  /// 允许本连接以只读会话形态服务副本读（READONLY 命令）
  ///
  /// libs/server/Cluster/IClusterSession.cs:SetReadOnlySession
  fn set_read_only_session(&self);

  /// 恢复默认的副本命令重定向行为（READWRITE 命令）
  ///
  /// libs/server/Cluster/IClusterSession.cs:SetReadWriteSession
  fn set_read_write_session(&self);

  /// 会话批次级纪元快照（0 = 批外空闲，非 0 = 批内持有的 provider 纪元；
  /// 配置过渡静止等待的观测面）
  ///
  /// libs/server/Cluster/IClusterSession.cs:LocalCurrentEpoch
  fn local_current_epoch(&self) -> i64;

  /// 消费批首获取纪元快照（RespServerSession 批入口调用）
  ///
  /// libs/server/Cluster/IClusterSession.cs:AcquireCurrentEpoch
  fn acquire_current_epoch(&self);

  /// 消费批尾释放纪元快照（RespServerSession 批 finally 调用）
  ///
  /// libs/server/Cluster/IClusterSession.cs:ReleaseCurrentEpoch
  fn release_current_epoch(&self);

  /// 多键槽位归属校验；返回 [`SlotVerifyGate::Redirected`] 表示已向
  /// `output` 写入 MOVED/ASK 等重定向错误（调用方据此跳过命令执行，对标
  /// CanServeSlot 取反门）；返回 [`SlotVerifyGate::Wait`] 表示键正处于
  /// 迁移传输/删除或槽位未稳定（C# CanOperateOnKey / WaitForSlotToStabalize
  /// 自旋等待面），切面已登记挂起等待体，调用方经 [`Self::take_pending_slow`]
  /// 取走并回退游标挂起重评
  ///
  /// libs/server/Cluster/IClusterSession.cs:NetworkMultiKeySlotVerify
  fn network_multi_key_slot_verify(
    &self,
    input: &ClusterSlotVerificationInput<'_>,
    args: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> SlotVerifyGate;

  /// 多键槽位归属校验无应答臂（C# 把「判定」与「渲染」分成两个方法：本口只作
  /// 裁决、不渲染错误字节，[`Self::network_multi_key_slot_verify`] 才写
  /// `output`）：返回 true = 不可由本节点服务（C# `vres.state != OK`），
  /// 键门挂起（[`SlotVerifyGate::Wait`] 同属非放行态）亦计 true。
  ///
  /// 供只需布尔判定的调用方（投机前视 GET 探测）使用——免按有应答臂形态
  /// 渲染 MOVED/ASK 字节到一块随即丢弃的输出缓冲；两臂共用切面内同一判定
  /// 核与同一键提取路径，本口不复建第二套槽位判定。
  ///
  /// libs/cluster/Session/SlotVerification/RespClusterSlotVerify.cs:NetworkMultiKeySlotVerifyNoResponse
  fn network_multi_key_slot_verify_no_response(
    &self,
    input: &ClusterSlotVerificationInput<'_>,
    args: &[&[u8]],
  ) -> bool;

  /// 处理 CLUSTER 子命令族（`cmd` 为解析器解析后的子命令枚举，`args` 为
  /// 子命令名之后的剩余参数，`slot` 为调用会话的库级槽位——CLUSTER KEYSLOT
  /// 按 doc/zh/db.md 4.1 回声会话当前库槽位，键内容不参与定槽）
  ///
  /// 本方法是 C# 两级同名重载合并后的唯一入口：入层判定与顶层
  /// MIGRATE / FAILOVER / REPLICAOF / SECONDARYOF 三臂在外层，CLUSTER_*
  /// 转调 switch 在内层，两级实现在宿主侧同一模块内分治，勿在 trait 上
  /// 并列第二枚入口
  ///
  /// libs/server/Cluster/IClusterSession.cs:ProcessClusterCommands
  fn process_cluster_commands(
    &self,
    cmd: RespCommand,
    args: &[&[u8]],
    output: &mut Vec<u8>,
    slot: u16,
  ) -> bool;

  /// 会话析构清理
  ///
  /// libs/server/Cluster/IClusterSession.cs:Dispose
  fn dispose(&self);

  /// gossip 对端节点 id（None = 非节点间连接；连接一旦确立不随 gossip
  /// 变更，唯一写入点 CLUSTER GOSSIP 建链 RespClusterBasicCommands.cs
  /// 内 RemoteNodeId 记忆）。CLIENT 族类型判定经它区分节点间连接
  ///（M/S，按远端节点角色 IsReplica）与普通/订阅连接（N/P）
  ///
  /// trait 方法对位接口属性，接口声明在 IClusterSession，实现与写入点在
  /// ClusterSession（partial RespClusterBasicCommands）
  ///
  /// libs/server/Cluster/IClusterSession.cs:RemoteNodeId
  fn remote_node_id(&self) -> Option<u128> {
    None
  }

  /// 取走切面挂起的慢路径执行体（无慢路径切面恒 None）
  ///
  /// CLUSTER RESET 等需异步闭环的集群命令：同步段仅校验参数，异步段
  ///（HasKeysInSlots 扫描 / 清库）经 [`SlowWait`] 由网络泵驱动——对标
  /// C# TryReset 内联扫描的整段语义
  fn take_pending_slow(&self) -> Option<SlowWait> {
    None
  }

  /// 取走切面登记的致命断流（C# 集群命令 `GarnetException`
  /// `clientResponse: false` 上抛的等价信号：不写错误应答行，发尽累积
  /// 应答后断连）；会话在 [`Self::process_cluster_commands`] 返回后立即
  /// 检查并转投影为会话致命哨兵。默认 None
  fn take_fatal_disconnect(&self) -> Option<String> {
    None
  }

  /// 重置迭代式槽位校验缓存（事务批次起点；无集群切面 no-op）
  ///
  /// 接口面默认空实现：真实重置在集群切面（wedb cluster_session 固有
  /// 方法，映射注释在该层）
  fn reset_cached_slot_verification_result(&self) {}

  /// 集群态 PUBLISH/SPUBLISH 跨节点广播（C# PubSubCommands.cs:140-147：
  /// EnableCluster 时网络线程 BlockingWait clusterProvider.ClusterPublishAsync；
  /// rust 会话→集群域唯一通道是本切面，C# 直连 provider 的形态由宿主实现
  /// 内部转达 provider 承接）
  ///
  /// 返回 false = trait 默认兜底（无集群切面）；true = 转发已同步闭环
  ///（compio 单线程执行域内联驱动，C# BlockingWait 等价）
  ///
  /// 集群未装配形态（等价 C# EnableCluster == false）由命令层门先行拦截：
  /// SPUBLISH 回 CLUSTER_DISABLED、PUBLISH 仅本地广播，本切面对未装配态不被
  /// 触达（判据单点见 wpubsub `session_commands` 的 `has_cluster_session` 门）
  fn cluster_publish(&self, cmd: RespCommand, channel: &[u8], message: &[u8]) -> bool {
    let _ = (cmd, channel, message);
    false
  }

  /// 集群拓扑配置刷盘（C# CONFIG REWRITE 路径 `storeWrapper.clusterProvider?
  /// .FlushConfig()`（ServerConfig.cs:105）的会话侧触达面：rust 会话→集群域
  /// 经本切面中转，同 [`Self::cluster_publish`] 的注入架构胶水形态（provider
  /// 侧实现映射见 IClusterProvider::flush_config）；无集群切面 no-op =
  /// C# 空条件调用符跳过）
  fn flush_config(&self) {}
}

/// 集群会话拥有态句柄（`Arc<dyn ClusterSessionFace>` trait 对象，对标 C#
/// 直接持 `IClusterSession` 接口引用的虚分派；单机形态注入 None）
pub type ClusterSession = Arc<dyn ClusterSessionFace>;
