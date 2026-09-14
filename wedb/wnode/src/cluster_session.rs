//! 集群会话切面（对标 libs/server/Cluster/IClusterSession.cs 与 IClusterProvider.cs
//! 的会话侧子集）
//!
//! C# `RespServerSession` 构造期经 `clusterProvider?.CreateClusterSession(...)`
//! 持有 `IClusterSession`，主消费循环、READONLY/READWRITE、CLUSTER 命令与
//! ROLE/HELLO 集群分支均经该切面外达集群域。Rust 依赖方向反转（wnode 不感知
//! 集群实现），由宿主（wedb）以静态虚表句柄注入，会话侧零集群实现耦合：
//! 单机形态注入 None，命令路径与 C# clusterSession == null 分支一致。

use std::{marker::PhantomData, ptr, sync::Arc};

use waof::AofAddress;
use wresp::RespCommand;

use crate::{
  RoleInfo, key_spec::SimpleRespKeySpec, resp::slow_path::SlowWait,
  session_parse_state_extensions::ManagerType,
};

/// 槽位多键校验函数指针类型
pub type SlotVerifyFn =
  unsafe fn(*const (), &ClusterSlotVerificationInput<'_>, &[&[u8]], &mut Vec<u8>) -> bool;

/// 集群命令处理函数指针类型
pub type ProcessClusterCmdFn = unsafe fn(*const (), RespCommand, &[&[u8]], &mut Vec<u8>) -> bool;

/// 集群槽位验证输入（借阅键规格，命令热路径零克隆）
///
/// libs/server/Cluster/ClusterSlotVerificationInput.cs:ClusterSlotVerificationInput
#[derive(Debug, Clone, Copy)]
pub struct ClusterSlotVerificationInput<'a> {
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

  /// 多键槽位归属校验；返回 true 表示已向 `output` 写入 MOVED/ASK 等
  /// 重定向错误（调用方据此跳过命令执行，对标 CanServeSlot 取反门）
  ///
  /// libs/server/Cluster/IClusterSession.cs:NetworkMultiKeySlotVerify
  fn network_multi_key_slot_verify(
    &self,
    input: &ClusterSlotVerificationInput<'_>,
    args: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> bool;

  /// 处理 CLUSTER 子命令族（`cmd` 为解析器解析后的子命令枚举，`args` 为
  /// 子命令名之后的剩余参数）
  ///
  /// libs/server/Cluster/IClusterSession.cs:ProcessClusterCommands
  fn process_cluster_commands(
    &self,
    cmd: RespCommand,
    args: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> bool;

  /// 当前节点是否为主节点（ROLE / HELLO 集群分支）
  ///
  /// libs/server/Cluster/IClusterProvider.cs:IsPrimary
  fn is_primary(&self) -> bool;

  /// 当前节点是否为副本节点（HELLO role 字段集群分支）
  ///
  /// libs/server/Cluster/IClusterProvider.cs:IsReplica
  fn is_replica(&self) -> bool;

  /// 主节点视角复制位点与全部挂载副本元数据（ROLE 集群分支主形态）
  ///
  /// libs/server/Cluster/IClusterProvider.cs:GetPrimaryInfo
  fn get_primary_info(&self) -> (AofAddress, Vec<RoleInfo>);

  /// 副本视角自身角色元数据（ROLE 集群分支副本形态）
  ///
  /// libs/server/Cluster/IClusterProvider.cs:GetReplicaInfo
  fn get_replica_info(&self) -> RoleInfo;

  /// AOF 物理子日志数（ROLE 命令 usingShardedLog 判定输入，
  /// C# serverOptions.AofPhysicalSublogCount）
  fn aof_sublog_count(&self) -> usize;

  /// 会话析构清理
  ///
  /// libs/server/Cluster/IClusterSession.cs:Dispose
  fn dispose(&self);

  /// 取走切面挂起的慢路径执行体（无慢路径切面恒 None）
  ///
  /// CLUSTER RESET 等需异步闭环的集群命令：同步段仅校验参数，异步段
  /// （HasKeysInSlots 扫描 / 清库）经 [`SlowWait`] 由网络泵驱动——对标
  /// C# TryReset 内联扫描的整段语义
  fn take_pending_slow(&self) -> Option<SlowWait> {
    None
  }

  /// DEBUG PURGEBP 集群侧缓冲池清洗（C# RespServerSession.ClusterPurgeBufferPool
  /// → clusterProvider.PurgeBufferPool；`manager_type` 已由命令层解析）
  ///
  /// 默认 no-op：无集群装配面的宿主由命令层回 CLUSTER_DISABLED，不经此面
  fn purge_buffer_pool(&self, manager_type: ManagerType) {
    let _ = manager_type;
  }
}

/// 静态虚表声明，消除动态分发
pub struct ClusterSessionVtable {
  pub set_read_only_session: unsafe fn(*const ()),
  pub set_read_write_session: unsafe fn(*const ()),
  pub network_multi_key_slot_verify: SlotVerifyFn,
  pub process_cluster_commands: ProcessClusterCmdFn,
  pub is_primary: unsafe fn(*const ()) -> bool,
  pub is_replica: unsafe fn(*const ()) -> bool,
  pub get_primary_info: unsafe fn(*const ()) -> (AofAddress, Vec<RoleInfo>),
  pub get_replica_info: unsafe fn(*const ()) -> RoleInfo,
  pub aof_sublog_count: unsafe fn(*const ()) -> usize,
  pub dispose: unsafe fn(*const ()),
  pub take_pending_slow: unsafe fn(*const ()) -> Option<SlowWait>,
  pub purge_buffer_pool: unsafe fn(*const (), ManagerType),
  pub drop: unsafe fn(*const ()),
  pub clone: unsafe fn(*const ()) -> *const (),
}

/// 集群会话句柄（类型擦除 + 静态虚表，零动态分发）
pub struct ClusterSession {
  ptr: *const (),
  vtable: &'static ClusterSessionVtable,
}

unsafe impl Send for ClusterSession {}
unsafe impl Sync for ClusterSession {}

impl ClusterSession {
  /// 从实现了 [`ClusterSessionFace`] 的实例构造句柄
  pub fn new<T: ClusterSessionFace + 'static>(target: T) -> Self {
    Self::from_arc(Arc::new(target))
  }

  /// 从 `Arc<T>` 构造句柄
  pub fn from_arc<T: ClusterSessionFace + 'static>(arc: Arc<T>) -> Self {
    struct VtableHolder<T>(PhantomData<T>);
    impl<T: ClusterSessionFace + 'static> VtableHolder<T> {
      const VTABLE: ClusterSessionVtable = ClusterSessionVtable {
        set_read_only_session: |ptr| unsafe { (*(ptr as *const T)).set_read_only_session() },
        set_read_write_session: |ptr| unsafe { (*(ptr as *const T)).set_read_write_session() },
        network_multi_key_slot_verify: |ptr, input, args, output| unsafe {
          (*(ptr as *const T)).network_multi_key_slot_verify(input, args, output)
        },
        process_cluster_commands: |ptr, cmd, args, output| unsafe {
          (*(ptr as *const T)).process_cluster_commands(cmd, args, output)
        },
        is_primary: |ptr| unsafe { (*(ptr as *const T)).is_primary() },
        is_replica: |ptr| unsafe { (*(ptr as *const T)).is_replica() },
        get_primary_info: |ptr| unsafe { (*(ptr as *const T)).get_primary_info() },
        get_replica_info: |ptr| unsafe { (*(ptr as *const T)).get_replica_info() },
        aof_sublog_count: |ptr| unsafe { (*(ptr as *const T)).aof_sublog_count() },
        dispose: |ptr| unsafe { (*(ptr as *const T)).dispose() },
        take_pending_slow: |ptr| unsafe { (*(ptr as *const T)).take_pending_slow() },
        purge_buffer_pool: |ptr, manager_type| unsafe {
          (*(ptr as *const T)).purge_buffer_pool(manager_type)
        },
        drop: |ptr| unsafe { drop(Arc::from_raw(ptr as *const T)) },
        clone: |ptr| unsafe {
          let arc = Arc::from_raw(ptr as *const T);
          let cloned = Arc::clone(&arc);
          let _ = Arc::into_raw(arc);
          Arc::into_raw(cloned) as *const ()
        },
      };
    }

    Self {
      ptr: Arc::into_raw(arc) as *const (),
      vtable: &VtableHolder::<T>::VTABLE,
    }
  }

  #[inline]
  pub fn set_read_only_session(&self) {
    unsafe { (self.vtable.set_read_only_session)(self.ptr) }
  }

  #[inline]
  pub fn set_read_write_session(&self) {
    unsafe { (self.vtable.set_read_write_session)(self.ptr) }
  }

  #[inline]
  pub fn network_multi_key_slot_verify(
    &self,
    input: &ClusterSlotVerificationInput<'_>,
    args: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> bool {
    unsafe { (self.vtable.network_multi_key_slot_verify)(self.ptr, input, args, output) }
  }

  #[inline]
  pub fn process_cluster_commands(
    &self,
    cmd: RespCommand,
    args: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> bool {
    unsafe { (self.vtable.process_cluster_commands)(self.ptr, cmd, args, output) }
  }

  #[inline]
  pub fn is_primary(&self) -> bool {
    unsafe { (self.vtable.is_primary)(self.ptr) }
  }

  #[inline]
  pub fn is_replica(&self) -> bool {
    unsafe { (self.vtable.is_replica)(self.ptr) }
  }

  #[inline]
  pub fn get_primary_info(&self) -> (AofAddress, Vec<RoleInfo>) {
    unsafe { (self.vtable.get_primary_info)(self.ptr) }
  }

  #[inline]
  pub fn get_replica_info(&self) -> RoleInfo {
    unsafe { (self.vtable.get_replica_info)(self.ptr) }
  }

  #[inline]
  pub fn aof_sublog_count(&self) -> usize {
    unsafe { (self.vtable.aof_sublog_count)(self.ptr) }
  }

  #[inline]
  pub fn dispose(&self) {
    unsafe { (self.vtable.dispose)(self.ptr) }
  }

  /// 取走切面挂起的慢路径执行体（CLUSTER RESET 等异步闭环集群命令）
  #[inline]
  pub fn take_pending_slow(&self) -> Option<SlowWait> {
    unsafe { (self.vtable.take_pending_slow)(self.ptr) }
  }

  /// DEBUG PURGEBP 集群侧缓冲池清洗（转发切面实现）
  #[inline]
  pub fn purge_buffer_pool(&self, manager_type: ManagerType) {
    unsafe { (self.vtable.purge_buffer_pool)(self.ptr, manager_type) }
  }
}

impl Drop for ClusterSession {
  fn drop(&mut self) {
    if !self.ptr.is_null() {
      unsafe { (self.vtable.drop)(self.ptr) };
      self.ptr = ptr::null();
    }
  }
}

impl Clone for ClusterSession {
  fn clone(&self) -> Self {
    Self {
      ptr: unsafe { (self.vtable.clone)(self.ptr) },
      vtable: self.vtable,
    }
  }
}

impl<T: ClusterSessionFace + 'static> From<Arc<T>> for ClusterSession {
  fn from(arc: Arc<T>) -> Self {
    Self::from_arc(arc)
  }
}
