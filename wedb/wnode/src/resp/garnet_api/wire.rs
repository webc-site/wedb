//! [`GarnetApi`] 双臂分派句柄（枚举 + `From` 转换 + 双臂纯转发宏）
//!
//! 对标 C# 直持 storageApi 引用；本文件只承接句柄组装与转发，
//! 命令执行臂见 [`super::exec`]，trait 面与执行域实现见 [`super`]。

use std::sync::Arc;

use wdev::SegmentedDevice;
use wkv::SessionLocking;
use wmetric::{DbSnapshot, PendingLatencyMeter};
use wresp::{command::RespCommand, metrics::InfoMetricsType};
use wval::SessionPrefixBuf;

use super::{GarnetApiFace, StoreGarnetApi};
use crate::resp::{
  RespServerSession,
  info_provider::InfoSurface,
  slow_path::{SlowFuture, SlowWait},
};

/// libs/server/API/IGarnetApi.cs:IGarnetApi
///
/// 存储执行域命令分派句柄（双臂枚举，对标 C# 直持 storageApi 引用）
///
/// 甄别结论：[`GarnetApiFace`] 全仓产线实现唯一（[`StoreGarnetApi`]，产线
/// 设备唯一为 `SegmentedDevice`），故产线臂单实例具体化为
/// `Arc<StoreGarnetApi<SegmentedDevice>>`——每命令热路径（[`Self::exec`] 与
/// 慢路径调度）零虚调用、AUTH/ACL 异步臂零装箱 future。trait 不删：
/// wnode/tests 与 wedb/tests 共 6 个无存储替身（OkApi / EchoApi / MockApi /
/// NeverApi / SlowMockApi / UnreachableApi）依赖同一切面注入会话（人工延时
/// 触发慢日志阈值、不可达哨兵断言命令不下沉存储等行为真存储无法承载），经
/// [`GarnetApi::Face`] 臂保留注入位，虚分派开销仅存在于测试路径
#[derive(Clone)]
pub enum GarnetApi {
  /// 产线形态：具体执行域单实例（构造单点
  /// `StorageSessionProvider::get_session`，单机与集群共用执行域）
  Store(Arc<StoreGarnetApi<SegmentedDevice>>),
  /// 测试注入臂（对象安全分派面 [`GarnetApiFace`]）：承接无存储测试替身
  /// （人工延时、不可达哨兵）与非产线设备形态的执行域（故障注入设备测试）；
  /// 产线构造点恒 [`Self::Store`] 臂
  Face(Arc<dyn GarnetApiFace>),
}

impl From<StoreGarnetApi<SegmentedDevice>> for GarnetApi {
  fn from(api: StoreGarnetApi<SegmentedDevice>) -> Self {
    Self::Store(Arc::new(api))
  }
}

impl From<Arc<StoreGarnetApi<SegmentedDevice>>> for GarnetApi {
  fn from(api: Arc<StoreGarnetApi<SegmentedDevice>>) -> Self {
    Self::Store(api)
  }
}

impl From<Arc<dyn GarnetApiFace>> for GarnetApi {
  fn from(api: Arc<dyn GarnetApiFace>) -> Self {
    Self::Face(api)
  }
}

/// [`GarnetApi`] 双臂纯转发宏（同步方法，`&self` 接收者）
///
/// 对位 C# GarnetApi.cs 的 partial 组织：双臂转发签名单点声明，`Store` /
/// `Face` 分派体单点展开，消除逐方法 match 样板（此前 14 方法 × 2 臂三层
/// 同签名重复之一）。条目签名与 [`GarnetApiFace`] / [`StoreGarnetApi`] 同名
/// 方法逐字一致，doc 与 `#[inline]` 等属性随条目透传，展开与原手写 match
/// 转发完全等价
macro_rules! forward_garnet_api {
  (
    $(
      $(#[$meta:meta])*
      fn $name:ident ($($arg:ident : $ty:ty),* $(,)?) -> $ret:ty;
    )*
  ) => {
    $(
      $(#[$meta])*
      pub fn $name(&self, $($arg: $ty),*) -> $ret {
        match self {
          Self::Store(api) => api.$name($($arg),*),
          Self::Face(api) => api.$name($($arg),*),
        }
      }
    )*
  };
}

/// [`GarnetApi`] 双臂纯转发宏（同步方法，`self` 接收者：句柄 move 进
/// `self: Arc<Self>` 接收者的慢路径臂）；条目形态同 [`forward_garnet_api!`]
macro_rules! forward_garnet_api_self {
  (
    $(
      $(#[$meta:meta])*
      fn $name:ident ($($arg:ident : $ty:ty),* $(,)?) -> $ret:ty;
    )*
  ) => {
    $(
      $(#[$meta])*
      pub fn $name(self, $($arg: $ty),*) -> $ret {
        match self {
          Self::Store(api) => api.$name($($arg),*),
          Self::Face(api) => api.$name($($arg),*),
        }
      }
    )*
  };
}

/// [`GarnetApi`] 双臂纯转发宏（异步方法，`&self` 接收者）
///
/// 两臂各自内联 `.await` 收割：产线 Store 臂 inherent async fn（RPITIT）
/// 零装箱、Face 臂手工装箱 future 仅在分派调用点内联收割不跨线程移交，
/// 两语义均与手写版完全一致
macro_rules! forward_garnet_api_async {
  (
    $(
      $(#[$meta:meta])*
      async fn $name:ident ($($arg:ident : $ty:ty),* $(,)?) -> $ret:ty;
    )*
  ) => {
    $(
      $(#[$meta])*
      pub async fn $name(&self, $($arg: $ty),*) -> $ret {
        match self {
          Self::Store(api) => api.$name($($arg),*).await,
          Self::Face(api) => api.$name($($arg),*).await,
        }
      }
    )*
  };
}

impl GarnetApi {
  forward_garnet_api! {
    /// 执行一条命令并写回应答到会话输出缓冲（每命令热路径单点：Store 臂
    /// 具体类型静态分派，Face 臂 trait 对象分派——仅测试路径）
    #[inline]
    fn exec(session: &mut RespServerSession, cmd: RespCommand, args: &[&[u8]]) -> ();

    /// 切换当前会话底层存储上下文（命名空间 + 数据库）
    ///
    /// 返回是否完成上下文物化：严格会话映射未装载（冷租户/冷库）时返回
    /// false，由调用方挂起 [`wkv::WedbStore::resolve_context`] 点查装载后重放
    #[inline]
    fn set_context(ns: u64, db: u64) -> bool;

    /// 执行域存储会话的**物理**归属前缀（`[NsVarint][DbVarint]`，锁轨种子与
    /// 一切物理寻址的真值源单点）；缺省根域 = 嵌入式/无存储执行域形态
    #[inline]
    fn session_prefix() -> SessionPrefixBuf;

    /// 执行域存储会话的**逻辑**归属前缀（版本轨种子单点：WATCH 分槽与写面
    /// 推进共用）；缺省根域口径同 [`Self::session_prefix`]
    #[inline]
    fn session_logical_prefix() -> SessionPrefixBuf;

    /// 刷新执行域会话物理前缀（换号清库后对齐虚拟数据库代数）
    #[inline]
    fn refresh_active_db() -> ();

    /// 引擎级 ACL 变更代数（跨连接改权收敛的唯一判据源）；缺省 None =
    /// 嵌入式 / 测试桩形态无存储执行域
    #[inline]
    fn acl_generation() -> Option<u64>;

    /// 全部库的存储域快照（STORE / PERSISTENCE 段与 MEMORY store_* 项的
    /// 数据面）；缺省空集：嵌入式 / 测试桩形态无存储执行域注入
    fn store_snapshots() -> Vec<DbSnapshot>;

    /// 装配期回挂 PENDING_LAT 计量槽（[`RespServerSession::set_garnet_api`]
    /// 挂入会话时调用）；非存储域执行形态无延迟面，默认空实现
    fn attach_pending_latency(meter: Arc<PendingLatencyMeter>) -> ();
  }

  forward_garnet_api_async! {
    /// AUTH / HELLO / ACL 族命令臂（异步域独占）
    ///
    /// 认证与 ACL 的底层存储点查须在 async 上下文内联收割（冷记录降级为
    /// 异步落盘回读，严禁同步驱动重入 compio 调度器），且认证成功后须回写
    /// 会话本地句柄/命名空间，仅分派段可达（慢路径仅产出应答字节，无会话态
    /// 变更面）。存储为 ACL 唯一真源，见 doc/zh/db.md §3。调用方以
    /// `cmd == Auth || cmd == Hello || is_acl_command(cmd)` 预筛后才进入本臂。
    /// Face 臂沿 [`GarnetApiFace`] 分派：真存储执行域（`consumer_on<D>` 故障
    /// 注入设备面裹入的 [`StoreGarnetApi`]）AUTH/ACL 链路真实闭环，无存储替身
    /// 沿 trait 默认臂恒 false，命令落 [`GarnetApiFace::exec`] 通用分派
    async fn exec_auth_acl(
      session: &mut RespServerSession,
      cmd: RespCommand,
      args: &[&[u8]],
    ) -> bool;

    /// ACL 挂载陈旧的异步刷新臂（重驱型：不产应答，刷新完成后原命令游标
    /// 回退重解析，门链以新挂载重评）
    ///
    /// 跨连接改权收敛预门 [`RespServerSession::refresh_acl_mount_if_stale`]
    /// 的点查臂：挂载代数落后即按会话已绑 `(ns, 用户名)` 点查存储真源重建
    /// 句柄。Face 臂沿 [`GarnetApiFace`] 分派：真存储执行域实现刷新点查臂，
    /// 无存储替身沿 trait 默认臂免刷新直过（会话挂载与引擎代数本就无源可陈旧）
    async fn exec_acl_refresh(session: &mut RespServerSession) -> ();

    /// 按 `(ns, 用户名)` 点查 ACL 用户规则字节（挂载代数陈旧时的句柄重建口）
    ///
    /// 调用约束同 [`crate::resp::acl_store::AclStore::read`]：须在批处理纪元
    /// 保护区外 await（冷记录降级落盘回读）。外层 None = 本执行域无 ACL 存储
    /// 真源面，与 [`Self::acl_generation`] 成对，非存储形态下会话不登记挂载态
    /// 故不可达。Face 臂沿 [`GarnetApiFace`] 分派：真存储执行域转发点查，
    /// 无存储替身沿 trait 默认臂恒 None
    async fn acl_user_record(ns: u64, username: &[u8]) -> Option<wkv::Result<Option<Vec<u8>>>>;
  }

  forward_garnet_api_self! {
    /// 慢路径异步执行（同步段返回 `Ok(false)` 的命令在异步域闭环）
    #[inline]
    fn exec_slow(cmd: RespCommand, args: Vec<Vec<u8>>, resp_version: u8) -> SlowFuture;

    /// 慢路径异步执行 + 调度点锁器模式快照下传（[`SlowWait::for_command`]
    /// 通道；[`Self::exec_slow`] 的带判据变体）
    #[inline]
    fn exec_slow_locked(
      cmd: RespCommand,
      args: Vec<Vec<u8>>,
      resp_version: u8,
      locking: SessionLocking,
    ) -> SlowFuture;
  }

  /// INFO 慢路径异步臂（凡段集含扫描族段的 INFO 请求整请求降级后的闭环）
  //（宏外特例手写：`self: Arc<Self>` 接收者的 trait 方法须先克隆拥有态
  // 句柄再调用，转发体非 `api.$m(...)` 单形，不进转发宏）
  #[inline]
  pub fn exec_slow_info(
    &self,
    sections: Vec<InfoMetricsType>,
    surface: InfoSurface,
    max_databases: u64,
    active_db: i32,
    resp_version: u8,
  ) -> SlowWait {
    match self {
      Self::Store(api) => {
        Arc::clone(api).exec_slow_info(sections, surface, max_databases, active_db, resp_version)
      }
      Self::Face(api) => {
        Arc::clone(api).exec_slow_info(sections, surface, max_databases, active_db, resp_version)
      }
    }
  }
}
