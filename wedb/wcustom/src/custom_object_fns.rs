//! 自定义对象命令静态执行体（对标 libs/server/Custom/CommandType.cs:CommandType
//! 与 libs/server/Custom/CustomObjectFunctions.cs 四接口的函数指针承接）。
//!
//! 扩展命令按转写规范走编译期静态分发（C# CustomCommandManager 动态注册
//! 管理层不转写），本模块只承载执行体签名与命令类型，注册面由各扩展 crate
//! （如 wext_roaring）以静态清单提供。
//!
//! 执行体取 const fn 指针结构体而非 enum + enum_dispatch：C#
//! `CustomObjectFunctions` 本就是抽象基类虚方法，每命令一个子类实例，const
//! 执行体集是其 1:1 静态投影（每命令一个编译期常量值，调用点直呼）；若在
//! 低层 wcustom 枚举全部扩展命令需依赖倒置 wext_json / wext_roaring，
//! 与 C# 虚调用同代价下无收益。

use core::fmt;

/// 命令类型（对齐 libs/server/Custom/CommandType.cs:CommandType）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CommandType {
  /// 只读。
  Read = 0,
  /// 读-改-写。
  ReadModifyWrite = 1,
}

/// 会话 RESP 协议版本（执行体写出 nil 应答时裁决帧型）
///
/// C# 侧 nil 帧由 `ref RespMemoryWriter`（构造时带 respProtocolVersion，
/// libs/common/RespMemoryWriter.cs 的 resp3 字段）与 `FunctionsState.nilResp`
/// （libs/server/Storage/Functions/FunctionsState.cs:nilResp）承载；rust 执行体
/// 为无捕获 fn 指针，故版本以入参穿到命令层，写出统一转调
/// wresp::ext::RespVecExt::write_resp_null_ver。
pub type RespVersion = u8;

/// NeedInitialUpdate 执行体签名：参数域 → RESP 输出（false = 已写错误应答）
pub type CustomNeedInitialFn = fn(&[&[u8]], &mut Vec<u8>, RespVersion) -> bool;
/// Updater 执行体签名：载荷就地改写 + 参数域 → RESP 输出
pub type CustomUpdaterFn = fn(&mut Vec<u8>, &[&[u8]], &mut Vec<u8>, RespVersion) -> bool;
/// Reader 执行体签名：载荷 + 参数域 → RESP 输出
pub type CustomReaderFn = fn(&[u8], &[&[u8]], &mut Vec<u8>, RespVersion) -> bool;
/// NotFound 执行体签名：参数域 → RESP 输出
pub type CustomNotFoundFn = fn(&[&[u8]], &mut Vec<u8>, RespVersion);
/// 空对象判定签名
pub type CustomIsEmptyFn = fn(&[u8]) -> bool;

/// 自定义对象命令执行体（CustomObjectFunctions 四接口的函数指针承接）
///
/// libs/server/Custom/CustomObjectFunctions.cs:NeedInitialUpdate / Updater /
/// Reader / NotFound
///
/// 载荷域为对象信封载荷字节（信封首字节类型标签由扩展静态描述单点承载，
/// 如 wext_roaring::RoaringCommand::OBJECT_ENTRY.tag）。空载荷 = 键缺失时
/// 新建对象的零载荷初值（C# 工厂 Create 的承接形态，Updater 自行解出）。
///
/// 对照 C# 分层：C# 框架（Tsavorite）持工厂并管理对象生命周期，命令类
/// 只收 `IGarnetObject`；rust 侧对象以信封载荷字节落库，序列化/反序列化
/// 内聚到执行体（等价 C# 工厂 Create/SerializeObject/Deserialize 分层）。
#[derive(Clone, Copy)]
pub struct CustomObjectFns {
  /// NeedInitialUpdate：键缺失时建对象前先行校验（防空墓碑）；
  /// false = output 已写错误应答，放弃建对象
  pub need_initial_update: CustomNeedInitialFn,
  /// Updater：读改写执行体（载荷就地改写后回写）；false = output 已写
  /// 错误应答，放弃落库（C# AbortWithErrorMessage）
  pub updater: CustomUpdaterFn,
  /// Reader：命中键只读执行体；false = output 已写错误应答
  pub reader: CustomReaderFn,
  /// NotFound：缺键只读应答（读不建键；C# 缺省实现 WriteNull）
  pub not_found: CustomNotFoundFn,
  /// 载荷是否空对象（空 → 整键回收，wedb 严格删空公理；
  /// 对照 C# 空对象常驻的刻意差异，见 wnode 执行层注释）
  pub is_empty: CustomIsEmptyFn,
}

impl fmt::Debug for CustomObjectFns {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    // 函数指针无稳定 Debug 形态，仅呈现结构面
    f.write_str("CustomObjectFns")
  }
}
