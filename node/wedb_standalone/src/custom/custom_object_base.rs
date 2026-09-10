//! 自定义对象基类（对标 libs/server/Custom/CustomObjectBase.cs）
//!
//! C# 抽象基类承载自定义对象的引用计数与 Operate 分派（经 CustomObject
//! 管理器与输入管道驱动）；custom 域核心（管理器 / 会话 / 过程）为并行
//! 推进面，本壳保留承接说明。

/// 自定义对象基类
pub struct CustomObjectBase;

impl CustomObjectBase {
  /// libs/server/Custom/CustomObjectBase.cs:Operate
  ///
  /// 缺口说明：自定义对象命令分派入口；rust custom 域并行推进面，
  /// 管理器与输入管道就绪后接线（豁免登记见 js/check/ignore/libs/server/
  /// Custom/CustomObjectBase.yml）
  pub fn operate() {}
}
