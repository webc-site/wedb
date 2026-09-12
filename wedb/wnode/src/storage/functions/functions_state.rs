//! 会话函数状态（对标 libs/server/Storage/Functions/FunctionsState.cs）
//!
//! C# 侧 FunctionsState 聚合 CustomCommandManager / CustomObjectFactory /
//! GarnetObjectSerializer 等会话级依赖；custom 命令域为并行转写域。

/// 会话函数状态容器
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct FunctionsState {
  /// 自定义命令管理器是否已挂载
  pub has_custom_commands: bool,
  /// 自定义对象工厂是否已挂载
  pub has_custom_object_factory: bool,
}

impl FunctionsState {
  /// 创建未挂载自定义扩展的默认状态
  pub fn new() -> Self {
    Self {
      has_custom_commands: false,
      has_custom_object_factory: false,
    }
  }
}
