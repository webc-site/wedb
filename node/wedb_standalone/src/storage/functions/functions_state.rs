//! 会话函数状态（对标 libs/server/Storage/Functions/FunctionsState.cs）
//!
//! C# 侧 FunctionsState 聚合 CustomCommandManager / CustomObjectFactory /
//! GarnetObjectSerializer 等会话级依赖；custom 命令域为并行转写域，本域以

/// 会话函数状态容器
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

  /// 获取自定义命令函数集合
  ///
  /// 缺口说明：依赖 custom 域（libs/server/Custom/CustomCommandManager.cs）
  /// 转写完成后的句柄接线；未挂载时返回 None。
  ///
  /// libs/server/Storage/Functions/FunctionsState.cs:GetCustomCommandFunctions
  pub fn get_custom_command_functions(&self) -> Option<()> {
    self.has_custom_commands.then_some(())
  }

  /// 获取自定义对象工厂
  ///
  /// 缺口说明：依赖 custom 域（libs/server/Custom/CustomObjectFactory.cs）
  /// 转写完成后的句柄接线；未挂载时返回 None。
  ///
  /// libs/server/Storage/Functions/FunctionsState.cs:GetCustomObjectFactory
  pub fn get_custom_object_factory(&self) -> Option<()> {
    self.has_custom_object_factory.then_some(())
  }

  /// 获取自定义对象子命令函数集合
  ///
  /// 缺口说明：同上，依赖 custom 域句柄接线；未挂载时返回 None。
  ///
  /// libs/server/Storage/Functions/FunctionsState.cs:GetCustomObjectSubCommandFunctions
  pub fn get_custom_object_sub_command_functions(&self) -> Option<()> {
    None
  }

  /// 拷贝默认 RESP 命令表
  ///
  /// 缺口说明：C# 侧把默认 RespCommand 元数据表拷入 FunctionsState 供自定义
  /// 命令引用；wserver 命令元数据由 resp 域静态表承担，本域返回空表。
  ///
  /// libs/server/Storage/Functions/FunctionsState.cs:CopyDefaultResp
  pub fn copy_default_resp(&self) -> Vec<u8> {
    Vec::new()
  }
}

impl Default for FunctionsState {
  fn default() -> Self {
    Self::new()
  }
}
