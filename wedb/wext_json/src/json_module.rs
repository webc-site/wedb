//! GarnetJSON 模块装载（对标 modules/GarnetJSON/JsonModule.cs:JsonModule）

use wcustom::{CommandType, CustomCommandInfo, GarnetModule, ModuleLoadContext};

/// GarnetJSON 模块
///
/// C# OnLoad：Initialize("GarnetJSON", 1) → RegisterType(jsonFactory) →
/// RegisterCommand("JSON.SET" / "JSON.GET")
pub struct JsonModule;

impl JsonModule {
  /// 对象类型名（C# GarnetJsonObjectFactory 的 RegisterType 承接形态）
  const TYPE_NAME: &str = "json";

  /// JSON.SET 元数据（C# RegisterCommand 默认 ReadModifyWrite）
  fn set_info() -> CustomCommandInfo {
    CustomCommandInfo {
      name: "JSON.SET".to_string(),
      arity: 3,
      acl_categories: vec!["write".to_string(), "json".to_string()],
    }
  }

  /// JSON.GET 元数据（C# RegisterCommand(..., CommandType.Read)）
  fn get_info() -> CustomCommandInfo {
    CustomCommandInfo {
      name: "JSON.GET".to_string(),
      arity: 2,
      acl_categories: vec!["read".to_string(), "json".to_string()],
    }
  }
}

impl GarnetModule for JsonModule {
  fn name(&self) -> &'static str {
    "GarnetJSON"
  }

  fn version(&self) -> u32 {
    1
  }

  /// 登记对象类型与 JSON.SET / JSON.GET 对象命令（路径见
  /// [`crate::json_commands`]，执行体随对象域接线）
  fn on_load(&self, ctx: &mut ModuleLoadContext<'_>) {
    let _ = ctx.new_type(Self::TYPE_NAME);
    let _ = ctx.new_command_object(
      Self::TYPE_NAME,
      "JSON.SET",
      CommandType::ReadModifyWrite,
      Some(Self::set_info()),
      None,
    );
    let _ = ctx.new_command_object(
      Self::TYPE_NAME,
      "JSON.GET",
      CommandType::Read,
      Some(Self::get_info()),
      None,
    );
  }
}
