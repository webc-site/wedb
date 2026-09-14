//! RoaringBitmap 模块装载（对标 modules/RoaringBitmap/RoaringBitmapModule.cs）

use wcustom::{CommandType, CustomCommandInfo, GarnetModule, ModuleLoadContext};

/// GarnetRoaringBitmap 模块
///
/// C# OnLoad：Initialize("GarnetRoaringBitmap", 1) →
/// RegisterType(factory) → RegisterCommand("R.SETBIT"/"R.GETBIT"/
/// "R.BITCOUNT"/"R.BITPOS")
pub struct RoaringBitmapModule;

impl RoaringBitmapModule {
  /// 对象类型名（C# RoaringBitmapFactory 的 RegisterType 承接形态）
  const TYPE_NAME: &str = "roaringbitmap";

  /// 对象命令元数据（name / arity，对标 C# RespCommandsInfo 逐条注册）
  fn command_info(name: &str, arity: i32) -> CustomCommandInfo {
    CustomCommandInfo {
      name: name.to_string(),
      arity,
      acl_categories: vec!["bitmap".to_string()],
    }
  }
}

impl GarnetModule for RoaringBitmapModule {
  fn name(&self) -> &'static str {
    "GarnetRoaringBitmap"
  }

  fn version(&self) -> u32 {
    1
  }

  /// 登记对象类型与 R.* 四条对象命令（解析/更新辅助见
  /// [`crate::roaring_bitmap_commands`]，执行体随对象域接线）
  fn on_load(&self, ctx: &mut ModuleLoadContext<'_>) {
    let _ = ctx.new_type(Self::TYPE_NAME);
    // (命令名, CommandType, arity)——对标 C# RegisterCommand 逐条实参
    const COMMANDS: [(&str, CommandType, i32); 4] = [
      ("R.SETBIT", CommandType::ReadModifyWrite, 4),
      ("R.GETBIT", CommandType::Read, 3),
      ("R.BITCOUNT", CommandType::Read, 2),
      ("R.BITPOS", CommandType::Read, -3),
    ];
    for (name, command_type, arity) in COMMANDS {
      let _ = ctx.new_command_object(
        Self::TYPE_NAME,
        name,
        command_type,
        Some(Self::command_info(name, arity)),
        None,
      );
    }
  }
}
