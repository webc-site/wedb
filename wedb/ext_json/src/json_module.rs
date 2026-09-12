use crate::error::Result;

pub struct JsonModule;

impl JsonModule {
  /// 在 garnet 中的相对路径:modules/GarnetJSON/JsonModule.cs:OnLoad
  pub fn on_load() -> Result<()> {
    Ok(())
  }
}
