use crate::error::Result;

pub struct JsonModule;

impl JsonModule {
  /// garnet相对路径:modules/GarnetJSON/JsonModule.cs:OnLoad
  pub fn on_load() -> Result<()> {
    Ok(())
  }
}
