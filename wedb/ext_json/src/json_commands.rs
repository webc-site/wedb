use crate::error::{Error, Result};

pub struct JsonCommands;

impl JsonCommands {
  /// 在 garnet 中的相对路径:modules/GarnetJSON/JsonCommands.cs:Updater
  pub fn updater() -> Result<()> {
    Err(Error::NotImplemented)
  }
}
