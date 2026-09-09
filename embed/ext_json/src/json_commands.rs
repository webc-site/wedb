use crate::error::Result;
use crate::error::Error;

pub struct JsonCommands;

impl JsonCommands {
  /// garnet相对路径:modules/GarnetJSON/JsonCommands.cs:Updater
  pub fn updater() -> Result<()> {
    Err(Error::NotImplemented)
  }
}
