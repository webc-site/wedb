use mlua::{Result, Value};

use crate::runner::LuaRunner;

/// 在 garnet 中的相对路径:libs/server/Lua/LuaCommands.cs:LuaCommands
pub struct LuaCommands;

impl LuaCommands {
  pub fn eval(
    runner: &LuaRunner,
    script: &str,
    keys: Vec<String>,
    args: Vec<String>,
  ) -> Result<Value> {
    runner.run_script(script, keys, args)
  }
}
