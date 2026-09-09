use mlua::Result;

use crate::runner::LuaRunner;

/// garnet相对路径:garnet/libs/server/Lua/LuaCommands.cs:LuaCommands
pub struct LuaCommands;

impl LuaCommands {
  pub fn eval(runner: &LuaRunner, script: &str) -> Result<()> {
    runner.run_script(script)
  }
}
