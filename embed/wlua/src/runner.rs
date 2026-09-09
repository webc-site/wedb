use mlua::{Lua, Result};

/// garnet相对路径:garnet/libs/server/Lua/LuaRunner.cs:LuaRunner
pub struct LuaRunner {
  lua: Lua,
}

impl LuaRunner {
  pub fn new() -> Result<Self> {
    let lua = Lua::new();
    Ok(Self { lua })
  }

  pub fn run_script(&self, script: &str) -> Result<()> {
    self.lua.load(script).exec()
  }
}
