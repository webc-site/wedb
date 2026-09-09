use mlua::{Result, Value};
use crate::runner::LuaRunner;

/// garnet相对路径:garnet/libs/server/Lua/LuaCommands.cs:LuaCommands
pub struct LuaCommands;

impl LuaCommands {
    pub fn eval(runner: &LuaRunner, script: &str, keys: Vec<String>, args: Vec<String>) -> Result<Value> {
        runner.run_script(script, keys, args)
    }
}
