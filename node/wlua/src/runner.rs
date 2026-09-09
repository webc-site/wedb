use mlua::{Lua, Result, Value, Variadic};

/// garnet相对路径:garnet/libs/server/Lua/LuaRunner.cs:LuaRunner
pub struct LuaRunner {
  lua: Lua,
}

impl LuaRunner {
  pub fn new() -> Result<Self> {
    let lua = Lua::new();

    // Emulate basic redis functionality in Lua
    let globals = lua.globals();
    let redis_table = lua.create_table()?;

    let call_func = lua.create_function(|lua, args: Variadic<String>| {
      if args.is_empty() {
        return Err(mlua::Error::RuntimeError(
          "Please specify at least one argument for redis.call()".to_string(),
        ));
      }
      // For now, stub the actual redis.call to return OK
      // In full implementation, this calls wserver / wresp
      Ok(Value::String(lua.create_string("OK")?))
    })?;

    let pcall_func = lua.create_function(|lua, args: Variadic<String>| {
      if args.is_empty() {
        return Err(mlua::Error::RuntimeError(
          "Please specify at least one argument for redis.pcall()".to_string(),
        ));
      }
      Ok(Value::String(lua.create_string("OK")?))
    })?;

    let sha1hex_func = lua.create_function(|_, script: String| {
      let mut hasher = sha1_smol::Sha1::new();
      hasher.update(script.as_bytes());
      Ok(hasher.digest().to_string())
    })?;

    redis_table.set("call", call_func)?;
    redis_table.set("pcall", pcall_func)?;
    redis_table.set("sha1hex", sha1hex_func)?;

    globals.set("redis", redis_table)?;

    Ok(Self { lua })
  }

  /// garnet相对路径:garnet/libs/server/Lua/LuaRunner.cs:RunScript
  pub fn run_script(&self, script: &str, keys: Vec<String>, args: Vec<String>) -> Result<Value> {
    let globals = self.lua.globals();

    let keys_table = self.lua.create_table()?;
    for (i, key) in keys.into_iter().enumerate() {
      keys_table.set(i + 1, key)?;
    }
    globals.set("KEYS", keys_table)?;

    let args_table = self.lua.create_table()?;
    for (i, arg) in args.into_iter().enumerate() {
      args_table.set(i + 1, arg)?;
    }
    globals.set("ARGV", args_table)?;

    let chunk = self.lua.load(script);
    chunk.eval()
  }

  /// garnet相对路径:garnet/libs/server/Lua/LuaRunner.cs:LoadScript
  pub fn load_script(&self, script: &str) -> Result<String> {
    let mut hasher = sha1_smol::Sha1::new();
    hasher.update(script.as_bytes());
    Ok(hasher.digest().to_string())
  }
}

impl Default for LuaRunner {
  fn default() -> Self {
    Self::new().expect("Failed to initialize LuaRunner")
  }
}
