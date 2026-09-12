//! Loader block：VM 初始化即装载的 Lua 沙箱引导代码
//! （对标 libs/server/Lua/LuaRunner.Loader.cs:LuaRunner）。
//!
//! C# 以 Lua 5.4 `load(source, nil, nil, env)` 装载沙箱环境；luau 的
//! `load` 不收环境参数，改经 `setfenv(func, sandbox_env)` 承接。
//! 超时在 C# 走 `debug.sethook`，luau 无 debug 库，改由宿主中断钩子
//! （`garnet_request_timeout`）承担。

use std::{str, sync::OnceLock};

use gxhash::HashSet;
use parking_lot::Mutex;

/// 生成 loader block 的缓存（对标 LoaderBlockCache）。
struct LoaderBlockCache {
  allowed_functions: HashSet<String>,
  loader_block: &'static str,
}

static LOADER_BLOCK_CACHE: Mutex<Option<LoaderBlockCache>> = Mutex::new(None);

/// Lua 代码：进 VM 即装载（对标 LoaderBlock 常量，luau 适配版）。
const LOADER_BLOCK: &str = r#"
-- disable for sandboxing purposes
import = function () end

-- unpack compatibility (Lua 5.1 style global in luau)
local unpack = table.unpack or unpack

-- common functions for handling error replies from Garnet
local chain_func = function(f1, f2)
    return function(...)
        return f1(f2, ...)
    end
end

local error_wrapper_r0 = function(rawFunc, ...)
    local err = rawFunc(...)
    if err then
        error(err, 0)
    end
end

local error_wrapper_r1 = function(rawFunc, ...)
    local r1, err = rawFunc(...)
    if err then
        error(err, 0)
    end

    return r1
end

local error_wrapper_r2 = function(rawFunc, ...)
    local r1, r2, err = rawFunc(...)
    if err then
        error(err, 0)
    end

    return r1, r2
end

local unpackTrampolineRef = garnet_unpack_trampoline
local error_wrapper_rvar_check = function(err, ...)
    if err then
        error(err, 0)
    end

    return ...
end

local error_wrapper_rvar = function(rawFunc, ...)
    -- variable numbers of returns require extra work
    -- and requires that the error token is FIRST, not last
    -- and a count of expected returns is around to handle
    -- trailing nils
    local rets = {rawFunc(...)}
    local err = rets[1]
    local count = rets[2]
    if err then
        error(err, 0)
    end

    return error_wrapper_rvar_check(unpackTrampolineRef(rets, count))
end

-- cutdown os for sandboxing purposes
local osClockRef = os.clock
os = {
    clock = osClockRef
}

-- define cjson for (optional) inclusion into sandbox_env
local cjson = {
    encode = chain_func(error_wrapper_r1, garnet_cjson_encode);
    decode = chain_func(error_wrapper_r1, garnet_cjson_decode);
}

-- define bit for (optional) inclusion into sandbox_env
local garnetBitopRef = chain_func(error_wrapper_r1, garnet_bitop)
local bit = {
    tobit = chain_func(error_wrapper_r1, garnet_bit_tobit);
    tohex = chain_func(error_wrapper_r1, garnet_bit_tohex);
    bnot = function(...) return garnetBitopRef(0, ...); end;
    bor = function(...) return garnetBitopRef(1, ...); end;
    band = function(...) return garnetBitopRef(2, ...); end;
    bxor = function(...) return garnetBitopRef(3, ...); end;
    lshift = function(...) return garnetBitopRef(4, ...); end;
    rshift = function(...) return garnetBitopRef(5, ...); end;
    arshift = function(...) return garnetBitopRef(6, ...); end;
    rol = function(...) return garnetBitopRef(7, ...); end;
    ror = function(...) return garnetBitopRef(8, ...); end;
    bswap = chain_func(error_wrapper_r1, garnet_bit_bswap);
}

-- define cmsgpack for (optional) inclusion into sandbox_env
local cmsgpack = {
    pack = chain_func(error_wrapper_r1, garnet_cmsgpack_pack);
    unpack = chain_func(error_wrapper_rvar, garnet_cmsgpack_unpack);
}

-- define struct for (optional) inclusion into sandbox_env
local struct = {
    pack = chain_func(error_wrapper_r1, garnet_struct_pack);
    unpack = chain_func(error_wrapper_rvar, garnet_struct_unpack);
    size = chain_func(error_wrapper_r1, garnet_struct_size);
}

-- define redis for (optional, but almost always) inclusion into sandbox_env
local garnetCallRef = chain_func(error_wrapper_r1, garnet_call)
local pCallRef = pcall
local redis = {
    status_reply = function(text)
        return text
    end,

    error_reply = function(text)
        return { err = 'ERR ' .. text }
    end,

    call = garnetCallRef,

    pcall = function(...)
        local success, errOrRes = pCallRef(garnetCallRef, ...)
        if success then
            return errOrRes
        end

        return { err = errOrRes }
    end,

    sha1hex = chain_func(error_wrapper_r1, garnet_sha1hex),

    LOG_DEBUG = 0,
    LOG_VERBOSE = 1,
    LOG_NOTICE = 2,
    LOG_WARNING = 3,

    log = chain_func(error_wrapper_r0, garnet_log),

    REPL_ALL = 3,
    REPL_AOF = 1,
    REPL_REPLICA = 2,
    REPL_SLAVE = 2,
    REPL_NONE = 0,

    set_repl = function(...)
        -- this is a giant footgun, straight up not implementing it
        error('ERR redis.set_repl is not supported in Garnet', 0)
    end,

    replicate_commands = function(...)
        return true
    end,

    breakpoint = function(...)
        -- this is giant and weird, not implementing
        error('ERR redis.breakpoint is not supported in Garnet', 0)
    end,

    debug = function(...)
        -- this is giant and weird, not implementing
        error('ERR redis.debug is not supported in Garnet', 0)
    end,

    acl_check_cmd = chain_func(error_wrapper_r1, garnet_acl_check_cmd),
    setresp = chain_func(error_wrapper_r0, garnet_setresp),

    REDIS_VERSION = garnet_REDIS_VERSION,
    REDIS_VERSION_NUM = garnet_REDIS_VERSION_NUM
}

-- added after Lua 5.1, removing to maintain Redis compat
string.pack = nil
string.unpack = nil
string.packsize = nil
math.maxinteger = nil
math.type = nil
math.mininteger = nil
math.tointeger = nil
math.ult = nil
table.pack = nil
table.unpack = nil
table.move = nil

-- in Lua 5.1 but not 5.4, so implemented on the host side
local loadstring = chain_func(error_wrapper_r2, garnet_loadstring)
math.atan2 = chain_func(error_wrapper_r1, garnet_atan2)
math.cosh = chain_func(error_wrapper_r1, garnet_cosh)
math.frexp = chain_func(error_wrapper_r2, garnet_frexp)
math.ldexp = chain_func(error_wrapper_r1, garnet_ldexp)
math.log10 = chain_func(error_wrapper_r1, garnet_log10)
math.pow = chain_func(error_wrapper_r1, garnet_pow)
math.sinh = chain_func(error_wrapper_r1, garnet_sinh)
math.tanh = chain_func(error_wrapper_r1, garnet_tanh)
table.maxn = chain_func(error_wrapper_r1, garnet_maxn)

local collectgarbageRef = collectgarbage
local setMetatableRef = setmetatable
local rawsetRef = rawset

-- prevent modification to metatables for readonly tables
-- Redis accomplishes this by patching Lua, we'd rather ship
-- vanilla Lua and do it in code
local setmetatable = function(table, metatable)
    if table and table.__readonly then
        error('Attempt to modify a readonly table', 0)
    end

    return setMetatableRef(table, metatable)
end

-- prevent bypassing metatables to update readonly tables
-- as above, Redis prevents this with a patch to Lua
local rawset = function(table, key, value)
    if table and table.__readonly then
        error('Attempt to modify a readonly table', 0)
    end

end

local tableInsertRef = table.insert
local tableRemoveRef = table.remove
table.insert = function(tbl, ...)
    if tbl and tbl.__readonly then
        error('Attempt to modify a readonly table', 0)
    end
    return tableInsertRef(tbl, ...)
end
table.remove = function(tbl, ...)
    if tbl and tbl.__readonly then
        error('Attempt to modify a readonly table', 0)
    end
    return tableRemoveRef(tbl, ...)
end

-- technically deprecated in 5.1, but available in Redis
-- this is only 'sort of' correct as 5.4 doesn't expose the same
-- gc primitives
local gcinfo = function()
    return collectgarbageRef('count'), 0
end

-- global object used for the sandbox environment
--
-- replacements are performed before VM initialization
-- to allow configuring available functions
sandbox_env = {
    _VERSION = _VERSION;

    KEYS = KEYS;
    ARGV = ARGV;

!!SANDBOX_ENV REPLACEMENT TARGET!!
}
-- timeout error must be raised on Lua
-- (luau has no debug hooks; the deadline is armed by the host interrupt)
function request_timeout()
    garnet_request_timeout()
end
-- no reference to outermost set of globals (_G) should survive sandboxing
sandbox_env._G = sandbox_env
-- lock down a table, recursively doing the same to all table members
local rawGetRef = rawget
local readonly_metatable = {
    __index = function(onTable, key)
        return rawGetRef(onTable, key)
    end,
    __newindex = function(onTable, key, value)
        error('Attempt to modify a readonly table', 0)
    end
}
function recursively_readonly_table(table)
    if table.__readonly then
        return table
    end

    table.__readonly = true

    for key, value in pairs(table) do
        if type(value) == 'table' and key ~= 'KEYS' and key ~= 'ARGV' then
            recursively_readonly_table(value)
        end
    end

    setMetatableRef(table, readonly_metatable)
end
-- do resets in the Lua side to minimize pinvokes
function reset_keys_and_argv(fromKey, fromArgv)
    local keyRef = sandbox_env.KEYS
    local keyCount = #keyRef
    for i = fromKey, keyCount do
        table.remove(keyRef)
    end

    local argvRef = sandbox_env.ARGV
    local argvCount = #argvRef
    for i = fromArgv, argvCount do
        table.remove(argvRef)
    end
end
-- force new 'global' environment to be readonly
recursively_readonly_table(sandbox_env)
-- responsible for sandboxing user provided code
-- (luau VM has no load global; the host compiles the chunk via garnet_load
-- and the env is bound with setfenv)
function load_sandboxed(source)
    local rawFunc, err = garnet_load(source)
    if rawFunc ~= nil then
        rawFunc = setfenv(rawFunc, sandbox_env)
    end

    return err, rawFunc
end
"#;

pub struct LuaRunnerLoader;

impl LuaRunnerLoader {
  /// 默认允许函数集（对标 DefaultAllowedFunctions）。
  pub fn default_allowed_functions() -> &'static HashSet<&'static str> {
    static DEFAULTS: OnceLock<HashSet<&'static str>> = OnceLock::new();
    DEFAULTS.get_or_init(|| {
      [
        // Built ins
        "assert",
        "collectgarbage",
        "coroutine",
        "error",
        "gcinfo",
        // Intentionally not supporting getfenv, as it's too weird to backport
        "getmetatable",
        "ipairs",
        // Intentionally not exposing load/loadstring to user scripts
        "math",
        "next",
        "pairs",
        "pcall",
        "rawequal",
        "rawget",
        // Note rawset is proxied to implement readonly tables
        "rawset",
        "select",
        // Intentionally not supporting setfenv, as it's too weird to backport
        // Note setmetatable is proxied to implement readonly tables
        "setmetatable",
        "string",
        "table",
        "tonumber",
        "tostring",
        "type",
        // Note unpack is actually table.unpack, and defined in the loader block
        "unpack",
        "xpcall",
        // Runtime libs
        "bit",
        "cjson",
        "cmsgpack",
        // Note os only contains clock due to definition in the loader block
        "os",
        // Note struct is actually implemented by Lua 5.4's string.pack/unpack/size
        "struct",
        // Interface force communicating back with Garnet
        "redis",
      ]
      .into_iter()
      .collect()
    })
  }

  /// libs/server/Lua/LuaRunner.Loader.cs:PrepareLoaderBlockBytes
  ///
  /// 按 `allowed_functions` 裁剪 sandbox_env 导出面，生成最终 loader 源码
  /// （luau 无 string.dump，缓存源码文本而非字节码）。
  pub fn prepare_loader_block_bytes(allowed_functions: &HashSet<String>) -> &'static str {
    let mut allowed = allowed_functions;
    let defaults_holder;
    if allowed.is_empty() {
      defaults_holder = Self::default_allowed_functions()
        .iter()
        .map(|s| (*s).to_string())
        .collect::<HashSet<_>>();
      allowed = &defaults_holder;
    }

    // 大多数会话导出面不变，缓存复用（C# 以引用相等判定；此处按值相等）。
    {
      let cache = LOADER_BLOCK_CACHE.lock();
      if let Some(cached) = cache.as_ref()
        && cached.allowed_functions == *allowed
      {
        return cached.loader_block;
      }
    }

    // Build the subset of a Lua table where we export all these functions
    let defaults = Self::default_allowed_functions();
    let mut whole_includes: HashSet<String> = HashSet::default();
    let mut replacement = String::new();
    for whole_ref in allowed.iter().filter(|x| !x.contains('.')) {
      if !defaults.contains(whole_ref.as_str()) {
        // Skip functions not intentionally exported
        continue;
      }

      replacement.push_str(&format!("    {whole_ref}={whole_ref};\n"));
      whole_includes.insert(whole_ref.clone());
    }

    // Partial includes (ie. os.clock) need special handling
    let mut partial: Vec<(String, String)> = allowed
      .iter()
      .filter(|x| x.contains('.'))
      .map(|x| {
        let dot = x.find('.').unwrap_or(0);
        (x[..dot].to_string(), x[dot + 1..].to_string())
      })
      .collect();
    partial.sort();
    let mut grouped_leading: Vec<String> = Vec::new();
    for (leading, _) in &partial {
      if !grouped_leading.contains(leading) {
        grouped_leading.push(leading.clone());
      }
    }
    for leading in grouped_leading {
      if whole_includes.contains(&leading) {
        // Including a subset of something included in whole doesn't affect things
        continue;
      }

      if !defaults.contains(leading.as_str()) {
        // Skip functions not intentionally exported
        continue;
      }

      replacement.push_str(&format!("    {leading}={{\n"));
      let mut trailings: Vec<String> = partial
        .iter()
        .filter(|(lead, _)| *lead == leading)
        .map(|(_, trail)| trail.clone())
        .collect();
      trailings.sort();
      trailings.dedup();
      for part in trailings {
        replacement.push_str(&format!("        {part}={leading}.{part};\n"));
      }
      replacement.push_str("    };\n");
    }

    // 返回 &'static str：以 Box::leak 固化（每导出面只生成一次，量小且进程内复用）。
    let final_loader_block: &'static str = Box::leak(
      LOADER_BLOCK
        .replace("!!SANDBOX_ENV REPLACEMENT TARGET!!", &replacement)
        .into_boxed_str(),
    );

    let mut cache = LOADER_BLOCK_CACHE.lock();
    *cache = Some(LoaderBlockCache {
      allowed_functions: allowed.clone(),
      loader_block: final_loader_block,
    });

    final_loader_block
  }

  /// libs/server/Lua/LuaRunner.Loader.cs:CompileSource
  ///
  /// 预编译脚本为字节码缓存形态。luau 不开放 string.dump，编译在装载时
  /// 进行：此处以临时 VM 校验可编译性，不可编译/装载失败时按 C# 失败
  /// 路径原样返回源码（后续装载会再次失败并报同样错误）。
  pub fn compile_source(source: &[u8]) -> Vec<u8> {
    let Ok(text) = str::from_utf8(source) else {
      return source.to_vec();
    };

    let mut state = crate::LuaState::new();
    if state.load_buffer(text.as_bytes(), "@user_script").is_err() {
      return source.to_vec();
    }

    source.to_vec()
  }
}

#[cfg(test)]
mod tests {
  use gxhash::HashSet;

  use super::LuaRunnerLoader;

  #[test]
  fn default_loader_block_exports_everything() {
    let empty = HashSet::default();
    let block = LuaRunnerLoader::prepare_loader_block_bytes(&empty);
    assert!(block.contains("sandbox_env = {"));
    assert!(block.contains("    redis=redis;"));
    // 默认导出面包含 math 与 os（整体导出，os 已在 block 内裁剪为只剩 clock）。
    assert!(block.contains("    math=math;"));
    assert!(block.contains("    os=os;"));
    assert!(!block.contains("!!SANDBOX_ENV REPLACEMENT TARGET!!"));
  }

  #[test]
  fn subset_loader_block_exports_only_allowed() {
    let allowed: HashSet<String> = ["redis", "os.clock"]
      .into_iter()
      .map(String::from)
      .collect();
    let block = LuaRunnerLoader::prepare_loader_block_bytes(&allowed);
    assert!(block.contains("    redis=redis;"));
    // os.clock 为局部导出形态。
    assert!(block.contains("    os={"));
    assert!(block.contains("clock=os.clock;"));
    assert!(!block.contains("    math=math;"));
    assert!(!block.contains("    cjson=cjson;"));
  }

  #[test]
  fn compile_source_passes_valid_and_keeps_invalid() {
    let valid = b"return 1";
    assert_eq!(LuaRunnerLoader::compile_source(valid), valid.to_vec());
    // 不可编译 → 按失败路径原样返回。
    let invalid = b"return ]]";
    assert_eq!(LuaRunnerLoader::compile_source(invalid), invalid.to_vec());
  }
}
