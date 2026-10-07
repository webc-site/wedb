#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! Lua 沙箱 loader block 引导代码生成测试
//! 对标 libs/server/Lua/LuaRunner.Loader.cs:PrepareLoaderBlockBytes

use wbase::map::HashSet;
use wlua::loader::LuaRunnerLoader;

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
