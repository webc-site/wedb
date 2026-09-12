//! 构建脚本：经 luau0-src 以 cc 编译 vendored 的官方 Luau（VM + Compiler + Ast）
//! 并输出链接声明。固定 LUAU_VERSION 由 luau0-src 版本号锁定（0.21.0+luau736）。

fn main() {
  // LUA_USE_LONGJMP：错误传播走 setjmp/longjmp（luaD_throw），
  // 保证从宿主蹦床内发起的 lua_error 只跨越无析构对象的裸指针帧。
  let mut build = luau0_src::Build::new();
  build.use_longjmp(true);
  build.build().print_cargo_metadata();
}
