//! 向量会话执行域守卫的编译期静态断言（原 trybuild compile_fail 形态已退役：
//! trybuild 冷缓存需为独立 crate 全量重编依赖树，分钟级；静态断言编译该测试
//! crate 即验证，运行零开销，且直接断言类型事实而非比对 rustc 错误文本，
//! 不受 rustc 版本 stderr 漂移影响）
//!
//! 对标 C# `[ThreadStatic]` 禁令的 rust 语言层承接：线程槽绑定守卫
//! （[`ActiveVectorSessionGuard`] / [`OwnedActiveVectorSession`] /
//! [`ActiveDedicatedVectorSession`]）承载「本执行域会话」，物理上禁止
//! 跨线程搬运——原形态搬运即 E0277，现以 `assert_not_impl_any!` 在编译期
//! 直接锁死 `!Send`：守卫意外变 `Send` 时该测试 crate 编译失败。

use static_assertions::assert_not_impl_any;
use wdev::SegmentedDevice;
use wnode::resp::vector::vector_store_callbacks::{
  ActiveDedicatedVectorSession, ActiveVectorSessionGuard, OwnedActiveVectorSession,
};

/// 执行域守卫 `!Send` 编译期断言（宏展开为 const 断言，编译期生效）
#[test]
fn vector_session_guards_not_send() {
  assert_not_impl_any!(ActiveVectorSessionGuard<'static, SegmentedDevice>: Send);
  assert_not_impl_any!(OwnedActiveVectorSession<SegmentedDevice>: Send);
  assert_not_impl_any!(ActiveDedicatedVectorSession: Send);
}
