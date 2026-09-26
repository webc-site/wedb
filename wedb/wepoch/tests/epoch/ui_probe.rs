//! `Participant: !Sync` 编译期静态断言（原 trybuild compile_fail 形态已退役：
//! trybuild 冷缓存需为独立 crate 全量重编依赖树，分钟级；且比对 rustc 错误
//! 文本，随工具链 stderr 漂移脆断——`PhantomData` 的诊断行即因此失配。静态
//! 断言编译本测试 crate 即验证，运行零开销，直接断言类型事实而非错误文本）
//!
//! 对标 C# LightEpoch `[ThreadStatic]` Metadata.Entries 的语言层禁令
//! （libs/storage/Tsavorite/cs/src/core/Epochs/LightEpoch.cs:52-85）：槽位索引
//! 线程私有，跨线程共享进出在同一时刻不可能发生。
//!
//! rust 侧曾以「使用纪律」约束该形态（本模块的前身是 8 线程 × 300 迭代共享
//! `Arc<Participant>` 风暴的运行时保守护栏用例，见 `shared_slot.rs` 模块头）。
//! 上游共享会话已按执行域私有化后，`Participant` 以 `PhantomData<Cell<()>>`
//! 收紧为 `!Sync`，该误用在编译期即被拒绝——原运行时风暴用例遂降格为本
//! 静态断言：任何试图把 `&Participant` 送入 `Sync` 上下文的写法编译不过。

use static_assertions::assert_not_impl_any;
use wepoch::Participant;

/// `Participant: !Sync` 编译期断言（宏展开为 const 断言，编译期生效；
/// `Participant` 意外变 `Sync` 时本测试 crate 编译失败）
#[test]
fn participant_not_sync() {
  assert_not_impl_any!(Participant: Sync);
}
