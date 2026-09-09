//! wcpr 检查点集成测试入口（对标 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Tsavorite.cs:test 检查点套件）
//!
//! 在 crate 层自建最小宿主 fixture（whlog + windex，不依赖 wkv），
//! 覆盖 token 流转、meta 完整性防线、CprStore/CprRecover 往返与并发检查点。

#[ctor::ctor(unsafe)]
fn _log_init() {
  log_init::init();
}

mod support;

mod concurrent_ckpt;
mod meta_tamper;
mod rc_tag;
mod roundtrip;
mod token_layout;
