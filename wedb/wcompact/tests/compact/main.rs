//! wcompact 日志紧缩集成测试入口（对标 C# Tsavorite compaction 套件）
//!
//! 在 crate 层自建最小宿主 fixture（whlog + windex，不依赖 wkv），覆盖 Lookup/Scan
//! 双模式判活、四通道死亡判定、并发紧缩竞争与截断边界语义。

#[ctor::ctor(unsafe)]
fn _log_init() {
  log_init::init();
}

mod support;

mod concurrency;
mod lookup_mode;
mod scan_mode;
mod truncation;
