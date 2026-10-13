//! wcompact 日志紧缩集成测试入口（对标 C# Tsavorite compaction 套件）
//!
//! 在 crate 层自建最小宿主 fixture（whlog + windex，不依赖 wkv），覆盖 Lookup/Scan
//! 双模式判活、四通道死亡判定、并发紧缩竞争与截断边界语义。

mod support;

mod concurrency;
mod fuzzy_bound;
// 渐进窗增长用例依赖 debug-only 注入钩 arm_probe_gap_hook，release 剔除
#[cfg(debug_assertions)]
mod grow_window;
mod lookup_mode;
mod scan_evict_stress;
mod scan_mode;
mod scan_sidecar_snapshot;
mod truncation;
