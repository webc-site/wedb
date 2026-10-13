//! 跨引擎对比评测：自家 `hash` / `bftree` 列与 `fjall` / `rocksdb` / `sqlite` 列同表对比。
//!
//! 表格口径、18 段 workload 与呈现全部来自 `wedb-bench`（redb-bench 同构移植）。
//! 本 crate 只承担第三方引擎适配，把带原生构建依赖的列（rocksdb）关在自己的
//! feature 里，CI 按列拆 job 时不会拖慢其余列。
//!
//! 与 redb 的分工一致：`wedb-bench` 是被测方 + harness，`wedb-bench-compare`
//! 是对照方；本 crate 默认编入全部五列，便于本地一次性出全表。

pub mod engines;

pub use engines::compare_specs;
