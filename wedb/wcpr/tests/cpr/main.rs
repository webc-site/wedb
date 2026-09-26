//! wcpr 检查点集成测试入口（对标 libs/storage/Tsavorite/cs/test/test.recovery/CheckpointManagerTests.cs:CheckpointManagerTests 检查点套件）
//!
//! 在 crate 层自建最小宿主 fixture（whlog + windex，不依赖 wkv），
//! 覆盖 token 流转、meta 完整性防线、CprStore/CprRecover 往返与并发检查点。

mod support;

mod checkpoint_slot;
mod concurrent_ckpt;
mod freeze_cross_round;
mod fuzzy_replay;
mod growing_gate;
mod index_snapshot_hang;
mod meta_sampling_race;
mod meta_tamper;
mod multi_instance_gate;
mod rc_eviction_ckpt;
mod rc_mid_chain_ckpt;
mod rc_tag;
mod roundtrip;
mod stale_window_sampling;
mod token_layout;
mod version_floor;

mod casread_gap_doubletake;
