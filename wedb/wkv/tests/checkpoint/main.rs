//! Checkpoint 集成测试入口（对标 Garnet Tsavorite cs/test/test.recovery 套件）。

mod checkpoint_manager;
mod edge;
mod fault_defense;
mod index_checkpoint;
mod recovery;
mod recovery_single_pass;
mod reviv_low_addr_recovery;
mod reviv_window_floor;
