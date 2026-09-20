//! 逻辑数据库与管理器（对标 libs/server/GarnetDatabase.cs 与
//! libs/server/Databases/{IDatabaseManager,SingleDatabaseManager,DatabaseManagerBase}.cs；
//! C# 同程序集承载 AOF 门面，rust 对应 GarnetAppendOnlyFile 直持于库容器；
//! WATCH 版本表采用节点级单 WatchVersionMap 架构，不随各逻辑库分立）

pub mod database_manager_base;
pub mod garnet_database;
pub mod i_database_manager;
pub mod single_database_manager;

pub use database_manager_base::{
  CHECKPOINT_RETAIN_GENERATIONS, CheckpointPolicy, DatabaseManagerBase, checkpoint_version,
};
pub use garnet_database::GarnetDatabase;
pub use i_database_manager::IDatabaseManager;
pub use single_database_manager::SingleDatabaseManager;
