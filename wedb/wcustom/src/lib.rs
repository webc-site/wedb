//! WeDB 自定义扩展系统基座 (`wcustom`)
//!
//! 对标微软 Garnet 自定义扩展系统架构（`libs/server/Custom/`）的编译期
//! 静态承接形态：
//! - 自定义对象命令执行体（`CustomObjectFns`、`CommandType`）
//! - 扩展对象静态描述单点（`CustomObjectEntry`：信封标签 + TYPE 注册名 +
//!   按名命令解析入口 + 载荷堆估算入口，C# CustomCommandManager 集中分配面
//!   与 `IHeapObject.HeapMemorySize` 对象单点提供面的编译期承接；
//!   `CustomCommandMeta` 另携 `KeyScope` 键作用域，多键读命令的分型知识
//!   入静态清单，执行面不再按命令名比串）
//!
//! 自定义事务过程机器（`CustomTransactionProcedure` 抽象基类、静态派发
//! `txn_proc`）已迁至服务层 `wnode::resp::custom_procedure`——C# 侧过程机器
//! 随 `RespServerSession` 同处 server 装配、依赖事务管理器与存储执行域，本
//! crate 只保留 `CustomObjectFunctions` 对位的纯元数据面，回到依赖图叶子。
//!
//! C# 侧 `CustomCommandManager` / `RegisterApi` 动态注册管理层按转写规范
//! 删除（js/check/ignore/server.yml 已声明），扩展命令由各扩展 crate
//! （如 wext_roaring）以静态枚举 / const 清单承接。

mod custom_object_fns;
mod object_desc;

pub use custom_object_fns::{
  CommandType, CustomIsEmptyFn, CustomNeedInitialFn, CustomNotFoundFn, CustomObjectFns,
  CustomReaderFn, CustomUpdaterFn, RespVersion,
};
pub use object_desc::{CustomArgs, CustomCommandMeta, CustomObjectEntry, KeyScope};
