//! JSON 静态命令执行面
//!
//! 对位面（garnet 真有 C# 实现者）仅 [`JsonCommands`] 的 SET / GET 两族，对标
//! modules/GarnetJSON/JsonCommands.cs 的 JsonSET / JsonGET 与
//! modules/GarnetJSON/JsonModule.cs 的 OnLoad 注册面（C# 只 RegisterCommand 此 2 条），
//! 及 CustomObjectFunctions 三钩子（NeedInitialUpdate / Updater / Reader）。
//!
//! 自 JSON.DEL 起的 19 条为 RedisJSON 兼容扩展面，garnet 无对位实现，
//! 依据 transpile 规范对扩展命令采编译期静态枚举 [`JsonCommand`]
//! 与 const 清单分发（零锁零分配完成命令匹配与分发），故本模块扩展命令臂只按 RESP
//! 语义与参数口径描述，不挂 `路径.cs:符号` 锚点。

mod array;
mod common;
mod dispatch;
mod mutate;
mod object;
mod resp_encode;
mod set_get;
mod string;

pub use dispatch::{COMMAND_INFOS, JsonCommand, JsonCommandInfo, is_command_registered};

/// JSON 命令执行面的零状态派发标记类型：各命令族以 `CustomObjectFns` 静态常量
/// 承接 need_initial_update / updater / reader / not_found / is_empty 五钩子。
pub struct JsonCommands;
