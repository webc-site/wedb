//! RESP 服务器会话（对标 libs/server/Resp/RespServerSession.cs:RespServerSession）
//!
//! C# 会话直接持有网络发送器 / Tsavorite 上下文 / 事务管理器；Rust 侧这些面
//! 分别由并行域（wkv 会话纪元 / transaction / cluster）承载，本结构承接会话
//! 状态本体：Id / 端点 / CreationTicks / RESP 协议版本 / clientName·lib-* /
//! useAsync / 数据库会话映射 / 订阅与事务模式 / 延迟与会话指标 / 接收缓冲
//! 解析游标 / 输出缓冲，以及 C# 的分派 · 发送 · 数据库切换方法族。
//!
//! 输出缓冲模型：C# 的 `dcurr/dend` 指针游标对应 `output: Vec<u8>` 的
//! `len/capacity`；`Send` 与 `SendAndReset` 合为 [`RespServerSession::take_output_into`]
//! 一枚冲取出面。
//!
//! 目录化拆分（对标 C# RespServerSession 分片文件）：[`core`] 结构体本体 /
//! 构造 / 切库与通用写面、[`consume`] 消费主循环、[`dispatch`] 分派器、
//! [`resume`] 慢路径续跑尾参编解码族、[`cold_context`] 冷上下文挂起面、
//! [`args`] 参数汇集面、[`parse`] 命令名与区间解析、[`pump`] 网络泵取出 /
//! 挂起 / 记账面、[`attach`] 装配面、[`auth`] 鉴权臂、[`custom`] 自定义
//! 命令、[`lua`] 脚本窗口与 no-script 位图、[`txn`] 事务面、[`metrics`]
//! 指标与延迟、[`pubsub`] 订阅会话面；本文件只做 mod 声明与重导出，
//! 对外路径不变。

mod args;
mod attach;
mod auth;
mod cold_context;
mod consume;
mod core;
mod custom;
mod dispatch;
mod lua;
mod metrics;
mod parse;
mod pubsub;
mod pump;
mod resume;
mod txn;

/// 连接保护选项引用面（wconf 单源定义；本 re-export 承接集成测试与本模块
/// 既有引用路径，外层 resp/mod.rs 不再二次转发）
///
/// libs/server/Auth/Settings/ConnectionProtectionOption.cs:ConnectionProtectionOption
pub use wconf::ConnectionProtectionOption;

pub(crate) use self::core::collect_arg_views;
pub use self::{
  attach::RespServerSessionOptions,
  core::{
    EtagResume, MsetnxResume, PROBE_PRESERVE_WATERMARK, REDIS_PROTOCOL_VERSION, RespServerSession,
    TtlLeg, TtlResume, is_command_arity_valid_checked,
  },
  custom::CustomCommandRef,
  parse::GET_COMMAND_RESP_PREFIX,
};
