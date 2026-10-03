//! pub/sub 通道 namespace 隔离键（wedb 自有架构，C# 无对位）
//!
//! C# Garnet 无 namespace 概念，[`crate::subscribe_broker`] 与 [`crate::session_commands`]
//! 本就裸 channel 收发；wedb 多租户架构（SKILL：认证 `<ns>#用户名`、物理键 `[NsVarint]`
//! 刚性隔离）要求消息域同口径隔离。本模块在会话侧把 ns 折叠进通道键，broker 表结构不改，
//! 隔离纯粹经前缀键达成。
//!
//! 编解码单源已收敛 [`wbase::ns_prefix`]（阻塞族经纪观察域同源复用，二进制
//! `[NsVarint]` 弃用缘由见彼处模块文档——glob 字面安全与段无歧义）；本模块仅保留
//! 通道域的转发别名与 glob 交互语义测试。

pub use wbase::ns_prefix::NsPrefix as ChannelNsPrefix;
