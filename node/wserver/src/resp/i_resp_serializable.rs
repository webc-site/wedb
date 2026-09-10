//! RESP 可序列化抽象（对标 libs/server/Resp/IRespSerializable.cs）
//!
//! C# 接口把命令元数据对象经 RespWriteUtils 直写网络缓冲（dcurr/dend
//! 指针管道）；rust 命令元数据应答由 resp_commands_info 域经
//! `Vec<u8>` 缓冲统一写出。

/// RESP 可序列化抽象
pub struct IRespSerializable;

impl IRespSerializable {
  /// libs/server/Resp/IRespSerializable.cs:ToRespFormat
  ///
  /// 缺口说明：C# 指针管道直写接口；rust 应答统一经 `Vec<u8>` 承接
  /// （豁免登记见 js/check/ignore/libs/server/Resp/IRespSerializable.yml）
  pub fn to_resp_format() {}
}
