//! 集合 RESP 输入标志位（对标 libs/server/InputHeader.cs 的 RespInputFlags 面）
//!
//! C# RespInputHeader 携带判别联合（cmd/subId + type）与 flags 字节；rust 侧
//! 对象命令经 `operate(sub_id, args, arg1, arg2)` 直传分派（判别联合通道由
//!装载层信封标签与显式参数承接），flags 经 ReplayInput 通道随 AOF 条目落盘
//! （DETERMINISTIC 决定副本重放口径，EXPIRED 承载单条化绝对过期）。

use bitflags::bitflags;

bitflags! {
  #[derive(Debug, Clone, Copy, PartialEq, Eq)]
  pub struct RespInputFlags: u8 {
    // C# InputHeader.cs 位 32 SetGet 在 rust 走 RMW 机制不经旗标，位恒空不设
    const DETERMINISTIC = 64;
    const EXPIRED = 128;
  }
}
