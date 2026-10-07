#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! 键生命周期管理命令形态与时间戳换算集成测试
//! （对应 libs/server/Resp/KeyAdminCommands.cs:NetworkEXPIRE / NetworkTTL / NetworkEXPIRETIME）

use wbase::{
  convert::{
    TICKS_PER_SECOND, expire_after_ms_to_ticks, expire_after_to_ticks,
    expire_at_milliseconds_to_ticks, expire_at_seconds_to_ticks,
  },
  time::now_ticks,
};
use wnode::resp::key_admin_commands::{ExpireCmd, ExpireTimeCmd, TtlCmd};

/// 命令端与 AOF 重放端的换算逐位一致（同输入同函数）：
/// 绝对域（EXPIREAT/PEXPIREAT）命令端换算面与 wbase 单点恒等
#[test]
fn expire_at_matches_replay_conversion() {
  for seconds in [0, 1, 1_700_000_000, 4_102_444_799, i64::MAX, -1] {
    assert_eq!(
      ExpireCmd::Expireat.expire_at_ticks(seconds),
      expire_at_seconds_to_ticks(seconds)
    );
  }
  for millis in [0, 1, 1_700_000_000_000, i64::MAX, -1] {
    assert_eq!(
      ExpireCmd::Pexpireat.expire_at_ticks(millis),
      expire_at_milliseconds_to_ticks(millis)
    );
  }
}

/// 相对域（EXPIRE/PEXPIRE）命令端与重放端共享同一饱和乘加单点；
/// 非饱和路径时钟在两次调用间推进，以 1 秒容差断言同源
#[test]
fn expire_after_matches_replay_conversion() {
  const DRIFT: i64 = TICKS_PER_SECOND;
  let now = now_ticks();
  for seconds in [0, 1, 60, 86_400] {
    let cmd_ticks = ExpireCmd::Expire.expire_at_ticks(seconds);
    assert!(
      (cmd_ticks - expire_after_to_ticks(now, seconds)).abs() <= DRIFT,
      "EXPIRE {seconds}s: {cmd_ticks} vs {}",
      expire_after_to_ticks(now, seconds)
    );
  }
  for millis in [0, 1, 500, 86_400_000] {
    let cmd_ticks = ExpireCmd::Pexpire.expire_at_ticks(millis);
    assert!(
      (cmd_ticks - expire_after_ms_to_ticks(now, millis)).abs() <= DRIFT,
      "PEXPIRE {millis}ms: {cmd_ticks} vs {}",
      expire_after_ms_to_ticks(now, millis)
    );
  }
  // 饱和边界：与 now 无关，重放端与命令端逐位一致（同钉 i64::MAX）
  assert_eq!(
    ExpireCmd::Expire.expire_at_ticks(i64::MAX),
    expire_after_to_ticks(now_ticks(), i64::MAX)
  );
  assert_eq!(
    ExpireCmd::Pexpire.expire_at_ticks(i64::MAX),
    expire_after_ms_to_ticks(now_ticks(), i64::MAX)
  );
}

/// 命令形态枚举文本与 C# command.ToString() 对齐（错误文案面）
#[test]
fn cmd_as_str() {
  assert_eq!(ExpireCmd::Expire.as_str(), "EXPIRE");
  assert_eq!(ExpireCmd::Pexpire.as_str(), "PEXPIRE");
  assert_eq!(ExpireCmd::Expireat.as_str(), "EXPIREAT");
  assert_eq!(ExpireCmd::Pexpireat.as_str(), "PEXPIREAT");
  assert_eq!(<&'static str>::from(TtlCmd::Ttl), "TTL");
  assert_eq!(<&'static str>::from(TtlCmd::Pttl), "PTTL");
  assert_eq!(
    <&'static str>::from(ExpireTimeCmd::Expiretime),
    "EXPIRETIME"
  );
  assert_eq!(
    <&'static str>::from(ExpireTimeCmd::Pexpiretime),
    "PEXPIRETIME"
  );
}

#[test]
fn expire_cmd_attributes() {
  assert!(!ExpireCmd::Expire.is_milliseconds());
  assert!(!ExpireCmd::Expire.is_timestamp());

  assert!(ExpireCmd::Pexpire.is_milliseconds());
  assert!(!ExpireCmd::Pexpire.is_timestamp());

  assert!(!ExpireCmd::Expireat.is_milliseconds());
  assert!(ExpireCmd::Expireat.is_timestamp());

  assert!(ExpireCmd::Pexpireat.is_milliseconds());
  assert!(ExpireCmd::Pexpireat.is_timestamp());
}
