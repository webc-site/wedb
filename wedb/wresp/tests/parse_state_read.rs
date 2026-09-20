//! `SessionParseState::read` 参数帧三态回归（票据 qw-net-parse-violation-single-length-header）
//!
//! 对标 C# libs/server/Resp/Parser/SessionParseState.cs:Read（:344-363）：
//! 帧头与值尾非法即 `throw RespParsingException`（上层写 `ERR Protocol Error`
//! 后断连），只有字节确实未到齐才 `return false`。rust 同口径以
//! `Err(Error)` = 违例、`Ok(false)` = 未到齐表达，四类情形不得坍缩为单一 false。
//!
//! 长度头判定全仓单点 `wresp::read::try_read_unsigned_length_header`
//!（C# libs/common/RespReadUtils.cs:321/:355 同名函数）：`-` 之外的前导符号
//!（含 `+`）落 `digitsRead == 0` 抛 UnexpectedToken（:404）。

use wresp::{Error, session_parse_state::SessionParseState};

/// 读首个参数槽（三态直证）
fn read_first(buffer: &[u8]) -> Result<bool, Error> {
  let mut state = SessionParseState::new();
  state.initialize(1);
  let mut ptr = 0usize;
  state.read(0, buffer, &mut ptr, buffer.len())
}

#[test]
fn well_formed_argument_fills_slot_and_advances() {
  let buffer = b"$3\r\nSET\r\n";
  let mut state = SessionParseState::new();
  state.initialize(1);
  let mut ptr = 0usize;
  assert!(state.read(0, buffer, &mut ptr, buffer.len()).unwrap());
  assert_eq!(state.arg_in(buffer, 0), b"SET");
  // 游标停在下一参数起点（C# ptr 越过值尾 \r\n）
  assert_eq!(ptr, buffer.len());

  // 空串参数 $0\r\n\r\n
  assert_eq!(read_first(b"$0\r\n\r\n"), Ok(true));
}

#[test]
fn leading_plus_length_is_protocol_violation() {
  // C# TryReadSignedLengthHeader 只取 '-' 为符号（RespReadUtils.cs:362），
  // `+` 落 digitsRead == 0 → ThrowUnexpectedToken（:404）
  assert_eq!(
    read_first(b"$+3\r\nabc\r\n"),
    Err(Error::UnexpectedToken(b'+'))
  );
}

#[test]
fn malformed_argument_frames_are_violations() {
  // 非 `$` sigil（C# ThrowUnexpectedToken）
  assert_eq!(read_first(b":5\r\n"), Err(Error::UnexpectedToken(b':')));
  // 长度头无数字
  assert_eq!(read_first(b"$abc\r\n"), Err(Error::UnexpectedToken(b'a')));
  // 长度头终止符不符
  assert_eq!(
    read_first(b"$3\rXabc\r\n"),
    Err(Error::UnexpectedToken(b'\r'))
  );
  // 值尾终止符不符（C# Read :359 —— 旧 rust 按 false 降级致会话挂死）
  assert_eq!(
    read_first(b"$3\r\nabcd\r\n"),
    Err(Error::UnexpectedToken(b'd'))
  );
  // 负长度：`$-1\r\n` 特例与任意负值均由 Unsigned 包装层抛错
  assert_eq!(read_first(b"$-1\r\n"), Err(Error::InvalidStringLength(-1)));
  assert_eq!(
    read_first(b"$-25\r\n"),
    Err(Error::InvalidStringLength(-25))
  );
  // RESP3 NULL 形态作参数
  assert_eq!(read_first(b"_\r\n"), Err(Error::InvalidStringLength(-1)));
  // 数值溢出携数字串（C# ThrowIntegerOverflow 的 ASCII 回显载荷）
  assert_eq!(
    read_first(b"$3000000000\r\n"),
    Err(Error::IntegerOverflow {
      digits: "3000000000".into()
    })
  );
  assert_eq!(
    read_first(format!("${}\r\n", "9".repeat(23)).as_bytes()),
    Err(Error::IntegerOverflow {
      digits: "9".repeat(19)
    })
  );
}

#[test]
fn not_yet_arrived_bytes_keep_waiting() {
  // 头不足 3 字节（C# ptr+3 > end）
  assert_eq!(read_first(b"$1"), Ok(false));
  // 头终止符仅到 \r
  assert_eq!(read_first(b"$3\r"), Ok(false));
  // 负载未达
  assert_eq!(read_first(b"$5\r\nab"), Ok(false));
  // 值尾 \r\n 仅到 \r
  assert_eq!(read_first(b"$3\r\nabc\r"), Ok(false));
  // 超 512MB 上限：C# Read :350 头消费后仅返回 false，同按未到齐处置
  assert_eq!(read_first(b"$536870913\r\n"), Ok(false));
}
