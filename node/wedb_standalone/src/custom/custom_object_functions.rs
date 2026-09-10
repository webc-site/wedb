use core::str;

pub struct CustomObjectFunctions;

impl CustomObjectFunctions {
  /// libs/server/Custom/CustomObjectFunctions.cs:GetNextArg
  pub fn get_next_arg<'a>(args: &'a [&'a [u8]], idx: &mut usize) -> Option<&'a [u8]> {
    if *idx < args.len() {
      let res = args[*idx];
      *idx += 1;
      Some(res)
    } else {
      None
    }
  }

  /// libs/server/Custom/CustomObjectFunctions.cs:GetNextString
  pub fn get_next_string<'a>(args: &'a [&'a [u8]], idx: &mut usize) -> Option<&'a str> {
    Self::get_next_arg(args, idx).and_then(|b| str::from_utf8(b).ok())
  }

  /// libs/server/Custom/CustomObjectFunctions.cs:GetFirstArg
  pub fn get_first_arg<'a>(args: &'a [&'a [u8]]) -> Option<&'a [u8]> {
    args.first().copied()
  }

  /// libs/server/Custom/CustomObjectFunctions.cs:AbortWithWrongNumberOfArguments
  pub fn abort_with_wrong_number_of_arguments(cmd: &str) -> String {
    format!("-ERR wrong number of arguments for '{cmd}' command\r\n")
  }

  /// libs/server/Custom/CustomObjectFunctions.cs:AbortWithErrorMessage
  pub fn abort_with_error_message(msg: &str) -> String {
    format!("-ERR {msg}\r\n")
  }

  /// libs/server/Custom/CustomObjectFunctions.cs:AbortWithSyntaxError
  pub fn abort_with_syntax_error() -> String {
    "-ERR syntax error\r\n".to_string()
  }
}
