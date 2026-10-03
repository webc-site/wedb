#[cfg(test)]
mod tests {
  use wresp::{
    check_args::{
      check_arg_count, check_args_len, parse_db_index_arg, parse_i32_arg, parse_i64_arg,
      unpack_args, unpack_args_rest,
    },
    cmd_strings::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER,
  };

  #[test]
  fn test_check_arg_count_macro() {
    let mut out = Vec::new();

    fn run_exact(args: &[&[u8]], out: &mut Vec<u8>) -> Result<bool, ()> {
      check_arg_count!(args, 2, out, "CMD");
      Ok(false)
    }

    let a: &[&[u8]] = &[b"k", b"v"];
    assert_eq!(run_exact(a, &mut out), Ok(false));

    let b: &[&[u8]] = &[b"k"];
    assert_eq!(run_exact(b, &mut out), Ok(true));
    assert!(out.starts_with(b"-ERR wrong number of arguments"));

    out.clear();
    fn run_min(args: &[&[u8]], out: &mut Vec<u8>) -> Result<bool, ()> {
      check_arg_count!(args, 2.., out, "CMD");
      Ok(false)
    }
    assert_eq!(run_min(b, &mut out), Ok(true));
    assert_eq!(run_min(a, &mut out), Ok(false));

    out.clear();
    fn run_range(args: &[&[u8]], out: &mut Vec<u8>) -> Result<bool, ()> {
      check_arg_count!(args, 1..=2, out, "AUTH");
      Ok(false)
    }
    assert_eq!(run_range(a, &mut out), Ok(false));
    let c: &[&[u8]] = &[b"1", b"2", b"3"];
    assert_eq!(run_range(c, &mut out), Ok(true));

    out.clear();
    fn run_custom_ret(args: &[&[u8]], out: &mut Vec<u8>) -> bool {
      check_arg_count!(args, 1.., out, "CMD", return false);
      true
    }
    let empty: &[&[u8]] = &[];
    assert!(!run_custom_ret(empty, &mut out));
    assert!(run_custom_ret(a, &mut out));
  }

  #[test]
  fn test_unpack_args() {
    let mut out = Vec::new();

    fn run_unpack(args: &[&[u8]], out: &mut Vec<u8>) -> Result<(&'static str, usize), ()> {
      let Some([k, v]) = unpack_args(args, out, "SET") else {
        return Err(());
      };
      assert_eq!(k, b"key");
      assert_eq!(v, b"val");
      Ok(("ok", 2))
    }

    let valid: &[&[u8]] = &[b"key", b"val"];
    assert_eq!(run_unpack(valid, &mut out), Ok(("ok", 2)));

    let invalid: &[&[u8]] = &[b"key"];
    assert_eq!(run_unpack(invalid, &mut out), Err(()));
    assert!(out.starts_with(b"-ERR wrong number of arguments"));

    out.clear();
    fn run_unpack_rest(args: &[&[u8]], out: &mut Vec<u8>) -> Result<(&'static str, usize), ()> {
      let Some(([k], rest)) = unpack_args_rest(args, out, "MGET") else {
        return Err(());
      };
      assert_eq!(k, b"key");
      Ok(("ok", rest.len()))
    }

    let multi: &[&[u8]] = &[b"key", b"v1", b"v2"];
    assert_eq!(run_unpack_rest(multi, &mut out), Ok(("ok", 2)));
    assert_eq!(run_unpack_rest(invalid, &mut out), Ok(("ok", 0)));
    assert_eq!(run_unpack_rest(&[], &mut out), Err(()));
  }

  #[test]
  fn test_check_args_len_and_parse_int() {
    let mut out = Vec::new();
    let args: &[&[u8]] = &[b"123", b"-456"];
    assert!(check_args_len(args, 2, &mut out, "TEST"));
    assert!(check_args_len(args, 1.., &mut out, "TEST"));
    assert!(!check_args_len(args, 3, &mut out, "TEST"));
    assert!(out.starts_with(b"-ERR wrong number of arguments"));

    out.clear();
    assert_eq!(parse_i64_arg(b"123", &mut out), Some(123));
    assert_eq!(parse_i32_arg(b"-456", &mut out), Some(-456));
    assert_eq!(parse_i64_arg(b"not_int", &mut out), None);
    assert!(out.starts_with(b"-ERR value is not an integer"));

    out.clear();
    assert_eq!(
      parse_db_index_arg(b"15", RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, &mut out),
      Some(15)
    );
    assert_eq!(
      parse_db_index_arg(b"-1", RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, &mut out),
      None
    );
    assert!(out.starts_with(b"-ERR DB index is out of range"));
  }
}
