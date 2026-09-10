pub struct RespCommandKeySpecification;

impl RespCommandKeySpecification {
  /// libs/server/Resp/RespCommandKeySpecification.cs:ToRespFormat
  pub fn to_resp_format(_output: &mut Vec<u8>) {}

  /// libs/server/Resp/RespCommandKeySpecification.cs:TryGetStartIndex
  pub fn try_get_start_index(args: &[&[u8]]) -> Option<usize> {
    if args.is_empty() { None } else { Some(0) }
  }

  /// libs/server/Resp/RespCommandKeySpecification.cs:ExtractKeys
  pub fn extract_keys<'a>(args: &'a [&'a [u8]]) -> Vec<&'a [u8]> {
    if args.is_empty() {
      Vec::new()
    } else {
      vec![args[0]]
    }
  }

  /// libs/server/Resp/RespCommandKeySpecification.cs:CanConvert
  pub fn can_convert() -> bool {
    true
  }
}
