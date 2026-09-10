use crate::resp::{parser::resp_ext::RespVecExt, resp_server_session::RespServerSession};

impl RespServerSession {
  /// libs/server/Resp/RespServerSessionOutput.cs:ProcessOutput
  pub fn process_output(&mut self, output: &[u8], target: &mut Vec<u8>) {
    target.extend_from_slice(output);
  }

  /// libs/server/Resp/RespServerSessionOutput.cs:WriteDirectLargeRespString
  pub fn write_direct_large_resp_string(&mut self, message: &[u8], target: &mut Vec<u8>) {
    target.write_resp_bulk_string(message);
  }

  /// libs/server/Resp/RespServerSessionOutput.cs:WriteInt32AsBulkString
  pub fn write_int32_as_bulk_string(&mut self, value: i32, target: &mut Vec<u8>) {
    let mut buf = itoa::Buffer::new();
    let s = buf.format(value);
    target.write_resp_bulk_string(s.as_bytes());
  }

  /// libs/server/Resp/RespServerSessionOutput.cs:WriteLargeVerbatimString
  pub fn write_large_verbatim_string(
    &mut self,
    message: &[u8],
    format: &[u8; 3],
    target: &mut Vec<u8>,
  ) {
    target.extend_from_slice(b"=");
    let total_len = message.len() + 4; // format (3) + ':' (1)
    let mut buf = itoa::Buffer::new();
    target.extend_from_slice(buf.format(total_len).as_bytes());
    target.extend_from_slice(b"\r\n");
    target.extend_from_slice(format);
    target.extend_from_slice(b":");
    target.extend_from_slice(message);
    target.extend_from_slice(b"\r\n");
  }
}
