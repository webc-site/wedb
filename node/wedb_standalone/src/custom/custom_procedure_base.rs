use crate::resp::parser::resp_ext::RespVecExt;

pub struct CustomProcedureBase;

impl CustomProcedureBase {
  /// libs/server/Custom/CustomProcedureBase.cs:WriteSimpleString
  pub fn write_simple_string(output: &mut Vec<u8>, msg: &str) {
    output.write_resp_simple_string(msg);
  }

  /// libs/server/Custom/CustomProcedureBase.cs:WriteBulkStringArray
  pub fn write_bulk_string_array(output: &mut Vec<u8>, items: &[&[u8]]) {
    output.write_resp_array_len(items.len());
    for item in items {
      output.write_resp_bulk_string(item);
    }
  }

  /// libs/server/Custom/CustomProcedureBase.cs:WriteBulkString
  pub fn write_bulk_string(output: &mut Vec<u8>, val: &[u8]) {
    output.write_resp_bulk_string(val);
  }

  /// libs/server/Custom/CustomProcedureBase.cs:WriteNullBulkString
  pub fn write_null_bulk_string(output: &mut Vec<u8>) {
    output.write_resp_null();
  }

  /// libs/server/Custom/CustomProcedureBase.cs:WriteError
  pub fn write_error(output: &mut Vec<u8>, msg: &str) {
    output.write_resp_error(msg);
  }

  /// libs/server/Custom/CustomProcedureBase.cs:GetNextArg
  pub fn get_next_arg<'a>(args: &'a [&'a [u8]], idx: &mut usize) -> Option<&'a [u8]> {
    if *idx < args.len() {
      let res = args[*idx];
      *idx += 1;
      Some(res)
    } else {
      None
    }
  }

  /// libs/server/Custom/CustomProcedureBase.cs:ParseCustomRawStringCommand
  pub fn parse_custom_raw_string_command(args: &[&[u8]]) -> bool {
    !args.is_empty()
  }

  /// libs/server/Custom/CustomProcedureBase.cs:ParseCustomObjectCommand
  pub fn parse_custom_object_command(args: &[&[u8]]) -> bool {
    !args.is_empty()
  }

  /// libs/server/Custom/CustomProcedureBase.cs:ExecuteCustomRawStringCommand
  pub fn execute_custom_raw_string_command() -> bool {
    false
  }

  /// libs/server/Custom/CustomProcedureBase.cs:ExecuteCustomObjectCommand
  pub fn execute_custom_object_command() -> bool {
    false
  }
}
