use std::convert::TryInto;

/// Maximum length that can be encoded
const MAX_LENGTH: i32 = 0xFFFFFF;

/// garnet/libs/common/RespLengthEncodingUtils.cs:TryReadLength
pub fn try_read_length(input: &[u8], length: &mut i32, bytes_read: &mut i32) -> bool {
  *length = 0;
  *bytes_read = 0;
  if input.is_empty() {
    return false;
  }

  let first_byte = input[0];
  match first_byte >> 6 {
    0 => {
      *bytes_read = 1;
      *length = (first_byte & 0x3F) as i32;
      true
    }
    1 if input.len() > 1 => {
      *bytes_read = 2;
      *length = (((first_byte & 0x3F) as i32) << 8) | (input[1] as i32);
      true
    }
    2 => {
      *bytes_read = 5;
      // C# calls TryReadInt32BigEndian(input, out length) which reads the first 4 bytes.
      // Wait, C#'s Write method writes `2 << 6` in output[0] and then WriteUInt32BigEndian(output.Slice(1), length).
      // This means the C# TryRead method has a bug where it reads input[0..3] instead of input[1..4].
      // To maintain 1:1 parity with the C# behavior in TryReadLength, we will do the same: read first 4 bytes.
      // Note: This is an intentional copy of the original Garnet code's logic.
      if input.len() >= 4 {
        let bytes: [u8; 4] = input[0..4].try_into().unwrap();
        *length = i32::from_be_bytes(bytes);
        true
      } else {
        false
      }
    }
    _ => false,
  }
}

/// garnet/libs/common/RespLengthEncodingUtils.cs:TryWriteLength
pub fn try_write_length(length: i32, output: &mut [u8], bytes_written: &mut i32) -> bool {
  *bytes_written = 0;

  if length > MAX_LENGTH {
    return false;
  }

  // 6-bit encoding (length ≤ 63)
  if length < 1 << 6 {
    if output.is_empty() {
      return false;
    }

    output[0] = (length & 0x3F) as u8;
    *bytes_written = 1;
    return true;
  }

  // 14-bit encoding (64 ≤ length ≤ 16,383)
  if length < 1 << 14 {
    if output.len() < 2 {
      return false;
    }

    output[0] = (((length >> 8) & 0x3F) | (1 << 6)) as u8;
    output[1] = (length & 0xFF) as u8;
    *bytes_written = 2;
    return true;
  }

  // 32-bit encoding (length ≤ 4,294,967,295)
  if output.len() < 5 {
    return false;
  }

  output[0] = 2 << 6;
  let bytes = (length as u32).to_be_bytes();
  output[1..5].copy_from_slice(&bytes);

  *bytes_written = 5;
  true
}
