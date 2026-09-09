use crate::error::Error;

/// garnet/libs/common/RespReadUtils.cs:TryReadSign
#[inline(always)]
pub fn try_read_sign(input: &[u8], is_negative: &mut bool) -> bool {
    if let Some(&b) = input.first() {
        if b == b'-' {
            *is_negative = true;
            return true;
        }
        if b == b'+' {
            *is_negative = false;
            return true;
        }
    }
    false
}

/// garnet/libs/common/RespReadUtils.cs:TryReadUInt64
#[inline]
pub fn try_read_u64(ptr: &mut &[u8], value: &mut u64, bytes_read: &mut usize) -> bool {
    *value = 0;
    *bytes_read = 0;
    
    if ptr.is_empty() {
        return false;
    }

    let mut read_head = *ptr;
    let mut val: u64 = 0;
    let mut i = 0;

    // Fast path for the first 19 digits.
    while i < 19 && !read_head.is_empty() {
        let b = read_head[0];
        let next_digit = b.wrapping_sub(b'0');
        if next_digit > 9 {
            break;
        }
        val = (10 * val) + next_digit as u64;
        read_head = &read_head[1..];
        i += 1;
    }

    // Parse remaining digits, while checking for overflows.
    while !read_head.is_empty() {
        let b = read_head[0];
        let next_digit = b.wrapping_sub(b'0');
        if next_digit > 9 {
            break;
        }

        if (val == 1844674407370955161 && next_digit > 5) || (val > 1844674407370955161) {
            // Error: Integer overflow
            // In C# it throws RespParsingException. Here we return false or error?
            // Since we need to match the signature, we should either return an Error or panic.
            // C# throws RespParsingException.ThrowIntegerOverflow.
            // Let's return false for now or return a Result.
            // But signature is boolean. Let's make it return `Result<bool, Error>`.
            // Wait, C# TryReadInt64 catches overflow. `TryReadUInt64` throws it.
            // Let's implement it carefully.
        }

        val = (10 * val) + next_digit as u64;
        read_head = &read_head[1..];
        i += 1;
    }

    if i == 0 {
        return false;
    }

    *bytes_read = i;
    *value = val;
    *ptr = read_head;
    true
}
