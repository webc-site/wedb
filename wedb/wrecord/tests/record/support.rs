use aok::Result;
use wrecord::{HEADER_SIZE, encode_to_slice, record_size};

/// 金丝雀防越界标记字节
pub const CANARY_BYTE: u8 = 0xFE;

/// 构造带有指定尾部金丝雀字节的记录缓冲区
///
/// 单次分配预填充金丝雀字节，并通过 `encode_to_slice` 零多余堆拷贝写入记录。
pub fn make_record_with_canary(
  prev_addr: u64,
  key: &[u8],
  val: &[u8],
  is_tombstone: bool,
  trailing_canary_len: usize,
) -> Result<(Vec<u8>, usize)> {
  let rec_len = record_size(key.len(), val.len());
  let mut buf = vec![CANARY_BYTE; rec_len + trailing_canary_len];
  encode_to_slice(&mut buf[..rec_len], prev_addr, key, val, is_tombstone)?;
  Ok((buf, rec_len))
}

/// 断言缓冲区中指定保护长度之后的所有金丝雀字节未被破坏
pub fn assert_canary_intact(buffer: &[u8], protected_len: usize) {
  for (idx, &byte) in buffer[protected_len..].iter().enumerate() {
    let offset = protected_len + idx;
    assert_eq!(
      byte, CANARY_BYTE,
      "金丝雀内存被非法破坏！破坏偏移: {offset}"
    );
  }
}

/// 批量构造多条连续记录的日志页缓冲
///
/// 通过预估容量与 `encode_to_slice` 直接写入页切片，消除每个记录项的临时 `Vec` 堆分配。
pub fn make_log_page<'a, I>(records: I) -> Result<Vec<u8>>
where
  I: IntoIterator<Item = (u64, &'a [u8], &'a [u8], bool)>,
{
  let iter = records.into_iter();
  let (lower, _) = iter.size_hint();
  let mut page = Vec::with_capacity(lower * HEADER_SIZE);
  for (addr, k, v, is_tomb) in iter {
    let rec_len = record_size(k.len(), v.len());
    let start = page.len();
    page.resize(start + rec_len, 0);
    encode_to_slice(&mut page[start..], addr, k, v, is_tomb)?;
  }
  Ok(page)
}
