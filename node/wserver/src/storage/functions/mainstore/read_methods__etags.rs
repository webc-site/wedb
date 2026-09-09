//! etag 读取辅助（对标 libs/server/Storage/Functions/MainStore/ReadMethods.Etags.cs）

use std::str;

/// etag 值读取（值 = 整数文本约定；缺失或非法返回 None）
///
/// libs/server/Storage/Functions/MainStore/ReadMethods.Etags.cs:HandleEtagReader
pub fn handle_etag_reader(value: Option<&[u8]>) -> Option<(Vec<u8>, u64)> {
  let v = value?;
  let etag = str::from_utf8(v).ok()?.trim().parse::<u64>().ok()?;
  Some((v.to_vec(), etag))
}
