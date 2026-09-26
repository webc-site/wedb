//! 段名编解码与目录恢复扫描
//!
//! 对位 C# 本地设备的 RecoverFiles 与原生设备的段大小校验：段号编码为定长小写
//! Base32，使文件名字典序与段号数值序严格一致；恢复扫描以首段前缀空隙重建
//! `start_segment` / `end_segment`，遇中部空洞 fail-fast 显式上抛杜绝隐蔽跳空。
//!
//! 自研依据: 分段设备恢复扫描（C# 对应 test.hlog/LogRecoverReadOnlyTests.cs 装载面）

use std::{
  fs::{DirEntry, ReadDir, read_dir},
  io::{ErrorKind, Result as IoResult},
  str::from_utf8,
  sync::atomic::Ordering,
};

use wbase::base32::{decode_u64, encode_u64};

use super::SegmentedDevice;
use crate::error::{Error, Result};

/// 解析段文件名后缀（`<base>.` 之后的 13 字符定长小写 Base32）为段号
///
/// 转写规范偏离（SKILL「文件名用 base32 编码」优先于 C# 十进制 1:1）：定长编码使
/// 文件名字典序与段号数值序严格一致。长度不符、非法字符、非规范小写（大写变体）、
/// 超 u32 段号域或非 UTF-8 的后缀一律判为非段文件（跨平台一致，枚举路径跳过）
#[inline]
fn parse_segment_suffix(rest: &[u8]) -> Option<u32> {
  let s = from_utf8(rest).ok()?;
  let val = decode_u64(s)?;
  // 规范性回验：仅认本设备写出的定长小写编码，杜绝大写变体杂散文件被误认段号
  if encode_u64(val).as_str() != s {
    return None;
  }
  u32::try_from(val).ok()
}

/// 目录中段文件迭代器（零多余内存分配，流式产出段号与目录项）
pub(super) struct SegmentEntries<'a> {
  prefix: &'a [u8],
  read_dir: ReadDir,
}

impl Iterator for SegmentEntries<'_> {
  type Item = IoResult<(u32, DirEntry)>;

  fn next(&mut self) -> Option<Self::Item> {
    loop {
      let entry = match self.read_dir.next()? {
        Ok(e) => e,
        Err(e) => return Some(Err(e)),
      };
      let name = entry.file_name();
      let name_bytes = name.as_encoded_bytes();
      let Some(rest) = name_bytes.strip_prefix(self.prefix) else {
        continue;
      };
      let Some(rest) = rest.strip_prefix(b".") else {
        continue;
      };
      let Some(id) = parse_segment_suffix(rest) else {
        continue;
      };
      return Some(Ok((id, entry)));
    }
  }
}

impl SegmentedDevice {
  /// 扫描目录中全部 `<base_name>.<段号>` 命名的段文件条目（流式迭代器，零多余内存分配）
  pub(super) fn segment_entries(&self) -> IoResult<Option<SegmentEntries<'_>>> {
    let Some(file_name) = self.base_path.file_name() else {
      return Ok(None);
    };
    let read_dir = read_dir(self.parent_dir())?;
    Ok(Some(SegmentEntries {
      prefix: file_name.as_encoded_bytes(),
      read_dir,
    }))
  }

  /// 从磁盘恢复设备元数据（须在首次 I/O 前调用）
  ///
  /// 对标 libs/storage/Tsavorite/cs/src/core/Device/LocalStorageDevice.cs:RecoverFiles + Native ValidateRecoveredSegments：
  /// libs/storage/Tsavorite/cs/src/core/Device/ManagedLocalStorageDevice.cs:RecoverFiles
  /// libs/storage/Tsavorite/cs/src/core/Device/RandomAccessLocalStorageDevice.cs:RecoverFiles
  /// 1. 扫描 `<base_path>.<id>` 段文件并解析段号；
  /// 2. 校验已存在段文件大小不超过配置段大小（超过返回 SegmentSizeMismatch）；
  /// 3. 段号出现空隙处即恢复后的 start_segment（空隙前的段视为已被截断删除，
  ///    防止重启后对已删段的访问幽灵重建段文件）；
  /// 4. end_segment 恢复为最大连续段号。
  pub fn recover(&self) -> Result<()> {
    let seg_size = self.segment_size;

    let mut segids: Vec<u32> = Vec::new();
    if let Some(entries) = self.segment_entries()? {
      for item in entries {
        let (id, entry) = item?;
        // 校验已存在段文件大小（对标 Native ValidateRecoveredSegments）
        match entry.metadata() {
          Ok(m) => {
            let file_size = m.len();
            if file_size > seg_size {
              return Err(Error::SegmentSizeMismatch {
                segment: id,
                file_size,
                segment_size: seg_size,
              });
            }
            segids.push(id);
          }
          // 扫描间隙被外部删除的文件不计数，避免恢复出幽灵段
          Err(e) if e.kind() == ErrorKind::NotFound => {}
          Err(e) => return Err(Error::Io(e)),
        }
      }
    }
    segids.sort_unstable();

    // 对齐 C# RecoverFiles 状态机：prev 初始 -1，首段前缀空隙处更新 start_segment，
    // 连续处更新 end_segment；已见段后若再扫到更高段号（出现中部空洞形态），fail-fast 显式上抛
    let mut prev: i64 = -1;
    let mut recovered_start = 0u32;
    for id in segids {
      if i64::from(id) != prev + 1 {
        if prev != -1 {
          return Err(Error::SegmentGap {
            gap: (prev + 1) as u32,
          });
        }
        recovered_start = id;
      } else {
        let seg = i32::try_from(id).unwrap_or(i32::MAX);
        self.end_segment.fetch_max(seg, Ordering::SeqCst);
      }
      prev = i64::from(id);
    }
    self
      .start_segment
      .fetch_max(recovered_start, Ordering::SeqCst);
    // 物理清理水位与恢复界对齐：recovered_start 之下磁盘上已无段文件（空隙前的段
    // 视为已截断删除），不存在待补删残留，重试短路判定自恢复起即成立
    self
      .purged_segment
      .fetch_max(recovered_start, Ordering::SeqCst);
    Ok(())
  }
}

#[cfg(test)]
mod tests {
  use wbase::base32::encode_u64;

  use super::parse_segment_suffix;

  /// 定长 Base32 段名的字典序与段号数值序严格一致（目录按名排序即按段号排序）
  #[test]
  fn test_segment_name_order_preserving() {
    let samples = [
      0u32,
      1,
      2,
      9,
      10,
      11,
      99,
      100,
      999,
      1000,
      u32::MAX - 1,
      u32::MAX,
    ];
    for pair in samples.windows(2) {
      let prev = encode_u64(u64::from(pair[0]));
      let next = encode_u64(u64::from(pair[1]));
      assert!(
        prev.as_str() < next.as_str(),
        "段 {} 与 {} 的文件名字典序应随段号单调递增（{} < {}）",
        pair[0],
        pair[1],
        prev.as_str(),
        next.as_str()
      );
    }
  }

  #[test]
  fn test_parse_segment_suffix() {
    // 编解码回环：边界段号全通过
    for seg in [0u32, 1, 2, 9, 10, 12, 1000, u32::MAX] {
      assert_eq!(
        parse_segment_suffix(encode_u64(u64::from(seg)).as_bytes()),
        Some(seg)
      );
    }

    // 长度不符（历史十进制短名与杂散后缀均非段文件）
    assert_eq!(parse_segment_suffix(b""), None);
    assert_eq!(parse_segment_suffix(b"0"), None);
    assert_eq!(parse_segment_suffix(b"12"), None);
    assert_eq!(parse_segment_suffix(b"txt"), None);
    assert_eq!(parse_segment_suffix(b"000000000000"), None);
    assert_eq!(parse_segment_suffix(b"00000000000000"), None);

    // 高位字符表边界：'u'=30、'v'=31 为合法最大数字位（11 个前导 '0' 凑足定长 13）
    assert_eq!(parse_segment_suffix(b"00000000000uv"), Some(30 * 32 + 31));

    // 非法字符与符号前缀
    assert_eq!(parse_segment_suffix(b"00000000000+"), None);
    assert_eq!(parse_segment_suffix(b"00000000000.0"), None);
    assert_eq!(parse_segment_suffix(b"00000000000w"), None);

    // 大写变体：可解码但非本设备规范小写命名，判非段文件
    assert_eq!(parse_segment_suffix(b"000000000000A"), None);

    // 超 u32 段号域（首字符载荷非 0，本设备永不生成）
    assert_eq!(parse_segment_suffix(encode_u64(1 << 40).as_bytes()), None);

    // 非 UTF-8 字节序列
    assert_eq!(parse_segment_suffix(b"0000000000\xff00"), None);
  }
}
