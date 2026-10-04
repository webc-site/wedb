use core::ops::Deref;

use wbase::simd::fast_key_eq;

use crate::{
  error::{Error, Result},
  header::{HEADER_SIZE, RecordHeader},
};

/// 记录的只读零拷贝视图
///
/// 紧凑包装底层切片借用，无任何堆内存分配与数据拷贝。头位段的公共只读访问面经
/// `Deref<Target = RecordHeader>` 直达记录头，本视图不写转发代理。
///
/// 生命周期安全（引用跨 epoch 保护边界）：当借用来自日志页缓冲时，Rust 生命周期只
/// 约束页缓冲存活，不约束页字节稳定——页槽位经内部可变性回收复用（head 滑动 + 清零
/// 覆写），脱离 epoch 保护（或页读锁）窗口后，key/value 切片内容可能被并发覆写（页
/// 内存随实例存活，绝无悬垂 UB，但读取语义失效）。C# 同风险显式规避：拉取式扫描器
/// 将记录整体拷贝至临时缓冲（SpanByteScanIterator「so we don't have a ref to log
/// data outside epoch protection」）。本类型在 whlog 的等价契约：零拷贝视图仅在
/// `HybridLog::probe_resident` / `ScanIterator` 的 epoch 守卫或页读锁窗口内消费，
/// 跨窗口持有须先经 `RecordOutput` 拷贝出页。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordRef<'a> {
  /// 记录头元数据
  pub header: RecordHeader,
  /// 键切片引用（零拷贝借用）
  pub key: &'a [u8],
  /// 值切片引用（零拷贝借用）
  pub value: &'a [u8],
}

impl<'a> RecordRef<'a> {
  /// 从字节切片直接构造只读零拷贝视图
  ///
  /// 若切片长度不足以容纳完整记录（对齐逻辑尺寸，含隐式对齐填充），返回
  /// `Error::BufferTooShort`。
  #[inline]
  pub fn from_slice(slice: &'a [u8]) -> Result<Self> {
    let header = RecordHeader::from_slice(slice)?;
    let key_len = header.key_len() as usize;
    let val_len = header.val_len() as usize;
    let total_size = header.record_size();

    if slice.len() < total_size {
      return Err(Error::BufferTooShort {
        expected: total_size,
        actual: slice.len(),
      });
    }

    let key_end = HEADER_SIZE + key_len;
    // SAFETY: 前面已校验 slice.len() >= total_size >= kv_size = HEADER_SIZE + key_len + val_len，
    // 键/值切片精确止于 KV 区段（隐式对齐填充不外露）
    let key = unsafe { slice.get_unchecked(HEADER_SIZE..key_end) };
    let value = unsafe { slice.get_unchecked(key_end..key_end + val_len) };

    Ok(Self { header, key, value })
  }

  /// 获取键切片借用
  #[inline]
  pub const fn key(&self) -> &'a [u8] {
    self.key
  }

  /// 基于 SIMD 高效比对当前记录键是否与指定目标键相同（对标 Tsavorite KeysEqual，键长不等由 fast_key_eq 极速短路）
  #[inline(always)]
  pub fn matches_key(&self, target_key: &[u8]) -> bool {
    fast_key_eq(self.key, target_key)
  }

  /// 获取值切片借用
  #[inline]
  pub const fn value(&self) -> &'a [u8] {
    self.value
  }

  /// 获取 48 位前驱版本逻辑地址（头的 `address` 字段在记录层语义即前驱，故改名承接）
  #[inline]
  pub const fn prev_address(&self) -> u64 {
    self.header.address()
  }

  /// 获取整条记录的对齐逻辑字节长度（头 + 键 + 值 + 隐式对齐填充）
  ///
  /// 对应头访问器 [RecordHeader::record_size]，记录层以 total_size 口径命名。
  #[inline]
  pub const fn total_size(&self) -> usize {
    self.header.record_size()
  }
}

/// 头位段只读访问面：单点解引用至 [RecordHeader]
///
/// 严格对标 libs/storage/Tsavorite/cs/src/core/Index/Common/RecordInfo.cs——C# 头访问器族
/// （Valid/Modified/IsClosed/IsSealed/IsInNewVersion/IsReadCache/Tombstone/SkipOnScan/
/// PreviousAddress 等）唯一定义于 RecordInfo 自身，记录视图侧直接取 recordInfo 实例访问，
/// 绝无第二层转发面。本视图据此撤除逐个手写的零参数代理，改由 Deref 承载，
/// `rec.is_tombstone()` / `rec.physical_size()` 等写法不变而定义只剩头一处。
///
/// 刻意**不实现 DerefMut**：只读视图无从改写头位段，键值长度与标志位的变更必须走
/// [crate::RecordMut] 的单字原子发布协议。
impl Deref for RecordRef<'_> {
  type Target = RecordHeader;

  #[inline(always)]
  fn deref(&self) -> &RecordHeader {
    &self.header
  }
}
