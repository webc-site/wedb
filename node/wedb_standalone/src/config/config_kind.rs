use bitflags::bitflags;

bitflags! {
  /// 运行时配置槽位的表示集合（对标 C# ConfigKind，底层 u16 位标志）。
  ///
  /// 槽位是原始 8 字节单元：storage 类别决定 CONFIG SET 的解析与校验方式、
  /// 以及 CONFIG GET 的格式化方式；附加的 duration 视图标志声明该选项
  /// 可以经哪些单位访问器读取。
  ///
  /// 每个选项必须恰好声明一个 storage 类别；时长类选项额外声明其全部
  /// 可读单位，使同一选项可按毫秒 / 秒 / TimeSpan 读取而无需调用方猜测存储单位。
  #[derive(Debug, Clone, Copy, PartialEq, Eq)]
  pub struct ConfigKind: u16 {
    /// 空集（对齐 C# ConfigKind.None）。
    const NONE = 0;

    // storage 类别：每个选项恰好一个。
    const INT32 = 1 << 0;
    const INT64 = 1 << 1;
    const BOOL = 1 << 2;
    const ENUM = 1 << 3;
    const STRING = 1 << 4;

    // duration 视图：任意组合，仅对声明了时间单位的选项有效。
    const MILLISECONDS = 1 << 5;
    const SECONDS = 1 << 6;
    const MICROSECONDS = 1 << 7;
    const TIME_SPAN = 1 << 8;

    /// storage 类别掩码（对齐 C# ConfigKind.StorageMask）。
    const STORAGE_MASK = Self::INT32.bits() | Self::INT64.bits() | Self::BOOL.bits() | Self::ENUM.bits() | Self::STRING.bits();

    /// duration 视图掩码（对齐 C# ConfigKind.DurationMask）。
    const DURATION_MASK = Self::MILLISECONDS.bits() | Self::SECONDS.bits() | Self::MICROSECONDS.bits() | Self::TIME_SPAN.bits();
  }
}
