/// 存储 API 调用返回状态（libs/server/API/GarnetStatus.cs:GarnetStatus）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum GarnetStatus {
  /// 成功
  Ok = 0,
  /// 未找到
  NotFound = 1,
  /// 已迁移（集群 MOVED 重定向）
  Moved = 2,
  /// 对持有错误类型值的键执行操作
  WrongType = 3,
}

impl GarnetStatus {
  /// 是否成功
  #[inline]
  pub const fn is_ok(self) -> bool {
    matches!(self, Self::Ok)
  }
}
