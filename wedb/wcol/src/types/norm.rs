//! 负下标换算 crate 级单源（Redis 语义：`i < 0 → len + i`，非负原样）
//!
//! libs/server/Objects/List/ListObjectImpl.cs 与
//! libs/server/Objects/SortedSet/SortedSetObject.cs 各自的负下标平移段同形，
//! rust 收敛为本单件，list/zset 命令臂共用，杜绝第二份换算体。

/// 负下标换算（Redis 语义 `i < 0 → len + i`，非负原样）：只平移、不钳界，返回
/// 带符号平移值——越界钳制与区间裁决由各命令臂自理（LINDEX 越界依赖负值经
/// `as usize` 回绕大数走 `get` 界外 None；LRANGE 越界臂自行 `max(0)` /
/// `min(len-1)`；LTRIM 依赖负值判 `end < 0` 走清空臂），故本单点不得内联钳制
#[inline]
pub(crate) const fn norm(i: i64, len: i64) -> i64 {
  if i < 0 { len + i } else { i }
}
