//! commit 元数据帧（对标 libs/storage/Tsavorite/cs/src/core/TsavoriteLog/
//! TsavoriteLog.cs:TryEnqueueCommitRecord 与 CommitInfo.cs:CommitInfo）
//!
//! 提交元数据以普通 WAL 记录承载：定长 24B 负载 = MAGIC(8B) || begin(8B LE) ||
//! cookie(8B LE)，帧完整性由记录头 CRC 统一校验。committed until 不入帧——
//! 即帧起始地址 + 全帧长（对标 C# `info.UntilAddress = logicalAddress +
//! allocatedLength` 覆盖 commit 记录自身）。恢复扫描至最后 commit 帧收敛提交
//! 上界，帧之后的记录视为未提交。
//!
//! 刻意差异：C# 双写 commit 元数据（in-log commit record +
//! logCommitManager 独立元数据文件），本实现仅保留 in-log 帧单写——帧与
//! 数据记录同一刷盘批次原子持久（帧随批尾写入、一次 flush 落盘），无独立
//! 元数据文件即可恢复精确提交边界。
//!
//! 自研依据: WAL 提交推进（C# 对应 TsavoriteLog commit 语义 libs/storage/Tsavorite/cs/test/test.hlog/LogFastCommitTests.cs）

use super::header::RECORD_HEADER_LEN;

/// 无提交 cookie 哨兵
pub const NO_COOKIE: i64 = i64::MIN;

/// commit 帧负载魔数（LE 首字节 0xFF 不在 AofEntryType 判别值域内，
/// AOF 条目负载不可能命中，杜绝语义误判）
const COMMIT_FRAME_MAGIC: u64 = 0x4D4D_4954_434F_4DFF;

/// commit 帧负载全长（MAGIC + begin + cookie，定长 24B）
pub const COMMIT_FRAME_PAYLOAD_LEN: usize = 24;

/// commit 帧全帧长（8B 记录头 + 24B 负载）；已提交上界 = 帧起始地址 + 本值
pub const COMMIT_FRAME_TOTAL_LEN: u64 = (RECORD_HEADER_LEN + COMMIT_FRAME_PAYLOAD_LEN) as u64;

/// 已持久化提交元数据（对标 TsavoriteLogRecoveryInfo 的
/// BeginAddress/Cookie 子集；UntilAddress 由帧位置承载不重复编码）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitMeta {
  /// 提交记录写出的 begin 快照
  pub begin: u64,
  /// 提交 cookie（[`NO_COOKIE`] = 无序列号）
  pub cookie: i64,
}

/// 编码 commit 帧负载（定长 24B）
#[inline]
pub const fn encode_payload(meta: CommitMeta) -> [u8; COMMIT_FRAME_PAYLOAD_LEN] {
  let m = COMMIT_FRAME_MAGIC.to_le_bytes();
  let b = meta.begin.to_le_bytes();
  let c = meta.cookie.to_le_bytes();
  [
    m[0], m[1], m[2], m[3], m[4], m[5], m[6], m[7], b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
    c[0], c[1], c[2], c[3], c[4], c[5], c[6], c[7],
  ]
}

/// 解码 commit 帧负载（长度或魔数不符即非 commit 帧，返回 None）
///
/// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/TsavoriteLog/
/// TsavoriteLogRecoveryInfo.cs:Initialize（副本回放链经本口解出主端帧的
/// begin/cookie，对标 C# 反序列化 TsavoriteLogRecoveryInfo）
#[inline]
pub const fn decode_payload(payload: &[u8]) -> Option<CommitMeta> {
  let Some(chunk) = payload.first_chunk::<COMMIT_FRAME_PAYLOAD_LEN>() else {
    return None;
  };
  let magic = u64::from_le_bytes([
    chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6], chunk[7],
  ]);
  if magic != COMMIT_FRAME_MAGIC {
    return None;
  }
  let begin = u64::from_le_bytes([
    chunk[8], chunk[9], chunk[10], chunk[11], chunk[12], chunk[13], chunk[14], chunk[15],
  ]);
  let cookie = i64::from_le_bytes([
    chunk[16], chunk[17], chunk[18], chunk[19], chunk[20], chunk[21], chunk[22], chunk[23],
  ]);
  Some(CommitMeta { begin, cookie })
}

/// 判定负载是否为 commit 帧（扫描面过滤与恢复收敛的单点判据，长度与魔数
/// 判定本体单点收敛于 [`decode_payload`]，本函数仅为其布尔视图）
#[inline]
pub fn is_commit_frame(payload: &[u8]) -> bool {
  decode_payload(payload).is_some()
}
