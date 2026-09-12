use std::str;

use bitcode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use wbase::{
  base32::{BASE32_LEN_U128, Base32Buf128, decode_u128, encode_u128},
  crc::Crc32Hasher,
};

use crate::{Error, Result};

/// CPR 检查点两阶段状态机阶段枚举
///
/// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/Index/Checkpointing/StateTransitions.cs:Phase
/// 状态流向：REST -> PREPARE -> IN_PROGRESS -> WAIT_FLUSH -> COMPLETE -> REST
#[derive(
  Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize, Encode, Decode,
)]
#[repr(u8)]
pub enum CprPhase {
  /// 空闲状态 (Phase.REST)
  #[default]
  Rest = 0,
  /// 准备阶段：捕获一致性截断点 tail，各会话准备进入新版本 (Phase.PREPARE)
  Prepare = 1,
  /// 进行中：新纪元推进，旧纪元在途操作排空 (Phase.IN_PROGRESS)
  InProgress = 2,
  /// 等待落盘：HybridLog 刷盘，RangeIndex 与 HashIndex 持久化 (Phase.WAIT_FLUSH)
  WaitFlush = 3,
  /// 持久化完成：元数据落盘，检查点提交完成 (Phase.PERSISTENCE_CALLBACK)
  Complete = 4,
}

impl CprPhase {
  /// 状态机下一阶段流转 (1:1 对标 C# FullCheckpointSM.NextState)
  #[inline]
  pub const fn next_phase(self) -> Self {
    match self {
      Self::Rest => Self::Prepare,
      Self::Prepare => Self::InProgress,
      Self::InProgress => Self::WaitFlush,
      Self::WaitFlush => Self::Complete,
      Self::Complete => Self::Rest,
    }
  }

  /// 转换为静态字符串切片 (const fn)
  #[inline]
  pub const fn as_str(self) -> &'static str {
    match self {
      Self::Rest => "REST",
      Self::Prepare => "PREPARE",
      Self::InProgress => "IN_PROGRESS",
      Self::WaitFlush => "WAIT_FLUSH",
      Self::Complete => "COMPLETE",
    }
  }

  /// 是否处于空闲状态
  #[inline]
  pub const fn is_rest(self) -> bool {
    matches!(self, Self::Rest)
  }

  /// 是否处于活动检查点状态机中
  #[inline]
  pub const fn is_active(self) -> bool {
    !self.is_rest()
  }
}

/// Checkpoint 元数据当前格式版本号
///
/// 恢复门控规则：`format_version <= FORMAT_VERSION` 的元数据均可读取
/// （0/1 视为早期未携带完整性封签字段的遗留格式，字段布局兼容）；
/// 超前版本一律拒绝恢复，防止新版字段被旧引擎按旧语义错误解读
pub const FORMAT_VERSION: u32 = 2;

/// 自该版本起元数据携带完整性封签（integrity_crc32），恢复时强制校验
pub const INTEGRITY_FROM_VERSION: u32 = 2;

/// 元数据文件名前缀与扩展名常量
pub const META_PREFIX: &str = "checkpoint_";
pub const META_EXT: &str = ".meta";
pub const INDEX_PREFIX: &str = "index_";
pub const INDEX_EXT: &str = ".ckpt";
pub const TMP_EXT: &str = ".tmp";

/// 内部统一 Base32 文件名构造器（预计算精确容量，零二次重分配）
#[inline]
fn build_base32_filename(prefix: &str, token: u128, ext: &str, suffix: &str) -> String {
  let b32 = encode_u128(token);
  let mut out = String::with_capacity(prefix.len() + b32.len() + ext.len() + suffix.len());
  out.push_str(prefix);
  out.push_str(b32.as_str());
  out.push_str(ext);
  out.push_str(suffix);
  out
}

/// 生成 checkpoint_{base32}.meta 文件名（规范统一为 Base32）
#[inline]
pub fn meta_filename(token: u128) -> String {
  build_base32_filename(META_PREFIX, token, META_EXT, "")
}

/// 生成 checkpoint_{base32}.meta.tmp 临时文件名
#[inline]
pub fn meta_tmp_filename(token: u128) -> String {
  build_base32_filename(META_PREFIX, token, META_EXT, TMP_EXT)
}

/// 生成 index_{base32}.ckpt 快照文件名
#[inline]
pub fn index_filename(token: u128) -> String {
  build_base32_filename(INDEX_PREFIX, token, INDEX_EXT, "")
}

/// 生成 index_{base32}.ckpt.tmp 临时快照文件名
#[inline]
pub fn index_tmp_filename(token: u128) -> String {
  build_base32_filename(INDEX_PREFIX, token, INDEX_EXT, TMP_EXT)
}

/// 将 128 位快照 Token 编码为 26 字符小写 Base32 栈缓冲 (RFC 4648 Base32hex)
#[inline]
pub fn token_to_base32(token: u128) -> Base32Buf128 {
  encode_u128(token)
}

/// 唯一解析 128 位 Token：仅支持 26 字符 Base32 编码 (RFC 4648 Base32hex)
#[inline]
pub fn parse_token(s: &str) -> Option<u128> {
  let s = s.trim();
  if s.len() == BASE32_LEN_U128 {
    decode_u128(s)
  } else {
    None
  }
}

/// 旧版元数据缺省的 ReadCache 页数（与 StoreConfig 缺省保持一致）
fn default_read_cache_pages() -> usize {
  8
}

/// 快照持久化类型
///
/// 两者在创建阶段采用同一条「封印只读 + 整库刷盘」崩溃一致路径（先封印后刷盘，
/// 杜绝在途原位写撕裂检查点）；差异体现在恢复阶段 ReadOnlyAddress 的重建语义：
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Encode, Decode)]
pub enum CheckpointType {
  /// 折叠模式（对标 C# Tsavorite CheckpointType.FoldOver）：
  /// 恢复时 ReadOnlyAddress 直接对齐 TailAddress，全部历史记录封印为只读，
  /// 后续更新全部走 RCU 追加（增量检查点友好，日志增长更快）
  FoldOver,
  /// 快照模式（对标 C# Tsavorite CheckpointType.Snapshot）：
  /// 恢复时按 mutable_fraction 重建内存可变区（CalculateReadOnlyAddress），
  /// 保留内存原位覆写性能（无需独立快照文件，Rust 原生简化设计）
  Snapshot,
}

/// 哈希索引快照元数据
///
/// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/Index/CheckpointManagement/RecoveryInfo.cs:IndexRecoveryInfo
/// （C# 记 table_size/num_ht_bytes/num_ofb_bytes/num_buckets，字节数可由桶数×64 推导，
/// 此处收敛为桶数；entry_count 为 Rust 侧新增的恢复期交叉校验观测量）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Encode, Decode)]
pub struct IndexMeta {
  /// 主哈希桶数量
  pub size: usize,
  /// 已分配溢出桶数量
  pub overflow_count: u64,
  /// 记录的有效条目总数
  pub entry_count: usize,
}

impl IndexMeta {
  /// 二进制元数据长度（3 个 64 位无符号整数 = 24 字节）
  pub const META_SIZE: usize = 24;

  /// 编码为 24 字节定长数组（小端编码，const fn）
  #[inline(always)]
  pub const fn to_bytes(&self) -> [u8; Self::META_SIZE] {
    let s = (self.size as u64).to_le_bytes();
    let o = self.overflow_count.to_le_bytes();
    let e = (self.entry_count as u64).to_le_bytes();
    [
      s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7], o[0], o[1], o[2], o[3], o[4], o[5], o[6],
      o[7], e[0], e[1], e[2], e[3], e[4], e[5], e[6], e[7],
    ]
  }

  /// 从 24 字节定长数组解码元数据（const fn）
  #[inline(always)]
  pub const fn from_bytes(bytes: [u8; Self::META_SIZE]) -> Self {
    let size = u64::from_le_bytes([
      bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ]) as usize;
    let overflow_count = u64::from_le_bytes([
      bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15],
    ]);
    let entry_count = u64::from_le_bytes([
      bytes[16], bytes[17], bytes[18], bytes[19], bytes[20], bytes[21], bytes[22], bytes[23],
    ]) as usize;
    Self {
      size,
      overflow_count,
      entry_count,
    }
  }

  /// 从切片解码元数据（const fn，不足 24 字节返回 None）
  #[inline(always)]
  pub const fn decode_opt(src: &[u8]) -> Option<Self> {
    if let Some(bytes) = src.first_chunk::<{ Self::META_SIZE }>() {
      Some(Self::from_bytes(*bytes))
    } else {
      None
    }
  }
}

/// 混合日志（HybridLog）逻辑地址状态快照元数据
///
/// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/Index/CheckpointManagement/RecoveryInfo.cs:HybridLogRecoveryInfo
/// （begin/head/flushed_until/tail 分别对标 beginAddress/headAddress/
/// flushedLogicalAddress/finalLogicalAddress；C# 另有的 version/nextVersion 由
/// 纪元机制取代、快照文件地址组由「单一截断点 + 原地刷盘」设计消除、cookie 属
/// 复制域不落地——见 manager.rs create_checkpoint 文档的逐项对标说明）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Encode, Decode)]
pub struct HlogMeta {
  /// 日志起始有效逻辑边界（该地址以下为废弃历史记录）
  pub begin_address: u64,
  /// 内存驻留区起始边界（该地址以下在磁盘，该地址以上在内存中）
  pub head_address: u64,
  /// 已安全落盘至存储介质的最高连续逻辑边界
  pub flushed_until_address: u64,
  /// 追加尾部逻辑地址（下一条记录写入位置）
  pub tail_address: u64,
}

impl HlogMeta {
  /// 二进制元数据长度（4 个 64 位无符号整数 = 32 字节）
  pub const META_SIZE: usize = 32;

  /// 编码为 32 字节定长数组（小端编码，const fn）
  #[inline(always)]
  pub const fn to_bytes(&self) -> [u8; Self::META_SIZE] {
    let b = self.begin_address.to_le_bytes();
    let h = self.head_address.to_le_bytes();
    let f = self.flushed_until_address.to_le_bytes();
    let t = self.tail_address.to_le_bytes();
    [
      b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7], h[0], h[1], h[2], h[3], h[4], h[5], h[6],
      h[7], f[0], f[1], f[2], f[3], f[4], f[5], f[6], f[7], t[0], t[1], t[2], t[3], t[4], t[5],
      t[6], t[7],
    ]
  }

  /// 从 32 字节定长数组解码元数据（const fn）
  #[inline(always)]
  pub const fn from_bytes(bytes: [u8; Self::META_SIZE]) -> Self {
    let begin_address = u64::from_le_bytes([
      bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ]);
    let head_address = u64::from_le_bytes([
      bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15],
    ]);
    let flushed_until_address = u64::from_le_bytes([
      bytes[16], bytes[17], bytes[18], bytes[19], bytes[20], bytes[21], bytes[22], bytes[23],
    ]);
    let tail_address = u64::from_le_bytes([
      bytes[24], bytes[25], bytes[26], bytes[27], bytes[28], bytes[29], bytes[30], bytes[31],
    ]);
    Self {
      begin_address,
      head_address,
      flushed_until_address,
      tail_address,
    }
  }

  /// 从切片解码元数据（const fn，不足 32 字节返回 None）
  #[inline(always)]
  pub const fn decode_opt(src: &[u8]) -> Option<Self> {
    if let Some(bytes) = src.first_chunk::<{ Self::META_SIZE }>() {
      Some(Self::from_bytes(*bytes))
    } else {
      None
    }
  }
}

/// 存储引擎配置元数据（用于在崩溃恢复时无缝还原配置）
///
/// 非对标结构（Rust 专有）：C# TsavoriteKV 的引擎配置由恢复方以构造参数另行提供，
/// 不随检查点元数据落盘；wedb 将其写入 meta 以支持仅凭检查点目录 + 设备文件完成
/// 无参恢复。字段逐项对应 wedb StoreConfig。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Encode, Decode)]
pub struct StoreMeta {
  /// 哈希索引主桶数
  pub index_size: usize,
  /// 混合日志单页大小
  pub page_size: usize,
  /// 环形缓冲区页数
  pub num_pages: usize,
  /// 内存中可变区所占比例
  pub mutable_fraction: f64,
  /// 最大并发客户端会话数
  pub max_sessions: usize,
  /// 是否启用空间复活回收池（严格对标 Garnet --reviv）
  #[serde(default)]
  pub enable_revivification: bool,
  /// 是否启用 ReadCache 独立只读内存日志（严格对标 Garnet ReadCacheEnabled）
  #[serde(default)]
  pub enable_read_cache: bool,
  /// ReadCache 内存页数（必须为 2 的幂）
  #[serde(default = "default_read_cache_pages")]
  pub read_cache_num_pages: usize,
  /// 基于磁盘的 RangeIndex 根目录路径（若有）
  #[serde(default)]
  pub range_index_dir: Option<String>,
  /// key_id 分配水位（下一待分配集合唯一 ID）
  ///
  /// 恢复时以 `next_key_id + KEY_ID_ASSIGN_MARGIN` 抬升新进程分配水位（fetch_max
  /// 单调），防止墙钟回退（NTP 步进 / VM 快照回滚）后 key_id 复用造成子键物理键
  /// 命名空间冲突。
  /// 兼容性说明：bitcode 为非自描述格式，新增字段后旧 checkpoint 的 bitcode 编码
  /// 不再可解码（本仓库规范不需要向上兼容）；JSON 格式经 serde 缺省 0 兜底。
  #[serde(default)]
  pub next_key_id: u64,
}

/// Checkpoint 完整持久化元数据结构
///
/// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/Index/CheckpointManagement/RecoveryInfo.cs
/// （C# 将 HybridLogRecoveryInfo 与 IndexRecoveryInfo 两份元数据经各自 token 分离提交；
/// 本结构将其统一为单 token 单文件发布——index_meta 对应 IndexRecoveryInfo，
/// hlog_meta 对应 HybridLogRecoveryInfo，元数据最后落盘即提交点，与 C#
/// PERSISTENCE_CALLBACK 阶段 WriteHybridLogMetaInfo/WriteIndexMetaInfo 的语义对齐）
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Encode, Decode)]
pub struct CheckpointMeta {
  /// 128 位全局唯一快照版本标识符（对标 Garnet Guid）
  pub token: u128,
  /// 快照生成类型（FoldOver / Snapshot）
  pub cp_type: CheckpointType,
  /// 哈希索引元数据
  pub index_meta: IndexMeta,
  /// 混合日志地址元数据
  pub hlog_meta: HlogMeta,
  /// 存储引擎核心配置元数据
  pub store_meta: StoreMeta,
  /// 快照创建时间戳（毫秒级 Unix 时间戳）
  ///
  /// 非对标字段（Rust 专有观测）：C# 检查点元数据（RecoveryInfo.cs 的
  /// HybridLogRecoveryInfo / IndexRecoveryInfo）不携带任何时间戳，仅含 guid、
  /// 版本与地址。本字段仅供运维排查检查点新旧，不参与恢复决策（恢复选点只依赖
  /// token 大小序），但纳入 integrity_crc32 封签覆盖。
  pub created_at: u64,
  /// 元数据格式版本号（缺失时反序列化缺省为 0，即遗留格式）
  #[serde(default)]
  pub format_version: u32,
  /// 完整性封签：除自身外全部字段的规范化 CRC32 摘要（v2 起发布前回填）
  ///
  /// JSON 文本的原位数字篡改/位翻转虽能通过反序列化（结构合法），但无法通过
  /// 逐字段摘要比对——拦截「静默错误恢复」类损坏（如 page_size 翻转为另一合法
  /// 值导致恢复后页寻址全错）。遗留格式（version < 2）无此字段，按旧语义放行。
  #[serde(default)]
  pub integrity_crc32: u32,
}

impl CheckpointMeta {
  /// 计算除 integrity_crc32 自身外全部字段的规范化 CRC32 摘要
  ///
  /// 逐字段按小端定长字节累积，不依赖任何序列化器的输出布局：sonic-rs/bitcode
  /// 的编码格式跨版本变化不影响校验结果。覆盖范围：版本、token、快照类型、
  /// 三区地址、索引统计、创建时间戳、引擎配置（含 mutable_fraction 位模式、
  /// 外部路径与 key_id 分配水位）——封签之外仅封签字段自身例外，杜绝任何字段
  /// 逃逸校验。
  pub fn integrity_digest(&self) -> u32 {
    let mut h = Crc32Hasher::new();
    h.update_u32(self.format_version);
    h.update(&self.token.to_le_bytes());
    h.update(&[match self.cp_type {
      CheckpointType::FoldOver => 0u8,
      CheckpointType::Snapshot => 1u8,
    }]);
    let g = &self.hlog_meta;
    h.update_u64(g.begin_address);
    h.update_u64(g.head_address);
    h.update_u64(g.flushed_until_address);
    h.update_u64(g.tail_address);
    h.update_u64(self.index_meta.size as u64);
    h.update_u64(self.index_meta.overflow_count);
    h.update_u64(self.index_meta.entry_count as u64);
    let s = &self.store_meta;
    h.update_u64(s.index_size as u64);
    h.update_u64(s.page_size as u64);
    h.update_u64(s.num_pages as u64);
    h.update_u64(s.mutable_fraction.to_bits());
    h.update_u64(s.max_sessions as u64);
    h.update(&[
      u8::from(s.enable_revivification),
      u8::from(s.enable_read_cache),
    ]);
    h.update_u64(s.read_cache_num_pages as u64);
    match &s.range_index_dir {
      Some(p) => {
        h.update_u64(p.len() as u64);
        h.update(p.as_bytes());
      }
      None => h.update_u64(u64::MAX),
    }
    h.update_u64(s.next_key_id);
    h.update_u64(self.created_at);
    h.finalize()
  }

  /// 计算并回填完整性封签（发布落盘前的封签动作）
  #[inline]
  pub fn seal(&mut self) {
    self.integrity_crc32 = self.integrity_digest();
  }

  /// 使用 bitcode 编码为二进制字节
  #[inline]
  pub fn encode(&self) -> Vec<u8> {
    bitcode::encode(self)
  }

  /// 从 bitcode 二进制字节反序列化
  #[inline]
  pub fn decode(bytes: &[u8]) -> Result<Self> {
    bitcode::decode(bytes).map_err(Error::from)
  }
}
