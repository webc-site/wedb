use std::str;

use bitcode::{Decode, Encode};
use wbase::{
  base32::{BASE32_LEN_U128, Base32Buf128, decode_u128, encode_u128},
  crc::Crc32Hasher,
};

use crate::{Error, Result};

/// Checkpoint 元数据当前格式版本号
///
/// 恢复门控规则：`format_version == FORMAT_VERSION`，非当前版本一律拒绝恢复
/// （对标 C# RecoveryInfo.cs：版本不匹配直接抛出异常；元数据无向下兼容语义，
/// 完整性封签 integrity_crc32 强制校验）
pub const FORMAT_VERSION: u32 = 4;

/// 元数据文件名前缀与扩展名常量
pub(crate) const META_PREFIX: &str = "checkpoint_";
pub(crate) const META_EXT: &str = ".meta";
pub(crate) const INDEX_PREFIX: &str = "index_";
pub(crate) const INDEX_EXT: &str = ".ckpt";
pub(crate) const TMP_EXT: &str = ".tmp";

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
pub(crate) fn parse_token(s: &str) -> Option<u128> {
  let s = s.trim();
  if s.len() == BASE32_LEN_U128 {
    decode_u128(s)
  } else {
    None
  }
}

/// 快照持久化类型
///
/// 两者在创建阶段采用同一条「封印只读 + 整库刷盘」崩溃一致路径（先封印后刷盘，
/// 杜绝在途原位写撕裂检查点）；差异体现在恢复阶段 ReadOnlyAddress 的重建语义：
#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
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
/// 此处收敛为桶数；entry_count 为 Rust 侧新增的恢复期交叉校验观测量。
/// C# 同结构的 startLogicalAddress/finalLogicalAddress 属检查点级的日志地址区间而非
/// 索引镜像的统计量，本实现相应挂在 [`CheckpointMeta::index_start_logical_address`]
/// 与 [`HlogMeta::tail_address`] 上，语义与 C# IndexRecoveryInfo 一一对应）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub struct IndexMeta {
  /// 主哈希桶数量
  pub size: usize,
  /// 已分配溢出桶数量
  pub overflow_count: u64,
  /// 记录的有效条目总数
  pub entry_count: usize,
}

/// 混合日志（HybridLog）逻辑地址状态快照元数据
///
/// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/Index/CheckpointManagement/RecoveryInfo.cs:HybridLogRecoveryInfo
/// （begin/head/flushed_until/tail 分别对标 beginAddress/headAddress/
/// flushedLogicalAddress/finalLogicalAddress；C# 另有的 version/nextVersion 由
/// 纪元机制取代、快照文件地址组由「单一截断点 + 原地刷盘」设计消除、cookie 属
/// 复制域不落地——见 manager::create 的逐项对标说明。此处「快照文件地址组」仅指
/// HybridLogCheckpointInfo 的页区间定位地址，不含 C# IndexRecoveryInfo 的模糊区
/// 窗口端点，后者由 [`CheckpointMeta::index_start_logical_address`] 与本结构
/// [`HlogMeta::tail_address`] 承载）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
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

/// 存储引擎配置元数据（用于在崩溃恢复时无缝还原配置）
///
/// 非对标结构（Rust 专有）：C# TsavoriteKV 的引擎配置由恢复方以构造参数另行提供，
/// 不随检查点元数据落盘；wedb 将其写入 meta 以支持仅凭检查点目录 + 设备文件完成
/// 无参恢复。字段逐项对应 wedb StoreConfig。
#[derive(Debug, Clone, PartialEq, Encode, Decode)]
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
  pub enable_revivification: bool,
  /// 是否启用 ReadCache 独立只读内存日志（严格对标 Garnet ReadCacheEnabled）
  pub enable_read_cache: bool,
  /// ReadCache 内存页数（必须为 2 的幂）
  pub read_cache_num_pages: usize,
  /// 基于磁盘的 RangeIndex 根目录路径（若有）
  pub range_index_dir: Option<String>,
  /// key_id 分配水位（下一待分配集合唯一 ID）
  ///
  /// 恢复时以 `next_key_id + KEY_ID_ASSIGN_MARGIN` 抬升新进程分配水位（fetch_max
  /// 单调），防止墙钟回退（NTP 步进 / VM 快照回滚）后 key_id 复用造成子键物理键
  /// 命名空间冲突。
  pub next_key_id: u64,
}

/// Checkpoint 完整持久化元数据结构
///
/// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/Index/CheckpointManagement/RecoveryInfo.cs
/// （C# 将 HybridLogRecoveryInfo 与 IndexRecoveryInfo 两份元数据经各自 token 分离提交；
/// 本结构将其统一为单 token 单文件发布——index_meta 对应 IndexRecoveryInfo，
/// hlog_meta 对应 HybridLogRecoveryInfo，index_start_logical_address 对应
/// IndexRecoveryInfo.startLogicalAddress（窗口终点即 hlog_meta.tail_address =
/// IndexRecoveryInfo.finalLogicalAddress），元数据最后落盘即提交点，与 C#
/// PERSISTENCE_CALLBACK 阶段 WriteHybridLogMetaInfo/WriteIndexMetaInfo 的语义对齐）
#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub struct CheckpointMeta {
  /// 128 位全局唯一快照版本标识符（对标 Garnet Guid）
  pub token: u128,
  /// 快照生成类型（FoldOver / Snapshot）
  pub cp_type: CheckpointType,
  /// 哈希索引元数据
  pub index_meta: IndexMeta,
  /// 索引快照模糊区窗口起点（扫描开跑时刻的日志尾地址）
  ///
  /// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/Index/CheckpointManagement/RecoveryInfo.cs:IndexRecoveryInfo
  /// （对标 `startLogicalAddress`，由
  /// libs/storage/Tsavorite/cs/src/core/Index/Checkpointing/IndexCheckpointSMTask.cs:34
  /// 在 PREPARE 入口 `hlog.GetTailAddress()` 取点；窗口终点对标同结构的
  /// `finalLogicalAddress`，即 [`HlogMeta::tail_address`]）
  ///
  /// 索引快照是单遍扫描，扫描期间前台哈希索引写不阻断（检查点屏障只覆盖 RangeIndex
  /// 树写与纪元排空窗口），故扫描漏掉的条目其记录地址必然落在
  /// `[index_start_logical_address, tail_address)` 内。恢复期据此重扫该区间并把记录
  /// 重插哈希索引（`manager::recover::run_recovery_kernel` 单趟扫描内核的
  /// 模糊区重插步骤，由宿主 `CprRecover::from_recovered` 驱动），补齐「快照未收录但日志已落盘」的
  /// 键。取点在本轮索引快照开跑之前、纪元排空屏障之后，保证该地址之下不存在
  /// 「记录已预占而索引 CAS 在途」的条目。纳入 integrity_crc32 封签覆盖。
  pub index_start_logical_address: u64,
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
  /// 检查点覆盖的 AOF 边界地址（快照提交时刻的共享日志尾地址）
  ///
  /// 在 garnet 中的相对路径:libs/server/GarnetCheckpointManager.cs:GetCookie
  /// （C# 经 GetCookie 把 CurrentSafeAofAddress 序列化进检查点 cookie 随元数据
  /// 持久化，恢复侧 RecoveredSafeAofAddress 供复制域消费；单机 AOF 重放位点
  /// 过滤不依赖它，由记录版本号 ShouldSkipRecord 承接）。None = 未启用 AOF，
  /// 或快照已提交而 AOF 边界补写（publish_checkpoint_aof_address）尚未落盘
  /// 时崩溃——Option 语义保证该窗口内单机恢复正确性不受影响。纳入
  /// integrity_crc32 封签覆盖。
  pub checkpoint_aof_address: Option<u64>,
  /// 元数据格式版本号
  pub format_version: u32,
  /// 完整性封签：除自身外全部字段的规范化 CRC32 摘要（发布前回填，恢复强制校验）
  ///
  /// 数字篡改/位翻转虽能通过反序列化（结构合法），但无法通过
  /// 逐字段摘要比对——拦截「静默错误恢复」类损坏（如 page_size 翻转为另一合法
  /// 值导致恢复后页寻址全错）。
  pub integrity_crc32: u32,
}

impl CheckpointMeta {
  /// 计算除 integrity_crc32 自身外全部字段的规范化 CRC32 摘要
  ///
  /// 逐字段按小端定长字节累积，不依赖任何序列化器的输出布局：bitcode
  /// 的编码格式跨版本变化不影响校验结果。覆盖范围：版本、token、快照类型、
  /// 三区地址、索引快照模糊区窗口起点、索引统计、创建时间戳、AOF 边界、引擎配置
  /// （含 mutable_fraction 位模式、外部路径与 key_id 分配水位）——封签之外仅
  /// 封签字段自身例外，杜绝任何字段逃逸校验。
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
    h.update_u64(self.index_start_logical_address);
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
    match self.checkpoint_aof_address {
      Some(a) => h.update_u64(a),
      None => h.update_u64(u64::MAX),
    }
    h.finalize()
  }

  /// 计算并回填完整性封签（发布落盘前的封签动作）
  #[inline]
  pub fn seal(&mut self) {
    self.integrity_crc32 = self.integrity_digest();
  }

  /// 使用 bitcode 编码为二进制字节（检查点元数据唯一持久化格式，元数据文件
  /// checkpoint_*.meta 落盘走此路径，恢复由 [`CheckpointMeta::decode`] 承接）
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
