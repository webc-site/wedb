//! AofEntryType 落盘判别值黄金快照。
//!
//! `AofEntryType` 的判别值以裸 `u8` 原样落在 16 字节 AOF 头的 `op_type` 位（见
//! `waof/src/aof/header/basic.rs:AofHeader.op_type`），随 AOF 落盘、随副本流上线、随 RangeIndex
//! 迁移分块进对端日志：改一个数即改一条已写记录、一条在途副本记录的语义，回放时 `TryFrom<u8>`
//! 反解即错位或撞不可解释字节。本文件把这些值钉成期望常量：判别值只允许追加到登记块尾之后，
//! 不允许改号、不允许填洞、不允许在已登记块内长成员。
//!
//! 对位 C# 事实源：`libs/server/AOF/AofEntryType.cs:AofEntryType`（黄金值硬编在本文件、自身即
//! 事实源：不读 garnet 目录——该目录被 gitignore，CI 工作树里不存在）。与 C# 的已知差分按名登记
//! 在 `aof_entry_type_registered_diffs_are_as_documented`，即固化差分清单而非假装零差分。
//!
//! 自研依据: AOF 条目类型枚举值稳定性（对标 C# PersistedEnumStabilityTests.cs 的枚举持久化稳定面）

use waof::AofEntryType;

/// 判别值反查（洞位返回 `None`）；对位 C# `(AofEntryType)b` 的可行域。
fn entry_at(value: u8) -> Option<AofEntryType> {
  AofEntryType::try_from(value).ok()
}

/// 登记判别空间的最大值（`RangeIndexStreamChunk = 0x80`）：高于此值只允许追加、且必须同步本文件。
const MAX_REGISTERED_VALUE: u8 = 0x80;
/// 判别值成员总数（store 3 + object 3 + txn 3 + checkpoint 2 + 存储过程 1 + flush 3 + 统一 4 + RI 1）。
const GOLDEN_MEMBER_COUNT: u8 = 20;

/// 按 nibble 分块的高位块（低位为块内序号），每块的 `[lo, hi]` 闭区间与黄金成员数。块界即持久契约，
/// 块内允许重排、块序与块界不允许动。
const BLOCKS: &[(u8, u8, u8)] = &[
  // (块首, 块尾, 黄金成员数)
  (0x00, 0x02, 3), // store：Upsert/RMW/Delete
  (0x10, 0x12, 3), // 对象存储：Upsert/RMW/Delete
  (0x20, 0x22, 3), // 事务：Start/Commit/Abort
  (0x30, 0x32, 2), // 统一 checkpoint：Start(0x30)/End(0x32)，0x31 是 C# 同款空洞
  (0x50, 0x50, 1), // 存储过程
  (0x60, 0x62, 3), // flush：All/Db + rust 追加的 Ns（见差分登记）
  (0x70, 0x73, 4), // 统一存储：StringUpsert/ObjectUpsert/RMW/Delete
  (0x80, 0x80, 1), // RangeIndex 迁移分块
];

/// 全体判别值黄金表（C# AofEntryType 逐值对位，减去未转写的流式 checkpoint 块 0x40..=0x43、
/// 加 rust 独有的 FlushNs 0x62，共 20 条；差分逐条见 `aof_entry_type_registered_diffs_are_as_documented`）。
const GOLDEN: &[(AofEntryType, u8)] = &[
  (AofEntryType::StoreUpsert, 0x00),
  (AofEntryType::StoreRMW, 0x01),
  (AofEntryType::StoreDelete, 0x02),
  (AofEntryType::ObjectStoreUpsert, 0x10),
  (AofEntryType::ObjectStoreRMW, 0x11),
  (AofEntryType::ObjectStoreDelete, 0x12),
  (AofEntryType::TxnStart, 0x20),
  (AofEntryType::TxnCommit, 0x21),
  (AofEntryType::TxnAbort, 0x22),
  (AofEntryType::CheckpointStartCommit, 0x30),
  (AofEntryType::CheckpointEndCommit, 0x32),
  (AofEntryType::StoredProcedure, 0x50),
  (AofEntryType::FlushAll, 0x60),
  (AofEntryType::FlushDb, 0x61),
  (AofEntryType::FlushNs, 0x62),
  (AofEntryType::UnifiedStoreStringUpsert, 0x70),
  (AofEntryType::UnifiedStoreObjectUpsert, 0x71),
  (AofEntryType::UnifiedStoreRMW, 0x72),
  (AofEntryType::UnifiedStoreDelete, 0x73),
  (AofEntryType::RangeIndexStreamChunk, 0x80),
];

/// 全体判别值逐值锁定 + `u8` 往返反查同值 + 首尾锚：改号、插队、删成员、反解不同值都必在此红。
#[test]
fn aof_entry_type_values_are_stable() {
  for (ty, expected) in GOLDEN {
    assert_eq!(
      *ty as u8, *expected,
      "落盘判别值漂移：{ty:?} 应为 {expected:#04x}"
    );
    // 三条正向路径（as_u8 / From<u8> / 原生 cast）必须与黄金值一致。
    assert_eq!(ty.as_u8(), *expected, "{ty:?} 的 as_u8 与黄金值不符");
    assert_eq!(
      u8::from(*ty),
      *expected,
      "{ty:?} 的 From<AofEntryType> for u8 与黄金值不符"
    );
    // 反查同值：判别值反解必须落回同一成员（往返闭合）。
    assert_eq!(
      entry_at(*expected),
      Some(*ty),
      "判别值 {expected:#04x} 反解不是 {ty:?}（值被占用或改号）"
    );
  }
  assert_eq!(
    GOLDEN.len(),
    GOLDEN_MEMBER_COUNT as usize,
    "黄金表条目数与登记成员总数不符"
  );

  // 空间首尾锚：StoreUpsert 锚在整个判别空间起点 0x00，RangeIndexStreamChunk 锚在登记块尾 0x80。
  assert_eq!(
    AofEntryType::StoreUpsert as u8,
    0x00,
    "空间首成员 STOREUPSERT 必须锚在 0x00"
  );
  assert_eq!(
    AofEntryType::RangeIndexStreamChunk as u8,
    MAX_REGISTERED_VALUE,
    "空间尾成员 RANGEINDEXSTREAMCHUNK 必须锚在登记块尾 {MAX_REGISTERED_VALUE:#04x}"
  );
}

/// 判别空间普查：全空间逐值反查，命中成员总数封顶、各块成员数与黄金计数一致、
/// 登记块尾之上不得长出成员——新增成员只能追加到 0x80 之后并同步本文件（总数或越界任一漂移即红）。
#[test]
fn aof_entry_type_discriminant_space_matches_golden_census() {
  let mut total = 0u8;
  let mut block_counts = [0u8; BLOCKS.len()];
  for value in u8::MIN..=u8::MAX {
    if entry_at(value).is_none() {
      continue;
    }
    total += 1;
    for (i, (lo, hi, _)) in BLOCKS.iter().enumerate() {
      if (*lo..=*hi).contains(&value) {
        block_counts[i] += 1;
      }
    }
  }
  assert_eq!(
    total, GOLDEN_MEMBER_COUNT,
    "判别值成员总数漂移（洞位被填，或块界外长出成员）"
  );
  for (i, (lo, hi, golden)) in BLOCKS.iter().enumerate() {
    assert_eq!(
      block_counts[i], *golden,
      "块 {lo:#04x}..={hi:#04x} 成员数漂移（应为 {golden}）"
    );
  }
  // 登记块尾之上必须保持为空：追加新成员需同步本文件的 MAX_REGISTERED_VALUE 与块表。
  for value in (MAX_REGISTERED_VALUE + 1)..=u8::MAX {
    assert_eq!(
      entry_at(value),
      None,
      "判别值 {value:#04x} 越登记块尾却长出成员（须同步黄金表与块表）"
    );
  }
}

/// 本仓与 C# 的已知差分清单，按名断言其确实成立：未转写块保持为洞（顺手把已写记录语义改掉即红）、
/// rust 独有成员钉在登记位上。
#[test]
fn aof_entry_type_registered_diffs_are_as_documented() {
  // 0x40..=0x43：C# MainStore/ObjectStore 的 StreamingCheckpointStart/EndCommit 是
  // ReplicaDisklessSync 流式复制同步标记（AofProcessor.cs 处 `Debug.Assert(ReplicaDisklessSync)` 门内），
  // 本仓未转写 diskless 流式 checkpoint 面，故 rust 不设成员、整块保持为洞。
  for value in 0x40..=0x43 {
    assert_eq!(
      entry_at(value),
      None,
      "{value:#04x} 是登记空洞（C# 流式 checkpoint 块，本仓未转写）"
    );
  }
  // 0x31：C# CheckpointStartCommit(0x30) 与 CheckpointEndCommit(0x32) 之间本就无成员，
  // 非本仓差分，钉为两侧同款空洞，防被误填。
  assert_eq!(
    entry_at(0x31),
    None,
    "0x31 是 C# 同款空洞（checkpoint 起止之间）"
  );
  // FlushNs(0x62)：rust 多租户共享单 AOF 扩展变体（整 ns 虚拟换号清库），C# 每实例单租户无此形态，
  // 追加在 flush 块尾、UnifiedStore 块之前，钉死其位。
  assert_eq!(
    entry_at(0x62),
    Some(AofEntryType::FlushNs),
    "FlushNs 追加位漂移"
  );
  assert_eq!(AofEntryType::FlushNs as u8, 0x62, "FlushNs 判别值漂移");
  // FlushNs 与 FlushAll/FlushDb 同块：无键条目（不入 HasKey 臂）。
  assert!(
    !AofEntryType::FlushNs.has_key(),
    "FlushNs 应与其余 flush 条目一样无 key 负载"
  );
}
