//! 向量域 drop 清扫（对标 libs/server/Resp/Vector/VectorManager.Cleanup.cs 的
//! PostDropCleanupFunctions + IterateLookupSnapshot 全日志扫描删除段）
//!
//! C# 侧 Vector Set 删除后由常驻清理任务对整条日志做快照扫描，凡命名空间
//! （即集合上下文）命中待清理集合的记录一律经 vector context 物理删除；rust
//! 侧向量元素记录以 `[prefix][KeyTag::Vector][context: 8B BE][key]` 物理键落盘
//! （context 低 3 位为项类型子域：完整向量/邻接表/属性/ID 映射等，见
//! wvector store::term），删除方（wnode）只持有集合上下文基址、无法重组物理
//! 前缀，故清扫以「解码比对上下文基址」为过滤内核，本模块即其单点。
//!
//! 在 garnet 中的相对路径: libs/server/Storage/Functions/MainStore/DeleteMethods.cs → VectorManager.RequestDeletion

use std::sync::Arc;

use wbase::map::{GxBuildHasher, HashSet};
use wdev::Device;
use wval::{KeyTag, NamespaceDbCodec};

use crate::{error::Result, session::StoreSession};

/// 上下文基址掩码：物理键 context 字段低 3 位为项类型子域（写入方约定，
/// 对齐 wnode CONTEXT_STEP = 8 与 C# ContextStep 的整块步长），清扫按
/// `stored & !MASK` 比对基址，一次覆盖该集合全部项类型记录
const CONTEXT_TERM_MASK: u64 = 0b111;

/// 物理键是否为命中 `context` 基址的向量域记录（零分配过滤内核）
///
/// 解码失败（非本仓编码/滑窗残片）按不命中处理，扫描侧跳过
#[inline]
fn matches_vector_context(key: &[u8], context: u64) -> bool {
  let Ok((_, _, tag, payload)) = NamespaceDbCodec::decode_tagged_key(key) else {
    return false;
  };
  tag == KeyTag::Vector
    && payload
      .first_chunk::<{ size_of::<u64>() }>()
      .is_some_and(|ctx| u64::from_be_bytes(*ctx) & !CONTEXT_TERM_MASK == context)
}

impl<D: Device> StoreSession<D> {
  /// 全日志扫描并物理清除指定上下文基址的全部向量域元素记录
  ///
  /// libs/server/Resp/Vector/VectorManager.Cleanup.cs:Reader
  ///（PostDropCleanupFunctions：HasNamespace 过滤 + `ns & ~(ContextStep - 1)`
  /// 基址配对 + vectorBasicContext.Delete 逐记录物理删除）与
  /// libs/server/Resp/Vector/VectorManager.Cleanup.cs:OnStart/OnStop
  ///（扫描起止回调 → rust 迭代器循环体收敛）在本 rust 单点的折叠映射。
  ///
  /// C# 对标差异两处（语义等价）：
  /// - C# 逐记录即时 Delete（同键多版本产生冗余墓碑）；rust 两段式——扫描段
  ///   去重收集物理键（同键 RCU 多版本只删一次），扫描结束后逐键
  ///   [`Self::delete_raw`] 墓碑（页读锁窗口内严禁写 store，同 ttl_sweep
  ///   「收集完成后统一删除」纪律），物理空间交后台 compaction 回收（与 DEL
  ///   同一 O(1) 墓碑语义）；
  /// - C# 全日志扫描按命名空间字节比对；rust 解码比对上下文基址，天然覆盖
  ///   全部 ns/db 物理前缀（上下文全局唯一，物理前缀随虚库换代不可重组）。
  ///
  /// 扫描区间 `[begin, tail)`；区间端点为调用时快照——调用方保证目标上下文
  /// 已隔离（清理中标记，索引已丢弃，无新写可达），快照后追加记录不可能属于
  /// 本上下文。与热写并发遵循 [`whlog::ScanIterator`] 在途零头自旋契约的最终
  /// 一致尽力语义。
  ///
  /// 返回实际新增墓碑键数：收集段去重后的命中键逐键删除，链首已是墓碑的键
  /// （元素级 VREM 已删等）被删除探测短路、零追加亦不计数。任一删除失败即
  /// 上抛，调用方须保持上下文隔离待重扫（C# 同语义：清理循环异常即留标记
  /// 重试，不归还上下文）。
  pub async fn purge_vector_context(&self, context: u64) -> Result<u64> {
    let store = Arc::clone(&self.store);
    let begin = store.begin_address();
    let tail = store.tail_address();

    // 段 1：扫描收集（键拷贝出页锁窗口；contains 预判 + HashSet 去重同键多版本，
    // 重复命中免分配后丢弃的键拷贝）
    let mut victims: HashSet<Box<[u8]>> = HashSet::with_hasher(GxBuildHasher::default());
    let mut it = store.hlog.scan_iter(begin, tail);
    while it
      .next_ref(|item| {
        if !item.rec.is_tombstone()
          && matches_vector_context(item.rec.key, context)
          && !victims.contains(item.rec.key)
        {
          victims.insert(Box::from(item.rec.key));
        }
        Ok(())
      })
      .await?
      .is_some()
    {}

    // 段 2：物理墓碑（去重后逐键一次；delete_raw 未命中返回假，幂等无害）
    let mut purged = 0u64;
    for key in &victims {
      if self.delete_raw(key).await? {
        purged += 1;
      }
    }
    Ok(purged)
  }
}

#[cfg(test)]
mod tests {
  use wval::{KeyTag, NamespaceDbCodec};

  use super::{CONTEXT_TERM_MASK, matches_vector_context};

  /// 过滤内核单测：基址配对覆盖项类型子域，异基址/异标签/坏编码不命中
  #[test]
  fn context_base_matching_rules() {
    assert_eq!(CONTEXT_TERM_MASK, 0b111);
    let prefix = [0u8, 0u8];
    let encode = |ctx: u64, key: &[u8]| {
      NamespaceDbCodec::encode_vector_key_with_prefix(&prefix, ctx, key).into_vec()
    };
    let other_tag = |payload: &[u8]| {
      NamespaceDbCodec::encode_with_session_prefix(&prefix, KeyTag::String, payload).into_vec()
    };

    // 全部项类型（低 3 位 0..7）命中同一基址
    for term in 0..8u64 {
      assert!(matches_vector_context(&encode(8 | term, b"k"), 8));
    }
    // 异基址不命中；查询侧携带子域位的非基址 context 同样不命中
    //（生产查询恒传基址，掩码只施加于存储侧）
    assert!(!matches_vector_context(&encode(16, b"k"), 8));
    assert!(!matches_vector_context(&encode(8, b"k"), 9));
    // 异标签不命中；坏编码（截断）不命中
    assert!(!matches_vector_context(&other_tag(&8u64.to_be_bytes()), 8));
    assert!(!matches_vector_context(&[KeyTag::Vector.as_u8(), 0, 0], 8));
  }
}
