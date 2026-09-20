//! Garnet 内存对象统一抽象（对标 libs/server/Objects/Types/IGarnetObject.cs）
//!
//! 支持直接对象引用与就地变异（Hash, Set, List, ZSet），消除修改小字段时
//! 整包序列化落盘引发的写放大。对象值编解码权威单点在
//! [`crate::object_payload`]（信封 `[标签][计数][bitcode 载荷]` 与
//! [`crate::object_payload::GarnetObjectPayload`] 契约），本文件只承载跨类型操作抽象。

use std::fmt::Debug;

use wbase::time::now_ticks;
use wval::GarnetObjectType;

use crate::{
  HashObject, ListObject, ObjectOutput, SetObject, SortedSetObject,
  types::member_ttl::encode_member,
};

/// 列表升阶序号基准（u128 16B 大端保序：低半区留白，LPUSH 自基准向下
/// 递减、RPUSH 向上递增，u64 次操作互不环绕覆盖；wnode exec_tiered_list
/// 升阶转存共用）
pub const LIST_SEQ_BASE: u128 = 1u128 << 64;

/// Garnet 对象统一抽象接口
///
/// libs/server/Objects/Types/IGarnetObject.cs:IGarnetObject
pub trait IGarnetObject: Send + Sync + Debug {
  /// 获取对象类型标签
  fn obj_type(&self) -> GarnetObjectType;

  /// 在对象上执行操作
  ///
  /// libs/server/Objects/Types/IGarnetObject.cs:Operate
  fn operate(
    &mut self,
    sub_id: u16,
    args: &[&[u8]],
    arg1: i32,
    arg2: i32,
    output: &mut ObjectOutput<'_>,
    resp_protocol_version: u8,
  ) -> bool;

  /// 扫描集合对象成员
  ///
  /// libs/server/Objects/Types/IGarnetObject.cs:Scan
  fn scan(&self, start: i64, count: i64, pattern: &[u8]) -> (Vec<Vec<u8>>, i64);

  /// 序列化为字节向量
  fn serialize_to_vec(&self) -> Vec<u8>;

  /// 估算堆内存占用
  fn heap_memory_size(&self) -> i64;

  /// 对象元素总数（raw len 只读口径，无副作用；升阶/降阶判定输入。
  /// 含成员级 TTL 的类型对外计数用具体类型 `purge_expired_len`
  /// （堆序剔除过期后直读），与本口径一分二，禁止混用）
  fn count(&self) -> usize;

  /// 导出所有元素为键值对（用于就地升阶转存至底层 BfTree 引擎）
  fn export_entries(&self) -> Vec<(Vec<u8>, Vec<u8>)>;

  /// 对象是否为空
  fn is_empty(&self) -> bool;

  /// 判定集合是否满足升阶为独立分层树的条件（双维度 OR 逻辑：条目数 >= 65536 或 内存体积 >= 4MB）
  #[inline]
  fn should_promote(&self) -> bool {
    crate::should_promote(self.count(), self.heap_memory_size().max(0) as usize)
  }

  /// 判定集合是否满足降阶回退为内存信封的条件（双维度 AND 逻辑：条目数 <= 32768 且 内存体积 <= 2MB）
  #[inline]
  fn should_demote(&self) -> bool {
    crate::should_demote(self.count(), self.heap_memory_size().max(0) as usize)
  }
}

impl IGarnetObject for HashObject {
  #[inline]
  fn obj_type(&self) -> GarnetObjectType {
    GarnetObjectType::Hash
  }

  #[inline]
  fn operate(
    &mut self,
    sub_id: u16,
    args: &[&[u8]],
    arg1: i32,
    arg2: i32,
    output: &mut ObjectOutput<'_>,
    resp_protocol_version: u8,
  ) -> bool {
    self.operate(
      sub_id as u8,
      args,
      arg1,
      arg2,
      output,
      resp_protocol_version,
    )
  }

  #[inline]
  fn scan(&self, start: i64, count: i64, pattern: &[u8]) -> (Vec<Vec<u8>>, i64) {
    self.scan(start, count, pattern, false)
  }

  #[inline]
  fn serialize_to_vec(&self) -> Vec<u8> {
    self.serialize_to_vec()
  }

  #[inline]
  fn heap_memory_size(&self) -> i64 {
    self.heap_memory_size
  }

  #[inline]
  fn count(&self) -> usize {
    self.hash.len()
  }

  #[inline]
  fn export_entries(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
    // 升阶导出对标 C# DoSerialize「写时过滤」：已过期成员不入树（防续命），
    // 挂 TTL 成员随树记录落 8B 过期刻度（member_ttl 单点 codec）
    let now = now_ticks();
    self
      .hash
      .iter()
      .filter(|(k, _)| !self.ledger.is_expired_at(k, now))
      .map(|(k, v)| (k.clone(), encode_member(v, self.ledger.get_time(k))))
      .collect()
  }

  #[inline]
  fn is_empty(&self) -> bool {
    self.is_empty()
  }
}

impl IGarnetObject for SetObject {
  #[inline]
  fn obj_type(&self) -> GarnetObjectType {
    GarnetObjectType::Set
  }

  #[inline]
  fn operate(
    &mut self,
    sub_id: u16,
    args: &[&[u8]],
    arg1: i32,
    arg2: i32,
    output: &mut ObjectOutput<'_>,
    resp_protocol_version: u8,
  ) -> bool {
    self.operate(
      sub_id as u8,
      args,
      arg1,
      arg2,
      output,
      resp_protocol_version,
    )
  }

  #[inline]
  fn scan(&self, start: i64, count: i64, pattern: &[u8]) -> (Vec<Vec<u8>>, i64) {
    self.scan(start, count, pattern)
  }

  #[inline]
  fn serialize_to_vec(&self) -> Vec<u8> {
    self.serialize_to_vec()
  }

  #[inline]
  fn heap_memory_size(&self) -> i64 {
    self.heap_memory_size
  }

  #[inline]
  fn count(&self) -> usize {
    self.set.len()
  }

  #[inline]
  fn export_entries(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
    self
      .set
      .iter()
      .map(|k| (k.clone(), crate::SET_MEMBER_DUMMY_VALUE.to_vec()))
      .collect()
  }

  #[inline]
  fn is_empty(&self) -> bool {
    self.is_empty()
  }
}

impl IGarnetObject for ListObject {
  #[inline]
  fn obj_type(&self) -> GarnetObjectType {
    GarnetObjectType::List
  }

  #[inline]
  fn operate(
    &mut self,
    sub_id: u16,
    args: &[&[u8]],
    arg1: i32,
    arg2: i32,
    output: &mut ObjectOutput<'_>,
    resp_protocol_version: u8,
  ) -> bool {
    self.operate(
      sub_id as u8,
      args,
      arg1,
      arg2,
      output,
      resp_protocol_version,
    )
  }

  #[inline]
  fn scan(&self, _start: i64, _count: i64, _pattern: &[u8]) -> (Vec<Vec<u8>>, i64) {
    (Vec::new(), 0)
  }

  #[inline]
  fn serialize_to_vec(&self) -> Vec<u8> {
    self.serialize_to_vec()
  }

  #[inline]
  fn heap_memory_size(&self) -> i64 {
    self.heap_memory_size
  }

  #[inline]
  fn count(&self) -> usize {
    self.list.len()
  }

  #[inline]
  fn export_entries(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
    self
      .list
      .iter()
      .enumerate()
      // 序号 u128 16B 大端（保序）：自 `LIST_SEQ_BASE` 起按元素序连续排布，
      // 低半区留白供 LPUSH 向下、RPUSH 向上伸缩（与 exec_tiered_list 的序号
      // 空间同一约定，升阶/重灌与推入面共用单一编码，杜绝位置索引第二形态）
      .map(|(idx, val)| ((LIST_SEQ_BASE + idx as u128).to_be_bytes().to_vec(), val.clone()))
      .collect()
  }

  #[inline]
  fn is_empty(&self) -> bool {
    self.is_empty()
  }
}

impl IGarnetObject for SortedSetObject {
  #[inline]
  fn obj_type(&self) -> GarnetObjectType {
    GarnetObjectType::SortedSet
  }

  #[inline]
  fn operate(
    &mut self,
    sub_id: u16,
    args: &[&[u8]],
    arg1: i32,
    arg2: i32,
    output: &mut ObjectOutput<'_>,
    resp_protocol_version: u8,
  ) -> bool {
    self.operate(
      sub_id as u8,
      args,
      arg1,
      arg2,
      output,
      resp_protocol_version,
    )
  }

  #[inline]
  fn scan(&self, start: i64, count: i64, pattern: &[u8]) -> (Vec<Vec<u8>>, i64) {
    let (items, cursor) = self.scan(start, count, pattern, false);
    (items.into_iter().flatten().collect(), cursor)
  }

  #[inline]
  fn serialize_to_vec(&self) -> Vec<u8> {
    self.serialize_to_vec()
  }

  #[inline]
  fn heap_memory_size(&self) -> i64 {
    self.heap_memory_size
  }

  #[inline]
  fn count(&self) -> usize {
    // 升阶判定（should_promote/should_demote）输入，取未剔过期 raw len 保持 O(1)
    // 且不副作用；对外计数口径以具体类型 SortedSetObject::purge_expired_len
    // （堆序 purge）为准
    self.sorted_set_dict.len()
  }

  #[inline]
  fn export_entries(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
    // 升阶导出「写时过滤 + TTL 随行」同 Hash 臂（见上）
    let now = now_ticks();
    self
      .sorted_set_dict
      .iter()
      .filter(|(k, _)| !self.ledger.is_expired_at(k, now))
      .map(|(k, score)| {
        (
          k.clone(),
          encode_member(&score.to_be_bytes(), self.ledger.get_time(k)),
        )
      })
      .collect()
  }

  #[inline]
  fn is_empty(&self) -> bool {
    self.is_empty()
  }
}
