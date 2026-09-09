//! 范围索引操作辅助（对标 libs/server/Storage/Session/MainStore/RangeIndexOps.cs，C# 为 StorageSession partial）
//!
//! 缺口总述：C# 侧 RangeIndex 直接持有 BfTree 裸指针（ExtractTreePtr）并在
//! StorageSession 内做"索引 → Tail 记录"的晋升/恢复/RESP 序列化；wkv 将范围
//! 索引内置于引擎（`WedbStore::range_index` + `RangeIndexListenerFn` 回写），
//! 无裸指针与手工晋升入口。本域保留纯输出组装函数的真实实现，引擎侧两函数
//! 退化为一致性空操作并注明缺口。

use wdev::Device;

use super::super::storage_session::StorageSession;

impl<'a, D: Device> StorageSession<'a, D> {
  /// 将范围索引数据晋升为 Tail 存储记录
  ///
  /// 缺口说明：wkv 引擎内部以 `RangeIndexListenerFn` 自动把索引变更落盘，
  /// 无外部晋升入口；本方法退化为空操作（返回晋升字节数 0）。
  ///
  /// libs/server/Storage/Session/MainStore/RangeIndexOps.cs:PromoteRangeIndexToTail
  pub fn promote_range_index_to_tail(&self) -> usize {
    0
  }

  /// 从 Tail 存根恢复范围索引
  ///
  /// 缺口说明：wkv 引擎在 `WedbStore::open` 时经 `recover_shared_bftree` /
  /// RangeIndexManager 自动恢复索引存根，无外部恢复入口；本方法退化为空操作。
  ///
  /// libs/server/Storage/Session/MainStore/RangeIndexOps.cs:RestoreRangeIndexStub
  pub fn restore_range_index_stub(&self) -> bool {
    true
  }

  /// 把扫描记录按 RESP 数组元素格式写入输出缓冲
  ///
  /// libs/server/Storage/Session/MainStore/RangeIndexOps.cs:WriteScanToOutput
  pub fn write_scan_to_output(&self, output: &mut Vec<u8>, records: &[Vec<u8>]) {
    output.reserve(16 + records.iter().map(|r| r.len() + 8).sum::<usize>());
    output.extend_from_slice(format!("*{}\r\n", records.len()).as_bytes());
    for record in records {
      output.extend_from_slice(format!("${}\r\n", record.len()).as_bytes());
      output.extend_from_slice(record);
      output.extend_from_slice(b"\r\n");
    }
  }

  /// 尝试将单条索引记录写成 RESP 批量字符串，返回是否写入
  ///
  /// libs/server/Storage/Session/MainStore/RangeIndexOps.cs:TryWriteRecordResp
  pub fn try_write_record_resp(&self, output: &mut Vec<u8>, record: &[u8]) -> bool {
    if record.is_empty() {
      output.extend_from_slice(b"$-1\r\n");
      return false;
    }
    output.extend_from_slice(itoa::Buffer::new().format(record.len()).as_bytes());
    output.extend_from_slice(b"\r\n");
    output.extend_from_slice(record);
    output.extend_from_slice(b"\r\n");
    true
  }

  /// 回填 RESP 数组头（先占位后回填，适配长度未知流式输出）
  ///
  /// libs/server/Storage/Session/MainStore/RangeIndexOps.cs:BackfillArrayHeader
  pub fn backfill_array_header(&self, output: &mut [u8], header_pos: usize, count: usize) {
    let header = format!("*{count}\r\n");
    let end = header_pos + header.len();
    if end <= output.len() {
      output[header_pos..end].copy_from_slice(header.as_bytes());
    }
  }

  /// 提取底层范围索引树句柄
  ///
  /// 缺口说明：C# 提取 BfTree 裸指针供扫描复用；wkv 引擎封装扫描入口
  /// （`WedbStore::scan_range_callback`）不外借树句柄，恒返回 None，
  /// 调用方应转用扫描回调路径。
  ///
  /// libs/server/Storage/Session/MainStore/RangeIndexOps.cs:ExtractTreePtr
  pub fn extract_tree_ptr(&self) -> Option<()> {
    None
  }
}
