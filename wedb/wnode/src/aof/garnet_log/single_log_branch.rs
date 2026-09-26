//! 背压等待与入队面（对标 libs/server/AOF/GarnetLog.cs:BackpressureWaitKey、
//! BackpressureWaitVector、Enqueue、EnqueueSpanChunked、EnqueueStoredProc、
//! EnqueueTxn、EnqueueBroadcastEntry、EnqueueDatabaseCommit、
//! EnqueueSafeFlushAOF 段；各入队口含单物理日志分支。键闸门只在顶层 Enqueue
//! 命中一次；C# 的 EnqueueObjectChunked / BackpressureWaitKeyHash 服务于对象
//! 存储流式分块形态，rust 信封值入队前已定长、无对应形态，见 ignore 登记）。

use smallvec::SmallVec;
use waof::{
  AofChunkHeader, AofEntryType, AofHeader, AofHeaderType, AofShardedHeader,
  AofShardedLogTransactionHeader, AofSingleLogTransactionHeader,
};
use wbase::store_type::REPLAY_TASK_ACCESS_VECTOR_BYTES;
use wtxn::SublogAccess;

use super::{ChunkedShape, GarnetLog, MIN_PARTIAL_ALLOC_SIZE, RecordShape};

impl GarnetLog {
  /// libs/server/AOF/GarnetLog.cs:BackpressureWaitKey
  #[inline]
  fn backpressure_wait_key(&self, key: &[u8]) {
    let Some(backpressure) = &self.backpressure else {
      return;
    };
    let sublog_idx = self.get_physical_sublog_idx(Self::hash(key));
    backpressure.wait(sublog_idx, self.get_tail_address(sublog_idx));
  }

  /// libs/server/AOF/GarnetLog.cs:BackpressureWaitVector
  ///
  /// 多子日志追加前的失效保护自检：对每个参与子日志做背压等待。
  #[inline]
  fn backpressure_wait_vector(&self, mut physical_sublog_access_vector: u64) {
    let Some(backpressure) = &self.backpressure else {
      return;
    };
    while physical_sublog_access_vector > 0 {
      let sublog_idx = physical_sublog_access_vector.trailing_zeros() as usize;
      backpressure.wait(sublog_idx, self.get_tail_address(sublog_idx));
      physical_sublog_access_vector &= physical_sublog_access_vector - 1;
    }
  }

  /// libs/server/AOF/GarnetLog.cs:IsChunkable
  ///
  /// key+value+input 总规模超过最小部分分配尺寸即需分块。
  pub fn is_chunkable(key_len: usize, value_len: usize, input_serialized_length: usize) -> bool {
    (key_len + value_len + input_serialized_length) as i64 > MIN_PARTIAL_ALLOC_SIZE
  }

  /// 头编码 + 负载拼装的通用入队：按拓扑选择 Basic/Sharded 头，
  /// 返回逻辑地址。
  ///
  /// 负载以部件表 scatter-write 直达子日志（Sharded 头拆为 Basic 头 + 序列号
  /// 两部件，布局与 `AofShardedHeader::to_bytes` 逐字节一致），零整包 Vec：
  /// 大 value/input 不再经历"拼整包 → enqueue 整量拷贝"的两轮 memcpy。
  fn enqueue_with_header(&self, record: &RecordShape<'_>) -> waof::Result<i64> {
    let RecordShape {
      op_type,
      version,
      session_id,
      key,
      value,
      input,
      database_id,
    } = *record;
    let mut header = AofHeader::new();
    header.op_type = op_type as u8;
    header.store_version = version;
    header.session_id = session_id;
    header.database_id = database_id;

    let physical_sublog_idx = if self.using_single_physical_log {
      0
    } else {
      self.get_physical_sublog_idx(Self::hash(key))
    };
    header.set_header_type(if self.using_single_physical_log {
      AofHeaderType::BasicHeader
    } else {
      AofHeaderType::ShardedHeader
    });
    // 单物理日志无序列号占位（不入部件表）；分片拓扑序列号紧跟 Basic 头之后
    let seq_bytes = if self.using_single_physical_log {
      [0u8; 8]
    } else {
      self.next_sequence_number().to_le_bytes()
    };
    let basic_bytes = header.to_bytes();
    let key_len_bytes = (key.len() as u32).to_le_bytes();
    let value_len_bytes;

    let mut parts: SmallVec<[&[u8]; 8]> = SmallVec::new();
    parts.push(&basic_bytes[..]);
    if !self.using_single_physical_log {
      parts.push(&seq_bytes[..]);
    }
    parts.push(&key_len_bytes[..]);
    parts.push(key);
    if op_type.has_chunk_value() {
      value_len_bytes = (value.len() as u32).to_le_bytes();
      parts.push(&value_len_bytes[..]);
      parts.push(value);
    }
    parts.push(input);

    let address = self
      .get_sub_log(physical_sublog_idx)
      .enqueue_parts(&parts)?;
    if self.auto_commit {
      self.commit();
    }
    Ok(address)
  }

  /// libs/server/AOF/GarnetLog.cs:Enqueue（upsert/RMW/delete 通用形状）
  ///
  /// 背压等待（全路径仅此一处闸门命中）→ 大记录自动分块（组件选择按 op 类型）
  /// → 头编码入队。
  /// 失败显式上抛由调用方拒绝该命令（C# 分配失败抛 TsavoriteException）。
  pub fn enqueue(&self, record: &RecordShape<'_>) -> waof::Result<i64> {
    self.backpressure_wait_key(record.key);
    if Self::is_chunkable(record.key.len(), record.value.len(), record.input.len()) {
      return self.enqueue_span_chunked(&ChunkedShape {
        record: record.clone(),
        write_value: record.op_type.has_chunk_value(),
        write_input: record.op_type.has_chunk_input(),
      });
    }
    self.enqueue_with_header(record)
  }

  /// libs/server/AOF/GarnetLog.cs:EnqueueSpanChunked
  /// libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLog.Chunked.cs:WriteOneRecord
  ///
  /// 大记录分块写入：全量长度预先盖入分块头，使读取器可按组件预分配。
  /// `write_value` / `write_input` 选择组件（key 恒写）。各帧以部件表
  /// scatter-write 直达子日志，零整包 Vec。对标 C#：闸门只由顶层 Enqueue
  /// 承担，本函数不再等待；key 哈希仅在组分块头时算一次，子日志路由
  /// 复用 `chunk_header.key_hash`（C# chunkHeader.keyHash 同位）。
  ///
  /// 线协议（C# WriteOneRecord 契约）：**每一帧**都携带完整帧头
  /// （帧头 + [序列号] + 分块帧头），op_type/version/session_id/database_id/
  /// key_hash/object_id 全帧一致，续块绝非裸数据帧——并行恢复的
  /// `can_replay` 对每条记录盲调头解析并按 key_hash 一致性路由，
  /// 裸续块帧会因缺头崩溃或误路由。objectId 唯一标识逻辑记录（C# 为首块
  /// 逻辑地址；rust 写端地址入队时才预留，以 key_hash 承担分组键：同记录
  /// 全帧同键、帧组经 enqueue_frames 原子入队绝不插花，顺序消费下进行中
  /// 记录天然不重叠）。
  fn enqueue_span_chunked(&self, chunk: &ChunkedShape<'_>) -> waof::Result<i64> {
    let ChunkedShape {
      record:
        RecordShape {
          op_type,
          version,
          session_id,
          key,
          value,
          input,
          database_id,
        },
      write_value,
      write_input,
    } = *chunk;
    let key_hash = Self::hash(key);
    let chunk_header = AofChunkHeader {
      overflow_key_length: key.len() as u32,
      overflow_value_length: if write_value { value.len() as u32 } else { 0 },
      input_length: if write_input { input.len() as u32 } else { 0 },
      object_id: key_hash as u64,
      key_hash,
    };
    let using_single_physical_log = self.using_single_physical_log;
    let header_size = if using_single_physical_log {
      AofHeader::TOTAL_SIZE
    } else {
      AofShardedHeader::TOTAL_SIZE
    };

    let page_payload = ((1usize << self.unsafe_get_log_page_size_bits() as u32)
      - header_size
      - AofChunkHeader::TOTAL_SIZE)
      .max(1);

    let mut header = AofHeader::new();
    header.set_header_type(if using_single_physical_log {
      AofHeaderType::BasicChunkHeader
    } else {
      AofHeaderType::ShardedChunkHeader
    });
    header.op_type = op_type as u8;
    header.store_version = version;
    header.session_id = session_id;
    header.database_id = database_id;
    let basic_bytes = header.to_bytes();
    // Sharded 分块头 = Basic 分块头 + 序列号两部件，布局与整包编码逐字节一致；
    // 单物理日志无序列号占位（不入部件表）。序列号全帧复用同一值（C# 模板
    // ChunkHeaderWriter 逐帧拷贝同一头的同位语义）
    let seq_bytes = if using_single_physical_log {
      [0u8; 8]
    } else {
      self.next_sequence_number().to_le_bytes()
    };
    let chunk_bytes = chunk_header.to_bytes();

    let physical_sublog_idx = if using_single_physical_log {
      0
    } else {
      self.get_physical_sublog_idx(chunk_header.key_hash)
    };

    // 帧部件表 = [帧头, (序列号), 分块帧头, 数据部件...]，头字节全帧复用同
    // 一栈数组。组件依序紧凑装箱（C# packed segments：key+value+input 可共享
    // 一帧），片边界只由页载荷上界决定，读端以声明长度驱动组件边界；
    // 空组件零部件（对齐读端 feed 的声明长度驱动）
    let comps: [&[u8]; 3] = [
      key,
      if write_value { value } else { &[] },
      if write_input { input } else { &[] },
    ];
    let mut frames: SmallVec<[SmallVec<[&[u8]; 8]>; 9]> = SmallVec::new();
    fn open_frame<'p>(
      basic: &'p [u8],
      seq: Option<&'p [u8]>,
      chunk: &'p [u8],
      frames: &mut SmallVec<[SmallVec<[&'p [u8]; 8]>; 9]>,
    ) {
      let mut parts = SmallVec::new();
      parts.push(basic);
      if let Some(seq) = seq {
        parts.push(seq);
      }
      parts.push(chunk);
      frames.push(parts);
    }
    let seq_part = if using_single_physical_log {
      None
    } else {
      Some(&seq_bytes[..])
    };
    open_frame(&basic_bytes[..], seq_part, &chunk_bytes[..], &mut frames);
    let mut used = 0usize;
    let mut ci = 0usize;
    let mut off = 0usize;
    while ci < comps.len() {
      let comp = comps[ci];
      if off >= comp.len() {
        ci += 1;
        off = 0;
        continue;
      }
      let cap = page_payload - used;
      if cap == 0 {
        open_frame(&basic_bytes[..], seq_part, &chunk_bytes[..], &mut frames);
        used = 0;
        continue;
      }
      let take = (comp.len() - off).min(cap);
      if let Some(frame) = frames.last_mut() {
        frame.push(&comp[off..off + take]);
      }
      used += take;
      off += take;
    }
    let frame_slices: SmallVec<[&[&[u8]]; 9]> =
      frames.iter().map(|frame| frame.as_slice()).collect();
    let address = self
      .get_sub_log(physical_sublog_idx)
      .enqueue_frames(&frame_slices)?;

    if self.auto_commit {
      self.commit();
    }
    Ok(address)
  }

  /// libs/server/AOF/GarnetLog.cs:EnqueueStoredProc
  ///
  /// 存储过程条目：单日志 BasicHeader / 单物理多回放轻量事务头 /
  /// 分片事务头逐参与子日志广播（位图逐子日志盖入，参与者计数随行）。
  /// 头与负载以部件表 scatter-write 单次入队（TsavoriteLog.cs:Enqueue 双部件
  /// 形态），空 body 部件表 [头] 与整包帧字节逐一致。
  pub fn enqueue_stored_proc(
    &self,
    op_type: AofEntryType,
    version: i64,
    session_id: i32,
    procedure_id: u8,
    body: &[u8],
    access: &SublogAccess<'_>,
  ) -> waof::Result<i64> {
    if self.using_single_physical_log {
      if let Some(gate) = &self.backpressure {
        gate.wait(0, self.get_tail_address(0));
      }
    } else {
      self.backpressure_wait_vector(access.physical_vector);
    }

    let address = if self.using_single_log {
      let mut header = AofHeader::new();
      header.set_header_type(AofHeaderType::BasicHeader);
      header.op_type = op_type as u8;
      header.procedure_id = procedure_id;
      header.store_version = version;
      header.session_id = session_id;
      let header_bytes = header.to_bytes();
      self
        .get_sub_log(0)
        .enqueue_parts(&[&header_bytes[..], body])?
    } else if self.using_single_physical_log {
      let mut header = AofHeader::new();
      header.set_header_type(AofHeaderType::SingleLogTransactionHeader);
      header.op_type = op_type as u8;
      header.procedure_id = procedure_id;
      header.store_version = version;
      header.session_id = session_id;
      let txn_header = AofSingleLogTransactionHeader {
        basic: header,
        participant_count: access.participant_count as i16,
        replay_task_access_vector: access
          .virtual_vectors
          .first()
          .copied()
          .unwrap_or([0; REPLAY_TASK_ACCESS_VECTOR_BYTES]),
      };
      let txn_bytes = txn_header.to_bytes();
      self.get_sub_log(0).enqueue_parts(&[&txn_bytes[..], body])?
    } else {
      let mut header = AofHeader::new();
      header.set_header_type(AofHeaderType::ShardedLogTransactionHeader);
      header.op_type = op_type as u8;
      header.procedure_id = procedure_id;
      header.store_version = version;
      header.session_id = session_id;
      let txn_header = AofShardedLogTransactionHeader {
        sharded: AofShardedHeader {
          basic: header,
          sequence_number: self.next_sequence_number(),
        },
        participant_count: access.participant_count as i16,
        replay_task_access_vector: [0; REPLAY_TASK_ACCESS_VECTOR_BYTES],
      };
      self.lock_sublogs(access.physical_vector);
      let mut address = 0;
      let mut vector = access.physical_vector;
      while vector > 0 {
        let sublog_idx = vector.trailing_zeros() as usize;
        vector &= vector - 1;
        let mut txn_header = txn_header;
        txn_header.replay_task_access_vector = access
          .virtual_vectors
          .get(sublog_idx)
          .copied()
          .unwrap_or([0; REPLAY_TASK_ACCESS_VECTOR_BYTES]);
        let txn_bytes = txn_header.to_bytes();
        address = self
          .get_sub_log(sublog_idx)
          .enqueue_parts(&[&txn_bytes[..], body])?;
      }
      self.unlock_sublogs(access.physical_vector);
      address
    };

    if self.auto_commit {
      self.commit();
    }
    Ok(address)
  }

  /// libs/server/AOF/GarnetLog.cs:EnqueueTxn
  ///
  /// 事务标记条目：与存储过程同形状（无过程 id、空体）。
  #[inline]
  pub fn enqueue_txn(
    &self,
    op_type: AofEntryType,
    version: i64,
    session_id: i32,
    access: &SublogAccess<'_>,
  ) -> waof::Result<i64> {
    self.enqueue_stored_proc(op_type, version, session_id, 0, &[], access)
  }

  /// libs/server/AOF/GarnetLog.cs:EnqueueBroadcastEntry
  ///
  /// 全子日志广播条目：须对所有回放任务可见的标记（数据库提交、
  /// safe-flush、checkpoint），位图全置位、参与者 = 虚拟子日志总数。
  /// `extra` 为头之后的域载荷尾段（FLUSH 族 ns/db 维度；其余标记为空）。
  fn enqueue_broadcast_entry(&self, basic_header: AofHeader, extra: &[u8]) -> waof::Result<i64> {
    if self.using_single_physical_log {
      if let Some(gate) = &self.backpressure {
        gate.wait(0, self.get_tail_address(0));
      }
    } else {
      self.backpressure_wait_vector(self.all_logs_bitmask());
    }

    if self.using_single_log {
      let mut payload = basic_header.to_bytes().to_vec();
      payload.extend_from_slice(extra);
      return self.get_sub_log(0).enqueue(&payload);
    }
    if self.using_single_physical_log {
      let mut basic = basic_header;
      basic.set_header_type(AofHeaderType::SingleLogTransactionHeader);
      let txn_header = AofSingleLogTransactionHeader {
        basic,
        participant_count: (self.physical_sublog_count * self.replay_task_count) as i16,
        replay_task_access_vector: [0xFF; REPLAY_TASK_ACCESS_VECTOR_BYTES],
      };
      let mut payload = txn_header.to_bytes().to_vec();
      payload.extend_from_slice(extra);
      return self.get_sub_log(0).enqueue(&payload);
    }
    let mut basic = basic_header;
    basic.set_header_type(AofHeaderType::ShardedLogTransactionHeader);
    let txn_header = AofShardedLogTransactionHeader {
      sharded: AofShardedHeader {
        basic,
        sequence_number: self.next_sequence_number(),
      },
      participant_count: (self.physical_sublog_count * self.replay_task_count) as i16,
      replay_task_access_vector: [0xFF; REPLAY_TASK_ACCESS_VECTOR_BYTES],
    };
    let physical_sublog_access_vector = self.all_logs_bitmask();
    self.lock_sublogs(physical_sublog_access_vector);
    let mut address = 0;
    let mut vector = physical_sublog_access_vector;
    let mut payload = txn_header.to_bytes().to_vec();
    payload.extend_from_slice(extra);
    while vector > 0 {
      let sublog_idx = vector.trailing_zeros() as usize;
      vector &= vector - 1;
      address = self.get_sub_log(sublog_idx).enqueue(&payload)?;
    }
    self.unlock_sublogs(physical_sublog_access_vector);
    if self.auto_commit {
      self.commit();
    }
    Ok(address)
  }

  /// libs/server/AOF/GarnetLog.cs:EnqueueDatabaseCommit
  /// libs/server/Databases/DatabaseManagerBase.cs:EnqueueDatabaseCommit
  ///（基类 protected 静态助手即 `db.AppendOnlyFile.Log.EnqueueDatabaseCommit`
  /// 单行转发，rust 共享单 AOF 面直接合并到本内核，不另设转发层）
  ///
  /// FLUSH 数据库/全部的提交标记（广播，sessionID = -1）。
  pub fn enqueue_database_commit(&self, op_type: AofEntryType, version: i64) -> waof::Result<i64> {
    let mut header = AofHeader::new();
    header.set_header_type(AofHeaderType::BasicHeader);
    header.op_type = op_type as u8;
    header.store_version = version;
    header.session_id = -1;
    self.enqueue_broadcast_entry(header, &[])
  }

  /// libs/server/AOF/GarnetLog.cs:EnqueueSafeFlushAOF
  ///
  /// 安全 flush 提交标记（广播，storeVersion = 0、sessionID = -1）。
  ///
  /// rust 扩展：头后追加 16B 域载荷 [ns: u64 LE][db: u64 LE]（FlushDb 携带
  /// (vns, 旧 vdb)、FlushNs 携带 (旧 vns, 0)、FlushAll 携带零值），域值与数据
  /// 条目物理键前缀同域（主库虚拟 ID 对，从库按「条目域值当逻辑值」重映射）；
  /// C# 的 databaseId 仅 1 字节且单租户无 ns 维度，rust 以载荷为单一真相源
  /// （u64 全宽），header.database_id 置 0 弃用。
  pub fn enqueue_safe_flush_aof(
    &self,
    op_type: AofEntryType,
    unsafe_truncate_log: bool,
    ns: u64,
    db: u64,
  ) -> waof::Result<i64> {
    let mut header = AofHeader::new();
    header.set_header_type(AofHeaderType::BasicHeader);
    header.op_type = op_type as u8;
    header.store_version = 0;
    header.session_id = -1;
    if unsafe_truncate_log {
      header.set_unsafe_truncate_log(true);
    }
    let mut extra = [0u8; 16];
    extra[0..8].copy_from_slice(&ns.to_le_bytes());
    extra[8..16].copy_from_slice(&db.to_le_bytes());
    self.enqueue_broadcast_entry(header, &extra)
  }
}
