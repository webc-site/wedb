//! 磁盘存储回调实现（对标 C# VectorManager.Callbacks.cs）
//!
//! 将 DiskANN 向量图的向量数据、邻接表（图拓扑）、量化状态、属性和 ID 映射
//! 通过统一的 `[命名空间字节][键字节]` 物理键落盘到 wkv 存储引擎（Tsavorite 混合日志）。
//!
//! C# 批量读构件 [`VectorReadBatch`] 的两项逐次策略在本端口由引擎单点承接，
//! 故本文件不再自建同名概念：
//!   * 初读尺寸 `InitialIORecordSize`（同文件 :38-57，配合
//!     `SetActiveReadGeometry` :312-339 按项类型预算单 IO 尺寸）→ wkv 冷读侧的
//!     探针 + 精确二次读（whlog `hlog/mod.rs:36` 探针长度、
//!     `hlog/io.rs:141-165` 单记录绝不整页读），故 [`StoreCallbacks::read_multi`]
//!     不消费 `length_hint`；
//!   * 回填拷贝 `ReadCopyOptions`（同文件 :68-85，图内小记录
//!     NeighborList/QuantizedVector/Metadata/InternalIdMap/ExternalIdMap 取
//!     `ReadCopyFrom.AllImmutable` + `CopyTo = StubReadCopyTo`（:295 由
//!     `EnableReadCache` 定为只读缓存或主日志尾部），FullVector/Attributes 取
//!     `None`）→ 引擎冷读回填单点（wkv `session/raw/read.rs:673-690`：启用
//!     `ReadCache` 时挂只读缓存、否则按会话 `copy_reads_to_tail` 晋升尾部），
//!     并以 `read_cache/append.rs:52-54`「单记录超页容量即不入缓存」的尺寸准入
//!     承接 C# 排除大尺寸 FullVector 的同一目的；
//!   * 批量下发与完成收割 `ReadCallbackUnmanaged`（同文件 :356-362 单次
//!     `ReadWithPrefetch` + `hasPending` 为真才 `CompletePending(wait: true)`）→
//!     [`StoreCallbacks::read_multi`] 的窗口化内存批量直读 + 冷候选单次批量收割。
//!
//! 冷读/落盘的同步收割只在调入线程内联完成（对标 C# DiskANN 经
//! `[UnmanagedCallersOnly]` 反向 p/invoke 调入后 `CompletePending(wait: true)`）：
//! 统一走 `wbase::future::blocking_wait` 单点，本文件不自建驱动器；向量命令均由
//! 每核 worker 线程（`Server::start_tcp_workers` 各自的 `Runtime::new` +
//! `block_on`）调入，收割即收割本线程 runtime 的任务队列与 I/O driver。

use std::sync::Arc;

use wbase::future::blocking_wait;
use wdev::Device;
use wkv::{PREFETCH_WINDOW, StoreResult, StoreSession};
use wval::TaggedKeyBuf;
use wvector::store::{LengthPrefixedIter, StoreCallbacks};

/// 基于 wkv 存储会话的真实向量磁盘存储回调
///
/// 生产装配的会话上下文单写者纪律（wkv `StoreSession` 类型注释、
/// doc/zh/db.md §1.2）唯一许可的跨任务长持形态：装配期一次性固化会话
/// 上下文（节点装配为根域 0,0），回调执行路径绝不触碰 `set_context` /
/// `set_active_db` / `set_virtual_context` 族，仅经 `session_prefix` 的虚库
/// 换代幂等刷新收敛物理前缀（清库换号后自动重对齐新域）。新增跨任务
/// 会话持有须比照本纪律收口，不得随手以 `Arc<StoreSession>` 存可变字段。
pub struct WedbVectorStoreCallbacks<D: Device + 'static> {
  session: Arc<StoreSession<D>>,
}

impl<D: Device + 'static> WedbVectorStoreCallbacks<D> {
  /// 创建存储回调绑定
  pub fn new(session: Arc<StoreSession<D>>) -> Self {
    Self { session }
  }

  /// 获取底层存储会话句柄
  #[inline]
  pub fn session(&self) -> &Arc<StoreSession<D>> {
    &self.session
  }
}

impl<D: Device + 'static> StoreCallbacks for WedbVectorStoreCallbacks<D> {
  /// 批量读（对标 C# ReadCallbackUnmanaged:341-363 的 `ReadWithPrefetch` +
  /// `CompletePending`）
  ///
  /// 单次折叠三段：
  ///   1. 前缀外提：整批仅读一次会话 ns/db，消除逐键原子重读与 Varint 重算；
  ///   2. 窗口化纯内存批量直读：与引擎内部分块常量 [`PREFETCH_WINDOW`]
  ///      严格对齐的栈上物理键窗口，每窗口一次纪元进入 + 两级 L1 流水线预取，
  ///      热批全程零堆物化、零异步进入；
  ///   3. 冷候选单次批量收割：仅当本批出现内存未命中时，对全部冷键做一次
  ///      [`blocking_wait`]（引擎内并发下发磁盘读、按下标升序合流），替代原逐键
  ///      `blocking_wait` 的 N 次驱动进入。
  ///
  /// 内存批量直读把「明确不存在/墓碑」与「已落盘」统一报为未命中，故缺失键随
  /// 冷批重探一次索引（纯内存路径，无磁盘 I/O）；命中项严格按键流对下标各回调
  /// 一次，未命中不回调（对标 C# `VectorSessionFunctions` 仅 found 时回调）。
  fn read_multi<F>(&self, context: u64, keys: &[u8], _length_hint: usize, mut f: F)
  where
    F: FnMut(u32, &[u8]),
  {
    let prefix = self.session.session_prefix();
    let prefix = prefix.as_slice();
    let mut window = [const { TaggedKeyBuf::new() }; PREFETCH_WINDOW];
    // 冷批两列平行（下标 / 物理键）：Vec::new 零分配，纯热批全程不触堆
    let mut cold_idx: Vec<u32> = Vec::new();
    let mut cold_keys: Vec<TaggedKeyBuf> = Vec::new();

    let mut stream = LengthPrefixedIter::new(keys).enumerate();
    loop {
      let mut base = 0u32;
      let mut len = 0;
      while len < PREFETCH_WINDOW {
        let Some((idx, key)) = stream.next() else {
          break;
        };
        if len == 0 {
          base = idx as u32;
        }
        window[len] = StoreSession::<D>::vector_key_with_prefix(prefix, context, key);
        len += 1;
      }
      if len == 0 {
        break;
      }

      // 已交付位图：批量直读中途上抛时只把未交付项并入冷批，杜绝同下标二次回调
      let mut delivered: u32 = 0;
      let res = self
        .session
        .try_read_batch_raw_in_memory(&window[..len], |i, val| {
          delivered |= 1 << i;
          match val {
            Some(v) => f(base + i as u32, v),
            None => {
              cold_idx.push(base + i as u32);
              cold_keys.push(window[i].clone());
            }
          }
        });
      if res.is_err() {
        for (i, phys_key) in window.iter().enumerate().take(len) {
          if delivered & (1 << i) == 0 {
            cold_idx.push(base + i as u32);
            cold_keys.push(phys_key.clone());
          }
        }
      }
    }

    if cold_idx.is_empty() {
      // 对标 C# `hasPending == false` 时整段跳过 CompletePending
      return;
    }
    let _ = blocking_wait(self.session.read_batch_raw_with(&cold_keys, |i, val| {
      if let Some(v) = val {
        f(cold_idx[i], v);
      }
    }));
  }

  /// 单键尺寸未知读（对标 C# ReadSizeUnknown:427-474）
  fn read<F>(&self, context: u64, key: &[u8], mut f: F) -> bool
  where
    F: FnMut(&[u8]),
  {
    let phys_key = self.session.vector_key(context, key);
    match self.session.try_read_raw_in_memory(&phys_key, |val| f(val)) {
      Ok(StoreResult::Success(_)) => true,
      Ok(StoreResult::NotFound) => false,
      _ => {
        let mut called = false;
        let res = blocking_wait(self.session.read_raw_with(&phys_key, |val| {
          called = true;
          f(val);
        }));
        res.is_ok_and(|opt| opt.is_some()) && called
      }
    }
  }

  /// 写入（对标 C# WriteCallbackUnmanaged:365-383 的 Upsert + 挂起即收割）
  fn write(&self, context: u64, key: &[u8], value: &[u8]) -> bool {
    let phys_key = self.session.vector_key(context, key);
    match self.session.try_upsert_raw_sync(&phys_key, value) {
      Ok(Ok(_)) => true,
      _ => blocking_wait(self.session.upsert_raw(&phys_key, value)).is_ok(),
    }
  }

  /// 删除（对标 C# DeleteCallbackUnmanaged:385-396；C# 断言删除不挂起，
  /// rust 冷区键的墓碑追加仍需一次驱动收割）
  fn delete(&self, context: u64, key: &[u8]) -> bool {
    let phys_key = self.session.vector_key(context, key);
    blocking_wait(self.session.delete_raw(&phys_key)).unwrap_or(false)
  }

  /// 读改写（对标 C# ReadModifyWriteCallbackUnmanaged:398-419）
  fn rmw<F>(&self, context: u64, key: &[u8], write_len: usize, mut f: F) -> bool
  where
    F: FnMut(&mut [u8]),
  {
    // 纯读短路（C# 谓词 WriteDesiredSize == 0 判假）：缺失键 NeedInitialUpdate
    // 判假 → NOTFOUND、已存在键 NeedCopyUpdate 判假 → SUCCESS，均不进 updater、
    // 绝不写记录；两种状态 IsCompletedSuccessfully 皆为真，故应答恒成功
    if write_len == 0 {
      let _ = self.read(context, key, |_| {});
      return true;
    }

    let mut buf = vec![0u8; write_len];
    let _ = self.read(context, key, |curr| {
      let n = buf.len().min(curr.len());
      buf[..n].copy_from_slice(&curr[..n]);
    });
    f(&mut buf);
    self.write(context, key, &buf)
  }

  fn filter(&self, _context: u64, _internal_id: u32) -> bool {
    true
  }

  fn log(&self, _context: u64, msg: &str) {
    log::info!("{msg}");
  }
}
