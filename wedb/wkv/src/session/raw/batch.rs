//! 批量预取读取路径（对标 C# Tsavorite ContextReadWithPrefetch）

use futures_util::future::join_all;
use wdev::Device;
use windex::{CandidateAddresses, HashIndex, prefetch_read_l1};
use wval::{KeyTag, NamespaceDbCodec, TaggedKeyBuf};

use super::MemRead;
use crate::{
  error::{Error, Result},
  session::StoreSession,
};

/// 批量读取流水线预取窗口大小（对齐 C# Garnet 12 项硬件级流水线预取）
pub const BATCH_READ_PREFETCH_SIZE: usize = 12;

impl<D: Device> StoreSession<D> {
  /// 批量分块回调索引平移适配：把底层相对本块切片的 idx 平移 chunk 基址后转发，
  /// 维持"idx 相对调用方全量键列表"的外部契约（返回 impl FnMut 保持 HRTB 借用直通）
  fn offset_batch_idx<'f>(
    cb: &'f mut (impl FnMut(usize, Option<&[u8]>) + ?Sized),
    base: usize,
  ) -> impl FnMut(usize, Option<&[u8]>) + 'f {
    move |i, v| cb(base + i, v)
  }

  /// 批量读取当前会话普通字符串记录（严格对照 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Tsavorite.cs:ContextReadWithPrefetch 实现 12 项硬件流水线预取）
  ///
  /// 按预取窗口常量分块，`TaggedKeyBuf`（Copy）在单个栈上数组内编码后逐块调用底层批量读，
  /// 任意批量规模全程零堆物化（仅磁盘候选收割冷路径按需分配）。
  pub async fn read_batch_with<K, F>(&self, keys: &[K], mut on_item: F) -> Result<()>
  where
    K: AsRef<[u8]>,
    F: FnMut(usize, Option<&[u8]>),
  {
    let prefix = self.session_prefix();
    let prefix_slice = prefix.as_slice();
    let mut stack_keys = [const { TaggedKeyBuf::new() }; BATCH_READ_PREFETCH_SIZE];
    // 契约：on_item 的 idx 相对调用方全量键列表（底层按本次调用键切片编号），
    // 分块调用必须回填 chunk 基址，否则多块时 idx 重复从 0 起算
    for (chunk_ix, chunk) in keys.chunks(BATCH_READ_PREFETCH_SIZE).enumerate() {
      let chunk_base = chunk_ix * BATCH_READ_PREFETCH_SIZE;
      for (stack_k, k) in stack_keys.iter_mut().zip(chunk.iter()) {
        *stack_k =
          NamespaceDbCodec::encode_with_session_prefix(prefix_slice, KeyTag::String, k.as_ref());
      }
      let mut f = Self::offset_batch_idx(&mut on_item, chunk_base);
      self
        .read_batch_raw_with(&stack_keys[..chunk.len()], &mut f)
        .await?;
    }
    Ok(())
  }

  /// 纯内存批量直读当前会话普通字符串记录（零堆分配与零异步开销）
  pub fn try_read_batch_in_memory<K, F>(&self, keys: &[K], mut on_item: F) -> Result<()>
  where
    K: AsRef<[u8]>,
    F: FnMut(usize, Option<&[u8]>),
  {
    let prefix = self.session_prefix();
    let prefix_slice = prefix.as_slice();
    let mut stack_keys = [const { TaggedKeyBuf::new() }; BATCH_READ_PREFETCH_SIZE];
    for (chunk_ix, chunk) in keys.chunks(BATCH_READ_PREFETCH_SIZE).enumerate() {
      let chunk_base = chunk_ix * BATCH_READ_PREFETCH_SIZE;
      for (stack_k, k) in stack_keys.iter_mut().zip(chunk.iter()) {
        *stack_k =
          NamespaceDbCodec::encode_with_session_prefix(prefix_slice, KeyTag::String, k.as_ref());
      }
      let mut f = Self::offset_batch_idx(&mut on_item, chunk_base);
      self.try_read_batch_raw_in_memory(&stack_keys[..chunk.len()], &mut f)?;
    }
    Ok(())
  }

  /// 底层物理批量读取记录（Raw）
  ///
  /// # 回调次序契约
  /// `on_item` 严格按 idx 升序对每个键恰好各回调一次；任一磁盘候选收割失败时以
  /// `Err` 中止交付（首个磁盘候选之前的内存项已先行交付、无法回滚，调用方须整体
  /// 丢弃本批部分结果）。`read_batch_with` 分块串行推进，跨块亦保持全局升序。
  /// MGET 线上协议（`mget_each` 无 idx 参数、按回调序对位写响应）依赖此契约。
  pub async fn read_batch_raw_with<K, F>(&self, keys: &[K], mut on_item: F) -> Result<()>
  where
    K: AsRef<[u8]>,
    F: FnMut(usize, Option<&[u8]>),
  {
    let count = keys.len();
    if count == 0 {
      return Ok(());
    }

    let mut next_batch_ix = 0;
    // 批间复用同一缓冲区：仅首次命中磁盘候选时分配一次，多批次 MGET 场景消除反复分配
    let mut pending: Vec<(usize, CandidateAddresses)> = Vec::new();

    while next_batch_ix < count {
      let batch_len = (count - next_batch_ix).min(BATCH_READ_PREFETCH_SIZE);

      let guard = Some(self.participant.enter());
      let first_addrs = self.prefetch_batch_addrs(keys, next_batch_ix, batch_len);

      // 3. 内存快路径同步直读；磁盘候选统一收集后并发提交批量收割
      //    （对标 C# CompletePending：compio 完成制模型下首轮 poll 提交、单线程重叠多路磁盘 I/O）
      pending.clear();
      // 混批保序暂存：本批一旦出现磁盘候选，其后命中的内存结果必须拷贝暂存
      //（纪元守卫释放后页内借引用即刻失效），待磁盘收割完成后合流按 idx 升序交付；
      // Vec::new 零分配，纯内存批（绝大多数）全程零拷贝直通
      let mut buffered: Vec<(usize, Option<Vec<u8>>)> = Vec::new();

      for (i, &first_addr) in first_addrs[..batch_len].iter().enumerate() {
        let item_idx = next_batch_ix + i;
        let key = unsafe { keys.get_unchecked(item_idx) }.as_ref();

        let disk_mixed = !pending.is_empty();
        let mut buffered_val = None;
        let mut item_f = Some(|v: &[u8]| {
          if disk_mixed {
            // 混批：值切片仅在纪元守卫存活期有效，收割等待期前必须拷贝为所有权值
            buffered_val = Some(v.to_vec());
          } else {
            on_item(item_idx, Some(v));
          }
        });
        // RETRY_LATER：刷新纪元后整链重试（守卫存活期内，密封在途记录终将解封）
        let mut first = first_addr;
        let mut disk_cands = None;
        let mut mem_missing = false;
        loop {
          match self.try_read_mem(key, first, &mut item_f)? {
            MemRead::Done(Some(())) => break,
            MemRead::Done(None) => {
              mem_missing = true;
              break;
            }
            MemRead::OnDisk(cands) => {
              disk_cands = Some(cands);
              break;
            }
            MemRead::Retry => {
              self.participant.refresh();
              first = self.store.index.find_tag(key);
            }
          }
        }
        if let Some(cands) = disk_cands {
          pending.push((item_idx, cands));
        } else if mem_missing {
          if disk_mixed {
            buffered.push((item_idx, None));
          } else {
            on_item(item_idx, None);
          }
        }
        if let Some(v) = buffered_val {
          buffered.push((item_idx, Some(v)));
        }
      }

      // 磁盘读提交前一次性释放纪元守卫（收集候选为纯本地操作无需让出），
      // 防止长时间 I/O 阻塞纪元推进
      drop(guard);

      if !pending.is_empty() {
        // 并发收割全部磁盘读（冷路径物化 Vec，规避跨并发闭包共享 FnMut）
        let futs = pending.drain(..).map(|(item_idx, cands)| async move {
          let key = unsafe { keys.get_unchecked(item_idx) }.as_ref();
          let val = self.read_from_disk(key, cands, |v| v.to_vec()).await?;
          Ok::<_, Error>((item_idx, val))
        });
        // 任一磁盘读失败即刻上抛中止交付（首个磁盘候选之前的内存项已先行交付，
        // 调用方须整体丢弃本批部分结果，契约详见方法注释）
        let mut disk_vals: Vec<(usize, Option<Vec<u8>>)> = Vec::with_capacity(batch_len);
        for res in join_all(futs).await {
          disk_vals.push(res?);
        }

        // 归并交付：磁盘候选之后的内存命中暂存与磁盘收割结果合流，
        // 按 idx 升序统一回调（严格对照 Redis MGET：结果顺序恒等于请求顺序、
        // 缺失键为 nil；MGET 线上协议按回调序对位写响应，故必须严格升序），
        // 合流规模上界 2×预取窗口，排序成本在磁盘 I/O 冷路径下可忽略
        disk_vals.extend(buffered);
        disk_vals.sort_unstable_by_key(|&(idx, _)| idx);
        for (item_idx, val) in disk_vals {
          on_item(item_idx, val.as_deref());
        }
      }

      next_batch_ix += batch_len;
    }

    Ok(())
  }

  /// 底层物理同步纯内存批量直读（Raw，严格对照 Tsavorite ContextReadWithPrefetch 12 项流水线预取）
  ///
  /// - 适用于纯内存驻留读场景或快速内存筛选；
  /// - 若记录处于内存中且存在，调用 `on_item(idx, Some(val))`；
  /// - 若记录不存在、为墓碑或已落盘驱逐，调用 `on_item(idx, None)`；
  /// - 全程纯同步调用栈执行，无任何堆分配与异步开销。
  pub fn try_read_batch_raw_in_memory<K, F>(&self, keys: &[K], mut on_item: F) -> Result<()>
  where
    K: AsRef<[u8]>,
    F: FnMut(usize, Option<&[u8]>),
  {
    let count = keys.len();
    if count == 0 {
      return Ok(());
    }

    let _guard = self.participant.enter();

    let mut next_batch_ix = 0;
    while next_batch_ix < count {
      let batch_len = (count - next_batch_ix).min(BATCH_READ_PREFETCH_SIZE);
      let first_addrs = self.prefetch_batch_addrs(keys, next_batch_ix, batch_len);

      // 3. 执行底层物理同步内存读取
      for (i, &first_addr) in first_addrs[..batch_len].iter().enumerate() {
        let item_idx = next_batch_ix + i;
        let key = unsafe { keys.get_unchecked(item_idx) }.as_ref();

        match self
          .try_read_raw_in_memory_with_addr(key, first_addr, |v| on_item(item_idx, Some(v)))?
        {
          Some(Some(())) => {}
          _ => on_item(item_idx, None),
        }
      }

      next_batch_ix += batch_len;
    }

    Ok(())
  }

  /// 批量读单批两级硬件预取（异步/同步批量读共用，严格对照 Tsavorite ContextReadWithPrefetch 两级预取）
  ///
  /// 1. 第一级：计算 64 位哈希并将对应哈希桶（64 字节 cacheline）拉入 CPU L1 数据缓存；
  /// 2. 第二级：探测 FindTag 首地址，若驻留内存有效区间 `[head, tail)` 则预取记录物理内存。
  ///
  /// 返回首地址探针数组（有效长度 `batch_len`，调用方须已处于纪元保护下）。
  #[inline]
  fn prefetch_batch_addrs<K>(
    &self,
    keys: &[K],
    base_ix: usize,
    batch_len: usize,
  ) -> [Option<u64>; BATCH_READ_PREFETCH_SIZE]
  where
    K: AsRef<[u8]>,
  {
    let mut hashes = [0u64; BATCH_READ_PREFETCH_SIZE];
    let mut first_addrs = [None; BATCH_READ_PREFETCH_SIZE];
    let mask = self.store.index.mask;

    // 1. 第一级硬件预取：哈希桶 cacheline
    for (i, hash_slot) in hashes[..batch_len].iter_mut().enumerate() {
      let key = unsafe { keys.get_unchecked(base_ix + i) }.as_ref();
      let hash = HashIndex::hash_key(key);
      *hash_slot = hash;
      let bucket_idx = (hash as usize) & mask;
      let bucket_ptr = unsafe { self.store.index.buckets.as_ptr().add(bucket_idx) };
      prefetch_read_l1(bucket_ptr);
    }

    // 2. 第二级硬件预取：记录物理地址（仅内存驻留区间内的地址有意义）
    let head_addr = self.store.head_address();
    let tail_addr = self.store.hlog.tail_address();
    for (i, addr_slot) in first_addrs[..batch_len].iter_mut().enumerate() {
      let addr_opt = self.store.index.find_tag_by_hash(hashes[i]);
      *addr_slot = addr_opt;
      if let Some(addr) = addr_opt
        && addr >= head_addr
        && addr < tail_addr
      {
        // SAFETY: addr ∈ [head, tail) 必然驻留内存，且调用方持纪元守卫保证页不被回收
        let phys_ptr = unsafe { self.store.hlog.get_physical_address(addr) };
        prefetch_read_l1(phys_ptr);
      }
    }

    first_addrs
  }
}
