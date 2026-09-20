import re

with open("wedb/wkv/src/session/raw/read.rs", "r") as f:
    content = f.read()

# Replace use statement
content = content.replace(
    "use super::{MemDrive, ReadProbeResult};",
    "use super::MemDrive;\nuse std::ops::ControlFlow;"
)

# Remove old enums and map_rc_visit
pattern_remove = re.compile(
    r"/// 内存直读内部结果.*?\n"
    r"enum MemRead<R> \{.*?\n"
    r"\}\n\n"
    r"/// ReadCache 整链走查结果.*?\n"
    r"enum RcWalk<R> \{.*?\n"
    r"\}\n\n"
    r"/// \[`RcVisit`\].*?fn map_rc_visit.*?\}\n\n"
    r"/// 内存反向回溯结果.*?\n"
    r"enum MemBack<R> \{.*?\n"
    r"\}\n\n",
    re.DOTALL
)
content = pattern_remove.sub("", content)

# Add MemAction
memaction = """
/// 内存扫描中间动作（统一替代原有的 RcWalk/MemBack/ReadProbeResult/MemRead）
///
/// 精简状态机：合并了所有内存段（ReadCache、Immutable、Mutable）内探针与回溯的
/// 动作语义，消除过多胶水转换代码。
enum MemAction<R> {
  /// 匹配成功并提取值（对应 SUCCESS / Found）
  Done(Option<R>),
  /// 需要刷新纪元并重试（对应 RETRY_LATER）
  Retry,
  /// 续链下一地址，或 0 表示终止（对应 Miss(prev) / Stopped）
  Next(u64),
}

"""

# find 'pub enum StoreResult<T>' and prepend memaction
content = content.replace("/// 同步内存直读权威状态枚举", memaction + "/// 同步内存直读权威状态枚举")

def replace_func(content, func_name, new_code):
    pattern = re.compile(rf"([ \t]*)(?:#\[.*?\]\n[ \t]*)*(?:pub(?:\(super\))? )?(?:async )?fn {func_name}.*?\n\1}}\n", re.DOTALL)
    if not pattern.search(content):
        print(f"Warning: func {func_name} not found!")
    return pattern.sub(new_code + "\n", content)


probe_code = """  #[inline]
  fn probe_hlog_record<R, F: RecordRead<R>>(
    rec: wrecord::RecordRef<'_>,
    key: &[u8],
    f: &mut Option<F>,
  ) -> MemAction<R> {
    if rec.matches_key(key) {
      if rec.is_closed() {
        MemAction::Retry
      } else if rec.is_tombstone() {
        MemAction::Done(None)
      } else {
        let func = unsafe { f.take().unwrap_unchecked() };
        MemAction::Done(Some(func.read_record(rec.value(), rec.physical_size())))
      }
    } else {
      MemAction::Next(rec.prev_address())
    }
  }"""
# Note: probe_hlog_record is not a method of StoreSession, it's a standalone fn
content = re.sub(r"(#\[inline\]\n)?fn probe_hlog_record<R, F: RecordRead<R>>.*?\}\n", probe_code.replace("  #[inline]", "#[inline]") + "\n", content, flags=re.DOTALL)


drive_mem_read_code = """  #[inline]
  pub(super) fn drive_mem_read<R>(
    &self,
    key: &[u8],
    hash: u64,
    mut first_addr: Option<u64>,
    f: &mut Option<impl RecordRead<R>>,
  ) -> Result<MemDrive<R>> {
    loop {
      if let ControlFlow::Break(res) = self.try_read_mem(key, hash, first_addr, f)? {
        return Ok(res);
      }
      self.participant.refresh();
      first_addr = self.reprobe_first_addr(hash);
    }
  }"""
content = replace_func(content, "drive_mem_read", drive_mem_read_code)

find_in_rc_code = """  #[inline]
  fn find_in_read_cache<R>(
    &self,
    key: &[u8],
    curr: &mut u64,
    f: &mut Option<impl RecordRead<R>>,
  ) -> MemAction<R> {
    while is_read_cache(*curr) {
      if self
        .store
        .read_cache
        .need_to_wait_for_eviction(*curr, || self.participant.refresh())
      {
        return MemAction::Retry;
      }
      let visit = self
        .store
        .read_cache
        .with_record(*curr, |rec_key, rec_val| {
          if fast_key_eq(rec_key, key) {
            let func = unsafe { f.take().unwrap_unchecked() };
            Some(func.read_record(rec_val, record_size(rec_key.len(), rec_val.len())))
          } else {
            None
          }
        });
      match visit {
        RcVisit::Found(val) => return MemAction::Done(Some(val)),
        RcVisit::Next(prev) => *curr = prev,
        RcVisit::Gone => return MemAction::Retry,
      }
      if *curr == 0 {
        break;
      }
    }
    MemAction::Next(*curr)
  }"""
content = replace_func(content, "find_in_read_cache", find_in_rc_code)

trace_back_code = """  #[inline]
  fn trace_back_for_key_match<R>(
    &self,
    key: &[u8],
    curr: &mut u64,
    head_addr: u64,
    ro_addr: u64,
    safe_ro_addr: u64,
    f: &mut Option<impl RecordRead<R>>,
  ) -> Result<MemAction<R>> {
    while *curr >= head_addr {
      let probed = if *curr < ro_addr {
        Some(unsafe {
          self
            .store
            .hlog
            .with_immutable_record(*curr, |rec| Ok(probe_hlog_record(rec, key, f)))?
        })
      } else {
        self
          .store
          .hlog
          .with_memory_record(*curr, |rec| Ok(probe_hlog_record(rec, key, f)))?
      };
      match probed {
        Some(MemAction::Done(Some(val))) => {
          if *curr < safe_ro_addr {
            self.promote_immutable_read_hit(*curr, key);
          }
          return Ok(MemAction::Done(Some(val)));
        }
        Some(MemAction::Done(None)) => return Ok(MemAction::Done(None)),
        Some(MemAction::Retry) => return Ok(MemAction::Retry),
        Some(MemAction::Next(next)) => {
          *curr = next;
          if next == 0 {
            return Ok(MemAction::Next(0));
          }
        }
        None => return Ok(MemAction::Next(*curr)),
      }
    }
    Ok(MemAction::Next(*curr))
  }"""
content = replace_func(content, "trace_back_for_key_match", trace_back_code)

try_read_mem_code = """  #[inline]
  fn try_read_mem<R>(
    &self,
    key: &[u8],
    hash: u64,
    first_addr: Option<u64>,
    f: &mut Option<impl RecordRead<R>>,
  ) -> Result<ControlFlow<MemDrive<R>, ()>> {
    let mut curr_addr = first_addr;
    if self.store.is_growing() {
      self.store.split_buckets(hash)?;
      curr_addr = self.reprobe_first_addr(hash);
    }
    let Some(mut curr_addr) = curr_addr else {
      return Ok(ControlFlow::Break(MemDrive::Done(None)));
    };

    let index = self.store.index.load();
    let _s_latch = if self.ephemeral_lock_enabled() {
      let bucket = index.bucket(index.bucket_index_for_hash(hash));
      let Some(latch) = bucket.lock_shared_guard() else {
        return Ok(ControlFlow::Continue(()));
      };
      let tag = HashBucketEntry::tag_from_hash(hash);
      if bucket.find_entry_by_address(tag, curr_addr).is_none()
        && let Some(hei) = index.find_tag_entry_by_hash_with_min_addr(hash, 0)
      {
        curr_addr = hei.address();
      }
      Some(latch)
    } else {
      None
    };

    let head_addr = self.store.head_address();
    let begin_addr = self.store.begin_address();
    let ro_addr = self.store.hlog.read_only_address();
    let safe_ro_addr = self.store.safe_read_only_address();

    match self.find_in_read_cache(key, &mut curr_addr, f) {
      MemAction::Done(res) => return Ok(ControlFlow::Break(MemDrive::Done(res))),
      MemAction::Retry => return Ok(ControlFlow::Continue(())),
      MemAction::Next(_) => {}
    }

    if !is_read_cache(curr_addr) && curr_addr >= head_addr {
      match self.trace_back_for_key_match(
        key,
        &mut curr_addr,
        head_addr,
        ro_addr,
        safe_ro_addr,
        f,
      )? {
        MemAction::Done(res) => return Ok(ControlFlow::Break(MemDrive::Done(res))),
        MemAction::Retry => return Ok(ControlFlow::Continue(())),
        MemAction::Next(_) => {}
      }
    }

    if curr_addr == 0 || (!is_read_cache(curr_addr) && curr_addr < begin_addr) {
      return Ok(ControlFlow::Break(MemDrive::Done(None)));
    }

    self.try_read_mem_fallback(
      key,
      hash,
      f,
      ReadMemBounds {
        begin_addr,
        head_addr,
        safe_ro_addr,
        chain_disk_addr: curr_addr,
      },
    )
  }"""
content = replace_func(content, "try_read_mem", try_read_mem_code)


try_read_mem_fallback_code = """  #[cold]
  fn try_read_mem_fallback<R>(
    &self,
    key: &[u8],
    hash: u64,
    f: &mut Option<impl RecordRead<R>>,
    bounds: ReadMemBounds,
  ) -> Result<ControlFlow<MemDrive<R>, ()>> {
    let mut addrs = self.store.index.load().lookup_candidates_by_hash(hash);
    if addrs.is_empty() {
      if bounds.chain_disk_addr != 0 && bounds.chain_disk_addr >= bounds.begin_addr {
        let mut disk = CandidateAddresses::new();
        disk.push(bounds.chain_disk_addr);
        return Ok(ControlFlow::Break(MemDrive::OnDisk(disk)));
      }
      return Ok(ControlFlow::Break(MemDrive::Done(None)));
    }
    addrs.sort_descending();

    let ro_addr = self.store.hlog.read_only_address();
    let mut disk_cands = CandidateAddresses::new();

    for &addr in addrs.iter() {
      let mut cur_addr = addr;
      match self.find_in_read_cache(key, &mut cur_addr, f) {
        MemAction::Done(res) => return Ok(ControlFlow::Break(MemDrive::Done(res))),
        MemAction::Retry => return Ok(ControlFlow::Continue(())),
        MemAction::Next(_) => {}
      }
      if cur_addr == 0 || cur_addr < bounds.begin_addr {
        continue;
      }

      match self.trace_back_for_key_match(
        key,
        &mut cur_addr,
        bounds.head_addr,
        ro_addr,
        bounds.safe_ro_addr,
        f,
      )? {
        MemAction::Done(res) => return Ok(ControlFlow::Break(MemDrive::Done(res))),
        MemAction::Retry => return Ok(ControlFlow::Continue(())),
        MemAction::Next(_) => {}
      }

      if cur_addr != 0 && cur_addr >= bounds.begin_addr {
        disk_cands.push(cur_addr);
      }
    }

    if disk_cands.is_empty() {
      Ok(ControlFlow::Break(MemDrive::Done(None)))
    } else {
      Ok(ControlFlow::Break(MemDrive::OnDisk(disk_cands)))
    }
  }"""
content = replace_func(content, "try_read_mem_fallback", try_read_mem_fallback_code)


with open("wedb/wkv/src/session/raw/read.rs", "w") as f:
    f.write(content)
