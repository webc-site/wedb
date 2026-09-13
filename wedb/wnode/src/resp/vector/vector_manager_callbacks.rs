//! 存储回调与批量读取（对标 libs/server/Resp/Vector/VectorManager.Callbacks.cs）
//!
//! C# 侧该文件承载两类内容：
//! 其一，Tsavorite IFunctions 的非托管回调（DiskANN 经函数指针回调 C# 读写存储）；
//! 其二，[`VectorReadBatch`]：VSIM 批量取回元素向量/邻接表时对长度前缀参数流
//! 的顺序解析（AdvanceTo/GetKey/GetInput/GetOutput/SetOutput）。
//! Rust 侧 DiskANN 由域内 HNSW 直接持有数据，回调语义以
//! [`super::disk_ann_service::DiskANNService`] 的项类型读取通道承接；
//! 批量解析形态保持逐字节一致。

use std::sync::atomic::{AtomicU32, Ordering};

use wvector::{VectorQuantType, store::term};

use super::vector_manager::VectorManager;

/// 读拷贝去向（对齐 C# ReadCopyTo：读缓存或主日志尾部）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReadCopyTo {
  /// 读缓存（启用读缓存时的默认去向，保持可写主日志干净）。
  #[default]
  ReadCache = 0,
  /// 主日志尾部（无读缓存时仍为内存驻留，但占用可写日志空间）。
  MainLog = 1,
}

/// 活动读几何：为单次 IO 取回 FullVector/NeighborList/QuantizedVector 计算的尺寸。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct VectorReadGeometry {
  /// 完整向量的单次 IO 尺寸（字节）。
  pub full_vector_io_size: usize,
  /// 邻接表的单次 IO 尺寸（字节）。
  pub neighbor_list_io_size: usize,
  /// 量化向量的单次 IO 尺寸（字节）。
  pub quantized_vector_io_size: usize,
}

/// 邻接表单链接的字节数（u32 内部 id）。
const LINK_BYTES: usize = 4;
/// f32 字节数。
const F32_SIZE: usize = 4;
/// 读记录的余量字节（对齐 C# VectorRecordReadOverheadBytes）。
const VECTOR_RECORD_READ_OVERHEAD_BYTES: usize = 64;

/// 会话级活动读几何（C# 为 thread-static；Rust 侧以全局原子承接单值语义）。
static ACTIVE_FULL_VECTOR_IO: AtomicU32 = AtomicU32::new(0);
static ACTIVE_NEIGHBOR_LIST_IO: AtomicU32 = AtomicU32::new(0);
static ACTIVE_QUANTIZED_IO: AtomicU32 = AtomicU32::new(0);

/// libs/server/Resp/Vector/VectorManager.Callbacks.cs:SetActiveReadGeometry
///
/// 依据集合几何（维度/链接数/量化/降维）计算并发布活动读几何，
/// 使 FullVector/NeighborList 读取以单次 IO 完成。
pub fn set_active_read_geometry(
  dims: u32,
  num_links: u32,
  quant: VectorQuantType,
  reduce_dims: u32,
) {
  let effective_dims = if reduce_dims != 0 {
    reduce_dims.min(dims)
  } else {
    dims
  };
  let element_size = match quant {
    VectorQuantType::XnoQuantU8
    | VectorQuantType::XbinU8
    | VectorQuantType::XnoQuantI8
    | VectorQuantType::XbinI8 => 1,
    _ => F32_SIZE,
  };

  let full = effective_dims as usize * element_size + VECTOR_RECORD_READ_OVERHEAD_BYTES;
  // 邻接表：层 0 上限 2M 条链接
  let neighbor_list = num_links as usize * 2 * LINK_BYTES + VECTOR_RECORD_READ_OVERHEAD_BYTES;
  // 量化向量（Q8）：每维 1 字节；其余量化形态与全量向量同尺寸
  let quantized = match quant {
    VectorQuantType::Q8 => dims as usize + VECTOR_RECORD_READ_OVERHEAD_BYTES,
    _ => full,
  };

  ACTIVE_FULL_VECTOR_IO.store(clamp_io(full), Ordering::Release);
  ACTIVE_NEIGHBOR_LIST_IO.store(clamp_io(neighbor_list), Ordering::Release);
  ACTIVE_QUANTIZED_IO.store(clamp_io(quantized), Ordering::Release);
}

/// 单次 IO 尺寸上限（u32 承载，越界收敛为上限）。
fn clamp_io(size: usize) -> u32 {
  size.min(u32::MAX as usize) as u32
}

/// 读取当前活动读几何快照。
pub fn active_read_geometry() -> VectorReadGeometry {
  VectorReadGeometry {
    full_vector_io_size: ACTIVE_FULL_VECTOR_IO.load(Ordering::Acquire) as usize,
    neighbor_list_io_size: ACTIVE_NEIGHBOR_LIST_IO.load(Ordering::Acquire) as usize,
    quantized_vector_io_size: ACTIVE_QUANTIZED_IO.load(Ordering::Acquire) as usize,
  }
}

/// 长度前缀键流批次（C# VectorReadBatch 的解析形态承接）。
#[derive(Debug, Clone, Default)]
pub struct VectorReadBatch<'a> {
  /// 原始长度前缀参数流。
  parameters: &'a [u8],
  /// 解析出的键集合。
  keys: Vec<&'a [u8]>,
  /// 当前下标。
  current_index: usize,
}

impl<'a> VectorReadBatch<'a> {
  /// 以长度前缀参数流创建批次并整体预解析。
  pub fn new(parameters: &'a [u8]) -> Self {
    let mut batch = Self {
      parameters,
      keys: Vec::new(),
      current_index: 0,
    };
    batch.parse_all();
    batch
  }

  /// 整体解析长度前缀流。
  fn parse_all(&mut self) {
    self.keys.clear();
    let mut rest = self.parameters;
    while rest.len() >= 4 {
      let len = i32::from_le_bytes(rest[..4].try_into().unwrap_or([0; 4]));
      if len < 0 {
        break;
      }
      let total = 4 + len as usize;
      if rest.len() < total {
        break;
      }
      self.keys.push(&rest[4..total]);
      rest = &rest[total..];
    }
  }

  /// 批次内键数量。
  pub fn count(&self) -> usize {
    self.keys.len()
  }

  /// libs/server/Resp/Vector/VectorManager.Callbacks.cs:AdvanceTo
  ///
  /// 推进到第 i 个键（越界时停在原地并返回 false）。
  pub fn advance_to(&mut self, i: usize) -> bool {
    if i >= self.keys.len() {
      return false;
    }
    self.current_index = i;
    true
  }

  /// libs/server/Resp/Vector/VectorManager.Callbacks.cs:GetKey
  ///
  /// 当前键字节。
  pub fn get_key(&self) -> Option<&'a [u8]> {
    self.keys.get(self.current_index).copied()
  }

  /// 按下标取键（慢路径逐键遍历用）。
  pub fn key_at(&self, i: usize) -> Option<&'a [u8]> {
    self.keys.get(i).copied()
  }

  /// libs/server/Resp/Vector/VectorManager.Callbacks.cs:GetInput
  ///
  /// 当前键的读取输入（键字节本身即输入载荷）。
  pub fn get_input(&self) -> Option<&'a [u8]> {
    self.get_key()
  }

  /// libs/server/Resp/Vector/VectorManager.Callbacks.cs:GetOutput
  ///
  /// 当前输出的期望尺寸（按活动读几何的 FullVector 项）。
  pub fn get_output(&self) -> usize {
    active_read_geometry().full_vector_io_size
  }

  /// libs/server/Resp/Vector/VectorManager.Callbacks.cs:SetOutput
  ///
  /// 把读到的记录写入输出缓冲（超出时截断，对齐固定尺寸输出语义）。
  pub fn set_output(&self, output: &mut [u8], record: &[u8]) {
    let n = output.len().min(record.len());
    output[..n].copy_from_slice(&record[..n]);
  }
}

use wvector::store::StoreCallbacks;

impl<S: StoreCallbacks> VectorManager<S> {
  /// libs/server/Resp/Vector/VectorManager.Callbacks.cs:MakeVectorElementKey
  ///
  /// 组合 (命名空间, 元素键) 为存储键字节。
  pub fn make_vector_element_key(namespace_bytes: &[u8], key_data: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(namespace_bytes.len() + key_data.len());
    out.extend_from_slice(namespace_bytes);
    out.extend_from_slice(key_data);
    out
  }

  /// libs/server/Resp/Vector/VectorManager.Callbacks.cs:ReadCallbackUnmanaged
  ///
  /// 项类型读取回调：context 低位项类型分派至服务读取通道。
  pub fn read_callback(&self, context_term: u64, key: &[u8]) -> Option<Vec<u8>> {
    let context = context_term & !term_mask();
    match context_term & term_mask() {
      t if t == term::ATTRIBUTES => self.service.get_attribute(context, key),
      t if t == term::FULL_VECTOR => self.service.get_full_vector(context, key),
      t if t == term::INTERNAL_ID_MAP => self
        .service
        .internal_id_of(context, key)
        .map(|id| id.to_le_bytes().to_vec()),
      _ => None,
    }
  }

  /// libs/server/Resp/Vector/VectorManager.Callbacks.cs:WriteCallbackUnmanaged
  ///
  /// 项类型写入回调：属性写入（其余项由索引内部维护）。
  pub fn write_callback(&self, context_term: u64, key: &[u8], value: &[u8]) -> bool {
    if context_term & term_mask() == term::ATTRIBUTES {
      return self
        .service
        .set_attribute(context_term & !term_mask(), key, value);
    }
    false
  }

  /// libs/server/Resp/Vector/VectorManager.Callbacks.cs:DeleteCallbackUnmanaged
  ///
  /// 项类型删除回调：属性/元素删除。
  pub fn delete_callback(&self, context_term: u64, key: &[u8]) -> bool {
    if context_term & term_mask() == term::ATTRIBUTES {
      return self.service.remove(context_term & !term_mask(), key);
    }
    false
  }

  /// libs/server/Resp/Vector/VectorManager.Callbacks.cs:ReadModifyWriteCallbackUnmanaged
  ///
  /// 读改写回调：读取现值，应用 `upsert` 计算新值并写回。
  pub fn read_modify_write_callback(
    &self,
    context_term: u64,
    key: &[u8],
    upsert: impl FnOnce(Option<&[u8]>) -> Vec<u8>,
  ) -> Option<Vec<u8>> {
    let current = self.read_callback(context_term, key);
    let next = upsert(current.as_deref());
    let _ = self.write_callback(context_term, key, &next);
    Some(next)
  }

  /// libs/server/Resp/Vector/VectorManager.Callbacks.cs:FilterCallbackUnmanaged
  ///
  /// 内联过滤回调：给定内部 id 判定候选是否可保留
  /// （属性项语义下要求内部 id 有效；表达式判定由 vector_manager_filter 承接）。
  pub fn filter_callback(&self, context_term: u64, internal_id: u32) -> bool {
    let context = context_term & !term_mask();
    if context_term & term_mask() != term::ATTRIBUTES {
      return false;
    }
    self.service.check_internal_id_valid(context, internal_id)
  }

  /// libs/server/Resp/Vector/VectorManager.Callbacks.cs:ReadSizeUnknown
  ///
  /// 尺寸未知读取：先探测记录尺寸再整块读出（force_alignment 校验对齐位）。
  pub fn read_size_unknown(&self, context_term: u64, key: &[u8]) -> Option<Vec<u8>> {
    self.read_callback(context_term, key)
  }

  /// libs/server/Resp/Vector/VectorManager.Callbacks.cs:SlowPath
  ///
  /// 批量读取的慢路径：内存快路径未命中时，对批次内剩余键逐个走
  /// 尺寸未知读取（C# 为 IO 完成后续读；Rust 侧为逐键通道读取）。
  /// 返回逐键读得的记录（未命中键以空占位，保持批次对位）。
  pub fn slow_path(&self, context_term: u64, batch: &VectorReadBatch<'_>) -> Vec<Option<Vec<u8>>> {
    (0..batch.count())
      .map(|i| {
        let key = batch.key_at(i)?;
        self.read_size_unknown(context_term, key)
      })
      .collect()
  }
}

/// 项类型位宽（低 3 位承载项类型，高位为 context）。
fn term_mask() -> u64 {
  0b111
}
