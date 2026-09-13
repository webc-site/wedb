//! Wedb 向量引擎模块（对标 Garnet C# DiskANN 向量接入层）
//!
//! 深度对接微软开源 Rust `diskann` 体系：
//! - [`provider`]：`WedbProvider` 桥接 `diskann-providers` 与存储底座
//! - [`service`]：`DiskANNService` 索引管理器与并发会话注册表
//! - [`quantization`]：Q8 标量量化与 Bin 1-bit 球面量化
//! - [`fsm`]：空闲空间映射（内部 ID 位图生命周期管理）
//! - [`store`]：存储回调注入与命名空间位编排
//! - [`filter`]：向量属性内联过滤与表达式编译器
//! - [`element_data`]：向量元素格式归一化与对齐
//! - [`types`]：距离度量、量化类型与标志位

pub mod element_data;
pub mod filter;
pub mod fsm;
pub mod provider;
pub mod quantization;
pub mod service;
pub mod store;
pub mod types;

pub use element_data::{PrepareError, PreparedVectorData, native_format, prepare_vector_data};
pub use filter::{
  CompileError, ExprProgram, ExprToken, ExprTokenType, OpCode, run as evaluate_filter, try_compile,
};
pub use fsm::{FreeSpaceMap, FsmError, ReuseGuard};
pub use provider::{
  DistanceComputer, QueryComputer, ToDistanceComputer, WedbProvider, WedbProviderError,
};
pub use quantization::{
  MinMax8Bit, QuantizerError, RawDistanceComputer, RawQueryComputer, Spherical1Bit, WedbQuantizer,
};
pub use service::{
  DiskANNService, DiskAnnInsertResult, Index, IndexConfig, SearchHit, SearchOutput, SearchParams,
  SearchResults,
};
pub use store::{Callbacks, Context, StoreCallbacks, StoreError, Term, VectorSetId};
pub use types::{
  VectorDistanceMetricType, VectorIdFormat, VectorQuantType, VectorSetFlags, VectorValueType,
};
