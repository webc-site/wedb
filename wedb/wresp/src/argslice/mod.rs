//! 参数切片与容器（对标 Garnet `libs/server/ArgSlice`）

pub mod arg_slice;
pub mod arg_slice_vector;

pub use arg_slice::ArgSlice;
pub use arg_slice_vector::{ArgSliceVector, DEFAULT_MAX_ITEM_NUM};
