//! 字符串族 / OBJECT / 位图族慢路径执行段（快路径 `Ok(false)` 降级承接）
//!
//! 对标 C# BasicCommands.cs / BitmapCommands.cs 同步函数体内 CompletePending
//! 就地闭环的应答形态：快路径在环形页翻转 / RI 门 / 存在性探针降级后，本
//! 模块以同一套参数解析单源（快侧纯函数）+ 存储会话异步口重放整条命令，
//! 产出与快路径逐字节一致的应答。参数推导一律转调快侧同一纯函数，
//! 不在本模块重写第二套推导。
//!
//! 目录化拆分：[`common`] 公共壳（arity / 出帧共同体 / 冷读折叠 / 写内核）、
//! `string_slow` 字符串族分派入口、[`set_conditional`] SET 条件写共同体、
//! `bitmap_slow` 位图族分派入口与 BITOP、[`object_slow`] OBJECT 四子命令。

mod bitmap_slow;
mod common;
mod object_slow;
mod set_conditional;
mod string_slow;

pub(crate) use bitmap_slow::bitmap_slow;
pub(crate) use common::{apply_set_with_expiry_async, clear_vector_registry};
pub(crate) use object_slow::{ENCODING_RAW, encoding_of_envelope_payload, encoding_of_object_type};
pub(crate) use string_slow::string_slow;
