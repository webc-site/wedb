acl_store.rs 私有 scan_err 复抄收敛到 array_key_iteration_functions 单点

来源：next/acl-store-scan-err-dedup.md（低优先级，去重条）。取证基线：主仓绝对根
/Users/z/git/db/wedb，分支 dev。

核实
票面成立。/Users/z/git/db/wedb/wedb/wnode/src/resp/acl_store.rs:65 私有 fn scan_err
（签名 `fn scan_err(e: impl fmt::Display) -> wkv::Error`，体 `WkvError::Io(io::Error::other(e.to_string()))`）
与 /Users/z/git/db/wedb/wedb/wnode/src/storage/session/common/array_key_iteration_functions.rs:35
的 pub(crate) fn scan_err 同签名同体逐字复抄（WkvError 即 wkv::Error 的导入别名，二者仅
import 命名不同）。acl_store 侧注释自认「同口径」却二次定义而非 use。模块链
storage/mod.rs:11、session/mod.rs:1、common/mod.rs:1 全程 pub，pub(crate) 对 wnode 全 crate
可达，单点消费为同文件 :244、:332、:372、:451、:496 五处，acl_store 消费 :195 一处。C# 无
对位（rust crate 内错误包装工具，属一处定义原则面，不涉及 garnet 转写偏差）。

修法
删 acl_store.rs 的私有 scan_err 定义，文件头改
use crate::storage::session::common::array_key_iteration_functions::scan_err;
随之清理仅该函数使用的 std fmt、io 与 wkv::Error as WkvError 导入。单点保留在
array_key_iteration_functions（消费多、且为 C# ArrayKeyIterationFunctions.cs 对位文件）。
:195 调用点零改动。block_on 复抄属 task/ing/block-on-single-source.md 射程，本单不碰。

验证
cargo check 全 workspace 零错误零警告。js/check.js 视角下重复定义消一。
