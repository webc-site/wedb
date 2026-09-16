# 项目依赖拓扑

wedb_standalone: wconf, wlua, wnode, wacl, waof, wbase, wbftree, wbitmap, wcol, wcpr, wdatabase, wdev, wedb_test, whasher, wkv, wmetric, wnode_test, wpubsub, wresp, wtxn, wval, wvector
wedb: waof, wbase, wconf, wconn, wdatabase, wdev, wkv, wlua, wmetric, wnode, wresp, wcustom, wedb_test, wnode_test, wtxn, wval
wnode: wacl, waof, wbase, wbftree, wbitmap, wcol, wconf, wcustom, wdatabase, wdev, wext_roaring, whyperlog, wkv, wlua, wmetric, wpubsub, wresources, wresp, wtxn, wval, wvector, wedb_test, wnode_test
wtxn: wbase
wresp: wbase, wresources
wacl: wbase, wresp
waof: wbase, wdev, wedb_test
wkv: wbase, wbftree, wcompact, wcpr, wdev, wepoch, whasher, whlog, windex, wrecord, wreviv, wval, wedb_test
wbftree: wbase, wdev, wepoch, whasher, wedb_test
wcompact: wbase, wdev, wepoch, whasher, whlog, windex, wrecord, wedb_test
wdatabase: waof, wbase, wcpr, wdev, wkv, wtxn, wval
wlua: wbase, wresp, wtxn, wedb_test

[清理死代码] 移除 wedb_standalone
c#: garnet/main/GarnetServer/Program.cs:Program.Main(args)
rust: wedb/wedb_standalone/Cargo.toml:1 (N/A)
现状: wedb_standalone 与 wedb 核心二进制包功能高度重合
方案: 删除 wedb_standalone 目录，将单机和集群模式统一在 wedb 这一入口下，通过参数或 Cargo feature 静态控制

[清理死代码] 移除无用的解析扩展方法
c#: garnet/libs/server/SessionParseStateExtensions.cs:SessionParseStateExtensions.TryGetInfoMetricsType(parseState, out type)
rust: wedb/wnode/src/session_parse_state_extensions.rs:97 (try_get_info_metrics_type)
现状: try_get_info_metrics_type 等大量方法仅在测试中被调用，未被实际命令实现引用
方案: 扫描该文件，删除所有死代码（如 try_get_info_metrics_type、try_get_latency_metrics_type、try_get_client_type 等），只保留被 wnode 内部或下游真实使用的解析方法

[去重合并] 合并集群槽位校验
c#: garnet/libs/cluster/Session/SlotVerification/RespClusterIterativeSlotVerify.cs:RespClusterIterativeSlotVerify.NetworkIterativeSlotVerify(key, readOnly, sessionAsking)
rust: wedb/wtxn/src/txn_slot_verify.rs:15 (TxnSlotVerifyFace)
现状: 槽位校验的三件套接口在 wtxn 和 wnode 被重复定义，且散落大量重复的 C# 映射注释
方案: 在 wbase/src/cluster/slot_verify.rs 中提取 SlotVerificationFace trait，在 wedb/src/server/cluster_session.rs 集中实现，同时删除 wtxn 和 wnode 里的冗余定义

[去重合并] 重构 ClusterSession 虚表分发
c#: garnet/libs/server/Cluster/IClusterSession.cs:IClusterSession
rust: wedb/wnode/src/cluster_session.rs:225 (ClusterSessionVtable)
现状: wnode 内部使用大量 unsafe 裸指针和手写 VTable 模拟动态分发，代码极度膨胀且不安全
方案: 删除 ClusterSessionVtable 及其相关 unsafe 代码。改用 enum_dispatch 或定义包含 Noop 和 Cluster 变体的标准 enum 替代，利用 Rust 静态分发机制实现零成本多态

[常量整合] 提取集群重定向协议文案
c#: garnet/libs/cluster/CmdStrings.cs:CmdStrings.RESP_ERR_CROSSSLOT
rust: wedb/wedb/src/server/slot_verify.rs:73 (SlotVerifyKind)
现状: CROSSSLOT 等响应文案散落在 wedb 和 wnode 多个文件中硬编码
方案: 将硬编码字符串统一提炼至 wresp/src/cmd_strings.rs，声明为 pub const CROSSSLOT: &str = "CROSSSLOT Keys in request do not hash to the same slot";，在所有引用处替换

[模块重组] 重塑会话解析扩展机制
c#: garnet/libs/server/SessionParseStateExtensions.cs:SessionParseStateExtensions
rust: wedb/wnode/src/session_parse_state_extensions.rs:70 (SessionParseStateExtensionAccess)
现状: 扩展方法采用 C# 的静态类扩展风格，不符合 Rust 惯用法，且名字冗长
方案: 文件重命名为 session_ext.rs，定义 pub trait SessionParseStateExt 并为 wresp::SessionParseState 直接提供方法实现，替换原有独立的辅助函数

[模块重组] 收拢集群提供者逻辑
c#: garnet/libs/server/Cluster/IClusterProvider.cs:IClusterProvider
rust: wedb/wnode/src/cluster_provider.rs:15 (ClusterProvider)
现状: ClusterProvider trait 接口过于宽泛，包含了 Gossip 等与网络会话无关的逻辑
方案: 将 Gossip、槽位迁移、故障转移等纯后端逻辑从 wnode 的 ClusterProvider trait 中剥离，深度封装在 wedb 内部。向 wnode 只暴露极简的鉴权与路由边界（如 IClusterSessionProvider）

[依赖优化] 解耦事务层槽位校验
c#: garnet/libs/server/Transaction/TxnKeyManager.cs:TxnKeyManager
rust: wedb/wtxn/src/txn_slot_verify.rs:15 (TxnSlotVerifyFace)
现状: wtxn 作为存储和事务引擎，直接依赖了网络语义的槽位校验抽象
方案: 移除 wtxn 对 TxnSlotVerifyFace 的依赖，改用闭包参数或注入无业务语义的通用泛型 SlotVerifyStrategy，切断 wtxn 向上层网络层的语义依赖
