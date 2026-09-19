同步收割 block_on 四份复抄收敛到 wbase::future 单点，acl_store 私有 scan_err 同文件复抄一并收口

来源：next/glm.design.md 第 6 轮条 1、条 2 合并立项（同一 file:line 交集在
wnode/src/resp/acl_store.rs 与 wnode/src/resp/vector/vector_store_callbacks.rs，拆两单必互踩
同一编辑面）。取证基线：主仓绝对根 /Users/z/git/db/wedb，分支 dev，行号按当下代码重取。

结论
同一职责（同步上下文内联驱动 compio future）在三个 crate 里存着四份定义、三种行为答案，单点
候选 wbase::future 就在场却零生产复用；同文件另有一处同 crate 错误包装工具的逐字复抄。判定成
立且待做。

现状

一 block_on 四份定义与行为分叉
- /Users/z/git/db/wedb/wedb/wbase/src/future.rs:30 pub fn block_on：纯 park 驱动，ThreadWaker
  内嵌同文件 :15-27。生产零消费（票面原述「生产消费仅 wbase/src/group_commit.rs:211」经复核不
  成立：该 use 位于 :200 起的 #[cfg(test)] mod tests 内，同 crate 另一消费点
  /Users/z/git/db/wedb/wedb/wbase/tests/main.rs:56 也是测试），即本单点是「仅测试可达」的纯
  park 版。
- /Users/z/git/db/wedb/wedb/wedb/src/server/cluster_session/mod.rs:340 pub(super) block_on：
  Runtime::try_current 内联收割 + park 回退，ThreadWaker 在 :346-358 就地内联第三次定义。消费
  面同文件 :326（cluster_publish）、/Users/z/git/db/wedb/wedb/wedb/src/server/cluster_session/
  basic.rs:191、:474、/Users/z/git/db/wedb/wedb/wedb/src/server/cluster_session/
  replication.rs:630。
- /Users/z/git/db/wedb/wedb/wnode/src/resp/acl_store.rs:37 私有 block_on：同 try_current + park
  形态，ThreadWaker 第四次定义（:43-52）；消费 :116、:136、:156、:206。其 :33-34 注释自称「与
  resp/vector/vector_store_callbacks.rs 同名工具同构」，而该「同构」对象现状实现已不是这个形态，
  声明失真。
- /Users/z/git/db/wedb/wedb/wnode/src/resp/vector/vector_store_callbacks.rs:47 pub(crate)
  block_on：Runtime::current() 无回退，:40-45 注释明示 park 分支「已删除——它在 io_uring 平台
  上必然永久挂起，在 poll 平台上也只是借 asyncify 线程池侥幸推进」。消费方
  /Users/z/git/db/wedb/wedb/wnode/src/storage/session/txn_proc_view.rs:30 跨模块 import 本
  pub(crate) 版并在 :61、:83、:90、:95、:101、:114、:120 使用，使「无线程上下文时 park 去留」
  这一问题在同 crate 内同时存在两种答案。

即对同一问题三份裁定：vector 判删、acl_store 与 cluster_session 判留、wbase 只有 park。留着 park
的理由两侧自述均为「单测无 runtime」，而该用例本可用 Runtime::new 包裹（同仓既有先例
/Users/z/git/db/wedb/wedb/wnode/src/aof/aof_backpressure.rs:355、
/Users/z/git/db/wedb/wedb/wcol/src/itembroker/collection_item_broker.rs:778 的
Runtime::new().unwrap().block_on），不需要在生产函数体里为测试保留一条会永久挂起的分支。

二 acl_store.rs 私有 scan_err 复抄同 crate 既有 pub(crate) 单点
- /Users/z/git/db/wedb/wedb/wnode/src/resp/acl_store.rs:65 私有 fn scan_err（whlog 错误 →
  WkvError::Io(io::Error::other(e.to_string()))），消费 :195 一处，注释自认「与
  array_key_iteration_functions::scan_err 同口径」。
- 单点 /Users/z/git/db/wedb/wedb/wnode/src/storage/session/common/
  array_key_iteration_functions.rs:35 pub(crate) fn scan_err 同签名同体逐字复抄（同文件 :244、
  :332、:372、:451、:496 五处消费）。模块链全程 pub（storage/mod.rs:11、session/mod.rs:1
  `pub mod common`、common/mod.rs:1），对 wnode 全 crate 可达，改 use 零成本。

C# 参考
- /Users/z/git/db/wedb/garnet/libs/common/AsyncUtils.cs:17-49 一个 BlockingWait 四重载定义全仓
  共用，无任何调用方自造同步收割（cluster_session 版 :334-336 注释自认对标它）。
- 同步收割对位 /Users/z/git/db/wedb/garnet/libs/server/Resp/Vector/ 侧
  ClientSession.CompletePending(wait: true) 亦单点形态。
- scan_err 无 C# 对位（rust crate 内错误包装工具复抄，属一处定义原则面）。

修法
1. /Users/z/git/db/wedb/wedb/wbase/src/future.rs 增 runtime 感知变体（try_current 命中即
   Runtime::block_on 内联收割，未命中的去留见步 3），并把 ThreadWaker 收为 wbase 一份
   （:15-27 现形态）；纯 park 版保留给无 runtime 的纯计算驱动用例，两者在模块头写清分工。
2. cluster_session/mod.rs:340、acl_store.rs:37、vector_store_callbacks.rs:47 三处定义删除，改
   use wbase::future::*；vector 侧现依赖「无回退即 panic 暴露装配错误」的纪律，单点须保留该裁
   定入口（或在该调用点显式用 Runtime::current 版），不得为收敛而把 panic 换成静默 park。
3. park 回退按 vector 版裁定统一删除：以 park 兜底为测试前提的用例改 Runtime::new 包裹，与本仓
   既有测试形态一致；同步修订 acl_store.rs:33-34 与 cluster_session/mod.rs:334-336 的注释，使之
   与实现一致（禁止只改注释不动实现）。
4. 删 acl_store.rs:65，改 use crate::storage::session::common::array_key_iteration_functions::
   scan_err。
5. 收口后按 SKILL 口径核对 js/check.js 无新增重复定义告警，并在各保留函数文档注释里写明
   `libs/common/AsyncUtils.cs:BlockingWait` 映射。

优先级
重复/多套架构（同一职责四定义三裁定，且单点在场零复用；park 回退是涉 IO 用例的永久挂起面）。

边界
task/ing/zero-consumer-dead-surfaces-batch-five.md 管零消费 pub 面普查，本条 block_on 单点有生产
消费者，不属其射程。本条只替换驱动来源，不改 txn_proc_view 各调用点的存储语义。

收口：分支 boss-land 已由前手代理自走 merge 入 dev（7cafcdbf，8 文件 118+/138-）；本票由主代理核对后转 done。wbase::future 单点 block_on 收口与 acl_store 私有 scan_err 复抄收口随该合并生效。
