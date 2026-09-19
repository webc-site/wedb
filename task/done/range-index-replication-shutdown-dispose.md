停机链补范围索引收口步（RangeIndexManagerReplication::dispose 死面二选一收口）

来源：next/glm.my.md 第 12 轮条（自定义优化上下游打通审查）。取证基线：主仓 /Users/z/git/db/wedb dev 工作树，行号为当下实况。
去重：并发分拣把同一原文照搬成六行 stub /Users/z/git/db/wedb/next/range-index-dispose-shutdown-gap.md，
本档是其细化版，同题只此一份；认领开发时删该 stub，勿据 stub 另开分支。

## 现状

/Users/z/git/db/wedb/wedb/wnode/src/rangeindex/range_index_manager_replication.rs:592-598
`pub fn dispose(&self)`（:596 dispose_incomplete_stream_reassembly、:597 self.engine.dispose()）
文档注释 :592 明挂 libs/server/Resp/RangeIndex/RangeIndexManager.cs:Dispose，全仓 src 与 tests 零调用
（grep 该符号只命中函数体自身），停机两侧均不触达：

- 两处构造点 /Users/z/git/db/wedb/wedb/wnode/src/service.rs:377-379（AofSinkContext.ri）与
  :463-466（回放会话装配）均从不调用；
- 同文件 Drop :612-616 只做 reassembly 清理，不做 engine 释放；
- engine 为 Arc<wbftree::RangeIndexManager>，与 store.range_index 共享同一 Arc
  （/Users/z/git/db/wedb/wedb/wkv/src/store/mod.rs:95），全部在线树释放的唯一显式挂点是
  WedbStore::drop 内 :605 `self.range_index.dispose()`；其余兜底是 wbftree 层
  /Users/z/git/db/wedb/wedb/wbftree/src/manager/mod.rs:542-545 的 Drop → dispose。

即停机收口从 C# 的「显式步骤」退化为「依赖 service/ctx/会话/detach 任务等所有 Arc 引用恰好归零」的
隐式析构：嵌入式或复用进程场景下 stop() 返回后任一滞留引用即令全部树句柄与 native 页缓存驻留不释。

C# 对位链完整：/Users/z/git/db/wedb/garnet/libs/server/StoreWrapper.cs:909-924 Dispose 第十步
:921 `rangeIndexManager?.Dispose()`（时序在 :915 clusterProvider?.Dispose() 之后、databaseManager 之前），
终点 garnet/libs/server/Resp/RangeIndex/RangeIndexManager.cs:484-502（reassembly 清理 +
逐树 Tree?.Dispose() + liveIndexes.Clear()）。

dispose() 自居 1:1 对标却无任何调用方，属 /Users/z/git/db/wedb/.agents/skills/transpile/SKILL.md:79
「严禁占位函数或虚设实现」射程内的虚设面。

## 修法（二选一，禁留静默分叉）

优先方向 A（补挂调用，与 C# 时序 1:1）：

1. 在宿主停机链显式补范围索引收口步：/Users/z/git/db/wedb/wedb/wnode/src/server.rs:701-720
   GarnetServer::stop 内、AOF 背压 dispose（:712-716）与 worker join（:717-719）之前调用复制面 dispose，
   使 reassembly 清理与 engine.dispose() 一并落地；句柄取用走既有装配路径
   （AofSinkContext.ri / processor.set_range_index_manager 已有 Arc，必要时由 session_provider 暴露
   一个只读 accessor，不新建第二份持有者）。
2. 单机统一尾部 server.rs:301-308（现仅 cluster_provider.dispose() + flush_config()）与
   stop() 的先后次序按 C# StoreWrapper.Dispose 步骤重排一次并写注释标明对应步骤号，
   避免「两处各收一半」。
3. 收口后 WedbStore::drop 的 range_index.dispose() 保留为兜底（幂等：wbftree manager dispose
   需可重入，若现有实现非幂等则先修幂等，禁靠调用方规避）。

方向 B（若复核裁定 Arc 归零析构为 rust 刻意范式）：删除 range_index_manager_replication.rs:592-598 的
dispose() 死面（保留 Drop 的 reassembly 臂与其测试），并在 wbftree manager 层文档声明
「树释放由 Arc 归零析构承接」的取舍与不受嵌入式滞留影响的依据。方向选择由落地子代理在动工前
一次判定并写进票尾，不许两不做。

## 边界

与 next/subscribe-broker-shutdown-dispose.md（pubsub SubscribeBroker::dispose 零调用）、
集合项经纪停机收口为同族不同件（同型病灶、不同符号，各自单列避免互踩 server.rs 同一函数的顺序可并行，
冲突时以订阅票先落为准）；零消费面普查批四/批五清单均不含本符号；
next/bftree-release-detached-guard-recheck.md 管树释放的纪元守卫，不改本收口步。

## 验收判据

- 走方向 A：grep 取证 dispose() 至少一处生产调用；新增停机路径用例（构造在线树 → stop() →
  断言 live_indexes 为空且树文件句柄释放，不依赖 store 析构）。
- 重复调用安全（drop 兜底 + 显式收口并存不 panic、不误删在途树）。
- 走方向 B：全仓无该符号残留，文档声明就位。
- cargo check --workspace --all-targets 绿；中文注释、禁 #[allow]。

优先级：P2（嵌入式/复用进程的资源滞留与虚设面，非数据正确性；改动局限停机链，宜与订阅 broker 票同批做）。

盘点补记（qw13.invB range-index-replication-shutdown-dispose）：dev e75716e 复核原样：range_index_manager_replication.rs:587 pub fn dispose 仍零生产调用；wnode/src/server.rs 停机链只有 cluster_provider.dispose()（:284）、registry.dispose_active_handlers（:707）、dispose_vector_cleanup（:737）、bp.dispose（:747），无范围索引步。方向 A/B 二择一的修法不变。

## 落地判定（fix-ri-dispose，dev cdf426b 基线取证）

走方向 A，收口步落位修正一处：C# GarnetServer.InternalDispose 全序为 Phase 1 停监听 → Phase 2 排空处理器 → Phase 3 Provider.Dispose（storeWrapper.Dispose 内 rangeIndexManager?.Dispose() 为第 7 条语句，clusterProvider→itemBroker→monitor→luaTimeoutManager→ctsCommit.Cancel→taskManager 之后、databaseManager 之前）。即 C# 收口发生在连接排空之后；票面「stop() 内 join 之前调用」与 C# 时序相反（join 前在途命令仍可触达在建树），故落位改为 stop() 内 join 之后、buffer_pool.purge 之前。其余：单机尾部集群收口未搬入 stop()（bootstrap 持集群提供者而 GarnetServer 不持，搬移即改泛型装配拓扑，超本票射程），改以注释标明步骤号与次序互换依据（worker join 后集群治理面已停摆，二者无交叉触达）。句柄链：NodeService::assemble 复制面单例先造后共享（与 AofSinkContext 同一 Arc）→ NodeService::ri() → provider.ri（open_with_config_and_aof / open_recovered_with_config_and_aof 两处转交，仿既有 provider.aof 装配）→ SessionProviderFace::dispose_range_index（默认空操作）→ GarnetServer::stop 调用；dispose 实现为复制面臂（清未完成流重组）+ 引擎臂（store() 现取在线引擎 dispose，覆盖无 AOF 形态与置换后新引擎），wbftree manager 逐树 take 幂等，与 WedbStore::drop 兜底并存安全。验收：新增 wnode/tests/range_index_shutdown_tests.rs（在线树 → stop() → live_index_count==0 → 显式重入不 panic）；cargo check --workspace --all-targets 绿。
