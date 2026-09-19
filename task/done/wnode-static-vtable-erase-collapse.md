wnode 三处手写静态虚表类型擦除收敛为 Arc<dyn trait>

来源：设计审查轮次甄别（原 qcode 条 1，票据已核销删除）。

结论
cluster_provider / cluster_session / garnet_api 三处各自重写了 Arc<dyn Trait> 的原生语义
（裸 ptr + 静态 fn 指针表 + unsafe impl Send/Sync + 手工引用计数），无任何收益（Arc<dyn> 分发同为一次
间接调用），却引入手写 clone/drop 正确性负担与全仓最大 unsafe 集群，且 C# 对位是直接持接口/对象引用
（虚分派）。转写凭空多出一层自造机制。判定成立且待做。

现状（HEAD 取证）
- wnode/src/cluster_provider.rs:279 ClusterProviderVtable、:303 ClusterProviderHandle（字段 vtable
  :305，unsafe impl Send/Sync :308-309，Drop :448，Clone :457，From<Arc<T>> :466，const fn
  make_vtable :314）。
- wnode/src/cluster_session.rs:209 ClusterSessionVtable、字段 vtable :233、:247-249
  struct VtableHolder + const VTABLE、:292 取 &VtableHolder::<T>::VTABLE。
- wnode/src/resp/garnet_api/mod.rs:119 type RawExecFn = unsafe fn(...)、:120 RawExecParts、:125
  struct GarnetApi（字段 exec :127）、:162/:182/:304 Arc::increment_strong_count 手工续引用。
- 三文件 unsafe 命中约 156 处（cluster_provider.rs 62、cluster_session.rs 56、garnet_api/mod.rs 38）。
- 外部持有面（收敛后签名须保持）：database/database_manager_base.rs:63/81、
  database/single_database_manager.rs:30/56、resp/resp_server_session.rs:376/764、
  resp/config_commands.rs:91/699、resp/resp_session_consumer.rs:62/87。
- 实现者齐备，可直连 Arc<dyn>：ClusterProvider 有 NoopClusterProvider + wedb ClusterProvider；
  ClusterSessionFace 有 wedb ClusterSession；GarnetApiFace 有 StoreGarnetApi。
- 同病灶先例已在代码中收敛为 &dyn：wtxn/src/txn_slot_verify.rs（&dyn TxnSlotVerifyFace 借用直穿
  run_transaction_proc），证明静态虚表层无必要。

C# 参考
- garnet/libs/server/Cluster/IClusterProvider.cs（ClusterProvider 直接持接口引用）。
- garnet/libs/cluster/Session/ClusterSession.cs（RespServerSession 持具体 ClusterSession 对象）。
- garnet/libs/server/API/IGarnetApi.cs（C# 以泛型静态分发，无任何手工擦除层）。

修订方向
三处统一收敛为 Arc<dyn trait>：删除 vtable 结构体、handle 结构体、unsafe impl Send/Sync、
make_vtable/VTABLE 常量、手工 clone/drop/increment_strong_count，unsafe 归零，不留性能旁路第二版。
GarnetApiFace::exec_slow 现为 RPITIT 不可 dyn，改返回具体 SlowFuture（构造点 garnet_api/mod.rs
from_arc 本就包 SlowFuture::new，属签名改型非语义变更）。ClusterProviderHandle/ClusterSession/
GarnetApi 对外类型别名尽量保留以少改外部签名（沿用 wtxn 先例）。

串接
本条与启动装配、集群挂起锁等在途分支同文件，须在其合并落地后开工并重核行号；勿再往已废弃的 vtable 加槽。

验收
三文件 grep unsafe 归零；GarnetApi/ClusterSession/ClusterProviderHandle 对外签名不变即编译通过；
resp 集群会话与 cluster_* 集成测试全绿。

优先级
重复/多套架构（自造 Arc<dyn> 替身、全仓最大 unsafe 集群）。

细化方案（开工重核后追加，基线 dev 49b6286）
- 串接前置核实：boot-assembly-projection-single-source 与 cluster-suspend-await-lock 仍在
  next/ 未认领未合并，本票为语义零变更收敛票，先落地反而消除后续票在 vtable 上继续堆槽的
  风险（票据自身要求勿往 vtable 加槽），开工判定成立。行号已按 HEAD 重核：garnet_api/mod.rs
  漂移为 RawExecFn :125、GarnetApi :131-139、手工 increment_strong_count :169/:188。
- 新发现一：garnet_api/mod.rs 内还有第四处同病灶 CollectionNotify（:264-338，裸 ptr +
  clone/drop fn 指针 + Arc::increment_strong_count :310/:317），验收三文件 unsafe 归零
  覆盖它，一并收敛为 pub type CollectionNotify = Arc<dyn Fn(&[u8]) + Send + Sync>，构造点
  service.rs 的 fn 指针 + ctx 组合改 move 闭包。
- 新发现二：wtxn 先例是 &dyn 借用，本票三处均为拥有态句柄，等价形态是类型别名
  Arc<dyn Face>（对齐票据修订方向），wtxn 借用形态不适用于跨连接持有的场景。
- 三处定式：
  cluster_provider.rs：删 ClusterProviderVtable/ClusterProviderHandle 句柄全套
  （make_vtable/Drop/Clone/From/unsafe impl），pub type ClusterProviderHandle =
  Arc<dyn ClusterProvider>；trait 与 Noop 保留；impl<T: ?Sized> ClusterProvider for Arc<T>
  转发层若无实际使用一并删除。
  cluster_session.rs：删 ClusterSessionVtable、SlotVerifyFn/ProcessClusterCmdFn/
  IterativeSlotVerifyFn/WriteCachedVerifyMsgFn 四个 unsafe fn 指针类型、ClusterSession
  句柄全套，pub type ClusterSession = Arc<dyn ClusterSessionFace>。
  garnet_api/mod.rs：删 RawExecFn/RawExecParts、GarnetApi 句柄全套（含 raw_exec_parts
  借用解耦机制），pub type GarnetApi = Arc<dyn GarnetApiFace>。
- GarnetApiFace::exec_slow 签名改型：RPITIT 改 self: Arc<Self> 接收者 + 返回 SlowFuture
  （object safe，Arc receiver 稳定合法），实现体 SlowFuture::new(async move { ... })，
  self move 进 future 保 'static，语义与 from_arc 原包裹一致；调用点 slow_path.rs
  for_command 改 Arc::clone(api).exec_slow(...)（每慢命令一次原子递增，对 SCAN/KEYS 级
  慢命令不可见）。
- 入口签名去 Into 泛型：set_garnet_api/attach_cluster_session/attach_cluster_provider 与
  RespSessionConsumer::new/with_cluster/attach_cluster_provider 参数直接用类型别名
  （参数位是 unsize coercion site，Arc<具体> 自动转 Arc<dyn>），删除 From<Arc<T>>/
  From<StoreGarnetApi> 构造 impl，杜绝第二套转换机制。
- dispatch_via_garnet_api 的借用解耦：原 (ptr, fn) 二元组机制删除，改 let-else 先
  self.garnet_api.clone()（一次原子递增）再 api.exec(self, ...)，消灭每命令 unsafe
  裸调，代价为命令分派路径单次原子递增（对比命令解析与存储执行开销不可见）。
- wedb 侧改动最小化：create_cluster_session 保持返回 Arc<wedb 具体 ClusterSession>
  （IClusterProvider trait 面零改动，15+ 测试的 Arc<ClusterSession> 标注零改动，装配
  参数位自动 coercion）；provider_handle 返回位 coercion 直接返回 self_arc；
  boot.rs/wedb_test node.rs 闭包内 StoreGarnetApi 入参加 Arc::new 包裹。
- 甄别结论补充：C# IClusterProvider.cs / ClusterSession.cs / IGarnetApi.cs 均直接持
  接口/对象引用虚分派，无任何手工擦除层，票据方向成立；票据所列行号漂移不影响判定。
