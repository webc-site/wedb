wnode/src/service.rs 按 C# 边界分文件（纯移动）—— 不成立，不立票

优先级：打磨（该档自评「打磨，结构对齐 C# 拓扑，不改行为、不修 bug，本波最后」）
来源：next/wnode-service-split.md（2026-09-19 分拣判否，原档整删）
取证基线：主仓 /Users/z/git/db/wedb，dev HEAD 90201c15，
/Users/z/git/db/wedb/wedb/wnode/src/service.rs 现 1816 行

原档观点（保留待档）

1. 单个 wnode/src/service.rs 混装五种职责：NodeService 本体、AOF 写事件汇
   （on_aof_store_event 单函数约 207 行）、节点装配自由函数群（spawn_* / store_config /
   apply_hlog_overrides / open_node* / recover_checkpoint_store / open_wal）、
   在线引擎置换槽 StoreSwapSlot、会话提供器 StorageSessionProvider 含
   SessionProviderFace 实现（原档估占全文 45%）。
2. C# 同域确为分立多文件，据 .agents/skills/transpile/SKILL.md:81「可以拆分、修订，
   让其拓扑和 c# 更加吻合」，属规范内结构对齐。
3. 方案：service.rs 改 service/ 目录六件套（mod.rs 再导出、node_service.rs、
   aof_sink.rs、assembly.rs、store_swap_slot.rs、session_provider.rs），
   「纯移动 + 再导出，不改实现、不重命名，不改对外可见性」（段间私有项升 pub(super) 例外），
   「禁止任何实现改动、顺手补 TODO」，一次提交只做移动。
4. 验收判据自陈「不设人为行数上限：行数非判据，拓扑对位才是」。
5. 门禁：session-metrics-counter、aof-sync-driver 已解除；txn-aof-marker-session-wiring
   未落地，故「须待其落地后再搬」。
6. 其 2026-09-19 认领核查一节：以「门禁未解除 + service.rs 是多路并发的共同落点，
   搬迁窗口必须落在同域写者收敛之后」为由自行退回 next/。

判否理由

一、它不消除任何真实重复机制，也不删任何死面，只搬 1816 行。
该档三条硬约束（纯移动、禁改实现、不改对外可见性）本身就排除了收益来源：
拆完 ServiceProviderFace 与其 inherent impl 仍在同一 crate 同一模块路径下，
五段的调用关系一字未变。按 fixloop 判据（死代码 > 重复逻辑/多套架构 > 污染扩散 >
功能缺口 > 打磨），「文件太长」的行数统计不构成问题。

二、装配段唯一真实的重复/多套事实已在册，本票重开即重复立项。
装配群的真问题不是「它在哪个文件」，而是同一启动事实在多处手抄：
NodeArgs 三字段样板在 /Users/z/git/db/wedb/wedb/wedb/src/server/boot.rs:31-45、
/Users/z/git/db/wedb/wedb/wedb_standalone/src/main.rs:52-66 双抄，第三形态
/Users/z/git/db/wedb/wedb/wedb/wnode/src/server.rs:870 run_node 起的自走臂（该票登记时行为
:784-800，已漂移）。
该事实由 task/ing/boot-assembly-projection-single-source.md 承接（其第 35 行反向引用本票，
仅表达「勿在纯搬票里改装配语义」，本票不开后该引用作废，勿据此重开）。
本档对这段只字未提重复，只提归属分件。

三、C# 分件的前提在 rust 不成立，「拓扑对位」论据被该档自陈否证。
C# 侧 GarnetServer.cs（garnet/libs/host/GarnetServer.cs，642 行，构造入口 :81/:149、
私有 :170 InitializeServer、:527 Start）、StoreWrapper.cs、GarnetProvider.cs、
GarnetDatabase.cs 各自是独立类型 + 独立生命周期职责，分件是类型的自然边界；
rust 侧这五段是同一类型簇的 inherent 成员与私有自由函数，
把 `impl<F> SessionProviderFace for StorageSessionProvider<F>`
（现 /Users/z/git/db/wedb/wedb/wnode/src/service.rs:1507-1691）与其 inherent impl
（:958-:1505）拆到两个文件，是逆 rust 惯例的额外间接层。
且该档自己规定「不设行数上限、行数非判据」，于是它的收益论证只剩「C# 也是几个文件」，
而其方案第 1 步又要求 mod.rs 逐字保持 wnode::service::* 对外路径不变——
拆完后 crate 对外的模块拓扑与 C# 仍然不对位（一个类型仍是一个类型，路径仍是一个路径）。

四、AOF 入账段的归口问题该档未定形，属另一票射程。
该档承认 on_aof_store_event 在 C# 属存储函数域
（garnet/libs/server/Storage/Functions/MainStore/PrivateMethods.cs:766 WriteLogUpsert），
却把去处写成「service/aof_sink.rs 或 wnode/src/aof/store_event_sink.rs 二选一，
取 C# 归属更贴切者」而不裁决；真正有结构意义的是后者（并入 aof/ 域），
那是「AOF 事件汇归口 wnode/src/aof/」的归口票（会改变 crate 内依赖方向，
须先核 on_aof_store_event 与 aof/ 是否成环，该档第 4 步也自陈「若需调结构，停下另立项」），
与「搬家」不是同一件事。本票不以其未定形形态立票；要立应由对 aof/ 域有取证的人另开。
本档核对期间实测：AOF 入账段无重复机制——physical_key（service.rs:132）
只是 NamespaceDbCodec::encode_tagged_key 的一行别名（与
wnode/src/resp/acl_store.rs:87 的同名私有 helper  arity 不同、各自单点），
enqueue_raw/enqueue_slices（:98/:113）是 GarnetAppendOnlyFile 同名方法上的错误映射薄壳，
全仓 Error::AofEnqueue 映射点仅 4 处（wnode/src/database/single_database_manager.rs:122 与
service.rs:108/:123/:328），无多套编码或入账机制。

五、成本由所有并行代理承担，且该档自己的退回理由仍然成立（换了理由而已）。
service.rs 是在途票密集落点：task/ing/session-metrics-option-dead-track.md、
task/ing/reviv-knobs-zero-production-wiring.md、task/ing/primary-checkpoint-cluster-callback.md、
task/ing/aof-replay-virtual-domain-context.md、task/ing/checkpoint-flush-throttle-knob.md 等
均改本文件语义。搬 1816 行会让每票重解一次整文件移动冲突，换到的只是文件切面。

六、取证已第五次漂移，且对外引用面比该档所记更小。
该档行号基于 1743 / 1784 两个旧快照，现值 1816 行，地标整体后移：
NodeService :87、on_aof_store_event :145、StoreSwapSlot :834、StorageSessionProvider :861、
SessionProviderFace impl :1507、hlog_assembly_tests :1694。
其修正过的「外部引用面 6 符号」经复核成立（open_node 与 open_node_with_config 之中，
前者经 /Users/z/git/db/wedb/wedb/wnode/src/lib.rs:60 顶层再导出的只有 open_node_with_config，
StoreSwapSlot 消费者为 wedb/src/server/cluster_provider.rs:36，
assemble_lua_timeout 消费者为 wnode/src/resp/resp_server_session.rs:83，
StorageSessionProvider 消费者含 wnode_test/src/lib.rs:25），
即跨 crate 面走 wnode:: 顶层路径，分文件对它们零影响——搬家收益再降一档。

七、门禁前提已消失，该票失去最后的存续理由。
其唯一未解除门禁 txn-aof-marker-session-wiring 已被判否归档
（/Users/z/git/db/wedb/task/reject/txn-aof-marker-session-wiring.md），
SessionDependencies 不再增 aof_log 字段。按该档「或该票已明确弃做」的条款，
门禁等于解除——也就是说，它现在就能做，而结论依然是不该做：
问题在方案本身，不在开工窗口。

分拣期间的真实发现（另票，不属本档）

wnode::service::open_node（现 /Users/z/git/db/wedb/wedb/wnode/src/service.rs:749-755）
全仓零消费者，且与 StorageSessionProvider::open（同文件 :966-968）构成缺省装配双入口，
属死代码/重复入口，已另立 task/ing/wnode-service-open-node-zero-consumer.md。
该档「拆分会暴露真实死面」的假设，唯一命中的就是这一口，而删它不需要拆文件。
