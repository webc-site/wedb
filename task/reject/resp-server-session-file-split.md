resp_server_session.rs 混装四段职责、按 C# partial 边界纯移动拆分 —— 不成立，不立票

优先级：打磨（该档自陈「优先级：打磨（文件组织与并行修改热点）」）
来源：next/resp-server-session-file-split.md（2026-09-19 分拣判否，原档整删）
取证基线：主仓 /Users/z/git/db/wedb，dev HEAD 228c1963，
/Users/z/git/db/wedb/wedb/wnode/src/resp/resp_server_session.rs 现 3166 行

原档观点（保留待档）

1. 本文件仍单文件混装四段：会话主生命周期/解析循环/命令分派、输出队列段
   （send_and_reset / take_output / take_output_into / take_output_watermark_yield）、
   lua/scripting 段（run_lua_command + struct RespScriptingApi + impl ScriptingApi）、
   两个 trait 桥（impl wtxn::TxnSession、impl PubSubSessionCommands 含 network_* 转发壳）。
2. range_index / vectors / slot_verify 已拆出，唯余本文件未拆，故「按 C# partial 边界纯移动」
   续拆：输出队列段并入 resp_server_session_output.rs（称此为「消第二处输出面」）、
   新增 resp_scripting_api.rs、新增 resp_txn_pubsub_bridges.rs，经 mod 再导出保
   wnode::resp::RespServerSession 路径不变、外部引用零改动。
3. 验收判据：本文件「显著回落（目标 < 2000 行）」+ git diff 呈整块搬迁 + cargo check 通过。
4. 开工前置：其自陈依赖 task/ing/resp3-storage-session-version-source.md（resp_version 双载体）
   与「功能缺口条 12」。

判否理由

一、按 fixloop 判据（死代码 > 重复逻辑/多套架构 > 污染扩散 > 功能缺口 > 打磨）它落在最低档，
且三条硬约束本身排除了收益。
该档规定「纯移动拆分（整块搬迁、无实现差异）」「经 mod 再导出保持路径不变、外部引用零改动」
「无实现差异即编译通过」——拆完四段的调用关系一字未变，crate 对外路径一字未变，
既不删任何死面，也不消任何重复机制。与 task/reject/wnode-service-split.md（同日、同型判否）
同理：「文件太长」的行数统计与「并行修改每次顶行号」的工时抱怨都不是代码问题。
本仓门禁已两次因此否掉同族搬票（task/reject/wnode-service-split.md、
task/reject/wext-json-commands-file-split-dup.md 的收口口径），本档不例外。

二、其核心动作「输出队列段并入 resp_server_session_output.rs，消第二处输出面」是事实错误：
rust 不存在第二处输出面，照做反而造出与 C# 相反的分界。实测：
- C# 的 SendAndReset 定义在主 partial：/Users/z/git/db/wedb/garnet/libs/server/Resp/
  RespServerSession.cs:1348（无参臂）与 :1368（IMemoryOwner 臂）；WriteDirectLarge 在同文件
  :1410；出向字节记账单点也在同文件 :1440 Send 内（:1462 一处 incr_total_net_output_bytes）。
- /Users/z/git/db/wedb/garnet/libs/server/Resp/RespServerSessionOutput.cs（296 行）全文只有
  :22 ProcessOutput 与 :31-:275 的 Write* 系列，对 SendAndReset 只调用不定义。
- rust 现状与 C# 完全同构：帧型 write_* 在
  /Users/z/git/db/wedb/wedb/wnode/src/resp/resp_server_session_output.rs（现 218 行，
  全部是 RespWriter 薄转调，文件头 :3 已声明「统一代理…消除重复拼装」），
  冲写与取件在 resp_server_session.rs。两者在 C# 是两个 partial，在 rust 也应是两个文件。
- 于是该档「按 C# partial 边界纯移动」的立场与其第一步动作互斥：
  把冲写段并进 _output.rs 恰是逆 C# 边界的合并。

三、其余三段逐段实测无重复机制、无第二套包装、无零消费者死面（本档未提出，此处补核）。
- lua/scripting 段：RespScriptingApi（:2704）是 wlua::api.rs:10 `ScriptingApi` trait 的
  会话适配器，除它以外该 trait 的另两个实现都在 wlua 自己的域内
  （/Users/z/git/db/wedb/wedb/wlua/src/runner/host.rs:191 ScriptSessionRef、
  wlua/src/commands.rs:433 NoopScriptingApi 且 #[cfg(test)]），wnode 侧无第二适配器；
  run_lua_command :2508 与 ScriptingApi 臂之间无同段重复实现。
- 两个 trait 桥：wtxn::TxnSession 在 wedb/src 内只有本实现一处
  （另一实现是 wtxn_test/src/lib.rs:49 的 mock），桥内十余枚一行转调是跨 crate trait
  形状所迫（C# 侧是同 partial class 成员，无 trait 位），不是「第二套包装」；
  真逻辑只在 write_proc_param_error :2874、park_iterative_slot_wait :2899、
  verify_cluster_txn_keys :2911 三处，各单点。
  PubSubSessionCommands 段的 network_* 十口（:3045-3096）全部是 with_pubsub :3033
  单骨架上的一行转调，且十个口在生产分派臂 :1455-1468 逐口有消费者，无一悬空。
- 本档若落地只是把「一行转调 + 单点逻辑」换个文件继续摆着。

四、交叉引用与取证已第三次漂移，且前置依赖指向不存在的档案。
地标全部后移：文件 3141 → 3166 行；resp_server_session_output.rs 224 → 218 行；
send_and_reset :2191 → :2222、take_output :2210 → :2241、take_output_into :2220 → :2251、
take_output_watermark_yield :2250 → :2281、run_lua_command :2477 → :2508、
struct RespScriptingApi :2684 → :2704、impl ScriptingApi :2699 → :2719、
impl wtxn::TxnSession :2765 → :2792、impl PubSubSessionCommands :2924 → :2959、
network_* 转发壳 :3005+ → :3045+。
其「交叉引用」段所指 task/ing/resp3-storage-session-version-source.md 在 task/ing 与
task/done 均不存在（该串只在 next/ 的两张在分拣档里被引用），其开工前置「功能缺口条 12」
也无从核对；即该档的开工门禁按自身文本已不可判。
另：C# 主 partial RespServerSession.cs 本身 1737 行，「主文件必须小」既非 C# 现实
也非质量判据，与本仓「不设人为行数上限」（见 task/reject/wnode-service-split.md 第三条）同判。

五、成本由全部并行代理承担。
本文件是在途票最密落点：task/ing/pending-lat-no-timing-site.md、
task/ing/session-metrics-option-dead-track.md、task/ing/zero-consumer-surfaces-batch-two.md、
task/ing/lua-call-pending-suspend-handoff.md 均改本文件语义段。搬 3166 行会让每票重解一次
整文件移动冲突，换到的只是文件切面。

六、既有台账登记的处置。
task/reject/qw11-inv3-consumed.md:36 记本条「open，打磨 LOW，3153 行拆分未动，仍成立」——
按本波门禁（纯搬家不立票）作废该登记；
task/done/async-command-no-read-side.md:78、task/done/zero-consumer-dead-surfaces-batch-four.md:67、
task/ing/lua-call-pending-suspend-handoff.md:95-98 三处对「resp_server-session-file-split 只管
移动、勿在搬票里夹带删改」的交叉引用自本档起无对应载体，各票按自身射程独立推进即可，
禁据此重开搬票。
在册的同类搬票（task/ing/cluster-provider-file-split.md、
task/ing/tiered-collection-ops-file-split.md、task/ing/json-commands-file-split-anchor-decl.md、
next/garnet-api-slow-path-command-split.md、next/zero-consumer-dead-surfaces-batch-six.md 的
门面文件条）不随本档重开或并档——它们各携自己的锚点口径或删除项，判定独立。

分拣期间的真实发现（另票，不属本档）

本档点名的「输出队列管理段」经逐口取证，暴露的是冲写口自身的重复与死面，不是它在哪个文件：
同一段「累计出向字节 + 会话指标」记账被抄四份（resp_server_session.rs:733-754、:2222-2232、
:2241-2248、:2251-2269），其中 take_output 全仓 128 处调用无一处来自 src/（测试专用），
send_and_reset 的唯一生产调用点（:2745-2746）与 take_output_into 同义，
resolve_blocked_wait :756-766 是 _into 的薄壳副本且注释自称「向后兼容接口」，
flushed_bytes 字段四处累加、全仓零读者。
已另立 task/ing/resp-session-output-drain-single-source.md。删这些不需要拆文件。

未立票的正交观察（留待有域者裁）
- ClusterSlotVerificationInput 构造点共 4 处（resp_server_session_slot_verify.rs:84、:114、:144
  与 resp_server_session.rs:2916），各携不同键规格与只读判据，属参数化构造而非重复机制；
  若要收口应为 `from_cmd(..)` 单点构造，收益是可选的字段新增免漏改，本档未提出、
  本轮亦不立票。
