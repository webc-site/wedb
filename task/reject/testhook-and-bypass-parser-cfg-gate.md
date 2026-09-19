fixloop 拒绝档案：next/testhook-and-bypass-parser-cfg-gate.md（测试钩子与旁路解析器 cfg 门）
核销 2026-09-19（判落地只认现刻代码；取证时主仓 HEAD 83e4b14 前后，行号按符号现取，他人并发合并致漂移时以符号名为准）。
判词：本票三族主张全部不成立——两族修法（TestHook 五口补门、旁路解析器补门并抽共享骨架）在现刻代码里早已落地且
落地形态比票面更强；第三族（active_instance_count/reset_all_instances 本不该公开）与 C# 可见性事实相反，属 1:1
对标件，关掉即失对标并打断测试面。零代码改动、未开 worktree、未做合并，票转本档结案。
本档不新增任何 js/check/ignore 登记，故未跑 check.js（语料零变化）。

----------------------------------------------------------------------

主张一：wepoch/src/epoch.rs test_hook 五口「以 pub 挂在主 LightEpoch impl 上」「无 cfg/可见性门」
原文（票面第 4-10 行转录）：测试/调试专用面缺 cfg 门两族：wepoch TestHook 系列由 C# internal + 独立分部文件
降级为生产 pub 成员……五口生产零消费者（读者仅 wepoch/tests/epoch/{protection,support,concurrency,drain}.rs），
且以 pub 挂在主 LightEpoch impl 上；C# 对位件是独立文件 LightEpoch.TestHooks.cs，五个口全为 internal。

拒绝理由（症状不存在，修法已在位且形态正确）
1. 现刻 wedb/wepoch/src/epoch.rs:734 即 `#[cfg(debug_assertions)] impl LightEpoch`，五口全在该门内：
   :738 test_hook_this_thread_entry、:744 test_hook_this_thread_announced_epoch、:753 test_hook_announced_epoch_at、
   :763 test_hook_thread_id_at、:774 test_hook_drain_list_capacity。主 LightEpoch impl 于 :724 收口，五口并不
   「挂在主 impl 上」；:726-733 是专门的文件门替位注记，逐字对位 C# TestHooks.cs 的「Read-only views …
   used only by the unit tests in Tsavorite.test.epoch」双门口径。
2. 「生产构建零暴露」不是注释口径，已实测：`CARGO_TARGET_DIR=/tmp/ct-testhook cargo check -p wepoch --profile
   release --tests` 退出 101，39 条 E0599 全为 `no method named test_hook_this_thread_entry /
   test_hook_this_thread_announced_epoch / test_hook_thread_id_at / test_hook_announced_epoch_at`，即发布档
   profile 下整块面不编译；同 target 的 `cargo check --tests -p wepoch`（dev）退出 0、18.57s，测试路径无损。
   wedb/Cargo.toml:232-238 的 [profile.release] 未开 debug-assertions，故门真实生效；wedb/test.sh:7 是
   `cargo nextest run --all-features`（dev profile），门禁不会因该门假红。
3. debug_assertions 是本仓既有且唯一的「仅测试/诊断可见」门形态，非本票首次自研：全仓 src（tests/ 除外）30 处
   在用，含批五登记的 whyperlog/src/regs.rs:3/:70/:81/:116、whyperlog/src/dense.rs:3/:70、
   whyperlog/src/sparse.rs:3/:430/:439、wdev/src/segmented_device.rs:52/:106 等，与 C# `#if DEBUG` 同族，
   票面「本条是 internal→pub 与 benchmark-only 两类不同门」的区分不改变门的种类归属。
4. 票面修法本身不可执行，照做会打断全族用例：五口读者是 wepoch/tests/epoch/main.rs 这个集成测试 crate
   （cargo 以「不带 cfg(test) 的依赖 rlib」链接 lib），补 #[cfg(test)] 与降 pub(crate) 在集成测试里都取不到，
   等价于第 2 点那条 release E0599 的失败形态；票给的「单列 test_hooks.rs 模块」只是把同一 impl 块换个文件，
   不增不减可见性收益，而 :731-733 已把「rust 无 internal 与分部文件两级机制」记为刻意差异。

----------------------------------------------------------------------

主张二：wnode 旁路解析器两口「C# 仅 benchmark/fuzz 消费而 rust 无门常驻」「骨架双抄」
原文（票面第 5、16-23 行转录）：wnode 命令缓冲旁路解析器两口 C# 仅 benchmark/fuzz 消费而 rust 无门常驻……
两口共享同一段「save_receive_state → 清 recv_buffer → 灌入旁路缓冲 → 复位 read_head → restore →
清 parse_violation」样板（骨架双抄）……修法：两口旁路解析器补门（#[cfg(any(test, feature = "bench"))]）
并抽共享 save/restore 骨架，或在 js/check/ignore 登记两口不转写。

拒绝理由（骨架已单点、一口已在门内、另一口 C# 本就有生产消费者）
1. 共享骨架早已抽出，两口非双抄：现刻 wedb/wnode/src/resp/parser/resp_command.rs:408-423
   `fn with_bypass_buffer<R>(&mut self, buffer: &[u8], parse: impl FnOnce(&mut Self) -> R) -> R` 一处承载
   「take_receive_state → 清 recv_buffer → 灌入旁路缓冲 → 复位 read_head → restore_receive_state →
   清 parse_violation」全链；:429 parse_resp_command_buffer 与 :441 fuzz_parse_command_buffer 各自只写一行
   委托（:430-433、:442-448），票面要求的「抽共享 save/restore 骨架」即此件。
2. fuzz 口已在门内：resp_command.rs:440 `#[cfg(debug_assertions)]` 紧贴 :441 fuzz_parse_command_buffer，
   :436-437 注记写明对位 C# `internal` 且「唯一消费者是 Garnet.fuzz / Garnet.test（本仓不转写 benchmark），
   发布构建不导出」。C# 事实核对成立：garnet/libs/server/Resp/Parser/RespCommand.cs:1097
   `internal bool FuzzParseCommandBuffer`，读者 garnet/test/Garnet.fuzz/Targets/RespCommandParsing.cs:24 与
   garnet/test/standalone/Garnet.test/Resp/RespParseFuzzRegressionTests.cs:44、:73、:100、:127，src 零消费。
   集成测试（wnode/tests/resp_command_parse.rs:379、:382）在 dev profile 下照常可见，与主张一同样的
   #[cfg(test)] 不可行理由一致。
3. parse 口「唯一消费者是 benchmark」失实，C# 侧本就有生产消费者：RespCommand.cs:1065
   `internal RespCommand ParseRespCommandBuffer` 的 libs 内消费位是
   garnet/libs/server/Lua/LuaRunner.Functions.cs:3060（redis.acl_check_cmd 的成帧后命令名有效性判定），
   benchmark/BDN.benchmark/Operations/CommandParsingBenchmark.cs:100-:256 只是第二个消费者。rust 与之一一对位：
   wedb/wlua/src/functions/redis.rs:332 `session.parse_resp_command_buffer(scratch)` → 同文件 :333
   check_acl_permissions，接口声明 wedb/wlua/src/api.rs:26-28（注记「redis.acl_check_cmd 的有效性判定经会话
   解析单点承接」），会话侧承接件 wedb/wnode/src/resp/resp_server_session.rs 的
   `fn parse_resp_command_buffer`（取证时 :2834，该文件另有他棒在途未提交改动，行号以符号为准）。
   对该口加 cfg 门或登 ignore 不转写，都是把 C# 生产链砍掉，与票的边界自述冲突，也与「判落地只认现刻代码」
   下票内盘点补记（qw13.invA：「票面两口加门项失效」「残留仅 TestHook 族」）一致。
4. 因此「在 js/check/ignore 登记两口不转写」这个退路同样不成立：两口都在转写且有 C# 锚点
   （resp_command.rs:423、:433 的 `libs/server/Resp/Parser/RespCommand.cs:ParseRespCommandBuffer` /
   `:FuzzParseCommandBuffer`），登记不转写属反向造假；且 js/check/ignore/storage.yml、server 域语料为门禁
   逐字节比对件，不为零收益的登记去动它。

----------------------------------------------------------------------

主张三：active_instance_count / reset_all_instances「按新形态本不该公开」
原文（票面第 11-15 行转录）：同文件 :187 active_instance_count、:196 reset_all_instances 两口是另一形态……
rust 因 :161-:165 已声明的「实例 ID 单调分配、永不复用」差异取消了上限分支，于是计数与复位两口都退成纯用例面
——随差异消失的是 C# 侧的真消费位，两口按新形态本不该公开。

拒绝理由（C# 侧两口本就是 public，rust pub 是可见性对标而非后门）
1. 可见性事实：garnet/libs/storage/Tsavorite/cs/src/core/Epochs/LightEpoch.cs:219
   `public static int ActiveInstanceCount()`、:233 `public static void ResetAllInstances()`；
   garnet/libs/client/LightEpoch.cs:204、:218 同形。它们是 C# 对外的 public 静态口，不是主张一里那种
   internal 测试面；rust 的 pub 与 C# 的 public 严格同档，降 pub(crate)/加门反而制造 C# 没有的可见性差异
   （spec：尽量 1:1 对标 C#，不实现自己的优化）。
2. 消费面事实：C# 这两个 public 口的读者不止「自身异常消息」。除 LightEpoch.cs:212 的
   InvalidOperationException 文案外，还有 garnet/test/standalone/Garnet.test/TestUtils.cs:1357、:1363、:1368、
   :1372，garnet/libs/storage/Tsavorite/cs/test/TestUtils.cs:339、:343，
   cs/test/test.hlog/NativeHashIndexTests.cs:483、:507，以及
   garnet/benchmark/Resp.benchmark/OfflineBench/AOFBench/AofBench.cs:83 的诊断打印——即「测试与诊断」双用面，
   C# 文档注释自述 :216「Number of active LightEpoch instances. Used for testing and diagnostics.」。
   rust 侧读者 wepoch/tests/epoch/concurrency.rs:427、:433、:435（饱和递减回归用例）正落在同一档，
   写侧在生产（epoch.rs:168 new 自增、:789 Drop 的 checked_sub 饱和递减），非零消费者死面。
3. 门禁事实：js/check/ignore/storage.yml:1077-1086 对 core/Epochs/LightEpoch.cs 的整文件登记理由写的是
   「能力 1:1 落地」，删口或降级可见性会打破该登记口径；同形判词已有先例——task/reject/agy.db.md 条 17 判
   「C# 同样保留该重载……删它或降级其可见性反而失去对标并打断测试面」。

----------------------------------------------------------------------

原票全文留存（转录，未改一字）

优先级：低
分拣注记（qw.design 第 11 轮条 5 拆出；浅核 2026-09-19：wepoch epoch.rs:721 test_hook_this_thread_entry、:196
reset_all_instances 与 wnode parser/resp_command.rs:409/:425 两口在场；与批五（zero-consumer-dead-surfaces-batch-five）
符号不重叠、与 ing/gate-anchor-drift-reclean.md（锚点漂移清理）不同题）

测试/调试专用面缺 cfg 门两族：wepoch TestHook 系列由 C# internal + 独立分部文件降级为生产 pub 成员，wnode
命令缓冲旁路解析器两口 C# 仅 benchmark/fuzz 消费而 rust 无门常驻
问题：wepoch/src/epoch.rs:721 test_hook_this_thread_entry、:727 test_hook_this_thread_announced_epoch、
:736 test_hook_announced_epoch_at、:746 test_hook_thread_id_at、:756 test_hook_drain_list_capacity
五口生产零消费者（读者仅 wepoch/tests/epoch/{protection,support,concurrency,drain}.rs），
且以 pub 挂在主 LightEpoch impl 上；C# 对位件是独立文件 LightEpoch.TestHooks.cs，五个口全为 internal、
文件头明确「used only by the unit tests in Tsavorite.test.epoch」，可见性门与文件门双缺一处。
同文件 :187 active_instance_count、:196 reset_all_instances 两口是另一形态：前者写侧在产（:168 自增、
:777 自减），后者只被 :197 自身写，两口读侧全部只有用例；C# ActiveInstanceCount（libs/client/LightEpoch.cs:204）
的真读者是其自身分配失败消息（:197 拼进 InvalidOperationException），rust 因 :161-:165 已声明的「实例 ID 单调分配、
永不复用」差异取消了上限分支，于是计数与复位两口都退成纯用例面——随差异消失的是 C# 侧的真消费位，
两口按新形态本不该公开。
wnode/src/resp/parser/resp_command.rs:409 parse_resp_command_buffer、:425 fuzz_parse_command_buffer 两口
同样生产零消费者（读者仅 wnode/tests/resp_command_parse.rs:371-:381），而两口共享同一段
「save_receive_state → 清 recv_buffer → 灌入旁路缓冲 → 复位 read_head → restore → 清 parse_violation」
样板（骨架双抄），且直接改写主会话接收缓冲做旁路解析；C# 侧同名两口的唯一消费者是
benchmark/BDN.benchmark/Operations/CommandParsingBenchmark.cs:100+ 与 fuzz 测试，本仓不转写 benchmark。
修法：六 TestHook 口补 #[cfg(test)]（或降 pub(crate) 并单列 test_hooks.rs 模块，对位 C# 文件门），
两口旁路解析器补门（#[cfg(any(test, feature = "bench"))]）并抽共享 save/restore 骨架，
或在 js/check/ignore 登记两口不转写。
c#：garnet/libs/storage/Tsavorite/cs/src/core/Epochs/LightEpoch.TestHooks.cs:7（文件头「used only by the unit
tests in Tsavorite.test.epoch」）、:15、:20、:29、:34、:39（五口全 internal，且 :39 为 static）；
garnet/libs/server/Resp/Parser/RespCommand.cs:ParseRespCommandBuffer、:FuzzParseCommandBuffer，
消费者 garnet/benchmark/BDN.benchmark/Operations/CommandParsingBenchmark.cs:100、:109、:118（仅 benchmark 一处）
边界：批五类二登记的是 #if DEBUG / [Conditional("DEBUG")] 两类门（whyperlog regs、wlua debug_check），
本条是 internal→pub 与 benchmark-only 两类不同门，符号不重叠。

盘点补记（qw13.invA testhook-and-bypass-parser-cfg-gate）：dev e75716e 复核，增量缩窄：旁路解析器两口已转生产链
（resp_command.rs parse_resp_command_buffer/fuzz_parse 经 with_bypass_buffer 单点承接，resp_server_session.rs:2784
与 wlua/functions/redis.rs:332 脚本域成帧解析在用，票面「两口加门」项失效）；残留仅 wepoch/src/epoch.rs:738-774
test_hook 五口（另 active_instance_count/reset_all_instances）仍裸 pub 无 cfg/可见性门，全仓 src 非测试消费零。
修法收敛为 TestHook 族单独加门或删。

----------------------------------------------------------------------

交叉与后续
1. 票内「盘点补记」对旁路解析器两口的缩窄结论正确（本档主张三/主张二第 3 点即其证据），但它对 TestHook 五口
   「仍裸 pub 无 cfg/可见性门」的残留判定与现刻代码不符——epoch.rs:734 的门自 root 提交即在
   （`git log --oneline -- wedb/wepoch/src/epoch.rs` 全史仅 31c2388 init，仓库为 98 提交、root 即全量入库，
   无从判「后补」，只认现刻代码即成立）。
2. 边界自述复核：批五/批六（zero-consumer-dead-surfaces-batch-five/six，后者在 task/ing/ 在途）与本票符号集
   test_hook_* / parse_resp_command_buffer / fuzz_parse_command_buffer / active_instance_count /
   reset_all_instances 不重叠；若批六日后另立「pub 但读者仅测试」条目，须先按本档主张三读 C# 可见性再动手，
   勿把 C# public 对标件当死面删。
3. 下一轮复审若再触同题，取证入口就是本档三处判据：epoch.rs:734 的门 + 该 crate 的 release 反证 E0599、
   resp_command.rs:413 的单点骨架与 :440 的门、C# LightEpoch.cs:219/:233 与 LuaRunner.Functions.cs:3060。
