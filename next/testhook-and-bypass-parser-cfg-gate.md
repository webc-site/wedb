优先级：低
分拣注记（qw.design 第 11 轮条 5 拆出；浅核 2026-09-19：wepoch epoch.rs:721 test_hook_this_thread_entry、:196 reset_all_instances 与 wnode parser/resp_command.rs:409/:425 两口在场；与批五（zero-consumer-dead-surfaces-batch-five）符号不重叠、与 ing/gate-anchor-drift-reclean.md（锚点漂移清理）不同题）

测试/调试专用面缺 cfg 门两族：wepoch TestHook 系列由 C# internal + 独立分部文件降级为
生产 pub 成员，wnode 命令缓冲旁路解析器两口 C# 仅 benchmark/fuzz 消费而 rust 无门常驻
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
