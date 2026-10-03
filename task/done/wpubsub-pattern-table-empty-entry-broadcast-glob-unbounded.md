甄别结论：通过（2026-09-29 主控甄别，定级 P3——broadcast subscribe_broker.rs:274-288 模式臂无 is_empty 过滤空集条目照跑 glob_match，同表 for_each_pattern :491/num_pattern_subscriptions :570 均有过滤，两种过滤形态坐实；C# SubscribeBroker.cs Broadcast :93-114 无条件 Match 空集照跑。修复：broadcast 空集短路零行为变化优侧增益，外层键收缩不做维持克制）

审核结论：通过（2026-09-29 甲轮35-B，P3 级）。broadcast 无空集过滤 vs for_each_pattern 有过滤臂单机制不闭环坐实；C# 上游同病（Broadcast 每条目无条件 Match）系优侧纯增益谱系（§108 在册）；空集投递本为零短路零行为变化。执行席遵照：外层键收缩维持「非必选、做须单独裁决」克制表述勿顺手做。

原票面：
模式表空条目零收缩且发布广播不跳空集：PUBLISH 热路径对全历史模式条目照跑 glob 匹配无界退化

问题分析：
1 Garnet 契约对齐：C# patternSubscriptions（ReadOptimizedConcurrentSet<PatternSubscriptionEntry>）只在 PatternSubscribe 时 TryAddAndGet，PatternUnsubscribe 与 RemoveSubscription 仅摘内层集合元素、外层条目终身驻留（garnet/libs/server/PubSub/SubscribeBroker.cs:206-216、:240-252、:56-74）；Broadcast 对每个条目无条件先 Match(key, pattern) 再遍历订阅集（同文件 :93-114），空集条目同样付出 glob 匹配成本——上游同病灶。
2 工程现状确证：rust 同构承接零收缩（wedb/wpubsub/src/subscribe_broker.rs pattern_unsubscribe 与 remove_subscription 仅摘内层、绝不删外层模式键，注释自证对标 C# 形态），广播臂 broadcast 对每个模式条目先 glob_match 后投递（subscribe_broker.rs:274-288），未复用同表另两消费点 for_each_pattern 与 num_pattern_subscriptions 已有的 entry.subscriptions.is_empty() 过滤臂——同表三消费点两种过滤形态，单机制不闭环。
3 逻辑危害确证：服务器生命周期内历史订阅过的全部去重模式键永不清除（C# 同病），每次 PUBLISH（数据面热路径）遍历全历史条目并对空集条目照跑 glob_match，长期运行发布成本随历史模式总数无界增长（O(历史模式数) × O(模式长)），无内存上限亦无遍历跳过；违审查板块 3.2 消除无界开销与 3.1 单次遍历收敛纪律。空集条目投递本就为零，glob 短路属零行为变化的 rust 优侧纯增益（同 §108 rust 优侧登记谱系）。

涉及代码：
rust 文件与函数：
wedb/wpubsub/src/subscribe_broker.rs:broadcast（模式臂 glob_match 前无空集过滤）、pattern_unsubscribe、remove_subscription（外层键零收缩）、for_each_pattern、num_pattern_subscriptions（同表既有 is_empty 过滤臂）

对应 c# 文件与函数：
garnet/libs/server/PubSub/SubscribeBroker.cs:Broadcast（:93-114 空集条目照跑 Match）、PatternUnsubscribe（:240-252）、RemoveSubscription（:56-74）

精炼执行方案：
1 broadcast 模式臂在 glob_match 前先 entry.subscriptions.is_empty() 短路跳过（与 for_each_pattern / num_pattern_subscriptions 收敛为同一过滤单源，空集条目本就零投递、零行为变化）
2 list_all_pattern_subscriptions 的 with_capacity(pattern_subscriptions.len()) 容量预估含空条目偏大，随臂一顺带改按过滤后计数（非阻塞项）
3 测试：wpubsub 单测补 PSUBSCRIBE→PUNSUBSCRIBE 后 publish_now 仍命中其余模式且空条目零通知零成本路径回归；外层键收缩（PUNSUBSCRIBE 摘空删键）涉 C# 外层键驻留的刻意形态（RemoveSubscription 注释自证），非本票必选，如做须单独裁决

终态注记（2026-09-29 执行席收口）：
合入 39cf063（merge c7fd22b，分支 fix-pubsub-pattern-skip）。收口形态：subscribe_broker.rs broadcast 模式臂 glob_match 前补 entry.subscriptions.is_empty() 短路，与 for_each_pattern / num_pattern_subscriptions 读侧过滤收敛同一单源；零行为变化（空集投递本为零），纯消除 PUBLISH 热路径对历史模式条目的无界 glob 成本。测试 tests/broadcast_empty_pattern_skip.rs 双路径回归锚（PUNSUBSCRIBE / remove_subscription 空集驻留：零投递零虚计、邻条目照投、同键再订阅恢复）。cargo check --all-targets 零警告。外层键收缩未做（票面克制维持）；方案第 2 点（list_all_pattern_subscriptions 容量预估）为非阻塞项未顺带做。
