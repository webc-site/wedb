gossip 抽样发送配额语义与 C# 分叉：登记刻意差异声明（现无声明）

来源：glm.net 第 3 条（分拣判定成立，采「声明」案）。取证基线：主仓 HEAD 1b944517，行号为当下实况。

现状
- rust：/Users/z/git/db/wedb/wedb/wedb/src/server/gossip/gossip_manager.rs:281-308 sample_gossip_send
  以 `for _ in 0..count {`（:289）固定次数封顶，:301-306 的 match 两支（Some(conn) → try_gossip、
  None → break）都不回补配额，即成功与失败各耗一轮，抽样模式下每轮至多发 count 个节点
  （count 计算 :287，`(total * percent/100).ceil().clamp(1,total)`，与 C# :477-478 的
  `Math.Max(Math.Min(1,nodeCount), fraction)` 同界）。
- C#：/Users/z/git/db/wedb/garnet/libs/cluster/Server/Gossip/Gossip.cs:474-522
  `while (count > 0)` 环内，成功支 :503-507 为 `gossipStats.gossip_success_count++; continue;`，
  continue 跳过循环尾 :521 的 `count--;` —— count 只在超时（:509-512）与异常（:513-518）后递减。
  环的自然终止条件是 :496 `if (currNode == null) break;`：本轮已发过的节点其 GossipSend 已推进到
  startTime 之后，不再被 :489 的 `c.GossipSend < minSend` 选中。净效果是「全部抽样成功时一轮可对
  最多 nodeCount 个节点发 gossip」，即实际扇出可超出 fraction。
- 差异无登记：gossip_manager.rs:280 文档注释只挂 C# 锚点，未点名该相对位置差异。

判定
分叉真实存在且静默。C# 该形态按 fraction（抽样百分比）的命名意图看疑似无意，但转写纪律是
「1:1 对标，或点名刻意差异」，二者必居其一；本仓同类先例为按可达路径登记差异声明
（done/hexpire-denied-arm-declaration、task/ing/count-get-keysinslot-islocal-note）。

修法（采声明案，不改行为）
1. sample_gossip_send 文档注释补一段刻意差异声明：点名 C# 的 continue 与 count-- 相对位置
   （Gossip.cs:503-507 vs :521）使成功不耗配额、终止靠 currNode==null，rust 采固定 count 封顶
   以保持每轮扇出上界可预测、gossip 统计口径与 gossip_delay 周期解耦。
2. 若后续实测集群收敛速度不足需对齐 C#：改 while 环并把 try_gossip 的 bool 回报
   （gossip_manager.rs:321 返回值，现 :303 丢弃）接进递减位，成功 continue / 失败或超时 count--，
   同时保留 :305 None → break 的终止条件。此路线须独立复核不会退化为无界环，不随本票默认落地。

优先级
打磨（行为差异登记；无正确性缺陷，扇出上界为收敛速度差异）。

C# 参考
- /Users/z/git/db/wedb/garnet/libs/cluster/Server/Gossip/Gossip.cs:474-522 GossipSampleSendAsync
  （抽样开关与 maxRandomNodesToPoll=3 见同文件 :486 与 rust :292）。

验收
- 该函数文档注释含差异声明且点名 C# 两处行号的相对位置；函数体零 diff（git diff 仅注释）。
- ./js/check.js 无新增缺失或虚构锚点（锚点仍指 GossipSampleSendAsync）。

细化方案（实现代理，2026-09-19）
- 勘误：C# `count--;` 实测在 Gossip.cs:520（票中原写 :521）；成功支 :503-507、终止条件 :496 `currNode == null` 确认无误。
- 核实：rust sample_gossip_send 唯一定义 gossip_manager.rs:281，调用方仅 gossip_step_async；
  测试 wedb/tests/gossip_manager.rs::test_sample_round_no_duplicate_pick_removal 不动。
- 改动：仅 gossip_manager.rs:280 锚点下方追加中文差异声明段（对标本文件 broadcast_gossip_send 注释风格），
  内容三点：C# 成功支 continue 跳过 count-- 使成功不耗配额、终止靠 currNode==null（成功节点 GossipSend
  已推进过 startTime 不再入选，净扇出可超 fraction）；rust 采固定 count 封顶，每轮扇出上界可预测、
  统计口径与 gossip_delay 周期解耦；函数体与测试零 diff。
- 本票不落地备选对齐路线（while 环 + try_gossip bool 接递减位），仅声明。
