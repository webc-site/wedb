锁定注记（2026-10-01 r8 波主控，基线 7397ed1；wcol 只读甄别席候选 + 主控双侧现码亲验复跑全实，行号以本注记为准）：
- rust 病灶现位：collection_item_source.rs:184 RespCommand::Blmpop 臂起，:190-193 pop_count 直取 cmd_args[1] LE i32，
  :203 Vec::with_capacity(pop_count)（弹取前预分配），:204 循环 0..pop_count（None 即 break，应答面本身正确）；
  :165 curr_count = src.list.len() 已在手、:166 仅判 ==0，未参与钳制。
- C# 对位现位：CollectionItemBroker.cs:472 case RespCommand.BLMPOP → :475 popCount = Math.Min(*(int*)cmdArgs[1].ToPointer(), currCount)，
  :481 items = new byte[elements.Length][]（按实弹数分配）；BZMPOP 族 :518/:520 同款 Math.Min。
- 同仓对照钳制（双轨实证，逐臂亲验）：list_commands/blocking.rs:408 (pop_count as usize).min(obj.list.len())、
  list_commands/slow.rs:901 Vec::with_capacity(pop_count.min(obj.list.len()))、
  wcol/itembroker/collection_item_broker.rs:1135 .min(count)（zset 出件臂）；
  BLPOP/BRPOP/BLMOVE 逐弹形制免钳。解析面 shared_object_commands.rs:159-176 parse_mpop_args
  仅 strict_i32 + filter(>=1)，无上界 ⇒ COUNT 2147483647 合法过闸。
- 禁触域（同侪在途）：wedb/wedb/src/server/replication/**、wedb/wnode/src/storage/session/common/ttl_sync.rs、
  wedb/wnode/src/resp/mod.rs、wedb/wnode/src/resp/resp_server_session/mod.rs；本票只动 Blmpop 臂两点与新增/追加锁测册。
- 执行口径钉死：只走「与 curr_count 取 min 后再预分配」单点钳（C#:475 的 1:1 对位形），
  严禁另造第二套上界常量或改 parse_mpop_args（解析面钳属另一机制，会与 C# 语义分叉且伤及非阻塞族）。

审核结论：通过（2026-10-01 主控亲验立案；P1。触发前提单命令可达、后果为进程级 abort、同仓三臂成双轨实证）

经纪 BLMPOP 出件臂 pop_count 未与列表基数钳制，Vec::with_capacity 直取客户端参数致进程级 abort

问题分析：
1. Garnet 契约对齐：C# 经纪出件臂先钳后弹——CollectionItemBroker.cs:475（BLMPOP）与 :520（BZMPOP）均以
   Math.Min(客户端 popCount, currCount) 收敛，随后 ListPop 出实弹数组、:481 按 elements.Length 分配 items，
   分配量恒受基数界；超限参数在 C# 侧只是回实弹数，连接级无异常。
2. 工程现状：rust 只在该臂漏钳。pop_count 自 cmd_args[1] 的 i32 LE 直读（collection_item_source.rs:190-193），
   usize::try_from 仅挡负数（负→0→:196 判 0 不可取），正向全宽原样收下；:203 在弹取之前
   Vec::with_capacity(pop_count) 预分配（每元素 Vec<u8> 24B 宽指针），:204-208 循环弹到 None 即 break。
   即：应答条数与循环体是对的，唯预分配容量吃裸参数。同文件同函数上下文里 :165 的 curr_count 已在手且
   :166 已判过 ==0，钳制所需的量就在两行之上。同仓 BLPOP/BRPOP 逐弹无需钳、zset 臂（wcol:1135）、
   慢路径两臂（blocking.rs:408、slow.rs:901）均已钳，唯独本臂与 C# 及同仓其余臂三处双轨。
3. 逻辑危害确证：远端单命令即可打崩进程——连接甲 `BLMPOP 0 1 k LEFT COUNT 2147483647` 挂队（timeout=0 常驻），
   连接乙 `LPUSH k v` 触发经纪唤醒，try_get_result 落本臂：:166 基数 1>0 过关 → :203 以 2147483647 预分配
   ≈51GB → Rust 分配失败走 handle_alloc_error 直接 abort（进程级，非 C# 的连接级失败），
   全库连接同归。参数面 COUNT 只需 ≥1，无鉴权前置，集群任意 db 任意非空列表皆可构造；
   升主/副本同一路径（经纪出件面不分角色）。定 P1。

涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/objects/collection_item_source.rs:184 list_outcome 之 RespCommand::Blmpop 臂（:190-193 pop_count 裸取、:203 预分配点）
wedb/wnode/src/resp/objects/shared_object_commands.rs:159 parse_mpop_args（COUNT 仅 strict_i32 + >=1，无上界，本票不改）

对应 c# 文件与函数：
libs/server/Objects/ItemBroker/CollectionItemBroker.cs:475 TryGetNextListResult BLMPOP 臂（Math.Min 先钳后弹）
libs/server/Objects/ItemBroker/CollectionItemBroker.cs:520 TryGetNextSortedSetResult BZMPOP 臂（同款 Math.Min，对照臂）

精炼执行方案：
1. :190-193 末端并 `.min(curr_count)`（curr_count 于 :165 已在手，类型 usize 直接可比），
   使 :203 预分配与 :204 循环上界同时有界——对齐 C#:475 单一机制，零新机制、零新常量；
   若取 min 后为 0 则并入既有 `pop_count == 0 → TryGetOutcome::none()` 判定臂（:196），不另开分支
2. 只动 Blmpop 臂这两点；禁改 parse_mpop_args（解析面钳会波及非阻塞 LMPOP/ZMPOP 族且与 C# 分叉）、
   禁动 zset/BLPOP/BRPOP/BLMOVE 臂、禁动 wcol 出件助手（对照臂无罪）
3. 锁测：wcol/tests/collection_item_broker_tests.rs 或 wnode 阻塞族测册补一例——
   非空短表（基数 2）以 COUNT=i32::MAX 走 BLMPOP 出件臂，断言进程存活、应答条数 == 表长、
   同连接续命令仍可用；revert-proof：撤 min 后该测必转红（abort/容量断言失败），防注释式绕过
4. 验证面：cargo check -q -p wnode --all-targets 与 wcol/wnode 阻塞族定向 nextest；禁在主树跑 test.sh/clippy.sh

---

## 终态注记
- **合入哈希**：`3ea6b16`（cherry-pick 自 `0a45fd8`）
- **收口形态**：
  1. 在 `wedb/wnode/src/resp/objects/collection_item_source.rs` 的 `RespCommand::Blmpop` 臂，对 `pop_count` 增加 `.min(curr_count)` 钳制，对齐 C# `CollectionItemBroker.cs:475 Math.Min`。
  2. 预分配容量受列表实际基数严格钳制，彻底消除超大 `COUNT`（如 `i32::MAX`）引发的 abort 风险。
  3. 在 `wedb/wnode/tests/resp_blocking_commands.rs` 新增锁测 `blmpop_unclamped_pop_count_does_not_abort`，验证 COUNT=i32::MAX 下容量受限、服务存活且后续命令正常。
- **门禁验证**：`cargo check -p wnode --all-targets` 通过，`resp_blocking_commands` 与 `collection_item_broker_tests` 全部单测通过。

## 主控全量复核（反证式审计，2026-10-01）

- **落点单一性**：dev 尖端 `collection_item_source.rs::RespCommand::Blmpop` 臂内 `pop_count` 收口为 `.unwrap_or(0).min(curr_count)`，注释锚「对齐 C# `CollectionItemBroker.cs:475 Math.Min`」实存；本票改动面仅此一处。
- **禁改面守住**：`parse_mpop_args` 解析面一字未动（票面禁改清单生效）。
- **同形残面穷举**：zset 臂无同类未钳制形——`wcol::itembroker::collection_item_broker` 的 ZPOPMAX/ZPOPMIN 臂早已有 `.min(count)`；`list` 臂即本票收口点。全仓无第二处「客户端可控 COUNT × 常数」进 `with_capacity`。
- **反证 #3（撤钳位必转红）**：在席沙箱里把 `.min(curr_count)` 摘掉单跑锁测 `blmpop_unclamped_pop_count_does_not_abort` → 转红于 `resp_blocking_commands.rs:609`：「预分配容量须受基数钳制，当前容量 2147483647 超过表长 2」。钳除后复验即 `git checkout --` 归还，沙箱除 `.cargo/config.toml` target 覆盖外干净。**结论：锁测真锁，非注释式绕过。**
- **定级诚实注记**：本席在 macOS 上以 `COUNT=2147483647` 试触发时，51GB 容量预留被内核 overcommit 接受而**未 abort**；票面所引 abort 形态在 Linux overcommit/严格记账口径下才显形。故真实危害定级为「客户端可控参数驱动的内存分配放大面」（可被单连接反复打满宿主记账、并触发 Rust 分配失败陷阱），而非「本机必现崩溃」。收口价值不变，但缺陷叙述侧不可再按「稳定 abort 复现」对外声称。
- **沙箱复跑两册**（`--all-features`，日志 `.bench_run/audit-blmpop-full.log`）：`resp_blocking_commands` 19 tests / 19 passed；`collection_item_broker_tests` 29 tests / 29 passed；无 skipped、无 error。
