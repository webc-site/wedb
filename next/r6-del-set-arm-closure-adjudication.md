# SET 臂闭合策略统筹：键闩串行 vs RMW 写回前复验

来源：r6-del 问题 2（task/ing/r6-del-expire-purge-key-latch.md 中断实录）与
r6-del 问题 3（已合并 ff0dbba6）的互斥后果。

## 冲突事实

两票分别落了两套 SET 臂策略，互斥不可共存：

1. 问题 3 已合并面（dev ff0dbba6）：RMW 落笔前复验信封存活与 String 域缺席，
   失配即弃写；对面 SET/DEL 不取 rmw 窗口，允许交叠发生，由复验兜住。
   回归测试 wnode/tests/rmw_writeback_revalidate.rs 的
   sync_window_concurrent_set_keeps_single_domain 与
   async_window_concurrent_set_keeps_single_domain 断言的正是
   「SET 能在 RMW 持窗期落笔并回 +OK，随后 RMW 弃写」。
2. 问题 2 分支面（parked 于 r6-del-expire-purge-key-latch，未合并）：
   把 SET 的 String 域 del_ttl 清退臂纳入同键主桶独占闩，
   SET 与对象 RMW 在同键上串行，交叠不再可能发生。
   并入后上述两条用例转红。

## C# 裁决依据

C# 只有一把记录 X 锁：对象族的 CopyUpdater/InPlaceUpdater
（libs/server/Storage/Functions/ObjectStore/RMWMethods.cs）与
主存 UpsertMethods.cs:InPlaceUpdater、HandleExpireInPlaceUpdate/HandlePersistInPlaceUpdate
全在同一记录锁内完成读改写。也就是说 C# 里 SET 与对象 RMW 对同一键根本不可能交叠，
「写回前复验」这层防御在 C# 无对应物，它是复现记录锁不可行时的替代物。

因此两票的取舍方向应当是：以键闩串行（问题 2 面）为正解靠近 C#，
复验（问题 3 面）保留为闩覆盖面之外的兜底而非主防线；
而不是两套策略各自为政、彼此把对方的用例砸红。

## 方案

1. 以 C# 记录锁的覆盖面为准，明确 rust 键闩应当覆盖哪些写入口
   （对象 RMW 装载→operate→写回、SET/MSET 的 String 域与 TTL/信封清退臂、
   DEL、EXPIRE/PERSIST 同步臂），列成一张覆盖面清单并对齐 C# 记锁语义。
2. 问题 2 分支（三段提交，编译干净，wkv ttl/ttl_purge/ttl_purge_latch/gc 全绿）
   重放到新尖，并把 rmw_writeback_revalidate.rs 两条用例的交叠构造改为
   「闩外 actor」（不取闩的路径）或改断言为串行后的实际序次，
   不得为过测试而删掉复验层。
3. 若裁定保留复验为主防线，则回退问题 2 的 SET 纳入同闩一步，
   并在 reject/done 侧登记「SET 与对象 RMW 允许交叠」为已声明偏离，
   同时把过期清退链的闩单独保留（该部分与 SET 臂无关）。
   两法择一，禁止现状的互相矛盾。

## 验收

1. cargo check -p wkv -p wnode --tests 零 error 零 warning。
2. 两票的测试同时为绿：rmw_writeback_revalidate.rs 全部用例 +
   wkv 的 ttl_purge_latch 系列 + 过期清退并发用例，无一被放宽或删除。
3. 在 doc/zh/collection.md 或 wnode 模块注释处写明该键的并发闭合模型
   （闩覆盖清单 + 复验的角色），后续轮次免再撞。
