分层重灌臂改先建后拆：复用 publish_tree_from_snapshot_locked 原子换入，消除键蒸发窗口

来源：next/glm.my.md 第 6 轮条之三（自定义优化上下游打通审查）。取证基线：主仓 /Users/z/git/db/wedb dev 工作树，行号为当下实况。
去重：并发分拣把同一原文照搬成六行 stub /Users/z/git/db/wedb/next/tiered-reload-atomic-publish-swap.md，
本档是其细化版，同题只此一份；认领开发时删该 stub，勿据 stub 另开分支。

## 现状

apply_rmw_post_operate 的重灌分支（分层键物化写回的常规路径，GEOADD 与未支持原生树内操作的对象穿透均达）
执行序是先物理销毁再重建：

- /Users/z/git/db/wedb/wedb/wnode/src/resp/objects/object_store_utils.rs:1035-1057
  `else if obj.should_promote() || (tiered && !obj.should_demote())` 分支内，tiered 时
  :1041 `handle_bftree_drain_and_delete(key, true)` —— 元记录物理墓碑 + delete_index 摘注册并投递树文件删除
  （/Users/z/git/db/wedb/wedb/wkv/src/range_index/stub.rs:45-71）；
- 随后 :1046-1051 `promote_collection_to_bftree(key, tag, entries)` 建新树、发流、落新 meta。
- 可达面示例：/Users/z/git/db/wedb/wedb/wnode/src/resp/objects/sorted_set_geo_commands.rs:736 obj_writeback_tiered。

两步之间崩溃即：旧 meta 已墓碑、旧树已投删、新 meta 未落、信封本就不存在（键早已是分层态）——
重启后键整体消失，孤儿树文件无引用残留（仅同名重建时被
/Users/z/git/db/wedb/wedb/wbftree/src/manager/lifecycle.rs:111-118 的旧世代工件清理顺带回收）。
AOF 完整时可经 RangeIndexDrop + RangeIndexStream 重放找回；无 AOF / 嵌入式 / AOF 尾部截断即数据全丢。
与 /Users/z/git/db/wedb/doc/zh/collection.md 5.3「重启无损恢复」和
/Users/z/git/db/wedb/.agents/skills/transpile/SKILL.md:27-30「自动透明就地升阶」不符。

先拆后建的根因是注册表拒重：lifecycle.rs:105-109 create_bftree_internal 对 live_indexes 已有条目
回 Err(IndexExists)，迫使重建必须先摘旧树。

同仓已有原子换入内核未被复用：lifecycle.rs:440-470+ publish_tree_from_snapshot_locked(key, snapshot_path, replace)
——临时路径建树 → Unix rename 原子换入（:431-439 注释自陈换入窗口内无文件缺失间隙）→ 数据 + 目录
fsync 双屏障 → replace=true 时锁内 remove_and_take_tree + dispose_bftree_deferred 排空旧树。
C# 侧等价操作（对象记录重写）单条日志原子完成、无销毁重建窗口
（garnet/libs/server/Storage/Functions/ObjectStore/VarLenInputMethods.cs:33-44 GetRMWModifiedFieldInfo），
MIGRATE 发布对位件为 garnet/libs/server/Resp/RangeIndex/RangeIndexManager.Replication.cs:352 PublishMigratedIndex。

## 修法

把崩溃窗口从「数据丢失」降为「旧域泄漏」，且只有一套换入内核：

1. 重灌分支改「先建后拆」：新数据灌临时树 → snapshot_tree_to_path → publish_tree_from_snapshot_locked(
   key, snap, replace=true) 原子换入新树 → upsert_raw 新 meta → 最后（换入成功后）投递旧树文件释放
   与信封清理。旧树文件释放由 replace=true 的锁内排空承接，不再先调 handle_bftree_drain_and_delete。
2. promote_collection_to_bftree 拆出可复用的「建树 + 灌入 + 快照」前段与「发布 + 落 meta + 清理」后段，
   首升阶（无旧树，replace=false）与重灌（replace=true）共用同一函数，禁在重灌臂另写一份建树工序；
   换号旁表登记（:156-157 register_bftree_key）与 RangeIndexStream AOF 入账次序在两态下保持现有语义
   （先发流后落 meta 的既有不变量不得因换序破坏）。
3. 中途失败的收敛：发布/meta 落盘失败按 :177-182 既有回滚臂同型处理（回滚到旧树在位的原态或
   error 留痕 + 上抛），禁止 `let _ =`；重灌臂的 keep_ttl 语义不变（键全程存活、零 TTL 事件）。
4. create_bftree_internal 的 IndexExists 拦截（lifecycle.rs:105-109）保留——它是首升阶防重的正确门，
   重灌改走 replace 通道后不再需要绕过它；不新增第二条「强制覆盖建树」旁路。
5. WATCH 推进与 AOF 事件仍一命令一次（apply_rmw_post_operate 头注 :996-1002 的分工不改）。

## 边界

与 task/ing/tiered-drain-envelope-tombstone.md（排空/删空链补信封墓碑、promote 吞错）同函数相邻，
两票改点集中在 object_store_utils.rs:1035-1057 与 stub.rs:45-200，须串行落地：先修墓碑票，再做本票换序，
避免互踩；树文件删除的纪元/守卫前置检查归 next/bftree-release-detached-guard-recheck.md，本票不重开。

## 验收判据

- 用例：分层大键触发重灌（GEOADD 或体积过阈值写）后 kill -9 / 模拟发布前失败并重启，键仍可读且为
  旧快照或新快照之一，绝不整体消失；无 AOF 形态同断言。
- 用例：重灌后同名继续升阶/降阶往返 N 次，live_indexes 与磁盘树文件无泄漏（孤儿文件计数为 0）。
- grep 取证：重灌路径不再调用 handle_bftree_drain_and_delete(…, true) 做销毁重建；
  建树+发布仅 publish_tree_from_snapshot_locked 一条内核。
- cargo check --workspace --all-targets 绿；中文注释、禁 #[allow]。

优先级：P1（数据丢失级，但窗口窄于 RMW 票且需分层态触发；排在墓碑票之后）。
