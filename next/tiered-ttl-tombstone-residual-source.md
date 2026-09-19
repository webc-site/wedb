优先级：功能缺口（正确性可达性——残余墓碑源仍能让单次树扫描按连跑长度压栈，整进程 SIGABRT）

单问题：task/ing/tiered-zset-demote-bf-tree-recursion-stack-overflow.md 的裁决 B 落地后，
分层集合的成员级 TTL 出账面仍向 bf-tree 写逐成员删除记录，即仍会累积连续墓碑连跑，
而 bf-tree 0.5.6 的 ScanIter::next 对「游标之后的每一条墓碑」压一帧（实测 ≈680 B/帧，
8MiB 默认栈边 ≈8000 条连跑），故 TTL 重的分层集合上一条客户端 HSCAN/ZSCAN/LPOP 仍可炸进程。

取证现状（2026-09-19 主代理在分支 fix-tiered-tombstone-density @ df35b160 上核得）
- 该分支按裁决 B 把四张分层快速通道表的删除重臂（HDEL / SREM+SPOP / ZREM / LPOP+RPOP）
  摘出，改走对象层「物化求值 + 整值重灌」（tiered_materialize_blob → 对象层 run_operate →
  apply_rmw_post_operate → bulk_load），其 commit message 自述：「残余墓碑源收敛为成员级
  TTL 物理出账（member_expire / member_persist / member_ttl_probe 三核 +
  collect_expired_members）」——即本票范围。
- 唯一残余树内删除漏斗：wnode/src/resp/objects/tiered_collection_ops.rs 的 `tree_del`
  （该分支 :170-174），调用者是同文件的 `member_expire_arm`（:258）、`member_ttl_probe`（:309）、
  `member_persist_arm`（:346）、`collect_expired_members`（:403）四处 TTL 面。
  合并到 dev 后行号会位移，按符号定位。
- 递归点与不可达修法的全量证据在
  /Users/z/git/db/wedb/task/reject/tiered-zset-demote-stack.md（栈帧归并表、规模分档、
  S1..S4 分段实验：紧上界键与 count 都不截断遍历，墓碑判定排在 bound_key 比较之前）。

修法要求
1 与裁决 B 同形、复用同一条既有通道，不新建机制：TTL 出账也走「物化求值 + 整值重灌」
  （删完即 bulk_load 重建），使分层树内不存在任何成员级删除记录 ⇒ 墓碑恒零、
  栈深自变量彻底消失。要一并核 `collect_expired_members` 的惰性过期探针
  （它是读路径上的顺带出账，改重灌要考虑读放大与锁窗口，给出实测代价）。
2 若某一 TTL 面确实无法走重灌（例如读路径探针不能整值写回），必须在回报里给出该面的
  实测残余风险边界（多大 TTL 批量、什么命令序列可触发炸栈），并明确标为未闭合，
  不接受「补一条注释说明有栈溢出风险」了事。
3 严禁：加栈大小、加阈值常量掩盖、测件里设 RUST_MIN_STACK、改测试规模。

改动域：wedb/wnode/src/resp/objects/tiered_collection_ops.rs 的 TTL 四核，
必要时 wkv 的 range_index 重灌面与 wnode/tests/ 相关测件。
禁止碰 wedb/wnode/src/service.rs、wedb/wtxn/**（并发会话在跑）。

验收
1 新建分层集合、对 >2×8000 个成员设 TTL 并等其批量出账（或直接驱动 collect_expired_members），
  默认栈（不设 RUST_MIN_STACK）下一条 `HSCAN key 0 COUNT 10` / `LPOP key 10` 不炸，
  给出实跑输出行。
2 `cargo check --all-targets -p wnode -p wbftree -p wkv` 零错零警告，禁 `#[allow(`。
3 全仓 grep `tree_del` 的调用面：若无残余调用者则整条漏斗连同 BfTreeDeleteResult 的
  死臂一起删净（不留只被测试引用的第二套删除面）。
