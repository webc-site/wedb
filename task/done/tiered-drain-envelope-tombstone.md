分层排空/删空链补信封域幂等墓碑，promote 删信封失败不再静默吞（双态残留键复活）

来源：next/glm.my.md 第 6 轮条之二（自定义优化上下游打通审查）。取证基线：主仓 /Users/z/git/db/wedb dev 工作树，行号为当下实况。
去重：并发分拣把同一原文照搬成六行 stub /Users/z/git/db/wedb/next/promote-dual-domain-write-atomic.md，
本档是其细化版（行号按当下代码重取），同题只此一份；认领开发时删该 stub，勿据 stub 另开分支。

## 现状

升阶收尾是两条独立物理写，非原子：/Users/z/git/db/wedb/wedb/wkv/src/range_index/stub.rs:107-200
promote_collection_to_bftree —— :186 `self.upsert_raw(&meta_k, &val).await?` 落元记录，
:188 `let _ = self.delete_raw(&env_k).await;` 删信封。中间窗口由三类事件打开：
崩溃/掉电、AOF 部分回放（RangeIndexStreamChunk 流块与信封 StoreDelete 墓碑之间截断）、
以及最廉价的信封删除 IO 失败（`let _ =` 静默吞错，函数照常返回 Ok）——三者都留下
「信封旧快照 + Meta 存根 + 树」双态且永久残留。

双态期命令路由 Meta 优先（/Users/z/git/db/wedb/wedb/wnode/src/resp/objects/object_store_utils.rs
分层态优先探测 Meta 的读写臂），读写走树无误；问题在删空链全部只清 Meta + 树 + TTL，不清信封：

- /Users/z/git/db/wedb/wedb/wkv/src/range_index/stub.rs:45-63 handle_bftree_drain_and_delete
  （drain_and_delete_collection_meta + delete_index + unregister_bftree_key + RangeIndexDrop，无信封臂）；
- 分层删空臂 /Users/z/git/db/wedb/wedb/wnode/src/resp/objects/tiered_collection_ops.rs:369-388
  drain_or_save（size==0 → handle_bftree_drain_and_delete(key,false)）；
- 通用内核删空臂 /Users/z/git/db/wedb/wedb/wnode/src/resp/objects/object_store_utils.rs:1019-1034
  apply_rmw_post_operate 的 is_empty && tiered 臂；
- RI 排空与 STORE 族清退同走该 drain 内核。

后果：双态键删空后 Meta 消失，命令回落到信封域（contains_key / read_tag_with 双域探测），
已删空的集合以升阶时刻的完整旧数据复活，EXISTS/HGETALL 全量可见——正是
/Users/z/git/db/wedb/.agents/skills/transpile/SKILL.md:44-45「严格删空生命周期与原子墓碑……杜绝幽灵空元记录」
要杜绝的幽灵形态。

同仓 DEL 臂证明双域清理是既知必要：/Users/z/git/db/wedb/wedb/wkv/src/session/collection.rs:25-64
delete() 在 drain 之后仍对 String 域与 ObjectEnvelope 域双域墓碑（:58-63），唯独分层排空链漏掉。

注释漂移：/Users/z/git/db/wedb/wedb/wkv/src/session/collection.rs:9-11 模块头宣称
「落元记录与删信封同帧完成」，与两条 await 写的实现直接矛盾（spec 漂移先例
spec-doc-drift-gcbarrier-ri-promote）；/Users/z/git/db/wedb/doc/zh/collection.md 5.3 节
「Checkpoint 快照与 AOF 重放器天然统一两态协议，重启无损恢复」同受该窗口影响。

## 修法

一处收口，与 DEL 复合臂同口径，不在各删空臂手抄：

1. handle_bftree_drain_and_delete（stub.rs:45-71）连带对信封域写幂等墓碑（键全程存活的
   keep_ttl=true 迁移臂按调用点决定是否清，见下条），使「分层态键消亡」必然同时清掉两个物理域；
   与 delete() 既有的双域墓碑臂共用同一实现，不新增第二条信封删除路径。
2. 迁移臂（重灌/懒降阶，keep_ttl=true）不消亡键：重灌臂随后 promote 内已删信封、降阶臂随后 obj_save
   写回新信封，两臂不得因第 1 条改动出现「先墓碑后写回」的自相残杀；以 keep_ttl 参数
   （或改名后的存活标志）分流是否清信封，口径写进函数文档。
3. promote 的信封删除失败不再 `let _ =`：IO 硬错上抛令升阶命令失败（与 :177-182 emit 失败回滚在线树、
   换号登记的既有回滚臂同型），或在无法回滚时至少 error 级留痕并保证第 1 条的墓碑幂等可收敛——
   二者择一，禁止静默返回 Ok。
4. collection.rs:9-11 与相关注释按真实写序改写（先发流、再落 meta、后删信封的三步序），
   禁靠改注释绕检。

## 边界

分层重灌「先拆后建」的崩溃丢键窗口另票（task/ing/tiered-reflush-atomic-swap.md，本票不碰其建树次序）；
树内多步写臂竞态归 next/tiered-write-arm-concurrency.md；本票只管两态残留与信封清理收口。

## 验收判据

- 新增用例：构造双态键（promote 后手工保留信封副本或注入信封删除失败），走 HDEL/SREM/ZREM/LPOP
  删空后 EXISTS=0、HGETALL 空、无旧数据复活；DEL 路径行为不变。
- 迁移臂用例：重灌与懒降阶后信封/Meta 域状态与改前一致（不因新墓碑丢存活键）。
- 信封删除失败可见（error 日志或用例断言），不再静默 Ok。
- cargo check --workspace --all-targets 绿；中文注释、禁 #[allow]。

优先级：P1（幽灵复活是用户可见数据错误，但触发需双态窗口先成立；修点小、口径清晰，宜与重灌票并行前置）。
