分层升降阶迁移臂保留键级 TTL（keep_ttl 分流）

【拒绝：已实现】票据方案与验收判据在当前 dev 已全部落地（历史经 squash 混入
8fc7b609，2026-09-19），无代码可写。甄别基线：dev HEAD 0cd7d5d5 逐点核对：

- keep_ttl: bool 已贯通 wkv/src/session/collection.rs:215
  drain_and_delete_collection_meta（`if !keep_ttl { del_ttl }`）与
  wkv/src/range_index/stub.rs:50 handle_bftree_drain_and_delete，单点分流
- 三迁移臂传 true：重灌臂 wnode/src/resp/objects/rmw_helpers.rs:383、
  懒降阶臂 :413（obj_save 后树清退）、后台降阶臂
  wnode/src/resp/objects/tiered_demote.rs:230（demote_candidate）
- 删键/清退臂传 false 维持随键清 TTL：删空自愈臂 rmw_helpers.rs:365、
  STORE 族 retire_tiered_dest :112、分层删减 drain_or_save
  tiered_collection_ops.rs:380、DEL wkv/src/session/collection.rs:49、
  SET 覆写 wkv/src/session/raw/write/mod.rs:189、wkv/src/range_index/ops.rs:307、
  wkv 测试调用点 tests/store/flush_database.rs:414/:547
- 验收四例已存在：wnode/tests/tiered_promote_demote_ttl.rs
  （refill/lazy_demote/background_demote 逐 tick 原值保留 +
  empty_self_heal 反面对照删空自愈清 TTL），与票据验收逐条对齐

来源：next/qcode8.data-b2.md 条 1（HIGH），主目录票据文件已由编排方移走，本节按该条原文实现。认领子代理：datab2 并发棒之一；分支 datab2-promote-demote-ttl（/tmp/fork 工作树）。

判定：成立。重灌臂、懒降阶臂（object_store_utils.rs apply_rmw_post_operate）与后台降阶臂（tiered_demote.rs demote_candidate）都走 handle_bftree_drain_and_delete 加 promote_collection_to_bftree 或 obj_save 组合，drain 内核（wkv session/collection.rs drain_and_delete_collection_meta）无条件 del_ttl；升阶内核只写元记录与删信封、无 TTL 写回。键全程存活却每次迁移被静默清过期，且 del_ttl 的 delete_raw 经写监听（wkv session/raw/mod.rs notify_write_listener）镜像成 TtlWrite(expire_at=None)，即 AOF/复制面 Persist 条目扩散。C# 对位：ObjectStore/VarLenInputMethods.cs:42 GetRMWModifiedFieldInfo 把 HasExpiration 从源记录原样前移到重写后记录，对象记录重写从不脱落过期字段且不产 TTL 事件。

方案：采用票据第二案，keep_ttl: bool 形参贯通 drain_and_delete_collection_meta 与 handle_bftree_drain_and_delete。升/降阶三臂传 true（只墓碑元记录、不碰 TTL 旁路，AOF 面零 Persist 扩散，且逐 tick 原值保留、不造第二写 TTL 路径、无并发 EXPIRE 竞态）；删空自愈臂、DEL、SET 覆写、分层删减 drain_or_save、STORE 族 retire_tiered_dest 与 wkv 测试调用点传 false，保留既有随键清除语义不变。

验收：新增集成测试 wnode/tests/tiered_promote_demote_ttl.rs 四例，重灌、懒降阶、后台降阶迁移后 ttl_of 逐 tick 原样（对照 C#：分层引擎为仓内自定义、C# 无对位用例，按重写不脱落过期不变式新增），反面对照删空自愈后 TTL 旁路必须清除（防过度矫正留孤儿）。cargo check --workspace --all-targets 私有 target 零错误零警告。
