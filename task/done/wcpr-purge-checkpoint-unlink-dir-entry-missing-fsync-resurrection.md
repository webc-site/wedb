甄别结论：通过 | 定级 P3 | 2026-09-27 主席现码复跑（逐点亲验）：接收面 purge_checkpoint_files_except（checkpoint_store.rs:30-41）与主侧淘汰链 delete_outdated_checkpoints（checkpoint_store.rs:192 起，purge_checkpoint 调用 :218）及 replica_diskbased_sync.rs:202 调用点，删除链全程无 sync_dir，坐实；wcpr::purge_checkpoint（manager/mod.rs:670-687）rm_path_best_effort 后确无目录屏障，坐实；「同缺」成立。wdev/src/lib.rs:56-70 双屏障口径唯一定义处仅覆新建/rename 发布形，删除形确系口径独立缺口；C# CheckpointStore.cs:163-195 DeleteOutdatedCheckpoints 无 Flush 对位亲验。订正危害面：票面「基座回退旧代」在现码被 token 单调（issue_token_after）+ find_latest_checkpoint 的 .meta 完整性过滤挡住，复活旧代 token 恒小于现役，实害收敛为孤儿文件集复活占盘（下一轮 Initialize purge 可再收），故 P3 非 P1；修复本身仍必要（口径对称收口）。并案核查：与 wbftree-detach（rename 形）、wdev-new-directory（新建目录形）、deviations §78（wdev 段新建形）四点异形不并案。执行按票面方案 1-3：删除循环收口后对被清目录补 wdev::sync_dir（先删后刷），主侧淘汰链同收；锁测 purge 后目录项断言+幂等回归；严禁第二套墓碑机制。

问题分析：
1 Garnet 契约对齐：C# 淘汰链 DeleteOutdatedCheckpoints 物理删除后未单独持久目录项（其文件系统语义依赖 NTFS 元数据落盘时机），rust 自定「持久化发布双屏障口径」（wdev/src/lib.rs:70，全仓唯一定义处）覆盖新建面，删除面对位缺口系 rust 侧独立完整性问题。
2 工程现状确证（c01i 票 rust_review 审查席上报备案）：副本接收收口 purge 旧代文件集（wedb/wedb/src/server/replication/replica_diskbased_sync.rs:202 purge；checkpoint_store.rs purge_checkpoint_files_except 同族）unlink 后无 sync_dir——POSIX 下删除的持久化同样须 fsync 父目录，掉电可致已 unlink 的旧代目录项复活，复活文件集与现役基座并存。主侧 publish 双屏障（wbftree lifecycle.rs:742-757 等）仅护发布面；c01i 票（合入 c6f893f）已闭合接收面新建三臂，删除面系另一维度，甄别席 J3 已裁不在其票面。
3 逻辑危害确证：掉电复活旧代 index/meta/hlog 段文件集后，recover_latest 若命中复活 token（时间序或枚举序依赖），基座回退旧代、与已确认导入的新代视图分叉；或残留孤儿文件集永不清（磁盘卫生面）。触发窗=导入收口 purge 与掉电竞速，窄时序。

涉及代码：
rust 文件与函数：
wedb/wedb/src/server/replication/replica_diskbased_sync.rs:purge 调用点（:202 区）
wedb/wedb/src/server/replication/checkpoint_store.rs:purge_checkpoint_files_except（删除循环）
wedb/wdev/src/lib.rs:sync_dir（唯一原语，删除面复用）
主侧对照面：wcpr/src/manager/（DeleteOutdatedCheckpoints 对应淘汰链是否同缺）

对应 c# 文件与函数：
garnet/libs/cluster/Server/Replication/CheckpointStore.cs:DeleteOutdatedCheckpoints（删除面，上游无目录屏障，rust 口径独立成立）

精炼执行方案：
1 purge 删除循环收口后对被清目录（checkpoint_dir 与 token 子目录）补 sync_dir（复用 wdev 单点原语，先删后刷）；主侧淘汰链同面同查同收。
2 测试验证点：purge 后目录项断言（判别用例，掉电不可模拟按仓内惯例）；复活面回归锁——purge 链路幂等（复活文件被下一轮 purge 再收）。
3 严禁为「防复活」引入第二套标记/墓碑机制，目录屏障即收口。
