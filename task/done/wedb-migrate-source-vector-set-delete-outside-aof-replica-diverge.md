归档注记：合入 14776a2a，缺席键盲墓碑镜像入AOF+副本重放缺席收口，迁移删臂改道 delete_string 单链

甄别结论：通过（甄别席 J3，2026-09-27，定级 P1——failover 复活幽灵集、主从永久发散）。两臂直调亲验：keys.rs:946-950 与 migrate_session_vector_set.rs:144-147 皆 vm.delete_migrated_vector_set_of(rk, src_index).await 复合键直删；delete_vector_set_of（vector_manager.rs:859-877，票面行号精确）仅 request_deletion 弃图 + remove_stored_index 摘表，全程零 replicate。不对称自证在码：目标端 vector_manager_migration.rs:181-183 replicate_vector_set_index 合成写 + 注释原文「副本端无迁移帧，仍需合成写注入 AOF 复制链供副本同步」。wkv 缺席零追加证据链亲证：inplace.rs:690-693 return Ok(Ok(false)) 无桶即返。C# 收口亲证：MigrateOperation.cs:269-277 DeleteVectorSet 经 BasicGarnetApi.DELETE（:274）、GarnetRecordTriggers.cs:78-82 Deleted 臂 RequestDeletion。deviations §1911-1917 系回建/换引擎面正交，五池无同案。派沙箱席 c01b。

审核结论：通过（P1 数据发散真案。keys.rs:945-950 与 migrate_session_vector_set.rs:144-147 直调 delete_migrated_vector_set_of→vector_manager.rs:859-877 仅弃图+摘表零 replicate；VectorRegistry 墓碑被 service.rs:219 AOF 镜像白名单排除；wkv 缺席删（inplace.rs:691-693）零追加零 notify。C# MigrateOperation.cs:269 经 DELETE 入日志、GarnetRecordTriggers.cs:80 副本重放收口实证；string/RI 源删经 StoreDelete 镜像收敛唯向量集漏网；目标端 migration.rs:181-185 合成写自证不对称。§95/§140/§160 正交五池净）

整理执行方案（审核席订正版，供 fix 消费）：
1 采一路：wkv delete「双域缺席+钩子命中」分支循 RC 墓碑纪律补发缺席键 StoreDelete 镜像、notify 恰一次；迁移两臂改走 delete_string，DELETING 门与 claim 纪律照旧；退一路（用户 DEL 同缺陷不愈）不合算
2 测试点保留，failover 复活幽灵集锁必加

迁移源端 DELETING 收口向量集清退不入 AOF 复制流，源端副本发散复活

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# MigrateOperation.DeleteVectorSet（garnet/libs/cluster/Server/Migration/MigrateOperation.cs:269-277）经 localServerSession.BasicGarnetApi.DELETE(key) 收口——C# 向量索引记录驻主存，DELETE 落主日志记录随复制链推至源端副本，副本重放经 GarnetRecordTriggers.OnDispose 的 Deleted 臂触发 VectorManager.RequestDeletion，副本同形收敛。向量集迁移删除是入日志、入复制流的。

2. 工程现状确证（Rust 现有实现路径与代码缺陷）
rust 向量索引记录驻 VectorManager 域登记表（wkv 值域外，deviations §79 前后既有口径）。迁移源端删除两臂直调 vm.delete_migrated_vector_set_of：
- wedb/wedb/src/server/migration/migrate_driver/keys.rs:execute_keys_migration DELETING 段（向量集清退循环，keys.rs:945-950）
- wedb/wedb/src/server/migration/migrate_session_vector_set.rs:migrate_vector_set_keys_async DELETING 段（:144-147）
该函数（wedb/wnode/src/resp/vector/vector_manager_migration.rs:delete_migrated_vector_set_of → vector_manager.rs:delete_vector_set_of:859-877）仅 request_deletion 弃内存图 + remove_stored_index 摘登记表并写透墓碑，全程零 replicate_* 合成写注入。登记表写透旁路记录仅供本机检查点持久，不承载副本同步——接收端同行代码自证（vector_manager_migration.rs:182 注：登记表虽经写透旁路记录持久化，但副本端无迁移帧，仍需合成写注入 AOF 复制链供副本同步）：目标端导入用 replicate_vector_set_index / replicate_vector_set_add 合成写收口自家副本，源端删除面对称缺位。
亦不经 wkv 删除单点补位：向量键双域探针恒判缺席，wkv 删除内核（wkv/src/session/raw/write/inplace.rs:delete_or_take_raw_sync_unprotected_with:690-693）无桶条目即回 Ok(Ok(false)) 零追加、零 notify_write_listener，无 AOF 镜像条目；wkv/src/session/collection.rs:delete 的缺席观测钩子臂（:131-136）命中登记也仅推进 WATCH 版本不落任何记录。即本清退在日志面无痕。
查重净：deviations 无同面裁决（§1911-1917 系全量回建/换引擎钩子面）；五池内 migrate 族在册票为导入裸写旁路（todo/wedb-migrate-import-frame-bare-write-outside-rmw-window.md）、DELETING 未持 claim（done）、纪元栅栏（§95），均非本面。

3. 逻辑危害确证
迁移成功非 COPY 交权后，源节点副本的内存登记表、HNSW 图与元素数据记录仍长期持有已迁走向量集，仅下一次全量同步才收口。槽位交换完成后 failover 到该副本即复活已归属目标端的向量集，双侧属主幽灵、已迁数据复活；槽位回迁时幽灵集随副本链再入目标域。主从键级发散不随增量复制自愈，与目标端合成写收敛形态不对称，坐实为漏项而非既定投影。

涉及代码：
rust 文件与函数：
wedb/wedb/src/server/migration/migrate_driver/keys.rs:execute_keys_migration（DELETING 向量集清退臂）
wedb/wedb/src/server/migration/migrate_session_vector_set.rs:migrate_vector_set_keys_async（SLOTS 链 DELETING 臂）
wedb/wnode/src/resp/vector/vector_manager_migration.rs:delete_migrated_vector_set_of
wedb/wnode/src/resp/vector/vector_manager.rs:delete_vector_set_of
wedb/wkv/src/session/raw/write/inplace.rs:delete_or_take_raw_sync_unprotected_with（缺席桶零追加无镜像证据链）
wedb/wkv/src/session/collection.rs:delete（缺席观测钩子臂零记录证据链）

对应 c# 文件与函数：
garnet/libs/cluster/Server/Migration/MigrateOperation.cs:DeleteVectorSet
garnet/libs/cluster/Server/Migration/MigrateSessionSlots.cs:CreateAndRunMigrateTasksAsync（调用点，向量集收尾段）
garnet/libs/server/Storage/Functions/MainStore/../GarnetRecordTriggers.cs:OnDispose（副本重放删除 → RequestDeletion 收口臂）

精炼执行方案：
1. 优先单机制归一：wkv collection.rs:delete 的「双域未命中 + delete_miss_hook 命中」分支持真时，循 RC 墓碑臂既有纪律（try_delete_raw_sync_unprotected_with 盲追加后 notify_write_listener 恰一次）补发一条缺席键墓碑镜像，使迁移源端、用户 DEL、SET 守卫共用同一 wkv 用户键删除单点与既有重放缺席观测钩子收口；迁移两臂的 vm.delete_migrated_vector_set_of 直调改走 storage.delete_string 通道（键权判据不变，DELETING 门与 claim 纪律照旧）。
2. 若裁决不收口 wkv 面（墓碑镜像波及用户面需另裁），退一路：按 RENAME 既有形态立迁移向量集清退合成写（vector_manager_replication.rs 增设清退哨兵，对标 replicate_vector_set_rename + 重放端 delete_vector_set_of 复用先例，禁第二套清退形态），两臂删除点注入。
3. 测试验证点：真帧迁移用例——源节点带副本，非 COPY 迁移向量集键成功后断言源副本登记表探针与 VCARD 判集已清（形态参照 wedb/tests/replica_diskbased_vector_rebuild.rs 锁面）；COPY 态回归不注入；目标端副本既有合成写锁面不回退。
