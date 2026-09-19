优先级：低
来源：next/muse.design.md 条 2、5、8、11 与 next/agy.design.md 条 16 / next/muse.design.md
条 9 的锚点复挂增量（结构体分层本体已裁定不成立，见 task/reject/design-snapshot-triple-struct-unify.md）。
取证基线：主仓 dev 当下代码；check.js 判重口径实测（js/check.js:304 dupDefFind，
按函数文档注释内 CS_REF_REGEX 匹配「路径.cs:符号」聚合，>1 处即报）。

问题
五组 C# 锚点一符号多挂（均挂在函数文档注释内，check.js 会报重复定义），复挂形态为
「装配/委托/分支侧重复挂真实现锚点」；只改注释即可消除，不动代码。

取证（逐组：锚点 → 挂载点）
1 GetSslServerAuthenticationOptions 双挂（muse 条 2 报三处，实测 from_pem_files 无锚点，实为两处）：
  wedb/wnode/src/tls/config.rs:57 from_der 的 doc、:91 server_config 的 doc
  （server_config 是装配单点保留，from_der 去锚点改散文说明）
2 OnDispose 四挂：
  wedb/wnode/src/resp/array_commands.rs:160 network_del、
  wedb/wnode/src/storage/session/storage_session.rs:500 vector_registry_delete_hook、
  wedb/wnode/src/resp/garnet_api/mod.rs:331 with_vector_manager、
  wedb/wnode/src/resp/vector/vector_manager.rs:533 delete_vector_set
  （删除单点 delete_vector_set 保留，其余三处去锚点改散文引用）
3 VectorManager 构造锚点双挂（两函数皆非构造）：
  wedb/wnode/src/resp/vector/vector_manager_cleanup.rs ensure_cleanup_tasks_started、
  wedb/wnode/src/service.rs with_vector_set_preview，两处去锚点
4 ContextReadWithPrefetch 双挂：
  wedb/wkv/src/session/raw/batch.rs:24 read_batch_with 的 doc（「严格对照
  libs/storage/.../Tsavorite.cs:ContextReadWithPrefetch」）、
  wedb/windex/src/table.rs:608 prefetch_batch_probes 的 doc
  （read_batch_with 是 C# 公共 API 直接对位者保留；prefetch_batch_probes 是其内部预取探针
  段，去锚点改散文「协作段见 read_batch_with」——注意 muse 条 11 建议的保留方向反了）
5 GetDatabaseStoreStats / GetDatabasePersistenceStats 双挂：
  wedb/wnode/src/resp/garnet_api/mod.rs:203 project_db_snapshot、:182 project_aof_snapshot
  （投影函数）与 wedb/wmetric/src/info/garnet_info_metrics.rs:683 get_database_store_stats、
  :790 get_database_persistence_stats（统计真实现）同挂
  （wmetric 真实现保留，投影函数去锚点，doc 已自称「wmetric DbSnapshot 的全仓唯一组装点」）
C# 对标：garnet/libs/server/TLS/GarnetTlsOptions.cs:GetSslServerAuthenticationOptions、
garnet/libs/server/Storage/Functions/GarnetRecordTriggers.cs:OnDispose、
garnet/libs/server/Resp/Vector/VectorManager.cs:VectorManager、
garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Tsavorite.cs:ContextReadWithPrefetch、
garnet/libs/server/Metrics/Info/GarnetInfoMetrics.cs 两统计函数。

修法建议
五组各保留一处真实现挂载（上文括号内判定），其余挂载点把「路径.cs:符号」改为散文式
说明（去冒号或改自然语言），一处一锚；落地后跑 ./js/check.js 确认重复组消除。
禁为去锚点删除任何语义说明文字。
